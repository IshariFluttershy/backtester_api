#[macro_use]
extern crate rocket;
extern crate queues;
extern crate strategy_backtester;
use binance::account::*;
use binance::api::*;
use binance::futures::account::*;
use binance::futures::*;
use binance::general::General;
use binance::market::Market;
use binance::model::KlineSummaries;
use binance::model::KlineSummary;
use chrono::{Datelike, Timelike, Utc};
use queues::*;
use rocket::data;
use rocket::tokio;
use rocket::tokio::spawn;
use rocket::tokio::sync::Mutex;
use rocket::State;
use rocket::{get, http::Status, serde::json::Json};
use serde::Serialize;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::thread;
use std::time;
use std::time::Duration;
use std::time::Instant;
use strategy_backtester::backtest::*;
use strategy_backtester::patterns::*;
use strategy_backtester::strategies::*;
use strategy_backtester::strategies_creator::*;
use strategy_backtester::*;

const DATA_FOLDER: &str = "data/";
const DATA_PATH: &str = "data/testdataPartDL.json";
const RESULTS_PATH: &str = "results/full/";
const AFFINED_RESULTS_PATH: &str = "results/affined/";
const MONEY_EVOLUTION_PATH: &str = "withMoneyEvolution/";
const START_MONEY: f64 = 100.;
const WORKERS: usize = 8;


pub struct DataDownloadingState {
    pub is_downloading_data: AtomicBool,
}

pub struct DataDownloadingStateSender {
    pub sender: std::sync::mpsc::Sender<KLineDatasId>,
}

#[derive(Clone)]
pub struct KLineDatasId {
    pub interval: String,
    pub symbol: String,
}

#[derive(Serialize)]
pub struct GenericResponse {
    pub status: String,
    pub message: String,
}

#[get("/healthchecker")]
pub async fn health_checker_handler() -> Result<Json<GenericResponse>, Status> {
    const MESSAGE: &str = "Build Simple CRUD API with Rust and Rocket";

    let response_json = GenericResponse {
        status: "success".to_string(),
        message: MESSAGE.to_string(),
    };
    Ok(Json(response_json))
}

#[get("/test?<symbol>&<interval>")]
pub async fn test_handler(
    symbol: String,
    interval: String,
) -> Result<Json<GenericResponse>, Status> {
    let klines: Vec<KlineSummary>;
    if let Ok(content) = fs::read_to_string(format!("{}{}-{}.json", DATA_FOLDER, symbol, interval))
    {
        println!("data file found, deserializing");
        klines = serde_json::from_str(&content).unwrap();
        println!("deserializing finished");
    } else {
        return Ok(Json(GenericResponse {
            status: "error".to_string(),
            message: format!(
                "There is no data. Download the corresponding data before. You sent {} - {}",
                symbol, interval
            ),
        }));
    }

    let strategies = create_w_and_m_pattern_strategies(
        START_MONEY,
        ParamMultiplier {
            min: 0.5,
            max: 4.,
            step: 1.,
        },
        ParamMultiplier {
            min: 0.2,
            max: 2.,
            step: 0.2,
        },
        ParamMultiplier {
            min: 1,
            max: 1,
            step: 1
        },
        ParamMultiplier {
            min: 10,
            max: 30,
            step: 5
        },
        ParamMultiplier {
            min: 1.,
            max: 1.,
            step: 1.,
        },
        MarketType::Spot
    );

    println!("{} strategies to test", strategies.len());
    let start = Instant::now();

    let mut threads_results = Vec::new();
    let chunk_size = (strategies.len() + WORKERS - 1) / WORKERS;
    for i in 0..WORKERS {
        let start = i * chunk_size;
        let mut num_elements = if i < WORKERS - 1 {
            chunk_size
        } else if i * chunk_size <= strategies.len() {
            strategies.len() - i * chunk_size
        } else {
            break;
        };
        
        if start >= strategies.len() {
            break;
        } else if start + num_elements >= strategies.len() {
            num_elements = strategies.len() - start;
        }

        println!("Worker n {} -- start = {} -- num_elements = {} -- chunk-size = {}", i, start, num_elements, chunk_size);
        let mut chunk = Vec::from(&strategies[start..][..num_elements]);
        
        let klines_clone = klines.clone();
        let result = thread::spawn(move || {
            Backtester::new(klines_clone, 1)
            .add_strategies(&mut chunk)
            .start()
            .get_results()
        });
        threads_results.push(result);
    }

    let mut results = Vec::new();
    for result in threads_results {
        results.append(&mut result.join().unwrap());
    }
    println!("Strategies result = {}", results.len());
    let duration = start.elapsed();
    println!("Elapsed total time : {}s", duration.as_secs());
    println!();

    let mut affined_results: Vec<StrategyResult> = results
        .iter()
        .filter(|x| x.total_closed > 100)
        .cloned()
        .collect();

    let results_json = serde_json::to_string_pretty(&results).unwrap();
    let mut file = File::create(
        RESULTS_PATH.to_owned()
            + MONEY_EVOLUTION_PATH
            + generate_result_name(&symbol, &interval).as_str(),
    )
    .unwrap();
    file.write_all(results_json.as_bytes()).unwrap();

    results.iter_mut().for_each(|x| x.money_evolution.clear());
    let results_json = serde_json::to_string_pretty(&results).unwrap();
    let mut file =
        File::create(RESULTS_PATH.to_owned() + generate_result_name(&symbol, &interval).as_str())
            .unwrap();
    file.write_all(results_json.as_bytes()).unwrap();

    let affined_results_json = serde_json::to_string_pretty(&affined_results).unwrap();
    let mut file = File::create(
        AFFINED_RESULTS_PATH.to_owned()
            + MONEY_EVOLUTION_PATH
            + generate_result_name(&symbol, &interval).as_str(),
    )
    .unwrap();
    file.write_all(affined_results_json.as_bytes()).unwrap();

    affined_results
        .iter_mut()
        .for_each(|x| x.money_evolution.clear());
    let affined_results_json = serde_json::to_string_pretty(&affined_results).unwrap();
    let mut file = File::create(
        AFFINED_RESULTS_PATH.to_owned() + generate_result_name(&symbol, &interval).as_str(),
    )
    .unwrap();
    file.write_all(affined_results_json.as_bytes()).unwrap();

    Ok(Json(GenericResponse {
        status: "success".to_string(),
        message: format!("Strategy tested for {} - {}", symbol, interval),
    }))
}

#[get("/dl?<symbol>&<interval>")]
pub async fn kline_data_dl_handler(
    symbol: String,
    interval: String,
    data_dl_state: &State<std::sync::Arc<std::sync::Mutex<DataDownloadingState>>>,
    sender_state: &State<std::sync::Arc<std::sync::Mutex<DataDownloadingStateSender>>>,
) -> Result<Json<GenericResponse>, Status> {
    let mut response_json = GenericResponse {
        status: "success".to_string(),
        message: format!("Downloading kline datas : {}-{}", symbol, interval),
    };
    let is_downloading_data = data_dl_state
        .lock()
        .unwrap()
        .is_downloading_data
        .load(Ordering::Relaxed);
    if is_downloading_data {
        response_json.message = format!(
            "Already downloading datas. Queuing : {}-{}",
            symbol, interval
        );
    }

    sender_state
        .lock()
        .unwrap()
        .sender
        .send(KLineDatasId { symbol, interval });

    Ok(Json(response_json))
}

fn kline_dl_manager(
    data_dl_state: &std::sync::Arc<std::sync::Mutex<DataDownloadingState>>,
    rx: &Receiver<KLineDatasId>,
) {
    let dl_pool: Arc<std::sync::Mutex<Queue<KLineDatasId>>> =
        Arc::new(std::sync::Mutex::new(queue![]));
    let clone = dl_pool.clone();
    let market: Market = Binance::new(None, None);
    let general: General = Binance::new(None, None);
    let data_dl_state = data_dl_state.clone();

    thread::spawn(move || loop {
        if clone.lock().unwrap().size() > 0 {
            data_dl_state
                .lock()
                .unwrap()
                .is_downloading_data
                .swap(true, Ordering::Relaxed);
        }

        while let Ok(id) = clone.lock().unwrap().remove() {
            let mut server_time = 0;
            let result = general.get_server_time();
            match result {
                Ok(answer) => {
                    println!("Server Time: {}", answer.server_time);
                    server_time = answer.server_time;
                }
                Err(e) => println!("Error: {}", e),
            }

            retreive_test_data(server_time, &market, id.clone());
            println!(
                "Data retreived from the server : {}-{}",
                id.symbol, id.interval
            );
        }

        data_dl_state
            .lock()
            .unwrap()
            .is_downloading_data
            .swap(false, Ordering::Relaxed);

        thread::sleep(Duration::from_secs(10));
    });

    loop {
        let kline_data = (rx).recv().unwrap();
        dl_pool.lock().unwrap().add(kline_data);
    }
}

#[launch]
fn rocket() -> _ {
    let state = Arc::new(std::sync::Mutex::new(DataDownloadingState {
        is_downloading_data: AtomicBool::new(false),
    }));
    let clone = state.clone();
    let (tx, rx) = channel::<KLineDatasId>();

    // Background processing thread
    thread::spawn(move || {
        kline_dl_manager(&clone, &rx);
    });

    let sender_state = Arc::new(std::sync::Mutex::new(DataDownloadingStateSender {
        sender: tx,
    }));
    rocket::build().manage(state).manage(sender_state).mount(
        "/api",
        routes![health_checker_handler, test_handler, kline_data_dl_handler,],
    )
}

fn retreive_test_data(
    server_time: u64,
    market: &Market,
    kline_data: KLineDatasId,
) -> Vec<KlineSummary> {
    let mut i: u64 = 100;
    let start_i = i;
    let mut j = 0;
    let mut start_time = server_time - (i * 60 * 1000 * 1000);
    let mut end_time = server_time - ((i - 1) * 60 * 1000 * 1000);

    let mut klines = Vec::new();
    while let Ok(retreive_klines) = market.get_klines(
        kline_data.symbol.clone(),
        kline_data.interval.clone(),
        1000,
        start_time,
        end_time,
    ) {
        if i == 0 {
            break;
        }
        if let KlineSummaries::AllKlineSummaries(mut retreived_vec) = retreive_klines {
            klines.append(&mut retreived_vec);
        }

        start_time = end_time + 1000 * 60;
        end_time = start_time + 60 * 1000 * 1000;

        i -= 1;
        j += 1;
        if i % 10 == 0 {
            println!("Retreived {}/{} bench of klines data", j, start_i);
        }
    }

    let serialized = serde_json::to_string_pretty(&klines).unwrap();
    let mut file = File::create(format!(
        "{}{}-{}.json",
        DATA_FOLDER, kline_data.symbol, kline_data.interval
    ))
    .unwrap();
    file.write_all(serialized.as_bytes()).unwrap();
    klines
}

fn generate_result_name(symbol: &String, interval: &String) -> String {
    let now = Utc::now();
    format!(
        "{}-{}_{}_{}_{}_{}h{}m{}s.json",
        symbol,
        interval,
        now.year(),
        now.month(),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    )
}
