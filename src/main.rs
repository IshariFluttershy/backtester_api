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
use strategy_backtester::tools::retreive_test_data;
use std::fs;
use std::fs::File;
use std::io;
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
const START_MONEY: f64 = 1000.;
const WORKERS: usize = 20;

pub struct DataDownloadingState {
    pub is_downloading_data: AtomicBool,
}

pub struct TestingState {
    pub is_testing: AtomicBool,
}

pub struct DataDownloadingStateSender {
    pub sender: std::sync::mpsc::Sender<KLineDatasId>,
}

pub struct TestingStateSender {
    pub sender: std::sync::mpsc::Sender<BacktesterDatas>,
}

#[derive(Clone)]
pub struct BacktesterDatas {
    pub data_id: KLineDatasId,
    pub tp: ParamMultiplier<f64>,
    pub sl: ParamMultiplier<f64>,
    pub kline_repetition: ParamMultiplier<usize>,
    pub kline_range: ParamMultiplier<usize>,
    pub risk: ParamMultiplier<f64>,
    pub market_type: MarketType,
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
    state: &State<TestingState>,
) -> Result<Json<GenericResponse>, Status> {
    let symbol = symbol.to_uppercase();
    let is_testing = state.is_testing.load(Ordering::Acquire);
    if is_testing {
        return Ok(Json(GenericResponse {
            status: "error".to_string(),
            message: "Strategy is already testing".to_string(),
        }));
    } else if !is_testing {
        state.is_testing.swap(true, Ordering::Relaxed);
    }

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
            min: 1.,
            max: 6.,
            step: 1.,
        },
        ParamMultiplier {
            min: 0.5,
            max: 2.,
            step: 0.5,
        },
        ParamMultiplier {
            min: 2,
            max: 5,
            step: 1,
        },
        ParamMultiplier {
            min: 25,
            max: 25,
            step: 5,
        },
        ParamMultiplier {
            min: 1.,
            max: 1.,
            step: 1.,
        },
        MarketType::Spot,
    );

    println!("{} strategies to test", strategies.len());
    let start = Instant::now();
    let readable_klines = Backtester::to_all_math_kline(klines);
    let arc_klines = Arc::new(readable_klines);

    let mut threads_results = Vec::new();
    let chunk_size = (strategies.len() + WORKERS - 1) / WORKERS;
    let (tx, rx) = channel::<(f32, usize)>();
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

        println!(
            "Worker n {} -- start = {} -- num_elements = {} -- chunk-size = {}",
            i, start, num_elements, chunk_size
        );
        let mut chunk = Vec::from(&strategies[start..][..num_elements]);
        let tx_clone = tx.clone();

        let arc_klines_clone = arc_klines.clone();
        let result = thread::spawn(move || {
            Backtester::new(arc_klines_clone, Some(tx_clone), Some(i), false)
                .add_strategies(&mut chunk)
                .start()
                .get_results()
        });
        threads_results.push(result);
    }

    let mut results = Vec::new();
    let mut trackers: Vec<f32> = vec![];
    for i in 0..threads_results.len() {
        trackers.push(0.);
    }
    let mut duration = Duration::new(0, 0);
    let mut last_display = 0;
    let num_of_threads = threads_results.len() as f32;

    for result in threads_results {
        while !result.is_finished() {
            if let Ok((done, id)) = rx.recv_timeout(Duration::new(1, 0)) {
                trackers[id] = done;
                duration = start.elapsed();
                if last_display + 1000 < duration.as_millis() {
                    print!("\rTotal done : {:.2}% -- Elapsed time : {}s -- Estimated total time : {}s                     ", (trackers.iter().sum::<f32>()/num_of_threads), duration.as_secs(), (100./(trackers.iter().sum::<f32>()/num_of_threads) * (duration.as_secs() as f32)) as u32);
                    io::stdout().flush().unwrap();
                    last_display = duration.as_millis();
                }
            } else {
                print!("\rTotal done : {:.2}% -- Elapsed time : {}s -- Estimated total time : {}s                             ", (trackers.iter().sum::<f32>()/num_of_threads), duration.as_secs(), (100./(trackers.iter().sum::<f32>()/num_of_threads) * (duration.as_secs() as f32)) as u32);
                io::stdout().flush().unwrap();
                last_display = duration.as_millis();
            }
        }
        results.append(&mut result.join().unwrap());
    }
    println!();
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

    state.is_testing.swap(false, Ordering::Release);

    Ok(Json(GenericResponse {
        status: "success".to_string(),
        message: format!("Strategy tested for {} - {}", symbol, interval),
    }))
}

fn test_manager(
    test_state: &std::sync::Arc<std::sync::Mutex<TestingState>>,
    rx: &Receiver<BacktesterDatas>,
) {
    let test_pool: Arc<std::sync::Mutex<Queue<BacktesterDatas>>> =
        Arc::new(std::sync::Mutex::new(queue![]));
    let clone = test_pool.clone();
    let test_state = test_state.clone();

    thread::spawn(move || loop {
        if clone.lock().unwrap().size() > 0 {
            test_state
                .lock()
                .unwrap()
                .is_testing
                .swap(true, Ordering::Relaxed);
        }

        while let Ok(id) = clone.lock().unwrap().remove() {

        }

        test_state
            .lock()
            .unwrap()
            .is_testing
            .swap(false, Ordering::Relaxed);

        thread::sleep(Duration::from_secs(10));
    });

    loop {
        let backtest_data = (rx).recv().unwrap();
        test_pool.lock().unwrap().add(backtest_data);
    }
}

#[get("/dl?<symbol>&<interval>")]
pub async fn kline_data_dl_handler(
    symbol: String,
    interval: String,
    data_dl_state: &State<std::sync::Arc<std::sync::Mutex<DataDownloadingState>>>,
    sender_state: &State<std::sync::Arc<std::sync::Mutex<DataDownloadingStateSender>>>,
) -> Result<Json<GenericResponse>, Status> {
    let symbol = symbol.to_uppercase();
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

            retreive_test_data(server_time, &market, id.symbol.clone(), id.interval.clone(), DATA_FOLDER.to_string(), 1, 1000, true);
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
    let dl_state = Arc::new(std::sync::Mutex::new(DataDownloadingState {
        is_downloading_data: AtomicBool::new(false),
    }));
    let test_state = Arc::new(std::sync::Mutex::new(TestingState {
        is_testing: AtomicBool::new(false),
    }));
    let dl_clone = dl_state.clone();
    let test_clone = test_state.clone();

    let (tx, rx) = channel::<KLineDatasId>();
    let (backtest_tx, backtest_rx) = channel::<BacktesterDatas>();

    // Background processing thread
    thread::spawn(move || {
        kline_dl_manager(&dl_clone, &rx);
    });

    thread::spawn(move || {
        test_manager(&test_clone, &backtest_rx);
    });

    let dl_sender_state = Arc::new(std::sync::Mutex::new(DataDownloadingStateSender {
        sender: tx,
    }));
    let backtest_sender_state = Arc::new(std::sync::Mutex::new(TestingStateSender {
        sender: backtest_tx,
    }));
    let test_state = TestingState {
        is_testing: false.into(),
    };
    rocket::build()
        .manage(dl_state)
        .manage(dl_sender_state)
        .manage(test_state)
        .manage(backtest_sender_state)
        .mount(
            "/api",
            routes![health_checker_handler, test_handler, kline_data_dl_handler,],
        )
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
