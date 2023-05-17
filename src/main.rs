#[macro_use]
extern crate rocket;
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
use strategy_backtester::backtest::*;
use strategy_backtester::patterns::*;
use strategy_backtester::strategies::*;
use strategy_backtester::*;

const DATA_FOLDER: &str = "data/";
const DATA_PATH: &str = "data/testdataPartDL.json";
const RESULTS_PATH: &str = "results/full/";
const AFFINED_RESULTS_PATH: &str = "results/affined/";
const MONEY_EVOLUTION_PATH: &str = "withMoneyEvolution/";
const START_MONEY: f64 = 100.;

pub struct DataDownloadingState {
    pub is_downloading_data: AtomicBool,
}

pub struct DataDownloadingStateSender {
    pub sender: std::sync::mpsc::Sender<KLineDatas>,
}

pub struct KLineDatas {
    pub interval: String,
    pub symbol: String,
}

struct ParamMultiplier {
    min: f64,
    max: f64,
    step: f64,
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

#[get("/test")]
pub async fn test_handler(
    data_dl_state: &State<std::sync::Arc<std::sync::Mutex<DataDownloadingState>>>,
) -> Result<Json<GenericResponse>, Status> {
    let is_downloading_data = data_dl_state
        .lock()
        .unwrap()
        .is_downloading_data
        .swap(true, Ordering::Relaxed);
    let message: String = format!("Hey ! Ca dl des datas ? : {}", is_downloading_data);

    let response_json = GenericResponse {
        status: "success".to_string(),
        message,
    };

    data_dl_state
        .lock()
        .unwrap()
        .is_downloading_data
        .swap(true, Ordering::Relaxed);

    let klines;
    if let Ok(content) = fs::read_to_string(DATA_PATH) {
        println!("data file found, deserializing");
        klines = serde_json::from_str(&content).unwrap();
        println!("deserializing finished");
    } else {
        return Err(Status::BadRequest);
    }

    let mut backtester = Backtester::new(klines, 64);
    create_w_and_m_pattern_strategies(
        &mut backtester,
        ParamMultiplier {
            min: 2.,
            max: 2.,
            step: 1.,
        },
        ParamMultiplier {
            min: 1.,
            max: 1.,
            step: 1.,
        },
        4,
        4,
        20,
        20,
        ParamMultiplier {
            min: 1.,
            max: 1.,
            step: 1.,
        },
        MarketType::Spot,
    );
    backtester.start();
    println!();

    let mut results = backtester.get_results();
    let mut affined_results: Vec<StrategyResult> = results
        .iter()
        .filter(|x| x.total_closed > 100)
        .cloned()
        .collect();

    let results_json = serde_json::to_string_pretty(&results).unwrap();
    let mut file = File::create(
        RESULTS_PATH.to_owned() + MONEY_EVOLUTION_PATH + generate_result_name().as_str(),
    )
    .unwrap();
    file.write_all(results_json.as_bytes()).unwrap();

    results.iter_mut().for_each(|x| x.money_evolution.clear());
    let results_json = serde_json::to_string_pretty(&results).unwrap();
    let mut file = File::create(RESULTS_PATH.to_owned() + generate_result_name().as_str()).unwrap();
    file.write_all(results_json.as_bytes()).unwrap();

    let affined_results_json = serde_json::to_string_pretty(&affined_results).unwrap();
    let mut file = File::create(
        AFFINED_RESULTS_PATH.to_owned() + MONEY_EVOLUTION_PATH + generate_result_name().as_str(),
    )
    .unwrap();
    file.write_all(affined_results_json.as_bytes()).unwrap();

    affined_results
        .iter_mut()
        .for_each(|x| x.money_evolution.clear());
    let affined_results_json = serde_json::to_string_pretty(&affined_results).unwrap();
    let mut file =
        File::create(AFFINED_RESULTS_PATH.to_owned() + generate_result_name().as_str()).unwrap();
    file.write_all(affined_results_json.as_bytes()).unwrap();

    Ok(Json(response_json))
}

#[get("/dl?<symbol>&<interval>")]
pub async fn kline_data_dl_handler(
    symbol: String,
    interval: String,
    data_dl_state: &State<std::sync::Arc<std::sync::Mutex<DataDownloadingState>>>,
    sender_state: &State<std::sync::Arc<std::sync::Mutex<DataDownloadingStateSender>>>,
) -> Result<Json<GenericResponse>, Status> {
    let is_downloading_data = data_dl_state
        .lock()
        .unwrap()
        .is_downloading_data
        .load(Ordering::Relaxed);
    if is_downloading_data {
        return Ok(Json(GenericResponse {
            status: "error".to_string(),
            message: "Data is already downloading, wait for it to finish then try again"
                .to_string(),
        }));
    }

    sender_state.lock().unwrap().sender.send(KLineDatas {
        symbol,
        interval,
    });

    let message: String = format!("Downloading kline datas : ");
    let response_json = GenericResponse {
        status: "success".to_string(),
        message,
    };

    Ok(Json(response_json))
}

fn kline_dl_manager(
    data_dl_state: &std::sync::Arc<std::sync::Mutex<DataDownloadingState>>,
    rx: &Receiver<KLineDatas>,
) {
    println!("En attente de message");
    let kline_data = rx.recv().unwrap();
    println!("Message recu !");

    if data_dl_state
        .lock()
        .unwrap()
        .is_downloading_data
        .load(Ordering::Relaxed)
    {
        return;
    }
    data_dl_state
        .lock()
        .unwrap()
        .is_downloading_data
        .swap(true, Ordering::Relaxed);

    let market: Market = Binance::new(None, None);
    let general: General = Binance::new(None, None);

    let mut server_time = 0;
    let result = general.get_server_time();
    match result {
        Ok(answer) => {
            println!("Server Time: {}", answer.server_time);
            server_time = answer.server_time;
        }
        Err(e) => println!("Error: {}", e),
    }

    println!("NO data file found, retreiving data from Binance server");
    retreive_test_data(server_time, &market, kline_data);
    println!("Data retreived from the server.");
    data_dl_state
        .lock()
        .unwrap()
        .is_downloading_data
        .swap(false, Ordering::Relaxed);
}

#[launch]
fn rocket() -> _ {
    let state = Arc::new(std::sync::Mutex::new(DataDownloadingState {
        is_downloading_data: AtomicBool::new(false),
    }));
    let clone = state.clone();
    let (tx, rx) = channel::<KLineDatas>();

    // Background processing thread
    thread::spawn(move || loop {
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

fn create_w_and_m_pattern_strategies(
    backtester: &mut Backtester,
    tp: ParamMultiplier,
    sl: ParamMultiplier,
    min_klines_repetitions: usize,
    max_klines_repetitions: usize,
    min_klines_range: usize,
    max_klines_range: usize,
    risk: ParamMultiplier,
    market_type: MarketType,
) {
    let mut strategies: Vec<Strategy> = Vec::new();
    let mut i = tp.min;
    while i <= tp.max {
        let mut j = sl.min;
        while j <= sl.max {
            for k in min_klines_repetitions..=max_klines_repetitions {
                for l in min_klines_range..=max_klines_range {
                    let mut m = risk.min;
                    while m <= risk.max {
                        let pattern_params_w: Vec<Arc<dyn PatternParams>> =
                            vec![Arc::new(WPatternParams {
                                klines_repetitions: k,
                                klines_range: l,
                                name: PatternName::W,
                            })];

                        let pattern_params_m: Vec<Arc<dyn PatternParams>> =
                            vec![Arc::new(MPatternParams {
                                klines_repetitions: k,
                                klines_range: l,
                                name: PatternName::M,
                            })];

                        strategies.push((
                            strategies::create_wpattern_trades,
                            StrategyParams {
                                tp_multiplier: i,
                                sl_multiplier: j,
                                risk_per_trade: m * 0.01,
                                money: START_MONEY,
                                name: StrategyName::W,
                                market_type,
                            },
                            Arc::new(pattern_params_w),
                        ));

                        /*strategies.push((
                            strategies::create_mpattern_trades,
                            StrategyParams {
                                tp_multiplier: i,
                                sl_multiplier: j,
                                risk_per_trade: m * 0.01,
                                money: START_MONEY,
                                name: StrategyName::M,
                                market_type
                            },
                            Arc::new(pattern_params_m),
                        ));*/
                        m += risk.step;
                    }
                }
            }
            j += sl.step;
        }

        i += tp.step;
    }

    backtester.add_strategies(&mut strategies);
}

fn retreive_test_data(
    server_time: u64,
    market: &Market,
    kline_data: KLineDatas,
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

fn generate_result_name() -> String {
    let now = Utc::now();
    format!(
        "{}_{}_{}_{}h{}m{}s.json",
        now.year(),
        now.month(),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    )
}
