use crate::ohlc_stream::Ohlc;
use csv::StringRecord;
use std::time::Duration;
use tokio::time;
use tracing::*;

pub fn self_update(ctx: crate::Context) -> tokio::task::JoinHandle<Result<(), anyhow::Error>> {
    tokio::spawn(async move {
        loop {
            info!("reading file ...");
            let file = std::fs::File::open("./mkt_data.csv")?;
            let mut records = csv::ReaderBuilder::new()
                .has_headers(false)
                .from_reader(file)
                .into_records();

            let mut interval = time::interval(Duration::from_millis(1000));
            loop {
                interval.tick().await;
                match records.next() {
                    Some(Ok(row)) => {
                        let v = to_vec(row);
                        let ohlc = build_query(v);
                        let mut map = ctx.map.lock().await;
                        if let Some(broadcast) = map.get(&ohlc.code) {
                            if let Err(err) = broadcast.send(ohlc) {
                                // all users are out, this message can never send to any
                                error!(?err);
                                // remove such code to save memory
                                map.remove(&err.0.code);
                            }
                        }
                    }

                    Some(Err(inner)) => {
                        error!(?inner);
                        return Err(inner.into());
                    }

                    None => break,
                }
            }
        }
    })
}

fn build_query(v: Vec<String>) -> Ohlc {
    let arr: [String; 10] = v.try_into().unwrap();
    match arr {
        [timestamp, symbol, market, open, high, low, close, volume, dod, _dod_percent] => Ohlc {
            code: symbol.clone() + "." + &to_market(&market)[0..1],
            timestamp: timestamp.parse().ok(),
            symbol: symbol.parse().ok(),
            market: Some(to_market(&market).into_owned()),
            open: open.parse().ok(),
            high: high.parse().ok(),
            low: low.parse().ok(),
            close: close.parse().ok(),
            volume: volume.parse().ok(),
            dod: dod.parse().ok(),
            step: Some(1.0),
            tick: Some(1),
        },
    }
}

fn to_market(m: &str) -> std::borrow::Cow<str> {
    match m {
        "1" => "Tokyo",
        "3" => "Nagoya",
        "6" => "Fukuoka",
        "8" => "Sapporo",
        _ => "unknown",
    }
    .into()
}

fn to_vec(s: StringRecord) -> Vec<String> {
    s.into_iter().map(ToString::to_string).collect::<Vec<_>>()
}

