use crate::ohlc_stream::ohlc_stream_client::OhlcStreamClient;
use crate::ohlc_stream::Ohlc;
use csv::StringRecord;
use csv::StringRecordsIntoIter;
use std::borrow::Cow;
use tokio::time;
use tokio::time::Duration;
use tonic::transport::Channel;
use tonic::Request;
use tracing::error;
use tracing::info;

fn to_vec(s: StringRecord) -> Vec<String> {
    s.into_iter().map(ToString::to_string).collect::<Vec<_>>()
}

fn to_market(m: &str) -> Cow<str> {
    match m {
        "1" => "Tokyo",
        "3" => "Nagoya",
        "6" => "Fukuoka",
        "8" => "Sapporo",
        _ => "unknown",
    }
    .into()
}

pub async fn update<R: std::io::Read>(
    mut client: OhlcStreamClient<Channel>,
    mut records: StringRecordsIntoIter<R>,
) -> Result<(), anyhow::Error> {
    info!("start updating");
    let mut interval = time::interval(Duration::from_millis(1000));
    loop {
        interval.tick().await;
        match records.next() {
            Some(Ok(row)) => {
                let v = to_vec(row);
                let arr: [String; 10] = v.try_into().unwrap();
                match arr {
                    [timestamp, symbol, market, open, high, low, close, volume, dod, _dod_percent] =>
                    {
                        let req = Request::new(Ohlc {
                            timestamp: timestamp.parse().ok(),
                            code: symbol.clone() + "." + &to_market(&market)[0..1],
                            symbol: symbol.parse().ok(),
                            market: Some(to_market(&market).into_owned()),
                            open: open.parse().ok(),
                            high: high.parse().ok(),
                            low: low.parse().ok(),
                            close: close.parse().ok(),
                            volume: volume.parse().ok(),
                            dod: dod.parse().ok(),
                            tick: Some(1),
                            step: Some(1.0),
                        });
                        let _res = client.update_ohlc(req).await?;
                    }
                }
            }
            others => {
                error!(?others);
                break;
            }
        }
    }
    Result::<_, anyhow::Error>::Ok(())
}

