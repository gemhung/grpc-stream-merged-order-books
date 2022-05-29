pub mod orderbook {
    tonic::include_proto!("orderbook");
}
mod models;

use crate::orderbook::order_book_aggregator_client::OrderBookAggregatorClient;
use crate::orderbook::{Data, OrderBook};
use futures_util::stream::StreamExt;
use futures_util::SinkExt;
use std::str::FromStr;
use structopt::StructOpt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::Message;
use tonic::transport::Channel;
use tonic::transport::Uri;
use tonic::Request;
use tracing::*;
use url::Url;

const BITSMAP_WS_API: &str = "wss://ws.bitstamp.net";

#[derive(Debug, StructOpt)]
#[structopt(name = "bitstamp client", about = "An example of StructOpt usage.")]
struct Opt {
    #[structopt(short, long, default_value = "btcusdt")]
    symbol: String,

    #[structopt(short, long, default_value = "http://[::1]:10000")]
    endpoint: String,

    #[structopt(short, long)]
    display: bool,
}

impl From<models::BitstampBook> for orderbook::OrderBook {
    fn from(data: models::BitstampBook) -> Self {
        OrderBook {
            exchange: "bitstamp".to_string(),
            asks: data
                .asks
                .into_iter()
                .take(10)
                .map(|ask| Data {
                    price: ask.price,
                    amount: ask.qty,
                })
                .collect::<Vec<_>>(),
            bids: data
                .bids
                .into_iter()
                .take(10)
                .map(|bid| Data {
                    price: bid.price,
                    amount: bid.qty,
                })
                .collect::<Vec<_>>(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt().init();
    let opt = Opt::from_args();

    info!("connecting to internal grpc server {:?} ...", opt.endpoint);
    let channel = Channel::builder(Uri::from_str(opt.endpoint.as_str())?)
        .keep_alive_while_idle(true)
        .connect()
        .await?;
    let mut client = OrderBookAggregatorClient::new(channel);
    // open a client streaming grpc
    let (grpc_tx, grpc_rx) = tokio::sync::mpsc::unbounded_channel();
    let req = Request::new(UnboundedReceiverStream::new(grpc_rx));
    // grpc client streaming
    client.push_orderbook(req).await?;
    info!("connected ...");

    let bitsmap_url = format!("{}", BITSMAP_WS_API,);
    let (socket, response) = connect_async(Url::parse(&bitsmap_url)?).await?;
    info!("Connected to bitsmap stream.");
    info!("HTTP status code: {}", response.status());
    info!(headers=?response.headers());

    // The type of `json` is `serde_json::Value`
    let subscribe = serde_json::json!({
        "event": "bts:subscribe",
        "data": {
            "channel": "order_book_".to_string() + opt.symbol.to_lowercase().as_str(),
        }
    });
    let (mut write, mut read) = socket.split();
    // subscribe
    write.send(serde_json::to_vec(&subscribe)?.into()).await?;

    // So read and write will be in different tokio tasks
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = tokio::spawn(async move {
        while let Some(inner) = rx.recv().await {
            write.send(inner).await?;
        }
        Result::<_, anyhow::Error>::Ok(())
    });

    while let Some(msg) = read.next().await {
        match msg? {
            Message::Text(str) => {
                let parsed: models::BitsMap = serde_json::from_str(&str)?;
                match parsed.data {
                    models::BitstampData::Book(mut inner) => {
                        
                        inner.bids.truncate(10);
                        inner.asks.truncate(10);
                        if opt.display {
                            info!(?inner.bids);
                            info!(?inner.asks);
                        }
                        grpc_tx.send(inner.into())?;
                    }
                    _ => {
                        warn!(uknown_received=?str);
                        continue;
                    }
                };
            }
            Message::Ping(p) => {
                info!("Ping message received! {:?}", String::from_utf8_lossy(&p));
                let pong = tungstenite::Message::Pong(vec![]);
                tx.send(pong)?;
            }
            Message::Pong(p) => info!("Pong received: {:?}", p),

            Message::Close(c) => {
                info!("Close received from bitsmap: {:?}", c);
                break;
            }
            unexpected_msg => {
                warn!(?unexpected_msg);
            }
        }
    }

    drop(read);
    handle.await??;

    Ok(())
}
