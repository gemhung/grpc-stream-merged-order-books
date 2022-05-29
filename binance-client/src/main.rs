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

static BINANCE_WS_API: &str = "wss://stream.binance.com:9443";

#[derive(Debug, StructOpt)]
#[structopt(name = "binance client", about = "An example of StructOpt usage.")]
struct Opt {
    #[structopt(short, long, default_value = "btcusdt")]
    symbol: String,

    /// 5, 10, 20
    #[structopt(short, long, default_value = "10")]
    level: usize,

    #[structopt(short, long, default_value = "http://[::1]:10000")]
    endpoint: String,

    #[structopt(short, long)]
    display: bool,
}

impl From<models::DepthStreamData> for orderbook::OrderBook {
    fn from(data: models::DepthStreamData) -> Self {
        OrderBook {
            exchange: "binance".to_string(),
            asks: data
                .asks
                .into_iter()
                .map(|ask| Data {
                    price: ask.price,
                    amount: ask.qty,
                })
                .collect::<Vec<_>>(),
            bids: data
                .bids
                .into_iter()
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

    // checking level aruments
    anyhow::ensure!(
        matches!(opt.level, 5 | 10 | 20),
        "argument level hast to be either 5, 10, 20"
    );

    let binance_url = format!(
        "{}/ws/{}@depth{}@100ms",
        BINANCE_WS_API,
        opt.symbol.to_lowercase(),
        opt.level,
    );

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

    // connect to exchange
    info!("connecting to binance exchange ...");
    let (socket, response) = connect_async(Url::parse(&binance_url)?).await?;
    info!("connected");
    // print meta data from exchange
    info!("http status code: {}", response.status());
    info!("response headers:");
    for (ref header, ref header_value) in response.headers() {
        info!("- {}: {:?}", header, header_value);
    }
    // so that read and write will be in different tokio tasks
    // for now, it's only used to write pong message back to exchange
    let (mut write, mut read) = socket.split();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let write_handle = tokio::spawn(async move {
        while let Some(inner) = rx.recv().await {
            write.send(inner).await?;
        }
        Result::<_, anyhow::Error>::Ok(())
    });

    // start receiving data from exchange
    while let Some(msg) = read.next().await {
        match msg? {
            Message::Text(str) => {
                let parsed: models::DepthStreamData = serde_json::from_str(&str)?;
                if opt.display {
                    info!(?parsed.bids);
                    info!(?parsed.asks);
                }
                // forward to grpc
                grpc_tx.send(parsed.into())?;
            }
            Message::Ping(p) => {
                info!("Ping message received! {:?}", String::from_utf8_lossy(&p));
                let pong = tungstenite::Message::Pong(vec![]);
                tx.send(pong)?;
            }
            Message::Pong(p) => info!("Pong received: {:?}", p),

            Message::Close(c) => {
                info!("Close frame received from binance : {:?}", c);
                break;
            }
            unexpected_msg => {
                info!(?unexpected_msg);
            }
        }
    }

    drop(read);
    write_handle.await??;

    Ok(())
}
