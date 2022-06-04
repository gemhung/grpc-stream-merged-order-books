pub mod orderbook {
    tonic::include_proto!("orderbook");
}
mod models;

use crate::orderbook::order_book_aggregator_client::OrderBookAggregatorClient;
use crate::orderbook::OrderBook;
use futures_util::stream::StreamExt;
use futures_util::SinkExt;
use prettytable::{color, format::Alignment, Attr, Cell, Row, Table};
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
#[structopt(name = "bitstamp client", about = "Receive market data")]
struct Opt {
    #[structopt(short, long, default_value = "btcusdt")]
    symbol: String,

    #[structopt(short, long, default_value = "http://[::1]:10000")]
    endpoint: String,

    #[structopt(short, long)]
    display: bool,

    #[structopt(short, long, default_value = "6")]
    point: usize,
}

// convert to grpc acceptable data format
impl From<models::BitstampBook> for orderbook::OrderBook {
    fn from(data: models::BitstampBook) -> Self {
        OrderBook {
            asks: data
                .asks
                .into_iter()
                .take(10)
                .map(|ask| orderbook::Level {
                    exchange: "bitstamp".to_string(),
                    price: ask.price,
                    amount: ask.qty,
                })
                .collect::<Vec<_>>(),
            bids: data
                .bids
                .into_iter()
                .take(10)
                .map(|bid| orderbook::Level {
                    exchange: "bitstamp".to_string(),
                    price: bid.price,
                    amount: bid.qty,
                })
                .collect::<Vec<_>>(),
        }
    }
}

// clear screen
fn cls() {
    print!("{esc}[2J{esc}[1;1H", esc = 27 as char);
}

fn display(book: &OrderBook, point: usize, symbol: &str) {
    let fmt = |f: &f64| format!("{0:>.1$}", f, point);
    let mut table = Table::new();
    table.set_titles(Row::new(vec![Cell::new_align(
        &("bitstamp".to_string() + "(" + symbol + ")"),
        Alignment::CENTER,
    )
    .with_hspan(6)]));
    table.add_row(Row::new(vec![
        Cell::new_align("bids", Alignment::CENTER).with_hspan(3),
        Cell::new_align("asks", Alignment::CENTER).with_hspan(3),
    ]));

    table.add_row(Row::new(vec![
        Cell::new_align("total", Alignment::CENTER),
        Cell::new_align("amount", Alignment::CENTER),
        Cell::new_align("price", Alignment::CENTER),
        Cell::new_align("price", Alignment::CENTER),
        Cell::new_align("amount", Alignment::CENTER),
        Cell::new_align("total", Alignment::CENTER),
    ]));

    let mut bids_iter = book.bids.iter().take(10);
    let mut asks_iter = book.asks.iter().take(10).rev();

    // add asks columns
    while let Some(inner) = asks_iter.next() {
        table.add_row(Row::new(vec![
            Cell::new_align("", Alignment::LEFT).with_hspan(3),
            Cell::new_align(&fmt(&inner.price), Alignment::RIGHT)
                .with_style(Attr::ForegroundColor(color::RED)),
            Cell::new_align(&fmt(&inner.amount), Alignment::RIGHT)
                .with_style(Attr::ForegroundColor(color::RED)),
            Cell::new_align(&fmt(&(&inner.amount * &inner.price)), Alignment::RIGHT)
                .with_style(Attr::ForegroundColor(color::RED)),
        ]));
    }

    // add spread
    table.add_row(Row::new(vec![Cell::new_align(
        (book
            .bids
            .first()
            .zip(book.asks.first())
            .map(|(b, a)| fmt(&(b.price - a.price).abs()))
            .unwrap_or_else(|| "None".to_string())
            + "(spread)")
            .as_str(),
        Alignment::CENTER,
    )
    .with_hspan(6)]));

    // add bids columns
    while let Some(inner) = bids_iter.next() {
        table.add_row(Row::new(vec![
            Cell::new_align(&fmt(&(&inner.amount * &inner.price)), Alignment::RIGHT)
                .with_style(Attr::ForegroundColor(color::GREEN)),
            Cell::new_align(&fmt(&inner.amount), Alignment::RIGHT)
                .with_style(Attr::ForegroundColor(color::GREEN)),
            Cell::new_align(&fmt(&inner.price), Alignment::RIGHT)
                .with_style(Attr::ForegroundColor(color::GREEN)),
            Cell::new_align("", Alignment::LEFT).with_hspan(2),
        ]));
    }

    cls();
    table.printstd();
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
    client.stream_bitstamp(req).await?;
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
    // check if successful subscribe
    match read.next().await {
        Some(Ok(msg)) => info!(?msg),
        Some(err) => err.map(|_| ())?,
        _ => Err(anyhow::anyhow!(
            "unexpected none value, channel maybe closed"
        ))?,
    }

    // So read and write will be in different tokio tasks
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = tokio::spawn(async move {
        while let Some(inner) = rx.recv().await {
            write.send(inner).await?;
        }
        Result::<_, anyhow::Error>::Ok(())
    });

    // core process to receive market data
    while let Some(msg) = read.next().await {
        match msg? {
            Message::Text(str) => {
                let parsed: models::BitsMap = serde_json::from_str(&str)?;
                match parsed.data {
                    models::BitstampData::Book(inner) => {
                        let r: orderbook::OrderBook = inner.into();
                        if opt.display {
                            display(&r, opt.point, opt.symbol.as_str());
                        }
                        grpc_tx.send(r)?;
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
