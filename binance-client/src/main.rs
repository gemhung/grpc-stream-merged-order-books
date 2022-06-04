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

    #[structopt(short, long, default_value = "6")]
    point: usize,
}

impl From<models::DepthStreamData> for orderbook::OrderBook {
    fn from(data: models::DepthStreamData) -> Self {
        OrderBook {
            asks: data
                .asks
                .into_iter()
                .map(|ask| orderbook::Level {
                    exchange: "binance".to_string(),
                    price: ask.price,
                    amount: ask.qty,
                })
                .collect::<Vec<_>>(),
            bids: data
                .bids
                .into_iter()
                .map(|bid| orderbook::Level {
                    exchange: "binance".to_string(),
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

fn display(book: &OrderBook, decimal_point: usize, symbol: &str) {
    let fmt = |f: &f64| format!("{:>.1$}", f, decimal_point);

    let mut table = Table::new();
    table.set_titles(Row::new(vec![Cell::new_align(
        &("binance".to_string() + "(" + symbol + ")"),
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
    client.stream_binance(req).await?;
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
                let orderbook: OrderBook = parsed.into();
                if opt.display {
                    display(&orderbook, opt.point, opt.symbol.as_str())
                }
                // forward to grpc
                grpc_tx.send(orderbook)?;
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
