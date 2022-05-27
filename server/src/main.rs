pub mod orderbook {
    tonic::include_proto!("orderbook");
}

use orderbook::order_book_aggregator_server::{OrderBookAggregator, OrderBookAggregatorServer};
use orderbook::{Empty, Level, OrderBook, Summary};
use structopt::StructOpt;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::*;

#[derive(Debug)]
enum Command {
    UpdateBinance(OrderBook),
    UpdateBitstamp(OrderBook),
    Merged(oneshot::Sender<Summary>),
}

//#[derive(Clone)]
pub struct Context {
    broadcast: broadcast::Sender<Summary>,
    inner_process: mpsc::UnboundedSender<Command>,
}

#[derive(StructOpt, Debug)]
#[structopt(name = "ohlc stream client", about = "An example of ohlc client usage")]
struct Opt {
    ///"cargo run --addr <IPv4|IPv6>, Ex: cargo run -- --addr "[::1]:10000"
    #[structopt(long, default_value = "[::1]:10000")]
    addr: String,
}

#[tonic::async_trait]
impl OrderBookAggregator for Context {
    async fn push_orderbook(
        &self,
        request: Request<tonic::Streaming<OrderBook>>,
    ) -> Result<Response<Empty>, Status> {
        let broadcast = self.broadcast.clone();

        let inner_sender = self.inner_process.clone();
        tokio::spawn(async move {
            let mut stream = request.into_inner();
            while let Some(orderbook) = stream.message().await? {
                let (tx, rx) = oneshot::channel();

                let cmd = match orderbook.exchange.as_str() {
                    "binance" => Command::UpdateBinance(orderbook),
                    "bitstamp" => Command::UpdateBitstamp(orderbook),
                    unknown_exchange => {
                        error!(?unknown_exchange);
                        continue;
                    }
                };

                inner_sender.send(cmd).unwrap();
                inner_sender.send(Command::Merged(tx)).unwrap();

                let summary = rx.await?;

                if broadcast.receiver_count() > 0 {
                    if let Err(err) = broadcast.send(summary) {
                        // this erro is because we broadcasted but there is no active usersa at all
                        // receiver_count > 0 didn't 100% gurantee to prevent from getting error, so we didn't break here and thus continue
                        warn!(?err);
                    }
                }
            }

            Result::<_, anyhow::Error>::Ok(())
        });

        Ok(Response::new(Empty::default()))
    }

    type BookSummaryStream = UnboundedReceiverStream<Result<Summary, Status>>;

    // it's where client booking the merged orderbooks and get a stream for top 10 bids/asks and spread
    async fn book_summary(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::BookSummaryStream>, Status> {
        let remote_addr = request.remote_addr().unwrap().to_string();
        info!("booking summary from {}", remote_addr);

        let (tx, rx) = mpsc::unbounded_channel();
        let mut rx_broadcast = self.broadcast.subscribe();
        // this is simply forwarding summary cause we cannot use tokio_stream::wrappers::BroadCastStream
        tokio::spawn(async move {
            loop {
                match rx_broadcast.recv().await {
                    err @ Err(broadcast::error::RecvError::Closed) => {
                        break err.map_err(anyhow::Error::new)
                    }
                    Err(broadcast::error::RecvError::Lagged(frames)) => {
                        warn!(
                            "receiving from broadcast is too slow and skipped {} frames",
                            frames
                        );
                    }
                    Ok(inner) => tx.send(Ok(inner)).map_err(anyhow::Error::new)?,
                }
            }
        });

        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }
}

async fn run_inner(mut rx: mpsc::UnboundedReceiver<Command>) -> Result<(), anyhow::Error> {
    let mut binance = Option::<OrderBook>::None;
    let mut bitstamp = Option::<OrderBook>::None;
    let mut merged = Option::<Summary>::None;
    while let Some(cmd) = rx.recv().await {
        match cmd {
            Command::UpdateBinance(book) => {
                binance = Some(book);
                merged = None; // indicate it's out-of-dated
            }
            Command::UpdateBitstamp(book) => {
                bitstamp = Some(book);
                merged = None; // indicate it's out-of-dated
            }
            Command::Merged(tx) => {
                let summary = merged.clone().unwrap_or_else(|| {
                    //let b

                    Summary {
                        spread: 1.0,
                        bids: vec![Level {
                            exchange: "binance".to_string(),
                            price: match binance {
                                Some(ref b) => b.bids.first().map(|data| data.price).unwrap_or(1.0),
                                None => 1.0,
                            },
                            amount: 2.0,
                        }],
                        asks: vec![Level {
                            exchange: "binance".to_string(),
                            price: 1.0,
                            amount: 2.0,
                        }],
                    }
                });

                if let Err(err) = tx.send(summary) {
                    error!(?err);
                }
            }
        }
    }

    Ok(())
}

fn merge_less(v1: &[Level], v2: &[Level]) -> Vec<Level> {
    let mut iter1 = v1.iter().peekable();
    let mut iter2 = v2.iter().peekable();

    //assert!(v1.len() + v2.len() <= usize::MAX);

    let mut ret = Vec::with_capacity(v1.len() + v2.len());

    loop {
        match (iter1.peek(), iter2.peek()) {
            (Some(Level { price: p1, .. }), Some(Level { price: p2, .. })) if p1 <= p2 => {
                ret.push(iter1.next().unwrap().clone())
            }

            (Some(Level { price: p1, .. }), Some(Level { price: p2, .. })) if p1 > p2 => {
                ret.push(iter2.next().unwrap().clone());
            }
            _ => {
                // one of them has finish iterating all values
                break;
            }
        }
    }

    ret.extend(iter1.cloned());
    ret.extend(iter2.cloned());

    ret
}

fn merge_greater(v1: &[Level], v2: &[Level]) -> Vec<Level> {
    let mut iter1 = v1.iter().peekable();
    let mut iter2 = v2.iter().peekable();

    //assert!(v1.len() + v2.len() <= usize::MAX);

    let mut ret = Vec::with_capacity(v1.len() + v2.len());

    loop {
        match (iter1.peek(), iter2.peek()) {
            (Some(Level { price: p1, .. }), Some(Level { price: p2, .. })) if p1 >= p2 => {
                ret.push(iter1.next().unwrap().clone())
            }

            (Some(Level { price: p1, .. }), Some(Level { price: p2, .. })) if p1 <= p2 => {
                ret.push(iter2.next().unwrap().clone());
            }
            _ => {
                // one of them has finish iterating all values
                break;
            }
        }
    }

    ret.extend(iter1.cloned());
    ret.extend(iter2.cloned());

    ret
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt().init();

    let opt = Opt::from_args();
    let addr = opt.addr.parse()?;

    let (broadcast_tx, rx) = broadcast::channel(100000);
    // drop rx so that it won't pending the receiving queue of broadcast
    drop(rx);

    let (tx, rx) = mpsc::unbounded_channel();

    let handle = tokio::spawn(run_inner(rx));

    let context = Context {
        broadcast: broadcast_tx,
        inner_process: tx,
    };

    info!("running with addr = {:?} ...", addr);
    let svc = OrderBookAggregatorServer::new(context);
    let keepalive = std::time::Duration::new(3600, 0);
    let server = Server::builder()
        .tcp_keepalive(Some(keepalive.clone()))
        .http2_keepalive_interval(Some(keepalive))
        .add_service(svc)
        .serve(addr);

    tokio::select! {
        inner = handle => inner?,
        ret = server => ret.map_err(anyhow::Error::new),
        else => return Err(anyhow::anyhow!("all branches in tokio select are disable but didn't catch anything")),
    }?;

    Ok(())
}
