pub mod orderbook {
    tonic::include_proto!("orderbook");
}

mod merge;
mod run;

use orderbook::order_book_aggregator_server::{OrderBookAggregator, OrderBookAggregatorServer};
use orderbook::{Empty, OrderBook, Summary};
use run::run_stream;
use run::Binance;
use run::Bitstamp;
use structopt::StructOpt;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::*;

#[derive(StructOpt, Debug)]
#[structopt(name = "ohlc stream client", about = "An example of ohlc client usage")]
struct Opt {
    ///"cargo run --addr <IPv4|IPv6>, Ex: cargo run -- --addr "[::1]:10000"
    #[structopt(long, default_value = "[::1]:10000")]
    addr: String,
}

pub struct Context {
    broadcast: broadcast::Sender<Summary>,
    inner_process: mpsc::UnboundedSender<run::Command>,
}

// grpc api
#[tonic::async_trait]
impl OrderBookAggregator for Context {
    async fn stream_binance(
        &self,
        request: Request<tonic::Streaming<OrderBook>>,
    ) -> Result<Response<Empty>, Status> {
        let remote_addr = request.remote_addr();
        let broadcast = self.broadcast.clone(); // we use this to broadcast the merge summary later
        let inner_sender = self.inner_process.clone();
        tokio::spawn(async move {
            if let Err(err) = run_stream::<Binance>(request, broadcast, inner_sender).await {
                error!(?remote_addr, ?err);
            }
        });

        Ok(Response::new(Empty::default()))
    }

    async fn stream_bitstamp(
        &self,
        request: Request<tonic::Streaming<OrderBook>>,
    ) -> Result<Response<Empty>, Status> {
        let remote_addr = request.remote_addr();
        let broadcast = self.broadcast.clone(); // we use this to broadcast the merge summary later
        let inner_sender = self.inner_process.clone();
        tokio::spawn(async move {
            if let Err(err) = run_stream::<Bitstamp>(request, broadcast, inner_sender).await {
                error!(?remote_addr, ?err);
            }
        });

        Ok(Response::new(Empty::default()))
    }

    // it's where client booking the merged orderbooks and get a stream for top 10 bids/asks and spread
    type SubscribeSummaryStream = UnboundedReceiverStream<Result<Summary, Status>>;
    async fn subscribe_summary(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::SubscribeSummaryStream>, Status> {
        let remote_addr = request.remote_addr();
        info!("booking summary from {:?}", remote_addr);

        let (tx, rx) = mpsc::unbounded_channel();
        let rx_broadcast = self.broadcast.subscribe();
        tokio::spawn(async move {
            if let Err(err) = run::run_subscribe(tx, rx_broadcast).await {
                error!(?remote_addr, ?err);
            }
        });

        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt().init();

    let opt = Opt::from_args();
    let addr = opt.addr.parse()?;

    let (broadcast_tx, rx) = broadcast::channel(10000);
    // Important! We must drop rx so that it won't pending the receiving queue of broadcast
    drop(rx);

    //  spawn a new task to handle update for each exchange and merge request
    let (tx, rx) = mpsc::unbounded_channel();
    let handle = tokio::spawn(run::run_inner(rx));

    // shared context for grpc api
    let context = Context {
        broadcast: broadcast_tx,
        inner_process: tx,
    };
    // build an grpc-server
    let service = OrderBookAggregatorServer::new(context);
    let keepalive = std::time::Duration::new(3600, 0);
    let server = Server::builder()
        .tcp_keepalive(Some(keepalive.clone()))
        .http2_keepalive_interval(Some(keepalive))
        .add_service(service)
        .serve(addr);
    info!("running server with addr = {:?} ...", addr);

    // run and await
    tokio::select! {
        inner = handle => inner?,
        ret = server => ret.map_err(anyhow::Error::new),
        else => return Err(anyhow::anyhow!("all branches in tokio select are disable but didn't catch anything")),
    }?;

    Ok(())
}

/*


#![allow(unused)]
use fixedbitset::FixedBitSet;
use std::collections::HashMap;

fn foo(v: &[FixedBitSet], dp: &mut[HashMap<FixedBitSet, Option<FixedBitSet>>], i: usize, cur: FixedBitSet) -> Option<FixedBitSet> {
    // able to compose
    if cur.count_ones(..) == 0 {
        return Some(cur);
    }

    // exhausted
    if i == v.len() {
        return None;
    }

    // check if already calculated
    if dp[i].contains_key(&cur){
        return dp[i][&cur].clone();
    }
    // pick
    let r1 = v[i]
        .is_subset(&cur)
        .then(|| {
            foo(v, dp, i + 1, &cur ^ &v[i]).map(|mut inner| {
                inner.insert(i);
                inner
            })
        })
        .flatten(); // Some(Some(_)) => Some(_) or Some(Nnne) => None

    // not pick
    let r2 = foo(v, dp, i + 1, cur.clone());

    // compare to find minimum one
    let ret = match (&r1, &r2) {
        (Some(ref f1), Some(ref f2)) => {
            if f1.count_ones(..) < f2.count_ones(..) {
                r1
            } else {
                r2
            }
        }
        _ => r1.or(r2), // return option which has a value
    };

    dp[i].insert(cur, ret.clone());
    ret
}

fn main() {

    let t = std::time::SystemTime::now();
    let v = vec![
        FixedBitSet::with_capacity_and_blocks(11, vec![1]),
        FixedBitSet::with_capacity_and_blocks(11, vec![2]),
        FixedBitSet::with_capacity_and_blocks(11, vec![4]),
        FixedBitSet::with_capacity_and_blocks(11, vec![8]),
        FixedBitSet::with_capacity_and_blocks(11, vec![16]),
        FixedBitSet::with_capacity_and_blocks(11, vec![32]),
        FixedBitSet::with_capacity_and_blocks(11, vec![371]),
        FixedBitSet::with_capacity_and_blocks(11, vec![64]),
        FixedBitSet::with_capacity_and_blocks(11, vec![128]),
        FixedBitSet::with_capacity_and_blocks(11, vec![256]),
        FixedBitSet::with_capacity_and_blocks(11, vec![512]),
        FixedBitSet::with_capacity_and_blocks(11, vec![1024]),
    ];

    let mut dp = vec![HashMap::new(); v.len()];

    let bs = FixedBitSet::with_capacity_and_blocks(v.len(), vec![371]);

    let r = foo(&v, dp.as_mut_slice(), 0, bs);
    println!("{:?}", t.elapsed());

    println!("{}", r.unwrap());
}

*/
