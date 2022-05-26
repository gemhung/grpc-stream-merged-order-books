pub mod orderbook {
    tonic::include_proto!("orderbook");
}

use orderbook::order_book_aggregator_server::{OrderBookAggregator, OrderBookAggregatorServer};
use orderbook::{Empty, Summary};
use structopt::StructOpt;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::*;

#[derive(Clone)]
pub struct Context {
    //orderbook: Arc<Mutex<HashMap<String, Summary>>,
    broadcast: broadcast::Sender<Summary>,
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
        /*
    async fn push_summary(&self, request: Request<Summary>) -> Result<Response<Empty>, Status> {
        let summary = request.into_inner();

        if let Err(err) = self.broadcast.send(summary) {
            error!(?err);
        }

        Ok(Response::new(Empty::default()))
    }
    */

    async fn push_summary(&self, request: Request<tonic::Streaming<Summary>>) -> Result<Response<Empty>, Status> {

        let broadcast = self.broadcast.clone();
        /*
            do the merge here
        */
        tokio::spawn(async move {
            let mut stream = request.into_inner();
            while let Some(summary) = stream.message().await? {
                if let Err(err) = broadcast.send(summary) {
                    error!(?err);
                    break;
                }
            }

            Result::<_, anyhow::Error>::Ok(())
        });

        Ok(Response::new(Empty::default()))
    }

    type BookSummaryStream = UnboundedReceiverStream<Result<Summary, Status>>;

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
                if let Err(err) = rx_broadcast
                    .recv()
                    .await
                    .map_err(anyhow::Error::new)
                    .and_then(|summary| tx.send(Ok(summary)).map_err(anyhow::Error::new))
                {
                    warn!(?err);
                    break;
                }
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

    let (tx, mut rx): (broadcast::Sender<Summary>, broadcast::Receiver<Summary>) =
        broadcast::channel(100000);
    let context = Context { broadcast: tx };

    let handle = tokio::spawn(async move {
        while let Ok(_) = rx.recv().await.map_err(anyhow::Error::new) {
            debug!("redundant loop don't care");
        }

        info!("exit because sender is closed");
        Result::<_, anyhow::Error>::Ok(())
    });

    let svc = OrderBookAggregatorServer::new(context);
    let five_seconds = std::time::Duration::new(5, 0);

    info!("running with addr = {:?} ...", addr);

    let server = Server::builder()
        .tcp_keepalive(Some(five_seconds.clone()))
        .http2_keepalive_interval(Some(five_seconds))
        .add_service(svc)
        .serve(addr);

    tokio::select! {
        inner = handle => inner?,
        ret = server => ret.map_err(Into::into),
        else => return Err(anyhow::anyhow!("all branches in tokio select are disable but didn't catch anything")),
    }?;

    Ok(())
}
