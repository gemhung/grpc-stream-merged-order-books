mod self_update;

pub mod ohlc_stream {
    tonic::include_proto!("ohlc_stream");
}

use ohlc_stream::ohlc_stream_server::{OhlcStream, OhlcStreamServer};
use ohlc_stream::{Code, NotImplemented, Ohlc};
use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::info;
use tracing::warn;
//use tonic::transport::ServerTlsConfig;
//use tonic::transport::Identity;

#[derive(Clone)]
pub struct Context {
    map: Arc<Mutex<HashMap<String, broadcast::Sender<Ohlc>>>>,
}

impl Hash for Code {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.code.hash(state);
    }
}

#[derive(StructOpt, Debug)]
#[structopt(name = "ohlc stream client", about = "An example of ohlc client usage")]
struct Opt {
    ///"cargo run --addr <IPv4|IPv6>, Ex: cargo run -- --addr "[::1]:10000"
    #[structopt(long, default_value = "[::1]:10000")]
    addr: String,
}

#[tonic::async_trait]
impl OhlcStream for Context {
    async fn update_ohlc(
        &self,
        _request: Request<ohlc_stream::Ohlc>,
    ) -> Result<Response<NotImplemented>, Status> {
        /*
        let ohlc = request.into_inner();
        let mut map = self.map.lock().await;
        if let Some(broadcast) = map.get(&ohlc.code) {
            if let Err(err) = broadcast.send(ohlc) {
                // all users are out, this message can never send to any
                error!(?err);
                // remove such code to save memory
                map.remove(&err.0.code);
            }
        }
        */
        warn!("Not implemented");
        Ok(Response::new(NotImplemented::default()))
    }

    type SubscribeStream = UnboundedReceiverStream<Result<Ohlc, Status>>;

    async fn subscribe(
        &self,
        request: Request<Code>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let remote_addr = request.remote_addr();
        let data = request.into_inner();
        info!("{:?} subscribe {}", remote_addr, data.code);

        let mut map = self.map.lock().await;
        let (mut rx_broadcast, active_users_num) = match map.get(&data.code) {
            Some(tx) => (tx.subscribe(), tx.receiver_count()),
            None => {
                let (tx, rx) = broadcast::channel(1000000);
                map.insert(data.code.clone(), tx);
                (rx, 1)
            }
        };

        info!(
            "code({:?}) has {} active users",
            &data.code, active_users_num
        );

        drop(map);

        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                if let Err(err) = rx_broadcast
                    .recv()
                    .await
                    .map_err(anyhow::Error::new)
                    .and_then(|ohlc| tx.send(Ok(ohlc)).map_err(anyhow::Error::new))
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

    //let cert = tokio::fs::read("../tls/server.pem").await?;
    //let key = tokio::fs::read("../tls/server.key").await?;
    //let identity = Identity::from_pem(cert, key);

    let opt = Opt::from_args();
    let addr = opt.addr.parse()?;

    let context = Context {
        map: Arc::new(Mutex::new(HashMap::default())),
    };

    let update_handle = self_update::self_update(context.clone()); // cheap clone

    let svc = OhlcStreamServer::new(context);
    let five_seconds = std::time::Duration::new(5, 0);

    info!("start with addr = {:?} ...", addr);

    let server = Server::builder()
        //.tls_config(ServerTlsConfig::new().identity(identity))?
        .tcp_keepalive(Some(five_seconds.clone()))
        .http2_keepalive_interval(Some(five_seconds))
        .add_service(svc)
        .serve(addr);

    tokio::select! {
        inner = update_handle => inner?,
        ret = server => ret.map_err(Into::into),
        else => return Err(anyhow::anyhow!("all branches in tokio select are disable but didn't catch anything")),
    }?;

    Ok(())
}

