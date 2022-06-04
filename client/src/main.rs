pub mod orderbook {
    tonic::include_proto!("orderbook");
}

mod subscribe;
mod update;

use crate::orderbook::order_book_aggregator_client::OrderBookAggregatorClient;
use std::str::FromStr;
use structopt::StructOpt;
use subscribe::subscribe;
use tonic::transport::Channel;
use tonic::transport::Uri;
use tracing::info;
use update::update;

#[derive(StructOpt, Debug)]
#[structopt(name = "ohlc client", about = "An example of ohlc client usage")]
struct Opt {
    /// cargo run -- --addr "http://[::1]:10000"
    #[structopt(short, long, default_value = "http://[::1]:10000")]
    endpoint: String,

    /// cargo run -- --subscribe "8743.T"
    #[structopt(short, long, conflicts_with = "update")]
    subscribe: bool,

    /// cargo run -- --update "8743.T"
    #[structopt(short, long, conflicts_with = "subscribe")]
    update: bool,

    /// decimal point
    #[structopt(short, long, default_value = "6")]
    point: usize,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt().init();
    let opt = Opt::from_args();

    info!("connecting to {:?} ...", opt.endpoint);
    let channel = Channel::builder(Uri::from_str(opt.endpoint.as_str())?)
        .keep_alive_while_idle(true)
        .connect()
        .await?;

    info!("connected ...");
    let client = OrderBookAggregatorClient::new(channel);
    match opt {
        // subscribe to get ohlc
        Opt {
            subscribe: true, ..
        } => subscribe(client, opt.point).await?,

        // update ohlc using file data
        Opt { update: true, .. } => update(client).await?,

        // help
        _ => Opt::clap().print_help()?,
    };

    Ok(())
}
