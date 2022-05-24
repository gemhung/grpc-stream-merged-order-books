pub mod ohlc_stream {
    tonic::include_proto!("ohlc_stream");
}

pub mod subscribe;
pub mod update;

use crate::ohlc_stream::ohlc_stream_client::OhlcStreamClient;
use std::str::FromStr;
use structopt::StructOpt;
use subscribe::subscribe;
use tonic::transport::Certificate;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
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
    subscribe: Option<String>,

    /// cargo run -- --update "8743.T"
    #[structopt(short, long, conflicts_with = "subscribe")]
    update: bool,

    // cargo run --update "8473.T" --file [$PATH]
    #[structopt(long, default_value = "../mkt_data.csv")]
    file: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt().init();
    let opt = Opt::from_args();

    info!("connect to {:?} ...", opt.endpoint);
    //let pem = tokio::fs::read("../tls/ca.pem").await?;
    let pem = tokio::fs::read("../tls/self-signed/server.crt").await?;
    let ca = Certificate::from_pem(pem);

    let tls = ClientTlsConfig::new()
        .ca_certificate(ca)
        //.domain_name("gem-LB-grpc-992484425.ap-northeast-1.elb.amazonaws.com");
        .domain_name("example.com");

    let five_seconds = std::time::Duration::new(5, 0);
    let channel = Channel::builder(Uri::from_str(opt.endpoint.as_str())?)
        .keep_alive_while_idle(true)
        .http2_keep_alive_interval(five_seconds.clone())
        .tcp_keepalive(Some(five_seconds))
        .tls_config(tls)?
        .connect()
        .await?;
    let client = OhlcStreamClient::new(channel);
    info!("connected ...");
    match opt {
        // subscribe to get ohlc
        Opt {
            subscribe: Some(code),
            ..
        } => {
            tokio::spawn(async move { subscribe(client, code).await }).await??;
        }

        // update ohlc using file data
        Opt {
            update: true, file, ..
        } => {
            tokio::spawn({
                async move {
                    let file = std::fs::File::open(file)?;
                    let csv = csv::ReaderBuilder::new()
                        .has_headers(false)
                        .from_reader(file);
                    let records = csv.into_records();
                    update(client, records).await
                }
            })
            .await??;
        }
        _ => {
            Opt::clap().print_help()?;
        }
    };

    Ok(())
}

