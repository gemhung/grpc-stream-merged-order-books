use crate::ohlc_stream::ohlc_stream_client::OhlcStreamClient;
use crate::ohlc_stream::Code;
use tonic::transport::Channel;
use tonic::Request;
use tracing::info;

pub async fn subscribe(
    mut client: OhlcStreamClient<Channel>,
    code: String,
) -> Result<(), anyhow::Error> {
    info!("subscribe with {}", code);
    let response = client.subscribe(Request::new(Code { code })).await?;
    let mut res = response.into_inner();
    while let Some(ohlc) = res.message().await? {
        info!(?ohlc);
    }
    Result::<(), anyhow::Error>::Ok(())
}

