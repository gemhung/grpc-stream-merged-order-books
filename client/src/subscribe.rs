use crate::orderbook::order_book_aggregator_client::OrderBookAggregatorClient;
use crate::orderbook::Empty;
use tonic::transport::Channel;
use tonic::Request;
use tracing::info;

pub async fn subscribe(
    mut client: OrderBookAggregatorClient<Channel>,
) -> Result<(), anyhow::Error> {
    info!("subscribe");

    let response = client.book_summary(Request::new(Empty::default())).await?;
    let mut res = response.into_inner();
    while let Some(summary) = res.message().await? {
        info!(?summary.spread);
        info!(?summary.bids);
        info!(?summary.asks);
    }

    Result::<(), anyhow::Error>::Ok(())
}
