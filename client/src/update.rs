use crate::orderbook::order_book_aggregator_client::OrderBookAggregatorClient;
use crate::orderbook::OrderBook;
use tokio::time;
use tokio::time::Duration;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Channel;
use tonic::Request;
use tracing::info;

pub async fn update(mut client: OrderBookAggregatorClient<Channel>) -> Result<(), anyhow::Error> {
    info!("start updating");
    let mut interval = time::interval(Duration::from_millis(1000));

    let mut s = OrderBook::default();

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    let req = Request::new(UnboundedReceiverStream::new(rx));
    let _res = client.stream_binance(req).await?;

    loop {
        interval.tick().await;

        s.bids[0].price += 1.0;
        //let req = Request::new(s.clone());
        tx.send(s.clone())?;
    }
}
