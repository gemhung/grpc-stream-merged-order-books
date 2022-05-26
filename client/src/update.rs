use crate::orderbook::order_book_aggregator_client::OrderBookAggregatorClient;
use crate::orderbook::{Level, Summary};
use tokio::time;
use tokio::time::Duration;
use tonic::transport::Channel;
use tonic::Request;
use tracing::info;
use tokio_stream::wrappers::UnboundedReceiverStream;

pub async fn update(mut client: OrderBookAggregatorClient<Channel>) -> Result<(), anyhow::Error> {
    info!("start updating");
    let mut interval = time::interval(Duration::from_millis(1000));

    let mut s = Summary {
        spread: 1.23,
        bids: vec![
            Level {
                exchange: "binance".to_string(),
                price: 1.24,
                amount: 1.25,
            },
            Level {
                exchange: "bitstamp".to_string(),
                price: 1.24,
                amount: 1.25,
            },
        ],
        ..Default::default()
    };

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    let req = Request::new(UnboundedReceiverStream::new(rx));
    let _res = client.push_summary(req).await?;

    loop {
        interval.tick().await;

        s.spread += 1.0;
        //let req = Request::new(s.clone());
        tx.send(s.clone())?;
    }
}
