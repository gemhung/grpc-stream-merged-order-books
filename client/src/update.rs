use crate::orderbook::order_book_aggregator_client::OrderBookAggregatorClient;
use crate::orderbook::{Data, Level, OrderBook, Summary};
use tokio::time;
use tokio::time::Duration;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Channel;
use tonic::Request;
use tracing::info;

pub async fn update(mut client: OrderBookAggregatorClient<Channel>) -> Result<(), anyhow::Error> {
    info!("start updating");
    let mut interval = time::interval(Duration::from_millis(1000));

    let mut s = OrderBook {
        exchange: "binance".to_string(),
        bids: vec![
            Data {
                //exchange: "binance".to_string(),
                price: 1.24,
                amount: 1.25,
            },
            Data {
                //exchange: "bitstamp".to_string(),
                price: 1.24,
                amount: 1.25,
            },
        ],
        ..Default::default()
    };

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    let req = Request::new(UnboundedReceiverStream::new(rx));
    let _res = client.push_orderbook(req).await?;

    loop {
        interval.tick().await;

        s.bids[0].price += 1.0;
        //let req = Request::new(s.clone());
        tx.send(s.clone())?;
    }
}

