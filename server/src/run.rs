use super::merge;
use crate::orderbook::{Level, OrderBook, Summary};
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tonic::Request;
use tonic::Status;
use tracing::*;

pub struct Binance;
pub struct Bitstamp;

#[derive(Debug)]
pub enum Command {
    UpdateBinance(OrderBook),
    UpdateBitstamp(OrderBook),
    Merged(oneshot::Sender<Summary>),
}

pub trait IntoCommand {
    fn into_command(orderbook: OrderBook) -> Command;
}

impl IntoCommand for Binance {
    fn into_command(orderbook: OrderBook) -> Command {
        Command::UpdateBinance(orderbook)
    }
}

impl IntoCommand for Bitstamp {
    fn into_command(orderbook: OrderBook) -> Command {
        Command::UpdateBitstamp(orderbook)
    }
}

pub async fn run_stream<T: IntoCommand>(
    request: Request<tonic::Streaming<OrderBook>>,
    broadcast: broadcast::Sender<Summary>,
    inner_sender: mpsc::UnboundedSender<Command>,
) -> Result<(), anyhow::Error> {
    let mut stream = request.into_inner();
    while let Some(orderbook) = stream.message().await? {
        let (tx, rx) = oneshot::channel();

        let cmd: Command = T::into_command(orderbook);

        // update orderbook
        inner_sender.send(cmd)?;

        // we merged orderbooks only if there are subscribers
        if broadcast.receiver_count() > 0 {
            inner_sender.send(Command::Merged(tx))?;
            let summary = rx.await?;
            if let Err(err) = broadcast.send(summary) {
                // this erro is because we broadcasted merged summary but there is no active users at all
                // receiver_count > 0 didn't 100% gurantee successful sending cause user may drop at this point
                // Hence we choose to eat this error and continue
                warn!(?err);
            }
        }
    }

    Result::<_, anyhow::Error>::Ok(())
}

pub async fn run_inner(mut rx: mpsc::UnboundedReceiver<Command>) -> Result<(), anyhow::Error> {
    let mut binance = OrderBook::default(); // empty orderbook
    let mut bitstamp = OrderBook::default(); // empty orderbook
    let mut merged = None;
    while let Some(cmd) = rx.recv().await {
        match cmd {
            Command::UpdateBinance(book) => {
                binance = book;
                merged = None; // indicate it's out-of-dated
            }
            Command::UpdateBitstamp(book) => {
                bitstamp = book;
                merged = None; // indicate it's out-of-dated
            }
            Command::Merged(tx) => {
                let summary = merged.clone().unwrap_or_else(|| {
                    let (bids1, asks1) = (&binance.bids, &binance.asks);
                    let (bids2, asks2) = (&bitstamp.bids, &bitstamp.asks);
                    let mut bids = merge::merge_greater(&[bids1, bids2]);
                    let mut asks = merge::merge_less(&[asks1, asks2]);
                    bids.truncate(10);
                    asks.truncate(10);
                    // empty orderbook for clean up
                    Summary {
                        spread: bids.first().zip(asks.first()).map(
                            |(Level { price: p1, .. }, Level { price: p2, .. })| (p1 - p2).abs(),
                        ),
                        bids,
                        asks,
                    }
                });
                //

                if let Err(err) = tx.send(summary) {
                    error!(?err);
                }
            }
        }
    }

    Ok(())
}

pub async fn run_subscribe(
    tx: mpsc::UnboundedSender<Result<Summary, Status>>,
    mut rx_broadcast: broadcast::Receiver<Summary>,
) -> Result<(), anyhow::Error> {
    loop {
        match rx_broadcast.recv().await {
            err @ Err(broadcast::error::RecvError::Closed) => {
                error!(?err);
                break Err(anyhow::Error::new(broadcast::error::RecvError::Closed));
            }
            Err(broadcast::error::RecvError::Lagged(frames)) => {
                error!("Lagges");
                warn!(
                    "receiving from broadcast is too slow and skipped {} frames",
                    frames
                );
            }
            Ok(inner) => tx.send(Ok(inner)).map(|_| ()).map_err(anyhow::Error::new)?,
        }
    }
}
