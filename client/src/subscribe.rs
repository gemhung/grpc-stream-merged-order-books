use prettytable::{color, format::Alignment, Attr, Cell, Row, Table};

use crate::orderbook::order_book_aggregator_client::OrderBookAggregatorClient;
use crate::orderbook::Empty;
use tonic::transport::Channel;
use tonic::Request;
use tracing::info;

fn cls() {
    print!("{esc}[2J{esc}[1;1H", esc = 27 as char);
}

pub async fn subscribe(
    mut client: OrderBookAggregatorClient<Channel>,
) -> Result<(), anyhow::Error> {
    info!("subscribe");

    let response = client
        .subscribe_summary(Request::new(Empty::default()))
        .await?;
    let mut res = response.into_inner();
    while let Some(summary) = res.message().await? {
        let mut table = Table::new();
        table.set_titles(Row::new(vec![
            Cell::new_align("exchange", Alignment::CENTER),
            Cell::new_align("bids", Alignment::CENTER).with_hspan(3),
            Cell::new_align("asks", Alignment::CENTER).with_hspan(3),
            Cell::new_align("exchange", Alignment::CENTER),
        ]));

        table.add_row(Row::new(vec![
            Cell::new_align("", Alignment::LEFT).with_hspan(1),
            Cell::new_align("total", Alignment::CENTER),
            Cell::new_align("amount", Alignment::CENTER),
            Cell::new_align("price", Alignment::CENTER),
            Cell::new_align("price", Alignment::CENTER),
            Cell::new_align("amount", Alignment::CENTER),
            Cell::new_align("total", Alignment::CENTER),
            Cell::new_align("", Alignment::CENTER).with_hspan(1),
        ]));

        let mut bids_iter = summary.bids.iter();
        let mut asks_iter = summary.asks.iter().rev();

        // add asks columns
        while let Some(inner) = asks_iter.next() {
            table.add_row(Row::new(vec![
                Cell::new_align("", Alignment::LEFT).with_hspan(4),
                Cell::new_align(&inner.price.to_string(), Alignment::LEFT)
                    .with_style(Attr::ForegroundColor(color::RED)),
                Cell::new_align(&inner.amount.to_string(), Alignment::LEFT)
                    .with_style(Attr::ForegroundColor(color::RED)),
                Cell::new_align(
                    &(&inner.amount * &inner.price).to_string(),
                    Alignment::RIGHT,
                )
                .with_style(Attr::ForegroundColor(color::RED)),
                Cell::new_align(&inner.exchange.to_string(), Alignment::RIGHT),
            ]));
        }

        // add spread
        table.add_row(Row::new(vec![Cell::new_align(
            (summary.spread.unwrap_or_default().to_string() + "(spread)").as_str(),
            Alignment::CENTER,
        )
        .with_hspan(8)]));

        // add bids columns
        while let Some(inner) = bids_iter.next() {
            table.add_row(Row::new(vec![
                Cell::new_align(&inner.exchange.to_string(), Alignment::LEFT)
                    .with_style(Attr::BackgroundColor(color::GREEN))
                    .with_style(Attr::ForegroundColor(color::BLACK)),
                Cell::new_align(
                    &(&inner.amount * &inner.price).to_string(),
                    Alignment::RIGHT,
                )
                .with_style(Attr::ForegroundColor(color::GREEN)),
                Cell::new_align(&inner.amount.to_string(), Alignment::LEFT)
                    .with_style(Attr::ForegroundColor(color::GREEN)),
                Cell::new_align(&inner.price.to_string(), Alignment::LEFT)
                    .with_style(Attr::ForegroundColor(color::GREEN)),
                Cell::new_align("", Alignment::LEFT).with_hspan(4),
            ]));
        }

        cls();
        table.printstd();
    }

    info!("end");
    Result::<(), anyhow::Error>::Ok(())
}
