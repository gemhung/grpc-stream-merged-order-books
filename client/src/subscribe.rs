use prettytable::{color, format::Alignment, Attr, Cell, Row, Table};

use crate::orderbook::order_book_aggregator_client::OrderBookAggregatorClient;
use crate::orderbook::{Empty, Summary};
use tonic::transport::Channel;
use tonic::Request;
use tracing::info;

fn cls() {
    print!("{esc}[2J{esc}[1;1H", esc = 27 as char);
}

fn exchange_cell(exchange: &str) -> Cell {
    Cell::new_align(exchange, Alignment::CENTER)
        .with_style(match exchange {
            "binance" => Attr::BackgroundColor(color::YELLOW),
            "bitstamp" => Attr::BackgroundColor(color::GREEN),
            _ => Attr::BackgroundColor(color::BLACK),
        })
        .with_style(Attr::ForegroundColor(color::BLACK))
    //.with_style(Attr::ForegroundColor(color::BLACK))
}

fn display(summary: &Summary, point: usize) {
    let fmt = |f: &f64| format!("{0:>.1$}", f, point);
    let mut table = Table::new();
    table.set_titles(Row::new(vec![Cell::new_align(
        "Summary",
        Alignment::CENTER,
    )
    .with_hspan(8)]));
    table.add_row(Row::new(vec![
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
            Cell::new_align(&fmt(&inner.price), Alignment::RIGHT)
                .with_style(Attr::ForegroundColor(color::RED)),
            Cell::new_align(&fmt(&inner.amount), Alignment::RIGHT)
                .with_style(Attr::ForegroundColor(color::RED)),
            Cell::new_align(&fmt(&(&inner.amount * &inner.price)), Alignment::RIGHT)
                .with_style(Attr::ForegroundColor(color::RED)),
            exchange_cell(&inner.exchange),
        ]));
    }

    // add spread
    table.add_row(Row::new(vec![Cell::new_align(
        &(fmt(&summary.spread.unwrap_or_default()) + "(spread)"),
        Alignment::CENTER,
    )
    .with_hspan(8)]));

    // add bids columns
    while let Some(inner) = bids_iter.next() {
        table.add_row(Row::new(vec![
            exchange_cell(&inner.exchange),
            Cell::new_align(&fmt(&(&inner.amount * &inner.price)), Alignment::RIGHT)
                .with_style(Attr::ForegroundColor(color::GREEN)),
            Cell::new_align(&fmt(&inner.amount), Alignment::RIGHT)
                .with_style(Attr::ForegroundColor(color::GREEN)),
            Cell::new_align(&fmt(&inner.price), Alignment::RIGHT)
                .with_style(Attr::ForegroundColor(color::GREEN)),
            Cell::new_align("", Alignment::LEFT).with_hspan(4),
        ]));
    }

    cls();
    table.printstd();
}

pub async fn subscribe(
    mut client: OrderBookAggregatorClient<Channel>,
    point: usize,
) -> Result<(), anyhow::Error> {
    info!("subscribe");

    let response = client
        .subscribe_summary(Request::new(Empty::default()))
        .await?;
    let mut res = response.into_inner();
    while let Some(summary) = res.message().await? {
        display(&summary, point);
    }

    info!("end");
    Result::<(), anyhow::Error>::Ok(())
}
