use crate::sources::common::*;
use crate::sources::split_quote;
use crate::types::Config;
use anyhow::Result;

/// Spot and USD-M perps share names and columns; the stems differ (config).
const FILES: Files = Files {
    monthly: "{sym}-aggTrades-{y:04}-{m:02}.zip",
    daily: "{sym}-aggTrades-{y:04}-{m:02}-{d:02}.zip",
    tz_h: 0,
};

pub fn venue(config: &Config) -> Result<Option<Venue>> {
    let (quote, perp) = split_quote(&config.quote);
    // Uppercase: the archive paths are uppercase-only (USDEUSDT, not USDeUSDT).
    let sym = format!("{}{}", config.base.to_uppercase(), quote.to_uppercase());
    Venue::new("binance", perp, sym, FILES, row).map(Some)
}

/// aggTrades: id, price, qty, first_id, last_id, time (ms; us on newer spot
/// files), is_buyer_maker. Old files have no header row, recent ones do.
fn row(r: &csv::StringRecord) -> Option<Row> {
    Some(Row {
        us: ms_to_us(field(r, 5)?)?,
        price: field(r, 1)?,
        size: field(r, 2)?,
        // is_buyer_maker=true -> the taker sold
        buyer: !r.get(6)?.eq_ignore_ascii_case("true"),
    })
}
