use crate::sources::common::*;
use crate::sources::split_quote;
use crate::types::Config;
use anyhow::Result;

const SPOT: Files = Files {
    monthly: "{sym}-{y:04}-{m:02}.csv.gz",
    daily: "{sym}_{y:04}-{m:02}-{d:02}.csv.gz",
    tz_h: 0,
};
/// Linear perps are daily-only.
const PERP: Files = Files {
    monthly: "",
    daily: "{sym}{y:04}-{m:02}-{d:02}.csv.gz",
    tz_h: 0,
};

pub fn venue(config: &Config) -> Result<Option<Venue>> {
    let (quote, perp) = split_quote(&config.quote);
    let base = config.base.to_uppercase();
    // USDC-margined perps are `<BASE>PERP`.
    let sym = if perp && quote.eq_ignore_ascii_case("USDC") {
        format!("{base}PERP")
    } else {
        format!("{base}{}", quote.to_uppercase())
    };
    let (files, row): (_, RowFn) = if perp {
        (PERP, perp_row)
    } else {
        (SPOT, spot_row)
    };
    Venue::new("bybit", perp, sym, files, row).map(Some)
}

/// Spot: id, timestamp (ms), price, volume (base), side, [rpi].
fn spot_row(r: &csv::StringRecord) -> Option<Row> {
    Some(Row {
        us: ms_to_us(field(r, 1)?),
        price: field(r, 2)?,
        size: field(r, 3)?,
        buyer: r.get(4)?.eq_ignore_ascii_case("buy"),
    })
}

/// Linear perp: timestamp (epoch seconds, fractional), symbol, side, size (base),
/// price, ... Written newest-first; the shared fetch restores time order.
fn perp_row(r: &csv::StringRecord) -> Option<Row> {
    Some(Row {
        us: secs_to_us(field(r, 0)?)?,
        price: field(r, 4)?,
        size: field(r, 3)?,
        buyer: r.get(2)?.eq_ignore_ascii_case("buy"),
    })
}
