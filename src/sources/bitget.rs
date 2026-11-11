use crate::sources::common::*;
use crate::sources::split_quote;
use crate::types::Config;
use anyhow::Result;

/// Daily-only, in numbered chunks per day, cut at UTC+8. Spot and perp share names and columns.
const FILES: Files = Files {
    monthly: "",
    daily: "{ds}_{seq:03}.zip",
    tz_h: 8,
};

pub fn venue(config: &Config) -> Result<Option<Venue>> {
    let (quote, perp) = split_quote(&config.quote);
    let (base, quote) = (config.base.to_uppercase(), quote.to_uppercase());
    // Perp product (the `{product}` stem segment): UMCBL = USDT-margined, CMCBL =
    // USDC-margined with symbol `<BASE>PERP`.
    let (product, sym) = match (perp, quote.as_str()) {
        (false, q) => ("", format!("{base}{q}")),
        (true, "USDT") => ("UMCBL", format!("{base}USDT")),
        (true, "USDC") => ("CMCBL", format!("{base}PERP")),
        (true, _) => return Ok(None),
    };
    Venue::new("bitget", perp, sym, FILES, row).map(|v| Some(v.with_product(product)))
}

/// trade_id, timestamp (ms), price, side, volume(quote), size(base).
fn row(r: &csv::StringRecord) -> Option<Row> {
    Some(Row {
        us: ms_to_us(field(r, 1)?),
        price: field(r, 2)?,
        size: field(r, 5)?,
        buyer: r.get(3)?.eq_ignore_ascii_case("buy"),
    })
}
