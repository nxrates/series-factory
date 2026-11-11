use crate::sources::common::*;
use crate::sources::split_quote;
use crate::types::Config;
use anyhow::Result;

/// USDT-margined perps (`download.gatedata.org/futures_usdt/trades`): monthly only,
/// and only a completed month is published. No spot archive.
const FILES: Files = Files {
    monthly: "{sym}-{y:04}{m:02}.csv.gz",
    daily: "",
    tz_h: 0,
};

pub fn venue(config: &Config) -> Result<Option<Venue>> {
    let (quote, perp) = split_quote(&config.quote);
    if !(perp && quote.eq_ignore_ascii_case("USDT")) {
        return Ok(None);
    }
    let sym = format!("{}_USDT", config.base.to_uppercase());
    let url = format!("https://api.gateio.ws/api/v4/futures/usdt/contracts/{sym}");
    let name = sym.clone();
    Ok(Some(
        Venue::new("gateio", true, sym, FILES, row)?.with_contract(url, move |j| {
            j["quanto_multiplier"]
                .as_str()
                .and_then(|v| v.parse().ok())
                .ok_or_else(|| anyhow::anyhow!("Gate {name}: no quanto_multiplier in reply"))
        }),
    ))
}

/// timestamp (epoch seconds, fractional), id, price, size: a signed contract
/// count (negative = taker sell); the contract's `quanto_multiplier` scales it to base.
fn row(r: &csv::StringRecord) -> Option<Row> {
    let size: f64 = field(r, 3)?;
    Some(Row {
        us: secs_to_us(field(r, 0)?)?,
        price: field(r, 2)?,
        size: size.abs(),
        buyer: size > 0.0,
    })
}
