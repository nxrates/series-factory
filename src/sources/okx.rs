use crate::sources::common::*;
use crate::sources::split_quote;
use crate::types::Config;
use anyhow::Result;

/// Spot and swap files share names and columns; the stems are the same dirs.
/// Days and months are cut at UTC+8 (verified on daily and monthly files).
const FILES: Files = Files {
    monthly: "{sym}-trades-{y:04}-{m:02}.zip",
    daily: "{sym}-trades-{y:04}-{m:02}-{d:02}.zip",
    tz_h: 8,
};

pub fn venue(config: &Config) -> Result<Option<Venue>> {
    let (quote, perp) = split_quote(&config.quote);
    let (base, quote) = (config.base.to_uppercase(), quote.to_uppercase());
    if perp && !matches!(quote.as_str(), "USDT" | "USDC") {
        return Ok(None);
    }
    let inst = format!("{base}-{quote}");
    if !perp {
        return Venue::new("okx", false, inst, FILES, row).map(Some);
    }
    let inst = format!("{inst}-SWAP");
    let url = format!("https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId={inst}");
    Ok(Some(
        Venue::new("okx", true, inst.clone(), FILES, row)?.with_contract(url, move |j| {
            // An inverse contract (valued in quote currency) would be mis-scaled by
            // price: only a linear swap valued in the base asset is accepted.
            let d = &j["data"][0];
            anyhow::ensure!(
                d["ctType"] == "linear" && d["ctValCcy"].as_str() == Some(base.as_str()),
                "OKX {inst}: not a linear swap valued in {base} (ctType={}, ctValCcy={})",
                d["ctType"],
                d["ctValCcy"]
            );
            d["ctVal"]
                .as_str()
                .and_then(|v| v.parse().ok())
                .ok_or_else(|| anyhow::anyhow!("OKX {inst}: no ctVal in instruments reply"))
        }),
    ))
}

/// instrument_name, trade_id, side, price, size (contracts on swaps), created_time (ms).
fn row(r: &csv::StringRecord) -> Option<Row> {
    Some(Row {
        us: ms_to_us(field(r, 5)?)?,
        price: field(r, 3)?,
        size: field(r, 4)?,
        buyer: r.get(2)?.eq_ignore_ascii_case("buy"),
    })
}
