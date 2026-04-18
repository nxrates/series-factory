use crate::types::Bar;
use chrono::DateTime;

pub fn display_data_table(bars: &[Bar]) {
    if bars.is_empty() {
        println!("No data to display");
        return;
    }

    println!("\n{}", "=".repeat(110));
    println!("                                    GENERATED DATA PREVIEW");
    println!("{}", "=".repeat(110));

    println!("{:<20} {:>12} {:>8} {:>8} {:>8} {:>10} {:>10} {:>10}",
        "Timestamp", "Close", "VBid", "VAsk", "Ticks", "Dispersion", "Drift", "VolImbal");
    println!("{}", "-".repeat(110));

    let display_row = |bar: &Bar| {
        // Copy fields from packed struct to avoid unaligned references
        let close = bar.close;
        let vbid = bar.vbid;
        let vask = bar.vask;
        let tc = bar.tick_count;
        let disp = bar.dispersion;
        let drift = bar.drift;
        let vi = bar.vol_imbalance;

        let dt = DateTime::from_timestamp_millis(bar.close_time_ms()).unwrap_or_default();
        let ts = dt.format("%Y%m%d %H:%M:%S%.3f").to_string();
        let price = if close < 1.0 { format!("{:.8}", close) }
            else if close < 100.0 { format!("{:.6}", close) }
            else { format!("{:.4}", close) };
        println!("{:<20} {:>12} {:>8} {:>8} {:>8} {:>10.6} {:>10.6} {:>10.6}",
            ts, price, vbid, vask, tc, disp, drift, vi);
    };

    println!("=== FIRST 10 ROWS ===");
    for bar in bars.iter().take(10) { display_row(bar); }

    if bars.len() > 10 {
        println!("\n=== LAST 10 ROWS ===");
        for bar in bars.iter().rev().take(10).rev() { display_row(bar); }
    }

    println!("{}", "=".repeat(110));
    println!("Total rows: {}", bars.len());

    let avg_price: f64 = bars.iter().map(|b| { let c = b.close; c }).sum::<f64>() / bars.len() as f64;
    let total_vol: u64 = bars.iter().map(|b| { let (vb, va) = (b.vbid, b.vask); (vb + va) as u64 }).sum();
    let synthetic = bars.iter().filter(|b| { let tc = b.tick_count; tc == 0 }).count();

    println!("Average Price: {:.4}", avg_price);
    println!("Total Volume: {}", total_vol);
    println!("Synthetic Bars: {} ({:.2}%)", synthetic, 100.0 * synthetic as f64 / bars.len() as f64);
    println!("{}", "=".repeat(110));
}
