use series_factory::types::{AggregationMode, Config, DataSource, GenerativeModel, TickFrame};
use series_factory::aggregation::Aggregator;
use series_factory::sources::create_source;
use mitch::bar::Bar;
use chrono::{Duration, Utc};
use tokio::sync::mpsc;

const TEST_STEP_MS: i64 = 10_000;

fn test_config() -> Config {
    Config {
        base: "BTC".to_string(),
        quote: "USDT".to_string(),
        sources: vec![],
        from: Utc::now() - Duration::days(30),
        to: Utc::now() - Duration::days(29),
        agg_mode: AggregationMode::Time,
        agg_step: TEST_STEP_MS as f64,
        cycle_ms: 50,
        stale_secs: 30.0,
        z_threshold: 6.0,
        ticks_dir: "/tmp/test_cache".into(),
        bars_dir: "/tmp/test_output".into(),
    }
}

fn aggregate(ticks: Vec<TickFrame>, config: &Config) -> Vec<Bar> {
    let mut agg = Aggregator::new(config.clone());
    let mut bars = agg.process_ticks(&ticks);
    bars.extend(agg.finalize());
    bars
}

async fn test_exchange_time_aggregation(exchange: &str, days_back: i64) {
    let mut config = test_config();
    config.sources = vec![exchange.to_string()];
    config.from = Utc::now() - Duration::days(days_back);
    config.to = Utc::now() - Duration::days(days_back - 1);

    let source = create_source(&DataSource::Exchange(exchange.to_string()))
        .await
        .unwrap();

    let (tx, mut rx) = mpsc::channel(100);
    let config_for_spawn = config.clone();
    tokio::spawn(async move {
        let _ = source.fetch_ticks(&config_for_spawn, tx).await;
    });

    let mut ticks = Vec::new();
    while let Some(batch) = rx.recv().await {
        ticks.extend(batch);
        if ticks.len() > 10_000 { break; }
    }

    if ticks.is_empty() { return; }

    let results = aggregate(ticks, &config);

    for window in results.windows(2) {
        let a = window[0].close_time_ms();
        let b = window[1].close_time_ms();
        let diff = b - a;
        assert_eq!(diff, TEST_STEP_MS, "{} time aggregates should have consistent steps", exchange);
    }
}

#[tokio::test]
async fn test_binance_time_aggregation() {
    test_exchange_time_aggregation("binance", 30).await;
}

#[tokio::test]
async fn test_bybit_time_aggregation() {
    test_exchange_time_aggregation("bybit", 30).await;
}

#[tokio::test]
async fn test_okx_time_aggregation() {
    test_exchange_time_aggregation("okx", 30).await;
}

#[tokio::test]
async fn test_bitget_time_aggregation() {
    test_exchange_time_aggregation("bitget", 30).await;
}

#[tokio::test]
async fn test_gbm_price_aggregation() {
    let mut config = test_config();
    config.sources = vec!["gbm".to_string()];
    config.from = Utc::now() - Duration::hours(1);
    config.to = Utc::now();
    config.agg_mode = AggregationMode::Tick;
    config.agg_step = 0.001;

    let model = GenerativeModel::GBM { mu: 0.0001, sigma: 0.001, base: 100.0 };
    let source = create_source(&DataSource::Synthetic(model)).await.unwrap();

    let (tx, mut rx) = mpsc::channel(100);
    let config_for_spawn = config.clone();
    tokio::spawn(async move {
        let _ = source.fetch_ticks(&config_for_spawn, tx).await;
    });

    let mut ticks = Vec::new();
    while let Some(batch) = rx.recv().await {
        ticks.extend(batch);
        if ticks.len() > 10_000 { break; }
    }

    let results = aggregate(ticks, &config);

    for window in results.windows(2) {
        let prev_close = window[0].close;
        let curr_close = window[1].close;
        let expected_upper = prev_close * 1.001;
        let expected_lower = prev_close / 1.001;
        assert!(curr_close >= expected_upper || curr_close <= expected_lower);
    }
}

#[tokio::test]
async fn test_all_synthetic_models() {
    let models = vec![
        GenerativeModel::GBM { mu: 0.0001, sigma: 0.001, base: 100.0 },
        GenerativeModel::FBM { mu: 0.0001, sigma: 0.001, hurst: 0.7, base: 100.0 },
        GenerativeModel::Heston {
            mu: 0.0001, sigma: 0.001, kappa: 1000.0, theta: 0.001, xi: 0.001, rho: -0.75, base: 100.0
        },
        GenerativeModel::NormalJumpDiffusion {
            mu: 0.0001, sigma: 0.001, lambda: 10.0, mu_jump: 0.0, sigma_jump: 0.1, base: 100.0
        },
        GenerativeModel::DoubleExpJumpDiffusion {
            mu: 0.0001, sigma: 0.001, lambda: 10.0, mu_pos_jump: 0.01, mu_neg_jump: -0.02, p_neg_jump: 0.6, base: 100.0
        },
    ];

    let from = Utc::now() - Duration::days(1);
    let to = Utc::now();

    let tasks: Vec<_> = models.into_iter().map(|model| {
        tokio::spawn(async move {
            let mut config = test_config();
            config.from = from;
            config.to = to;

            let source = create_source(&DataSource::Synthetic(model)).await.unwrap();
            let (tx, mut rx) = mpsc::channel(100);

            let config_for_spawn = config.clone();
            tokio::spawn(async move {
                let _ = source.fetch_ticks(&config_for_spawn, tx).await;
            });

            let mut ticks = Vec::new();
            while let Some(batch) = rx.recv().await {
                ticks.extend(batch);
                if ticks.len() > 1000 { break; }
            }

            assert!(!ticks.is_empty());

            let results = aggregate(ticks, &config);
            assert!(!results.is_empty());

            for bar in &results {
                assert!(bar.close_time_ms() > 0);
                let close = bar.close;
                assert!(close > 0.0);
            }
        })
    }).collect();

    for task in tasks {
        task.await.unwrap();
    }
}

#[tokio::test]
async fn test_aggregate_fields() {
    let mut config = test_config();
    config.from = Utc::now() - Duration::hours(1);
    config.to = Utc::now();

    let model = GenerativeModel::GBM { mu: 0.0001, sigma: 0.001, base: 100.0 };
    let source = create_source(&DataSource::Synthetic(model)).await.unwrap();

    let (tx, mut rx) = mpsc::channel(100);
    let config_for_spawn = config.clone();
    tokio::spawn(async move {
        let _ = source.fetch_ticks(&config_for_spawn, tx).await;
    });

    let mut ticks = Vec::new();
    while let Some(batch) = rx.recv().await {
        ticks.extend(batch);
        if ticks.len() > 1000 { break; }
    }

    let results = aggregate(ticks, &config);

    for bar in &results {
        let (o, h, l, c) = (bar.open, bar.high, bar.low, bar.close);
        assert!(bar.close_time_ms() > 0);
        assert!(o > 0.0);
        assert!(h >= l);
        assert!(c > 0.0);
        assert!(h >= o && h >= c);
        assert!(l <= o && l <= c);
    }
}
