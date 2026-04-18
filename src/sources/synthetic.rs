use crate::types::{Config, GenerativeModel, TickFrame};
use crate::sources::TickSource;
use anyhow::Result;
use async_trait::async_trait;
use mitch::Tick;
use rand::{rngs::StdRng, Rng, SeedableRng};
use rand_distr::{Distribution, Exp, Normal, Poisson};
use tokio::sync::mpsc;
use tracing::info;

pub struct SyntheticSource {
    model: GenerativeModel,
}

impl SyntheticSource {
    pub fn new(model: GenerativeModel) -> Self {
        Self { model }
    }

    fn generate_ticks(&self, config: &Config) -> Vec<TickFrame> {
        let mut rng = StdRng::from_entropy();
        let mut ticks = Vec::new();

        // Fixed 500ms epoch for synthetic data generation
        const SYNTHETIC_EPOCH_MS: f64 = 500.0;
        const EPOCHS_PER_YEAR: f64 = (365.25 * 24.0 * 60.0 * 60.0 * 1000.0) / SYNTHETIC_EPOCH_MS; // ~63,115,200 epochs per year

        // Get ticker ID for synthetic data
        let ticker_id = nxr_sdk::resolve_ticker_id(
            &format!("{}{}", config.base, config.quote)
        );

        // Calculate number of ticks to generate based on time range
        let duration_ms = (config.to.timestamp_millis() - config.from.timestamp_millis()) as f64;
        let num_ticks = (duration_ms / SYNTHETIC_EPOCH_MS) as usize;
        let mut timestamp = config.from.timestamp_millis();

        match &self.model {
            GenerativeModel::GBM { mu, sigma, base } => {
                let mut price = *base;
                let normal = Normal::new(0.0, 1.0).unwrap();

                // Convert yearly parameters to per-epoch
                let mu_per_epoch = mu / EPOCHS_PER_YEAR;
                let sigma_per_epoch = sigma / EPOCHS_PER_YEAR.sqrt();

                for _ in 0..num_ticks {
                    let z = normal.sample(&mut rng);
                    let drift = mu_per_epoch;
                    let diffusion = sigma_per_epoch * z;

                    price = price * (1.0 + drift + diffusion);

                    let spread = price * 0.0001; // 0.01% spread
                    let is_buy = rng.gen_bool(0.5);

                    let bid = price - spread / 2.0;
                    let ask = price + spread / 2.0;
                    let vbid = if is_buy { 0 } else { (rng.gen::<f64>() * 10000.0) as u32 };
                    let vask = if is_buy { (rng.gen::<f64>() * 10000.0) as u32 } else { 0 };

                    ticks.push(TickFrame::new(0, mitch::timestamp::from_epoch_ms(timestamp), Tick::new_unchecked(ticker_id, bid, ask, vbid, vask)));

                    timestamp += SYNTHETIC_EPOCH_MS as i64;
                }
            }
            GenerativeModel::FBM { mu, sigma, hurst, base } => {
                let mut price = *base;
                let normal = Normal::new(0.0, 1.0).unwrap();

                // Simple FBM approximation using fractional noise
                let mu_per_epoch = mu / EPOCHS_PER_YEAR;
                let sigma_per_epoch = sigma / EPOCHS_PER_YEAR.sqrt();

                for _ in 0..num_ticks {
                    let z = normal.sample(&mut rng);

                    // FBM: scale by Hurst parameter for memory effect
                    let memory = (*hurst - 0.5) * 2.0; // -1 to 1
                    let scaled_z = z * (1.0 + memory);

                    price = price * (1.0 + mu_per_epoch + sigma_per_epoch * scaled_z);

                    let spread = price * 0.0001;
                    let is_buy = rng.gen_bool(0.5);

                    let bid = price - spread / 2.0;
                    let ask = price + spread / 2.0;
                    let vbid = if is_buy { 0 } else { (rng.gen::<f64>() * 10000.0) as u32 };
                    let vask = if is_buy { (rng.gen::<f64>() * 10000.0) as u32 } else { 0 };

                    ticks.push(TickFrame::new(0, mitch::timestamp::from_epoch_ms(timestamp), Tick::new_unchecked(ticker_id, bid, ask, vbid, vask)));

                    timestamp += SYNTHETIC_EPOCH_MS as i64;
                }
            }
            GenerativeModel::Heston { mu, sigma, kappa, theta, xi, rho, base } => {
                let mut price = *base;
                let mut volatility = *sigma;
                let normal = Normal::new(0.0, 1.0).unwrap();

                let mu_per_epoch = mu / EPOCHS_PER_YEAR;

                for _ in 0..num_ticks {
                    let z1 = normal.sample(&mut rng);
                    let z2 = normal.sample(&mut rng);

                    // Correlated shocks
                    let z_v = z1;
                    let z_p = *rho * z_v + (1.0 - rho * rho).sqrt() * z2;

                    // Heston volatility dynamics
                    let kappa_per_epoch = kappa / EPOCHS_PER_YEAR;
                    volatility = volatility + kappa_per_epoch * (theta - volatility) + xi * volatility.sqrt() * z_v / EPOCHS_PER_YEAR.sqrt();
                    volatility = volatility.max(0.0001);

                    // Price dynamics with stochastic volatility
                    let drift = mu_per_epoch;
                    let diffusion = volatility / EPOCHS_PER_YEAR.sqrt() * z_p;

                    price = price * (1.0 + drift + diffusion);

                    let spread = price * 0.0001;
                    let is_buy = rng.gen_bool(0.5);

                    let bid = price - spread / 2.0;
                    let ask = price + spread / 2.0;
                    let vbid = if is_buy { 0 } else { (rng.gen::<f64>() * 10000.0) as u32 };
                    let vask = if is_buy { (rng.gen::<f64>() * 10000.0) as u32 } else { 0 };

                    ticks.push(TickFrame::new(0, mitch::timestamp::from_epoch_ms(timestamp), Tick::new_unchecked(ticker_id, bid, ask, vbid, vask)));

                    timestamp += SYNTHETIC_EPOCH_MS as i64;
                }
            }
            GenerativeModel::NormalJumpDiffusion { mu, sigma, lambda, mu_jump, sigma_jump, base } => {
                let mut price = *base;
                let normal = Normal::new(0.0, 1.0).unwrap();
                let poisson = Poisson::new(*lambda / EPOCHS_PER_YEAR).unwrap();
                let jump_normal = Normal::new(*mu_jump, *sigma_jump).unwrap();

                let mu_per_epoch = mu / EPOCHS_PER_YEAR;
                let sigma_per_epoch = sigma / EPOCHS_PER_YEAR.sqrt();

                for _ in 0..num_ticks {
                    let z = normal.sample(&mut rng);

                    // Diffusion component
                    let drift = mu_per_epoch;
                    let diffusion = sigma_per_epoch * z;

                    // Jump component
                    let num_jumps = poisson.sample(&mut rng) as i32;
                    let mut jump_component = 0.0;
                    for _ in 0..num_jumps {
                        jump_component += jump_normal.sample(&mut rng);
                    }

                    price = price * (1.0 + drift + diffusion + jump_component);

                    let spread = price * 0.0001;
                    let is_buy = rng.gen_bool(0.5);

                    let bid = price - spread / 2.0;
                    let ask = price + spread / 2.0;
                    let vbid = if is_buy { 0 } else { (rng.gen::<f64>() * 10000.0) as u32 };
                    let vask = if is_buy { (rng.gen::<f64>() * 10000.0) as u32 } else { 0 };

                    ticks.push(TickFrame::new(0, mitch::timestamp::from_epoch_ms(timestamp), Tick::new_unchecked(ticker_id, bid, ask, vbid, vask)));

                    timestamp += SYNTHETIC_EPOCH_MS as i64;
                }
            }
            GenerativeModel::DoubleExpJumpDiffusion { mu, sigma, lambda, mu_pos_jump, mu_neg_jump, p_neg_jump, base } => {
                let mut price = *base;
                let normal = Normal::new(0.0, 1.0).unwrap();
                let poisson = Poisson::new(*lambda / EPOCHS_PER_YEAR).unwrap();
                let exp_pos = Exp::new(1.0 / mu_pos_jump.abs()).unwrap();
                let exp_neg = Exp::new(1.0 / mu_neg_jump.abs()).unwrap();

                let mu_per_epoch = mu / EPOCHS_PER_YEAR;
                let sigma_per_epoch = sigma / EPOCHS_PER_YEAR.sqrt();

                for _ in 0..num_ticks {
                    let z = normal.sample(&mut rng);

                    // Diffusion component
                    let drift = mu_per_epoch;
                    let diffusion = sigma_per_epoch * z;

                    // Jump component
                    let num_jumps = poisson.sample(&mut rng) as i32;
                    let mut jump_component = 0.0;
                    for _ in 0..num_jumps {
                        if rng.gen::<f64>() < *p_neg_jump {
                            // Negative jump
                            jump_component -= exp_neg.sample(&mut rng);
                        } else {
                            // Positive jump
                            jump_component += exp_pos.sample(&mut rng);
                        }
                    }

                    price = price * (1.0 + drift + diffusion + jump_component);

                    let spread = price * 0.0001;
                    let is_buy = rng.gen_bool(0.5);

                    let bid = price - spread / 2.0;
                    let ask = price + spread / 2.0;
                    let vbid = if is_buy { 0 } else { (rng.gen::<f64>() * 10000.0) as u32 };
                    let vask = if is_buy { (rng.gen::<f64>() * 10000.0) as u32 } else { 0 };

                    ticks.push(TickFrame::new(0, mitch::timestamp::from_epoch_ms(timestamp), Tick::new_unchecked(ticker_id, bid, ask, vbid, vask)));

                    timestamp += SYNTHETIC_EPOCH_MS as i64;
                }
            }
        }

        ticks
    }
}

#[async_trait]
impl TickSource for SyntheticSource {
    async fn fetch_ticks(
        &self,
        config: &Config,
        tx: mpsc::Sender<Vec<TickFrame>>,
    ) -> Result<()> {
        info!("Generating synthetic data using {:?}", self.model);

        let ticks = self.generate_ticks(config);
        let total_ticks = ticks.len();

        // Send in batches
        for chunk in ticks.chunks(10000) {
            tx.send(chunk.to_vec()).await?;
        }

        info!("Generated {} synthetic ticks", total_ticks);
        Ok(())
    }
}
