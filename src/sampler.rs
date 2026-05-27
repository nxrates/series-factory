//! Search algorithms for Renko multiplier optimization.
//!
//! 1D search space: multiplier (log-uniform 0.005–0.5).
//! Phase A: log-uniform exploration, Phase B: local refinement around best.

use nxr_sdk::renko::RenkoConfig;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use rand_distr::Normal;

/// Configuration for the search process
#[derive(Debug, Clone)]
pub struct SearchConfig {
    /// Phase A: number of exploration points
    pub n_explore: usize,
    /// Phase B: number of refinement points
    pub n_refine: usize,
    /// Random seed
    pub seed: u64,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self { n_explore: 200, n_refine: 100, seed: 42 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SearchPhase {
    Explore,
    Refine,
    Done,
}

pub struct SearchState {
    pub phase: SearchPhase,
    pub trial_count: usize,
    pub best_score: f64,
    pub incumbent: Option<RenkoConfig>,
    pub history: Vec<(RenkoConfig, f64)>,
    pub config: SearchConfig,
    rng: ChaCha8Rng,
}

impl SearchState {
    pub fn new(config: SearchConfig) -> Self {
        let rng = ChaCha8Rng::seed_from_u64(config.seed);
        Self {
            phase: SearchPhase::Explore,
            trial_count: 0,
            best_score: f64::NEG_INFINITY,
            incumbent: None,
            history: Vec::new(),
            config,
            rng,
        }
    }

    /// Generate the next configuration to test.
    pub fn next_config(&mut self) -> Option<RenkoConfig> {
        match self.phase {
            SearchPhase::Explore => {
                if self.trial_count >= self.config.n_explore {
                    self.phase = SearchPhase::Refine;
                    return self.next_config();
                }
                Some(self.sample_explore())
            }
            SearchPhase::Refine => {
                if self.trial_count >= self.config.n_explore + self.config.n_refine {
                    self.phase = SearchPhase::Done;
                    return None;
                }
                let base = self.incumbent.unwrap_or_default();
                Some(self.perturb(&base))
            }
            SearchPhase::Done => None,
        }
    }

    /// Log-uniform exploration of multiplier space.
    fn sample_explore(&mut self) -> RenkoConfig {
        let log_min = 0.005f64.ln();
        let log_max = 0.5f64.ln();
        let log_m = log_min + self.rng.gen::<f64>() * (log_max - log_min);
        let multiplier = log_m.exp() as f32;

        RenkoConfig { multiplier, ..Default::default() }
    }

    /// Small perturbation around a base config.
    fn perturb(&mut self, base: &RenkoConfig) -> RenkoConfig {
        let normal = Normal::new(0.0, 1.0).unwrap();

        let log_m = (base.multiplier as f64).ln();
        let new_log_m = log_m + 0.15 * self.rng.sample(normal);
        let multiplier = new_log_m.exp().clamp(0.005, 0.5) as f32;

        RenkoConfig { multiplier, ..Default::default() }
    }

    /// Update with trial result.
    pub fn update(&mut self, config: RenkoConfig, score: f64) {
        self.history.push((config, score));
        self.trial_count += 1;

        if score > self.best_score {
            self.best_score = score;
            self.incumbent = Some(config);
        }
    }

    /// Get top K configurations.
    pub fn top_k(&self, k: usize) -> Vec<(RenkoConfig, f64)> {
        let mut sorted = self.history.clone();
        sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        sorted.truncate(k);
        sorted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_state() {
        let mut state = SearchState::new(SearchConfig { n_explore: 10, n_refine: 5, seed: 42 });

        for _ in 0..10 {
            let config = state.next_config().unwrap();
            assert!(config.validate().is_ok());
            state.update(config, 0.5);
        }

        // After 10 explore trials, next call transitions to Refine
        let config = state.next_config().unwrap();
        assert_eq!(state.phase, SearchPhase::Refine);
        assert!(config.validate().is_ok());
    }
}
