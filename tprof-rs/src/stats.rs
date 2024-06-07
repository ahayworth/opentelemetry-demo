use statrs::{
    statistics,
    statistics::{Distribution, OrderStatistics},
};

use std::ops::Sub;

#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub count: usize,
    pub mean: f64,
    pub std_dev: f64,
    pub percentile_50: f64,
    pub percentile_99: f64,
}

impl Sub for &Stats {
    type Output = Stats;

    fn sub(self, other: Self) -> Self::Output {
        Self::Output {
            count: 0,
            mean: self.mean - other.mean,
            std_dev: self.std_dev - other.std_dev,
            percentile_50: self.percentile_50 - other.percentile_50,
            percentile_99: self.percentile_99 - other.percentile_99,
        }
    }
}

impl From<&[f64]> for Stats {
    fn from(data: &[f64]) -> Self {
        let mut data = statistics::Data::new(data.to_owned());
        Stats {
            count: data.len(),
            mean: data.mean().unwrap_or_default(),
            std_dev: data.std_dev().unwrap_or_default(),
            percentile_50: data.percentile(50),
            percentile_99: data.percentile(99),
        }
    }
}
