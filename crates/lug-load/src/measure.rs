use crate::{Result, error};
use hdrhistogram::Histogram;
use serde_json::{Value, json};
use std::time::{Duration, Instant};

pub fn offset(sequence: u64, rate: u64) -> Duration {
    Duration::from_nanos((u128::from(sequence) * 1_000_000_000 / u128::from(rate)) as u64)
}

pub struct Latency(Histogram<u64>);

impl Latency {
    pub fn new() -> Result<Self> {
        Ok(Self(Histogram::new(3)?))
    }

    pub fn record(&mut self, intended: Instant, observed: Instant) -> Result<()> {
        let elapsed = observed
            .checked_duration_since(intended)
            .ok_or_else(|| error("observation precedes intended send time"))?;
        let micros = u64::try_from(elapsed.as_nanos().div_ceil(1000))?;
        self.0.record(micros.max(1))?;
        Ok(())
    }

    pub fn json(&self) -> Value {
        if self.0.is_empty() {
            return json!({"count": 0, "p50_us": null, "p99_us": null, "p999_us": null});
        }
        json!({
            "count": self.0.len(),
            "p50_us": self.0.value_at_quantile(0.5),
            "p99_us": self.0.value_at_quantile(0.99),
            "p999_us": self.0.value_at_quantile(0.999),
        })
    }
}

pub struct Metrics {
    pub append: Latency,
    pub delivery: Latency,
    pub scheduled: u64,
    pub acknowledged: u64,
    pub delivered: u64,
    pub in_window_acks: u64,
    pub in_window_deliveries: u64,
    pub elapsed: Duration,
    pub tx_bytes: u64,
    pub rx_bytes: u64,
    pub rss_peak: u64,
}

impl Metrics {
    pub fn new() -> Result<Self> {
        Ok(Self {
            append: Latency::new()?,
            delivery: Latency::new()?,
            scheduled: 0,
            acknowledged: 0,
            delivered: 0,
            in_window_acks: 0,
            in_window_deliveries: 0,
            elapsed: Duration::ZERO,
            tx_bytes: 0,
            rx_bytes: 0,
            rss_peak: 0,
        })
    }

    pub fn json(&self, duration: Duration) -> Value {
        json!({
            "scheduled_appends": self.scheduled,
            "acknowledged_appends": self.acknowledged,
            "appends_per_sec": self.acknowledged as f64 / self.elapsed.as_secs_f64(),
            "window_appends_per_sec": self.in_window_acks as f64 / duration.as_secs_f64(),
            "append_latency": self.append.json(),
            "subscriber_records": self.delivered,
            "subscriber_records_per_sec": self.delivered as f64 / self.elapsed.as_secs_f64(),
            "window_subscriber_records_per_sec": self.in_window_deliveries as f64 / duration.as_secs_f64(),
            "publish_to_subscriber_latency": self.delivery.json(),
            "tx_bytes": self.tx_bytes, "rx_bytes": self.rx_bytes,
            "bytes_per_sec": (self.tx_bytes + self.rx_bytes) as f64 / self.elapsed.as_secs_f64(),
            "scheduled_seconds": duration.as_secs_f64(),
            "elapsed_seconds_including_drain": self.elapsed.as_secs_f64(),
            "server_rss_peak_bytes": self.rss_peak,
        })
    }
}

pub fn check_fd_growth(baseline: usize, after: usize, slack: usize) -> Result<()> {
    if after > baseline.saturating_add(slack) {
        return Err(error(format!(
            "FD LEAK: quiescent server fds grew from {baseline} to {after}, allowed slack {slack}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_includes_queueing_not_just_round_trip() {
        let start = Instant::now();
        let mut h = Latency::new().unwrap();
        for n in 0..1000 {
            h.record(start + offset(n, 1000), start + Duration::from_secs(2))
                .unwrap();
        }
        let j = h.json();
        assert_eq!(j["count"], 1000);
        assert!(j["p50_us"].as_u64().unwrap() > 1_400_000);
        assert!(j["p999_us"].as_u64().unwrap() >= 1_990_000);
    }

    #[test]
    fn schedule_is_absolute_and_has_no_cumulative_roundoff() {
        assert_eq!(offset(3, 3), Duration::from_secs(1));
        assert_eq!(offset(10_000, 10_000), Duration::from_secs(1));
    }

    #[test]
    fn empty_histogram_is_not_zero_latency() {
        assert!(Latency::new().unwrap().json()["p99_us"].is_null());
    }

    #[test]
    fn fd_check_uses_fixed_baseline_not_moving_goalposts() {
        assert!(check_fd_growth(20, 20, 0).is_ok());
        assert!(check_fd_growth(20, 21, 0).is_err());
        assert!(check_fd_growth(20, 22, 2).is_ok());
        assert!(check_fd_growth(20, 23, 2).is_err());
    }
}
