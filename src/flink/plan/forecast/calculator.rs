use std::time::Duration;

use super::{RecordsPerSecond, TimestampSeconds, WorkloadForecast, WorkloadForecastBuilder, WorkloadMeasurement};
use crate::error::PlanError;

#[derive(Debug)]
pub struct ForecastCalculator<F: WorkloadForecastBuilder> {
    forecast_builder: F,
    pub restart: Duration,
    pub max_catch_up: Duration,
    pub valid_offset: Duration,
}

impl<F: WorkloadForecastBuilder> ForecastCalculator<F> {
    pub fn new(
        forecast_builder: F, restart: Duration, max_catch_up: Duration, valid_offset: Duration,
    ) -> Result<Self, PlanError> {
        let restart = Self::check_duration(restart)?;
        let max_catch_up = Self::check_duration(max_catch_up)?;
        let valid_offset = Self::check_duration(valid_offset)?;
        Ok(Self {
            forecast_builder,
            restart,
            max_catch_up,
            valid_offset,
        })
    }

    fn check_duration(d: Duration) -> Result<Duration, PlanError> {
        if (f64::MAX as u128) < d.as_millis() {
            return Err(PlanError::DurationLimitExceeded(d.as_millis()));
        }

        Ok(d)
    }

    pub fn observations_needed(&self) -> (usize, usize) {
        self.forecast_builder.observations_needed()
    }

    pub fn add_observation(&mut self, measurement: WorkloadMeasurement) {
        tracing::debug!(?measurement, "adding workload measurement to forecast calculator.");
        self.forecast_builder.add_observation(measurement)
    }

    pub fn clear(&mut self) {
        self.forecast_builder.clear()
    }

    #[tracing::instrument(
    level="debug",
    skip(self),
    fields(restart=?self.restart, max_catch_up=?self.max_catch_up, valid_offset=?self.valid_offset)
    )]
    pub fn calculate_target_rate(&mut self, buffered_records: f64) -> Result<RecordsPerSecond, PlanError> {
        let now = chrono::Utc::now().into();
        let recovery = self.calculate_recovery_timestamp_from(now);
        let valid = self.calculate_valid_timestamp_after_recovery(recovery);
        tracing::debug!( now=?now.as_utc(), recovery=?recovery.as_utc(), valid=?valid.as_utc(), "scaling adjustment timestamps estimated." );

        let forecast = self.forecast_builder.build_forecast()?;
        tracing::debug!(?forecast, "workload forecast model calculated.");

        let total_records = self.total_records_between(&forecast, now, recovery)? + buffered_records;
        tracing::debug!(%total_records, "estimated total records to process before valid time");

        let recovery_rate = self.recovery_rate(total_records);
        let valid_workload_rate = forecast.workload_at(valid)?;
        let target_rate = RecordsPerSecond::max(recovery_rate, valid_workload_rate);

        tracing::debug!(
            %recovery_rate,
            %valid_workload_rate,
            %target_rate,
            "target rate calculated as max of recovery and workload (at valid time) rates."
        );

        Ok(target_rate)
    }

    fn calculate_recovery_timestamp_from(&self, timestamp: TimestampSeconds) -> TimestampSeconds {
        timestamp + self.restart + self.max_catch_up
    }

    fn calculate_valid_timestamp_after_recovery(&self, recovery: TimestampSeconds) -> TimestampSeconds {
        recovery + self.valid_offset
    }

    fn total_records_between(
        &self, forecast: &Box<dyn WorkloadForecast>, start: TimestampSeconds, end: TimestampSeconds,
    ) -> Result<f64, PlanError> {
        let total = forecast.total_records_between(start, end)?;
        tracing::debug!("total records between [{}, {}] = {}", start, end, total);
        Ok(total)
    }

    fn recovery_rate(&self, total_records: f64) -> RecordsPerSecond {
        let catch_up = self.max_catch_up.as_secs_f64();
        RecordsPerSecond::new(total_records / catch_up)
    }
}
