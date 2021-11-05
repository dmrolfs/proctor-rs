mod test_basic_clearinghouse_subscription;
mod test_from_telemetry_stage;
mod test_make_telemetry_cvs_source;
mod test_make_telemetry_rest_api_source;
mod test_policy_filter;

use chrono::{DateTime, TimeZone, Utc};
use once_cell::sync::Lazy;

pub static DEFAULT_LAST_DEPLOYMENT: Lazy<DateTime<Utc>> =
    Lazy::new(|| Utc.datetime_from_str("1970-08-30 11:32:09", "%Y-%m-%d %H:%M:%S").unwrap());
