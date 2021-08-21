mod test_basic_clearinghouse_subscription;
mod test_from_telemetry_stage;
mod test_make_telemetry_cvs_source;
mod test_make_telemetry_rest_api_source;
mod test_policy_filter;

use chrono::{DateTime, TimeZone, Utc};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref DEFAULT_LAST_DEPLOYMENT: DateTime<Utc> =
        Utc.datetime_from_str("1970-08-30 11:32:09", "%Y-%m-%d %H:%M:%S").unwrap();
}
