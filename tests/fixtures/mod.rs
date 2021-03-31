use proctor::telemetry::{get_subscriber, init_subscriber};

pub fn init_tracing<S: AsRef<str>>(name: S) {
    lazy_static::initialize(&proctor::telemetry::TEST_TRACING);
    tracing::info!("TRACING enabled");
    // let subscriber = get_subscriber(name.as_ref(), "warn");
    // init_subscriber(subscriber);
}
