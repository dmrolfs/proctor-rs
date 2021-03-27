use proctor::telemetry::{get_subscriber, init_subscriber};

pub fn init_tracing<S: AsRef<str>>(name: S) {
    let subscriber = get_subscriber(name.as_ref(), "warn");
    init_subscriber(subscriber);
}
