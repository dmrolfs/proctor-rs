use tracing::{subscriber::set_global_default, Subscriber};
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_flame::FlameLayer;
use tracing_log::LogTracer;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Registry};

lazy_static::lazy_static! {
    pub static ref TEST_TRACING: () = {
        let filter = if std::env::var("TEST_LOG").is_ok() { "trace" } else { "" };
        let subscriber = get_subscriber("test", filter);
        init_subscriber(subscriber);
    };
}

/// Compose multiple layers into a `tracing`'s subscriber.
///
/// # Implementation Notes
///
/// We are using `impl Subscriber` as return type to avoid having to spell out the actual type of
/// the returned subscriber, which is indeed quite complex.
/// We need to explicitly call out that returned subscriber is `Send` and `Sync` to make it
/// possible to pass it to `init_subscriber` later on.
pub fn get_subscriber(name: impl Into<String>, env_filter: impl AsRef<str>) -> impl Subscriber + Sync + Send {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(env_filter));

    // let (flame_subscriber, _guard) = FlameLayer::with_file("./tracing.folded").unwrap();

    let formatting_layer = BunyanFormattingLayer::new(name.into(), std::io::stdout);

    Registry::default()
        .with(env_filter)
        // .with(flame_subscriber)
        .with(JsonStorageLayer)
        .with(formatting_layer)
}

/// Register a subscriber as global default to process span data.
///
/// It should be only called once!
pub fn init_subscriber(subscriber: impl Subscriber + Sync + Send) {
    LogTracer::init().expect("Failed to set logger");
    set_global_default(subscriber).expect("Failed to set subscriber");
}
