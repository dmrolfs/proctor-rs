use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;

use futures::future::FutureExt;
use reqwest_middleware::ClientBuilder;
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc;

use super::SensorSetting;
use crate::elements::Telemetry;
use crate::error::{IncompatibleSensorSettings, SenseError};
use crate::graph::stage::tick::TickCmd;
use crate::graph::stage::{CompositeSource, SourceStage, WithApi};
use crate::graph::{stage, Connect, Graph, SinkShape, SourceShape};

/// Naive CVS sensor, which loads a `.cvs` file, then publishes via its outlet.
///
/// An improvement to consider is to behave lazily and iterate through the source file(s)
/// upon downstream demand.
#[tracing::instrument(level = "trace", skip(name))]
pub fn make_telemetry_cvs_sensor<T, S>(name: S, setting: &SensorSetting) -> Result<TelemetrySensor, SenseError>
where
    T: Serialize + DeserializeOwned + Debug,
    S: Into<String>,
{
    if let SensorSetting::Csv { path } = setting {
        let name = name.into();
        let mut telemetry_name = format!("telemetry_{}", name.as_str());

        if let Some(file_name) = path.file_name() {
            match file_name.to_str() {
                None => (),
                Some(file_name) => telemetry_name.push_str(format!("_{}", file_name).as_str()),
            }
        }

        let csv_span = tracing::debug_span!("sourcing CSV", %telemetry_name, ?path);
        let _csv_span_guard = csv_span.enter();

        let mut records: Vec<Telemetry> = vec![];
        let mut reader = csv::Reader::from_path(path)?;

        tracing::trace!("loading records from CSV...");
        for result in reader.deserialize() {
            let record: T = result?;

            // todo: for a sensor, which is better? config-style conversion via serde (active here) or
            // Into<Telemetry>?
            let telemetry_record = Telemetry::try_from(&record)?;

            records.push(telemetry_record);
        }
        tracing::debug!("deserialized {} records from CSV.", records.len());

        let source = stage::Sequence::new(telemetry_name, records);
        let stage: Option<Box<dyn SourceStage<Telemetry>>> = Some(Box::new(source));

        Ok(TelemetrySensor { name, stage, tx_stop: None })
    } else {
        Err(IncompatibleSensorSettings::ExpectedTypeError {
            expected: "cvs".to_string(),
            settings: setting.clone(),
        }
        .into())
    }
}

#[tracing::instrument(level = "trace", skip(name))]
pub async fn make_telemetry_rest_api_sensor<T>(
    name: String, setting: &SensorSetting,
) -> Result<TelemetrySensor, SenseError>
where
    T: Serialize + DeserializeOwned + Debug,
{
    if let SensorSetting::RestApi(query) = setting {
        // scheduler
        let tick = stage::Tick::new(
            format!("telemetry_{}_tick", name),
            Duration::from_nanos(0),
            query.interval,
            (),
        );
        let tx_tick_api = tick.tx_api();

        // generator via rest api
        let headers = query.header_map()?;
        let client = reqwest::Client::builder().default_headers(headers).build()?;
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(query.max_retries);
        let client = ClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        let method = query.method.clone();
        let url = query.url.clone();

        let gen = move |_: ()| {
            let client = client.clone();
            let method = method.clone();
            let url = url.clone();

            async move {
                let record: T = client
                    .request(method.clone(), url.clone())
                    .send()
                    .await?
                    .json::<T>()
                    .await?;

                let telemetry_record = Telemetry::try_from(&record)?;
                std::result::Result::<Telemetry, SenseError>::Ok(telemetry_record)
            }
            .map(|d| d.unwrap())
        };

        let collect_telemetry =
            stage::TriggeredGenerator::<_, _, Telemetry>::new(format!("telemetry_{}_gen", name), gen);

        // compose tick & generator into a source shape
        let composite_outlet = collect_telemetry.outlet().clone();
        (tick.outlet(), collect_telemetry.inlet()).connect().await;

        let mut cg = Graph::default();
        cg.push_back(Box::new(tick)).await;
        cg.push_back(Box::new(collect_telemetry)).await;
        let composite: CompositeSource<Telemetry> =
            stage::CompositeSource::new(format!("telemetry_{}", name).into(), cg, composite_outlet).await;
        let stage: Option<Box<dyn SourceStage<Telemetry>>> = Some(Box::new(composite));

        Ok(TelemetrySensor { name, stage, tx_stop: Some(tx_tick_api) })
    } else {
        Err(IncompatibleSensorSettings::ExpectedTypeError {
            expected: "HTTP query".to_string(),
            settings: setting.clone(),
        }
        .into())
    }
}

pub struct TelemetrySensor {
    pub name: String,
    pub stage: Option<Box<dyn SourceStage<Telemetry>>>,
    pub tx_stop: Option<mpsc::UnboundedSender<TickCmd>>,
}

impl TelemetrySensor {
    #[tracing::instrument(level = "trace")]
    pub async fn from_settings<T>(settings: &HashMap<String, SensorSetting>) -> Result<Vec<Self>, SenseError>
    where
        T: Serialize + DeserializeOwned + Debug,
    {
        let mut sensors = Vec::with_capacity(settings.len());
        for (name, sensor_setting) in settings {
            let src = match sensor_setting {
                SensorSetting::RestApi(_query) => {
                    make_telemetry_rest_api_sensor::<T>(name.clone(), sensor_setting).await?
                },
                SensorSetting::Csv { path: _ } => make_telemetry_cvs_sensor::<T, _>(name, sensor_setting)?,
            };
            sensors.push(src);
        }

        Ok(sensors)
    }
}
