use std::fmt::Debug;
use std::time::Duration;

use super::SourceSetting;
use crate::elements::Telemetry;
use crate::error::{CollectionError, SettingsError};
use crate::graph::stage::tick::TickMsg;
use crate::graph::stage::{CompositeSource, SourceStage, WithApi};
use crate::graph::{stage, Connect, Graph, SinkShape, SourceShape};
use futures::future::FutureExt;
use reqwest_middleware::ClientBuilder;
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use tokio::sync::mpsc;

pub struct TelemetrySource {
    pub name: String,
    pub stage: Option<Box<dyn SourceStage<Telemetry>>>,
    tx_stop: Option<mpsc::UnboundedSender<TickMsg>>,
}

impl TelemetrySource {
    pub fn new(
        name: impl AsRef<str>, stage: Box<dyn SourceStage<Telemetry>>, tx_stop: Option<mpsc::UnboundedSender<TickMsg>>,
    ) -> Self {
        Self {
            name: name.as_ref().to_string(),
            stage: Some(stage),
            tx_stop,
        }
    }

    #[tracing::instrument(level = "info")]
    pub async fn collect_from_settings<T>(
        settings: &HashMap<String, SourceSetting>,
    ) -> Result<Vec<Self>, CollectionError>
    where
        //todo: for a source, which is better? config-style conversion via serde (active here) or Into<Telemetry>?
        T: Serialize + DeserializeOwned + Debug,
        // T: Into<Telemetry> + DeserializeOwned + Debug,
    {
        let mut sources = Vec::with_capacity(settings.len());
        for (name, source_setting) in settings {
            let src = match source_setting {
                SourceSetting::RestApi(_query) => make_telemetry_rest_api_source::<T, _>(name, source_setting).await?,
                SourceSetting::Csv { path: _ } => {
                    let foo = make_telemetry_cvs_source::<T, _>(name, source_setting)?;
                    foo
                }
            };
            sources.push(src);
        }

        Ok(sources)
    }

    #[tracing::instrument(level="info", skip(self), fields(source_name=%self.name))]
    pub async fn stop(&self) -> Result<(), CollectionError> {
        if let Some(ref tx) = self.tx_stop {
            let (stop, rx_ack) = TickMsg::stop();
            tx.send(stop).map_err(|err| CollectionError::StageError(err.into()))?;
            rx_ack
                .await
                .map_err(|err| CollectionError::StageError(err.into()))?
                .map_err(|err| CollectionError::StageError(err.into()))?;
        }

        Ok(())
    }
}

/// Naive CVS source, which loads a `.cvs` file, then publishes via its outlet.
///
/// An improvement to consider is to behave lazily and iterate through the source file(s)
/// upon downstream demand.
#[tracing::instrument(
    level="info",
    skip(name, setting),
    fields(stage=%name.as_ref(), ?setting),
)]
pub fn make_telemetry_cvs_source<T, S>(name: S, setting: &SourceSetting) -> Result<TelemetrySource, CollectionError>
where
    //todo: for a source, which is better? config-style conversion via serde (active here) or Into<Telemetry>?
    T: Serialize + DeserializeOwned + Debug,
    // T: Into<Telemetry> + DeserializeOwned + Debug,
    S: AsRef<str>,
{
    if let SourceSetting::Csv { path } = setting {
        let mut telemetry_name = format!("telemetry_{}", name.as_ref());

        if let Some(file_name) = path.file_name() {
            match file_name.to_str() {
                None => (),
                Some(file_name) => telemetry_name.push_str(format!("_{}", file_name).as_str()),
            }
        }

        let csv_span = tracing::info_span!("sourcing CSV", %telemetry_name, ?path);
        let _csv_span_guard = csv_span.enter();

        let mut records: Vec<Telemetry> = vec![];
        let mut reader = csv::Reader::from_path(path)?;

        tracing::debug!("loading records from CSV...");
        for result in reader.deserialize() {
            let record: T = result?;

            //todo: for a source, which is better? config-style conversion via serde (active here) or Into<Telemetry>?
            let telemetry_record = Telemetry::try_from(&record)?;
            // let telemetry_record = record.into();

            records.push(telemetry_record);
        }
        tracing::info!("deserialized {} records from CSV.", records.len());

        let source = stage::Sequence::new(telemetry_name, records);

        Ok(TelemetrySource::new(name, Box::new(source), None))
    } else {
        Err(SettingsError::Bootstrap {
            message: "incompatible setting for local cvs source".to_string(),
            setting: format!("{:?}", setting),
        }
        .into())
    }
}

#[tracing::instrument(
    level="info",
    skip(name, setting),
    fields(stage=%name.as_ref(), ?setting),
)]
pub async fn make_telemetry_rest_api_source<T, S>(
    name: S, setting: &SourceSetting,
) -> Result<TelemetrySource, CollectionError>
where
    //todo: for a source, which is better? config-style conversion via serde (active here) or Into<Telemetry>?
    T: Serialize + DeserializeOwned + Debug,
    // T: Into<Telemetry> + DeserializeOwned + Debug,
    S: AsRef<str>,
{
    if let SourceSetting::RestApi(query) = setting {
        // scheduler
        let tick = stage::Tick::new(
            format!("telemetry_{}_tick", name.as_ref()),
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

                //todo: for a source, which is better? config-style conversion via serde (active here) or Into<Telemetry>?
                let telemetry_record = Telemetry::try_from(&record)?;
                // let telemetry_record: Telemetry = record.into();

                std::result::Result::<Telemetry, CollectionError>::Ok(telemetry_record)
            }
            .map(|d| d.unwrap())
        };

        let collect_telemetry =
            stage::TriggeredGenerator::<_, _, Telemetry>::new(format!("telemetry_{}_gen", name.as_ref()), gen);

        // compose tick & generator into a source shape
        let composite_outlet = collect_telemetry.outlet().clone();
        (tick.outlet(), collect_telemetry.inlet()).connect().await;

        let mut cg = Graph::default();
        cg.push_back(Box::new(tick)).await;
        cg.push_back(Box::new(collect_telemetry)).await;
        let composite: CompositeSource<Telemetry> =
            stage::CompositeSource::new(format!("telemetry_{}", name.as_ref()), cg, composite_outlet).await;
        let composite: Box<dyn SourceStage<Telemetry>> = Box::new(composite);

        Ok(TelemetrySource::new(name, composite, Some(tx_tick_api)))
    } else {
        Err(SettingsError::Bootstrap {
            message: "incompatible setting for rest api source".to_string(),
            setting: format!("{:?}", setting),
        }
        .into())
    }
}
