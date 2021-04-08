use crate::elements::TelemetryData;
use crate::error::{ConfigError, GraphError, ProctorError};
use crate::graph::stage::{tick, Stage, WithApi};
use crate::graph::{stage, Connect, Graph, GraphResult, SinkShape, SourceShape};
use crate::settings::SourceSetting;
use crate::ProctorResult;
use futures::future::FutureExt;
use serde::de::DeserializeOwned;
use std::time::Duration;
use tokio::sync::mpsc;

//todo api access should follow new convention rt via returned tuple
pub type TelemetrySource = Box<dyn TelemetrySourceStage>;
pub trait TelemetrySourceStage: Stage + SourceShape<Out = TelemetryData> + 'static {}
impl<T: 'static + Stage + SourceShape<Out = TelemetryData>> TelemetrySourceStage for T {}

/// Naive CVS source, which loads a `.cvs` file, then publishes via its outlet.
///
/// An improvement to consider is to behave lazily and iterate through the source file(s)
/// upon downstream demand.
#[tracing::instrument(
    level="info",
    skip(name, setting),
    fields(name=%name.as_ref(), ?setting),
)]
pub fn make_telemetry_cvs_source<S>(name: S, setting: &SourceSetting) -> ProctorResult<TelemetrySource>
where
    S: AsRef<str>,
{
    if let SourceSetting::Csv { path } = setting {
        let mut records: Vec<TelemetryData> = vec![];
        let mut name = format!("telemetry_{}", name.as_ref());

        if let Some(file_name) = path.file_name() {
            match file_name.to_str() {
                None => (),
                Some(file_name) => name.push_str(format!("_{}", file_name).as_str()),
            }
        }

        let mut reader = csv::Reader::from_path(path).map_err::<ProctorError, _>(|err| err.into())?;

        for data in reader.deserialize::<TelemetryData>() {
            let mut data = data.map_err::<ProctorError, _>(|err| err.into())?;
            data.retain(|_, v| !v.is_empty());
            records.push(data);
        }
        let source = stage::Sequence::new(name, records);

        Ok(Box::new(source))
    } else {
        Err(ConfigError::Bootstrap(format!("incompatible setting for local cvs source: {:?}", setting)).into())
    }
}

#[tracing::instrument(
    level="info",
    skip(name, setting),
    fields(name=%name.as_ref(), ?setting),
)]
pub async fn make_telemetry_rest_api_source<D, S>(
    name: S, setting: &SourceSetting,
) -> ProctorResult<(TelemetrySource, mpsc::UnboundedSender<tick::TickMsg>)>
where
    D: DeserializeOwned + Into<TelemetryData>,
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
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .map_err::<GraphError, _>(|err| err.into())?;
        let method = query.method.clone();
        let url = query.url.clone();

        // let mut gen : dyn FnMut(()) -> dyn futures::future::Future<Output=SpringlineResult<TelemetryData>> = move |_| {
        let gen = move |_: ()| {
            let client = client.clone();
            let method = method.clone();
            let url = url.clone();

            async move {
                let resp = client
                    // .clone()
                    .request(method.clone(), url.clone())
                    .send()
                    .await
                    .map_err::<GraphError, _>(|err| err.into())?
                    .json::<D>()
                    .await
                    .map_err::<GraphError, _>(|err| err.into())?;

                let data: GraphResult<TelemetryData> = Ok(resp.into());
                data
            }
            .map(|d| d.unwrap())
        };

        let collect_telemetry =
            stage::TriggeredGenerator::<_, _, TelemetryData>::new(format!("telemetry_{}_gen", name.as_ref()), gen);

        // compose tick & generator into a source shape
        let composite_outlet = collect_telemetry.outlet().clone();
        (tick.outlet(), collect_telemetry.inlet()).connect().await;

        let mut cg = Graph::default();
        cg.push_back(Box::new(tick)).await;
        cg.push_back(Box::new(collect_telemetry)).await;
        let composite = stage::CompositeSource::new(format!("telemetry_{}", name.as_ref()), cg, composite_outlet).await;
        let composite: Box<dyn TelemetrySourceStage> = Box::new(composite);

        Ok((composite, tx_tick_api))
    } else {
        Err(ProctorError::Bootstrap(
            ConfigError::Bootstrap(format!("incompatible setting for rest api source: {:?}", setting)).into(),
        ))
    }
}
