use std::collections::HashSet;
use std::convert::TryFrom;
use std::path::PathBuf;

use ::serde::{Deserialize, Serialize};
use anyhow::Result;
use proctor::elements::Telemetry;
use proctor::error::TelemetryError;
use proctor::graph::{stage, Connect, Graph, SinkShape, SourceShape};
use proctor::phases::sense::clearinghouse::TelemetryCacheSettings;
use proctor::phases::sense::{self, Sense, SensorSetting};
use proctor::phases::DataSet;
use proctor::tracing::{get_subscriber, init_subscriber};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Data {
    pub pos: Option<usize>,
    pub value: Option<f64>,
    pub cat: String,
}

impl Default for Data {
    fn default() -> Self {
        Self { pos: None, value: None, cat: "".to_string() }
    }
}

impl Into<Telemetry> for Data {
    fn into(self) -> Telemetry {
        let mut telemetry = Telemetry::default();
        telemetry.insert("pos".to_string(), self.pos.into());
        telemetry.insert("value".to_string(), self.value.into());
        telemetry.insert("cat".to_string(), self.cat.into());
        telemetry
    }
}

impl TryFrom<Telemetry> for Data {
    type Error = TelemetryError;

    fn try_from(telemetry: Telemetry) -> Result<Self, Self::Error> {
        let pos = telemetry.get("pos").map(|val| usize::try_from(val.clone())).transpose()?;

        let value = telemetry.get("value").map(|val| f64::try_from(val.clone())).transpose()?;

        let cat = telemetry
            .get("cat")
            .map(|val| String::try_from(val.clone()))
            .transpose()?
            .unwrap_or(String::default());

        Ok(Self { pos, value, cat })
    }
}

const POS_FIELD: &str = "pos";
const _VALUE_FIELD: &str = "value";
const _CAT_FIELD: &str = "cat";

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() -> Result<()> {
    let subscriber = get_subscriber("sandbox", "trace", std::io::stdout);
    init_subscriber(subscriber);

    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    let base_path = std::env::current_dir()?;
    let cvs_path = base_path.join(PathBuf::from("tests/data/cats.csv"));
    let cvs_setting = SensorSetting::Csv { path: cvs_path };
    let mut cvs_source = sense::make_telemetry_cvs_sensor::<Data, _>("cvs", &cvs_setting)?;
    let cvs_stage = cvs_source.stage.take().unwrap();

    let pos_stats_fields = maplit::hashset! { POS_FIELD.to_string() };
    let collect = Sense::single_node_builder("collect", vec![cvs_stage], &TelemetryCacheSettings::default())
        .await
        .build_for_telemetry_out(pos_stats_fields.clone(), HashSet::<String>::default())
        .await?;

    let mut pos_stats =
        stage::Fold::<_, DataSet<Telemetry>, (usize, usize)>::new("pos_stats", (0, 0), move |(count, sum), data| {
            let data = data.into_inner();
            let delivered = data.keys().cloned().collect::<HashSet<_>>();
            let allowed = &pos_stats_fields;
            let unexpected = delivered.difference(allowed).collect::<HashSet<_>>();
            if !unexpected.is_empty() {
                tracing::error!(?unexpected, "fields delivered beyond allowed.");
                panic!("fields fields beyond allowed: {:?}", unexpected);
            }
            let pos = usize::try_from(data.get(POS_FIELD).unwrap().clone()).unwrap();
            (count + 1, sum + pos)
        });
    let rx_pos_stats = pos_stats.take_final_rx().unwrap();
    (collect.outlet(), pos_stats.inlet()).connect().await;

    let mut g = Graph::default();
    g.push_back(Box::new(collect)).await;
    g.push_back(Box::new(pos_stats)).await;

    g.run().await?;

    let (actual_count, actual_sum) = rx_pos_stats.await?;
    assert_eq!(actual_count, 3);
    assert_eq!(actual_sum, 8);

    Ok(())
}
