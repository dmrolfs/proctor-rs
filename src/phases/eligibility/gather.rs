// use crate::graph::{stage, ThroughShape};
// use crate::phases::collection::TelemetryData;
// use super::EligibilityContext;
//
// pub trait TelemetryIntoEligibilityContext: Stage + ThroughShape<>
// #[tracing::instrument(level="info", skip(), fields(),)]
// pub fn make_telemetry_into_eligibility<F>() ->
// where
//     F: FnMut(TelemetryData) -> Option<EligibilityContext> + Send,
// {
//     let op= |data: TelemetryData| {
//         let c = config::Config::default().merge(data);
//         let context: Option<EligibilityContext> = match c {
//             Ok(c) => {
//                 let result = c.try_into::<EligibilityContext>();
//                 let foo = result.ok();
//                 foo
//             },
//             Err(err) => {
//                 tracing::error!(error=?err, telemetry=?data, "failed to prep telemetry for eligibility conversion");
//                 None
//             },
//         };
//         context
//     };
//
//     let map = stage::Map::new(
//         "telemetry_to_eligibility_context",
//         op
//     );
//     map
// }
