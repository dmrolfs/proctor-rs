// use crate::AppData;
// use chrono::{DateTime, Utc};
//
// #[derive(Debug, PartialEq, Clone)]
// pub struct Metric<Q>
// where
//     Q: AppData + PartialEq + Clone,
// {
//     pub name: String,
//     pub quantity: Q,
//     pub timestamp: DateTime<Utc>,
// }
//
// impl<Q> Metric<Q>
// where
//     Q: AppData + PartialEq + Clone,
// {
//     pub fn new<S: Into<String>>(name: S, quantity: Q) -> Self {
//         Self {
//             name: name.into(),
//             quantity,
//             timestamp: Utc::now(),
//         }
//     }
// }
//
// impl<Q> Into<f32> for Metric<Q>
// where
//     Q: AppData + PartialEq + Clone + Into<f32>,
// {
//     fn into(self) -> f32 {
//         self.quantity.into()
//     }
// }
