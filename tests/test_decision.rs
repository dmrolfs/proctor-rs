mod fixtures;

use lazy_static::lazy_static;
use oso::{Oso, PolarClass, ToPolar};
use proctor::elements::{PolicySource, PolicySubscription};
use proctor::flink::decision::context::FlinkDecisionContext;
use proctor::phases::collection::TelemetrySubscription;
use proctor::{AppData, ProctorContext};
use std::collections::HashSet;
use std::marker::PhantomData;

#[derive(Debug)]
struct TestDecisionPolicy<D> {
    custom_fields: Option<HashSet<String>>,
    policy: PolicySource,
    data_marker: PhantomData<D>,
}

impl<D> TestDecisionPolicy<D> {
    pub fn new(policy: PolicySource) -> Self {
        policy.validate().expect("failed to parse policy");
        Self {
            custom_fields: None,
            policy,
            data_marker: PhantomData,
        }
    }

    pub fn with_custom(self, custom_fields: HashSet<String>) -> Self {
        Self {
            custom_fields: Some(custom_fields),
            ..self
        }
    }
}

impl<D: AppData> PolicySubscription for TestDecisionPolicy<D> {
    type Context = FlinkDecisionContext;

    fn do_extend_subscription(&self, subscription: TelemetrySubscription) -> TelemetrySubscription {
        todo!()
    }
}
