use crate::elements::PolicySource;
use crate::Ack;
use std::fmt::Debug;
use tokio::sync::{broadcast, mpsc, oneshot};

pub type PolicyFilterApi<C, D> = mpsc::UnboundedSender<PolicyFilterCmd<C, D>>;
pub type PolicyFilterApiReceiver<C, D> = mpsc::UnboundedReceiver<PolicyFilterCmd<C, D>>;
pub type PolicyFilterMonitor<T, C> = broadcast::Receiver<PolicyFilterEvent<T, C>>;

#[derive(Debug)]
pub enum PolicyFilterCmd<C, D> {
    ReplacePolicies {
        new_policies: Vec<PolicySource>,
        new_template_data: Option<D>,
        tx: oneshot::Sender<Ack>,
    },
    AppendPolicy {
        additional_policy: PolicySource,
        new_template_data: Option<D>,
        tx: oneshot::Sender<Ack>,
    },
    Inspect(oneshot::Sender<PolicyFilterDetail<C, D>>),
}

impl<C, D> PolicyFilterCmd<C, D> {
    pub fn replace_policies(
        new_policies: impl IntoIterator<Item = PolicySource>, new_template_data: Option<D>,
    ) -> (PolicyFilterCmd<C, D>, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        let new_policies = new_policies.into_iter().collect();
        (Self::ReplacePolicies { new_policies, new_template_data, tx }, rx)
    }

    pub fn append_policy(
        additional_policy: PolicySource, new_template_data: Option<D>,
    ) -> (PolicyFilterCmd<C, D>, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (Self::AppendPolicy { additional_policy, new_template_data, tx }, rx)
    }

    pub fn inspect() -> (PolicyFilterCmd<C, D>, oneshot::Receiver<PolicyFilterDetail<C, D>>) {
        let (tx, rx) = oneshot::channel();
        (Self::Inspect(tx), rx)
    }
}

#[derive(Debug)]
pub struct PolicyFilterDetail<C, D> {
    pub name: String,
    pub context: Option<C>,
    pub policy_template_data: Option<D>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PolicyFilterEvent<T, C> {
    ContextChanged(Option<C>),
    ItemPassed(T),
    ItemBlocked(T),
}

#[cfg(test)]
mod tests {
    use super::*;
    use claim::assert_ok;
    use pretty_assertions::assert_eq;
    use serde_test::{assert_tokens, Token};
    use std::path::PathBuf;

    // assert_impl_all!(PolicyFilterCmd<C>: Sync, Send);
    // assert_impl_all!(PolicyFilterDetail: Sync, Send);
    // assert_impl_all!(PolicyFilterEvent: Sync, Send);

    #[test]
    fn test_serde_policy_source() {
        let ps = assert_ok!(PolicySource::from_string("template_name", "foo"));
        assert_tokens(
            &ps,
            &vec![
                Token::Struct { name: "PolicySource", len: 2 },
                Token::Str("source"),
                Token::Str("string"),
                Token::Str("policy"),
                Token::Struct { name: "string", len: 2 },
                Token::Str("name"),
                Token::Str("template_name"),
                Token::Str("policy"),
                Token::Str("foo"),
                Token::StructEnd,
                Token::StructEnd,
            ],
        );

        let ps = assert_ok!(PolicySource::from_string(
            "template_name",
            r##"
            |foobar
            |zed
            "##
        ));
        assert_tokens(
            &ps,
            &vec![
                Token::Struct { name: "PolicySource", len: 2 },
                Token::Str("source"),
                Token::Str("string"),
                Token::Str("policy"),
                Token::Struct { name: "string", len: 2 },
                Token::Str("name"),
                Token::Str("template_name"),
                Token::Str("policy"),
                Token::Str(
                    r##"foobar
zed"##,
                ),
                Token::StructEnd,
                Token::StructEnd,
            ],
        );

        let ps = assert_ok!(PolicySource::from_file(PathBuf::from("./resources/policy.polar")));
        assert_tokens(
            &ps,
            &vec![
                Token::Struct { name: "PolicySource", len: 2 },
                Token::Str("source"),
                Token::Str("file"),
                Token::Str("policy"),
                Token::Str("./resources/policy.polar"),
                Token::StructEnd,
            ],
        );
    }

    #[test]
    fn test_serde_ron_policy_source() {
        let ps = vec![
            assert_ok!(PolicySource::from_string("template", "foobar")),
            assert_ok!(PolicySource::from_file(PathBuf::from("./resources/policy.polar"))),
        ];

        let rep = assert_ok!(ron::to_string(&ps));
        assert_eq!(
            rep,
            r#"[(source:"string",policy:(name:"template",policy:"foobar")),(source:"file",policy:"./resources/policy.polar")]"#
        );
    }
}
