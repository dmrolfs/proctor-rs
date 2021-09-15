use crate::elements::PolicySource;
use crate::Ack;
use tokio::sync::{broadcast, mpsc, oneshot};

pub type PolicyFilterApi<C> = mpsc::UnboundedSender<PolicyFilterCmd<C>>;
pub type PolicyFilterApiReceiver<C> = mpsc::UnboundedReceiver<PolicyFilterCmd<C>>;
pub type PolicyFilterMonitor<T, C> = broadcast::Receiver<PolicyFilterEvent<T, C>>;

#[derive(Debug)]
pub enum PolicyFilterCmd<C> {
    ReplacePolicy {
        new_policy: PolicySource,
        tx: oneshot::Sender<Ack>,
    },
    AppendPolicy {
        additional_policy: PolicySource,
        tx: oneshot::Sender<Ack>,
    },
    Inspect(oneshot::Sender<PolicyFilterDetail<C>>),
}

impl<C> PolicyFilterCmd<C> {
    pub fn replace_policy(new_policy: PolicySource) -> (PolicyFilterCmd<C>, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (Self::ReplacePolicy { new_policy, tx }, rx)
    }

    pub fn append_policy(additional_policy: PolicySource) -> (PolicyFilterCmd<C>, oneshot::Receiver<Ack>) {
        let (tx, rx) = oneshot::channel();
        (Self::AppendPolicy { additional_policy, tx }, rx)
    }

    pub fn inspect() -> (PolicyFilterCmd<C>, oneshot::Receiver<PolicyFilterDetail<C>>) {
        let (tx, rx) = oneshot::channel();
        (Self::Inspect(tx), rx)
    }
}

#[derive(Debug)]
pub struct PolicyFilterDetail<C> {
    pub name: String,
    pub context: Option<C>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PolicyFilterEvent<T, C> {
    ContextChanged(Option<C>),
    ItemPassed,
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
        let ps = PolicySource::from_string("foo");
        assert_tokens(
            &ps,
            &vec![
                Token::Struct { name: "PolicySource", len: 2 },
                Token::Str("source"),
                Token::Str("string"),
                Token::Str("policy"),
                Token::Str("foo"),
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
            PolicySource::from_string("foobar"),
            assert_ok!(PolicySource::from_file(PathBuf::from("./resources/policy.polar"))),
        ];

        let rep = assert_ok!(ron::to_string(&ps));
        assert_eq!(
            rep,
            r#"[(source:"string",policy:"foobar"),(source:"file",policy:"./resources/policy.polar")]"#
        );
    }
}
