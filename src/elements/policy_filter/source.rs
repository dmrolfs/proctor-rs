use crate::error::PolicyError;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::io::Read;
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;
use trim_margin::MarginTrimmable;

#[derive(Debug)]
pub enum PolicySourcePath {
    File(PathBuf),
    String(NamedTempFile),
}

impl AsRef<std::path::Path> for PolicySourcePath {
    fn as_ref(&self) -> &Path {
        match self {
            Self::File(path) => path,
            Self::String(tmp) => tmp.path(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", content = "policy", rename_all = "snake_case")]
pub enum PolicySource {
    File(PathBuf),
    String { name: String, policy: String },
}

#[cfg(test)]
assert_impl_all!(PolicySource: serde::ser::Serialize, Sync, Send);

impl PolicySource {
    /// Creates a PolicySource from a string. Multi-line strings must begin with a '|' margin
    /// character in order to facilitate trimming with pleasant alignment. Single-lined strings
    /// do not need to begin with the margin character.
    pub fn from_string<S0, S1>(name: S0, policy: S1) -> Result<Self, PolicyError>
    where
        S0: Into<String>,
        S1: AsRef<str>,
    {
        let lines: Vec<&str> = policy.as_ref().lines().take(2).collect();
        let multi_line = 1 < lines.len();
        let policy_rep = if multi_line {
            policy.trim_margin_with("|").ok_or(PolicyError::StringPolicyError(
                "Multi-line policy strings must begin each line with the '|' margin character.".to_string(),
            ))
        } else {
            Ok(policy.as_ref().to_string())
        };

        policy_rep.map(|policy| Self::String { name: name.into(), policy })
    }

    pub fn from_file(policy_path: impl AsRef<Path>) -> Result<Self, PolicyError> {
        if !policy_path.as_ref().extension().map(|ext| ext == "polar").unwrap_or(false) {
            return Err(oso::OsoError::IncorrectFileType {
                filename: policy_path.as_ref().to_string_lossy().into_owned(),
            })
            .map_err(|err| err.into());
        }

        Ok(Self::File(policy_path.as_ref().to_path_buf()))
    }

    pub fn name(&self) -> Cow<'_, str> {
        match self {
            Self::String { name, policy: _ } => name.into(),
            Self::File(path) => path.file_stem().expect("policy file needs a filename").to_string_lossy(),
        }
    }
}

impl TryInto<String> for PolicySource {
    type Error = PolicyError;

    fn try_into(self) -> Result<String, Self::Error> {
        (&self).try_into()
    }
}

impl TryInto<String> for &PolicySource {
    type Error = PolicyError;

    fn try_into(self) -> Result<String, Self::Error> {
        match self {
            PolicySource::String { name: _, policy } => Ok(policy.clone()),
            PolicySource::File(path) => {
                let mut f = std::fs::File::open(path)?;
                let mut policy = String::new();
                f.read_to_string(&mut policy)?;
                Ok(policy)
            }
        }
    }
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
