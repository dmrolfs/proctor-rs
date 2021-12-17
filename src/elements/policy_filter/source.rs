use std::borrow::Cow;
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use trim_margin::MarginTrimmable;

use crate::error::PolicyError;

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
    File {
        path: PathBuf,
        #[serde(default)]
        is_template: bool,
    },
    String {
        name: String,
        polar: String,
        #[serde(default)]
        is_template: bool,
    },
}

#[cfg(test)]
assert_impl_all!(PolicySource: serde::ser::Serialize, Sync, Send);

impl PolicySource {
    /// Creates a PolicySource from a complete polar string. Multi-line strings must begin with a
    /// '|' margin character in order to facilitate trimming with pleasant alignment. Single-lined
    /// strings do not need to begin with the margin character.
    pub fn from_complete_string<S0, S1>(name: S0, polar: S1) -> Result<Self, PolicyError>
    where
        S0: Into<String>,
        S1: AsRef<str>,
    {
        Self::from_string(name, false, polar)
    }

    /// Creates a PolicySource from a polar string template. Multi-line strings must begin with a
    /// '|' margin character in order to facilitate trimming with pleasant alignment. Single-lined
    /// strings do not need to begin with the margin character.
    pub fn from_template_string<S0, S1>(name: S0, polar: S1) -> Result<Self, PolicyError>
    where
        S0: Into<String>,
        S1: AsRef<str>,
    {
        Self::from_string(name, true, polar)
    }

    /// Creates a PolicySource from a string. Multi-line strings must begin with a '|' margin
    /// character in order to facilitate trimming with pleasant alignment. Single-lined strings
    /// do not need to begin with the margin character.
    pub fn from_string<S0, S1>(name: S0, is_template: bool, polar: S1) -> Result<Self, PolicyError>
    where
        S0: Into<String>,
        S1: AsRef<str>,
    {
        #[allow(clippy::needless_collect)]
        let lines: Vec<&str> = polar.as_ref().lines().take(2).collect();
        let multi_line = 1 < lines.len();
        let polar_rep = if multi_line {
            polar.trim_margin_with("|").ok_or_else(|| {
                PolicyError::StringPolicyError(
                    "Multi-line policy strings must begin each line with the '|' margin character.".to_string(),
                )
            })
        } else {
            Ok(polar.as_ref().to_string())
        };

        polar_rep.map(|polar| Self::String { name: name.into(), polar, is_template })
    }

    pub fn from_complete_file(polar_path: impl AsRef<Path>) -> Result<Self, PolicyError> {
        Self::from_file(polar_path, false)
    }

    pub fn from_template_file(polar_path: impl AsRef<Path>) -> Result<Self, PolicyError> {
        Self::from_file(polar_path, true)
    }

    pub fn from_file(polar_path: impl AsRef<Path>, is_template: bool) -> Result<Self, PolicyError> {
        if !polar_path.as_ref().extension().map(|ext| ext == "polar").unwrap_or(false) {
            return Err(oso::OsoError::IncorrectFileType {
                filename: polar_path.as_ref().to_string_lossy().into_owned(),
            })
            .map_err(|err| err.into());
        }

        Ok(Self::File {
            path: polar_path.as_ref().to_path_buf(),
            is_template,
        })
    }

    pub fn name(&self) -> Cow<'_, str> {
        match self {
            Self::String { name, polar: _, is_template: _ } => name.into(),
            Self::File { path, is_template: _ } => {
                path.file_stem().expect("policy file needs a filename").to_string_lossy()
            },
        }
    }

    pub fn is_template(&self) -> bool {
        match self {
            Self::File { path: _, is_template } => *is_template,
            Self::String { name: _, polar: _, is_template } => *is_template,
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
            PolicySource::String { name: _, polar, is_template: _ } => Ok(polar.clone()),
            PolicySource::File { path, is_template: _ } => {
                let mut f = std::fs::File::open(path)?;
                let mut policy = String::new();
                f.read_to_string(&mut policy)?;
                Ok(policy)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use claim::assert_ok;
    use pretty_assertions::assert_eq;
    use serde_test::{assert_tokens, Token};

    use super::*;

    // assert_impl_all!(PolicyFilterCmd<C>: Sync, Send);
    // assert_impl_all!(PolicyFilterDetail: Sync, Send);
    // assert_impl_all!(PolicyFilterEvent: Sync, Send);

    #[test]
    fn test_serde_policy_source() {
        let ps = assert_ok!(PolicySource::from_complete_string("template_name", "foo"));
        assert_tokens(
            &ps,
            &vec![
                Token::Struct { name: "PolicySource", len: 2 },
                Token::Str("source"),
                Token::Str("string"),
                Token::Str("policy"),
                Token::Struct { name: "string", len: 3 },
                Token::Str("name"),
                Token::Str("template_name"),
                Token::Str("polar"),
                Token::Str("foo"),
                Token::Str("is_template"),
                Token::Bool(false),
                Token::StructEnd,
                Token::StructEnd,
            ],
        );

        let ps = assert_ok!(PolicySource::from_complete_string(
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
                Token::Struct { name: "string", len: 3 },
                Token::Str("name"),
                Token::Str("template_name"),
                Token::Str("polar"),
                Token::Str(
                    r##"foobar
zed"##,
                ),
                Token::Str("is_template"),
                Token::Bool(false),
                Token::StructEnd,
                Token::StructEnd,
            ],
        );

        let ps = assert_ok!(PolicySource::from_template_file(PathBuf::from(
            "./resources/policy.polar"
        )));
        assert_tokens(
            &ps,
            &vec![
                Token::Struct { name: "PolicySource", len: 2 },
                Token::Str("source"),
                Token::Str("file"),
                Token::Str("policy"),
                Token::Struct { name: "file", len: 2 },
                Token::Str("path"),
                Token::Str("./resources/policy.polar"),
                Token::Str("is_template"),
                Token::Bool(true),
                Token::StructEnd,
                Token::StructEnd,
            ],
        );
    }

    #[test]
    fn test_serde_ron_policy_source() {
        let ps = vec![
            assert_ok!(PolicySource::from_string("template", false, "foobar")),
            assert_ok!(PolicySource::from_file(PathBuf::from("./resources/policy.polar"), true)),
        ];

        let rep = assert_ok!(ron::to_string(&ps));
        assert_eq!(
            rep,
            r#"[(source:"string",policy:(name:"template",polar:"foobar",is_template:false)),(source:"file",policy:(path:"./resources/policy.polar",is_template:true))]"#
        );
    }
}
