use crate::error::PolicyError;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

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
    String(String),
}

#[cfg(test)]
assert_impl_all!(PolicySource: serde::ser::Serialize, Sync, Send);

impl PolicySource {
    pub fn from_string<S: Into<String>>(policy: S) -> Self {
        Self::String(policy.into())
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

    pub fn source_path(&self) -> PolicySourcePath {
        match self {
            Self::File(pathbuf) => PolicySourcePath::File(pathbuf.clone()),
            Self::String(policy) => {
                let tempdir =
                    std::env::current_dir().expect("no current directory to store temporary string policy files");
                // let tempdir = std::env::temp_dir().expect("no temp directory to store string policy file");

                let mut tmp = tempfile::Builder::new()
                    .prefix("policy_str_")
                    .rand_bytes(4)
                    .suffix(".polar")
                    .tempfile_in(tempdir.clone())
                    .expect(format!("failed to create string policy tempfile at {:?}", tempdir).as_str());

                write!(tmp.as_file_mut(), "{}", policy)
                    .expect(format!("failed to write policy into tempfile: {:?}", self).as_str());

                PolicySourcePath::String(tmp)
            }
        }
    }

    pub fn validate(&self) -> Result<(), PolicyError> {
        let polar = polar_core::polar::Polar::new();
        let policy: String = self.clone().into();
        polar.load_str(policy.as_str())?;
        Ok(())
    }
}

impl Into<String> for PolicySource {
    fn into(self) -> String {
        match self {
            Self::String(policy) => policy,
            Self::File(path) => {
                let mut f = match std::fs::File::open(path) {
                    Ok(file) => file,
                    Err(err) => panic!("failed to open policy file: {:?}", err),
                };
                let mut policy = String::new();
                f.read_to_string(&mut policy)
                    .expect("failed to read policy file into string.");
                policy
            }
        }
    }
}
