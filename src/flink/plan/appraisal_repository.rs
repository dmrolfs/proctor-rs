use std::fmt::{Debug, Display};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};

use crate::error::PlanError;
use crate::flink::plan::Appraisal;

pub fn make_appraisal_repository(settings: AppraisalSettings) -> Result<Box<dyn AppraisalRepository>, PlanError> {
    match settings.storage {
        AppraisalRepositoryType::Memory => Ok(Box::new(MemoryAppraisalRepository::default())),
        AppraisalRepositoryType::File => {
            let path = settings.storage_path.unwrap_or("appraisals.data".to_string());
            Ok(Box::new(FileAppraisalRepository::new(path)))
        },
    }
}


#[async_trait]
pub trait AppraisalRepository: Debug + Sync + Send {
    async fn load(&mut self, name: &str) -> Result<Appraisal, PlanError>;
    async fn save(&mut self, name: &str, appraisal: &Appraisal) -> Result<(), PlanError>;
    async fn close(self: Box<Self>) -> Result<(), PlanError>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AppraisalSettings {
    pub storage: AppraisalRepositoryType,
    pub storage_path: Option<String>,
}

#[derive(Debug, Display, SerializeDisplay, DeserializeFromStr)]
pub enum AppraisalRepositoryType {
    Memory,
    File,
}

impl FromStr for AppraisalRepositoryType {
    type Err = PlanError;

    fn from_str(rep: &str) -> Result<Self, Self::Err> {
        match rep.to_lowercase().as_str() {
            "memory" => Ok(AppraisalRepositoryType::Memory),
            "file" => Ok(AppraisalRepositoryType::File),
            s => Err(PlanError::ParseError(format!(
                "unknown appraisal repository type, {}",
                s
            ))),
        }
    }
}


#[derive(Debug, Default)]
pub struct MemoryAppraisalRepository(Arc<DashMap<String, Appraisal>>);

impl MemoryAppraisalRepository {}

#[async_trait]
impl AppraisalRepository for MemoryAppraisalRepository {
    #[tracing::instrument(level = "info", skip(self))]
    async fn load(&mut self, name: &str) -> Result<Appraisal, PlanError> {
        let result = if self.0.contains_key(name) {
            self.0.get(name).unwrap().clone()
        } else {
            let appraisal = Appraisal::default();
            self.save(name, &appraisal).await?;
            appraisal
        };

        Ok(result)
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn save(&mut self, name: &str, appraisal: &Appraisal) -> Result<(), PlanError> {
        let old = self.0.insert(name.to_string(), appraisal.clone());
        tracing::debug!(?old, "replacing appraisal in repository.");
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn close(mut self: Box<Self>) -> Result<(), PlanError> {
        self.0.clear();
        Ok(())
    }
}

#[derive(Debug)]
pub struct FileAppraisalRepository {
    root_path: PathBuf,
}

impl FileAppraisalRepository {
    pub fn new(root: impl AsRef<str>) -> Self {
        let mut root_path = PathBuf::from(root.as_ref());
        root_path.push("/");
        Self { root_path }
    }

    fn path_for(&self, filename: &str) -> PathBuf {
        let mut path = self.root_path.clone();
        path.push(filename);
        path.into()
    }

    fn file_for(&self, filename: &str) -> Result<File, PlanError> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(self.path_for(filename))?;

        Ok(f)
    }
}

#[async_trait]
impl AppraisalRepository for FileAppraisalRepository {
    async fn load(&mut self, name: &str) -> Result<Appraisal, PlanError> {
        let f = self.file_for(name)?;
        let reader = BufReader::new(f);
        let appraisal = serde_json::from_reader(reader)?;
        Ok(appraisal)
    }

    async fn save(&mut self, name: &str, appraisal: &Appraisal) -> Result<(), PlanError> {
        let f = self.file_for(name)?;
        let writer = BufWriter::new(f);
        serde_json::to_writer(writer, appraisal)?;
        Ok(())
    }

    async fn close(self: Box<Self>) -> Result<(), PlanError> {
        Ok(())
    }
}
