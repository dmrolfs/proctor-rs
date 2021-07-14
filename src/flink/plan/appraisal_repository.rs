use std::fmt::{Debug, Display};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::{PathBuf, Path};
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};

use crate::error::PlanError;
use crate::flink::plan::Appraisal;
use serde_json::error::Category;

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
    async fn load(&self, job_name: &str) -> Result<Option<Appraisal>, PlanError>;
    async fn save(&mut self, job_name: &str, appraisal: &Appraisal) -> Result<(), PlanError>;
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
    async fn load(&self, job_name: &str) -> Result<Option<Appraisal>, PlanError> {
        let appraisal = self.0.get(job_name).map(|a| a.clone());
        tracing::debug!(?appraisal, "memory loaded appraisal.");
        Ok(appraisal)
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn save(&mut self, job_name: &str, appraisal: &Appraisal) -> Result<(), PlanError> {
        let old = self.0.insert(job_name.to_string(), appraisal.clone());
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
        Self { root_path }
    }

    fn file_name_for(&self, job_name: &str) -> String {
        format!("{}.json", job_name)
    }

    #[tracing::instrument(level = "info")]
    fn path_for(&self, filename: &str) -> PathBuf {
        let mut path = self.root_path.clone();
        path.push(filename);
        tracing::debug!(?path, "looking for appraisal repository at file path.");
        path
    }

    #[tracing::instrument(level = "info", skip(path), fields(path=?path.as_ref()))]
    fn file_for(&self, path: impl AsRef<Path>, read_write: bool) -> Result<File, std::io::Error> {
        let mut options = OpenOptions::new();
        options.read(true);
        if read_write { options.write(true).create(true); }
        Ok(options.open(path)?)
    }
}

#[async_trait]
impl AppraisalRepository for FileAppraisalRepository {
    #[tracing::instrument(level = "info", skip(self))]
    async fn load(&self, job_name: &str) -> Result<Option<Appraisal>, PlanError> {
        let appraisal_history_path = self.path_for(self.file_name_for(job_name).as_str());
        let appraisal_history = self.file_for(appraisal_history_path.clone(), false);
        tracing::debug!(?appraisal_history, "file_for: {}", job_name);

        match appraisal_history {
            Ok(history_file) => {
                let mut reader = BufReader::new(history_file);
                let appraisal = match serde_json::from_reader(reader) {
                    Ok(a) => Ok(Some(a)),
                    Err(err) if err.classify() == Category::Eof => {
                        tracing::debug!(?appraisal_history_path, "appraisal history empty, creating new.");
                        Ok(None)
                    },
                    Err(err) => Err(err),
                };
                tracing::debug!(?appraisal, "file loaded appraisal.");

                Ok(appraisal?)
            },
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn save(&mut self, job_name: &str, appraisal: &Appraisal) -> Result<(), PlanError> {
        let appraisal_history_path = self.path_for(self.file_name_for(job_name).as_str());
        let appraisal_history = self.file_for(appraisal_history_path.clone(), true)?;
        let writer = BufWriter::new(appraisal_history);
        serde_json::to_writer(writer, appraisal)?;
        tracing::debug!(?appraisal_history_path, "saved appraisal data");
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn close(self: Box<Self>) -> Result<(), PlanError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::{assert_eq};
    use claim::{assert_none, assert_some, assert_ok};
    use tokio_test::block_on;
    use crate::flink::plan::Benchmark;
    use crate::flink::plan::benchmark::BenchmarkRange;
    use chrono::format::Item;
    use tokio::sync::Mutex;

    async fn do_test_repository<'a>( repo: &mut impl AppraisalRepository, jobs_a_b: (&'a str, &'a str), ) -> anyhow::Result<()> {
        let (name_a, name_b) = jobs_a_b;
        let actual = repo.load(name_a).await;
        let actual = assert_ok!(actual);
        assert_none!(actual);

        let mut appraisal = Appraisal::default();
        appraisal.add_upper_benchmark(Benchmark::new(4, 3.5.into()));
        let actual = repo.save(name_a, &appraisal).await;
        assert_ok!(actual);

        appraisal.add_upper_benchmark(Benchmark::new(4, 21.3.into()));
        appraisal.add_upper_benchmark(Benchmark::new(12, 37.324.into()));
        let actual = repo.save(name_b, &appraisal).await;
        assert_ok!(actual);

        let actual_a = repo.load(name_a).await;
        let actual_a = assert_ok!(actual_a);
        let actual_a = assert_some!(actual_a);
        let mut expected = Appraisal::default();
        expected.add_upper_benchmark(Benchmark::new(4, 3.5.into()));
        assert_eq!(actual_a, expected);

        let actual_b = repo.load(name_b).await;
        let actual_b = assert_ok!(actual_b);
        let actual_b = assert_some!(actual_b);
        let mut expected = Appraisal::default();
        expected.add_upper_benchmark(Benchmark::new(4, 21.3.into()));
        expected.add_upper_benchmark(Benchmark::new(12, 37.324.into()));
        assert_eq!(actual_b, expected);

        let actual = repo.load("dummy").await;
        let actual = assert_ok!(actual);
        assert_none!(actual);

        Ok(())
    }

    #[test]
    fn test_memory_appraisal_repository() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_memory_appraisal_repository");
        let _main_span_guard = main_span.enter();

        let mut repo = MemoryAppraisalRepository::default();
        block_on(async {
            let test_result = do_test_repository(&mut repo, ("AAA", "BBB")).await;
            assert_ok!(test_result);

            let actual = Box::new(repo).close().await;
            assert_ok!(actual);
            Ok(())
        })
    }

    #[test]
    fn test_file_appraisal_repository() -> anyhow::Result<()> {
        lazy_static::initialize(&crate::tracing::TEST_TRACING);
        let main_span = tracing::info_span!("test_file_appraisal_repository");
        let _main_span_guard = main_span.enter();

        let aaa = "AAA";
        let bbb = "BBB";
        let mut repo = FileAppraisalRepository::new("./target");
        let (aaa_path, bbb_path) = block_on( async {
            (
                repo.path_for(repo.file_name_for(aaa).as_str()),
            repo.path_for(repo.file_name_for(bbb).as_str())
                )
        });

        block_on(async {
            let result = do_test_repository(&mut repo, (aaa, bbb)).await;
            assert_ok!(result);

            let actual = Box::new(repo).close().await;
            assert_ok!(actual);
        });

        std::fs::remove_file(aaa_path)?;
        std::fs::remove_file(bbb_path)?;
        Ok(())
    }
}