use super::MetricLabel;
use either::{Either, Left};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("IO failed: {0}")]
    IO(#[from] std::io::Error),

    #[error("failed to parse yaml: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("failed to parse json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("failed to parse ron: {0}")]
    Ron(#[from] ron::Error),

    #[error("unknown metric const_labels format: {0}")]
    UnknownFormat(String),
}

impl MetricLabel for MetricsError {
    fn slug(&self) -> String {
        "metrics".into()
    }

    fn next(&self) -> Either<String, Box<&dyn MetricLabel>> {
        match self {
            Self::IO(_) => Left("io".into()),
            Self::Yaml(_) => Left("yaml".into()),
            Self::Json(_) => Left("json".into()),
            Self::Ron(_) => Left("ron".into()),
            Self::UnknownFormat(_) => Left("unknown_format".into()),
        }
    }
}
