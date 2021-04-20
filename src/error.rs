use std::fmt;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;

/// A result type where the error variant is always a `ProctorError`.
pub type ProctorResult<T> = std::result::Result<T, ProctorError>;

/// Error variants related to the internals of proctor execution.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProctorError {
    /// There is an error during bootstrapping the application.
    #[error("{0}")]
    Bootstrap(anyhow::Error),
    /// There is an error in the `Collection` stage.
    #[error("{0}")]
    Collection(anyhow::Error),
    /// There is an error in the `Eligibility` stage.
    #[error("{0}")]
    Eligibility(anyhow::Error),
    /// There is an error in the `Decision` stage.
    #[error("{0}")]
    Decision(anyhow::Error),
    /// There is an error in the `Forecast` stage.
    #[error("{0}")]
    Forecast(anyhow::Error),
    /// There is an error in the `Governance` stage.
    #[error("{0}")]
    Governance(anyhow::Error),
    /// There is an error in the `Execution` stage.
    #[error("{0}")]
    Execution(anyhow::Error),
    /// There is an error in the proctor execution pipeline.
    #[error("{0}")]
    Pipeline(anyhow::Error),
    /// An internal proctor error indicating that proctor is shutting down.
    #[error("proctor is shutting down")]
    ShuttingDown,
}

impl From<ConfigError> for ProctorError {
    fn from(that: ConfigError) -> Self {
        ProctorError::Bootstrap(that.into())
    }
}

impl From<GraphError> for ProctorError {
    fn from(that: GraphError) -> Self {
        ProctorError::Pipeline(that.into())
    }
}

impl From<config::ConfigError> for ProctorError {
    fn from(that: config::ConfigError) -> Self {
        ProctorError::Bootstrap(that.into())
    }
}

impl From<tokio::io::Error> for ProctorError {
    fn from(that: tokio::io::Error) -> Self {
        ProctorError::Pipeline(that.into())
    }
}

impl From<toml::de::Error> for ProctorError {
    fn from(that: toml::de::Error) -> Self {
        ProctorError::Bootstrap(that.into())
    }
}

impl From<serde_json::Error> for ProctorError {
    fn from(that: serde_json::Error) -> Self {
        ProctorError::Bootstrap(that.into())
    }
}

impl From<csv::Error> for ProctorError {
    fn from(that: csv::Error) -> Self {
        ProctorError::Bootstrap(that.into())
    }
}

impl<T: fmt::Debug + Send + Sync + 'static> From<mpsc::error::SendError<T>> for ProctorError {
    fn from(that: mpsc::error::SendError<T>) -> Self {
        ProctorError::Pipeline(that.into())
    }
}

impl From<mpsc::error::RecvError> for ProctorError {
    fn from(that: mpsc::error::RecvError) -> Self {
        ProctorError::Pipeline(that.into())
    }
}

impl From<oneshot::error::RecvError> for ProctorError {
    fn from(that: oneshot::error::RecvError) -> Self {
        ProctorError::Pipeline(that.into())
    }
}

/// Error variants related to configuration.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// Error understanding execution environment.
    #[error("{0}")]
    Environment(String),
    /// Error in configuration settings.
    #[error("{0}")]
    Configuration(String),
    /// Error in parsing configuration settings.
    #[error("{0}")]
    Parse(anyhow::Error),
    /// Error in bootstrapping execution from configuration.
    #[error("{0}")]
    Bootstrap(String),
}

impl From<config::ConfigError> for ConfigError {
    fn from(that: config::ConfigError) -> Self {
        ConfigError::Parse(that.into())
    }
}

impl From<toml::de::Error> for ConfigError {
    fn from(that: toml::de::Error) -> Self {
        ConfigError::Parse(that.into())
    }
}

impl From<serde_json::Error> for ConfigError {
    fn from(that: serde_json::Error) -> Self {
        ConfigError::Parse(that.into())
    }
}

impl From<reqwest::header::InvalidHeaderName> for ConfigError {
    fn from(that: reqwest::header::InvalidHeaderName) -> Self {
        ConfigError::Configuration(format!("invalid header name: {:?}", that))
    }
}

impl From<reqwest::header::InvalidHeaderValue> for ConfigError {
    fn from(that: reqwest::header::InvalidHeaderValue) -> Self {
        ConfigError::Configuration(format!("invalid header value: {:?}", that))
    }
}

/// Error variants related to graph execution.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum GraphError {
    /// Attempt to use detached port.
    #[error("Improper attempt to use detached port, {0}")]
    GraphPortDetached(String),
    /// Error occurred in underlying channel.
    #[error("{0}")]
    GraphChannel(String),
    /// Error occurred in underlying asynchronous task.
    #[error("{0}")]
    GraphStage(anyhow::Error),
    /// Error occurred at graph node boundary point.
    #[error("{0}")]
    GraphSerde(String),
    ///Error occurred at graph node boundary point.
    #[error("{0}")]
    GraphBoundary(anyhow::Error),
    ///Precondition error occurred in graph setup.
    #[error("{0}")]
    GraphPrecondition(String),
}

// impl std::error::Error for GraphError {}

// impl fmt::Display for GraphError {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "{}", self.to_string())
//     }
// }

impl From<config::ConfigError> for GraphError {
    fn from(that: config::ConfigError) -> Self {
        GraphError::GraphBoundary(that.into())
    }
}

impl serde::de::Error for GraphError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        GraphError::GraphSerde(msg.to_string())
    }
}

impl serde::ser::Error for GraphError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        GraphError::GraphSerde(msg.to_string())
    }
}

// impl From<std::option::NoneError> for GraphError {
//     fn from(that: std::option::NoneError) -> Self {
//         GraphError::Task(that.into());
//     }
// }

impl From<tokio::sync::oneshot::error::RecvError> for GraphError {
    fn from(that: tokio::sync::oneshot::error::RecvError) -> Self {
        GraphError::GraphChannel(format!("failed to receive oneshot message: {:?}", that))
    }
}

impl From<tokio::sync::mpsc::error::RecvError> for GraphError {
    fn from(that: tokio::sync::mpsc::error::RecvError) -> Self {
        GraphError::GraphChannel(format!("failed to receive mpsc message: {:?}", that))
    }
}

impl From<tokio::sync::broadcast::error::RecvError> for GraphError {
    fn from(that: tokio::sync::broadcast::error::RecvError) -> Self {
        GraphError::GraphChannel(format!("failed to receive broadcast message: {:?}", that))
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for GraphError {
    fn from(that: tokio::sync::mpsc::error::SendError<T>) -> Self {
        //todo: can I make this into anyhow::Error while avoid `T: 'static` requirement?
        GraphError::GraphChannel(format!("failed to send: {}", that))
    }
}

impl<T> From<tokio::sync::broadcast::error::SendError<T>> for GraphError {
    fn from(that: tokio::sync::broadcast::error::SendError<T>) -> Self {
        //todo: can I make this into anyhow::Error while avoid `T: 'static` requirement?
        GraphError::GraphChannel(format!("failed to send: {}", that))
    }
}

impl From<tokio::task::JoinError> for GraphError {
    fn from(that: JoinError) -> Self {
        GraphError::GraphStage(that.into())
    }
}

impl<T> From<std::sync::PoisonError<T>> for GraphError
where
    T: fmt::Debug,
{
    fn from(that: std::sync::PoisonError<T>) -> Self {
        GraphError::GraphChannel(format!("Mutex is poisoned: {:?}", that))
    }
}

impl From<reqwest::Error> for GraphError {
    fn from(that: reqwest::Error) -> Self {
        GraphError::GraphStage(that.into())
    }
}

impl From<serde_json::Error> for GraphError {
    fn from(that: serde_json::Error) -> Self {
        GraphError::GraphStage(that.into())
    }
}

impl From<oso::OsoError> for GraphError {
    fn from(that: oso::OsoError) -> Self {
        GraphError::GraphStage(that.into())
    }
}

impl From<polar_core::error::PolarError> for GraphError {
    fn from(that: polar_core::error::PolarError) -> Self {
        GraphError::GraphStage(that.into())
    }
}

impl From<std::io::Error> for GraphError {
    fn from(that: std::io::Error) -> Self {
        GraphError::GraphStage(that.into())
    }
}

impl From<serde_cbor::Error> for GraphError {
    fn from(that: serde_cbor::Error) -> Self {
        GraphError::GraphBoundary(that.into())
    }
}
