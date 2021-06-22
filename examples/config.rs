use std::convert::{TryFrom, TryInto};
use std::path::PathBuf;

use anyhow::Result;
use clap::Clap;
use config::Config;
use proctor::error::SettingsError;
use proctor::settings::{HttpQuery, Settings, SourceSetting};
use proctor::tracing::{get_subscriber, init_subscriber};
use reqwest::Url;

fn main() -> Result<()> {
    let subscriber = get_subscriber("config", "trace");
    init_subscriber(subscriber);

    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    let s1 = Settings {
        sources: maplit::btreemap! {
            "httpbin_cluster".to_string() => SourceSetting::RestApi(HttpQuery {
                interval: std::time::Duration::from_secs(10),
                method: reqwest::Method::GET,
                url: Url::parse("https://httpbin.org/get?is_redeploying=false?last_deployment%3D1979-05-27%2007%3A32%3A00Z")?,
                headers: vec![
                    ("authorization".to_string(), "Basic Zm9vOmJhcg==".to_string()),
                    ("host".to_string(), "example.com".to_string()),
                ],
            }),
            "httpbin_local".to_string() => SourceSetting::RestApi(HttpQuery {
                interval: std::time::Duration::from_secs(17),
                method: reqwest::Method::GET,
                url: Url::parse("https://httpbin.org/get?task_in_failure=false")?,
                headers: vec![
                    ("authorization".to_string(), "Basic Zm9vOmJhcg==".to_string()),
                    ("host".to_string(), "example.com".to_string()),
                ],
            }),
            "local".to_string() => SourceSetting::Csv{path: PathBuf::from("tests/resources/base.csv")},
        },
    };
    tracing::error!("ser json: {:?}", serde_json::to_string_pretty(&s1));

    let _json_str = r#"{
  "sources": {
    "httpbin_local": {
      "type": "RestApi",
      "method": "GET",
      "url": "https://httpbin.org/get?task_in_failure=false",
      "headers": [
        [
          "authorization",
          "Basic Zm9vOmJhcg=="
        ],
        [
          "host",
          "example.com"
        ]
      ]
    },
    "httpbin_cluster": {
      "type": "RestApi",
      "method": "GET",
      "url": "https://httpbin.org/get?is_redeploying=false?last_deployment%3D1979-05-27%2007%3A32%3A00Z",
      "headers": [
        [
          "authorization",
          "Basic Zm9vOmJhcg=="
        ],
        [
          "host",
          "example.com"
        ]
      ]
    },
    "local": {
      "type": "Local",
      "path": "tests/resources/base.csv"
    }
  }
}"#;

    let toml_str = r#"
    [sources.httpbin_local]
type = "RestApi"
interval_secs = 13
method = "GET"
url = "https://httpbin.org/get?task_in_failure=false"
headers = [
  [ "authorization", "Basic Zm9vOmJhcg==" ],
  [ "host", "example.com" ]
]

[sources.httpbin_cluster]
type = "RestApi"
interval_secs = 17
method = "GET"
url = "https://httpbin.org/get?is_redeploying=false?last_deployment%3D1979-05-27%2007%3A32%3A00Z"
headers = [
  [ "authorization", "Basic Zm9vOmJhcg==" ],
  [ "host", "example.com" ]
]

[sources.local]
type = "Local"
path = "tests/resources/base.csv"
    "#;

    let decoded: Result<Settings, SettingsError> = toml::from_str(toml_str).map_err(|err| err.into());
    tracing::warn!(?decoded, "deserialized from string");

    let mut c = config::Config::default();

    let base_path = std::env::current_dir()?;
    let configuration_directory = base_path.join("configuration");
    c.merge(config::File::from(configuration_directory.join("base")).required(true))?;

    let opts = get_command_options()?;
    let c = load_configuration(c, &configuration_directory, &opts)?;
    let mut c = load_secrets(c, &configuration_directory, &opts)?;

    c.merge(config::Environment::with_prefix("app").separator("__"))?;

    let settings: Result<Settings, SettingsError> = c.try_into().map_err(|err| err.into());
    tracing::warn!(?settings, "settings loaded: {}", settings.is_ok());
    Ok(())
}

#[tracing::instrument(level = "info")]
fn load_configuration(mut c: Config, dir: &PathBuf, opts: &CliOptions) -> anyhow::Result<Config> {
    match &opts.config {
        Some(config_path) => {
            let config_path = PathBuf::from(dir.join(config_path));
            c.merge(config::File::from(config_path).required(true))?;
        },
        None => {
            let environment: Environment = std::env::var("APP_ENVIRONMENT")
                .unwrap_or_else(|_| "local".into())
                .try_into()?;
            c.merge(config::File::from(dir.join(environment.as_ref())).required(true))?;
        },
    };

    Ok(c)
}

#[tracing::instrument(level = "info")]
fn load_secrets(mut c: Config, dir: &PathBuf, opts: &CliOptions) -> anyhow::Result<Config> {
    if let Some(secrets_path) = &opts.secrets {
        let secrets_path = PathBuf::from(dir.join(secrets_path));
        c.merge(config::File::from(secrets_path).required(true))?;
    }

    Ok(c)
}

#[tracing::instrument(level = "info")]
fn get_command_options() -> anyhow::Result<CliOptions> {
    let opts = CliOptions::parse();
    tracing::info!(options=?opts.config, secrets=?opts.secrets, "CLI pasred.");
    Ok(opts)
}

#[derive(Clap, Debug)]
#[clap(version = "0.1.0", author = "Damon Rolfs")]
struct CliOptions {
    /// override environment-based configuration file to load.
    /// Default behavior is to load configuration based on `APP_ENVIRONMENT` envvar.
    #[clap(short, long)]
    pub config: Option<String>,
    /// specify path to secrets configuration file
    #[clap(short, long)]
    pub secrets: Option<String>,
}

#[derive(Debug)]
pub enum Environment {
    Local,
    Production,
}

impl AsRef<str> for Environment {
    fn as_ref(&self) -> &str {
        match self {
            Environment::Local => "local",
            Environment::Production => "production",
        }
    }
}

impl TryFrom<String> for Environment {
    type Error = SettingsError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "local" => Ok(Self::Local),
            "production" => Ok(Self::Production),
            other => Err(SettingsError::Environment(format!(
                "do not recognize {} environment.",
                other
            ))),
        }
    }
}
