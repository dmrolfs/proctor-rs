pub use model::*;

mod model;

use crate::error::SettingsError;
use clap::Clap;
use std::convert::{TryFrom, TryInto};
use std::path::PathBuf;

pub fn get_settings() -> Result<Settings, SettingsError> {
    let mut settings = config::Config::default();

    let base_path = std::env::current_dir()?;
    let configuration_directory = base_path.join("configuration");
    settings.merge(config::File::from(configuration_directory.join("base")).required(true))?;

    let opts = get_command_options()?;
    match opts.config {
        Some(config_path) => {
            let config_path = PathBuf::from(config_path);
            settings.merge(config::File::from(config_path).required(true))?;
        }

        None => {
            let environment: Environment = std::env::var("APP_ENVIRONMENT")
                .unwrap_or_else(|_| "local".into())
                .try_into()?;
            settings.merge(config::File::from(configuration_directory.join(environment.as_ref())).required(true))?;
        }
    };

    // Add in settings from environment variables (with a prefix of APP and '__' as separator)
    // E.g. `APP_APPLICATION__PORT=5001 would set `Settings.application.port`
    settings.merge(config::Environment::with_prefix("app").separator("__"))?;

    settings.try_into().map_err(|err| err.into())
}

fn get_command_options() -> Result<CliOptions, SettingsError> {
    let opts = CliOptions::parse();
    tracing::info!(option=?opts.config, secrets=%opts.secrets, "CLI parsed");
    Ok(opts)
}

#[derive(Clap)]
#[clap(version = "0.1.0", author = "Damon Rolfs")]
struct CliOptions {
    /// override environment-based configuration file to load.
    /// Default behavior is to load configuration based on `APP_ENVIRONMENT` envvar.
    #[clap(short, long)]
    config: Option<String>,
    /// specify path to secrets configuration file
    #[clap(short, long)]
    secrets: String,
}

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
