use config::{Config, ConfigError, File};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Server {
    pub addr: String
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub server: Server
}

const CONFIG_FILE_PATH: &str = "./config/Default.toml";

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let settings = Config::builder().add_source(config::File::with_name(CONFIG_FILE_PATH)).build()?;
        settings.try_deserialize()
    }
}