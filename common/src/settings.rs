use std::time::Duration;
use config::{Config, ConfigError, File};
use serde::Deserialize;
use duration_str::deserialize_duration;

#[derive(Debug, Deserialize, Clone)]
pub struct Server {
    pub addr: String
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServiceDiscovery {
    pub name: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub server: Server,
    pub service_discovery: ServiceDiscovery,
    pub consul: Consul
}

#[derive(Debug, Deserialize, Clone)]
pub struct Consul {
    pub address: String,
    pub health_check: ConsulHC
}

#[derive(Debug, Deserialize, Clone)]
pub struct ConsulHC {
    pub name: String,
    pub id: String,
    pub ttl: String,
    #[serde(deserialize_with = "deserialize_duration")]
    pub interval_duration: Duration
}

const CONFIG_FILE_PATH: &str = "./config/Default.toml";

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let settings = Config::builder()
            .add_source(File::with_name(CONFIG_FILE_PATH))
            .build()?;
        settings.try_deserialize()
    }
}