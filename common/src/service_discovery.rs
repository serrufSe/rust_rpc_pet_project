use std::collections::HashMap;
use serde_json;
use std::fs;

const PATH: &str = "../service_discovery.json";

pub fn sd_from_file() -> HashMap<String, String> {
    let data = fs::read_to_string(PATH).expect("SD file read error");
    serde_json::from_str(&data).expect("DS file parsing error")
}