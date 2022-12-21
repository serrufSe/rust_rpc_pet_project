use std::collections::HashMap;

pub struct StaticRouting {
    routing: HashMap<String, Vec<String>>,
}

impl StaticRouting {
    pub fn init() -> StaticRouting {
        StaticRouting {
            routing: HashMap::from([
                (String::from("start"), vec![String::from("server1")]),
                (String::from("server1"), vec![String::from("server2"), String::from("client")]),
                (String::from("server2"), vec![String::from("client")]),
            ])
        }
    }
}

pub trait RoutingService {
    fn connect_to_nodes(&self, service_name: &str) -> Vec<String>;
    fn receive_from_nodes(&self, service_name: &str) -> Vec<String>;
}

impl RoutingService for StaticRouting {
    fn connect_to_nodes(&self, service_name: &str) -> Vec<String> {
        self.routing.get(service_name)
            .expect(&format!("{} service not found in static routing", service_name))
            .to_owned()
    }

    fn receive_from_nodes(&self, service_name: &str) -> Vec<String> {
        self.routing.iter().filter_map(|(key, value)| {
            if value.contains(&service_name.to_string()) {
                Some(key.clone())
            } else { None }
        }).collect::<Vec<String>>()
    }
}