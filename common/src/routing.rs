use std::collections::HashMap;

const START_NAME: &'static str = "start";

pub struct StaticRouting {
    routing: HashMap<String, Vec<String>>,
}

impl StaticRouting {
    pub fn init() -> StaticRouting {
        StaticRouting {
            routing: HashMap::from([
                (String::from("start"), vec![String::from("server1"), String::from("server2")]),
                (String::from("server1"), vec![String::from("client")]),
                (String::from("server2"), vec![String::from("client")]),
            ])
        }
    }

    fn get_receiver_rec(&self, current: &str, target: &str) -> Vec<String> {
        let nodes = self.routing.get(current).expect("Empty");
        nodes.iter().flat_map(|node| {
            if node == target { vec![current.to_string()] }
            else {self.get_receiver_rec(node, target, )}
        }).collect()
    }
}

pub trait RoutingService {
    fn connect_to_nodes(&self, service_name: &str) -> Vec<String>;
    fn receive_from_nodes(&self, service_name: &str) -> Vec<String>;
    fn receive_from_scale(&self, service_name: &str) -> HashMap<String, usize> {
        let mut with_scale: HashMap<String, usize> = HashMap::new();
        for node in self.receive_from_nodes(service_name) {
            *with_scale.entry(node).or_insert(0) += 1;
        }
        with_scale
    }
}

impl RoutingService for StaticRouting {

    fn connect_to_nodes(&self, service_name: &str) -> Vec<String> {
        self.routing.get(service_name)
            .expect(&format!("{} service not found in static routing", service_name))
            .to_owned()
    }

    fn receive_from_nodes(&self, service_name: &str) -> Vec<String> {
        self.get_receiver_rec(START_NAME, service_name)
    }
}