use std::sync::Arc;
use consulrs::api::check::common::AgentServiceCheckBuilder;
use consulrs::api::service::requests::RegisterServiceRequest;
use consulrs::client::{ConsulClient, ConsulClientSettingsBuilder};
use crate::Settings;
use tokio::time;
use futures::future::join_all;

pub struct NodeInfo {
    pub node_name: String,
    pub node_address: String,
}

pub struct DiscoveryService<'a> {
    consul_client: Arc<ConsulClient>,
    config: &'a Settings
}

impl<'a> DiscoveryService<'a> {

    pub fn new(config: &Settings) -> DiscoveryService {
        let client =  ConsulClient::new(
            ConsulClientSettingsBuilder::default()
                .address(&config.consul.address)
                .build()
                .unwrap()
        ).unwrap();
        DiscoveryService{consul_client: Arc::new(client), config}
    }

    pub async fn register(&self) {
        let address = format!("http://{}", self.config.server.addr);
        consulrs::service::register(
            &*self.consul_client,
            &self.config.service_discovery.name,
            Some(
                RegisterServiceRequest::builder()
                    .address(&address)
                    .check(
                        AgentServiceCheckBuilder::default()
                            .name(&self.config.consul.health_check.name)
                            .check_id(&self.config.consul.health_check.id)
                            .ttl(&self.config.consul.health_check.ttl)
                            .build()
                            .unwrap(),
                    )
            ),
        ).await.unwrap();
    }

    pub async fn stat_check(&self) {
        let client = Arc::clone(&self.consul_client);
        let mut interval = time::interval(self.config.consul.health_check.interval_duration);
        let check_id = self.config.consul.health_check.id.clone();
        tokio::spawn(async move {
            loop {
                interval.tick().await;
                consulrs::check::pass(&*client, &check_id, None).await.unwrap();
            }
        });
    }

    async fn get_service_address(&self, service_name: &str) -> String {
        let response = consulrs::service::read(&*self.consul_client, service_name, None).await.unwrap();
        response.response.address.expect(&format!("No address for {}", service_name))
    }

    pub async fn get_nodes(&self, connect_to_nodes: Vec<String>) -> Vec<NodeInfo> {
        let futures = connect_to_nodes.into_iter().map(|node| async {
            let address = self.get_service_address(&node).await;

            NodeInfo { node_name: node, node_address: address }
        });
        join_all(futures).await
    }
}