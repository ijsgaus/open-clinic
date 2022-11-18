use std::time::Duration;

use lapin::types::FieldTable;

pub struct ConnectionOptions {
    pub uri: String,
    pub name: String,
    pub reconnect: Duration,
    //pub topology: Vec<Box<dyn Topology>>,
    pub locale: String,
    pub properties: FieldTable,
}

impl Into<lapin::ConnectionProperties> for &ConnectionOptions {
    #[cfg(target_family = "unix")]
    fn into(self) -> lapin::ConnectionProperties {
        use std::sync::Arc;

        use lapin::ConnectionProperties;

        ConnectionProperties {
            locale: self.locale.clone(),
            client_properties: self.properties.clone(),
            executor: Some(Arc::new(tokio_executor_trait::Tokio::current())),
            reactor: Some(Arc::new(tokio_reactor_trait::Tokio)),
        }
    }

    #[cfg(target_family = "windows")]
    fn into(self) -> ConnectionProperties {
        ConnectionProperties {
            locale: self.locale.clone(),
            client_properties: self.properties.clone(),
            executor: Some(Arc(tokio_executor_trait::Tokio::current())),
            reactor: None,
        }
    }
}

impl ConnectionOptions {
    pub fn new(uri: impl Into<String>, name: impl Into<String>) -> Self {
        ConnectionOptions {
            uri: uri.into(),
            name: name.into(),
            reconnect: Duration::from_secs(3),
            //topology: Default::default(),
            locale: "en-US".to_owned(),
            properties: Default::default(),
        }
    }

    pub fn with_reconnect(mut self, reconnect: Duration) -> Self {
        self.reconnect = reconnect;
        self
    }

    pub fn with_locale(mut self, locale: impl Into<String>) -> Self {
        self.locale = locale.into();
        self
    }

    pub fn with_props(mut self, props: FieldTable) -> Self {
        self.properties = props;
        self
    }

    // pub fn with_topology(mut self, topology: Vec<Box<dyn Topology>>) -> Self {
    //     self.topology = topology;
    //     self
    // }

    // pub fn add_topology(mut self, topology: impl Topology + 'static) -> Self {
    //     self.topology.push(Box::new(topology));
    //     self
    // }
}