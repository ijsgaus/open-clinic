use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};

use async_trait::async_trait;
use lapin::{
    types::{FieldTable, LongString},
    ConnectionProperties,
};
use tokio::{
    sync::{watch, RwLock},
    time::{sleep, Duration},
};

use super::topology::Topology;
use tracing::{error, info, trace, warn};

/// connection state
#[derive(Debug, Clone)]
pub enum ConnectionState {
    /// starting state
    Unknown,
    /// connected to RabbitMq and ready to work
    Ready,
    /// error connection to RabbitMq, wait for reconnect
    Fail(lapin::Error),
    /// connection is closed
    Closed,
}

impl Display for ConnectionState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionState::Closed => write!(f, "CLOSED"),
            ConnectionState::Fail(e) => write!(f, "ERROR: {}", e),
            ConnectionState::Ready => write!(f, "READY"),
            ConnectionState::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

impl PartialEq<ConnectionState> for ConnectionState {
    fn eq(&self, other: &ConnectionState) -> bool {
        match self {
            Self::Unknown => match other {
                Self::Unknown => true,
                _ => false,
            },
            Self::Ready => match other {
                Self::Ready => true,
                _ => false,
            },
            Self::Closed => match other {
                Self::Closed => true,
                _ => false,
            },
            Self::Fail(e1) => match other {
                Self::Fail(e2) => super::lapin_error_eq(e1, e2),
                _ => false,
            },
        }
    }
}

#[derive(Default)]
struct State {
    state: inner::State,
    state_watcher: Option<watch::Sender<ConnectionState>>,
    uri: String,
    reconnect: Duration,
    props: ConnectionProperties,
    topology: Vec<Box<dyn Topology>>,
}

impl Drop for State {
    fn drop(&mut self) {
        self.set_inner_state(inner::State::Closed);
        trace!(rabbit = self.uri, "connection dropped");
    }
}

impl State {
    fn set_inner_state(&mut self, new: inner::State) {
        if self.state == new {
            return;
        }

        match &new {
            inner::State::Fail(err) => error!(
                rabbit = self.uri,
                error = format!("{err}"),
                "connect error, retry in {:?}",
                self.reconnect
            ),
            inner::State::Ready(_) => warn!(rabbit = self.uri, "connected"),
            inner::State::Closed => warn!(rabbit = self.uri, "closed"),
            inner::State::Unknown => (),
        };

        if let Some(watcher) = &self.state_watcher {
            if !watcher.is_closed() {
                watcher.send_replace((&new).into());
            }
        }
        self.state = new;
    }
}

#[async_trait]
trait Connector {
    async fn connect(&self) -> bool;
    async fn reconnect(self);
}

#[async_trait]
impl Connector for Arc<RwLock<State>> {
    async fn connect(&self) -> bool {
        let mut state = self.write().await;
        match state.state {
            inner::State::Closed => true,
            inner::State::Ready(_) => true,
            _ => match lapin::Connection::connect(&state.uri, state.props.clone()).await {
                Ok(c) => {
                    if state.topology.len() > 0 {
                        match c.create_channel().await {
                            Ok(ch) => {
                                async fn apply(
                                    v: &Vec<Box<dyn Topology>>,
                                    ch: &lapin::Channel,
                                    uri: &str,
                                ) -> lapin::Result<()> {
                                    for t in v {
                                        match t.apply(ch).await {
                                            Ok(_) => info!(rabbit = uri, "created: {}", t.name()),
                                            Err(e) => {
                                                error!(
                                                    rabbit = uri,
                                                    error = format!("{e}"),
                                                    "on create: {}",
                                                    t.name()
                                                );
                                                return Err(e);
                                            }
                                        };
                                    }
                                    Ok(())
                                }
                                match apply(&state.topology, &ch, &state.uri).await {
                                    Ok(_) => (),
                                    Err(e) => {
                                        state.set_inner_state(inner::State::Fail(e));
                                        return false;
                                    }
                                }
                            }
                            Err(e) => {
                                state.set_inner_state(inner::State::Fail(e));
                                return false;
                            }
                        }
                    }
                    let handle = tokio::runtime::Handle::current();
                    let st = Arc::downgrade(self);
                    c.on_error(move |e| {
                        let st = st.clone();
                        handle.spawn(async move {
                            let st = st.upgrade();
                            if let Some(st) = st {
                                {
                                    let mut state = st.write().await;
                                    state.set_inner_state(inner::State::Fail(e));
                                }
                                if st.connect().await {
                                    return;
                                }

                                tokio::spawn(async move { st.reconnect().await });
                            }
                        });
                    });
                    state.set_inner_state(inner::State::Ready(c));
                    return true;
                }
                Err(e) => {
                    state.set_inner_state(inner::State::Fail(e));
                    false
                }
            },
        }
    }

    async fn reconnect(self) {
        let reconnect = {
            let state = self.read().await;
            state.reconnect
        };
        tokio::spawn(async move {
            loop {
                sleep(reconnect).await;
                if self.connect().await {
                    return;
                }
            }
        });
    }
}

#[derive(Clone)]
pub struct Connection(Arc<RwLock<State>>);

impl Connection {
    pub async fn uri(&self) -> String {
        self.0.read().await.uri.clone()
    }

    pub async fn state_watcher(&self) -> watch::Receiver<ConnectionState> {
        let mut state = self.0.write().await;
        if let Some(w) = &state.state_watcher {
            return w.subscribe();
        }
        let (tx, rx) = watch::channel((&state.state).into());
        state.state_watcher = Some(tx);
        rx
    }

    pub fn ref_count(&self) -> usize {
        Arc::strong_count(&self.0)
    }

    pub async fn close(self, reply_code: u16, reply_text: &str) -> Result<(), lapin::Error> {
        let mut state = self.0.write().await;
        if let inner::State::Ready(c) = &state.state {
            c.close(reply_code, reply_text).await?;
        }
        state.set_inner_state(inner::State::Closed);
        Ok(())
    }

    pub async fn create_channel(&self) -> Result<lapin::Channel, ConnectionError> {
        let state = self.0.read().await;
        if let inner::State::Ready(c) = &state.state {
            return c
                .create_channel()
                .await
                .map_err(|e| ConnectionError::Fail(e));
        }
        Err((&state.state).try_into().unwrap())
    }
}

pub struct ConnectionBuilder {
    uri: String,
    reconnect: Duration,
    props: ConnectionProperties,
    topology: Vec<Box<dyn Topology>>,
}

impl ConnectionBuilder {
    #[cfg(target_family = "unix")]
    pub fn new(uri: String) -> Self {
        let props = ConnectionProperties::default()
            .with_executor(tokio_executor_trait::Tokio::current())
            .with_reactor(tokio_reactor_trait::Tokio::default());
        Self {
            uri,
            reconnect: Duration::from_secs(2),
            props,
            topology: Default::default(),
        }
    }

    #[cfg(target_family = "windows")]
    pub fn new(uri: String) -> Self {
        let props =
            ConnectionProperties::default().with_executor(tokio_executor_trait::Tokio::current());
        Self {
            uri,
            reconnect: Duration::from_secs(2),
            props,
            topology: Default::default(),
        }
    }

    pub fn with_reconnect(mut self, interval: Duration) -> Self {
        self.reconnect = interval;
        self
    }

    pub fn with_props(mut self, props: FieldTable) -> Self {
        self.props.client_properties = props;
        self
    }

    pub fn with_locale(mut self, locale: String) -> Self {
        self.props.locale = locale;
        self
    }

    pub fn with_connection_name(mut self, connection_name: LongString) -> Self {
        self.props = self.props.with_connection_name(connection_name);
        self
    }

    pub fn with_topology(mut self, topology: Vec<Box<dyn Topology>>) -> Self {
        self.topology = topology;
        self
    }

    pub async fn build(self) -> Connection {
        let lock = Arc::new(RwLock::new(State {
            props: self.props,
            reconnect: self.reconnect,
            state: Default::default(),
            state_watcher: None,
            topology: self.topology,
            uri: self.uri,
        }));
        warn!("builder {}", Arc::strong_count(&lock));
        {
            let clone = lock.clone();
            tokio::spawn(async move {
                if clone.connect().await {
                    return;
                }
                clone.reconnect().await;
            });
        }
        Connection(lock)
    }
}

#[derive(Debug, thiserror::Error, Clone)]
pub enum ConnectionError {
    #[error("AMQP error: {}", .0)]
    Fail(#[from] lapin::Error),
    #[error("bunny connection closed")]
    Closed,
    #[error("bunny connection not started")]
    Unknown,
}

impl TryFrom<&inner::State> for ConnectionError {
    type Error = ();
    fn try_from(value: &inner::State) -> core::result::Result<Self, Self::Error> {
        match value {
            inner::State::Closed => Ok(ConnectionError::Closed),
            inner::State::Fail(err) => Ok(ConnectionError::Fail(err.clone())),
            inner::State::Ready(_) => Err(()),
            inner::State::Unknown => Ok(ConnectionError::Unknown),
        }
    }
}

impl PartialEq<ConnectionError> for ConnectionError {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Self::Fail(e1) => match other {
                Self::Fail(e2) => return super::lapin_error_eq(e1, e2),
                _ => return false,
            },
            Self::Closed => match other {
                Self::Closed => return true,
                _ => return false,
            },
            Self::Unknown => match other {
                Self::Unknown => return true,
                _ => return false,
            },
        }
    }
}

mod inner {
    use super::ConnectionState;
    use crate::rabbit::lapin_error_eq;

    #[derive(Debug)]
    pub(super) enum State {
        Unknown,
        Ready(lapin::Connection),
        Fail(lapin::Error),
        Closed,
    }

    impl Default for State {
        fn default() -> Self {
            State::Unknown
        }
    }

    impl PartialEq<State> for State {
        fn eq(&self, other: &State) -> bool {
            match self {
                Self::Unknown => match other {
                    Self::Unknown => true,
                    _ => false,
                },
                Self::Closed => match other {
                    Self::Closed => true,
                    _ => false,
                },
                Self::Fail(e1) => match other {
                    Self::Fail(e2) => lapin_error_eq(e1, e2),
                    _ => false,
                },
                Self::Ready(c1) => match other {
                    Self::Ready(c2) => c1 as *const _ == c2 as *const _,
                    _ => false,
                },
            }
        }
    }

    impl Into<ConnectionState> for &State {
        fn into(self) -> ConnectionState {
            match self {
                State::Closed => ConnectionState::Closed,
                State::Fail(err) => ConnectionState::Fail(err.clone()),
                State::Ready(_) => ConnectionState::Ready,
                State::Unknown => ConnectionState::Unknown,
            }
        }
    }
}
