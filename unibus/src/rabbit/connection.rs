use std::{fmt::{Display, Formatter}, time::Duration};

use lapin::ConnectionProperties;
use tokio::sync::watch;

use super::topology::Topology;

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

    impl Into<ConnectionState> for State {
        fn into(self) -> ConnectionState {
            match self {
                Self::Closed => ConnectionState::Closed,
                Self::Fail(err) => ConnectionState::Fail(err.clone()),
                Self::Ready(_) => ConnectionState::Ready,
                Self::Unknown => ConnectionState::Unknown,
            }
        }
    }
}
