mod actor;
mod options;
mod state;
use actix::{Addr, MailboxError};
pub(super) use actor::{ConnectionActor, GetStateWatch};
pub use options::*;
pub use state::*;
use tokio::sync::watch;


pub struct Connection(Addr<ConnectionActor>);

impl Connection {
    pub(super) fn new(addr: Addr<ConnectionActor>) -> Self {
        Connection(addr)
    }

    pub async fn state_watcher(&self) -> Result<watch::Receiver<ConnectionState>, MailboxError> {
        self.0.send(GetStateWatch).await
    }
}