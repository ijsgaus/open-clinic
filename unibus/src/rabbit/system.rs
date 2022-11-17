use std::{collections::HashMap, thread};

use actix::prelude::*;

use tokio::sync::{oneshot, watch};
use tracing::{error, info};

use super::{
    connection::{ConnectionActor, GetStateWatch, Connection},
    ConnectionOptions, ConnectionState,
};

#[derive(Default)]
struct RabbitActor;

impl Actor for RabbitActor {
    type Context = Context<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        info!("rabbit client system started");
    }
    fn stopped(&mut self, ctx: &mut Self::Context) {
        System::current().stop();
        info!("rabbit client system stopped");
    }
}

#[derive(Message)]
#[rtype(result = "Addr<ConnectionActor>")]
struct Open(ConnectionOptions);

impl Handler<Open> for RabbitActor {
    type Result = Addr<ConnectionActor>;
    fn handle(&mut self, msg: Open, ctx: &mut Self::Context) -> Self::Result {
        ConnectionActor::new(msg.0).start()
    }
}

impl Handler<super::Close> for RabbitActor {
    type Result = ();
    fn handle(&mut self, _: super::Close, ctx: &mut Self::Context) -> Self::Result {
        ctx.stop();
    }
}

pub struct RabbitClient(Addr<RabbitActor>);

impl Drop for RabbitClient {
    fn drop(&mut self) {
        self.0.do_send(super::Close);
    }
}

impl RabbitClient {
    pub async fn connect(&self, options : ConnectionOptions) -> Result<Connection, MailboxError> {
        let addr = self.0.send(Open(options)).await?;
        Ok(Connection::new(addr))
    }
}

pub async fn start() -> RabbitClient {
    let (tx, rx) = oneshot::channel::<Addr<RabbitActor>>();
    _ = thread::spawn(move || {
        let sys = System::new();
        _ = sys.block_on(async move {
            let addr = RabbitActor {}.start();
            _ = tx.send(addr);
        });
        match sys.run() {
            Ok(_) => info!("system finished"),
            Err(e) => error!(error = format!("{e}"), "system finished"),
        };
    });
    RabbitClient(rx.await.unwrap())
}
