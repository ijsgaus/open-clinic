use tokio::sync::watch;
use tracing::{info, trace_span, Span, warn, error };
use actix::prelude::*;

use super::{ConnectionState, ConnectionOptions};

enum State {
    None,
    Ready(lapin::Connection),
    Error(lapin::Error),
}

impl Into<ConnectionState> for &State {
    fn into(self) -> ConnectionState {
        match (self) {
            State::None => ConnectionState::None,
            State::Ready(_) => ConnectionState::Ready,
            State::Error(e) => ConnectionState::Error(e.clone()),
        }
    }
}



pub struct ConnectionActor {
    state: State,
    options: ConnectionOptions,
    state_subject: watch::Sender<ConnectionState>,
}

impl Drop for ConnectionActor {
    fn drop(&mut self) {
        let span = self.make_span();
        let _e = span.enter();
        info!("dropped");
    }
}

impl ConnectionActor {
    fn make_span(&self) -> Span {
        trace_span!("rabbit", name = self.options.name)
    }

    fn set_state(&mut self, state: State) {
        let span = self.make_span();
        let _e = span.enter();
        //let _st = span.enter();

        let old_state: ConnectionState = (&self.state).into();
        if old_state != (&state).into() {
            match &state {
                State::None => {}
                State::Error(e) => error!(error = format!("{e}"), "connection error"),
                State::Ready(_) => warn!("connected"),
            };
            self.state_subject.send_replace((&state).into());
        }

        self.state = state;
    }

    pub fn new(options: ConnectionOptions) -> Self {
        let (tx, _) = watch::channel(ConnectionState::None);
        ConnectionActor {
            state: State::None,
            options,
            state_subject: tx,
        }
    }
}

impl Actor for ConnectionActor {
    type Context = Context<ConnectionActor>;
    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.notify(Connect);
    }

    fn stopping(&mut self, ctx: &mut Self::Context) -> Running {
        let state = std::mem::replace(&mut self.state, State::None);
        match state {
            State::Ready(c) => {
                _ = ctx.spawn(
                    async move {
                        _ = c.close(0, "connection closed").await;
                    }
                    .into_actor(self),
                )
            }
            _ => (),
        };
        Running::Stop
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct Connect;

impl Handler<Connect> for ConnectionActor {
    type Result = ResponseActFuture<Self, ()>;
    fn handle(&mut self, msg: Connect, ctx: &mut Self::Context) -> Self::Result {
        match &self.state {
            State::Ready(_) => Box::pin(async {}.into_actor(self).map(|_, _, _| ())),
            _ => {
                let uri = self.options.uri.clone();
                let props = (&self.options).into();
                Box::pin(
                    async move { lapin::Connection::connect(&uri, props).await }
                        .into_actor(self)
                        .map(|res, mut act, ctx| {
                            match res {
                                Ok(c) => {
                                    let this = ctx.address();
                                    c.on_error(move |e| {
                                        this.do_send(Disconnected(e));
                                    });
                                    act.set_state(State::Ready(c));
                                }
                                Err(e) => {
                                    act.set_state(State::Error(e));
                                    let this = ctx.address();
                                    let wait = act.options.reconnect;
                                    tokio::spawn(async move {
                                        tokio::time::sleep(wait).await;
                                        this.do_send(Connect);
                                    });
                                }
                            };
                        }),
                )
            }
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct Disconnected(lapin::Error);

impl Handler<Disconnected> for ConnectionActor {
    type Result = ();
    fn handle(&mut self, msg: Disconnected, ctx: &mut Self::Context) -> Self::Result {
        self.set_state(State::Error(msg.0));
        ctx.address().do_send(Connect);
    }
}

#[derive(Message)]
#[rtype(result = "watch::Receiver<ConnectionState>")]
pub struct GetStateWatch;

impl Handler<GetStateWatch> for ConnectionActor {
    type Result = MessageResult<GetStateWatch>;
    fn handle(&mut self, _: GetStateWatch, _: &mut Self::Context) -> Self::Result {
        MessageResult(self.state_subject.subscribe())
    }
}
