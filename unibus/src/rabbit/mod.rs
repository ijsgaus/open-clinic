use actix::prelude::*;
mod system;
mod connection;


pub use connection::{ ConnectionOptions, ConnectionState, Connection };
pub use system::*;


pub(self) fn lapin_error_eq(e1: &lapin::Error, e2: &lapin::Error) -> bool {
    format!("{e1}") == format!("{e2}")
}



#[derive(Message)]
#[rtype(result = "()")]
pub(self) struct Close;



