mod connection;
pub mod topology;

pub(crate) fn lapin_error_eq(e1: &lapin::Error, e2: &lapin::Error) -> bool {
    format!("{e1}") == format!("{e2}")
}