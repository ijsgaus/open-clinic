use crate::rabbit::lapin_error_eq;

#[derive(Clone, Debug)]
pub enum ConnectionState {
    None,
    Ready,
    Error(lapin::Error),
}

impl PartialEq for ConnectionState {
    fn eq(&self, other: &Self) -> bool {
        match self {
            ConnectionState::None => {
                if let ConnectionState::None = other {
                    true
                } else {
                    false
                }
            }
            ConnectionState::Ready => {
                if let ConnectionState::Ready = other {
                    true
                } else {
                    false
                }
            }
            ConnectionState::Error(e1) => {
                if let ConnectionState::Error(e2) = other {
                    lapin_error_eq(e1, e2)
                } else {
                    false
                }
            }
        }
    }
}