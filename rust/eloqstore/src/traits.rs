use super::{error::KvError, response::Response, store::EloqStore};

pub trait Request {
    type Response: Response;

    fn execute(&self, store: &EloqStore) -> Result<Self::Response, KvError>;
}
