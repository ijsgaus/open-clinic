use async_trait::async_trait;
use lapin::{topology::ExchangeDefinition, Channel};

#[async_trait]
pub trait Topology: Send + Sync {
    async fn apply(&self, ch: &Channel) -> lapin::Result<()>;
    fn name(&self) -> String;
}

pub trait CanBound: Send + Sync {
    fn bound_type() -> &'static str;
}

#[async_trait]
impl Topology for ExchangeDefinition {
    async fn apply(&self, ch: &Channel) -> lapin::Result<()> {
        ch.exchange_declare(
            self.name.as_str(),
            self.kind.clone().unwrap_or_default(),
            self.options.unwrap_or_default(),
            self.arguments.clone().unwrap_or_default(),
        )
        .await?;
        for j in 0..self.bindings.len() {
            let b = &self.bindings[j];
            ch.exchange_bind(
                self.name.as_str(),
                b.source.as_str(),
                b.routing_key.as_str(),
                Default::default(),
                b.arguments.clone(),
            )
            .await?;
        }
        Ok(())
    }

    fn name(&self) -> String {
        let kind = self
            .kind
            .as_ref()
            .map(|v| format!(", {:?}", v))
            .unwrap_or("".to_owned());
        format!("Exchange({}{})", self.name, kind)
    }
}

mod exchange;
pub use exchange::*;
mod binding;
pub use binding::*;
mod queue;
pub use queue::*;
