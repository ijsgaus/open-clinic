use std::{fmt::Display, marker::PhantomData};

use async_trait::async_trait;
use lapin::{
    types::{FieldTable, ShortString},
    Channel,
};

use super::{CanBound, Exchange, Topology, Queue};

#[derive(Clone, Debug)]
pub struct Binding<T: CanBound> {
    source: ShortString,
    target: ShortString,
    _routing_key: Option<ShortString>,
    _arguments: Option<FieldTable>,
    kind: PhantomData<T>,
}

impl<T: CanBound> Display for Binding<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> {}({})", self.source, T::bound_type(), self.target)?;
        if let Some(v) = &self._routing_key {
            write!(f, ", routing key: {v}")?;
        }
        if let Some(v) = &self._arguments {
            write!(f, ", arguments: {v:?}")?;
        }
        Ok(())
    }
}

impl<T: CanBound> Binding<T> {
    pub fn new(source: impl Into<ShortString>, target: impl Into<ShortString>) -> Self {
        Self {
            source: source.into(),
            target: target.into(),
            _routing_key: None,
            _arguments: None,
            kind: Default::default(),
        }
    }

    pub fn routing_key(mut self, routing_key: impl Into<ShortString>) -> Self {
        self._routing_key = Some(routing_key.into());
        self
    }

    pub fn arguments(mut self, arguments: FieldTable) -> Self {
        self._arguments = Some(arguments);
        self
    }

    pub fn unbind(self) -> Unbind<T> {
        Unbind(self)
    }
}

#[async_trait]
impl Topology for Binding<Exchange> {
    fn name(&self) -> String {
        format!("{self}")
    }

    async fn apply(&self, ch: &Channel) -> lapin::Result<()> {
        ch.exchange_bind(
            self.target.as_str(),
            self.source.as_str(),
            self._routing_key.clone().unwrap_or_default().as_str(),
            Default::default(),
            self._arguments.clone().unwrap_or_default(),
        )
        .await
    }
}

#[async_trait]
impl Topology for Binding<Queue> {
    fn name(&self) -> String {
        format!("{self}")
    }

    async fn apply(&self, ch: &Channel) -> lapin::Result<()> {
        ch.queue_bind(
            self.target.as_str(),
            self.source.as_str(),
            self._routing_key.clone().unwrap_or_default().as_str(),
            Default::default(),
            self._arguments.clone().unwrap_or_default(),
        )
        .await
    }
}

#[derive(Clone, Debug)]
pub struct Unbind<T: CanBound>(Binding<T>);

impl<T: CanBound> Display for Unbind<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[async_trait]
impl Topology for Unbind<Exchange> {
    fn name(&self) -> String {
        format!("unbind: {}", self.0)
    }

    async fn apply(&self, ch: &Channel) -> lapin::Result<()> {
        ch.exchange_unbind(
            self.0.target.as_str(),
            self.0.source.as_str(),
            self.0._routing_key.clone().unwrap_or_default().as_str(),
            Default::default(),
            self.0._arguments.clone().unwrap_or_default(),
        )
        .await
    }
}

#[async_trait]
impl Topology for Unbind<Queue> {
    fn name(&self) -> String {
        format!("unbind: {}", self.0)
    }

    async fn apply(&self, ch: &Channel) -> lapin::Result<()> {
        ch.queue_unbind(
            self.0.target.as_str(),
            self.0.source.as_str(),
            self.0._routing_key.clone().unwrap_or_default().as_str(),
            self.0._arguments.clone().unwrap_or_default(),
        )
        .await
    }
}
