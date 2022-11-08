use std::fmt::Display;

use async_trait::async_trait;
use lapin::{
    options::ExchangeDeclareOptions,
    types::{AMQPValue, FieldTable},
    Channel,
    {types::ShortString, ExchangeKind},
};

use super::{Binding, CanBound, Topology};

#[derive(Debug, Clone)]
pub struct ExchangePassive(ShortString);

impl ExchangePassive {
    pub fn new(name: impl Into<ShortString>) -> Self {
        ExchangePassive(name.into())
    }
}

impl Display for ExchangePassive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Exchange({}) as passive", self.0)
    }
}

#[async_trait]
impl Topology for ExchangePassive {
    fn name(&self) -> String {
        format!("{}", self)
    }

    async fn apply(&self, ch: &Channel) -> lapin::Result<()> {
        ch.exchange_declare(
            self.0.as_str(),
            ExchangeKind::Custom("".into()),
            ExchangeDeclareOptions {
                passive: false,
                ..Default::default()
            },
            Default::default(),
        )
        .await
    }
}

#[derive(Debug, Clone)]
pub struct Exchange {
    name: ShortString,
    kind: ExchangeKind,
    _auto_delete: bool,
    durable: bool,
    _internal: bool,
    _alternate_exchange: Option<ShortString>,
    bindings: Vec<Binding<Exchange>>,
}

impl Display for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "exchange({}, {:?}", self.name, self.kind)?;
        if self.durable {
            write!(f, ", durable")?;
        }
        if self._auto_delete {
            write!(f, ", auto-delete")?;
        }
        if self._internal {
            write!(f, ", internal")?;
        }
        if let Some(v) = &self._alternate_exchange {
            write!(f, ", alternate-exchange = {}", v)?;
        }
        if self.bindings.len() > 0 {
            write!(f, ", bound to {}", self.bindings.len())?;
        }
        write!(f, ")")
    }
}

impl CanBound for Exchange {
    fn bound_type() -> &'static str {
        "exchange"
    }
}

impl Exchange {
    pub fn new(name: impl Into<ShortString>, kind: ExchangeKind) -> Self {
        Exchange {
            name: name.into(),
            kind,
            _auto_delete: false,
            durable: true,
            _internal: false,
            _alternate_exchange: None,
            bindings: Default::default(),
        }
    }

    pub fn non_durable(mut self) -> Self {
        self.durable = false;
        self
    }

    pub fn auto_delete(mut self) -> Self {
        self._auto_delete = true;
        self
    }

    pub fn internal(mut self) -> Self {
        self._internal = true;
        self
    }

    pub fn alternate_exchange(mut self, alternate_exchange: impl Into<ShortString>) -> Self {
        self._alternate_exchange = Some(alternate_exchange.into());
        self
    }

    pub fn add_binding<F>(mut self, source: impl Into<ShortString>, f: F) -> Self
    where
        F: FnOnce(Binding<Exchange>) -> Binding<Exchange>,
    {
        self.bindings.push(f(Binding::<Exchange>::new(
            source.into(),
            self.name.as_str(),
        )));
        self
    }
}

#[async_trait]
impl Topology for Exchange {
    async fn apply(&self, ch: &Channel) -> lapin::Result<()> {
        ch.exchange_declare(
            self.name.as_str(),
            self.kind.clone(),
            ExchangeDeclareOptions {
                passive: false,
                durable: self.durable,
                auto_delete: self._auto_delete,
                internal: self._internal,
                nowait: false,
            },
            self._alternate_exchange
                .as_ref()
                .map(|ax| {
                    let mut ft = FieldTable::default();
                    ft.insert(
                        "alternate-exchange".into(),
                        AMQPValue::ShortString(ax.clone()),
                    );
                    ft
                })
                .unwrap_or_default(),
        )
        .await?;

        for b in &self.bindings {
            b.apply(ch).await?
        }
        Ok(())
    }

    fn name(&self) -> String {
        format!("{self}")
    }
}

#[test]
fn check_builder() {
    let ex = Exchange::new("test", ExchangeKind::Direct)
        .non_durable()
        .auto_delete()
        .alternate_exchange("456")
        .add_binding("789", |b| b.routing_key("uir"));
    assert_eq!(ex.bindings.len(), 1);
    assert_eq!(
        format!("{ex}"),
        "Exchange(test, Direct, auto-delete, alternate-exchange = 456, bound to 1)"
    );
}
