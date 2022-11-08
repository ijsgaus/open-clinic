use std::{fmt::Display, time::Duration};

use lapin::{
    options::QueueDeclareOptions,
    types::{FieldTable, ShortString},
    Channel,
};

use super::{Binding, CanBound, Topology};
use async_trait::async_trait;

#[derive(Clone, Debug)]
pub struct QueuePassive(ShortString);

impl Display for QueuePassive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "queue({}, passive)", self.0)
    }
}

#[async_trait]
impl Topology for QueuePassive {
    fn name(&self) -> String {
        format!("{}", self)
    }

    async fn apply(&self, ch: &Channel) -> lapin::Result<()> {
        ch.queue_declare(
            self.0.as_str(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            Default::default(),
        )
        .await
        .map(|_| ())
    }
}

#[derive(Clone, Debug)]
pub struct Queue {
    name: ShortString,
    durable: bool,
    _exclusive: bool,
    _auto_delete: bool,
    _message_ttl: Option<Duration>,
    _expires: Option<Duration>,
    _max_priority: Option<u8>,
    _max_length: Option<i64>,
    _dead_letter_exchange: Option<ShortString>,
    _dead_letter_routing_key: Option<ShortString>,
    bindings: Vec<Binding<Queue>>,
}

impl Queue {
    pub fn new(name: impl Into<ShortString>) -> Self {
        Queue {
            name: name.into(),
            durable: true,
            _exclusive: false,
            _auto_delete: false,
            _message_ttl: None,
            _expires: None,
            _max_priority: None,
            _max_length: None,
            _dead_letter_exchange: None,
            _dead_letter_routing_key: None,
            bindings: Default::default(),
        }
    }

    pub fn non_durable(mut self) -> Self {
        self.durable = false;
        self
    }

    pub fn exclusive(mut self) -> Self {
        self._exclusive = true;
        self
    }

    pub fn auto_delete(mut self) -> Self {
        self._auto_delete = true;
        self
    }

    pub fn message_ttl(mut self, message_ttl: Duration) -> Self {
        self._message_ttl = Some(message_ttl);
        self
    }

    pub fn expires(mut self, expires: Duration) -> Self {
        self._expires = Some(expires);
        self
    }

    pub fn max_priority(mut self, max_priority: u8) -> Self {
        self._max_priority = Some(max_priority);
        self
    }

    pub fn max_length(mut self, max_length: i64) -> Self {
        self._max_length = Some(max_length);
        self
    }

    pub fn dead_letter_exchange(mut self, dead_letter_exchange: impl Into<ShortString>) -> Self {
        self._dead_letter_exchange = Some(dead_letter_exchange.into());
        self
    }

    pub fn dead_letter_routing_key(
        mut self,
        dead_letter_routing_key: impl Into<ShortString>,
    ) -> Self {
        self._dead_letter_routing_key = Some(dead_letter_routing_key.into());
        self
    }

    pub fn add_binding<F>(mut self, source: impl Into<ShortString>, f: F) -> Self
    where
        F: FnOnce(Binding<Queue>) -> Binding<Queue>,
    {
        self.bindings
            .push(f(Binding::<Queue>::new(source.into(), self.name.as_str())));
        self
    }
}

impl Display for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "queue({}", self.name)?;
        if self.durable {
            write!(f, ", durable")?;
        }
        write!(f, ")")
    }
}

impl CanBound for Queue {
    fn bound_type() -> &'static str {
        "queue"
    }
}

#[async_trait]
impl Topology for Queue {
    fn name(&self) -> String {
        format!("{}", self)
    }

    async fn apply(&self, ch: &Channel) -> lapin::Result<()> {
        let mut arguments: FieldTable = Default::default();
        if let Some(ttl) = self._message_ttl {
            if let Ok(v) = ttl.as_millis().try_into() {
                arguments.insert(
                    "x-message-ttl".into(),
                    lapin::types::AMQPValue::LongLongInt(v),
                );
            }
        }
        if let Some(x) = self._expires {
            if let Ok(v) = x.as_millis().try_into() {
                arguments.insert("x-expires".into(), lapin::types::AMQPValue::LongLongInt(v));
            }
        }

        if let Some(a) = self._max_priority {
            arguments.insert(
                "x-max-priority".into(),
                lapin::types::AMQPValue::ShortShortUInt(a),
            );
        }

        if let Some(a) = self._max_length {
            arguments.insert(
                "x-max-length-bytes".into(),
                lapin::types::AMQPValue::LongLongInt(a),
            );
        }

        if let Some(dlx) = &self._dead_letter_exchange {
            arguments.insert(
                "x-dead-letter-exchange".into(),
                lapin::types::AMQPValue::ShortString(dlx.clone()),
            );

            if let Some(dlx) = &self._dead_letter_routing_key {
                arguments.insert(
                    "x-dead-letter-exchange".into(),
                    lapin::types::AMQPValue::ShortString(dlx.clone()),
                );
            }
        }

        ch.queue_declare(
            self.name.as_str(),
            QueueDeclareOptions {
                auto_delete: self._auto_delete,
                durable: self.durable,
                exclusive: self._exclusive,
                nowait: false,
                passive: false,
            },
            arguments,
        )
        .await
        .map(|_| ())
    }
}
