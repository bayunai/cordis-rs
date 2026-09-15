//! 跨集成测试的共享夹具与 re-export，不承载具体合同断言。
//!
//! 各测试二进制通过 `#[path = "common/helpers.rs"] mod common;` 引入。
#![allow(dead_code, unused_imports)]

pub use async_trait::async_trait;
pub use cordis_core::{
    Context, CoreError, EventKey, FiberState, InjectionState, ListenOptions, ParallelKey, Plugin,
    Runtime, SerialKey, ServiceKey, WaterfallKey,
};
pub use cordis_testkit::{
    EventRecorder, TestPlugin, assert_service_unavailable, wait_injection, wait_until,
};
pub use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
pub use tokio::sync::{broadcast::error::TryRecvError, oneshot};

#[derive(Debug, Clone)]
pub struct Number(pub usize);

#[derive(Debug)]
pub struct Other;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ping(pub u32);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pong(pub u32);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision(pub String);

pub static NUMBER: ServiceKey<Number> = ServiceKey::new("test.number@1");
pub static OTHER_NUMBER: ServiceKey<Other> = ServiceKey::new("test.number@1");
pub static DERIVED: ServiceKey<Number> = ServiceKey::new("test.derived@1");
pub static PING: EventKey<Ping> = EventKey::new("test.ping@1");
pub static PONG_AS_PING: EventKey<Pong> = EventKey::new("test.ping@1");
pub static TRANSFORM: WaterfallKey<Ping> = WaterfallKey::new("test.transform@1");
pub static TRANSFORM_AS_OBSERVE: EventKey<Ping> = EventKey::new("test.transform@1");
pub static DECIDE: SerialKey<Ping, Decision> = SerialKey::new("test.decide@1");
pub static DECIDE_WRONG_ANSWER: SerialKey<Ping, Pong> = SerialKey::new("test.decide@1");
pub static FANOUT: ParallelKey<Ping> = ParallelKey::new("test.fanout@1");

pub fn runtime() -> Runtime {
    Runtime::new().expect("tokio runtime required")
}
