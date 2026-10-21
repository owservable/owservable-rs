//! owservable core: reactive data protocol, stores and backend interface.
//!
//! Wire-compatible with `@owservable/core` v3: a [`Client`] per websocket turns
//! `subscribe`/`unsubscribe`/`reload` messages into reactive stores over registered
//! [`ObservableBackend`]s and emits `update`/`increment`/`total`/`delete` messages.

mod backend;
mod client;
pub mod query;
mod store;

pub use backend::{ConnectionManager, Error, Middleware, ObservableBackend, Registry};
pub use client::Client;
