#![forbid(unsafe_code)]
#![allow(clippy::result_large_err)] // tonic::Status is intentionally the adapter error type.

pub mod command;
pub mod components;
pub mod config;
pub mod credit;
pub mod grpc;
pub mod id;
pub mod load_test;
pub mod matching;
pub mod metrics;
pub mod party;
pub mod placement;
pub mod profile;
pub mod proposal;
pub mod rating;
pub mod resources;
pub mod server;
pub mod session;
pub mod transport;
pub mod world;

pub use config::ErpsConfig;
