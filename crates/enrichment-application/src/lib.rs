//! Typed application composition for additive transcript enrichment.
//!
//! This crate is the only morphism that joins locally prepared transcripts,
//! a provider-independent knowledge extractor, and the enrichment-only graph
//! port. It cannot construct deterministic [`harness_graph_graph_port::GraphCommand`]
//! values, so model output is structurally unable to replace the authoritative
//! graph.
//!
//! Chunk-level cited narrative episodes are projected here. A session synopsis
//! is intentionally not accepted because the current extraction and graph
//! ports have no separately checkpointed reduction contract; silently dropping
//! or recomputing a paid synopsis would violate resumability.

mod config;
mod conversion;
mod error;
mod service;

pub use config::*;
pub use error::*;
pub use service::*;
