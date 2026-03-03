// #![warn(unused)]

use crate::vendor::diamond_types::{DTRange, Frontier, KVPair, Graph};
use crate::vendor::diamond_types::causalgraph::agent_assignment::AgentAssignment;

pub(crate) mod storage;
mod causalgraph;
mod check;
pub mod graph;
mod eq;
pub mod entry;
pub mod summary;
pub mod agent_span;
pub mod agent_assignment;

#[cfg(test)]
mod enc_fuzzer;
#[cfg(feature = "dot_export")]
pub mod dot;

#[derive(Clone, Debug, Default)]
pub struct CausalGraph {
    pub agent_assignment: AgentAssignment,

    /// Transaction metadata (succeeds, parents) for all operations on this document. This is used
    /// for `diff` and `branchContainsVersion` calls on the document, which is necessary to merge
    /// remote changes.
    ///
    /// At its core, this data set compactly stores the list of parents for every operation.
    pub graph: Graph,

    /// This is the version you get if you load the entire causal graph
    pub version: Frontier,
}
