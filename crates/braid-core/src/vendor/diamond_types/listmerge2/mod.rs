mod action_plan;
mod test_conversion;

// #[cfg(feature = "dot_export")]
mod dot;
mod index_gap_buffer;
mod yjsspan;

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use smallvec::{SmallVec, smallvec};
use crate::vendor::rle::SplitableSpan;
use crate::vendor::diamond_types::{DTRange, Frontier, LV};
use crate::vendor::diamond_types::causalgraph::graph::tools::DiffFlag;

type Index = usize;



// #[test]
// fn foo() {
//     let a = RevSortFrontier::from(1);
//     let b = RevSortFrontier::from([0usize, 1].as_slice());
//     dbg!(a.cmp(&b));
// }

// fn peek_when_matches<T: Ord, F: FnOnce(&T) -> bool>(heap: &BinaryHeap<T>, pred: F) -> Option<&T> {
//     if let Some(peeked) = heap.peek() {
//         if pred(peeked) {
//             return Some(peeked);
//         }
//     }
//     None
// }
