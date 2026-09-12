//! A thin viewer over a lug log.
//!
//! Two modes. Log mode is a tail: records as they arrive, newest at the bottom.
//! Reducible mode renders the materialized view as a tree and repaints it when
//! a patch lands, so state can be watched moving. Nothing else.

pub mod app;
pub mod follow;
pub mod json;
pub mod script;
pub mod tail;
pub mod tree;
