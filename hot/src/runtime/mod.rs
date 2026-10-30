//! Effectful runtime adapters composed by the binary entrypoint.

pub(crate) mod accounting;
pub(crate) mod control;
pub(crate) mod dashboard_model;
pub(crate) mod execution;
pub(crate) mod exits;
pub(crate) mod exit_sweep;
pub(crate) mod flatten_task;
pub(crate) mod http;
pub(crate) mod merge;
pub(crate) mod operator;
pub(crate) mod position_reconciliation;
pub(crate) mod reconcile;
pub(crate) mod signal;
pub(crate) mod telemetry;
