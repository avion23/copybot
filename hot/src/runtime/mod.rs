//! Effectful runtime adapters composed by the binary entrypoint.

pub(crate) mod accounting;
pub(crate) mod bankroll_monitor;
pub(crate) mod control;
pub(crate) mod custody_monitor;
pub(crate) mod dashboard_model;
pub(crate) mod dashboard_positions;
pub(crate) mod dashboard_pool;
pub(crate) mod dashboard_trades;
pub(crate) mod equity_monitor;
pub(crate) mod execution;
pub(crate) mod exits;
pub(crate) mod exit_sweep;
pub(crate) mod flatten_task;
pub(crate) mod http;
pub(crate) mod leader_pnl;
pub(crate) mod merge;
pub(crate) mod operator;
pub(crate) mod orphan_sweep;
pub(crate) mod position_reconciliation;
pub(crate) mod politics;
pub(crate) mod reconcile;
pub(crate) mod signal;
pub(crate) mod telemetry;
pub(crate) mod transport;
pub(crate) mod wallet_sync;
