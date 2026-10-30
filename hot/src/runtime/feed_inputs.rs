//! Feed input setup, shared book-cache socket workers and scored socket recycle.
use super::telemetry::{now_ms, Emitter};
use copybot_hot::book::{BookCache, Levels, TradePrints};
use copybot_hot::config::{BootProfile, Root};
use copybot_hot::feeds::{FeedConfig, FeedRegistry, FeedStats, RawTx, WatchedAddresses};
use copybot_hot::lanes::Router;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

/// Original feed inventory and single boot event.
pub(crate) fn configure(
    root: &Root,
    router: &Arc<Router>,
    emitter: &Arc<Emitter>,
    shadow: bool,
    live: bool,
    boot_profile: BootProfile,
) -> Vec<FeedConfig> {
    let feeds: Vec<FeedConfig> = root
        .feed
        .iter()
        .map(|f| FeedConfig {
            name: f.name.clone(),
            url: f.url.clone(),
            sockets: f.sockets,
        })
        .collect();
    let total_sockets: usize = feeds.iter().map(|f| f.sockets).sum();
    emitter
        .emit(
            serde_json::json!(
                { "t" : now_ms(), "ev" : "boot", "mode" : root.bot.mode, "lanes" : router
                .snapshot().iter().map(| l | serde_json::json!({ "name" : l.cfg.name,
                "wallet" : format!("0x{}", hex::encode(l.cfg.wallet20)), "sizing" :
                format!("{:?}", l.policy().sizing), "execution" : format!("{:?}", l.cfg
                .execution), "daily_usd" : l.policy().caps.daily_usd, })).collect::< Vec
                < _ >> (), "feeds" : feeds.iter().map(| f | format!("{}x{}", f.name, f
                .sockets)).collect::< Vec < _ >> (), "sockets" : total_sockets, "note" :
                if boot_profile == BootProfile::DryObserveOnly {
                    "DRY OBSERVE ONLY — no enabled lanes; feed and auth inspection; no orders"
                } else if shadow {
                "SHADOW — full live path, credentials forced OFF, every \
                             order will be rejected 401"
                } else if live { "LIVE — lanes still need arming in the control file" }
                else { "DRY — nothing will be submitted" }, }
            ),
        );
    feeds
}

pub(crate) struct Feeds {
    pub(crate) books: Arc<BookCache>,
    pub(crate) watch_tokens: Arc<Mutex<Vec<String>>>,
    pub(crate) rx: mpsc::UnboundedReceiver<RawTx>,
    pub(crate) watched_addrs: Arc<WatchedAddresses>,
    pub(crate) feed_registry: Arc<FeedRegistry>,
    pub(crate) _handles: Vec<tokio::task::JoinHandle<()>>,
}

/// Exactly one book feed, one feed receiver and the original socket handle set.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn(
    feeds: &[FeedConfig],
    stats: &Arc<FeedStats>,
    prints: &Arc<TradePrints>,
    levels: &Arc<Levels>,
    emitter: &Arc<Emitter>,
    recycle_secs: u64,
) -> Feeds {
    let books = Arc::new(BookCache::new());
    let watch_tokens: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(
        std::sync::Mutex::new(Vec::new()),
    );
    {
        tokio::spawn(
            copybot_hot::book::run_book_feed_multi(
                books.clone(),
                watch_tokens.clone(),
                Some(prints.clone()),
                Some(levels.clone()),
                3,
            ),
        );
    }
    let (tx, rx) = mpsc::unbounded_channel::<RawTx>();
    let watched_addrs = Arc::new(copybot_hot::feeds::WatchedAddresses::new());
    let (_handles, feed_registry) = copybot_hot::feeds::spawn_all(
        &feeds,
        tx.clone(),
        stats.clone(),
        watched_addrs.clone(),
    );
    let feed_registry = Arc::new(feed_registry);
    if recycle_secs > 0 {
        let (reg, ev) = (feed_registry.clone(), emitter.clone());
        let every = recycle_secs;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(every)).await;
                let ranked = reg.wins_snapshot();
                let losers = reg.score_and_recycle(2, 20);
                if !losers.is_empty() {
                    ev.emit(
                        serde_json::json!(
                            { "t" : now_ms(), "ev" : "socket_recycle", "recycled" :
                            losers, "ranking" : ranked.iter().take(6).map(| (n, w) |
                            serde_json::json!({ "s" : n, "wins" : w })).collect::< Vec <
                            _ >> (), }
                        ),
                    );
                }
            }
        });
        eprintln!(
            "[feeds] socket scoring on: ranking every {every}s, protecting the top 2"
        );
    }
    Feeds { books, watch_tokens, rx, watched_addrs, feed_registry, _handles }
}
