# Leader-to-copy lead-time capture (A7)

**Status: recorder implemented; real independently sourced exchange-stage captures: n=0.** This is a local-only ingestion/measurement tool, not an exchange client or trading-path hook. No live wallet/feed/order code, provider integration, data acquisition, credential or deployment was changed.

## Causal stage and capture responsibility

The pending Polygon settlement received by this bot follows the leader's earlier off-chain match. The local WebSocket receipt timestamp, block/indexer timestamp, provider arrival difference and local order-ack time are **not** the leader's exchange match time. A valid lag requires two independently acquired *venue execution-stage* match occurrences, with reported clock uncertainty. Do not infer missing fields from settlement times. No current exporter is available for these independent occurrences, so real sample size and market-day groups are zero.

A future authorized exporter must emit one JSON object per line using all these fields, with no defaults:

- `schema: 1`; `role: "leader"|"follower"`; `stage: "exchange_match"`; `source_kind: "exchange_execution"`.
- `source` and `source_event_id`: explicit provenance and occurrence identity from an independently sourced execution-stage record.
- `copy_attempt_id`: externally verified pairing key; `chain`, `protocol`, `market`, `token`, `side: "BUY"|"SELL"`, `leader_order_id`, `leader_fill_occurrence`, `follower_order_id`: full correlation identity. No truncated token prefixes, inferred order identity or implicit side match.
- `match_utc_ns`: source-reported match-stage UTC nanoseconds; `clock_error_ns`: nonnegative upper bound on error of that timestamp, documented by the exporter.

The recorder validates the exact schema and creates a new mode-0600 JSONL capture, refusing an existing destination. It never reads keys or uses networks. Example, with the operator's independent export on standard input:

```bash
python3 scripts/lead_time_recorder.py capture --output /absolute/owned/evidence/capture.jsonl < /absolute/independent-export.jsonl
python3 scripts/lead_time_recorder.py summary --input /absolute/owned/evidence/capture.jsonl
```

The summary requires one leader and one follower event per complete copy-attempt identity. Duplicated occurrences, missing sides and shared event provenance are counted but excluded. Lag is `(follower.match_utc_ns - leader.match_utc_ns) / 1e6` milliseconds; lower-than-zero lag beyond the sum of independent clock bounds is reported separately. Median and nearest-rank p95 return JSON null when there are no pairs. Source-pair and independent market-day counts indicate support; no confidence interval or speedup claim is available at n=0, and no causal attribution to feed providers follows from paired lag alone.

**Limitations:** The tool cannot authenticate claims in a supplied JSONL file or verify exchange clock calibration; an authorized independent capture process and reconciliation to durable copy-attempt/order identities remain missing. Synthetic evidence under `copybot-evidence/COPYBOT-4` exercises parser/measurement logic only; its 20 ms pair is not market performance. Do not connect this recorder to live feeds, order placement or paid providers without a separate authorized design and gate.
