# Validation — 2026-07-11

- Exact source stream: 177 files, 881,069 JSONL rows, 0 malformed source rows.
- Edge fixture: 4 rows -> 1 clustered, 1 grouped equivalent, 1 insufficient,
  1 intentionally malformed; rerun added no duplicate current-epoch rows.
- Provider proof: LM Studio loaded
  `lfm2.5-350m-heretic-high-reasoning-i1` at context 2048 and parallel 4;
  four requests reached peak concurrency 4 with no context failure.
- Model quality note: the four-record smoke returned no accepted novel mechanic,
  so the deterministic grounded extractor supplied those mechanics. A larger LFM
  quality reconciliation remains required before full production promotion.
- Endurance shadow: 50,001 records, 0 malformed failures, 15,959 insufficient,
  49,980 evidence records, 23 clusters, 2 ranked gaps, 2 candidate requests,
  completed source/evidence shards gzip-compressed, 29.06 GiB free.
- Holds: unreachable endpoint and forced low-disk threshold both entered `hold`
  without consuming or promoting work.
- Resume: repeated starts continued from the exact source cursor; code edits
  created new epochs and preserved prior-epoch evidence.
- Kit gate: one RAWG request completed the full dry-run batch path after one
  rejected replacement; the accepted candidate retained cluster ID, evidence
  hash, and 6 RAWG source IDs.
- Runtime proof: valid fixture passed 23 checks as `runtime-proven`; broken
  fixture failed 9 checks and remained `proof-only`.
- Human view: desktop operator showed all hero controls and reconciled metrics;
  Playwright reported 0 console errors and 0 warnings. Screenshot:
  `output/playwright/rawg-operator-final.png` (generated and ignored).

The 881,069-record model pass and production promotion were not launched. They
remain staged operator actions after extraction-quality reconciliation.
