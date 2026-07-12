# KitUniverse 1000

Status: active

## Goal

Build 1,000 cumulative promotion-ready kits with one count-driven workflow that behaves identically for `--count 1`, `--count 50`, or any bounded target.

## Batch Contract

- Normalize `--source` or `--source-file` and derive a deterministic operation, boundary, lifecycle, and source-focus matrix.
- Generate one candidate per matrix node with LM Studio `lfm2.5-350m-heretic-high-reasoning-i1`; default to one active prediction.
- Gate each candidate through fresh-process structural/semantic validation, exact and canonical duplicate checks, the real NexusSimulator `kit.contract-proof` tool, and one read-only `gpt-5.6-sol` medium review per batch.
- Promote only the accepted subset using a file lock and prepared/committed transaction journal.
- Quarantine failures without advancing the requested count, then generate replacements under `--max-attempts`.
- Checkpoint every completed generation so `--resume` never repeats successful provider work.
- Keep dry-run and reviewer-skip paths non-promoting.

## Progress

- Raw committed: 66
- Promotion-ready: 58
- Target: 1,000
- Remaining: 942
- Historical quarantine: 8
- Batch quarantine: 106
- Total quarantine: 114
- Validated links: 32
- Latest production run: `runs/workflow-harnesses/kit-universe-batch/20260710-021420-248/final-report.json` (50/50 promoted)
- Resume proof: `runs/workflow-harnesses/kit-universe-batch/20260710-023726-100/final-report.json` (zero regenerated candidates)
- Universe manifest: `runs/kit-universe-1000/manifest.json`
- Promotion audit: `runs/kit-universe-1000/promotion-audit.json`

## Next Quality Focus

Raise Sol acceptance yield without weakening gates. Improve only source-grounded matrix behaviors and compact review evidence, then compare accepted-per-attempt and latency against the 50-kit baseline.
