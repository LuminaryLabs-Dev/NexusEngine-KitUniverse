# Goal

Status: active

## Intent

Reliably grow KitUniverse to 1,000 promotion-ready, nonduplicate kits through one parameterized and resumable batch harness.

## Batch Contract

- `--count` is the exact number of newly promoted kits requested; `1`, `50`, and larger values use the same loop.
- A source string or source file is normalized once, then expanded through a deterministic coverage matrix.
- LM Studio `lfm2.5-350m-heretic-high-reasoning-i1` drafts semantic slots with one active model prediction by default.
- Every candidate independently passes fresh-process contract validation, exact/canonical duplicate checks, the real NexusSimulator kit contract proof, and `gpt-5.6-sol` medium batch review.
- Rejected candidates are quarantined with evidence and replacements continue until the requested count is reached or the bounded attempt limit is exhausted.
- Only passing subsets are promoted through a file-locked prepared/committed journal; resume must not regenerate completed candidates.
- Dry runs never modify the cumulative universe.

## Current Proof

- Production run: `runs/workflow-harnesses/kit-universe-batch/20260710-021420-248/final-report.json`
- Result: 50 promoted from 104 attempts across four batches; 54 rejected candidates were preserved.
- Every promoted record passed the current 31-check contract, real NexusSimulator proof, Sol medium review, and four duplicate signatures.
- Resume proof: `runs/workflow-harnesses/kit-universe-batch/20260710-023726-100/final-report.json` resumed a held batch with zero LM calls and an unchanged generation ledger hash.
- Current universe: 66 raw records, 58 promotion-ready, 114 total quarantined, 32 validated reference-only links, and 942 remaining.
- Five 5-second implementation proof clips: `runs/proof-videos/kit-universe-batch/20260710-implementation/`.

## Completion Criteria

- `runs/kit-universe-1000/manifest.json` reports 1,000 promotion-ready records and zero remaining.
- All promoted records have complete validation, simulator, review, duplicate, and transaction evidence.
- Ledger reconciliation proves unique IDs and semantic signatures, valid links, and no partial transaction.

## RAWG Source Goal

Process all 881,069 RAWG records through an editable, resumable live worker,
deduplicate equivalent evidence before local-model extraction, compare grounded
capability clusters against live Nexus Engine and ProtoKits contracts, and feed
only reviewed missing capabilities into the existing KitUniverse promotion
gates.

Current implementation proof covers the exact 177-file source count, edge
fixtures, four concurrent model requests, safe hold/restart behavior, a
50,001-record deterministic endurance shadow, source-to-kit provenance, and
valid/invalid NexusSimulator runtime proof fixtures. Full production processing
remains gated on a larger LFM extraction-quality reconciliation; automatic
promotion is off by default until that gate is accepted.

The selective `domain-loop` extension may deepen representative or unresolved
records through bounded act-review passes. It is successful only when its
staged candidates survive deterministic evidence checks and optional Codex
review; it must not be multiplied across the full corpus without prior
signature aggregation and a measured call budget.
