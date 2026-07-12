# RAWG Domain Act–Review Loop

This harness runs one bounded, indexed discovery loop over one normalized RAWG
record. It is a staging surface for semantic merging, not a kit generator and
not a production promotion path.

```text
source record
-> deterministic mechanical sentence/tag selection
-> structured fact extraction
-> deterministic fact grounding
-> focused yes/no review per fact
-> fact-to-capability seed conversion
-> high-temperature act by domain type
-> deterministic evidence and duplicate gate
-> low-temperature self-review
-> durable accepted/rejected loop state
-> optional Codex CLI final review
```

## Run

```bash
python3 -m kituniverse_harness.cli domain-loop \
  --game-id baldurs-gate-iii \
  --max-passes 8 \
  --codex-review

python3 -m kituniverse_harness.cli domain-loop \
  --record-file workflow_harnesses/rawg_domain_loop/fixtures/explicit-party-game.json \
  --max-passes 8 \
  --max-empty-passes 8 \
  --codex-review
```

Defaults target `lfm2.5-1.2b-instruct`, context 24576, parallel 1, act
temperature 0.8, review temperature 0.1, and eight exploration types. Every
pass writes `act.json`, `evidence-gate.json`, and `review.json`; evidence-chain
artifacts live under `mechanical-evidence-chain/`; durable state lives in
`loop-state.json`.

The harness owns candidate IDs when the small model returns numeric or
placeholder IDs. It rejects broad genres, platforms, generic system labels,
unsupported source terms, repeats, and malformed review envelopes. The review
must decide every real proposal exactly once. Codex-approved candidates are
written separately from locally accepted staging candidates.

## Stop conditions

- Configured pass limit.
- Two consecutive empty passes after at least four domain types.
- Provider/configuration hold.
- Minimum accepted or Codex-approved count not reached.

## Scaling boundary

The final eight-pass real-record proof required 21 local-model calls.
Do not apply it blindly to 881,069 records. Use it for aggregate evidence
signatures, representative records, or unresolved sources after cheaper
deterministic extraction.

The evidence chain rejects narrative/modal claims before domain discovery and
passes only source-ID-grounded facts forward. Codex remains the final quality
boundary. See `VALIDATION.md`.
