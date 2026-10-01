# RAWG Chainstorm Validation

## Live configuration

- Endpoint: `http://10.0.0.137:1234/v1`
- Model: `lfm2.5-1.2b-instruct`
- Context: 2,048
- Parallel predictions: 1
- Local ceiling: four rounds with a 10-second adaptive budget
- Default beam: three active pointers

## Proofs

- `20260712-003806-422`: two rounds and six nodes in 8.586 seconds; the scheduler stopped before a predicted over-budget third round.
- `20260712-003708-719`: two rounds in 10.477 seconds; Codex produced `choice-reaction-resolver`, demonstrating the proposal and build-request handoff before the later evidence hardening.
- `20260712-003938-789`: two rounds in 9.245 seconds; Codex returned zero kits after finding overlap and unsupported abstractions.
- `20260712-004249-105`: narrative and engine-marketing seed filtering completed two rounds in 7.255 seconds; Codex produced two proposal-only requests, exposing a stopword bug in lexical evidence matching.
- `20260712-004440-246`: direct evidence and lineage evidence were separated; Codex rejected all nine unsupported nodes. Its third round exceeded the budget, leading to the final conservative `max_observed_call * 1.25` admission rule.
- `20260712-015155-335`: final-code Half-Life 2 proof completed two local rounds in 7.748 seconds, preserved four filtered nodes with zero malformed JSONL, and Codex rejected all four because none had direct evidence. The run occupied 29,240 logical bytes and 65,536 allocated bytes.

## Acceptance

- LFM performs brainstorming only and has no tools, review authority, repository access, or promotion authority.
- Every round persists raw output, accepted and rejected nodes, active pointers, elapsed time, and provenance.
- Codex may emit zero kits. A zero-kit result is correct when evidence or novelty is insufficient.
- Build requests are proposal-only and never promote automatically.
- Direct evidence IDs and lineage IDs are distinct; ancestry never counts as proof.
- Corpus-scale storage uses gzip-compressed JSONL shards; per-game directories are validation artifacts only.
