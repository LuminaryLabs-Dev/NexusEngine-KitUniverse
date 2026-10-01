# RAWG Matrix Optimizer Validation

## Winning profile

- Profile: `12b-seed-12b-walk-lean-beam2-d2`
- Seed model: `lfm2.5-1.2b-instruct`
- Seed temperature: 1.2
- Walk model: `lfm2.5-1.2b-instruct`
- Walk temperature: 0.7
- Beam: 2
- Depth: 2
- Parallel predictions: 8
- Context ceiling: 2,000 tokens per prediction; 16,000 loaded context for eight shared slots

## Matrix progression

- `20260712-020340-599`: four-game four-profile comparison. Codex selected 1.2B seed and walk over faster but ungrounded 350M output.
- `20260712-020531-135`: eight-slot comparison proved peak concurrency 8; the best lane reached 23.391 games/minute but only 2/8 complete grounded hierarchies.
- `20260712-020944-146`: pointer-per-parent walking raised the balanced profile to 41.305 games/minute and 4/8 complete hierarchies; 350M walking remained weaker.
- `20260712-021116-767`: beam-two depth-two reached 69.444 games/minute, 5/8 complete hierarchies, 10/10 accepted nodes directly grounded, and an 8.811-day naive corpus estimate. Depth three was slower and less reliable.
- `20260712-021345-556`: 32-game endurance comparison stabilized the winner at 64.867 games/minute, 17/32 complete grounded hierarchies, 58 directly evidenced unique nodes, 2.25 calls/game, zero malformed JSONL, and a 9.432-day estimate. Codex retained the 1.2B beam-two profile over the 350M seed alternative.

## Production proof

- Workspace: `runs/rawg-881k/matrix-production`
- First invocation: 16 records, 9 successful hierarchies, peak 8 predictions, zero failures.
- Resume invocation: skipped 16 existing identities and processed records 17-32 without regeneration.
- JSONL reconciliation at 32: 32 unique completion identities, 32 result records, zero malformed lines.
- The first Codex cluster review rejected all five support-two candidates because broad tags had produced generic `*-hub` clusters. `hub` is now banned, and child nodes must ground every newly introduced term in direct game evidence.
- Resume proof after that gate change skipped all 264 existing identities and processed exactly eight new records; no `hub` node survived.
- The full worker resumed from 272 completed identities with eight-way concurrency; monitor `status.json` rather than starting another worker.
- Shards are capped at 90,000,000 bytes and processing holds below 10 GiB free.
- At 288 records, measured result, completion-ledger, and cluster-event density projects approximately 5-7 GiB for the full corpus; use 8 GiB as the operating allowance.
- Mechanics-only evidence selector proof `20260712-023137-425`: platform tags were removed, all 5 accepted nodes retained direct evidence, and the profile ran at 58.625 games/minute.
- Three-options-per-parent proof `20260712-023321-043`: grounded hierarchy completion improved from 25% to 62.5%, all 13 accepted nodes retained direct evidence, peak concurrency remained eight, and throughput was 47.436 games/minute.
- Production resume after the fail-forward changes skipped 424 existing identities, processed exactly eight new records, and recorded zero failures.
- Incremental clustering then consumed exactly 40 unseen results and filtered nine synthetic-container clusters before Codex invocation.
- Repair proof `20260712-023419-204-repair-proof`: Codex received `explore-puzzle`, completed a typed second-stage repair decision, and correctly declined to invent an atomic transition absent from the cited evidence.

## Additive AST fast lane

- Three deterministic 20,000-record scans reached 59,134-61,324 records/minute with zero malformed JSONL; a later stratified scan reached 84,646 records/minute.
- LM Studio 350M was safely reconfigured to context 32,768 / parallel 64, providing 512 tokens per slot. A 256-call proof reached peak 64 active predictions.
- Clean 20,000-record support-first proof selected 1,126 representatives, ran 822 350M calls at 344.653 representatives/minute, emitted 2,139 deterministic evidence pairs plus 279 model-added ideas, and merged 619 clusters.
- Support-first 1.2B refinement reached 1.203 accepted subdomains per call, approximately 10.6x the earlier per-representative refinement yield.
- Strict atomic refinement reserved 16/96 calls for singleton recall, recovered one rare subdomain, retained bounded evidence excerpts for every accepted result, and produced 13 canonical inventory-missing keys before Codex quality review.
- The corrected pipelined proof reconciled 577/577 selected/swarmed representatives, refined 277 support or recall milestones, and measured 69.851 seconds of overlap between 64-lane 350M and eight-lane 1.2B work.
- Shadow production uses up to 12 secondary evidence strata per coarse group, 90 MB JSONL shards, a 10 GiB disk hold, append-only resume identities, and no promotion.

## Full-corpus fast-lane proof

- Workspace: `runs/rawg-881k/fast-support-first-production`.
- The deterministic scan processed exactly 881,069 records in 618.791 seconds at 85,431.321 records/minute, with zero malformed records and 881,069 unique source identities.
- Fixed and adaptive evidence strata produced 7,164 unique representatives. Representative, swarm-ledger, and swarm-result counts all reconcile at 7,164 with no duplicates or missing results.
- The 350M stage used 64 parallel slots with 512 tokens per slot; the 1.2B stage used eight slots with a 700-token response ceiling. The 350M model supplied breadth, while deterministic evidence and 1.2B refinement owned quality.
- The refinement ledger and result set reconcile at 1,166 unique identities. Strict relation revalidation retained 26 atomic candidates from 124 accepted nodes and rejected 86 unsupported or non-atomic relations.
- One bounded, read-only Codex proposal review accepted 2/26 candidates: Card Upgrade Kit and Deck Composition Kit. Both remain `proposal-only`; nothing was promoted into KitUniverse, ProtoKits, or Nexus Engine.
- The adaptive continuation added 1,308 representatives without adding or rewriting source-ledger rows. Source and group ledgers still reconcile exactly at 881,069 identities.
- All 24 JSONL shards parse cleanly. The largest is 89,999,955 bytes, below the 90,000,000-byte cap.
- Full scan plus first support-first pipeline completed in about 45 minutes; the adaptive continuation added 164.411 seconds. The result is comfortably inside the six-hour target on the measured machine.
