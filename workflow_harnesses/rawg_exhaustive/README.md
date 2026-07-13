# RAWG Exhaustive Game-to-Kit Lane

This additive lane corrects the earlier representative-only interpretation of “processed.” Every RAWG row receives a persistent evidence map. Every supported evidence clause can produce atomic mechanic interactions, kit observations, a domain/subdomain map, DSK boundaries, temporal behavior, proof hooks, and master-inventory evidence.

The 350M model expands direct evidence with up to 64 concurrent calls. The 1.2B model refines missing canonical master kits with up to eight concurrent calls. Neither model may infer genre conventions that are absent from the source record.

```bash
kituniverse rawg-exhaustive \
  --ast workflow_harnesses/rawg_exhaustive/configs/smoke.ast.json \
  --workspace runs/rawg-881k/exhaustive-smoke
```

Production uses `configs/production.ast.json`. Source, page, refinement, and build-request identities are append-only and resumable. JSONL shards cap at 90,000,000 bytes. Promotion remains disabled; build requests must enter the existing KitUniverse validation, simulator, duplicate, and transaction gates before they count as built kits.

`kit.build-runtime-prove` materializes each aligned missing contract as an ESM domain service kit under the run workspace and invokes NexusSimulator `kit.runtime-proof`. A queued request does not count as built. A passing runtime report counts as an implementation proof, while final master-inventory admission still requires semantic novelty review and the existing KitUniverse promotion transaction.
