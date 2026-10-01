# RAWG Exhaustive Game-to-Kit Lane

This additive lane corrects the earlier representative-only interpretation of “processed.” Every RAWG row receives a persistent evidence map. Every supported evidence clause can produce atomic mechanic interactions, kit observations, a domain/subdomain map, DSK boundaries, temporal behavior, proof hooks, and master-inventory evidence.

The 350M model expands direct evidence with up to 64 concurrent calls. The 1.2B model refines missing canonical master kits with up to eight concurrent calls. Neither model may infer genre conventions that are absent from the source record.

```bash
kituniverse rawg-exhaustive \
  --ast workflow_harnesses/rawg_exhaustive/configs/smoke.ast.json \
  --workspace runs/rawg-881k/exhaustive-smoke
```

Production uses `configs/production.ast.json`. Source, page, refinement, and build-request identities are append-only and resumable. JSONL shards cap at 90,000,000 bytes. Promotion remains disabled; build requests must enter the existing KitUniverse validation, simulator, duplicate, and transaction gates before they count as built kits.

`codex.author-runtime-kits` turns each accepted contract into a capability-specific ESM module plus a `node:test` suite covering its real rules. Production does not allow the shared generic template. `kit.build-runtime-prove` copies that authored package into `.kit-staging/` and invokes NexusSimulator `kit.runtime-proof`. Passing implementations move atomically into `kits/<kit-id>/`; failures move to `quarantine/kits/`. A queued request, generic template, authored source, or staged package does not count as built.

`kit.place-domain-architecture` writes one immutable membership packet per passing kit under `domains/<domain>/<subdomain>/`. `kit.audit-domain-architecture` then reconciles Codex acceptances, build requests, runtime proofs, `kits/`, and `domains/` one-to-one. Catch-all domains, duplicate IDs, orphans, proof failures, and contract/placement mismatches hold the workflow.

```text
<workspace>/
|- kits/<kit-id>/{index.js,kit.json,runtime-proof.json,...}
|- domains/<domain>/domain.json
|  `- <subdomain>/{subdomain.json,<kit-id>.json}
|- quarantine/kits/<build-identity>/
`- domain-architecture-reports/<pipeline-epoch>.json
```
