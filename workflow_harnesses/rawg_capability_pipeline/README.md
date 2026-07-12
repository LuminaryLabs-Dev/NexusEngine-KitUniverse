# RAWG Capability Pipeline

This workflow streams the external RAWG metadata corpus into grounded,
reviewable KitUniverse build requests. It does not copy the source dataset and
does not write to Nexus Engine or ProtoKits.

## Run

```bash
python3 -m kituniverse_harness.cli serve \
  --workspace runs/rawg-881k/live --open

python3 -m kituniverse_harness.cli rawg-process \
  --workspace runs/rawg-881k/shadow --max-records 1000 --no-model
```

The operator is local-only by default at `http://127.0.0.1:8765`. Its API is:

- `GET /api/status`
- `GET /api/events`
- `POST /api/start`
- `POST /api/pause`
- `POST /api/resume`
- `POST /api/drain`
- `POST /api/stop`

## Records and provenance

The feed-forward contracts are `rawg.source.v1`, `mechanic.evidence.v1`,
`capability.cluster.v1`, `engine.gap.v1`, and `kit.build-request.v1`. Every
downstream build request cites an accepted cluster, RAWG source IDs, and the
cluster evidence hash. Source ledger identity is dataset + game ID + source
hash + pipeline epoch.

The adapter ignores the importer timeline as mechanic evidence. Descriptions,
genres, tags, platforms, and explicit metadata are retained. Equivalent
fingerprints share one extraction; each release still receives its own ledger
record and provenance.

## Safety and epochs

State is append-only or atomically replaced under the workspace. Completed
10,000-record source and evidence shards are gzip-compressed. The supervisor
detects a source-code hash change, drains the current batch, checkpoints it,
and starts a new epoch. Earlier records remain immutable prior-epoch evidence.

The worker holds when disk falls below 10 GiB or a configured provider/reviewer
gate fails. Automatic KitUniverse promotion is available but disabled by
default during staged rollout. Even when enabled, the existing candidate
validation, duplicate signatures, NexusSimulator proof, Codex review,
quarantine, and transaction journal remain mandatory.

## Promotion levels

- `proof-only`: contract passed NexusSimulator `kit.contract-proof`.
- `runtime-proven`: an implementation passed `kit.runtime-proof` in disposable
  staged space, including real import/install, transitions, replay, snapshot,
  load, reset, syntax, public import, package, and declared tests.
- `engine-candidate`: a runtime-proven kit selected for manual integration.

Nothing in this workflow automatically changes ProtoKits or Nexus Engine.

## Rollout gate

Run 100, 1,000, one 5,000-record chunk, and 50,000 records before the complete
pass. Enable model extraction during quality proof. Enable auto-promotion only
after cluster reconciliation is clean. The full source pass is intentionally a
separate operator decision because it is long-running and may create production
KitUniverse records.
