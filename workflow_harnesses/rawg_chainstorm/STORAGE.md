# RAWG Chainstorm Storage Budget

Measured 2026-07-12 across twelve live runs:

| Profile | Bytes per game | Full 881,069 projection |
| --- | ---: | ---: |
| Local logical artifacts | 32,112 | 26.35 GiB |
| Codex-reviewed logical artifacts | 37,232 | 30.55 GiB |
| Codex-reviewed filesystem allocation in per-game folders | 72,704 | 59.66 GiB |
| Local artifacts in compressed shards | 5,032 | 4.13 GiB |
| Codex-reviewed artifacts in compressed shards | 6,134 | 5.03 GiB |

Per-game directories are forbidden for corpus scale. The measured runs created
13 to 15 files per game, which would become roughly twelve million files.
Production ingestion must append compact records to stage-specific JSONL shards,
rotate at a bounded record count, gzip completed shards, and retain a small
pointer/index ledger. Do not duplicate the external RAWG source dataset.

Reserve 6 to 8 GiB for compressed chainstorm data, indexes, checkpoints,
clusters, and proposal work orders. Keep the existing 10 GiB low-disk hold.
Codex should review aggregated clusters rather than write one review packet per
game, keeping real storage near the local compressed projection.
