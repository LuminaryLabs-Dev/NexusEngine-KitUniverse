from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


DEFAULT_ROOT = Path("runs/ingestion")
DEFAULT_SHARDS = 256


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="ingestion-service")
    subcommands = parser.add_subparsers(dest="command", required=True)

    create_parser = subcommands.add_parser("create")
    create_parser.add_argument("--root", default=str(DEFAULT_ROOT))
    create_parser.add_argument("--run-id")
    create_parser.add_argument("--shards", type=int, default=DEFAULT_SHARDS)

    ingest_parser = subcommands.add_parser("ingest")
    ingest_parser.add_argument("--run-dir", required=True)
    ingest_parser.add_argument("--record-json", required=True)

    stress_parser = subcommands.add_parser("stress")
    stress_parser.add_argument("--root", default=str(DEFAULT_ROOT))
    stress_parser.add_argument("--run-id")
    stress_parser.add_argument("--shards", type=int, default=DEFAULT_SHARDS)
    stress_parser.add_argument("--records", type=int, default=256)
    stress_parser.add_argument("--concurrency", type=int, default=256)

    args = parser.parse_args(argv)
    if args.command == "create":
        service = ShardedJsonlIngestionService.create(
            root=Path(args.root),
            run_id=args.run_id,
            shard_count=args.shards,
        )
        print(json.dumps(service.manifest(), indent=2, sort_keys=True))
        return 0
    if args.command == "ingest":
        report = asyncio.run(_ingest_one(Path(args.run_dir), args.record_json))
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report.get("ok") else 1
    if args.command == "stress":
        report = asyncio.run(
            stress_test(
                root=Path(args.root),
                run_id=args.run_id,
                shard_count=args.shards,
                records=args.records,
                concurrency=args.concurrency,
            )
        )
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report.get("ok") else 1
    parser.error(f"unknown command: {args.command}")
    return 2


@dataclass
class IngestResult:
    ok: bool
    run_id: str
    record_id: str
    shard: int
    shard_path: str
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "run_id": self.run_id,
            "record_id": self.record_id,
            "shard": self.shard,
            "shard_path": self.shard_path,
            "error": self.error,
        }


class ShardedJsonlIngestionService:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        manifest = _read_json(run_dir / "manifest.json")
        self.run_id = str(manifest["run_id"])
        self.shard_count = int(manifest["shard_count"])
        self.shards_dir = run_dir / "shards"
        self._locks = [asyncio.Lock() for _ in range(self.shard_count)]

    @classmethod
    def create(
        cls,
        root: Path = DEFAULT_ROOT,
        run_id: Optional[str] = None,
        shard_count: int = DEFAULT_SHARDS,
    ) -> "ShardedJsonlIngestionService":
        if shard_count < 1:
            raise ValueError("shard_count must be at least 1")
        if run_id is None:
            run_id = _run_id()
        run_dir = root / run_id
        shards_dir = run_dir / "shards"
        shards_dir.mkdir(parents=True, exist_ok=True)
        for shard in range(shard_count):
            (shards_dir / _shard_name(shard)).touch(exist_ok=True)
        _write_json(
            run_dir / "manifest.json",
            {
                "run_id": run_id,
                "created_at": _timestamp(),
                "shard_count": shard_count,
                "storage": "sharded-jsonl",
                "record_contract": {
                    "required": ["record_id", "created_at", "payload"],
                    "note": "Any extra fields are preserved.",
                },
            },
        )
        return cls(run_dir)

    @classmethod
    def open(cls, run_dir: Path) -> "ShardedJsonlIngestionService":
        return cls(run_dir)

    def manifest(self) -> Dict[str, Any]:
        return {
            **_read_json(self.run_dir / "manifest.json"),
            "run_dir": str(self.run_dir),
            "shards_dir": str(self.shards_dir),
        }

    async def ingest(self, record: Dict[str, Any]) -> IngestResult:
        normalized = _normalize_record(record)
        record_id = normalized["record_id"]
        shard = _shard_for(record_id, self.shard_count)
        shard_path = self.shards_dir / _shard_name(shard)
        line = json.dumps(normalized, sort_keys=True, separators=(",", ":")) + "\n"
        try:
            async with self._locks[shard]:
                await asyncio.to_thread(_append_text, shard_path, line)
        except Exception as exc:  # noqa: BLE001 - return normalized ingest failure.
            return IngestResult(
                ok=False,
                run_id=self.run_id,
                record_id=record_id,
                shard=shard,
                shard_path=str(shard_path),
                error=str(exc),
            )
        return IngestResult(
            ok=True,
            run_id=self.run_id,
            record_id=record_id,
            shard=shard,
            shard_path=str(shard_path),
        )

    async def ingest_many(
        self,
        records: List[Dict[str, Any]],
        concurrency: int = DEFAULT_SHARDS,
    ) -> List[IngestResult]:
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        semaphore = asyncio.Semaphore(concurrency)

        async def run_one(record: Dict[str, Any]) -> IngestResult:
            async with semaphore:
                return await self.ingest(record)

        return await asyncio.gather(*(run_one(record) for record in records))

    def report(self) -> Dict[str, Any]:
        shard_counts = []
        total = 0
        malformed = 0
        for shard in range(self.shard_count):
            path = self.shards_dir / _shard_name(shard)
            count, bad = _count_jsonl(path)
            total += count
            malformed += bad
            if count or bad:
                shard_counts.append(
                    {
                        "shard": shard,
                        "path": str(path),
                        "records": count,
                        "malformed": bad,
                    }
                )
        report = {
            "ok": malformed == 0,
            "run_id": self.run_id,
            "run_dir": str(self.run_dir),
            "shard_count": self.shard_count,
            "total_records": total,
            "malformed_records": malformed,
            "active_shards": len(shard_counts),
            "shards": shard_counts,
            "created_at": _timestamp(),
        }
        _write_json(self.run_dir / "report.json", report)
        return report


async def stress_test(
    root: Path,
    run_id: Optional[str],
    shard_count: int,
    records: int,
    concurrency: int,
) -> Dict[str, Any]:
    if records < 1:
        raise ValueError("records must be at least 1")
    service = ShardedJsonlIngestionService.create(
        root=root,
        run_id=run_id,
        shard_count=shard_count,
    )
    started = time.time()
    payloads = [
        {
            "record_id": f"stress-{index:06d}",
            "source": "stress",
            "payload": {
                "index": index,
                "input": [f"a-{index}", f"b-{index}", f"c-{index}"],
                "output": f"d-{index}",
            },
        }
        for index in range(records)
    ]
    results = await service.ingest_many(payloads, concurrency=concurrency)
    ingest_elapsed = round(time.time() - started, 3)
    report = service.report()
    failures = [result.to_dict() for result in results if not result.ok]
    stress_report = {
        **report,
        "ok": report["ok"] and not failures and report["total_records"] == records,
        "records_requested": records,
        "records_written": report["total_records"],
        "concurrency": concurrency,
        "ingest_elapsed_seconds": ingest_elapsed,
        "failures": failures,
    }
    _write_json(service.run_dir / "stress-report.json", stress_report)
    return stress_report


async def _ingest_one(run_dir: Path, record_json: str) -> Dict[str, Any]:
    service = ShardedJsonlIngestionService.open(run_dir)
    record = json.loads(record_json)
    if not isinstance(record, dict):
        raise ValueError("--record-json must decode to an object")
    result = await service.ingest(record)
    return result.to_dict()


def _normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(record)
    payload = normalized.get("payload")
    if payload is None:
        payload = {
            key: value
            for key, value in normalized.items()
            if key not in {"record_id", "created_at", "payload"}
        }
        normalized["payload"] = payload
    if "record_id" not in normalized or not str(normalized["record_id"]).strip():
        normalized["record_id"] = _fingerprint(json.dumps(payload, sort_keys=True))
    normalized["record_id"] = str(normalized["record_id"])
    normalized.setdefault("created_at", _timestamp())
    return normalized


def _count_jsonl(path: Path) -> tuple[int, int]:
    count = 0
    malformed = 0
    if not path.exists():
        return count, malformed
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                json.loads(line)
            except json.JSONDecodeError:
                malformed += 1
                continue
            count += 1
    return count, malformed


def _append_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()


def _shard_for(record_id: str, shard_count: int) -> int:
    digest = hashlib.sha256(record_id.encode("utf-8")).hexdigest()
    return int(digest[:12], 16) % shard_count


def _shard_name(shard: int) -> str:
    return f"shard-{shard:03d}.jsonl"


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _run_id() -> str:
    return f"{time.strftime('%Y%m%d-%H%M%S')}-{int((time.time() % 1) * 1000):03d}"


def _timestamp() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S %Z")


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
