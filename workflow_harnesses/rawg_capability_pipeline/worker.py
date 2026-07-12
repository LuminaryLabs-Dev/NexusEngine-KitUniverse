from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Optional

from .pipeline import run_pipeline


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="kituniverse rawg-process")
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--max-records", type=int)
    parser.add_argument("--no-model", action="store_true")
    args = parser.parse_args(argv)
    override = {}
    if args.max_records is not None:
        override["max_records"] = args.max_records
    if args.no_model:
        override["use_model"] = False
    report = asyncio.run(run_pipeline(args.workspace, override))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("status") in {"complete", "limit-complete", "drained", "stopped"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
