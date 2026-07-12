from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from workflow_harnesses.kit_universe_batch.simulator_adapter import resolve_simulator_cli, run_runtime_proof


def main(argv: Optional[list[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="kituniverse runtime-proof")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--simulator-cli")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--timeout-seconds", type=int, default=300)
    args = parser.parse_args(argv)
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    output = args.output or args.manifest.with_suffix(".runtime-proof.json")
    report = run_runtime_proof(resolve_simulator_cli(args.simulator_cli), args.manifest, output, run_id, args.timeout_seconds)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
