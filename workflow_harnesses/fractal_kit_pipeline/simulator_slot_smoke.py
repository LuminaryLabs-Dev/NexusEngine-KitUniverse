from __future__ import annotations

import json
from typing import Any, Dict, List


REQUIRED_SLOTS = ["name", "domain_path", "requires", "provides", "resources", "public_api", "tests"]


def run_simulator_slot_smoke(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    simtime = create_kit_slot_simtime()
    simtime.reset()
    checks = []
    for record in records:
        result = simtime.step({"command": "slotFill", "kit": record["payload"]})
        checks.append(result)
        if not result["ok"]:
            break
    snapshot = simtime.snapshot()
    return {
        "ok": bool(checks and all(check["ok"] for check in checks) and snapshot["accepted"] == len(records)),
        "stage": "simulator-slot-smoke",
        "source": "simplified NexusSimulator simtime reset/step/snapshot loop",
        "records_tested": len(records),
        "accepted": snapshot["accepted"],
        "rejected": snapshot["rejected"],
        "required_slots": REQUIRED_SLOTS,
        "checks": checks[:100],
        "snapshot": snapshot,
    }


def create_kit_slot_simtime() -> Any:
    class KitSlotSimTime:
        def __init__(self) -> None:
            self.reset()

        def reset(self) -> None:
            self.state = {"accepted": 0, "rejected": 0, "events": [], "last": None}

        def step(self, event: Dict[str, Any]) -> Dict[str, Any]:
            kit = event.get("kit") or {}
            missing = [slot for slot in REQUIRED_SLOTS if not kit.get(slot)]
            renderer_owned = any(
                kit.get("renderer_boundary", {}).get(key)
                for key in ["ownsDom", "ownsCanvas", "ownsThreeObjects"]
            )
            ok = not missing and not renderer_owned and bool(kit.get("idempotent")) and bool(kit.get("atomic"))
            result = {"ok": ok, "name": kit.get("name"), "missing": missing, "renderer_owned": renderer_owned}
            if ok:
                self.state["accepted"] += 1
            else:
                self.state["rejected"] += 1
            self.state["last"] = result
            self.state["events"].append(result)
            return result

        def snapshot(self) -> Dict[str, Any]:
            return json.loads(json.dumps(self.state))

    return KitSlotSimTime()
