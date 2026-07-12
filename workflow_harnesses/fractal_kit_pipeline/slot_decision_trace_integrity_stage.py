from __future__ import annotations

from typing import Any, Dict, List, Set


def build_slot_decision_trace_integrity(
    final_records: List[Dict[str, Any]],
    slot_decision_report: Dict[str, Any],
    simulator_report: Dict[str, Any],
) -> Dict[str, Any]:
    required_slots = list(simulator_report.get("required_slots") or slot_decision_report.get("slots") or [])
    sampled_records = final_records[: slot_decision_report.get("records_tested", 0)]
    sampled_ids = {str(record.get("record_id", "")) for record in sampled_records}
    decisions = slot_decision_report.get("decisions", [])
    decision_ids = {str(decision.get("record_id", "")) for decision in decisions}
    checks = [
        _check(
            "slot-report-ok",
            "LFM slot decision report accepted every sampled record",
            slot_decision_report.get("ok")
            and slot_decision_report.get("records_accepted") == slot_decision_report.get("records_tested")
            and slot_decision_report.get("records_rejected") == 0,
            {
                "records_requested": slot_decision_report.get("records_requested"),
                "records_tested": slot_decision_report.get("records_tested"),
                "records_accepted": slot_decision_report.get("records_accepted"),
                "records_rejected": slot_decision_report.get("records_rejected"),
            },
        ),
        _check(
            "sample-record-ids",
            "slot decisions correspond to the sampled final records",
            sampled_ids == decision_ids,
            {
                "sampled_count": len(sampled_ids),
                "decision_count": len(decision_ids),
                "missing_decisions": sorted(sampled_ids - decision_ids)[:20],
                "extra_decisions": sorted(decision_ids - sampled_ids)[:20],
            },
        ),
        _check(
            "required-slots-match-simulator",
            "slot decision required slots match simulator required slots",
            list(slot_decision_report.get("slots", [])) == required_slots
            and list(simulator_report.get("required_slots", [])) == required_slots,
            {
                "slot_decision_slots": slot_decision_report.get("slots", []),
                "simulator_required_slots": simulator_report.get("required_slots", []),
            },
        ),
        _check(
            "node-slot-coverage",
            "each decision has exactly one accepted Y node for every required slot",
            not _slot_coverage_failures(decisions, required_slots),
            {"failures": _slot_coverage_failures(decisions, required_slots)[:20]},
        ),
        _check(
            "node-prompts-bounded",
            "slot decision prompts remain short and one-question bounded",
            not _prompt_failures(decisions),
            {"failures": _prompt_failures(decisions)[:20]},
        ),
        _check(
            "final-slot-values-filled",
            "sampled final records have all required slot values filled",
            not _final_slot_failures(sampled_records, required_slots),
            {"failures": _final_slot_failures(sampled_records, required_slots)[:20]},
        ),
        _check(
            "simulator-agrees",
            "simplified simulator accepted at least the LFM sample size with no rejections",
            simulator_report.get("ok")
            and simulator_report.get("accepted", 0) >= len(sampled_records)
            and simulator_report.get("rejected") == 0,
            {
                "simulator_ok": simulator_report.get("ok"),
                "records_tested": simulator_report.get("records_tested"),
                "lfm_sampled_records": len(sampled_records),
                "accepted": simulator_report.get("accepted"),
                "rejected": simulator_report.get("rejected"),
            },
        ),
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "stage": "slot-decision-trace-integrity",
        "counts": {
            "sampled_records": len(sampled_records),
            "decision_records": len(decisions),
            "required_slots": len(required_slots),
            "decision_nodes": sum(len(decision.get("nodes", [])) for decision in decisions),
            "simulator_records_tested": simulator_report.get("records_tested", 0),
        },
        "required_slots": required_slots,
        "checks": checks,
        "failed": [check["name"] for check in checks if not check["ok"]],
    }


def _slot_coverage_failures(decisions: List[Dict[str, Any]], required_slots: List[str]) -> List[Dict[str, Any]]:
    required = set(required_slots)
    failures = []
    for decision in decisions:
        nodes = decision.get("nodes", [])
        node_slots = [str(node.get("slot", "")) for node in nodes]
        node_slot_set = set(node_slots)
        duplicate_slots = sorted({slot for slot in node_slots if node_slots.count(slot) > 1})
        bad_nodes = [
            {
                "slot": node.get("slot"),
                "verdict": node.get("verdict"),
                "raw": node.get("raw"),
                "attempts": node.get("attempts"),
            }
            for node in nodes
            if node.get("verdict") != "Y" or not str(node.get("raw", "")).strip().upper().startswith("Y")
        ]
        if node_slot_set != required or duplicate_slots or bad_nodes or not decision.get("accepted"):
            failures.append(
                {
                    "record_id": decision.get("record_id"),
                    "missing_slots": sorted(required - node_slot_set),
                    "extra_slots": sorted(node_slot_set - required),
                    "duplicate_slots": duplicate_slots,
                    "bad_nodes": bad_nodes[:10],
                    "accepted": decision.get("accepted"),
                }
            )
    return failures


def _prompt_failures(decisions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    failures = []
    for decision in decisions:
        for node in decision.get("nodes", []):
            prompt = str(node.get("prompt", ""))
            if len(prompt.split()) > 64 or prompt.count("?") != 1 or "Y or N" not in prompt:
                failures.append(
                    {
                        "record_id": decision.get("record_id"),
                        "slot": node.get("slot"),
                        "word_count": len(prompt.split()),
                        "question_marks": prompt.count("?"),
                        "preview": prompt[:160],
                    }
                )
    return failures


def _final_slot_failures(records: List[Dict[str, Any]], required_slots: List[str]) -> List[Dict[str, Any]]:
    failures = []
    for record in records:
        payload = record.get("payload", {})
        missing = [slot for slot in required_slots if not _filled(payload.get(slot))]
        if missing:
            failures.append({"record_id": record.get("record_id"), "missing_slots": missing})
    return failures


def _filled(value: Any) -> bool:
    if isinstance(value, list):
        return bool(value)
    if isinstance(value, dict):
        return bool(value)
    return bool(str(value or "").strip())


def _check(name: str, requirement: str, ok: bool, evidence: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "requirement": requirement,
        "ok": bool(ok),
        "evidence": evidence,
    }
