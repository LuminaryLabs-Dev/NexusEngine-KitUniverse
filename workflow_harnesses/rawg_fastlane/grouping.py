from __future__ import annotations

from collections import Counter
from typing import Any, Dict, Iterable


class GroupAccumulator:
    def __init__(self) -> None:
        self.counts: Counter[str] = Counter()
        self.expandable: Dict[str, bool] = {}
        self.selected_secondary: Dict[str, set[str]] = {}

    def add(self, fingerprint: str, expandable: bool, secondary: str, max_representatives: int) -> bool:
        self.counts[fingerprint] += 1
        self.expandable[fingerprint] = expandable
        selected = self.selected_secondary.setdefault(fingerprint, set())
        choose = expandable and secondary not in selected and len(selected) < max_representatives
        if choose:
            selected.add(secondary)
        return choose

    def apply_event(self, event: Dict[str, Any]) -> None:
        self.counts[str(event["fingerprint"])] += 1
        self.expandable[str(event["fingerprint"])] = event.get("status") == "expandable"
        if event.get("representative"):
            self.selected_secondary.setdefault(str(event["fingerprint"]), set()).add(str(event.get("secondary_fingerprint") or event["fingerprint"]))

    @property
    def group_count(self) -> int:
        return len(self.counts)

    @property
    def expandable_group_count(self) -> int:
        return sum(1 for key in self.counts if self.expandable.get(key))

    @property
    def representative_count(self) -> int:
        return sum(len(values) for values in self.selected_secondary.values())
