from __future__ import annotations

from dataclasses import dataclass


def majority(size: int) -> int:
    return size // 2 + 1


@dataclass(frozen=True)
class Membership:
    mode: str
    voters: tuple[str, ...] = ()
    old_voters: tuple[str, ...] = ()
    new_voters: tuple[str, ...] = ()

    @classmethod
    def stable(cls, voters):
        return cls("stable", tuple(sorted(voters)))

    @classmethod
    def joint(cls, old_voters, new_voters):
        return cls(
            "joint",
            (),
            tuple(sorted(old_voters)),
            tuple(sorted(new_voters)),
        )

    def accepts(self, supporters: set[str]) -> bool:
        if self.mode == "stable":
            return len(supporters.intersection(self.voters)) >= majority(len(self.voters))
        old_ok = len(supporters.intersection(self.old_voters)) >= majority(len(self.old_voters))
        new_ok = len(supporters.intersection(self.new_voters)) >= majority(len(self.new_voters))
        return old_ok and new_ok


@dataclass(frozen=True)
class RangeLife:
    range_id: str
    start_key: str
    end_key: str
    active_from: int
    active_to: int | None

    def owns(self, key: str, ts: int) -> bool:
        if ts < self.active_from:
            return False
        if self.active_to is not None and ts >= self.active_to:
            return False
        return self.start_key <= key < self.end_key


@dataclass(frozen=True)
class WalRecord:
    node: str
    index: int
    term: int
    prev_index: int
    prev_term: int
    prev_hash: str
    payload_hash: str
    payload: dict
    source: str
    offset: int


@dataclass(frozen=True)
class MvccRow:
    range_id: str
    record_id: str
    key: str
    end_key: str | None
    write_ts: int
    kind: str
    value: str | None
    txn_id: str | None
    txn_epoch: int | None
    seq: int | None

    def covers(self, key: str) -> bool:
        return self.kind == "RANGE_DEL" and self.end_key is not None and self.key <= key < self.end_key
