from __future__ import annotations

import csv
import hashlib
from pathlib import Path

from model import MvccRow, RangeLife


def load_mvcc(data_dir: Path) -> dict[str, list[MvccRow]]:
    by_range: dict[str, list[MvccRow]] = {}
    for path in sorted((data_dir / "mvcc").glob("*.csv")):
        rows: list[MvccRow] = []
        with path.open(newline="", encoding="utf-8") as f:
            for raw in csv.DictReader(f):
                rows.append(MvccRow(
                    range_id=path.stem,
                    record_id=raw["record_id"],
                    key=raw["key"],
                    end_key=raw["end_key"] or None,
                    write_ts=int(raw["write_ts"]),
                    kind=raw["kind"],
                    value=raw["value"] if raw["value"] != "" else None,
                    txn_id=raw["txn_id"] or None,
                    txn_epoch=int(raw["txn_epoch"]) if raw["txn_epoch"] else None,
                    seq=int(raw["seq"]) if raw["seq"] else None,
                ))
        by_range[path.stem] = rows
    return by_range


def intent_hash(row: MvccRow) -> str:
    value = "" if row.value is None else row.value
    message = f"{row.txn_id}|{row.txn_epoch}|{row.seq}|{row.key}|{row.kind}|{value}"
    return hashlib.sha256(message.encode("utf-8")).hexdigest()


def active_range(ranges: list[RangeLife], key: str, ts: int) -> RangeLife:
    matches = [item for item in ranges if item.owns(key, ts)]
    if len(matches) != 1:
        raise ValueError(f"expected exactly one owner for {key} at {ts}, found {matches}")
    return matches[0]


def _rolled_back(outcome: dict, seq: int) -> bool:
    for lo, hi in outcome.get("rolled_back", []):
        if lo <= seq <= hi:
            return True
    return False


def _point_effective_ts(row: MvccRow, outcomes: dict[str, dict], read_ts: int) -> int | None:
    if row.kind in {"PUT", "DEL"}:
        return row.write_ts if row.write_ts <= read_ts else None
    if row.kind not in {"INTENT_PUT", "INTENT_DEL"}:
        return None
    outcome = outcomes.get(row.txn_id)
    if not outcome or outcome["status"] != "COMMITTED":
        return None
    if row.txn_epoch != outcome["txn_epoch"]:
        return None
    if row.seq is None or row.seq > outcome["final_seq"] or _rolled_back(outcome, row.seq):
        return None
    commit_ts = outcome["commit_ts"]
    if commit_ts is None or commit_ts > read_ts:
        return None
    return commit_ts


def visible_row(rows: list[MvccRow], outcomes: dict[str, dict], key: str, read_ts: int):
    point_candidates: list[tuple[int, MvccRow]] = []
    tombstone_ts: int | None = None

    for row in rows:
        if row.kind == "RANGE_DEL":
            if row.write_ts <= read_ts and row.covers(key):
                if tombstone_ts is None or row.write_ts > tombstone_ts:
                    tombstone_ts = row.write_ts
            continue
        if row.key != key:
            continue
        effective = _point_effective_ts(row, outcomes, read_ts)
        if effective is not None:
            point_candidates.append((effective, row))

    if not point_candidates:
        return None
    effective, row = max(point_candidates, key=lambda item: (item[0], item[1].record_id))
    if tombstone_ts is not None and tombstone_ts >= effective:
        return None
    if row.kind in {"DEL", "INTENT_DEL"}:
        return None
    return {
        "key": key,
        "value": row.value,
        "visible_ts": effective,
        "source_kind": row.kind,
    }


def answer_requests(data_dir: Path, ranges: list[RangeLife], outcomes: dict[str, dict], by_range: dict[str, list[MvccRow]]):
    import json

    requests = []
    with (data_dir / "audit_requests.jsonl").open(encoding="utf-8") as f:
        for line in f:
            if line.strip():
                requests.append(json.loads(line))

    all_keys = sorted({row.key for rows in by_range.values() for row in rows if row.kind != "RANGE_DEL"})
    answers = []
    for req in requests:
        if req["kind"] == "point":
            keys = [req["key"]]
        elif req["kind"] == "scan":
            keys = [key for key in all_keys if req["start_key"] <= key < req["end_key"]]
        else:
            raise ValueError(f"unknown request kind {req['kind']}")

        result_rows = []
        for key in keys:
            owner = active_range(ranges, key, req["read_ts"])
            physical = by_range.get(owner.range_id, [])
            visible = visible_row(physical, outcomes, key, req["read_ts"])
            if visible is not None:
                visible["range_id"] = owner.range_id
                result_rows.append(visible)
        result_rows.sort(key=lambda item: item["key"])
        answers.append({
            "request_id": req["request_id"],
            "read_ts": req["read_ts"],
            "kind": req["kind"],
            "rows": result_rows,
        })
    return answers
