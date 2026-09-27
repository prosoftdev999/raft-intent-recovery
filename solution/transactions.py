from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

from model import RangeLife
from mvcc import active_range, intent_hash
from wal import lease_at


def read_jsonl(path: Path):
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def _authoritative_records(records: list[dict], lease_timeline: list[dict]):
    accepted = []
    for record in records:
        lease = lease_at(lease_timeline, record["observed_ts"])
        if record["lease_epoch"] == lease["lease_epoch"] and record["lease_token"] == lease["lease_token"]:
            accepted.append(record)
    return accepted


def _intent_witnesses(by_range, txn_id: str, epoch: int):
    witnesses: dict[tuple[str, int, str, str], bool] = {}
    for range_id, rows in by_range.items():
        for row in rows:
            if row.txn_id != txn_id or row.txn_epoch != epoch:
                continue
            if row.kind not in {"INTENT_PUT", "INTENT_DEL"} or row.seq is None:
                continue
            witnesses[(range_id, row.seq, row.key, intent_hash(row))] = True
    return witnesses


def _staging_proven(chosen: dict, acks: list[dict], ranges: list[RangeLife], by_range) -> bool:
    txn_id = chosen["txn_id"]
    epoch = chosen["txn_epoch"]
    witnesses = _intent_witnesses(by_range, txn_id, epoch)

    valid_ack_keys = set()
    for ack in acks:
        if ack["txn_id"] != txn_id or ack["txn_epoch"] != epoch:
            continue
        try:
            owner = active_range(ranges, ack["key"], ack["ack_ts"])
        except ValueError:
            continue
        if owner.range_id != ack["range_id"]:
            continue
        witness_key = (ack["range_id"], ack["seq"], ack["key"], ack["intent_hash"])
        if witness_key not in witnesses:
            continue
        valid_ack_keys.add((ack["seq"], ack["key"], ack["intent_hash"]))

    required = {
        (item["seq"], item["key"], item["intent_hash"])
        for item in chosen.get("required_writes", [])
    }
    return required.issubset(valid_ack_keys)


def recover_transactions(data_dir: Path, lease_timeline: list[dict], ranges: list[RangeLife], by_range):
    records = read_jsonl(data_dir / "coordinator_records.jsonl")
    acks = read_jsonl(data_dir / "participant_acks.jsonl")
    grouped: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        grouped[record["txn_id"]].append(record)

    outcomes: dict[str, dict] = {}
    for txn_id in sorted(grouped):
        valid = _authoritative_records(grouped[txn_id], lease_timeline)
        if not valid:
            raise ValueError(f"no authoritative coordinator record for {txn_id}")
        epoch = max(item["txn_epoch"] for item in valid)
        chosen = dict(max((item for item in valid if item["txn_epoch"] == epoch), key=lambda item: item["observed_ts"]))
        chosen.setdefault("rolled_back", [])

        if chosen["status"] == "STAGING":
            if _staging_proven(chosen, acks, ranges, by_range):
                chosen["status"] = "COMMITTED"
            else:
                chosen["status"] = "PENDING"
                chosen["commit_ts"] = None
        outcomes[txn_id] = chosen
    return outcomes
