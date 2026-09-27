from __future__ import annotations

import hashlib
import json
import struct
import zlib
from collections import defaultdict
from pathlib import Path

from model import Membership, RangeLife, WalRecord

MAGIC = b"RWAL"
VERSION = 1
HEADER = struct.Struct("<4sBBHQIQI32sII")
TRAILER = struct.Struct("<I")


def _scan_segment(path: Path, node: str) -> list[WalRecord]:
    raw = path.read_bytes()
    out: list[WalRecord] = []
    cursor = 0
    while True:
        pos = raw.find(MAGIC, cursor)
        if pos < 0:
            break
        cursor = pos + 1
        if pos + HEADER.size + TRAILER.size > len(raw):
            continue
        try:
            (magic, version, flags, header_len, index, term, prev_index, prev_term,
             prev_hash_raw, payload_len, payload_crc) = HEADER.unpack_from(raw, pos)
        except struct.error:
            continue
        if magic != MAGIC or version != VERSION or flags != 0 or header_len != HEADER.size:
            continue
        payload_start = pos + HEADER.size
        payload_end = payload_start + payload_len
        frame_end = payload_end + TRAILER.size
        if frame_end > len(raw):
            continue
        payload_bytes = raw[payload_start:payload_end]
        if zlib.crc32(payload_bytes) & 0xFFFFFFFF != payload_crc:
            continue
        expected_frame_crc = TRAILER.unpack_from(raw, payload_end)[0]
        actual_frame_crc = zlib.crc32(raw[pos:payload_end]) & 0xFFFFFFFF
        if expected_frame_crc != actual_frame_crc:
            continue
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        payload_hash = hashlib.sha256(payload_bytes).hexdigest()
        out.append(WalRecord(
            node=node,
            index=index,
            term=term,
            prev_index=prev_index,
            prev_term=prev_term,
            prev_hash=prev_hash_raw.hex(),
            payload_hash=payload_hash,
            payload=payload,
            source=path.name,
            offset=pos,
        ))
    return out


def load_wal_records(data_dir: Path) -> list[WalRecord]:
    records: list[WalRecord] = []
    control_dir = data_dir / "control"
    for node_dir in sorted(p for p in control_dir.iterdir() if p.is_dir() and p.name.startswith("node-")):
        for path in sorted(node_dir.glob("*.wal")):
            records.extend(_scan_segment(path, node_dir.name))
    return records


def _supporters(candidates: list[WalRecord]) -> set[str]:
    return {record.node for record in candidates}


def recover_committed_log(data_dir: Path):
    snapshot = json.loads((data_dir / "control/snapshot.json").read_text())
    membership = Membership.stable(snapshot["initial_voters"])
    all_records = load_wal_records(data_dir)
    by_index: dict[int, list[WalRecord]] = defaultdict(list)
    for record in all_records:
        by_index[record.index].append(record)

    prev_index = snapshot["snapshot_index"]
    prev_term = snapshot["snapshot_term"]
    prev_hash = snapshot["snapshot_payload_hash"]
    committed: list[WalRecord] = []

    while True:
        index = prev_index + 1
        groups: dict[tuple, list[WalRecord]] = defaultdict(list)
        per_node_seen: set[tuple[str, tuple]] = set()
        for record in by_index.get(index, []):
            if (record.prev_index, record.prev_term, record.prev_hash) != (prev_index, prev_term, prev_hash):
                continue
            identity = (record.term, record.payload_hash, record.prev_index, record.prev_term, record.prev_hash)
            key = (record.node, identity)
            if key in per_node_seen:
                continue
            per_node_seen.add(key)
            groups[identity].append(record)

        durable: list[list[WalRecord]] = []
        for group in groups.values():
            if membership.accepts(_supporters(group)):
                durable.append(group)
        if len(durable) != 1:
            break

        chosen = min(durable[0], key=lambda r: (r.node, r.source, r.offset))
        committed.append(chosen)
        prev_index, prev_term, prev_hash = chosen.index, chosen.term, chosen.payload_hash

        payload = chosen.payload
        if payload["kind"] == "joint_config":
            if membership.mode != "stable":
                raise ValueError("nested joint configuration")
            if tuple(sorted(payload["old_voters"])) != membership.voters:
                raise ValueError("joint configuration does not start from active voters")
            membership = Membership.joint(payload["old_voters"], payload["new_voters"])
        elif payload["kind"] == "finalize_config":
            if membership.mode != "joint":
                raise ValueError("finalize outside joint configuration")
            if tuple(sorted(payload["voters"])) != membership.new_voters:
                raise ValueError("finalized voters do not match joint target")
            membership = Membership.stable(payload["voters"])

    return snapshot, committed


def replay_control_state(snapshot: dict, committed: list[WalRecord]):
    ranges: dict[str, RangeLife] = {
        item["range_id"]: RangeLife(
            item["range_id"], item["start_key"], item["end_key"], item["active_from"], None
        )
        for item in snapshot["initial_ranges"]
    }
    lease_timeline = [dict(snapshot["initial_control_lease"])]

    def retire(range_id: str, ts: int):
        old = ranges[range_id]
        if old.active_to is not None:
            raise ValueError(f"range {range_id} already retired")
        ranges[range_id] = RangeLife(old.range_id, old.start_key, old.end_key, old.active_from, ts)

    for record in committed:
        payload = record.payload
        kind = payload["kind"]
        ts = payload["logical_ts"]
        if kind == "lease":
            lease_timeline.append({
                "lease_epoch": payload["lease_epoch"],
                "holder": payload["holder"],
                "lease_token": payload["lease_token"],
                "active_from": ts,
            })
        elif kind == "split":
            retire(payload["parent"], ts)
            for child in payload["children"]:
                ranges[child["range_id"]] = RangeLife(
                    child["range_id"], child["start_key"], child["end_key"], ts, None
                )
        elif kind == "merge":
            for parent in payload["parents"]:
                retire(parent, ts)
            replacement = payload["replacement"]
            ranges[replacement["range_id"]] = RangeLife(
                replacement["range_id"], replacement["start_key"], replacement["end_key"], ts, None
            )
        elif kind in {"barrier", "joint_config", "finalize_config"}:
            pass
        else:
            raise ValueError(f"unknown control payload kind {kind}")

    ordered_ranges = sorted(ranges.values(), key=lambda r: (r.active_from, r.range_id))
    lease_timeline.sort(key=lambda x: x["active_from"])
    return ordered_ranges, lease_timeline


def lease_at(lease_timeline: list[dict], ts: int) -> dict:
    eligible = [item for item in lease_timeline if item["active_from"] <= ts]
    if not eligible:
        raise ValueError(f"no control lease at {ts}")
    return max(eligible, key=lambda x: x["active_from"])
