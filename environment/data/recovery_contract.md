# Recovery contract

The capture is from a replicated MVCC key/value service used below a SQL layer. Timestamps are integer logical timestamps, and key spans are half-open: `start_key <= key < end_key`.

The evidence is synthetic, but it follows the failure modes of a real interrupted replica repair: overlapping WAL segments, damaged tail records, membership changes, historical range reconfiguration, stale lease-holder observations, parallel-commit evidence, rolled-back mutation sequences, and MVCC range tombstones.

## Control WAL

`control/snapshot.json` describes the durable state through `snapshot_index`. The three `control/node-*` directories contain captured WAL segments from individual replicas. Segment filenames and byte offsets are physical capture details; they do not define log order, and the same logical record can appear more than once on one replica.

A WAL frame uses little-endian integers and this layout:

| field | type |
| --- | --- |
| magic | 4 bytes, ASCII `RWAL` |
| version | uint8, currently `1` |
| flags | uint8, currently `0` |
| header_len | uint16 |
| index | uint64 |
| term | uint32 |
| prev_index | uint64 |
| prev_term | uint32 |
| prev_hash | 32 raw SHA-256 bytes |
| payload_len | uint32 |
| payload_crc | uint32 |
| payload | `payload_len` bytes of UTF-8 JSON |
| frame_crc | uint32 |

`header_len` is 72 bytes. `payload_crc` is the unsigned IEEE CRC-32 of the payload. `frame_crc` is the unsigned IEEE CRC-32 of the complete header followed by the payload. A frame with the wrong magic, version, header length, CRC, truncated payload, or invalid JSON is not a log record.

The payload hash of a valid record is SHA-256 over the payload bytes exactly as stored. A record extends another record only when its `prev_index`, `prev_term`, and `prev_hash` equal that predecessor's index, term, and payload hash. The first post-snapshot record must extend the snapshot's `(snapshot_index, snapshot_term, snapshot_payload_hash)` tuple.

Durability is evaluated using the voter configuration that is already durable before the candidate record. In a stable configuration, supporters must include a strict majority of the listed voters. In a joint configuration, supporters must include a strict majority of both the old and new voter sets. Multiple copies on one replica count once. A durable `joint_config` enters the joint configuration named in its payload; a durable `finalize_config` enters its `voters` set. The recovered control history is contiguous from the snapshot. A later physical record cannot bypass a missing durable predecessor.

Control payloads also include:

- `lease`: replaces the control lease at `logical_ts`.
- `split`: retires `parent` and activates all `children` at `logical_ts`.
- `merge`: retires every listed parent and activates `replacement` at `logical_ts`.
- `barrier`: changes no logical database state.

The snapshot's initial range is active from its `active_from` value. Range descriptors created only on a non-durable log branch never become active, even if corresponding files remain in the capture.

## Coordinator observations

`coordinator_records.jsonl` contains observations from coordinator caches. A record is authoritative only when its `lease_epoch` and `lease_token` match the durable control lease active at `observed_ts`.

For each transaction, use authoritative observations from the highest `txn_epoch`. Within that epoch, the observation with the greatest `observed_ts` is the recovered coordinator outcome.

`final_seq` is the largest mutation sequence that can survive from that attempt. `rolled_back` contains inclusive sequence intervals that were removed before the recorded outcome; a sequence in one of those intervals does not survive even when it is at or below `final_seq`.

A `COMMITTED` observation supplies the transaction's commit timestamp. `ABORTED` and `PENDING` do not. A `STAGING` observation is an implicit commit only if every entry in `required_writes` is independently supported by the participant capture:

- a row in `participant_acks.jsonl` has the same transaction ID, transaction epoch, mutation sequence, key, and `intent_hash`;
- the acknowledgement's `range_id` owns that key at `ack_ts` in the durable descriptor history; and
- that range's MVCC extract contains the corresponding intent with the same hash.

The hash for an intent is SHA-256 of the UTF-8 string

`txn_id|txn_epoch|seq|key|kind|value`

where a missing value is represented by the empty string. If the required write set is not completely supported, report the recovered transaction as `PENDING` with a null commit timestamp.

## MVCC extracts

`mvcc/*.csv` are physical range extracts. A file can belong to a retired range or to a range created only by the interrupted repair, so file presence is not range ownership.

The columns are `record_id,key,end_key,write_ts,kind,value,txn_id,txn_epoch,seq`.

Point records use `PUT`, `DEL`, `INTENT_PUT`, or `INTENT_DEL`. `RANGE_DEL` is a non-transactional range tombstone over `[key, end_key)`.

Reads use the descriptor active at the request timestamp and only the extract for the range that owns the key at that time. A non-transactional point record has effective timestamp `write_ts`.

An intent contributes a committed point version only when its recovered transaction is `COMMITTED`, its transaction epoch matches the recovered epoch, its sequence is at or below `final_seq`, its sequence is not in a `rolled_back` interval, and the recovered commit timestamp is not later than the read timestamp. Its effective timestamp is the recovered commit timestamp, not its provisional `write_ts`.

For a key, the visible point version is the eligible point record with greatest effective timestamp at or before the read timestamp. This capture contains no semantic ties at that timestamp. A visible `DEL` or `INTENT_DEL` removes the key. A `RANGE_DEL` at or before the read timestamp removes an otherwise visible point version when the tombstone covers the key and its timestamp is greater than or equal to the point version's effective timestamp. A later point version can therefore make the key visible again.

## Audit requests and output

`audit_requests.jsonl` contains point requests and half-open scans. A point request returns zero or one row. A scan returns each visible key in its span, ordered lexicographically by key.

Returned rows contain `key`, `value`, `visible_ts`, `range_id`, and `source_kind`. `source_kind` is the physical point-record kind that supplied the visible value. Delete records and range tombstones do not produce output rows.

The recovered artifact also records the durable control index, every descriptor lifetime created by the durable control history, and one recovered outcome for every transaction ID found in the coordinator capture. Use `active_to = null` for descriptors still active at the end of the durable history.
