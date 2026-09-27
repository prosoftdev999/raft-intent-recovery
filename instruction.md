# Rebuild the historical audit reads

A replicated range was recovered after an interrupted control-plane repair. The capture under `/app/data` contains three control-log replicas, coordinator observations, participant acknowledgements, MVCC range extracts, and the historical audit requests that now need to be checked.

Read `/app/data/recovery_contract.md` before working with the capture. It defines which control entries are committed, how lease-fenced transaction records are interpreted, how range lifetimes affect the physical extracts, and how MVCC intents become visible.

Write `/app/output/recovered_audit.json` with exactly these top-level fields:

- `committed_control_index`: integer.
- `ranges`: a list of objects with `range_id`, `start_key`, `end_key`, `active_from`, and `active_to`.
- `transactions`: one object for every transaction ID present in the coordinator capture, with `txn_id`, `txn_epoch`, recovered `status`, recovered `commit_ts`, `final_seq`, `observed_ts`, and `lease_epoch`. A `STAGING` coordinator record is reported as `COMMITTED` only when the contract's participant proof is complete; otherwise report it as `PENDING` with a null `commit_ts`.
- `requests`: one object for every request in `audit_requests.jsonl`, with `request_id`, `read_ts`, `kind`, and `rows`. Each row must contain `key`, `value`, `visible_ts`, `range_id`, and `source_kind`.

Use JSON `null` for absent timestamps. Do not add other top-level fields. Ordering of the three lists is not significant, and request-row order is checked by key rather than by JSON text order.

The recovered file is used as an incident audit record, so derive it from the supplied evidence rather than from assumptions about the current on-disk ranges.

You have 7200 seconds to complete this task. Do not cheat by using online solutions or hints specific to this task.
