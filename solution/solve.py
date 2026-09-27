from __future__ import annotations

import json
from pathlib import Path

from mvcc import answer_requests, load_mvcc
from transactions import recover_transactions
from wal import recover_committed_log, replay_control_state

DATA = Path('/app/data')
OUTPUT = Path('/app/output/recovered_audit.json')


def main():
    snapshot, committed = recover_committed_log(DATA)
    ranges, lease_timeline = replay_control_state(snapshot, committed)
    by_range = load_mvcc(DATA)
    outcomes = recover_transactions(DATA, lease_timeline, ranges, by_range)
    requests = answer_requests(DATA, ranges, outcomes, by_range)

    payload = {
        'committed_control_index': committed[-1].index if committed else snapshot['snapshot_index'],
        'ranges': [
            {
                'range_id': item.range_id,
                'start_key': item.start_key,
                'end_key': item.end_key,
                'active_from': item.active_from,
                'active_to': item.active_to,
            }
            for item in ranges
        ],
        'transactions': [
            {
                'txn_id': txn_id,
                'txn_epoch': item['txn_epoch'],
                'status': item['status'],
                'commit_ts': item['commit_ts'],
                'final_seq': item['final_seq'],
                'observed_ts': item['observed_ts'],
                'lease_epoch': item['lease_epoch'],
            }
            for txn_id, item in sorted(outcomes.items())
        ],
        'requests': requests,
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
