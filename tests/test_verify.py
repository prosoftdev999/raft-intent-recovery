from __future__ import annotations

import json
from pathlib import Path

ARTIFACT = Path('/app/output/recovered_audit.json')
EXPECTED = Path(__file__).resolve().parent / 'expected.json'

TOP_KEYS = {'committed_control_index', 'ranges', 'transactions', 'requests'}
RANGE_KEYS = {'range_id', 'start_key', 'end_key', 'active_from', 'active_to'}
TXN_KEYS = {'txn_id', 'txn_epoch', 'status', 'commit_ts', 'final_seq', 'observed_ts', 'lease_epoch'}
REQ_KEYS = {'request_id', 'read_ts', 'kind', 'rows'}
ROW_KEYS = {'key', 'value', 'visible_ts', 'range_id', 'source_kind'}


def load(path: Path):
    return json.loads(path.read_text())


def normalize(doc):
    return {
        'committed_control_index': doc['committed_control_index'],
        'ranges': sorted(doc['ranges'], key=lambda x:(x['range_id'], x['active_from'])),
        'transactions': sorted(doc['transactions'], key=lambda x:x['txn_id']),
        'requests': sorted([
            {**req, 'rows': sorted(req['rows'], key=lambda row:row['key'])}
            for req in doc['requests']
        ], key=lambda x:x['request_id']),
    }


def test_artifact_exists_and_is_json():
    assert ARTIFACT.is_file(), f'missing {ARTIFACT}'
    load(ARTIFACT)


def test_schema_is_exact():
    doc = load(ARTIFACT)
    assert set(doc) == TOP_KEYS
    assert isinstance(doc['committed_control_index'], int)
    assert isinstance(doc['ranges'], list)
    assert isinstance(doc['transactions'], list)
    assert isinstance(doc['requests'], list)
    assert all(set(item) == RANGE_KEYS for item in doc['ranges'])
    assert all(set(item) == TXN_KEYS for item in doc['transactions'])
    assert all(set(item) == REQ_KEYS for item in doc['requests'])
    assert all(isinstance(req['rows'], list) for req in doc['requests'])
    assert all(set(row) == ROW_KEYS for req in doc['requests'] for row in req['rows'])


def test_recovered_state_matches():
    actual = normalize(load(ARTIFACT))
    expected = normalize(load(EXPECTED))
    assert actual['committed_control_index'] == expected['committed_control_index']
    assert actual['ranges'] == expected['ranges']
    assert actual['transactions'] == expected['transactions']


def test_historical_requests_match():
    actual = normalize(load(ARTIFACT))
    expected = normalize(load(EXPECTED))
    assert actual['requests'] == expected['requests']
