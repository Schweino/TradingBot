from __future__ import annotations

import json
import os
import tempfile

import review_packet
from event_store import event_counts, record_event


def test_compact_review_packet_is_small_surface():
    packet = review_packet.build_packet(compact=True)
    assert packet['state_summary']['trade_count'] >= 0
    assert len(packet.get('latest_trades', [])) <= 5
    assert packet.get('skipped_corpus_tail') == []
    assert 'artifact_manifest' in packet


def test_event_store_records_count():
    record_event('test_event', {'ok': True}, symbol='CLSK', trade_id='unit-test')
    counts = event_counts()
    assert counts.get('test_event', 0) >= 1


def test_config_has_side_score_gap():
    with open(os.path.join(os.path.dirname(__file__), 'trading_config.json'), encoding='utf-8') as f:
        cfg = json.load(f)
    assert cfg['smart_entry']['side_score_gap_min'] >= 0
