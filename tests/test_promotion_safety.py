import pytest

import promotion_safety


def test_assert_promotable_raises_with_failed_gate(monkeypatch):
    monkeypatch.setattr(
        promotion_safety.promotion_gate,
        'evaluate',
        lambda candidate, days=None: {
            'ok': False,
            'checks': [{'name': 'golden_parity_suite', 'ok': False}],
        },
    )
    with pytest.raises(promotion_safety.PromotionBlocked):
        promotion_safety.assert_promotable({})


def test_promotable_true_when_gate_passes(monkeypatch):
    monkeypatch.setattr(
        promotion_safety.promotion_gate,
        'evaluate',
        lambda candidate, days=None: {'ok': True, 'checks': []},
    )
    assert promotion_safety.promotable({}) is True
