import execution_kernel
import promotion_gate


def test_candidate_summary_finds_nested_kernel_hash():
    kernel_hash = execution_kernel.contract_from_config({})['execution_kernel_hash']
    row = {
        'score': {
            'execution_kernel_hash': kernel_hash,
            'result': {'pnl': 10.0, 'trades': 2, 'wins': 1, 'losses': 1},
        }
    }
    summary = promotion_gate._candidate_summary(row)
    assert summary['execution_kernel_hash'] == kernel_hash
    assert summary['pnl'] == 10.0


def test_gate_fails_closed_without_candidate_kernel_hash(monkeypatch):
    monkeypatch.setattr(promotion_gate.golden_parity_suite, 'run', lambda days=None: {'ok': True, 'results': []})
    payload = promotion_gate.evaluate(candidate={}, days=[])
    failed = {row['name'] for row in payload['checks'] if not row['ok']}
    assert 'candidate_execution_kernel_hash_present' in failed
