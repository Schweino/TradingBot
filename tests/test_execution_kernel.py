import execution_kernel


def test_contract_hash_is_stable_and_ignores_embedded_hash():
    contract = execution_kernel.contract_from_config({
        'step2': {
            'same_ticker_reentry_cooldown_sec': 5,
            'require_conviction': 0,
            'setup_state_enabled': 0,
        },
        'trade_size_pct': 1.0,
    })
    mutated = dict(contract)
    mutated['execution_kernel_hash'] = 'not-part-of-the-hash'
    assert execution_kernel.contract_hash(contract) == execution_kernel.contract_hash(mutated)


def test_round_price_to_cent_uses_half_up():
    assert execution_kernel.round_price_to_cent(23.9373) == 23.94
    assert execution_kernel.round_price_to_cent(23.934) == 23.93
    assert execution_kernel.round_price_to_cent(23.935) == 23.94


def test_one_ticker_reentry_cooldown_window():
    contract = execution_kernel.contract_from_config({
        'step2': {'same_ticker_reentry_cooldown_sec': 5},
    })
    open_until = execution_kernel.ticker_open_until(100, 12, contract)
    assert open_until == 117
    assert execution_kernel.can_enter_ticker(116, open_until)['allowed'] is False
    assert execution_kernel.can_enter_ticker(117, open_until)['allowed'] is True


def test_allocation_matches_step2_cent_rounding():
    contract = execution_kernel.contract_from_config({'trade_size_pct': 1.0})
    assert execution_kernel.allocation(100000, 3, contract) == 33333.33


def test_exit_hit_respects_side():
    assert execution_kernel.exit_hit('LONG', 10.04, 10.04, 9.96) == 'take_profit'
    assert execution_kernel.exit_hit('LONG', 9.96, 10.04, 9.96) == 'stop_loss'
    assert execution_kernel.exit_hit('SHORT', 9.96, 9.96, 10.04) == 'take_profit'
    assert execution_kernel.exit_hit('SHORT', 10.04, 9.96, 10.04) == 'stop_loss'
