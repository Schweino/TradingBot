import live_signal_step2_parity


def _row(side: str = "SHORT") -> dict:
    return {
        "decision": "entered",
        "ticker": "CLSK",
        "side": side,
        "created_at": 1000,
        "created_at_ct": "2026-05-11T08:39:15-05:00",
        "price": 13.96,
        "extra": {
            "sl": 14.03,
            "tp": 13.92,
            "qty": 590,
        },
    }


def test_short_take_profit_requires_executable_ask_when_quotes_exist():
    paths = live_signal_step2_parity._exit_path([
        {
            "kind": "stock_trade",
            "symbol": "CLSK",
            "t": 1002447,
            "row": {"p": 13.9179, "s": 37, "t": 1002447, "c": ["@", "I"]},
        },
        {
            "kind": "stock_quote",
            "symbol": "CLSK",
            "t": 1002447,
            "row": {"bp": 13.99, "ap": 14.00, "bs": 100, "as": 100, "t": 1002447},
        },
        {
            "kind": "stock_quote",
            "symbol": "CLSK",
            "t": 1010000,
            "row": {"bp": 14.02, "ap": 14.03, "bs": 100, "as": 100, "t": 1010000},
        },
    ])
    outcome = live_signal_step2_parity._simulate_outcome(
        _row(),
        paths,
        trade={"entry": 13.95, "latency_chain": {"entry_filled_ms": 1000000}},
        model={},
    )

    assert outcome["reason"] == "stop_loss"
    assert outcome["exit"] == 14.03
    assert outcome["exit_evidence"]["kind"] == "quote"


def test_short_take_profit_uses_ask_when_it_is_really_available():
    paths = live_signal_step2_parity._exit_path([
        {
            "kind": "stock_quote",
            "symbol": "CLSK",
            "t": 1002447,
            "row": {"bp": 13.91, "ap": 13.92, "bs": 100, "as": 600, "t": 1002447},
        },
    ])
    outcome = live_signal_step2_parity._simulate_outcome(
        _row(),
        paths,
        trade={"entry": 13.95, "latency_chain": {"entry_filled_ms": 1000000}},
        model={},
    )

    assert outcome["reason"] == "take_profit"
    assert outcome["exit"] == 13.92
    assert outcome["exit_evidence"]["ask"] == 13.92
