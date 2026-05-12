import replay_artifacts


def test_historical_short_tp_ignores_trade_print_without_executable_ask():
    start = 1000
    end = 1010
    ts_values, prices = replay_artifacts.price_series_from_events([
        {
            "kind": "stock_trade",
            "symbol": "CLSK",
            "t": 1002000,
            "row": {"p": 13.9179, "s": 37, "t": 1002000, "c": ["@", "I"]},
        },
        {
            "kind": "stock_quote",
            "symbol": "CLSK",
            "t": 1002000,
            "row": {"bp": 13.99, "ap": 14.00, "bs": 100, "as": 100, "t": 1002000},
        },
        {
            "kind": "stock_quote",
            "symbol": "CLSK",
            "t": 1008000,
            "row": {"bp": 14.02, "ap": 14.03, "bs": 100, "as": 100, "t": 1008000},
        },
    ], "CLSK", start, end)
    _exit_ts, exit_points = replay_artifacts.exit_series_from_events([
        {
            "kind": "stock_trade",
            "symbol": "CLSK",
            "t": 1002000,
            "row": {"p": 13.9179, "s": 37, "t": 1002000, "c": ["@", "I"]},
        },
        {
            "kind": "stock_quote",
            "symbol": "CLSK",
            "t": 1002000,
            "row": {"bp": 13.99, "ap": 14.00, "bs": 100, "as": 100, "t": 1002000},
        },
        {
            "kind": "stock_quote",
            "symbol": "CLSK",
            "t": 1008000,
            "row": {"bp": 14.02, "ap": 14.03, "bs": 100, "as": 100, "t": 1008000},
        },
    ], "CLSK", start, end)

    legacy = replay_artifacts.replay_outcome("SHORT", "CLSK", start, 13.96, prices, start, end, end)
    quote_aware = replay_artifacts.replay_outcome("SHORT", "CLSK", start, 13.96, exit_points, start, end, end)

    assert legacy["reason"] == "take_profit"
    assert quote_aware["reason"] == "stop_loss"
    assert quote_aware["exit_evidence"]["kind"] == "quote"


def test_historical_short_tp_uses_quote_when_ask_is_available():
    start = 1000
    end = 1005
    _exit_ts, exit_points = replay_artifacts.exit_series_from_events([
        {
            "kind": "stock_quote",
            "symbol": "CLSK",
            "t": 1002000,
            "row": {"bp": 13.91, "ap": 13.92, "bs": 100, "as": 600, "t": 1002000},
        },
    ], "CLSK", start, end)

    outcome = replay_artifacts.replay_outcome("SHORT", "CLSK", start, 13.96, exit_points, start, end, end)

    assert outcome["reason"] == "take_profit"
    assert outcome["exit_evidence"]["ask"] == 13.92
