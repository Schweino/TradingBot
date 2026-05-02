from __future__ import annotations

from flask import jsonify, request


def register_mock_routes(app, get_mock_trader):
    @app.route('/mock/start', methods=['POST'])
    def mock_start():
        mt = get_mock_trader()
        mt.start()
        return jsonify({'ok': True, 'status': mt.status()})

    @app.route('/mock/stop', methods=['POST'])
    def mock_stop():
        mt = get_mock_trader()
        mt.stop()
        return jsonify({'ok': True, 'status': mt.status()})

    @app.route('/mock/reset', methods=['POST'])
    def mock_reset():
        mt = get_mock_trader()
        mt.reset()
        return jsonify({'ok': True, 'status': mt.status()})

    @app.route('/mock/kill-switch', methods=['POST'])
    def mock_kill_switch():
        mt = get_mock_trader()
        data = request.get_json(silent=True) or {}
        enabled = bool(data.get('enabled'))
        reason = data.get('reason') or 'manual'
        mt.set_kill_switch(enabled, reason)
        return jsonify({'ok': True, 'status': mt.status()})

    @app.route('/mock/status', methods=['GET'])
    def mock_status():
        mt = get_mock_trader()
        full = str(request.args.get('full', '')).lower() in ('1', 'true', 'yes')
        return jsonify(mt.status(full=full))

    @app.route('/mock/trades', methods=['GET'])
    def mock_trades():
        mt = get_mock_trader()
        limit_raw = request.args.get('limit')
        try:
            limit = int(limit_raw) if limit_raw not in (None, '') else None
        except ValueError:
            return jsonify({'error': 'limit must be an integer'}), 400
        day_iso = request.args.get('day') or None
        full = str(request.args.get('full', '')).lower() in ('1', 'true', 'yes')
        return jsonify(mt.trade_history(limit=limit, day_iso=day_iso, full=full))

    @app.route('/mock/quality', methods=['GET'])
    def mock_quality():
        from elite_control import live_quality
        mt = get_mock_trader()
        return jsonify(live_quality(mt))

    @app.route('/mock/candidate-config/<day_iso>', methods=['GET'])
    def mock_candidate_config(day_iso):
        from elite_control import build_candidate_config
        return jsonify(build_candidate_config(day_iso))

    @app.route('/mock/setup-capital', methods=['GET'])
    def mock_setup_capital():
        from elite_control import rolling_setup_capital_table
        days = int(request.args.get('days', 7))
        return jsonify(rolling_setup_capital_table(max_days=days))

    @app.route('/mock/ev-table/<day_iso>', methods=['GET'])
    def mock_ev_table(day_iso):
        from ev_analytics import build_ev_table
        lookback = int(request.args.get('lookback_days', 10))
        return jsonify(build_ev_table(day_iso, lookback_days=lookback))
