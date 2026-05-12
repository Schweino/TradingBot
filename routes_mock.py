from __future__ import annotations

from flask import jsonify, request


def register_mock_routes(app, get_mock_trader):
    @app.route('/mock/start', methods=['POST'])
    def mock_start():
        mt = get_mock_trader()
        result = mt.start() or {'ok': True}
        return jsonify({**result, 'status': mt.status()}), (200 if result.get('ok') else 409)

    @app.route('/mock/stop', methods=['POST'])
    def mock_stop():
        mt = get_mock_trader()
        result = mt.stop() or {'ok': True}
        return jsonify({**result, 'status': mt.status()}), (200 if result.get('ok') else 409)

    @app.route('/mock/reset', methods=['POST'])
    def mock_reset():
        mt = get_mock_trader()
        result = mt.reset() or {'ok': True}
        return jsonify({**result, 'status': mt.status()}), (200 if result.get('ok') else 409)

    @app.route('/mock/rollover', methods=['POST'])
    def mock_rollover():
        mt = get_mock_trader()
        data = request.get_json(silent=True) or {}
        day_iso = request.args.get('day') or data.get('day') or None
        dry_run = str(request.args.get('dry_run') or data.get('dry_run') or '').lower() in ('1', 'true', 'yes')
        reason = request.args.get('reason') or data.get('reason') or 'api'
        result = mt.rollover_market_day_state(day_iso=day_iso, reason=reason, dry_run=dry_run)
        return jsonify({**result, 'status': mt.status()}), (200 if result.get('ok') else 409)

    @app.route('/mock/clear-broker-block', methods=['POST'])
    def mock_clear_broker_block():
        mt = get_mock_trader()
        force = str(request.args.get('force', '')).lower() in ('1', 'true', 'yes')
        result = mt.clear_broker_exposure_block(force=force) or {'ok': True}
        return jsonify({**result, 'status': mt.status()}), (200 if result.get('ok') else 409)

    @app.route('/mock/broker-lifecycle', methods=['POST', 'GET'])
    def mock_broker_lifecycle():
        mt = get_mock_trader()
        data = request.get_json(silent=True) or {}
        label = request.args.get('label') or data.get('label') or 'api'
        enforce = str(request.args.get('enforce') or data.get('enforce') or '').lower() in ('1', 'true', 'yes')
        result = mt.broker_lifecycle_gate(label=label, enforce=enforce) or {'ok': True}
        return jsonify({**result, 'status': mt.status()}), (200 if result.get('ok') else 409)

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
