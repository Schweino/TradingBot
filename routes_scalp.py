from __future__ import annotations

from flask import jsonify, request


def register_scalp_routes(app, get_scalp_engine, load_scalp_state,
                          save_scalp_state, is_mock_running):
    @app.route('/scalp/start', methods=['POST'])
    def scalp_start():
        body = request.json or {}
        ticker = (body.get('ticker') or '').upper().strip()
        btc = bool(body.get('btc', False))
        toast_level = body.get('toast_level', 'HIGH')
        if not ticker:
            return jsonify({'error': 'ticker required'}), 400
        eng = get_scalp_engine()
        eng.subscribe(ticker, btc=btc)
        save_scalp_state({
            'scalp_on': True,
            'ticker': ticker,
            'btc': btc,
            'toast_level': toast_level,
        })
        return jsonify({'ok': True, 'snapshot': eng.get_snapshot()})

    @app.route('/scalp/stop', methods=['POST'])
    def scalp_stop():
        eng = get_scalp_engine()
        mt_running = bool(is_mock_running())
        if eng and not mt_running:
            eng.unsubscribe_all()
        save_scalp_state({'scalp_on': False})
        return jsonify({'ok': True, 'streams_kept_for_mock_trader': mt_running})

    @app.route('/scalp/status', methods=['GET'])
    def scalp_status():
        eng = get_scalp_engine()
        if eng is None:
            return jsonify({'active': False})
        return jsonify(eng.get_snapshot())

    @app.route('/scalp/prefs', methods=['POST'])
    def scalp_prefs():
        body = request.json or {}
        state = load_scalp_state()
        if 'toast_level' in body:
            state['toast_level'] = body['toast_level']
        save_scalp_state(state)
        return jsonify({'ok': True, 'state': state})
