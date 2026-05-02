from __future__ import annotations


def register_watcher_routes(app, start_cb, stop_cb, status_cb, enter_cb, exit_cb):
    @app.route('/watcher/start', methods=['POST'])
    def watcher_start():
        return start_cb()

    @app.route('/watcher/stop', methods=['POST'])
    def watcher_stop():
        return stop_cb()

    @app.route('/watcher/status', methods=['GET'])
    def watcher_status():
        return status_cb()

    @app.route('/watcher/enter', methods=['POST'])
    def watcher_enter():
        return enter_cb()

    @app.route('/watcher/exit', methods=['POST'])
    def watcher_exit():
        return exit_cb()
