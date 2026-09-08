"""Corrections review routes; background startup remains owned by the app."""
from flask import Blueprint, jsonify, render_template, request

from .feed import FeedFailure


def register_corrections_routes(app, components, load_config):
    routes = Blueprint('corrections', __name__)

    @routes.get('/corrections')
    def review():
        status = request.args.get('status', 'all')
        if status not in ('all', 'pending', 'conflict', 'unmatched', 'applied', 'dismissed', 'superseded'):
            return jsonify(success=False, error='Unknown correction status.'), 400
        raw_cursor = request.args.get('before_id')
        if raw_cursor is not None and (not raw_cursor.isascii() or not raw_cursor.isdecimal() or len(raw_cursor) > 18 or int(raw_cursor) < 1):
            return jsonify(success=False, error='Invalid correction page cursor.'), 400
        before_id = int(raw_cursor) if raw_cursor is not None else None
        service, application = components()
        decision_page = application.decision_page(status=status, limit=100, before_id=before_id)
        return render_template('corrections.html', config=load_config(),
                               feed=service.summary(), decisions=decision_page['items'],
                               decision_page=decision_page, selected_status=status, before_id=before_id,
                               counts=decision_page['status_counts'])

    @routes.get('/api/corrections/summary')
    def summary():
        service, application = components()
        return jsonify(feed=service.summary(), **application.summary())

    @routes.get('/api/corrections/<int:decision_id>')
    def detail(decision_id):
        _, application = components()
        decision = application.detail(decision_id)
        if not decision:
            return jsonify(success=False, error='Correction not found.'), 404
        return jsonify(success=True, decision=decision)

    @routes.post('/api/corrections/<int:decision_id>/<action>')
    def decide(decision_id, action):
        if action not in ('apply', 'dismiss'):
            return jsonify(success=False, error='Unknown correction action.'), 404
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify(success=False, error='A JSON review decision is required.'), 400
        revision = data.get('expected_revision')
        if type(revision) is not int or revision < 0:
            return jsonify(success=False, error='Refresh the review before deciding.'), 400
        _, application = components()
        if action == 'apply':
            result = application.apply(decision_id, revision, explicit=True)
        else:
            result = application.dismiss(decision_id, revision)
        return jsonify(result), 200 if result.get('success') else 409

    @routes.post('/api/corrections/check')
    def check():
        if not isinstance(request.get_json(silent=True), dict):
            return jsonify(success=False, error='A JSON check request is required.'), 400
        service, _ = components()
        result = service.request_poll()
        return jsonify(success=True, result=result)

    @routes.post('/api/corrections/reset')
    def reset():
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify(success=False, error='A JSON confirmation is required.'), 400
        if data.get('confirm') is not True:
            return jsonify(success=False, error='Confirm reconciliation before restarting the feed.'), 400
        service, _ = components()
        result = service.reset(since=data.get('since') or None)
        success = result.get('status') == 'reset'
        return jsonify(success=success, result=result, error=None if success else result.get('status')), 200 if success else 409

    @routes.errorhandler(FeedFailure)
    def feed_error(error):
        return jsonify(success=False, error=error.code), 409

    app.register_blueprint(routes)
