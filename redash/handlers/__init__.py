from flask import jsonify, url_for
from flask_login import login_required

from redash import settings
from redash.authentication.org_resolving import current_org
from redash.handlers.api import api
from redash.handlers.base import routes
from redash.monitor import get_status
from redash.permissions import require_super_admin


def base_href():
    if settings.MULTI_ORG:
        base_href = url_for('redash.index', _external=True, org_slug=current_org.slug)
    else:
        base_href = url_for('redash.index', _external=True)

    return base_href


@routes.route('/ping', methods=['GET'])
def ping():
    return 'PONG.'


@routes.route('/status.json')
@login_required
@require_super_admin
def status_api():
    status = get_status()

    return jsonify(status)


def init_app(app):
    from redash.handlers import embed, queries, static, authentication
    app.register_blueprint(routes)
    api.init_app(app)
