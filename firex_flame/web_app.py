from glob import glob
import json
import logging
import os
import importlib_resources
import urllib.parse
from contextlib import ExitStack
import atexit

import socketio
from gevent import pywsgi
from geventwebsocket.handler import WebSocketHandler

from flask import Flask, redirect, send_from_directory, Response, render_template

from firex_flame.api import create_socketio_task_api, create_revoke_api, create_rest_task_api
from firexapp.submit.reporting import REL_COMPLETION_REPORT_PATH
from firexapp.engine.run_controller import FireXRunController
from firex_flame.flame_helper import FlameServerConfig, get_flame_url_from_port
from firex_flame.controller import FlameAppController


logger = logging.getLogger(__name__)

# This static directory can be used during development to test unreleased versions of the UI.
static_files_dir = os.path.join(os.path.dirname(__file__), "static")

if os.path.isdir(static_files_dir):
    UI_RESOURCE_DIR = static_files_dir
else:
    # Load UI from pypi install case.
    file_manager = ExitStack()
    atexit.register(file_manager.close)
    UI_RESOURCE_DIR = str(file_manager.enter_context(importlib_resources.files('firex_flame_ui')))

REL_UI_RESOURCE_PATH = '/ui'

# Never changes per flame instance, populated on first request.
cached_ui_index_response = None


def _get_asset_rel_path(flame_ui_built_root_dir, asset_suffix) -> str:
    asset_glob =  os.path.join(flame_ui_built_root_dir, 'assets', f'index.*.{asset_suffix}')
    found_assets =  glob(asset_glob)
    assert len(found_assets) == 1, f'Found {len(found_assets)} assets matching glob, require exactly 1. Glob: {asset_glob}'
    return os.path.relpath(found_assets[0], flame_ui_built_root_dir)


class FlameResponse(Response):

    def __init__(self, response, **kwargs):
        # Avoid default octet-stream that causes download instead of display for some log files.
        if kwargs.get('mimetype', None) == 'application/octet-stream':
            kwargs['mimetype'] = 'text/plain'
        super(FlameResponse, self).__init__(response, **kwargs)


def create_ui_index_render_function(central_server, central_server_ui_path):
    def ui_root_render():
        global cached_ui_index_response

        if not cached_ui_index_response:
            rel_js_path = _get_asset_rel_path(UI_RESOURCE_DIR, 'js')
            rel_css_path = _get_asset_rel_path(UI_RESOURCE_DIR, 'css')

            if central_server and central_server_ui_path:
                served_app_js_url = urllib.parse.urljoin(
                    central_server,
                    os.path.join(central_server_ui_path, rel_js_path))

                served_app_css_url = urllib.parse.urljoin(
                    central_server,
                    os.path.join(central_server_ui_path, rel_css_path))
            else:
                # When central valus not supplied, the index.html template produces an html file that references
                # only resources served by this server.
                served_app_js_url = os.path.join(REL_UI_RESOURCE_PATH, rel_js_path)
                served_app_css_url = os.path.join(REL_UI_RESOURCE_PATH, rel_css_path)

            cached_ui_index_response = render_template(
                'index.html',
                served_app_js_url=served_app_js_url,
                served_app_css_url=served_app_css_url,
            )
        return cached_ui_index_response

    return ui_root_render


def ok_json_response(response_data):
    r = Response(response=json.dumps(response_data), status=200, mimetype="application/json")
    r.cache_control.max_age = 2592000
    return r


def create_ui_config(run_metadata):
    return {
        # Tells UI to fetch task data from the origin via Flame's socketio API.
        "access_mode": 'socketio-origin',
        "model_path_template": None,
        "is_central": False,
        "central_server": run_metadata['central_server'],
        "central_server_ui_path": run_metadata['central_server_ui_path'],
        "central_documentation_url": run_metadata['central_documentation_url'],
        "firex_bin": run_metadata['firex_bin'],
        "logs_serving": {
            "serve_mode": "central-webserver",
            "url_format": None,
        },
        'rel_completion_report_path': REL_COMPLETION_REPORT_PATH,
        "linkify_prefixes": ["/auto/", "/ws/"],
    }


def create_web_app(run_metadata, serve_logs_dir):
    web_app = Flask(__name__)
    web_app.response_class = FlameResponse
    web_app.secret_key = os.urandom(24)
    web_app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

    # Redirect root path to UI.
    web_app.add_url_rule('/', 'flame_ui',
                         # TODO: set cache control.
                         create_ui_index_render_function(run_metadata['central_server'],
                                                         run_metadata['central_server_ui_path']))

    # Serve UI artifacts relatively.
    web_app.add_url_rule(REL_UI_RESOURCE_PATH + '/<path:f>', 'ui_artifacts',
                         lambda f: send_from_directory(UI_RESOURCE_DIR, f))

    # img assets are un-prefixed in build artifacts, so redirect to served img directory.
    web_app.add_url_rule('/img/<path:p>', 'ui_img_artifacts', lambda p: redirect(REL_UI_RESOURCE_PATH + '/img/' + p))

    ui_config = create_ui_config(run_metadata)
    web_app.add_url_rule('/flame-ui-config.json', 'ui_config', lambda: ok_json_response(ui_config))

    # Trivial 'OK' endpoint for testing if the server is up.
    web_app.add_url_rule('/alive', 'alive', lambda: ('', 200))

    # Redirect for old URLs. These links should be updated in emails etc, then this redirect removed.
    web_app.add_url_rule('/root/<uuid>', 'subtree', lambda uuid: redirect('/#/root/' + uuid))

    if serve_logs_dir:
        # Add directory listings and file serve for logs.
        from flask import Blueprint
        from flask_autoindex import AutoIndexBlueprint
        auto_index = Blueprint('auto_index', __name__)
        AutoIndexBlueprint(auto_index, browse_root=run_metadata['logs_dir'])
        web_app.register_blueprint(auto_index, url_prefix=run_metadata['logs_dir'])

    return web_app


def start_web_server(
    server_config: FlameServerConfig,
    controller: FlameAppController,
    celery_app,
) -> pywsgi.WSGIServer:
    web_app = create_web_app(controller.run_metadata, server_config.serve_logs_dir)

    controller.set_sio_server(
        # TODO: parametrize cors_allowed_origins.
        socketio.Server(cors_allowed_origins='*', async_mode='gevent')
    )
    sio_web_app = socketio.WSGIApp(controller.sio_server, web_app)

    create_socketio_task_api(controller)
    create_rest_task_api(web_app, controller.graph, controller.run_metadata)

    server = pywsgi.WSGIServer(
        ('', server_config.webapp_port),
        sio_web_app,
        handler_class=WebSocketHandler,
        spawn=30,
    )
    server.start()  # Need to start() to get port if supplied 0.

    if celery_app:
        # Celery app means this is initial launch, not a replay from a rec file.
        # Can only dump initial metadata now that flame_url is set.
        assert server.server_port, 'Web server port not set after start.'
        controller.dump_updated_metadata(
            {'flame_url': get_flame_url_from_port(server.server_port)},
        )
        create_revoke_api(
            controller,
            web_app,
            FireXRunController(
                celery_app,
                controller.run_metadata['logs_dir']),
            server_config.authed_user_request_path,
        )

    return server
