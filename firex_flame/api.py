"""
Flask API module for interacting with celery tasks.
"""

import logging
from socket import gethostname
import os
import subprocess
import requests
from typing import Optional
import urllib.parse
import getpass

from flask import jsonify, request
from gevent import spawn, sleep
import paramiko

from firexapp.engine.run_controller import FireXRunController
from firex_flame.flame_helper import wait_until, REVOKE_REASON_KEY
from firex_flame.flame_task_graph import FlameTaskGraph, is_task_dict_complete
from firex_flame.controller import FlameAppController
from firex_flame.model_dumper import wait_and_get_flame_url


logger = logging.getLogger(__name__)

subprocess_dict = {}


def _run_metadata_to_api_model(run_metadata, root_uuid):
    return {
        'uid': run_metadata['uid'],
        'logs_dir': run_metadata['logs_dir'],
        'root_uuid': root_uuid,
        'chain': run_metadata['chain'],
        'logs_server': run_metadata['logs_server'],
    }


def poll_channel_readable(channel, timeout=0):
    interval = 0.1
    so_far = 0
    while so_far <= timeout:
        if channel.recv_ready():
            return True
        sleep(interval)
        so_far += interval
    return False


def monitor_file(sio_server, sid, host, filename):
    # To avoid issues with huge log files, we only get the last 50000 lines
    max_lines = 50000

    # helper to send data
    def emit_line_data(data):
        sio_server.emit('file-data', data, room=sid)

    # check if host is localhost or remote - use ssh if remote
    if host in ['127.0.0.1', 'localhost', gethostname()]:
        # Read file locally - output to be sent to requesting client
        logger.info(f"Will start monitoring file {filename} locally")

        if not os.path.isfile(filename):
            emit_line_data(f"File {filename} does not exist.")
            return

        if not os.access(filename, os.R_OK):
            emit_line_data(f"File {filename} is not accessible.")
            return

        try:
            proc = subprocess.Popen(['/usr/bin/tail', '-n', str(max_lines), '--follow=name', filename, '2>/dev/null'],
                                    stdout=subprocess.PIPE)
        except Exception:
            logger.warning("Exception raised while trying to spawn subprocess to monitor file: ", exc_info=True)
            return

        # keep track of sub-procs for later cleanup
        subprocess_dict[sid] = proc

        while True:
            line = proc.stdout.readline()
            if not line:
                break
            emit_line_data(line.decode('utf-8'))

    else:
        # spawn ssh to host to tail -f the file - output to be sent to requesting client
        logger.info("Will start monitoring file %s on host %s" % (filename, host))
        # noinspection PyBroadException
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(host, 22, timeout=60, compress=True)

            # Run find to locate file and match perms
            _, stdout, stderr = ssh.exec_command("\\find %s -perm -004" % filename)
            # Wait for command to return
            stdout.channel.recv_exit_status()

            # Read command stdout/err
            res_out = stdout.read()
            res_err = stderr.read()

            # Check our results
            if not res_out or res_out == b'':
                if not res_err or res_err == b'':
                    emit_line_data("ERROR: File access permissions prevent viewing of file.\n")
                else:
                    res_err = res_err.decode('utf-8', 'ignore')
                    if "No such file or directory" in res_err:
                        # File no longer exists on remote host
                        emit_line_data('[Temporary file no longer exists - executed command has completed]\n')
                    else:
                        emit_line_data("ERROR: Unexpected error while checking file existence and permissions: %s" %
                                       res_err)
                return
            else:
                res_out = res_out.decode('utf-8', 'ignore')
                if filename not in res_out:
                    emit_line_data("ERROR: Unexpected output while checking file existence and permissions: %s" % res_out)
                    return

            try:
                # File exists and has open permissions - tail it
                _, stdout, _ = ssh.exec_command("""/bin/bash -c '/usr/bin/tail -n %d --follow=name %s 2>/dev/null' """ %
                                            (max_lines, filename), bufsize=128, get_pty=True)

                # Keep track of all spawned processes to be able to manage them later
                subprocess_dict[sid] = ssh

                # Set read timeout to 1s
                stdout.channel.settimeout(1)

                # local helper function
                def get_data_chunk():
                    from socket import timeout
                    data_chunk = ''
                    max_chunk_lines = 10000
                    num_chunk_lines = 0
                    end_of_file = False
                    while num_chunk_lines < max_chunk_lines:
                        try:
                            line = stdout.readline()
                            if not line or line == '':
                                # empty line signifies eof
                                end_of_file = True
                                break
                        except timeout:
                            # No more data available within timeout: consider this a full data_chunk to be sent off
                            break
                        else:
                            data_chunk += line
                            num_chunk_lines += 1
                    return data_chunk, num_chunk_lines, end_of_file

                total_num_lines = 0
                eof = False
                while not eof:
                    chunk, num_lines, eof = get_data_chunk()
                    if num_lines:
                        if not total_num_lines:
                            chunk = "[start of data received]\n" + chunk
                        emit_line_data(chunk)
                        total_num_lines += num_lines

                if total_num_lines:
                    emit_line_data('[End of file - program exited]\n')
                    return

            except Exception:
                logger.warning("Exception raised while trying to spawn subprocess to monitor file: ", exc_info=True)

        except Exception as e:
            emit_line_data("ERROR: Spawned subprocess to monitor file failed:\n")
            emit_line_data(str(e))


def term_subproc(sid):
    if sid not in subprocess_dict:
        logger.warning(f"SID {sid} not in subprocess list")
        return

    subproc = subprocess_dict[sid]
    subproc.terminate()
    del subprocess_dict[sid]


def term_all_subprocs():
    for sid in subprocess_dict:
        term_subproc(sid)


def create_socketio_task_api(controller: FlameAppController):

    @controller.sio_server.on('send-graph-state')
    def emit_frontend_tasks_by_uuid(sid, data=None):
        """ Send 'slim' fields for all tasks. This allows visualization of the graph."""
        if (
            isinstance(data, dict)
            and {'task_queries', 'model_file_name'}.intersection(data.keys())
        ):
            tasks_to_send = controller.query_full_tasks(
                data.get('task_queries'),
                data.get('model_file_name'),
            )
        else:
            tasks_to_send = controller.graph.get_slim_tasks_by_uuid()
        controller.sio_server.emit('graph-state', tasks_to_send, room=sid)

    @controller.sio_server.on('send-graph-fields')
    def emit_task_fields_by_uuid(sid, fields):
        """ Send the requested fields for all tasks."""
        response = controller.graph.get_all_tasks_fields(fields)
        controller.sio_server.emit('graph-fields', response, room=sid)

    @controller.sio_server.on('send-run-metadata')
    def emit_run_metadata(sid):
        """ Get static run-level data."""
        response = _run_metadata_to_api_model(
            controller.run_metadata, controller.graph.root_uuid
        )
        controller.sio_server.emit('run-metadata', response, room=sid)

    @controller.sio_server.on('send-task-details')
    def emit_detailed_tasks(sid, uuids):
        """ Get all fields for requested task UUIDs.
        Arguments:
            sid: The session ID to emit to.
            uuids (str or list of str): The uuid of the desired task to get details for.
        """
        if isinstance(uuids, str):
            uuid = uuids
            response = controller.graph.get_full_task_dict(uuid)
            controller.sio_server.emit('task-details-' + uuid, response, room=sid)
        else:
            if not isinstance(uuids, list):
                response = []
            else:
                response = [controller.graph.get_full_task_dict(u) for u in uuids]
            controller.sio_server.emit('task-details', response, room=sid)

    @controller.sio_server.on('start-listen-file')
    def start_file_monitor(sid, args):
        if 'host' not in args or 'filepath' not in args:
            controller.sio_server.emit(
                'file-line',
                f"File monitoring request is missing either host and/or filepath parameters: {args}",
                room=sid)
        else:
            spawn(monitor_file, sio_server=controller.sio_server, sid=sid, host=args['host'], filename=args['filepath'])

    @controller.sio_server.on('stop-listen-file')
    def stop_file_monitor(sid):
        term_subproc(sid)

    @controller.sio_server.on('start-listen-task-query')
    def start_listen_task_query(sid, args):
        if not {'task_queries', 'model_file_name', 'query_config'}.intersection(args.keys()):
            logger.error("Received request to start listening to query without config.")
        else:
            # keep legacy name temporarily for cutover.
            task_queries = args.get('task_queries', args.get('query_config'))
            controller.add_client_task_query_config(
                sid,
                task_queries,
                args.get('model_file_name'),
            )

    @controller.sio_server.on('disconnect')
    def disconnect(sid):
        controller.remove_client_task_query(sid)


def create_rest_task_api(
    web_app,
    task_graph: FlameTaskGraph,
    run_metadata,
):

    @web_app.route('/api/tasks')
    def get_all_tasks_by_uuid():
        return jsonify(task_graph.get_slim_tasks_by_uuid())

    @web_app.route('/api/tasks/<uuid>')
    def get_task_details(uuid):
        task = task_graph.get_full_task_dict(uuid)
        if task is None:
            return '', 404
        return jsonify(task)

    @web_app.route('/api/run-metadata')
    def get_run_metadata():
        return jsonify(_run_metadata_to_api_model(run_metadata, task_graph.root_uuid))


def _data_from_environ_path(environ, path, default):
    if len(path) == 3:
        if path[0] == 'request' and path[1] == 'headers':
            header_key = path[2]
            return  environ.get(f'HTTP_{header_key.upper()}', default)
    return default

def _data_from_request_path(path):
    if len(path) == 3:
        if path[0] == 'request' and path[1] == 'headers':
            header_key = path[2]
            return request.headers.get(header_key)
    return None

def _uuid_and_reason_from_revoke_data(revoke_data):
    if isinstance(revoke_data, str):
        uuid = revoke_data
        revoke_reason = None
    elif isinstance(revoke_data, dict):
        uuid = revoke_data.get('uuid')
        revoke_reason = revoke_data.get(REVOKE_REASON_KEY)
    else:
        uuid = None
        revoke_reason = None
    return uuid, revoke_reason

def create_revoke_api(
    controller: FlameAppController,
    web_app,
    run_controller: FireXRunController,
    authed_user_request_path,
):
    assert controller.sio_server is not None

    sids_to_details = {}
    NO_USER = 'nouser'

    @controller.sio_server.event
    def connect(sid, environ):
        requester = _data_from_environ_path(
            environ, authed_user_request_path, NO_USER)
        sids_to_details[sid] = {
            'requester': requester,
        }

    @controller.sio_server.event
    def disconnect(sid):
        sids_to_details.pop(sid, None)

    @controller.sio_server.on('revoke-task')
    def socketio_revoke_task(sid, revoke_data):
        uuid, revoke_reason = _uuid_and_reason_from_revoke_data(revoke_data)
        if uuid:
            requester = sids_to_details[sid].get('requester', NO_USER)
            revoked = _revoke_task(uuid, 'SocketIO', requester, revoke_reason)
            response_event = 'revoke-success' if revoked else 'revoke-failed'
        else:
            response_event = 'revoke-failed'
        controller.sio_server.emit(response_event, room=sid)

    def _rest_revoke_task(uuid):
        authed_user = _data_from_request_path(authed_user_request_path)
        revoke_reason = request.args.get(REVOKE_REASON_KEY)
        claimed_user = request.args.get('revoking_user')

        user = authed_user or claimed_user or NO_USER
        revoked = _revoke_task(uuid, 'REST', user, revoke_reason)
        return '', 200 if revoked else 500

    @web_app.route('/api/revoke/<uuid>', methods=['GET', 'POST'])
    def rest_revoke_task(uuid):
        return _rest_revoke_task(uuid)

    @web_app.route('/api/revoke', methods=['GET', 'POST'])
    def rest_revoke_root_task():
        # a revoke run quickly after submission
        # might be received before the root task is known,
        # so wait a brief amount of time.
        root_uuid = wait_until(
            controller.graph.is_root_started,
            timeout=60, # startup can be slow.
            sleep_for=0.1,
        )
        root_uuid = controller.graph.root_uuid
        logger.debug(f'Revoking entire run via root task UUID: {root_uuid}')
        if not root_uuid:
            return '', 500
        return _rest_revoke_task(root_uuid)

    def _revoke_task(uuid, type, user, revoke_reason):
        msg = f"Received {type} request to revoke {uuid} from user {user}"
        if revoke_reason:
            msg += f' with reason: {revoke_reason}'
            revoke_reason = revoke_reason[:200] # trim since we'll store this.
        logger.info(msg)
        task = controller.graph.get_slim_task_dict(uuid)
        if task is None:
            return False

        if not is_task_dict_complete(task):
            run_controller.revoke_task(
                uuid,
                revoke_reason,
                revoking_user=user,
                is_root_task=controller.graph.root_uuid == uuid,
            )
        else:
            logger.info(f"Task {uuid} already in terminal state {task['state']}")

        # Wait for the task to become revoked
        revoke_timeout = 10

        revoked = wait_until(controller.graph.was_revoked, timeout=revoke_timeout, sleep_for=0.5, uuid=uuid)
        if not revoked:
            task = controller.graph.get_slim_task_dict(uuid)
            logger.warning(
                f"Failed to revoke  task: waited {revoke_timeout} sec and runstate is currently {task['state']}")
        else:
            logger.debug(f"Successfully revoked task {uuid}.")

        return revoked  # If the task was successfully revoked, return true


def flame_revoke(
    logs_dir : str,
    task_uuid : Optional[str]=None, # None revokes the whole run by revoking the root task.
    revoke_reason: Optional[str]=None,
    revoking_user: Optional[str]=getpass.getuser(),
    timeout=10*60,
) -> Optional[requests.Response]:

    flame_url = wait_and_get_flame_url(firex_logs_dir=logs_dir)
    if not flame_url:
        logger.warning(f"Flame URL not found for {logs_dir}; revoke via Flame will likely fail.")
    else:
        # requesting /api/revoke will revoke the root task, which revoked the entire run.
        url_path = '/api/revoke'
        if task_uuid:
            url_path = os.path.join(url_path, task_uuid)

        url_params = {'revoking_user': revoking_user}
        if revoke_reason:
            url_params[REVOKE_REASON_KEY] = revoke_reason

        return requests.get(
            urllib.parse.urljoin(flame_url, url_path),
            params=url_params,
            timeout=timeout)

    return None