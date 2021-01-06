import _ from 'lodash';
import io from 'socket.io-client';
import untar from 'js-untar';
import { ungzip } from 'pako';

import { templateFireXId, fetchRunModelMetadata } from './utils';

function socketRequestResponse(socket, requestEvent, successEventName, failedEventName, timeout) {
  const p = new Promise(
    (resolve, reject) => {
      // Track if we've received either a success or failure response, for timeout.
      let responseReceived = false;

      // Handle success socket response (success event name required).
      socket.on(successEventName, (data) => {
        responseReceived = true;
        resolve(data);
      });

      // Handle failure socket response, if event name supplied (optional).
      if (!_.isNil(failedEventName)) {
        socket.on(failedEventName, (data) => {
          responseReceived = true;
          reject(data);
        });
      }

      // Register timeout if supplied.
      if (!_.isNil(timeout)) {
        setTimeout(() => {
          if (!responseReceived) {
            // eslint-disable-next-line
            reject({ timeout: true });
          }
        }, timeout);
      }

      // Send request event.
      if (requestEvent.data !== undefined) {
        socket.emit(requestEvent.name, requestEvent.data);
      } else {
        socket.emit(requestEvent.name);
      }
    },
  );
  // Stop listening after resolved/rejected.
  p.finally(() => {
    socket.off(successEventName);
    if (!_.isNil(failedEventName)) {
      socket.off(failedEventName);
    }
  });
  return p;
}

function createFlameSocket(url, options) {
  const socketOptions = {
    transports: ['websocket'],
    reconnectionAttempts: 3,
  };
  let socket;
  // The path portion of URLs given to socketio specify socketio namespaces to connect to,
  // they don't specify a path to establish transport to.
  if (options.socketPathTemplate) {
    const parsedUrl = new URL(url);
    socketOptions.path = _.template(options.socketPathTemplate,
      { evaluate: null, interpolate: null })({ host: parsedUrl.host });
    socket = io(socketOptions);
  } else {
    // No path, connect directly to URL.
    socket = io(url, socketOptions);
  }

  // TODO: consider making this configurable. Current main deployment no longer supports polling.
  // In case websocket fails, retry with both polling and websocket.
  // socket.on('reconnect_attempt', () => {
  //   socket.io.opts.transports = ['websocket', 'polling'];
  // });

  if (_.has(options, 'onConnect')) {
    socket.on('connect', options.onConnect);
  }
  if (_.has(options, 'onDisconnect')) {
    socket.on('disconnect', options.onDisconnect);
  }

  return socket;
}

function createSocketApiAccessor(url, options) {
  const socket = createFlameSocket(url, options);
  return {
    // TODO: add failure, timeout, or auto-handle elsewhere.
    getFireXRunMetadata: () => socketRequestResponse(
      socket, { name: 'send-run-metadata' }, 'run-metadata',
    ),

    // TODO: add failure, timeout, or auto-handle elsewhere.
    getTaskGraph: () => socketRequestResponse(
      socket, { name: 'send-graph-state' }, 'graph-state',
    ),

    // TODO: add failure, timeout, or auto-handle elsewhere.
    fetchTaskDetails: uuid => socketRequestResponse(
      socket, { name: 'send-task-details', data: uuid }, `task-details-${uuid}`,
    ),

    fetchTaskFields: fields => socketRequestResponse(
      socket, { name: 'send-graph-fields', data: fields }, 'graph-fields',
    ),

    startLiveUpdate(callback) {
      socket.on('tasks-update', callback);
    },

    stopLiveUpdate() {
      socket.off('tasks-update');
    },

    revoke: uuid => socketRequestResponse(
      socket, { name: 'revoke-task', data: uuid }, 'revoke-success',
      'revoke-failed', 10000,
    ),

    isLiveFileListenSupported: () => true,

    startLiveFileListen(host, filepath, addNewLinesCallback) {
      socket.on('file-data', addNewLinesCallback);
      socket.emit('start-listen-file', { host, filepath });
    },

    stopLiveFileListen() {
      socket.off('stop-listen-file');
    },

    cleanup: () => { socket.off(); socket.removeAllListeners(); socket.close(); },

  };
}

function createWebFileAccessor(firexId, modelPathTemplate) {
  const modelBasePath = templateFireXId(modelPathTemplate, firexId);

  const modelBaseUrl = new URL(modelBasePath, window.location.origin);

  const graphUrl = (new URL('slim-tasks.json', modelBaseUrl)).toString();

  return {
    // TODO: add failure, timeout, or auto-handle elsewhere.
    getFireXRunMetadata: () => fetchRunModelMetadata(firexId, modelPathTemplate),

    // TODO: add failure, timeout, or auto-handle elsewhere.
    getTaskGraph: () => fetch(graphUrl).then(r => r.json(), () => {}),

    // TODO: add failure, timeout, or auto-handle elsewhere.
    fetchTaskDetails: uuid => fetch((new URL(`full-tasks/${uuid}.json`, modelBaseUrl)).toString())
      .then(r => r.json(), () => {}),

    // TODO: add failure, timeout, or auto-handle elsewhere.
    fetchTaskFields: fields => fetch(
      (new URL('full-run-state.tar.gz', modelBaseUrl)).toString(),
    )
      .then(r => r.arrayBuffer())
      .then((blob) => {
        try {
          return ungzip(blob).buffer;
        } catch (error) {
          if (error === 'incorrect header check') {
            // Assume already ungzipped (e.g. by server).
            return blob;
          }
          throw error;
        }
      })
      .then(ungzippedContent => untar(ungzippedContent))
      .then((extractedFiles) => {
        const fieldsByUuid = {};

        extractedFiles.forEach((extractedFile) => {
          const fileName = extractedFile.name;

          // Get requested fields from the full task dump files.
          if (fileName.startsWith('full-tasks/') && fileName.endsWith('.json')) {
            try {
              const task = extractedFile.readAsJSON();
              fieldsByUuid[task.uuid] = _.pick(task, fields);
            } catch (error) {
              // Very large task files can fail to be extracted due to:
              // 'Maximum call stack size exceeded'
              // eslint-disable-next-line no-console
              console.error(`Failed extracting data from ${fileName}: ${error}.`);
            }
          }
        });
        return fieldsByUuid;
      }),

    isLiveFileListenSupported: () => false,

    /*
     * Noop operations, since this accessor is only used on completed runs.
     */
    startLiveUpdate: () => {},
    stopLiveUpdate: () => {},
    startLiveFileListen: () => {},
    stopLiveFileListen: () => {},
    revoke: () => {},
    cleanup: () => {},
  };
}

let apiAccessor = null;

function setAccessor(apiType, apiTypeKey, options) {
  // Cleanup current accessor.
  if (!_.isNull(apiAccessor)) {
    apiAccessor.cleanup();
  }

  if (apiType === 'socketio') {
    apiAccessor = createSocketApiAccessor(apiTypeKey, options);
  } else if (apiType === 'dump-files') {
    apiAccessor = createWebFileAccessor(apiTypeKey, options.modelPathTemplate);
  } else {
    throw Error(`Unknown API type: ${apiType}`);
  }
}

function getFireXRunMetadata() {
  return apiAccessor.getFireXRunMetadata();
}

function getTaskGraph() {
  return apiAccessor.getTaskGraph();
}

function fetchTaskDetails(uuid) {
  return apiAccessor.fetchTaskDetails(uuid);
}

function fetchTaskFields(fields) {
  return apiAccessor.fetchTaskFields(fields);
}

function startLiveUpdate(callback) {
  return apiAccessor.startLiveUpdate(callback);
}

function stopLiveUpdate() {
  return apiAccessor.stopLiveUpdate();
}

function revokeTask(uuid) {
  return apiAccessor.revoke(uuid);
}

function isLiveFileListenSupported() {
  return apiAccessor.isLiveFileListenSupported();
}

function startLiveFileListen(host, filepath, addNewLinesCallback) {
  return apiAccessor.startLiveFileListen(host, filepath, addNewLinesCallback);
}

function stopLiveFileListen() {
  return apiAccessor.stopLiveFileListen();
}

export {
  setAccessor,
  getFireXRunMetadata,
  getTaskGraph,
  fetchTaskDetails,
  fetchTaskFields,
  startLiveUpdate,
  stopLiveUpdate,
  revokeTask,
  templateFireXId,
  isLiveFileListenSupported,
  startLiveFileListen,
  stopLiveFileListen,
};
