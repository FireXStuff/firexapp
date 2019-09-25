import _ from 'lodash';
import Vue from 'vue';
import { flextree } from 'd3-flextree';

const FIREX_ID_REGEX_STR = 'FireX-.*-(\\d\\d)(\\d\\d)(\\d\\d)-\\d{6}-\\d+';
const FIREX_ID_REGEX = new RegExp(FIREX_ID_REGEX_STR);

function captureEventState(event, tasksByUuid, taskNum) {
  let isNew = false;
  if (!_.has(event, 'uuid')) {
    // log
    return isNew;
  }

  const taskUuid = event.uuid;
  let task;
  if (!_.has(tasksByUuid, taskUuid)) {
    isNew = true;
    task = { uuid: taskUuid, task_num: taskNum };
    tasksByUuid[taskUuid] = task;
  } else {
    task = tasksByUuid[taskUuid];
  }

  const copyFields = ['hostname', 'parent_id', 'type', 'retries', 'firex_bound_args', 'flame_additional_data',
    'actual_runtime', 'support_location', 'utcoffset', 'type', 'code_url', 'firex_default_bound_args',
    'from_plugin', 'chain_depth', 'firex_result', 'exception', 'traceback'];
  copyFields.forEach((field) => {
    if (_.has(event, field)) {
      task[field] = event[field];
    }
  });

  const fieldsToTransforms = {
    name(e) {
      return { name: _.last(e.name.split('.')), long_name: e.name };
    },
    type(e) {
      const stateTypes = ['task-received', 'task-blocked', 'task-started', 'task-succeeded', 'task-shutdown',
        'task-failed', 'task-revoked', 'task-incomplete', 'task-unblocked'];
      if (_.includes(stateTypes, e.type)) {
        return { state: e.type };
      }
      return {};
    },
    url(e) {
      return { logs_url: e.url };
    },
  };

  _.keys(fieldsToTransforms).forEach((field) => {
    if (_.has(event, field)) {
      tasksByUuid[taskUuid] = _.merge(tasksByUuid[taskUuid], fieldsToTransforms[field](event));
    }
  });

  return isNew;
}

function parseRecFileContentsToNodesByUuid(recFileContents) {
  let taskNum = 1;
  const tasksByUuid = {};
  recFileContents.split(/\r?\n/).forEach((line) => {
    if (line && line.trim()) {
      const isNewTask = captureEventState(JSON.parse(line), tasksByUuid, taskNum);
      if (isNewTask) {
        taskNum += 1;
      }
    }
  });
  return tasksByUuid;
}

function getRoot(tasksByUuid) {
  // TODO: error handling for not exactly 1 root
  return _.filter(_.values(tasksByUuid), task => _.isNull(task.parent_id)
    || !_.has(tasksByUuid, task.parent_id))[0];
}

function flatGraphToTree(tasksByUuid) {
  const root = getRoot(tasksByUuid);
  // TODO: manipulating input is bad.
  // Initialize all nodes as having no children.
  _.values(tasksByUuid).forEach((n) => {
    n.children = [];
  });

  const tasksByParentId = _.groupBy(_.values(tasksByUuid), 'parent_id');
  let uuidsToCheck = [root.uuid];
  while (uuidsToCheck.length > 0) {
    // TODO: guard against loops.
    const curUuid = uuidsToCheck.pop();
    const curTask = tasksByUuid[curUuid];
    if (tasksByParentId[curUuid]) {
      curTask.children = tasksByParentId[curUuid];
    }
    uuidsToCheck = uuidsToCheck.concat(_.map(curTask.children, 'uuid'));
  }
  return root;
}

function isChainInterrupted(exception) {
  if (!exception) {
    return false;
  }
  return exception.trim().startsWith('ChainInterruptedException');
}

function runStatePredicate(runState) {
  return (runState.state === 'task-failed'
    // Show leaf nodes that are chain interrupted exceptions (e.g. RunChildFireX).
    && (!isChainInterrupted(runState.exception) || runState.isLeaf))
    || _.includes(['task-started', 'task-unblocked'], runState.state);
}

function createRunStateExpandOperations(runStateByUuid) {
  const showUuidsToUuids = _.pickBy(runStateByUuid, runStatePredicate);
  return _.mapValues(showUuidsToUuids,
    () => [{ targets: ['ancestors', 'self'], operation: 'expand' }]);
}

function calculateNodesPositionByUuid(nodesByUuid) {
  const newRootForLayout = flatGraphToTree(_.cloneDeep(nodesByUuid));
  // This calculates the layout (x, y per node) with dynamic node sizes.
  const verticalSpacing = 50;
  const horizontalSpacing = 0;
  const flextreeLayout = flextree({
    spacing: horizontalSpacing,
    nodeSize: node => [node.data.width, node.data.height + verticalSpacing],
  });
  const laidOutTree = flextreeLayout.hierarchy(newRootForLayout);
  // Modify the input tree, adding x, y, left, top attributes to each node.
  // This is the computed layout.
  flextreeLayout(laidOutTree);

  // The flextreeLayout does some crazy stuff to its input data, where as we only care
  // about a couple fields. Therefore just extract the fields.
  const calcedDimensionsByUuid = {};
  laidOutTree.each((dimensionNode) => {
    calcedDimensionsByUuid[dimensionNode.data.uuid] = {
      x: dimensionNode.left,
      y: dimensionNode.top,
    };
  });
  return calcedDimensionsByUuid;
}

function getCenteringTransform(rectToCenter, container, scaleBounds, verticalPadding) {
  // TODO: padding as percentage of available area.
  const widthToCenter = rectToCenter.right - rectToCenter.left;
  const heightToCenter = rectToCenter.bottom - rectToCenter.top + verticalPadding;
  const xScale = container.width / widthToCenter;
  const yScale = container.height / heightToCenter;
  const scale = _.clamp(_.min([xScale, yScale]), scaleBounds.min, scaleBounds.max);

  const scaledWidthToCenter = widthToCenter * scale;
  let xTranslate = rectToCenter.left * scale;

  // Center based on (scaled) extra horizontal or vertical space.
  if (Math.round(container.width) > Math.round(scaledWidthToCenter)) {
    const remainingHorizontal = container.width - scaledWidthToCenter;
    xTranslate -= remainingHorizontal / 2;
  }

  const scaledHeightToCenter = heightToCenter * scale;
  let yTranslate = (rectToCenter.top - verticalPadding / 2) * scale;
  if (Math.round(container.height) > Math.round(scaledHeightToCenter)) {
    const remainingVertical = container.height - scaledHeightToCenter;
    yTranslate -= remainingVertical / 2;
  }
  return { x: -xTranslate, y: -yTranslate, scale };
}

function isTaskStateIncomplete(state) {
  const incompleteStates = ['task-blocked', 'task-started', 'task-received', 'task-unblocked'];
  return _.includes(incompleteStates, state);
}

function hasIncompleteTasks(nodesByUuid) {
  return _.some(nodesByUuid, n => isTaskStateIncomplete(n.state));
}

function getDescendantUuids(nodeUuid, nodesByUuid) {
  let resultUuids = [];
  const nodesByParentId = _.groupBy(_.values(nodesByUuid), 'parent_id');
  let uuidsToCheck = _.map(nodesByParentId[nodeUuid], 'uuid');
  while (uuidsToCheck.length > 0) {
    resultUuids = resultUuids.concat(uuidsToCheck);
    const allToCheckChildren = _.map(
      uuidsToCheck, c => _.map(_.get(nodesByParentId, c, []), 'uuid'),
    );
    uuidsToCheck = _.flatten(allToCheckChildren);
  }
  return resultUuids;
}

function durationString(duractionSecs) {
  if (!_.isNumber(duractionSecs)) {
    return '';
  }

  const hours = Math.floor(duractionSecs / (60 * 60));
  const hoursInSecs = hours * 60 * 60;
  const mins = Math.floor((duractionSecs - hoursInSecs) / 60);
  const minsInSecs = mins * 60;
  const secs = Math.floor(duractionSecs - hoursInSecs - minsInSecs);

  let result = '';
  if (hours > 0) {
    result += `${hours}h `;
  }
  if (mins > 0) {
    result += `${mins}m `;
  }
  if (secs > 0) {
    result += `${secs}s`;
  }
  if (hours === 0 && mins === 0 && secs === 0) {
    result += '<1s';
  }
  return result;
}

function orderByTaskNum(nodesByUuid) {
  return _.mapValues(_.groupBy(_.sortBy(nodesByUuid, 'task_num'), 'uuid'), _.head);
}

/* eslint-disable */
function uuidv4() {
  return ([1e7] + -1e3 + -4e3 + -8e3 + -1e11).replace(/[018]/g, c =>
    (c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c / 4).toString(16)
  )
}
/* eslint-disable */

const successGreen = '#2A2';
const statusToProps = {
  'task-received': {
    background: '#888',
    priority: 6,
    display: 'Received',
  },
  'task-blocked': {
    background: '#888',
    priority: 7,
    display: 'Blocked',
  },
  'task-started': {
    background: 'cornflowerblue', // animated in SVG, not in HTML.
    priority: 1,
    display: 'In-Progress',
  },
  'task-succeeded': {
    background: successGreen,
    priority: 9,
    display: 'Success',
  },
  'task-shutdown': {
    background: successGreen,
    priority: 10,
    display: 'Success',
  },
  'task-failed': {
    background: '#900',
    priority: 3,
    display: 'Failed',
  },
  'task-revoked': {
    background: '#F40',
    priority: 4,
    display: 'Revoked',
  },
  'task-incomplete': {
    background: 'repeating-linear-gradient(45deg,#888,#888 5px,#444 5px,#444 10px)',
    priority: 5,
    display: 'Incomplete',
  },
  'task-unblocked': {
    background: 'cornflowerblue',
    priority: 2,
    display: 'In-Progress',
  },
};

function getNodeBackground(exception, state) {
  if (isChainInterrupted(exception)) {
    return 'repeating-linear-gradient(45deg,#888,#888 5px,#893C3C 5px,#F71818  10px)';
  }
  const defaultColor = '#888';
  return _.get(statusToProps, [state, 'background'], defaultColor);
}

function getPrioritizedTaskStateBackground(states) {
  const minState = _.minBy(states, s =>
    _.get(statusToProps, [s, 'priority'], 1000));
  return getNodeBackground(null, minState);
}

function getRunstateDisplayName(state) {
  return _.get(statusToProps, state, { display: 'Unknown' }).display;
}

function concatArrayMergeCustomizer(objValue, srcValue) {
  if (_.isArray(objValue)) {
    return objValue.concat(srcValue);
  }
}

function containsAll(superset, subset) {
  return _.difference(subset, superset).length === 0;
}

function containsAny(first, second) {
  return _.some(first, x => _.includes(second, x));
}

function getTaskNodeBorderRadius(chain_depth) {
  const isChained = Boolean(chain_depth);
  return !isChained ? '8px' : '';
}

function twoDepthAssign(existingData, newData) {
  const newIncomingKeyData = _.pickBy(newData, (v, k) => !_.has(existingData, k));
  const updateIncomingKeyData = _.pickBy(newData, (v, k) => _.has(existingData, k));

  const result = Object.assign({}, existingData, newIncomingKeyData);
  _.each(updateIncomingKeyData, (updateData, k) => {
    result[k] = Object.assign({}, existingData[k], updateData);
  });
  return result;
}

const DEFAULT_UI_CONFIG = {
  /*
   * Allowed values: webserver-file, socketio-param, socketio-origin
   */
  access_mode: 'socketio-origin',
  model_path_template: null,
  redirect_to_alive_flame: false,
  is_central: false,
  central_server: null,
  central_server_ui_path: null,
  central_documentation_url: null,
  firex_bin: null,
  logs_serving: {
    /*
   * Allowed values:
   *  central-webserver: prepends the 'central_webserver' to log paths. Assumes the central
   *    webserver supports listing directories.
   *
   *  local-webserver: path-only links to the same webserver serving the UI. Assumes
   *    the webserver serving the UI supports listing directories.
   *
   *  google-cloud-storage: fetches logs and directory (object) listings from the URL
   *    format specified by 'url_format'. Directories are expected to be in google storage
   *    format: https://cloud.google.com/storage/docs/listing-objects.
   */
    serve_mode: 'central-webserver',
    /*
     * supports firex_id in lodash templating format, for example:
     *  https://www.googleapis.com/storage/v1/b/some_bucket/o?prefix=runs/<%- firex_id %>
     */
    url_format: null, // only needed for 'google-cloud-storage' mode.
  }
};

function isRequiredAccessModeDataPresent(accessMode, route) {
  if (accessMode === 'socketio-origin') {
    // Always have origin, no data needed from route.
    return true;
  }
  if (accessMode === 'webserver-file' && !_.isNil(route.params.inputFireXId)) {
    return true;
  }
  if (accessMode === 'socketio-param' && !_.isNil(route.query.flameServer)) {
    return true;
  }
  return false;
}

function supportsFindView(accessMode) {
  return accessMode === 'webserver-file';
}

function fetchUiConfig() {
  return fetch('flame-ui-config.json')
    .then((r) => {
        if (r.ok) {
          return r.json();
        }
        return DEFAULT_UI_CONFIG;
      },
      () => DEFAULT_UI_CONFIG)
    .catch(() => DEFAULT_UI_CONFIG);
}

function fetchRunModelMetadata(firexId, modelPathTemplate) {
  if (!isFireXIdValid(firexId)) {
    return new Promise((resolve) => resolve(false));
  }
  return fetch(templateFireXId(modelPathTemplate, firexId))
    .then(r => r.json(), () => false)
    .catch(() => false);
}

function tasksViewKeyRouteChange(to, from, next, setUiConfigFn) {
  fetchUiConfig().then(uiConfig => {
    if (isRequiredAccessModeDataPresent(uiConfig.access_mode, to)) {
      // If UI Config indicates redirect, check if the flame server is still alive & redirect
      // if it is.
      if (uiConfig.redirect_to_alive_flame && to.params.inputFireXId
          // Avoid re-querying the flame server if the firexId hasn't changed, since timeouts
          // aren't cached and introduce delays between view transitions.
          && to.params.inputFireXId !== from.params.inputFireXId) {
        fetchRunModelMetadata(to.params.inputFireXId, uiConfig.model_path_template)
          .then(
            (runMetadata) => {
              if (runMetadata === false) {
                // If can't fetch metadata, route to error
                next(errorRoute(`Can't find data for ${to.params.inputFireXId}`));
              } else {
                return redirectToFlameIfAlive(runMetadata.flame_url, to.path);
              }
            })
          // If flame server is not alive, just route to local task view.
          .catch(() => next(vm => setUiConfigFn(vm, uiConfig)))
      } else {
        next(vm => setUiConfigFn(vm, uiConfig));
      }
    } else {
      // Missing key required to show task data -- send to find view if supported.
      if (supportsFindView(uiConfig.access_mode)) {
        next('/find');
      } else {
        next(errorRoute(`Can't show tasks for access mode '${uiConfig.access_mode}'.`));
      }
    }
  });
}

function errorRoute(message) {
  console.error(message);
  return { name: 'error', query: { message } };
}

function findRunPathSuffix(path) {
  const pathFireXIdRegex = new RegExp(`^#?/.*${FIREX_ID_REGEX_STR}`);
  if (pathFireXIdRegex.test(path)) {
    return _.replace(path, pathFireXIdRegex, '')
  }
  return '/';
}

function redirectToFlameIfAlive(flameServerUrl, path) {
  return fetchWithTimeout(
    (new URL('/alive', flameServerUrl)).toString(),
    {mode: 'no-cors'},
    5000,
    () => new Error('fetch timeout'))
    // If the flame for the selected run is still alive, redirect the user there.
    .then(() => {
      const redirectPath = findRunPathSuffix(path);
      window.location.href = (new URL(`#${redirectPath}`, flameServerUrl)).toString();
    });
}

function templateFireXId(template, firexId) {
  const firexIdParts = getFireXIdParts(firexId);
  const templateOptions = { evaluate: null, interpolate: null };
  return _.template(template, templateOptions)(firexIdParts);
}

function isFireXIdValid(firexId) {
  return FIREX_ID_REGEX.test(firexId)
}

function getFireXIdParts(firexId) {
  if (!isFireXIdValid(firexId)) {
    throw Error(`Invalid firex_id can't have parts extracted: ${firexId}`);
  }
  const match = FIREX_ID_REGEX.exec(firexId);
  return {
    'year': match[1],
    'month': match[2],
    'day': match[3],
    'firex_id': firexId,
  };
}

function fetchWithTimeout(fetchUrl, fetchOptions, timeout = 5000, onTimeout) {
  return Promise.race([
        fetch(fetchUrl, fetchOptions),
        new Promise((_, reject) =>
            setTimeout(() => reject(onTimeout()), timeout)
        )
    ]);
}

// See https://vuejs.org/v2/guide/migration.html#dispatch-and-broadcast-replaced
const eventHub = new Vue();

export {
  parseRecFileContentsToNodesByUuid,
  eventHub,
  calculateNodesPositionByUuid,
  getCenteringTransform,
  isTaskStateIncomplete,
  hasIncompleteTasks,
  isChainInterrupted,
  getDescendantUuids,
  durationString,
  orderByTaskNum,
  uuidv4,
  getNodeBackground,
  getPrioritizedTaskStateBackground,
  getRunstateDisplayName,
  createRunStateExpandOperations,
  concatArrayMergeCustomizer,
  containsAll,
  containsAny,
  getTaskNodeBorderRadius,
  twoDepthAssign,
  isFireXIdValid,
  getFireXIdParts,
  tasksViewKeyRouteChange,
  fetchUiConfig,
  templateFireXId,
  redirectToFlameIfAlive,
  fetchRunModelMetadata,
  findRunPathSuffix,
  errorRoute,
};
