import _ from 'lodash';
import Vue from 'vue';
import { flextree } from 'd3-flextree';

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
    'local_received', 'actual_runtime', 'support_location', 'utcoffset', 'type', 'code_url', 'firex_default_bound_args',
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
        'task-failed', 'task-revoked', 'task-incomplete', 'task-completed'];
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
  const tasks = _.values(tasksByUuid);
  _.each(tasksByUuid, (n) => {
    n.children_uuids = _.map(_.filter(tasks, t => t.parent_id === n.uuid), 'uuid');
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

function getAncestorUuids(node, nodesByUuid) {
  let curNode = node;
  const resultUuids = [];
  while (!_.isNil(curNode.parent_id) && _.has(nodesByUuid, curNode.parent_id)) {
    curNode = nodesByUuid[curNode.parent_id];
    resultUuids.push(curNode.uuid);
  }
  return resultUuids;
}

function runStatePredicate(node) {
  return (node.state === 'task-failed'
    // Show leaf nodes that are chain interrupted exceptions (e.g. RunChildFireX).
    && (!isChainInterrupted(node.exception) || node.children_uuids.length === 0))
    || node.state === 'task-started';
}

function createRunStateExpandOperations(nodesByUuid) {
  const showUuidsToUuids = _.keyBy(_.map(_.filter(nodesByUuid, runStatePredicate), 'uuid'));
  return _.mapValues(showUuidsToUuids,
    () => [{ targets: ['ancestors', 'self', 'descendants'], operation: 'expand' }]);
}

function createCollapseRootOperation(nodesByUuid) {
  const rootUuid = getRoot(nodesByUuid).uuid;
  return {
    [[rootUuid]]: [{ targets: ['descendants'], operation: 'collapse' }],
  };
}

function calculateNodesPositionByUuid(nodesByUuid) {
  const newRootForLayout = flatGraphToTree(_.cloneDeep(nodesByUuid));
  // This calculates the layout (x, y per node) with dynamic node sizes.
  const verticalSpacing = 50;
  const horizontalSpacing = 50;
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

function routeTo(vm, name, params) {
  const route = {
    name,
    query: {
      logDir: vm.$route.query.logDir,
      flameServer: vm.$route.query.flameServer,
    },
  };
  if (params) {
    route.params = params;
  }
  return route;
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

function socketRequestResponse(socket, requestEvent, successEvent, failedEvent, timeout) {
  let responseReceived = false;
  socket.on(successEvent.name, (data) => {
    responseReceived = true;
    successEvent.fn(data);
    socket.off(successEvent.name);
    if (!_.isNil(failedEvent)) {
      socket.off(failedEvent.name);
    }
  });
  if (!_.isNil(failedEvent)) {
    socket.on(failedEvent.name, (data) => {
      responseReceived = true;
      failedEvent.fn(data);
      socket.off(successEvent.name);
      socket.off(failedEvent.name);
    });
  }

  if (!_.isNil(timeout)) {
    setTimeout(() => {
      if (!responseReceived) {
        timeout.fn();
      }
    }, timeout.waitTime);
  }
  if (requestEvent.data !== undefined) {
    socket.emit(requestEvent.name, requestEvent.data);
  } else {
    socket.emit(requestEvent.name);
  }
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

  let result = 'time: ';
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
  'task-received': {background: '#888', priority: 6},
  'task-blocked': {background: '#888', priority: 7},
  'task-started': {
    background: 'cornflowerblue', // animated in SVG, not in HTML.
    priority: 1,
  },
  'task-succeeded': {background: successGreen, priority: 9},
  'task-shutdown': {background: successGreen, priority: 10},
  'task-failed': {background: '#900', priority: 3},
  'task-revoked': {background: '#F40', priority: 4},
  'task-incomplete': {
    background: 'repeating-linear-gradient(45deg,#888,#888 5px,#444 5px,#444 10px)',
    priority: 5,
  },
  'task-completed': {background: '#AAA', priority: 8},
  'task-unblocked': {background: 'cornflowerblue', priority: 2},
};

function getNodeBackground(exception, state) {
  if (isChainInterrupted(exception)) {
    return 'repeating-linear-gradient(45deg,#888,#888 5px,#893C3C 5px,#F71818  10px)';
  }
  const defaultColor = '#888';
  return _.get(statusToProps, [state, 'background'], defaultColor);
}

function getPrioritizedTaskStateBackgrounds(states) {
  const orderedStates = _.sortBy(states, s =>
    _.get(statusToProps, [s, 'priority'], -1));
  return _.map(_.uniq(orderedStates), s => getNodeBackground(null, s));
}

function getNodesByParentId(nodesByUuid) {
  const nodesByParentId = _.groupBy(_.values(nodesByUuid), 'parent_id');
  return _.merge(_.mapValues(nodesByUuid, n => []), nodesByParentId);
}

//TODO: don't use getAncestorUUIDs, only walk the tree once instead.
function getAncestorsByUuid(nodesByUuid) {
  return _.mapValues(nodesByUuid, n => getAncestorUuids(n, nodesByUuid))
}

//TODO: don't use getDescendantUuids, only walk the tree once instead.
function getDescendantsByUuid(nodesByUuid) {
  // Descendants will be ordered nearest to farthest from key node uuid.
  return _.mapValues(nodesByUuid, n => getDescendantUuids(n.uuid, nodesByUuid))
}

function _findCollapsedAncestors(ancestorUuids, calcedAncestorCollapseStateByUuid) {
 return _.filter(ancestorUuids,
      a => _.get(calcedAncestorCollapseStateByUuid, [a, 'collapsed'], false))
}

function _getOpsAndDistanceForTarget(uuids, collapseOpsByUuid, target) {
  // Note we map before we filter so that we have the real distance, not the distance of only
  // nodes with operations.
  const uuidsToDistance = _.keyBy(_.map(uuids, (u, i) => ({uuid: u, distance: i + 1})),
    'uuid');
  const opsWithTargetByUuid = _.mapValues(collapseOpsByUuid,
      ops => _.filter(ops, o => _.includes(o.targets, target)));
  const opsAndDistancesByUuid = _.mapValues(opsWithTargetByUuid,
    (ops, uuid) => _.map(ops,
        o => _.merge({distance: uuidsToDistance[uuid].distance}, o)));
  return _.flatten(_.values(opsAndDistancesByUuid))
}

function _getAllOpsAffectingCollapseState(
  curNodeUuid, collapseOpsByUuid, ancestorUuidsByUuid, descendantUuidsByUuid,
  isParentCollapsed) {

  const expandCollapseInfluences = {
    self: _.filter(_.get(collapseOpsByUuid, curNodeUuid, []),
    op => _.includes(op.targets, 'self')),

    ancestor:
      _getOpsAndDistanceForTarget(ancestorUuidsByUuid[curNodeUuid],
        _.pick(collapseOpsByUuid, ancestorUuidsByUuid[curNodeUuid]), 'descendants'),

    descendant:
      _getOpsAndDistanceForTarget(descendantUuidsByUuid[curNodeUuid],
        _.pick(collapseOpsByUuid, descendantUuidsByUuid[curNodeUuid]), 'ancestors'),

    grandparent:
      _getOpsAndDistanceForTarget(_.tail(ancestorUuidsByUuid[curNodeUuid]),
        _.pick(collapseOpsByUuid, _.tail(ancestorUuidsByUuid[curNodeUuid])), 'grandchildren'),

    // If no reason to show, and parent is collapsed, have a very low-priority collapse.
    // TODO: consider if this is the right place to do this. Could exclude these nodes downstream
    // instead.
    parent: isParentCollapsed ? [{ operation: 'collapse', priority: 10 }] : [],
  }
  const targetNameToPriority = {
    self: 1,
    ancestor: 2,
    descendant: 3,
    grandparent: 4,
    parent: 5
  }
  // at otherwise equal priorities, expand beats collapse.
  const operationToPriority = {
    'expand': 0,
    'collapse': 1,
  };

  return _.flatMap(expandCollapseInfluences,
    (ops, targetName) => _.map(ops,
        op =>_.merge({
          target: targetName,
          targetPriority: targetNameToPriority[targetName],
          opPriority: operationToPriority[op.operation],
        }, _.omit(op, ['targets']))))
}

function _findMinPriorityOp(affectingOps) {
  // Unfortunately need to sort since minBy doesn't work like sortBy w.r.t. array of properties.
  const sorted = _.sortBy(affectingOps, ['priority', 'opPriority', 'targetPriority', 'distance']);
  return _.head(sorted);
}

function resolveCollapseStatusByUuid(nodesByUuid, collapseOpsByUuid) {
  const resultNodesByUuid = {}
  const root = getRoot(nodesByUuid);
  let toCheck = [root]
  const nodesByParentId = _.groupBy(_.values(nodesByUuid), 'parent_id');
  const ancestorUuidsByUuid = getAncestorsByUuid(nodesByUuid);
  const descendantUuidsByUuid = getDescendantsByUuid(nodesByUuid);
  while (toCheck.length > 0) {
    const curNode = toCheck.pop();

    const isParentCollapsed = _.get(resultNodesByUuid, [curNode.parent_id, 'collapsed'], false)
    const affectingOps = _getAllOpsAffectingCollapseState(
      curNode.uuid, collapseOpsByUuid, ancestorUuidsByUuid, descendantUuidsByUuid,
      isParentCollapsed);

    const minPriorityOp = _findMinPriorityOp(affectingOps);
    resultNodesByUuid[curNode.uuid] = {
      parent_id: curNode.parent_id,
      uuid: curNode.uuid,
      // If no rules affect this node (minPriorityOp undefined), default to false.
      collapsed: _.isUndefined(minPriorityOp) ? false : minPriorityOp.operation === 'collapse',
      affectingOps,
      minPriorityOp,
    };
    toCheck = toCheck.concat(_.get(nodesByParentId, curNode.uuid, []))
  }
  // TODO: this is unfortunately necessary b/c collapsing the root shows nothing.
  // Consider supporting 'collapse down to nearest uncollapsed descendant'
  resultNodesByUuid[root.uuid].collapsed = false;

  return resultNodesByUuid;
}

function createSimpleCollapsedNode(taskUuid) {
  return {
    // TODO: this should be allContainedCollapsedNodeUuids
    collapsedDescendantUuids: [taskUuid],
  }
}

function getCollapsedGraphByNodeUuid(collapsedByUuid) {
  const nodesByParentId = getNodesByParentId(collapsedByUuid)
  // TODO: implicitly, collapse operations on the root node are ignored. Could collapse 'down',
  //  though it's unclear if that's preferable.
  return recursiveGetCollapseNodes(getRoot(collapsedByUuid).uuid,
    nodesByParentId);
}

function recursiveGetCollapseNodes(curUuid, nodesByParentId) {

  let childResults = _.map(nodesByParentId[curUuid], c =>
    recursiveGetCollapseNodes(c.uuid, nodesByParentId));
  let resultDescendantsByUuid = _.reduce(childResults, _.merge, {});

  let collapsedChildren = _.filter(nodesByParentId[curUuid], c => c.collapsed);
  let collapsedDescendantUuids = _.flatMap(collapsedChildren,
      c => [c.uuid].concat(resultDescendantsByUuid[c.uuid]))

  return _.assign({[[curUuid]]: collapsedDescendantUuids}, resultDescendantsByUuid);
}

function createCollapseEvent(uuids, operation, target) {
  const priority = -(new Date).getTime();
  return _.mapValues(_.keyBy(uuids),
    () => [{
      targets: [target],
      operation: operation,
      priority: priority,
      stateSource: 'ui'}]);
}

function loadDisplayConfigs() {
  if (_.has(localStorage, 'displayConfigs')) {
    try {
      // TODO: validate stored configs.
      return JSON.parse(localStorage.getItem('displayConfigs'));
    } catch (e) {
      // Delete bad persisted state, provide default.
      localStorage.removeItem('displayConfigs');
    }
  }
  return [];
}

function concatArrayMergeCustomizer(objValue, srcValue) {
  if (_.isArray(objValue)) {
    return objValue.concat(srcValue);
  }
}

function containsAll(superset, subset) {
  return _.difference(subset, superset).length === 0;
}

// See https://vuejs.org/v2/guide/migration.html#dispatch-and-broadcast-replaced
const eventHub = new Vue();

export {
  parseRecFileContentsToNodesByUuid,
  eventHub,
  calculateNodesPositionByUuid,
  getCenteringTransform,
  socketRequestResponse,
  routeTo,
  isTaskStateIncomplete,
  hasIncompleteTasks,
  isChainInterrupted,
  getDescendantUuids,
  durationString,
  orderByTaskNum,
  getAncestorUuids,
  uuidv4,
  getNodeBackground,
  getPrioritizedTaskStateBackgrounds,
  resolveCollapseStatusByUuid,
  getCollapsedGraphByNodeUuid,
  createCollapseEvent,
  createRunStateExpandOperations,
  loadDisplayConfigs,
  concatArrayMergeCustomizer,
  createCollapseRootOperation,
  containsAll,
};
