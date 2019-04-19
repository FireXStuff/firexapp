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

function getUuidsToRoot(node, nodesByUuid) {
  return [node.uuid].concat(getAncestorUuids(node, nodesByUuid));
}

function nodesInRootLeafPathWithFailureOrInProgress(nodesByUuid) {
  const failurePredicate = node => (node.state === 'task-failed'
    // Show leaf nodes that are chain interrupted exceptions (e.g. RunChildFireX).
    && (!isChainInterrupted(node.exception) || node.children_uuids.length === 0))
    || node.state === 'task-started';
  if (_.some(_.values(nodesByUuid), failurePredicate)) {
    const parentIds = _.map(_.values(nodesByUuid), 'parent_id');
    // TODO: why not check node.children_uuids.length?
    const leafNodes = _.filter(_.values(nodesByUuid), n => !_.includes(parentIds, n.uuid));

    const leafUuidPathsToRoot = _.map(leafNodes, l => getUuidsToRoot(l, nodesByUuid));
    const uuidPathsPassingPredicate = _.filter(leafUuidPathsToRoot,
      pathUuids => _.some(_.values(_.pick(nodesByUuid, pathUuids)), failurePredicate));

    return _.flatten(uuidPathsPassingPredicate);
  }
  return _.keys(nodesByUuid);
}

function createRunStateCollapseOperations(nodesByUuid) {
  const showPredicate = node => (node.state === 'task-failed'
    // Show leaf nodes that are chain interrupted exceptions (e.g. RunChildFireX).
    && (!isChainInterrupted(node.exception) || node.children_uuids.length === 0))
    || node.state === 'task-started';
  const showUuidsToUuids = _.keyBy(_.map(_.filter(nodesByUuid, showPredicate), 'uuid'));
  const operationsByUuid = _.mapValues(showUuidsToUuids, uuid => ({
    [[uuid]]: {
      ancestors: { operation: 'expand', priority: 3, stateSource: 'run-state' },
      self: { operation: 'expand', priority: 3, stateSource: 'run-state' },
      descendants: { operation: 'expand', priority: 3, stateSource: 'run-state' },
    },
  }));
  const rootUuid = getRoot(nodesByUuid).uuid;
  const rootOp = {
    [[rootUuid]]: { descendants: { operation: 'collapse', priority: 4, stateSource: 'run-state' } },
  };

  // Keys are unique, so expect no overwritting from _.merge.
  return _.reduce(operationsByUuid, _.merge, rootOp);
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

function getAllDescendantsUuidsInclusive(uuids, nodesByUuid) {
  return _.uniq(uuids.concat(
    _.flatten(_.map(uuids, cc => getDescendantUuids(cc, nodesByUuid))),
  ));
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

function rollupTaskStatesBackground(states) {
  const minState = _.minBy(states, s => _.get(statusToProps, [s, 'priority'], -1));
  return getNodeBackground(null, minState);
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
  return _.filter(
    _.map(uuids, (u, i) => _.merge({distance: i + 1}, _.get(collapseOpsByUuid, [u, target], {}))),
    // Only keep entries that had data an operation to begin with.
    u => _.has(u, 'operation'))
}

function _getAllAffectingCollapseState(
  curNodeUuid, collapseOpsByUuid, ancestorUuidsByUuid, descendantUuidsByUuid,
  isParentCollapsed) {

  const expandCollapseInfluences = {
    self: _.has(collapseOpsByUuid, [curNodeUuid, 'self'])
      ? [_.merge({distance: 0}, _.get(collapseOpsByUuid, [curNodeUuid, 'self']))] : [],

    ancestor:
      _getOpsAndDistanceForTarget(ancestorUuidsByUuid[curNodeUuid], collapseOpsByUuid, 'descendants'),

    descendant:
      _getOpsAndDistanceForTarget(descendantUuidsByUuid[curNodeUuid], collapseOpsByUuid, 'ancestors'),

    // If no reason to show, and parent is collapsed, have a very low-priority collapse.
    parentCollapse: isParentCollapsed ? [{ operation: 'collapse', priority: 10 }] : [],
  }
  const targetNameToPriority = {
    self: 1,
    ancestor: 2,
    descendant: 3,
    parentCollapse: 4
  }

  return _.flatMap(expandCollapseInfluences,
    (ops, targetName) => _.map(ops,
        op =>_.merge({
          target: targetName,
          targetPriority: targetNameToPriority[targetName],
        }, op)))
}

function _findMinPriorityOp(affectingOps) {
  // Unfortunately need to sort since minBy doesn't work like sortBy w.r.t. array of properties.
  const sorted = _.sortBy(affectingOps, ['priority', 'targetPriority', 'distance']);
  return _.head(sorted);
}

function resolveCollapseStatusByUuid(nodesByUuid, collapseOpsByUuid) {
  const resultNodesByUuid = {}
  let toCheck = [getRoot(nodesByUuid)]
  const nodesByParentId = _.groupBy(_.values(nodesByUuid), 'parent_id');
  const ancestorUuidsByUuid = getAncestorsByUuid(nodesByUuid);
  const descendantUuidsByUuid = getDescendantsByUuid(nodesByUuid);
  while (toCheck.length > 0) {
    const curNode = toCheck.pop();

    const isParentCollapsed = _.get(resultNodesByUuid, [curNode.parent_id, 'collapsed'], false)
    const affectingOps = _getAllAffectingCollapseState(
      curNode.uuid, collapseOpsByUuid, ancestorUuidsByUuid, descendantUuidsByUuid,
      isParentCollapsed);

    const minPriorityOp = _findMinPriorityOp(affectingOps);
    resultNodesByUuid[curNode.uuid] = {
      uuid: curNode.uuid,
      parent_id: curNode.parent_id,
      // If no rules affect this node (minPriorityOp undefined), default to false.
      collapsed: _.isUndefined(minPriorityOp) ? false : minPriorityOp.operation === 'collapse',
      affectingOps,
      minPriorityOp,
    };
    toCheck = toCheck.concat(_.get(nodesByParentId, curNode.uuid, []))
  }
  return resultNodesByUuid;
}

function createSimpleCollapsedNode(taskUuid, parentId, uuidFn) {
  return {
    uuid: uuidFn(),
    parent_id: parentId,
    collapsed: true,
    // TODO: this should be allContainedCollapsedNodeUuids
    allRepresentedNodeUuids: [taskUuid],
    // TODO: Needs a better name. 'rootRepresentedUuids'.
    representedChildrenUuids: [taskUuid],
  }
}

function getCollapsedGraphByNodeUuid(collapsedByUuid, uuidFn) {
  const nodesByParentId = getNodesByParentId(collapsedByUuid)
  return recursiveGetCollapseNodes(getRoot(collapsedByUuid),
    nodesByParentId, null, uuidFn);
}

function recursiveGetCollapseNodes(node, childrenByUuid, collapseParent, uuidFn) {
  let resultNode;
  if (node.collapsed) {
    if (!_.isNil(collapseParent) && collapseParent.collapsed) {
      // both this node and its parent is collapsed, so re-use parent except
      // update it to include this node's UUID as being represented.
      let newAllCollapsed = collapseParent.allRepresentedNodeUuids.concat([node.uuid])
      resultNode = _.merge({}, collapseParent, {allRepresentedNodeUuids: newAllCollapsed})
    } else {
      // Parent isn't collapsed and this node is collapsed, need a new collapsed node.
      resultNode = createSimpleCollapsedNode(
        node.uuid, _.get(collapseParent, 'uuid', null), uuidFn)
    }
  } else {
    // Uncollapsed. Keep node, but possibly change parent if this nodes parent is collapsed.
    resultNode = _.merge({}, node, {parent_id: _.get(collapseParent, 'uuid', null)})
  }

  let children = childrenByUuid[node.uuid]
  let resultDescendants = _.map(children,
      c => recursiveGetCollapseNodes(c, childrenByUuid, resultNode, uuidFn))

  let combinedDescendantsByUuid = {}
  combinedDescendantsByUuid[resultNode.uuid] = resultNode
  _.each(resultDescendants, childrenDescendentsByUUid => _.each(childrenDescendentsByUUid, d => {
      if (d.uuid === resultNode.uuid) {
        // Child node collapsed to current result node, include its descendants and children
        // in current result node.
        resultNode.allRepresentedNodeUuids = _.uniq(
          resultNode.allRepresentedNodeUuids.concat(d.allRepresentedNodeUuids))
      }
      else {
        // All UUIDs are unique, except possibly children that collapsed to the resultNode.
        combinedDescendantsByUuid[d.uuid] = d
      }
  }))

  // All collapsed children should be represented by a single node, so combine them here:
  const collapsedChildren = _.filter(combinedDescendantsByUuid,
      d => d.collapsed && d.parent_id === resultNode.uuid)
  if (!_.isEmpty(collapsedChildren)) {
    const combinedCollapseNode = {
      uuid: uuidFn(),
      parent_id: resultNode.uuid,
      collapsed: true,
      allRepresentedNodeUuids: _.flatMap(collapsedChildren, 'allRepresentedNodeUuids'),
      representedChildrenUuids: _.flatMap(collapsedChildren, 'representedChildrenUuids'),
    }
    resultNode.collapsedChildrenUuids = _.map(collapsedChildren, 'uuid');

    // remove all the nodes combined in to a single collapsed child from the result.
    combinedDescendantsByUuid = _.pickBy(combinedDescendantsByUuid,
        d => !_.includes(resultNode.collapsedChildrenUuids, d.uuid))

    // Update references from the collapsed children to the newly created combined node.
    _.each(_.filter(combinedDescendantsByUuid,
        d => _.includes(resultNode.collapsedChildrenUuids, d.parent_id) ),
        d => d.parent_id = combinedCollapseNode.uuid);
    // Add the newly created combined collapse node to the result.
    combinedDescendantsByUuid[combinedCollapseNode.uuid] = combinedCollapseNode;
  }

  return combinedDescendantsByUuid;
}

function createCollapseNodesByUuid(nodesByUuid, collapseOpsByUuid) {
  const collapseStateByUuid = resolveCollapseStatusByUuid(nodesByUuid, collapseOpsByUuid);
  return getCollapsedGraphByNodeUuid(collapseStateByUuid, uuidv4)
}

function createCollapseEvent(uuids, operation, target) {
  const priority = -(new Date).getTime();
  return _.mapValues(_.keyBy(uuids),
    () => _.identity({[[target]]: {operation: operation, priority: priority, stateSource: 'ui'}}));
}

// See https://vuejs.org/v2/guide/migration.html#dispatch-and-broadcast-replaced
const eventHub = new Vue();

export {
  parseRecFileContentsToNodesByUuid,
  eventHub,
  nodesInRootLeafPathWithFailureOrInProgress,
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
  getAllDescendantsUuidsInclusive,
  getNodeBackground,
  rollupTaskStatesBackground,
  resolveCollapseStatusByUuid,
  getCollapsedGraphByNodeUuid,
  createCollapseNodesByUuid,
  createCollapseEvent,
  createRunStateCollapseOperations,
};
