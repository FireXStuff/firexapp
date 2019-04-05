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


function flatGraphToTree(tasksByUuid) {
  // TODO: error handling for not exactly 1 root
  const root = _.filter(_.values(tasksByUuid), task => _.isNull(task.parent_id)
    || !_.has(tasksByUuid, task.parent_id))[0];
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
  // TODO: shouldn't be necessary.
  return _.keys(nodesByUuid);
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
  const resultUuids = [];
  let uuidsToCheck = _.clone(nodesByUuid[nodeUuid].children_uuids);
  while (uuidsToCheck.length > 0) {
    const curNodeUuid = uuidsToCheck.pop();
    if (!_.includes(resultUuids, curNodeUuid)) {
      const childrenIds = nodesByUuid[curNodeUuid].children_uuids;
      uuidsToCheck = uuidsToCheck.concat(childrenIds);
      resultUuids.push(curNodeUuid);
    }
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
};
