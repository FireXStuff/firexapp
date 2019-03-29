import _ from 'lodash'
import Vue from 'vue'
import {flextree} from 'd3-flextree'

// See https://vuejs.org/v2/guide/migration.html#dispatch-and-broadcast-replaced
let eventHub = new Vue()

export {
  // invokePerNode,
  parseRecFileContentsToNodesByUuid,
  // flatGraphToTree,
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
}

// function invokePerNode (root, fn) {
//   let doneUuids = []
//   let nodesToCheck = [root]
//   while (nodesToCheck.length > 0) {
//     let node = nodesToCheck.pop()
//     // Avoid loops in graph.
//     if (!_.includes(doneUuids, node.uuid)) {
//       doneUuids.push(node.uuid)
//       fn(node)
//       nodesToCheck = nodesToCheck.concat(node.children)
//     }
//   }
// }

function parseRecFileContentsToNodesByUuid (recFileContents) {
  let taskNum = 1
  let tasksByUuid = {}
  recFileContents.split(/\r?\n/).forEach(function (line) {
    if (line && line.trim()) {
      let isNewTask = captureEventState(JSON.parse(line), tasksByUuid, taskNum)
      if (isNewTask) {
        taskNum += 1
      }
    }
  })
  let tasks = _.values(tasksByUuid)
  _.each(tasksByUuid, n => {
    n.children_uuids = _.map(_.filter(tasks, t => t.parent_id === n.uuid), 'uuid')
  })
  return tasksByUuid
}

function captureEventState (event, tasksByUuid, taskNum) {
  let isNew = false
  if (!_.has(event, 'uuid')) {
    // log
    return isNew
  }

  let taskUuid = event['uuid']
  let task
  if (!_.has(tasksByUuid, taskUuid)) {
    isNew = true
    task = {uuid: taskUuid, task_num: taskNum}
    tasksByUuid[taskUuid] = task
  } else {
    task = tasksByUuid[taskUuid]
  }

  let copyFields = ['hostname', 'parent_id', 'type', 'retries', 'firex_bound_args', 'flame_additional_data',
    'local_received', 'actual_runtime', 'support_location', 'utcoffset', 'type', 'code_url', 'firex_default_bound_args',
    'from_plugin', 'chain_depth', 'firex_result', 'exception', 'traceback']
  copyFields.forEach(function (field) {
    if (_.has(event, field)) {
      task[field] = event[field]
    }
  })

  let fieldsToTransforms = {
    'name': function (event) {
      return {'name': _.last(event.name.split('.')), 'long_name': event.name}
    },
    'type': function (event) {
      let stateTypes = ['task-received', 'task-blocked', 'task-started', 'task-succeeded', 'task-shutdown',
        'task-failed', 'task-revoked', 'task-incomplete', 'task-completed']
      if (_.includes(stateTypes, event.type)) {
        return {'state': event.type}
      }
      return {}
    },
    'url': function (event) {
      return {'logs_url': event.url}
    },
  }

  _.keys(fieldsToTransforms).forEach(function (field) {
    if (_.has(event, field)) {
      tasksByUuid[taskUuid] = _.merge(tasksByUuid[taskUuid], fieldsToTransforms[field](event))
    }
  })

  return isNew
}

function flatGraphToTree (tasksByUuid) {
  // TODO: error handling for not exactly 1 root
  let root = _.filter(_.values(tasksByUuid), function (task) {
    return _.isNull(task.parent_id) || !_.has(tasksByUuid, task.parent_id)
  })[0]
  // TODO: manipulating input is bad.
  // Initialize all nodes as having no children.
  _.values(tasksByUuid).forEach(function (n) {
    n['children'] = []
  })

  let tasksByParentId = _.groupBy(_.values(tasksByUuid), 'parent_id')
  let uuidsToCheck = [root.uuid]
  while (uuidsToCheck.length > 0) {
    // TODO: guard against loops.
    let curUuid = uuidsToCheck.pop()
    let curTask = tasksByUuid[curUuid]
    if (tasksByParentId[curUuid]) {
      curTask['children'] = tasksByParentId[curUuid]
    }
    uuidsToCheck = uuidsToCheck.concat(_.map(curTask['children'], 'uuid'))
  }
  return root
}

function isChainInterrupted (exception) {
  if (!exception) {
    return false
  }
  return exception.trim().startsWith('ChainInterruptedException')
}

function nodesInRootLeafPathWithFailureOrInProgress (nodesByUuid) {
  let failurePredicate = (node) => {
    return (node.state === 'task-failed' &&
              // Show leaf nodes that are chain interrupted exceptions (e.g. RunChildFireX).
              (!isChainInterrupted(node.exception) || node.children_uuids.length === 0)) ||
            node.state === 'task-started'
  }
  if (_.some(_.values(nodesByUuid), failurePredicate)) {
    let parentIds = _.map(_.values(nodesByUuid), 'parent_id')
    // TODO: why not check node.children_uuids.length?
    let leafNodes = _.filter(_.values(nodesByUuid), n => !_.includes(parentIds, n.uuid))
    let leafUuidPathsToRoot = _.map(leafNodes, l => getUuidsToRoot(l, nodesByUuid))
    let uuidPathsPassingPredicate = _.filter(leafUuidPathsToRoot,
      pathUuids => _.some(_.values(_.pick(nodesByUuid, pathUuids)), failurePredicate))
    let keepUuids = _.flatten(uuidPathsPassingPredicate)
    return keepUuids
  }
  // TODO: shouldn't be necessary.
  return _.keys(nodesByUuid)
}

function getAncestorUuids (node, nodesByUuid) {
  let curNode = node
  let resultUuids = []
  while (true) {
    if (_.isNil(curNode.parent_id) || !_.has(nodesByUuid, curNode.parent_id)) {
      break
    }
    curNode = nodesByUuid[curNode.parent_id]
    resultUuids.push(curNode.uuid)
  }
  return resultUuids
}

function getUuidsToRoot (node, nodesByUuid) {
  return [node.uuid].concat(getAncestorUuids(node, nodesByUuid))
}

function calculateNodesPositionByUuid (nodesByUuid) {
  let newRootForLayout = flatGraphToTree(_.cloneDeep(nodesByUuid))
  // This calculates the layout (x, y per node) with dynamic node sizes.
  let verticalSpacing = 50
  let horizontalSpacing = 25
  let flextreeLayout = flextree({
    spacing: horizontalSpacing,
    nodeSize: node => [node.data.width, node.data.height + verticalSpacing],
  })
  let laidOutTree = flextreeLayout.hierarchy(newRootForLayout)
  // Modify the input tree, adding x, y, left, top attributes to each node. This is the computed layout.
  flextreeLayout(laidOutTree)

  // The flextreeLayout does some crazy stuff to its input data, where as we only care about a couple fields.
  // Therefore just extract the fields.
  let calcedDimensionsByUuid = {}
  laidOutTree.each(dimensionNode => {
    calcedDimensionsByUuid[dimensionNode.data.uuid] = {
      x: dimensionNode.left,
      y: dimensionNode.top,
    }
  })
  return calcedDimensionsByUuid
}

function routeTo (vm, name, params) {
  let route = {
    name: name,
    query: {
      logDir: vm.$route.query.logDir,
      flameServer: vm.$route.query.flameServer,
    },
  }
  if (params) {
    route['params'] = params
  }
  return route
}

function getCenteringTransform (rectToCenter, container, scaleBounds, verticalPadding) {
  // TODO: padding as percentage of available area.
  let widthToCenter = rectToCenter.right - rectToCenter.left
  let heightToCenter = rectToCenter.bottom - rectToCenter.top + verticalPadding
  let xScale = container.width / widthToCenter
  let yScale = container.height / heightToCenter
  let scale = _.clamp(_.min([xScale, yScale]), scaleBounds.min, scaleBounds.max)

  let scaledWidthToCenter = widthToCenter * scale
  let xTranslate = rectToCenter.left * scale

  // Center based on (scaled) extra horizontal or vertical space.
  if (Math.round(container.width) > Math.round(scaledWidthToCenter)) {
    let remainingHorizontal = container.width - scaledWidthToCenter
    xTranslate = xTranslate - remainingHorizontal / 2
  }

  let scaledHeightToCenter = heightToCenter * scale
  let yTranslate = (rectToCenter.top - verticalPadding / 2) * scale
  if (Math.round(container.height) > Math.round(scaledHeightToCenter)) {
    let remainingVertical = container.height - scaledHeightToCenter
    yTranslate = yTranslate - remainingVertical / 2
  }
  return {x: -xTranslate, y: -yTranslate, scale: scale}
}

function socketRequestResponse (socket, requestEvent, successEvent, failedEvent, timeout) {
  let responseReceived = false
  socket.on(successEvent.name, (data) => {
    responseReceived = true
    successEvent.fn(data)
    socket.off(successEvent.name)
    if (!_.isNil(failedEvent)) {
      socket.off(failedEvent.name)
    }
  })
  if (!_.isNil(failedEvent)) {
    socket.on(failedEvent.name, (data) => {
      responseReceived = true
      failedEvent.fn(data)
      socket.off(successEvent.name)
      socket.off(failedEvent.name)
    })
  }

  if (!_.isNil(timeout)) {
    setTimeout(() => {
      if (!responseReceived) {
        timeout.fn()
      }
    }, timeout.waitTime)
  }
  if (requestEvent.data !== undefined) {
    socket.emit(requestEvent.name, requestEvent.data)
  } else {
    socket.emit(requestEvent.name)
  }
}

function isTaskStateIncomplete (state) {
  let incompleteStates = ['task-blocked', 'task-started', 'task-received', 'task-unblocked']
  return _.includes(incompleteStates, state)
}

function hasIncompleteTasks (nodesByUuid) {
  return _.some(nodesByUuid, n => isTaskStateIncomplete(n.state))
}

function getDescendantUuids (nodeUuid, nodesByUuid) {
  let resultUuids = []
  let uuidsToCheck = _.clone(nodesByUuid[nodeUuid]['children_uuids'])
  while (uuidsToCheck.length > 0) {
    let nodeUuid = uuidsToCheck.pop()
    if (!_.includes(resultUuids, nodeUuid)) {
      let childrenIds = nodesByUuid[nodeUuid]['children_uuids']
      uuidsToCheck = uuidsToCheck.concat(childrenIds)
      resultUuids.push(nodeUuid)
    }
  }
  return resultUuids
}

function durationString (duractionSecs) {
  if (!_.isNumber(duractionSecs)) {
    return ''
  }

  let hours = Math.floor(duractionSecs / (60 * 60))
  let hoursInSecs = hours * 60 * 60
  let mins = Math.floor((duractionSecs - hoursInSecs) / 60)
  let minsInSecs = mins * 60
  let secs = Math.floor(duractionSecs - hoursInSecs - minsInSecs)

  let result = 'time: '
  if (hours > 0) {
    result += hours + 'h '
  }
  if (mins > 0) {
    result += mins + 'm '
  }
  if (secs > 0) {
    result += secs + 's'
  }
  if (hours === 0 && mins === 0 && secs === 0) {
    result += '<1s'
  }
  return result
}

function orderByTaskNum (nodesByUuid) {
  return _.mapValues(_.groupBy(_.sortBy(nodesByUuid, 'task_num'), 'uuid'), _.head)
}
