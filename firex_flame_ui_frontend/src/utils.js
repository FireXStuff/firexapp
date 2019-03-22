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
  nodesWithAncestorOrDescendantFailure,
  calculateNodesPositionByUuid,
  xNodeAttributeTo,
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
    if (line) {
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
    'from_plugin', 'chain_depth', 'firex_result']
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

function nodesWithAncestorOrDescendantFailure (nodesByUuid) {
  let failurePredicate = {'state': 'task-failed'}
  if (_.some(_.values(nodesByUuid), failurePredicate)) {
    let parentIds = _.map(_.values(nodesByUuid), 'parent_id')
    let leafNodes = _.filter(_.values(nodesByUuid), n => !_.includes(parentIds, n.uuid))
    let leafUuidPathsToRoot = _.map(leafNodes, l => getUuidsToRoot(l, nodesByUuid))
    let uuidPathsWithFailure = _.filter(leafUuidPathsToRoot,
      pathUuids => _.some(_.values(_.pick(nodesByUuid, pathUuids)), failurePredicate))
    let keepUuids = _.flatten(uuidPathsWithFailure)
    return _.difference(_.keys(nodesByUuid), keepUuids)
  }
  return []
}

function getUuidsToRoot (node, nodesByUuid) {
  let curNode = node
  let resultUuids = []
  while (true) {
    resultUuids.push(curNode.uuid)
    if (_.isNull(curNode.parent_id)) {
      break
    }
    curNode = nodesByUuid[curNode.parent_id]
  }
  return resultUuids
}

function calculateNodesPositionByUuid (nodesByUuid) {
  let newRootForLayout = flatGraphToTree(_.cloneDeep(nodesByUuid))
  // This calculates the layout (x, y per node) with dynamic node sizes.
  let flextreeLayout = flextree({
    spacing: 75,
    nodeSize: node => [node.data.width, node.data.height],
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
      y: dimensionNode.top + dimensionNode.depth * 50, // Separate each node by some fixed amount (e.g. 50).
    }
  })
  return calcedDimensionsByUuid
}

function xNodeAttributeTo (uuid, vm) {
  return {
    name: 'XNodeAttributes',
    params: {'uuid': uuid},
    query: {
      logDir: vm.$route.query.logDir,
      flameServer: vm.$route.query.flameServer,
    },
  }
}
