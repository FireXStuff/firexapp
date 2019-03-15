import _ from 'lodash'

export {parseRecFileContentsToNodesByUuid, flatGraphToTree}

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
        'task-failed', 'task-revoked', 'task-incomplete', 'from_plugin']
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
    return _.isNull(task.parent_id)
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
