import _ from 'lodash'

export {parseRecFileContentsToTree}

function parseRecFileContentsToTree (recFileContents) {
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
  return flatGraphToTree(tasksByUuid)
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
    'local_received']
  copyFields.forEach(function (field) {
    if (_.has(event, field)) {
      task[field] = event[field]
    }
  })

  let fieldsToTransforms = {
    'name': function (event) {
      return {'name': _.last(event.name.split('.'))}
    },
    'type': function (event) {
      let stateTypes = ['task-received', 'task-blocked', 'task-started', 'task-succeeded', 'task-shutdown',
        'task-failed', 'task-revoked', 'task-incomplete']
      if (_.includes(stateTypes, event.type)) {
        return {'state': event.type}
      }
      return {}
    }
  }

  _.keys(fieldsToTransforms).forEach(function (field) {
    if (_.has(event, field)) {
      tasksByUuid[taskUuid] = _.merge(tasksByUuid[taskUuid], fieldsToTransforms[field](event))
    }
  })

  // if (_.has(event, 'flame_additional_data')) {
  //   if (!_.has(task, 'flame_additional_data')) {
  //     task['flame_additional_data'] = []
  //   }
  //   task['flame_additional_data'].push(event['flame_additional_data'])
  // }
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
//
// fetch('/auto/firex-logs-sjc/djungic/FireX-djungic-190311-152310-63727/flame.rec')
//   .then(function (r) {
//     return r.text()
//   })
//   .then(function (body) {
//     body.split(/\r?\n/).forEach(function (line) {
//       if (line) {
//         captureEventState(JSON.parse(line))
//       }
//     })
//     document.getElementById('content').innerText = JSON.stringify(flatGraphToTree(tasksByUuid),
//       null, 2)
//   })
