import _ from 'lodash'

export default {
  invokePerNode: function (root, fn) {
    let doneUuids = []
    let nodesToCheck = [root]
    while (nodesToCheck.length > 0) {
      let node = nodesToCheck.pop()
      // Avoid loops in graph.
      if (!_.includes(doneUuids, node.uuid)) {
        doneUuids.push(node.uuid)
        fn(node)
        nodesToCheck = nodesToCheck.concat(node.children)
      }
    }
  },
}
