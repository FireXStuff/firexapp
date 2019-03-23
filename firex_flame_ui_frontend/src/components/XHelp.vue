<template>
  <div class="help-content">
    <h1>Types of Tasks</h1>
    <div style="display: flex; text-align: center; margin-left: 100px">
      <div style="margin: 15px">
        <h2>Started</h2>
        <div style="display: inline-block">
          <x-node :node="startedNode" height="auto" width="200px" :allow-click-to-attributes="false"></x-node>
        </div>
      </div>
      <div style="margin: 15px">
        <h2>Succeeded</h2>
        <div style="display: inline-block">
          <x-node :node="succeededNode" height="auto" width="200px" :allow-click-to-attributes="false"></x-node>
        </div>
      </div>
      <div style="margin: 15px">
        <h2>Failed</h2>
        <div style="display: inline-block">
          <x-node :node="failedNode" height="auto" width="200px" :allow-click-to-attributes="false"></x-node>
        </div>
      </div>
      <div style="margin: 15px">
        <h2>Revoked</h2>
        <div style="display: inline-block">
          <x-node :node="revokedNode" height="auto" width="200px" :allow-click-to-attributes="false"></x-node>
        </div>
      </div>
      <div style="margin: 15px">
        <h2>Plugin with Success</h2>
        <div style="display: inline-block">
          <x-node :node="pluginSucceededNode" height="auto" width="200px" :allow-click-to-attributes="false"></x-node>
        </div>
      </div>
    </div>
    <h1>Difference between round and square corners</h1>
    <div class="orgchart">
      <ul>
        <li>
          <h2>Round cornered tasks are tasks which are spawned from within another task. The task which spawns them
            appears as their parent. These tasks can be the root of a larger chain.</h2>
        </li>
        <li>
          <h2>Square cornered tasks are tasks which are chained behind another task. The task which they are chained
            with (or precedes them) appears as their parent. The root of a chain will always have round corners.</h2>
        </li>
      </ul>
    </div>
  </div>
</template>

<script>

import XNode from './XNode'
import _ from 'lodash'

export default {
  name: 'XHelp',
  components: {XNode},
  data () {
    let baseNode = {
      name: 'noop',
      hostname: 'hostname',
      children_uuids: [],
      task_num: 1,
      actual_runtime: 0.5,
      uuid: '372bcc97-36a0-45cd-a322-3253155da856',
    }
    return {
      startedNode: _.merge({}, baseNode, {'state': 'task-started'}),
      succeededNode: _.merge({}, baseNode, {'state': 'task-succeeded'}),
      failedNode: _.merge({}, baseNode, {'state': 'task-failed'}),
      revokedNode: _.merge({}, baseNode, {'state': 'task-revoked'}),
      pluginSucceededNode: _.merge({}, baseNode, {'state': 'task-succeeded', 'from_plugin': true}),
    }
  },
}
</script>

<style scoped>
h1 {
    font-family: 'Source Sans Pro',sans-serif;
    font-size: 1.4em;
    margin-bottom: 0;
    font-weight: 700;
}

ul li {
    padding: 12px;
    max-width: 540px;
    text-align: center;
    display: inline-table;
    border-left: 1px solid #000;
}

ul li:first-child {
  border: 0;
}

</style>
