<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;">
    <x-header :title="headerParams.title"
              :links="headerParams.links"></x-header>

    <div class="help-content">
      <h1>Types of Tasks</h1>
      <div style="display: flex; margin-left: 100px">
        <div style="margin: 15px">
          <h2 class="node-type">Running</h2>
          <div style="display: inline-block; width: 200px;">
            <x-node taskUuid="startedNode" :allow-click-to-attributes="false" :isLeaf="true">
            </x-node>
          </div>
        </div>
        <div style="margin: 15px">
          <h2 class="node-type">Succeeded</h2>
          <div style="display: inline-block; width: 200px;">
            <x-node taskUuid="succeededNode" :allow-click-to-attributes="false" :isLeaf="true">
            </x-node>
          </div>
        </div>
        <div style="margin: 15px">
          <h2 class="node-type">Failed</h2>
          <div style="display: inline-block; width: 200px;">
            <x-node taskUuid="failedNode" :allow-click-to-attributes="false" :isLeaf="true">
            </x-node>
          </div>
        </div>
        <div style="margin: 15px">
          <h2 class="node-type">Revoked</h2>
          <div style="display: inline-block; width: 200px;">
            <x-node taskUuid="revokedNode" :allow-click-to-attributes="false" :isLeaf="true">
            </x-node>
          </div>
        </div>
        <div style="margin: 15px">
          <h2 class="node-type">Blocked</h2>
          <div style="display: inline-block; width: 200px;">
            <x-node taskUuid="blockedNode" :allow-click-to-attributes="false" :isLeaf="true">
            </x-node>
          </div>
        </div>
      </div>
      <div style="display: flex; margin-left: 100px">
        <div style="margin: 15px">
          <h2 class="node-type">Plugin with Success</h2>
          <div style="display: inline-block; width: 200px;">
            <x-node taskUuid="pluginSucceededNode" :allow-click-to-attributes="false"
                    :isLeaf="true"></x-node>
          </div>
        </div>
        <div style="margin: 15px">
          <h2 class="node-type">Failed by Descendant</h2>
          <div style="display: inline-block; width: 200px;">
            <x-node taskUuid="descendantFailedNode" :allow-click-to-attributes="false"
                    :isLeaf="true"></x-node>
          </div>
        </div>
      </div>
      <h1>Difference between round and square corners</h1>
      <div>
        <ul>
          <li>
            <h2 style="font-weight: normal;">Round cornered tasks are tasks which are spawned
              from within another task. The task which spawns them
              appears as their parent. These tasks can be the root of a larger chain.</h2>
          </li>
          <li>
            <h2 style="font-weight: normal;">Square cornered tasks are tasks which are chained
              behind another task. The task which they are chained
              with (or precedes them) appears as their parent. The root of a chain
              will always have round corners.</h2>
          </li>
        </ul>
      </div>
    </div>
  </div>
</template>

<script>

import _ from 'lodash';
import { mapGetters } from 'vuex';

import XNode from './nodes/XTaskNode.vue';
import XHeader from './XHeader.vue';

export default {
  name: 'XHelp',
  components: { XHeader, XNode },
  data() {
    return {
      baseNode: {
        name: 'noop',
        hostname: 'hostname',
        task_num: 1,
        actual_runtime: 0.5,
        uuid: '372bcc97-36a0-45cd-a322-3253155da856',
      },
    };
  },
  created() {
    const tasksByUuid = {
      startedNode: _.merge({}, this.baseNode, { state: 'task-started' }),
      succeededNode: _.merge({}, this.baseNode, { state: 'task-succeeded' }),
      failedNode: _.merge({}, this.baseNode, { state: 'task-failed' }),
      revokedNode: _.merge({}, this.baseNode, { state: 'task-revoked' }),
      blockedNode: _.merge({}, this.baseNode, { state: 'task-blocked' }),
      pluginSucceededNode: _.merge({}, this.baseNode, { state: 'task-succeeded', from_plugin: true }),
      descendantFailedNode: _.merge({}, this.baseNode,
        { state: 'task-failed', exception: 'ChainInterruptedException' }),
    };
    this.$store.dispatch('tasks/setTasks', tasksByUuid);
  },
  computed: {
    ...mapGetters({
      documentationHeaderEntry: 'header/documentationHeaderEntry',
    }),
    tasksByUuid() {
      return {
        startedNode: _.merge({}, this.baseNode, { state: 'task-started' }),
        succeededNode: _.merge({}, this.baseNode, { state: 'task-succeeded' }),
        failedNode: _.merge({}, this.baseNode, { state: 'task-failed' }),
        revokedNode: _.merge({}, this.baseNode, { state: 'task-revoked' }),
        blockedNode: _.merge({}, this.baseNode, { state: 'task-blocked' }),
        pluginSucceededNode: _.merge({}, this.baseNode,
          { state: 'task-succeeded', from_plugin: true }),
      };
    },
    headerParams() {
      return {
        title: 'Help',
        links: [
          {
            name: 'shortcuts',
            to: { name: 'XShortcuts' },
            text: 'Shortcuts',
            icon: 'keyboard',
          },
          this.documentationHeaderEntry,
        ],
      };
    },
  },
  watch: {
    $route: {
      immediate: true,
      handler() {
        this.$store.dispatch('tasks/setTasks', this.tasksByUuid);
      },
    },
  },
};
</script>

<style scoped>
h1 {
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

.help-content {
  font-family: 'Source Sans Pro',sans-serif;
  margin: 10px;
}

.node-type {
  text-align: center;
  font-weight: normal;
}

</style>
