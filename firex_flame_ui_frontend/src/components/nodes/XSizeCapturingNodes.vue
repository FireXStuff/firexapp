<template>
  <div style="overflow: hidden; width: 100%; height: 100%; position: absolute; top: 0;
    z-index: -10">
    <!-- This is very gross, but the nodes that will be put on the graph are rendered
    invisibly in order for the browser to calculate their intrinsic size. Each node's size is
    then passed to the graph layout algorithm before the actual SVG graph is rendered.-->
    <!--
      Need inline-block display per node to get each node's intrinsic width (i.e. don't want it
      to force fill parent).
    -->
    <div v-for="uuid in uncollapsedNodeUuids" :key="uuid"
         style="display: inline-block; position: absolute; top: 0; z-index: -1000;">
      <!-- TODO: profile event performance with thousands of nodes. Might be worthwhile
          to catch all and send in one big event. -->
      <x-core-task-node
        :taskUuid="uuid"
        :emitDimensions="true"
        :allowCollapse="false"/>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';

import { containsAll, eventHub } from '../../utils';
import { useTasksStore } from '../../store/modules/tasks';
import { useGraphStore } from '../../store/modules/graph';
import XCoreTaskNode from './XCoreTaskNode.vue';

export default {
  name: 'XSizeCapturingNodes',
  components: { XCoreTaskNode },
  data() {
    return {
      sizesToCommit: {},
    };
  },
  computed: {
    dimensionsByUuid() {
      return useTasksStore().taskNodeSizeByUuid;
    },
    uncollapsedNodeUuids() {
      return useGraphStore().uncollapsedNodeUuids;
    },
  },
  created() {
    eventHub.$on('task-node-size', this.receiveSize);
  },
  methods: {
    receiveSize(taskSize) {
      if (_.isEmpty(this.dimensionsByUuid)) {
        // Doing initial sizing collect -- save size & send once all retrieved.
        Object.assign(this.sizesToCommit, taskSize);
        // Check if this latest size completed the dimensions & should therefore cause commit.
        if (containsAll(_.keys(this.sizesToCommit), this.uncollapsedNodeUuids)) {
          useTasksStore().addTaskNodeSize(this.sizesToCommit);
          this.sizesToCommit = {};
        }
      } else {
        // Just send the latest event immediately (live-update).
        useTasksStore().addTaskNodeSize(taskSize);
      }
    },
  },
};
</script>

<style scoped>

</style>
