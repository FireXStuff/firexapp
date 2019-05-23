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
    <div v-for="uuid in allUuids" :key="uuid"
         style="display: inline-block; position: absolute; top: 0; z-index: -1000;">
      <!-- TODO: profile event performance with thousands of nodes. Might be worthwhile
          to catch all and send in one big event. -->
      <x-task-node
        :taskUuid="uuid"
        :emitDimensions="true"
        v-on:task-node-size="receiveSize"
        :isLeaf="true"></x-task-node>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';

import { containsAll } from '../../utils';
import XTaskNode from './XTaskNode.vue';

export default {
  name: 'XSizeCapturingNodes',
  components: { XTaskNode },
  data() {
    return {
      sizesToCommit: {},
    };
  },
  computed: {
    dimensionsByUuid() {
      return this.$store.state.tasks.taskNodeSizeByUuid;
    },
    allUuids() {
      return this.$store.getters['tasks/allTaskUuids'];
    },
  },
  methods: {
    receiveSize(taskSize) {
      if (_.isEmpty(this.dimensionsByUuid)) {
        // Doing initial sizing collect -- save size & send once all retreived.
        Object.assign(this.sizesToCommit, taskSize);
        // Check if this latest size completed the dimensions & should therefore cause commit.
        if (containsAll(_.keys(this.sizesToCommit), this.allUuids)) {
          this.$store.dispatch('tasks/addTaskNodeSize', this.sizesToCommit);
          this.sizesToCommit = {};
        }
      } else {
        // Just send the latest event immediately (live-update).
        this.$store.dispatch('tasks/addTaskNodeSize', taskSize);
      }
    },
  },
};
</script>

<style scoped>

</style>
