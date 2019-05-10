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
      <!--   :displayDetails="getDisplayDetails(resolvedCollapseStateByUuid, n.uuid)"
-->
      <x-task-node
        :taskUuid="uuid"
        :emitDimensions="true"
        :isLeaf="true"></x-task-node>
    </div>
  </div>
</template>

<script>
import XTaskNode from './XTaskNode.vue';

export default {
  name: 'XSizeCapturingNodes',
  components: { XTaskNode },
  computed: {
    allUuids() {
      return this.$store.getters['tasks/allTaskUuids'];
    },
  },
};
</script>

<style scoped>

</style>
