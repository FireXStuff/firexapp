<template>
  <transition-group :name="fadeTransitionName" tag="g">
    <!-- uuid+'' hack is to ignore vue compiler warning. Map keys are safe, array indexes aren't.-->
    <g v-for="(nodeLayout, uuid) in nodeLayoutsByUuid"
       :key="uuid + ''"
       :transform="'translate(' + nodeLayout.x + ',' + nodeLayout.y + ')'"
       :width="nodeLayout.width + 10"
       :height="nodeLayout.height + 10"
       :class="{ faded: hasFocusedTaskUuid && focusedTaskUuid !== uuid }">

      <foreignObject :width="nodeLayout.width + 10" :height="nodeLayout.height + 10">
        <x-collapsable-task-node
          :taskUuid="uuid"
          :width="nodeLayout.width"
          :height="nodeLayout.height"></x-collapsable-task-node>
      </foreignObject>
    </g>
  </transition-group>
</template>

<script>
import _ from 'lodash';
import XCollapsableTaskNode from './XCollapsableTaskNode.vue';

export default {
  name: 'XTaskSvgNodes',
  components: { XCollapsableTaskNode },
  props: {
    nodeLayoutsByUuid: { required: true, type: Object },
  },
  computed: {
    shouldFadeInNewNodes() {
      // TODO: transition-group fade in causes errant tasks during fast live-update changes.
      // A better fix is likely possible, but for now disable fade-in on incomplete tasks.
      return !this.$store.getters['tasks/hasIncompleteTasks']
        && !this.$store.state.graph.isFirstLayout;
    },
    focusedTaskUuid() {
      return this.$store.state.tasks.focusedTaskUuid;
    },
    hasFocusedTaskUuid() {
      return !_.isNull(this.focusedTaskUuid);
    },
    fadeTransitionName() {
      return this.shouldFadeInNewNodes ? 'fade' : null;
    },
  },
};
</script>

<style scoped>

  .faded {
    opacity: 0.3;
  }

  .fade-enter-active {
    transition: opacity 2s;
  }

  .fade-enter {
    opacity: 0.1;
  }

</style>
