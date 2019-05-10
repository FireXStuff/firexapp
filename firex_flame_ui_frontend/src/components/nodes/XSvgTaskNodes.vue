<template>
  <g>
    <g v-for="(nodeLayout, uuid) in nodeLayoutsByUuid"
       :key="uuid"
       :transform="'translate(' + nodeLayout.x + ',' + nodeLayout.y + ')'"
       :width="nodeLayout.width + 10" :height="nodeLayout.height + 10"
       :class="{ inprogress: isInProgressByUuid[uuid] }">

      <defs v-if="isInProgressByUuid[uuid]">
        <filter id="shadow" x="-40%" y="-40%" height="200%" width="200%">
          <feOffset result="offOut" in="SourceAlpha" dx="0" dy="0"/>
          <feGaussianBlur id="blur" result="blurOut" in="offOut" stdDeviation="10"/>
          <feBlend in="SourceGraphic" in2="blurOut" mode="normal"/>
        </filter>
        <animate xlink:href="#blur" attributeName="stdDeviation"
               values="2;12;2" dur="3s" begin="0s" repeatCount="indefinite"/>
      </defs>

      <foreignObject :width="nodeLayout.width + 10" :height="nodeLayout.height + 10">
        <x-collapsable-task-node
          :taskUuid="uuid"
          :width="nodeLayout.width"
          :height="nodeLayout.height"></x-collapsable-task-node>
      </foreignObject>
    </g>
  </g>

</template>

<script>
import _ from 'lodash';
import XCollapsableTaskNode from './XCollapsableTaskNode.vue';

export default {
  name: 'XTaskSvgNodes',
  components: { XCollapsableTaskNode },
  props: {
    nodeLayoutsByUuid: { required: true, type: Object },
    // TODO: fix by supplying focused node and calculating
    //  opacity in here, or reading from global state.
    opacity: { default: 1 },
  },
  computed: {
    taskRunStateByUuid() {
      return this.$store.getters['tasks/runStateByUuid'];
    },
    isInProgressByUuid() {
      return _.map(this.taskRunStateByUuid, r => r.state === 'task-started');
    },
    // groupStyle() {
    //   const style = { opacity: this.opacity };
    //   if (this.isInProgress) {
    //     _.merge(style,
    //       {
    //         filter: 'url(#shadow)',
    //         transform: 'translate3d(0, 0, 1)',
    //         'backface-visibility': 'hidden',
    //         perspective: 1000,
    //       });
    //   }
    //   return style;
    // },
  },
};
</script>

<style scoped>

  .inprogress {
    filter: url(#shadow);
    transform: translate3d(0, 0, 1);
    backface-visibility: hidden;
    perspective: 1000;
  }

  .faded {
    opacity: 0.3;
  }

</style>
