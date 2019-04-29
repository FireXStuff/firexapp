<template>
  <g :transform="transform" :width="dimensions.width + 10" :height="dimensions.height + 10"
      :style="groupStyle">
    <defs v-if="isInProgress">
      <filter id="shadow" x="-40%" y="-40%" height="200%" width="200%">
        <feOffset result="offOut" in="SourceAlpha" dx="0" dy="0"/>
        <feGaussianBlur id="blur" result="blurOut" in="offOut" stdDeviation="10"/>
        <feBlend in="SourceGraphic" in2="blurOut" mode="normal"/>
      </filter>
      <animate xlink:href="#blur" attributeName="stdDeviation"
             values="2;12;2" dur="3s" begin="0s" repeatCount="indefinite"/>
    </defs>

    <foreignObject :width="dimensions.width + 10" :height="dimensions.height + 10">
      <x-collapsable-task-node
        :node="node"
        :showUuid="showUuid"
        :liveUpdate="liveUpdate"
        :dimensions="dimensions"
        :collapseDetails="collapseDetails"
        :displayDetails="displayDetails"></x-collapsable-task-node>
    </foreignObject>

  </g>
</template>

<script>
import _ from 'lodash';
import XCollapsableTaskNode from './XCollapsableTaskNode.vue';

/**
 * TODO: it might not be worth having this as a component. Maybe split XGraph, e.g.
 *  XCollapsableGraph vs XSvgGraph, and put this component's content in XSvgGraph.
 *  The outer graph would guard against rendering
 *  the SVG graph pre-layout calculation.
 */
export default {
  name: 'XTaskSvgNode',
  components: { XCollapsableTaskNode },
  props: {
    node: {
      type: Object,
      required: true,
      validator(value) {
        return _.difference(['x', 'y'], _.keys(value));
      },
    },
    showUuid: {},
    position: { required: true, type: Object },
    dimensions: { required: true, type: Object },
    opacity: { default: 1 },
    liveUpdate: { required: true, type: Boolean },
    collapseDetails: { required: true },
    displayDetails: { required: true },
  },
  computed: {
    transform() {
      return `translate(${this.position.x},${this.position.y})`;
    },
    isInProgress() {
      return this.node.state === 'task-started';
    },
    groupStyle() {
      const style = { opacity: this.opacity };
      if (this.isInProgress) {
        _.merge(style,
          {
            filter: 'url(#shadow)',
            transform: 'translate3d(0, 0, 1)',
            'backface-visibility': 'hidden',
            perspective: 1000,
          });
      }
      return style;
    },
  },
};
</script>

<style scoped>

</style>
