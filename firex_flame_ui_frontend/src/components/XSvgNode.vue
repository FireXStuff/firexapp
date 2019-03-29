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
         <x-node :node="node"
                 :showUuid="showUuid"
                 :liveUpdate="liveUpdate"
                 :style="style"
                 :isAnyChildCollapsed="isAnyChildCollapsed"
                 v-on:collapse-node="$emit('collapse-node')"></x-node>
    </foreignObject>

  </g>
</template>

<script>
import _ from 'lodash'
import XNode from './XNode'

export default {
  name: 'XSvgNode',
  components: {XNode},
  props: {
    node: {
      type: Object,
      required: true,
      validator: function (value) {
        return _.difference(['x', 'y'], _.keys(value))
      },
    },
    showUuid: {},
    position: {required: true, type: Object},
    dimensions: {required: true, type: Object},
    opacity: {default: 1},
    liveUpdate: {required: true, type: Boolean},
    isAnyChildCollapsed: {required: true, type: Boolean},
  },
  computed: {
    transform () {
      return 'translate(' + this.position.x + ',' + this.position.y + ')'
    },
    isInProgress () {
      return this.node.state === 'task-started'
    },
    style () {
      return {
        // Super gross, we want to size the node excluding its padding + border width, so we fudge 10 here.
        // This 10 will be taken up by padding + border width.
        // TODO: should actually emitt the inner and out box dimensions, and use both here to set outer (foreignObject)
        // and inner (node) rect sizes.
        'width': (this.dimensions.width) + 'px',
        'height': (this.dimensions.height) + 'px',
        'opacity': this.opacity,
      }
    },
    groupStyle () {
      if (this.isInProgress) {
        return {
          filter: 'url(#shadow)',
          transform: 'translate3d(0, 0, 1)',
          'backface-visibility': 'hidden',
          'perspective': 1000,
        }
      }
      return {}
    },
  },
}
</script>

<style scoped>

</style>
