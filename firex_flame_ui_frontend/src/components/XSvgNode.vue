<template>
  <!--:class="[{ progress: node.state === 'task-started' }]"-->
  <g class="node" :transform="t(node.x, node.y)"
     :width="node.width" :height="node.height" >
    <foreignObject :width="computedWidth" :height="computedHeight">
      <x-node :node="node" :key="node.uuid"
              v-on:collapse-node="$emit('collapse-node')"
              v-on:node-dimensions="$emit('node-dimensions', $event)"></x-node>
    </foreignObject>

    <!-- could use this shadow for animation of in-progress nodes -->
     <!--style="filter:url(#dropshadow)"-->
    <!--<filter id="dropshadow" height="130%">-->
      <!--<feGaussianBlur in="SourceAlpha" stdDeviation="10"/> &lt;!&ndash; stdDeviation is how much to blur &ndash;&gt;-->
      <!--<feOffset dx="5" dy="5" result="offsetblur"/> &lt;!&ndash; how much to offset &ndash;&gt;-->
      <!--<feComponentTransfer>-->
        <!--<feFuncA type="linear" slope="0.5"/> &lt;!&ndash; slope is the opacity of the shadow &ndash;&gt;-->
      <!--</feComponentTransfer>-->
      <!--<feMerge>-->
        <!--<feMergeNode/> &lt;!&ndash; this contains the offset blurred image &ndash;&gt;-->
        <!--<feMergeNode in="SourceGraphic"/> &lt;!&ndash; this contains the element that the filter is applied to &ndash;&gt;-->
      <!--</feMerge>-->
    <!--</filter>-->

  </g>
</template>

<script>
import _ from 'lodash'
import XNode from './XNode'

// "duration": '17m 15s',

// let inProgressAnimationStyle = {
//   'animation-name': 'taskRunning',
//   'animation-duration': '1.5s',
//   'animation-timing-function': 'ease',
//   'animation-delay': '0s',
//   'animation-iteration-count': 'infinite',
//   'animation-direction': 'normal',
//   'animation-fill-mode': 'none',
//   'animation-play-state': 'running'
// }

export default {
  name: 'XSvgNode',
  components: {XNode},
  props: {
    node: {
      type: Object,
      required: true,
      validator: function (value) {
        return _.difference(['x', 'y', 'width', 'height'], _.keys(value)).length === 0
      }
    }
  },
  computed: {
    computedHeight () {
      return _.isNumber(this.node.height) ? this.node.height : 0
    },
    computedWidth () {
      return _.isNumber(this.node.width) ? this.node.width : 0
    }
  },
  methods: {
    t (x, y) {
      return 'translate(' + x + ',' + y + ')'
    }
  }
}
</script>

<style scoped>
</style>
