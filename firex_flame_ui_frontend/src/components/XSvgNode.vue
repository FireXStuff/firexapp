<template>
  <!--:class="[{ progress: node.state === 'task-started' }]"-->
  <g class="node" :transform="transform"
     :width="computedDimensions.width" :height="computedDimensions.height" >
    <foreignObject :width="computedDimensions.width" :height="computedDimensions.height">
      <div style="display: inline-block">
      <x-node :node="node"
              v-on:collapse-node="$emit('collapse-node')"
              v-on:node-dimensions="$emit('node-dimensions', $event)"></x-node>
        </div>
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
        let missing = _.difference(['x', 'y', 'width', 'height'], _.keys(value))
        let hasRequired = missing.length === 0

        if (!hasRequired) {
          console.log('Missing: ' + missing)
          console.log(value)
        }

        // TODO: always use size or always use width/height
        // let hasWidth = _.has(value, 'width') || _.has(value, 'size')
        // let hasHeight = _.has(value, 'height') || _.has(value, 'size')

        return hasRequired // && hasWidth && hasHeight
      },
    },
  },
  computed: {
    // TODO: verify internal padding is necessary.
    // computedHeight () {
    //   return _.isNumber(this.node.height) ? this.node.height + 10 : 10000
    // },
    // computedWidth () {
    //   return _.isNumber(this.node.width) ? this.node.width + 10 : 10000
    // },
    computedDimensions () {
      // TODO: is node.width always defaultWidth?
      return {
        width: _.isNumber(this.node.width) ? this.node.width + 10 : 10000,
        height: _.isNumber(this.node.height) ? this.node.height + 10 : 10000,
      }
    },
    transform () {
      return 'translate(' + this.node.x + ',' + this.node.y + ')'
    },
  },
}
</script>

<style scoped>

  /* set foreignObject dimensions small, then rely on overflow to show */
  /* This trick causes crazy rendering -- no good. -->
/*foreignObject {*/
  /*overflow: visible;*/
/*}*/
</style>
