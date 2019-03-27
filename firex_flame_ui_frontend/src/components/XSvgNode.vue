<template>
  <g :transform="transform" :width="dimensions.width" :height="dimensions.height">
    <foreignObject :width="dimensions.width" :height="dimensions.height">
      <!--:style="'width: ' + dimensions.width + 'px; height: ' + dimensions.height + ';'"-->
        <x-node :node="node"
                :showUuid="showUuid"
                :liveUpdate="liveUpdate"
                :style="style"
                v-on:collapse-node="$emit('collapse-node')"></x-node>
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
        return _.difference(['x', 'y'], _.keys(value))
      },
    },
    showUuid: {},
    position: {required: true, type: Object},
    dimensions: {required: true, type: Object},
    opacity: {default: 1},
    liveUpdate: {required: true, type: Boolean},
  },
  computed: {
    transform () {
      return 'translate(' + this.position.x + ',' + this.position.y + ')'
    },
    style () {
      return {
        // Super gross, we want to size the node excluding its padding + border width, so we fudge 10 here.
        // This 10 will be taken up by padding + border width.
        // TODO: should actually emitt the inner and out box dimensions, and use both here to set outer (foreignObject)
        // and inner (node) rect sizes.
        'width': (this.dimensions.width - 10) + 'px',
        'height': (this.dimensions.height - 10) + 'px',
        'opacity': this.opacity,
      }
    },
  },
}
</script>

<style scoped>

</style>
