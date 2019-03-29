<template>
  <!-- TODO: FIND A BETTER WAY! visiblity:collapse prevents table height from being calculated, so instead
    draw everything at z-index=-1000 and make sure the SVG & header cover these nodes.-->
  <!--
    Need inline-block display per node to get each node's intrinsic width (i.e. don't want it to force fill parent).
  -->
  <div style="display: inline-block; position: absolute; top: 0; z-index: -1000;">
    <x-node :allowCollapse="false" :showUuid="showUuid" :node="node"
            :allowClickToAttributes="false"
             v-on:node-dimensions="updateNodeDimensions($event)"></x-node>
  </div>
</template>

<script>
import XNode from './XNode'

export default {
  name: 'XSizeCapturingNode',
  components: {XNode},
  props: {
    node: {
      type: Object,
      required: true,
    },
    showUuid: {default: false},
  },
  data () {
    return {
      intrinsicDimensions: {width: null, height: null},
    }
  },
  mounted () {
    this.emit_dimensions()
  },
  updated () {
    this.emit_dimensions()
  },
  methods: {
    emit_dimensions () {
      this.$nextTick(function () {
        let r = this.$el.getBoundingClientRect()
        let renderedWidth = r.width // this.$el.clientWidth
        let renderedHeight = r.height // this.$el.clientHeight
        if (renderedWidth && renderedHeight) {
          let dimensionChanged = this.intrinsicDimensions.width !== renderedWidth || this.intrinsicDimensions.height !== renderedHeight
          if (dimensionChanged) {
            this.$emit('node-dimensions', {uuid: this.node.uuid, height: renderedHeight, width: renderedWidth})
            this.intrinsicDimensions = {width: renderedWidth, height: renderedHeight}
          }
        }
      })
    },
  },
}
</script>

<style scoped>

</style>
