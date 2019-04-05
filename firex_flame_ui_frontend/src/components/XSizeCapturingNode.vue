<template>
  <!--
    Need inline-block display per node to get each node's intrinsic width (i.e. don't want it
    to force fill parent).
  -->
  <div style="display: inline-block; position: absolute; top: 0; z-index: -1000;">
    <x-node :allowCollapse="false" :showUuid="showUuid" :node="node"
            :allowClickToAttributes="false"
             v-on:node-dimensions="updateNodeDimensions($event)"></x-node>
  </div>
</template>

<script>
import _ from 'lodash';
import XNode from './XNode.vue';

export default {
  name: 'XSizeCapturingNode',
  components: { XNode },
  props: {
    node: {
      type: Object,
      required: true,
    },
    showUuid: { default: false },
  },
  data() {
    return {
      latestEmittedDimensions: { width: null, height: null },
    };
  },
  mounted() {
    this.emit_dimensions();
  },
  methods: {
    emit_dimensions() {
      this.$nextTick(() => {
        const r = this.$el.getBoundingClientRect();
        const renderedWidth = r.width; // this.$el.clientWidth
        const renderedHeight = r.height; // this.$el.clientHeight
        if (renderedWidth && renderedHeight) {
          const renderedDimensions = { width: r.width, height: r.height };
          if (!_.isEqual(this.latestEmittedDimensions, renderedDimensions)) {
            this.$emit('node-dimensions', _.merge({ uuid: this.node.uuid }, renderedDimensions));
            this.latestEmittedDimensions = renderedDimensions;
          }
        }
      });
    },
  },
  watch: {
    // TODO: Gross that this comp needs to know where x-node's size variability comes from.
    //    This is likely an argument
    //  for putting the size capturing back in x-node and using Vue's 'updated' lifecycle hook.
    'node.flame_additional_data': () => {
      this.emit_dimensions();
    },
  },
};
</script>

<style scoped>

</style>
