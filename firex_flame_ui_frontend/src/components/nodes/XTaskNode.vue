<template>
  <div v-on:click.shift.prevent="nodeShiftClick" >
    <router-link v-if="allowClickToAttributes" :to="routeToAttributes()">
      <x-core-task-node
        :allowCollapse="allowCollapse"
        :taskUuid="taskUuid"
        :toCollapse="toCollapse"
        :isLeaf="isLeaf"
        class="node-container"
      ></x-core-task-node>
    </router-link>
    <x-core-task-node v-else
      :allowCollapse="allowCollapse"
      :taskUuid="taskUuid"
      :toCollapse="toCollapse"
      :isLeaf="isLeaf"
    ></x-core-task-node>
  </div>
</template>

<script>
import _ from 'lodash';

import { mapGetters } from 'vuex';
import XCoreTaskNode from './XCoreTaskNode.vue';


export default {
  name: 'XNode',
  components: { XCoreTaskNode },
  props: {
    allowCollapse: { default: true },
    taskUuid: { required: true },
    allowClickToAttributes: { default: true },
    toCollapse: { default: false },
    emitDimensions: { default: false, type: Boolean },
    isLeaf: { required: true, type: Boolean },
  },
  data() {
    return {
      latestEmittedDimensions: { width: -1, height: -1 },
    };
  },
  computed: {
    ...mapGetters({
      getTaskRoute: 'header/getTaskRoute',
      getCustomRootRoute: 'header/getCustomRootRoute',
    }),
    task() {
      return this.$store.state.tasks.allTasksByUuid[this.taskUuid];
    },
  },
  mounted() {
    this.emit_dimensions();
    // TODO: might still be worth considering in conjunction with other solutions.
    // Confirmed not affected by zoom, but still doesn't solve all flame-data node-sizing issues.
    // if (this.emitDimensions && typeof ResizeObserver !== 'undefined') {
    //   const resizeObserver = new ResizeObserver(() => {
    //     this.emit_dimensions();
    //   });
    //   resizeObserver.observe(this.$el);
    // }
  },
  updated() {
    this.emit_dimensions();
  },
  methods: {
    routeToAttributes() {
      return this.getTaskRoute(this.taskUuid);
    },
    nodeShiftClick() {
      if (this.allowClickToAttributes) {
        this.$router.push(this.getCustomRootRoute(this.taskUuid));
      }
    },
    // TODO: debounce?
    emit_dimensions() {
      if (this.emitDimensions) {
        this.$nextTick(() => {
          const r = this.$el.getBoundingClientRect();
          const renderedWidth = r.width; // this.$el.clientWidth
          const renderedHeight = r.height; // this.$el.clientHeight
          if (renderedWidth && renderedHeight) {
            const renderedDimensions = { width: renderedWidth, height: renderedHeight };
            if (!_.isEqual(this.latestEmittedDimensions, renderedDimensions)) {
              this.latestEmittedDimensions = renderedDimensions;
              this.$emit('task-node-size', { [[this.taskUuid]]: renderedDimensions });
            }
          }
        });
      }
    },
  },
};
</script>

<style scoped>

a {
  color: inherit;
  cursor: pointer;
  text-decoration: none;
}

.node-container:hover {
  background: #000 !important;
}

</style>
