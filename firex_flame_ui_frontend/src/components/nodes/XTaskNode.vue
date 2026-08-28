<template>
  <div v-on:click.shift.prevent="nodeShiftClick" >
    <router-link v-if="allowClickToAttributes" :to="routeToAttributes()">
      <x-core-task-node
        :allowCollapse="allowCollapse"
        :taskUuid="taskUuid"
        :toCollapse="toCollapse"
        :isLeaf="isLeaf"
        :emitDimensions="emitDimensions"
        :showFlameData="showFlameData"
        class="node-container"
      />
    </router-link>
    <x-core-task-node v-else
      :allowCollapse="allowCollapse"
      :taskUuid="taskUuid"
      :toCollapse="toCollapse"
      :isLeaf="isLeaf"
      :emitDimensions="emitDimensions"
    />
  </div>
</template>

<script>
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
    showFlameData: { default: true, type: Boolean },
  },
  computed: {
    ...mapGetters({
      getTaskRoute: 'header/getTaskRoute',
      getCustomRootRoute: 'header/getCustomRootRoute',
    }),
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
