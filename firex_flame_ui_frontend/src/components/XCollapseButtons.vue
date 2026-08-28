<template>
  <div style="padding-right: 8px; display: flex; height: 100%; align-items: center;">
    <x-header-button :link="expandAllLink"></x-header-button>

    <popper v-if="anyCollapseOptionsAvailable"
      trigger="hover" :options="{ placement: 'bottom'}"
      :disabled="dropDownDisabled">
      <div class="popper collapse-menu" @click="toggleDropdownDisabled">
        <div v-if="hasCollapsedNodes"
             class="collapse-menu-item"
             @click="dispatchCollapseAction('graph/expandAll')">
          <font-awesome-icon icon="expand-arrows-alt"></font-awesome-icon>
          Expand All
        </div>
        <div v-if="canRestoreDefault"
             class="collapse-menu-item"
             @click="dispatchCollapseAction('graph/restoreCollapseDefault')">
          <font-awesome-icon icon="undo"></font-awesome-icon>
          Restore Default
        </div>
        <div v-if="canShowOnlyFailed"
             class="collapse-menu-item"
             @click="dispatchCollapseAction('graph/collapseSuccessPaths')">
          <font-awesome-icon icon="exclamation-circle" style="color: #900">
          </font-awesome-icon>
          Show only failed
        </div>
      </div>
      <span slot="reference" class="collapse-button">
        <font-awesome-icon icon="caret-down"></font-awesome-icon>
      </span>
    </popper>
  </div>
</template>

<script>
import _ from 'lodash';
import { mapGetters, mapState } from 'vuex';
import Popper from 'vue-popperjs';
import 'vue-popperjs/dist/vue-popper.css';

import { eventHub } from '../utils';
import XHeaderButton from './XHeaderButton.vue';

export default {
  name: 'XCollapseButtons',
  components: { XHeaderButton, Popper },
  data() {
    return {
      dropDownDisabled: false,
    };
  },
  computed: {
    ...mapState({
      collapseConfig: state => state.graph.collapseConfig,
    }),
    ...mapGetters({
      collapsedNodeUuids: 'graph/collapsedNodeUuids',
      userDisplayConfigOperationsByUuid: 'graph/userDisplayConfigOperationsByUuid',
      flameDataDisplayOperationsByUuid: 'graph/flameDataDisplayOperationsByUuid',
      runStateByUuid: 'tasks/runStateByUuid',
    }),
    hasCollapsedNodes() {
      return this.collapsedNodeUuids.length > 0;
    },
    canRestoreDefault() {
      const otherConfigApplied = this.collapseConfig.hideSuccessPaths
        || !_.isEmpty(this.collapseConfig.uiCollapseStateByUuid);
      const hasDefaultAffectingOps = _.size(this.userDisplayConfigOperationsByUuid) > 0
        || _.size(this.flameDataDisplayOperationsByUuid) > 0;
      return hasDefaultAffectingOps
        && (otherConfigApplied || !this.collapseConfig.applyDefaultCollapseOps);
    },
    canShowOnlyFailed() {
      const alreadyApplied = this.collapseConfig.hideSuccessPaths;
      const userTouched = !_.isEmpty(this.collapseConfig.uiCollapseStateByUuid);
      return this.hasFailures && (!alreadyApplied || userTouched);
    },
    hasFailures() {
      return _.some(_.values(this.runStateByUuid), { state: 'task-failed' });
    },
    anyCollapseOptionsAvailable() {
      return this.canRestoreDefault || this.canShowOnlyFailed;
    },
    expandAllLink() {
      return {
        on: () => { this.dispatchCollapseAction('graph/expandAll'); },
        title: 'Expand All',
        icon: 'expand-arrows-alt',
      };
    },
  },
  methods: {
    dispatchCollapseAction(action) {
      this.$store.dispatch(action);
      // TODO: should the event handler operate on nextTick?
      this.$nextTick(() => { eventHub.$emit('center'); });
    },
    toggleDropdownDisabled() {
      this.dropDownDisabled = true;
      // Disable just to dismiss popover -- re-enable on next tick.
      this.$nextTick(() => { this.dropDownDisabled = false; });
    },
  },
};
</script>

<style scoped>

  .collapse-menu {
    text-align: left;
    font-family: 'Source Sans Pro',sans-serif;
    cursor: auto;
    font-size: 20px;
  }

  .collapse-button {
    font-size: 20px;
    cursor: pointer;
    color: #000;
  }

  .collapse-button:hover {
    color: #2980ff;
    cursor: pointer;
  }

  .collapse-menu-item:hover {
    color: #2980ff;
    cursor: pointer;
  }

  /deep/ .flame-link {
    padding-right: 0;
  }

</style>
