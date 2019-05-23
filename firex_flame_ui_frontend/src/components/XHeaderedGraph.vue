<template>
  <!-- Need prevent to avoid default browser behaviour of ctrl-f.  -->
  <!-- 191 is the '/' keycode -->
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;"
       @keydown.ctrl.f.prevent="focusOnFind"
       @keyup.191.prevent="focusOnFind"
       @keydown.u.exact="$store.dispatch('graph/toggleLiveUpdate')"
       @keydown.d.exact="$store.dispatch('graph/toggleShowTaskDetails')"
       @keydown.r.exact="refreshGraph"
       tabindex="0">
    <x-header :title="headerParams.title"
              :links="headerParams.links"
              :legacyPath="headerParams.legacyPath"
              :enableSearch="true">
      <template v-slot:postsearch>
        <!-- TODO: consider externalizing collapse buttons to own component.-->
        <div style="border-left: 1px solid #000; padding: 0 8px;">

          <font-awesome-layers
            class="fa-fw collapse-button"
            @click="dispatchCollapseAction('graph/expandAll')"
            title="Expand All">
             <font-awesome-icon icon="expand-arrows-alt"/>
             <font-awesome-layers-text
               v-if="hasCollapsedNodes"
               class="fa-layers-counter collapsed-tasks-counter"
               :title="collasedNodeCount + ' Collapsed Tasks'"
               transform="up-1 right-20" :value="collasedNodeCount"/>
          </font-awesome-layers>

          <popper trigger="hover" :options="{ placement: 'bottom'}">
            <div class="popper collapse-menu">
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
    </x-header>
    <!-- TODO: not sure where the best level to gate on UID is, but need UID to key on
        localStorage within x-graph-->
    <x-graph v-if="runMetadata.uid"></x-graph>
  </div>
</template>

<script>
import _ from 'lodash';
import { mapGetters, mapState } from 'vuex';
import Popper from 'vue-popperjs';
import 'vue-popperjs/dist/vue-popper.css';

import XGraph from './XGraph.vue';
import XHeader from './XHeader.vue';
import {
  eventHub, routeTo2,
} from '../utils';

export default {
  name: 'XHeaderedGraph',
  components: { Popper, XGraph, XHeader },
  props: {
    // The root UUID to show, not necessarily the root UUID from the runMetadata.
    rootUuid: { default: null },
  },
  computed: {
    ...mapState({
      collapseConfig: state => state.graph.collapseConfig,
    }),
    ...mapGetters({
      isCollapsedByUuid: 'graph/isCollapsedByUuid',
      userDisplayConfigOperationsByUuid: 'graph/userDisplayConfigOperationsByUuid',
      flameDataDisplayOperationsByUuid: 'graph/flameDataDisplayOperationsByUuid',
      runStateByUuid: 'tasks/runStateByUuid',
    }),
    toggleStates() {
      return {
        liveUpdate: this.$store.state.graph.liveUpdate,
        showTaskDetails: this.$store.state.graph.showTaskDetails,
      };
    },
    runMetadata() {
      return this.$store.state.firexRunMetadata;
    },
    canRevoke() {
      return this.$store.getters['tasks/canRevoke'];
    },
    headerParams() {
      let links = [
        {
          name: 'liveUpdate',
          on: () => this.$store.dispatch('graph/toggleLiveUpdate'),
          toggleState: this.toggleStates.liveUpdate,
          icon: ['far', 'eye'],
        },
        {
          name: 'center',
          on: () => eventHub.$emit('center'),
          icon: 'bullseye',
          title: 'Center',
        },
        {
          name: 'showTaskDetails',
          on: () => this.$store.dispatch('graph/toggleShowTaskDetails'),
          toggleState: this.toggleStates.showTaskDetails,
          icon: 'plus-circle',
          title: 'Show Details',
        },
        {
          name: 'list',
          to: routeTo2(this.$route.query, 'XList'),
          icon: 'list-ul',
          title: 'List View',
        },
        {
          name: 'time-chart',
          to: routeTo2(this.$route.query, 'XTimeChart'),
          icon: 'clock',
          title: 'Time Chart',
        },
        {
          name: 'kill',
          on: () => eventHub.$emit('revoke-root'),
          _class: 'kill-button',
          icon: 'times',
          title: 'Kill',
        },
        {
          name: 'logs',
          // TODO replace with central server from metadata.
          href: this.$store.getters['firexRunMetadata/logsUrl'],
          text: 'View logs',
        },
        { name: 'help', to: routeTo2(this.$route.query, 'XHelp'), text: 'Help' },
      ];
      // Remove live update and kill options if the run isn't alive.
      if (!this.canRevoke) {
        links = _.filter(links, l => !_.includes(['liveUpdate', 'kill'], l.name));
      }

      return {
        title: this.runMetadata.uid,
        legacyPath: '/',
        links,
      };
    },
    collapsedNodeUuids() {
      return _.keys(_.pickBy(this.isCollapsedByUuid));
    },
    collasedNodeCount() {
      return this.collapsedNodeUuids.length;
    },
    hasCollapsedNodes() {
      return this.collapsedNodeUuids.length > 0;
    },
    canRestoreDefault() {
      const otherConfigApplied = this.collapseConfig.hideSuccessPaths
        || !_.isEmpty(this.collapseConfig.uiCollapseOperationsByUuid);
      const hasDefaultAffectingOps = _.size(this.userDisplayConfigOperationsByUuid) > 0
        || _.size(this.flameDataDisplayOperationsByUuid) > 0;
      return hasDefaultAffectingOps
        && (otherConfigApplied || !this.collapseConfig.applyDefaultCollapseOps);
    },
    canShowOnlyFailed() {
      const alreadyApplied = this.collapseConfig.hideSuccessPaths;
      const userTouched = !_.isEmpty(this.collapseConfig.uiCollapseOperationsByUuid);
      return this.hasFailures && (!alreadyApplied || userTouched);
    },
    hasFailures() {
      return _.some(_.values(this.runStateByUuid), { state: 'task-failed' });
    },
  },
  methods: {
    focusOnFind() {
      this.$store.commit('tasks/toggleSearchOpen');
    },
    refreshGraph() {
      eventHub.$emit('graph-refresh');
    },
    dispatchCollapseAction(action) {
      this.$store.dispatch(action);
      // TODO: should the event handler operate on nextTick?
      this.$nextTick(() => { eventHub.$emit('center'); });
    },
  },
  watch: {
    rootUuid: {
      handler() {
        // Center new graph when root node changes on next render
        // (after nodes for only the new root are shown).
        if (this.rootUuid !== null) {
          this.$nextTick(() => { eventHub.$emit('center'); });
        }
        this.$store.dispatch('tasks/selectRootUuid', this.rootUuid);
      },
      immediate: true,
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
    line-height: 40px;
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

  .collapsed-tasks-counter {
    color: black;
    font-size: 11px;
    overflow: visible;
    width: fit-content;
    background-color: deepskyblue;
  }

</style>
