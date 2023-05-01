<template>
  <!-- Need prevent to avoid default browser behaviour of ctrl-f.  -->
  <!-- 191 is the '/' keycode -->
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;"
       @keydown.ctrl.f.prevent="focusOnFind"
       @keyup.191.prevent="focusOnFind"
       @keyup.u.exact="$store.dispatch('graph/toggleLiveUpdate')"
       @keyup.d.exact="$store.dispatch('graph/toggleShowTaskDetails')"
       @keyup.r.exact="refreshGraph"
       tabindex="0">
    <x-header :title="headerParams.title"
              :showCompletionReportLink="true"
              :links="headerParams.links">
      <template v-slot:prebuttons>
        <x-task-node-search :findUncollapsedAncestor="true">
        </x-task-node-search>
        <x-collapse-buttons></x-collapse-buttons>
      </template>
    </x-header>
    <!-- TODO: not sure where the best level to gate on UID is, but need UID to key on
        localStorage within x-graph-->
    <x-graph v-if="runMetadata.uid"></x-graph>
  </div>
</template>

<script>
import _ from 'lodash';
import { mapGetters } from 'vuex';

import XGraph from './XGraph.vue';
import XHeader from './XHeader.vue';
import XCollapseButtons from './XCollapseButtons.vue';
import XTaskNodeSearch from './XTaskNodeSearch.vue';

import { eventHub } from '../utils';

export default {
  name: 'XHeaderedGraph',
  components: {
    XGraph,
    XHeader,
    XCollapseButtons,
    XTaskNodeSearch,
  },
  computed: {
    ...mapGetters({
      // The root UUID to show, not necessarily the root UUID from the runMetadata.
      rootUuid: 'tasks/selectedRoot',
      canRevoke: 'tasks/canRevoke',
      listViewHeaderEntry: 'header/listViewHeaderEntry',
      runLogsViewHeaderEntry: 'header/runLogsViewHeaderEntry',
      helpViewHeaderEntry: 'header/helpViewHeaderEntry',
      timeChartViewHeaderEntry: 'header/timeChartViewHeaderEntry',
      centerHeaderEntry: 'header/centerHeaderEntry',
      showTaskDetailsHeaderEntry: 'header/showTaskDetailsHeaderEntry',
      liveUpdateToggleHeaderEntry: 'header/liveUpdateToggleHeaderEntry',
      killHeaderEntry: 'header/killHeaderEntry',
      inputsViewHeaderEntry: 'header/inputsViewHeaderEntry',
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
    headerParams() {
      let links = [
        this.liveUpdateToggleHeaderEntry(() => this.$store.dispatch('graph/toggleLiveUpdate')),
        this.centerHeaderEntry,
        this.showTaskDetailsHeaderEntry(
          () => this.$store.dispatch('graph/toggleShowTaskDetails'),
        ),
        this.inputsViewHeaderEntry,
        this.listViewHeaderEntry,
        this.timeChartViewHeaderEntry,
        this.runLogsViewHeaderEntry,
        this.helpViewHeaderEntry,
        this.killHeaderEntry,
      ];
      // Remove live update and kill options if the run isn't alive.
      if (!this.canRevoke) {
        links = _.filter(links, l => !_.includes(['liveUpdate', 'kill'], l.name));
      }

      return {
        title: this.runMetadata.uid,
        links,
      };
    },
  },
  methods: {
    focusOnFind() {
      this.$store.commit('tasks/toggleSearchOpen');
    },
    refreshGraph() {
      eventHub.$emit('graph-refresh');
    },
  },
};
</script>

<style scoped>
</style>
