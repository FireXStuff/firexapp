<template>
  <!-- Need prevent to avoid default browser behaviour of ctrl-f.  -->
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;"
       @keydown.ctrl.f.prevent="focusOnFind"
       @keyup="onFindKey"
       @keyup.u.exact="toggleLiveUpdate"
       @keyup.d.exact="toggleShowTaskDetails"
       @keyup.r.exact="refreshGraph"
       tabindex="0">
    <x-header :title="headerParams.title"
              :showCompletionReportLink="true"
              :links="headerParams.links">
      <template v-slot:prebuttons>
        <x-task-node-search :findUncollapsedAncestor="true"/>
        <x-collapse-buttons/>
      </template>
    </x-header>
    <!-- TODO: not sure where the best level to gate on UID is, but need UID to key on
        localStorage within x-graph-->
    <x-graph v-if="runMetadata.uid"/>
  </div>
</template>

<script>
import _ from 'lodash';
import { mapActions, mapState } from 'pinia';

import XGraph from './XGraph.vue';
import XHeader from './XHeader.vue';
import XCollapseButtons from './XCollapseButtons.vue';
import XTaskNodeSearch from './XTaskNodeSearch.vue';

import { useTasksStore } from '../store/modules/tasks';
import { useGraphStore } from '../store/modules/graph';
import { useHeaderStore } from '../store/modules/header';
import { useFirexRunMetadataStore } from '../store/modules/firexRunMetadata';
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
    ...mapState(useTasksStore, {
      // The root UUID to show, not necessarily the root UUID from the runMetadata.
      rootUuid: 'selectedRoot',
      canRevoke: 'canRevoke',
    }),
    ...mapState(useHeaderStore, [
      'listViewHeaderEntry',
      'runLogsViewHeaderEntry',
      'helpViewHeaderEntry',
      'timeChartViewHeaderEntry',
      'centerHeaderEntry',
      'showTaskDetailsHeaderEntry',
      'liveUpdateToggleHeaderEntry',
      'killHeaderEntry',
      'inputsViewHeaderEntry',
    ]),
    ...mapState(useGraphStore, ['liveUpdate', 'showTaskDetails']),
    toggleStates() {
      return {
        liveUpdate: this.liveUpdate,
        showTaskDetails: this.showTaskDetails,
      };
    },
    runMetadata() {
      return useFirexRunMetadataStore();
    },
    headerParams() {
      let links = [
        this.liveUpdateToggleHeaderEntry(this.toggleLiveUpdate),
        this.centerHeaderEntry,
        this.showTaskDetailsHeaderEntry(this.toggleShowTaskDetails),
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
    ...mapActions(useGraphStore, ['toggleLiveUpdate', 'toggleShowTaskDetails']),
    ...mapActions(useTasksStore, ['toggleSearchOpen']),
    focusOnFind() {
      this.toggleSearchOpen();
    },
    onFindKey(event) {
      // Vue 3 dropped keyCode modifiers; this was @keyup.191 ('/').
      if (event.key === '/') {
        event.preventDefault();
        this.focusOnFind();
      }
    },
    refreshGraph() {
      eventHub.$emit('graph-refresh');
    },
  },
};
</script>

<style scoped>
</style>
