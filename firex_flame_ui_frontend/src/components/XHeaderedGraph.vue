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
              :links="headerParams.links"
              :legacyPath="headerParams.legacyPath">
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

import XGraph from './XGraph.vue';
import XHeader from './XHeader.vue';
import XCollapseButtons from './XCollapseButtons.vue';
import XTaskNodeSearch from './XTaskNodeSearch.vue';

import {
  eventHub, routeTo2,
} from '../utils';

export default {
  name: 'XHeaderedGraph',
  components: {
    XGraph,
    XHeader,
    XCollapseButtons,
    XTaskNodeSearch,
  },
  props: {
    // The root UUID to show, not necessarily the root UUID from the runMetadata.
    rootUuid: { default: null },
  },
  computed: {
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
          title: 'Live Update',
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
          text: 'Logs',
          icon: 'file-alt',
        },
        {
          name: 'help',
          to: routeTo2(this.$route.query, 'XHelp'),
          text: 'Help',
          icon: 'question-circle',
        },
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
  },
  methods: {
    focusOnFind() {
      this.$store.commit('tasks/toggleSearchOpen');
    },
    refreshGraph() {
      eventHub.$emit('graph-refresh');
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
</style>
