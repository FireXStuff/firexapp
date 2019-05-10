<template>
  <!-- Need prevent to avoid default browser behaviour of ctrl-f.  -->
  <!-- 191 is the '/' keycode -->
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;"
       @keydown.ctrl.f.prevent="focusOnFind"
       @keyup.191.prevent="focusOnFind"
       @keydown.u.exact="toggleButtonState('liveUpdate')"
       @keydown.d.exact="toggleButtonState('showTaskDetails')"
       @keydown.r.exact="refreshGraph"
       tabindex="0">
    <x-header :title="headerParams.title"
              :links="headerParams.links"
              :legacyPath="headerParams.legacyPath"
              :enableSearch="true"
    ></x-header>
    <!-- TODO: not sure where the best level to gate on UID is, but need UID to key on
    localStorage within x-graph-->
    <x-graph v-if="runMetadata.uid"></x-graph>
  </div>
</template>

<script>
import _ from 'lodash';
import XGraph from './XGraph.vue';
import XHeader from './XHeader.vue';
import {
  eventHub, routeTo2,
} from '../utils';

export default {
  name: 'XHeaderedGraph',
  components: { XGraph, XHeader },
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
      // return this.isConnected && this.hasIncompleteTasks;
    },
    // TODO: add support, setting root in state store & trickling down the selected graph.
    // rootDescendantsByUuid() {
    //   if (this.rootUuid === null) {
    //     return this.nodesByUuid;
    //   }
    //   return this.$store.getters['tasks/descendantTasksByUuid'](this.rootUuid);
    // },
    headerParams() {
      let links = [
        {
          name: 'liveUpdate',
          on: () => this.$store.dispatch('graph/toggleLiveUpdate'),
          toggleState: this.toggleStates.liveUpdate,
          icon: ['far', 'eye'],
        },
        { name: 'center', on: () => eventHub.$emit('center'), icon: 'bullseye' },
        {
          name: 'showTaskDetails',
          on: () => this.$store.dispatch('graph/toggleShowTaskDetails'),
          toggleState: this.toggleStates.showTaskDetails,
          icon: 'plus-circle',
        },
        { name: 'list', to: routeTo2(this.$route.query, 'XList'), icon: 'list-ul' },
        {
          name: 'kill', on: () => eventHub.$emit('revoke-root'), _class: 'kill-button', icon: 'times',
        },
        {
          name: 'logs',
          href: `http://firex.cisco.com${this.runMetadata.logs_dir}`,
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
  },
  methods: {
    focusOnFind() {
      eventHub.$emit('find-focus');
    },
    toggleButtonState(stateKey) {
      this.toggleStates[stateKey] = !this.toggleStates[stateKey];
    },
    refreshGraph() {
      eventHub.$emit('graph-refresh');
    },
  },
  watch: {
    rootUuid() {
      // Center new graph when root node changes on next render
      // (after nodes for only the new root are shown).
      if (this.rootUuid !== null) {
        this.$nextTick(() => { eventHub.$emit('center'); });
      }
    },
  },
};
</script>

<style scoped>
</style>
