<template>
  <!-- Need prevent to avoid default browser behaviour.  -->
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
    <x-graph
      v-if="runMetadata.uid"
      :nodesByUuid="rootDescendantsByUuid"
      :showUuids="toggleStates.showTaskDetails"
      :liveUpdate="toggleStates.liveUpdate"
      :firexUid="runMetadata.uid"></x-graph>
  </div>
</template>

<script>
import _ from 'lodash';
import XGraph from './XGraph.vue';
import XHeader from './XHeader.vue';
import {
  eventHub, routeTo, hasIncompleteTasks, getDescendantUuids, orderByTaskNum,
} from '../utils';

export default {
  name: 'XHeaderedGraph',
  components: { XGraph, XHeader },
  props: {
    nodesByUuid: { required: true, type: Object },
    runMetadata: { required: true, type: Object },
    // The connected state of the socket data is being received from. False if there is no socket.
    isConnected: { required: true, type: Boolean },
    // The root UUID to show, not necessarily the root UUID from the runMetadata.
    rootUuid: { default: null },
  },
  data() {
    return {
      toggleStates: {
        liveUpdate: true,
        showTaskDetails: false,
      },
    };
  },
  created() {
    // Set initial live update state.
    const liveUpdate = _.find(this.headerParams.links, { name: 'liveUpdate' });
    if (liveUpdate) {
      liveUpdate.on(this.toggleStates.liveUpdate);
    }
    eventHub.$on('toggle-live-update', () => {
      this.toggleButtonState('liveUpdate');
    });
    eventHub.$on('toggle-uuids', () => { this.toggleButtonState('showTaskDetails'); });
  },
  computed: {
    isUidValid() {
      if (!this.runMetadata.uid) {
        return false;
      }
      return this.runMetadata.uid.startsWith('FireX-');
    },
    isAlive() {
      return this.isConnected && this.hasIncompleteTasks;
    },
    hasIncompleteTasks() {
      return hasIncompleteTasks(this.rootDescendantsByUuid);
    },
    rootDescendantsByUuid() {
      if (this.rootUuid === null) {
        return this.nodesByUuid;
      }
      const rootDescendantUuids = getDescendantUuids(this.rootUuid, this.nodesByUuid);
      return orderByTaskNum(_.pick(this.nodesByUuid, [this.rootUuid].concat(rootDescendantUuids)));
    },
    headerParams() {
      let links = [
        {
          name: 'liveUpdate',
          on: () => { eventHub.$emit('toggle-live-update'); },
          toggleState: this.toggleStates.liveUpdate,
          icon: ['far', 'eye'],
        },
        { name: 'center', on: () => eventHub.$emit('center'), icon: 'bullseye' },
        {
          name: 'showTaskDetails',
          on: () => eventHub.$emit('toggle-uuids'),
          toggleState: this.toggleStates.showTaskDetails,
          icon: 'plus-circle',
        },
        { name: 'list', to: routeTo(this, 'XList'), icon: 'list-ul' },
        {
          name: 'kill', on: () => eventHub.$emit('revoke-root'), _class: 'kill-button', icon: 'times',
        },
        {
          name: 'logs',
          href: `http://firex.cisco.com${this.runMetadata.logs_dir}`,
          text: 'View logs',
        },
        { name: 'help', to: routeTo(this, 'XHelp'), text: 'Help' },
      ];
      // Remove live update and kill options if the run isn't alive.
      if (!this.isAlive) {
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
    'toggleStates.liveUpdate': () => {
      eventHub.$emit('set-live-update', this.toggleStates.liveUpdate);
    },
  },
};
</script>

<style scoped>
</style>
