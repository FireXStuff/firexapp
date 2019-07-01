<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;">
    <!-- TODO: replace with toastr or similar -->
    <div v-if="displayMessage.content" class='tasks-header notification'
           :style="'background: ' + displayMessage.color">
        <span style="top: 50%">{{displayMessage.content}}</span>
    </div>
    <div v-if="updating" style="text-align: center; padding: 0 10px">
        <div :class="{spinner: updating}"></div>
    </div>
    <!-- Only show main panel after data is loaded. This guarantees safe access to api operations
    from child views, since the only way for tasks to be present is for the api accessor
    to be initialized. -->
    <router-view v-if="hasTasks"></router-view>
  </div>
</template>

<script>
import _ from 'lodash';

import * as api from '../api';
import {
  parseRecFileContentsToNodesByUuid, eventHub, orderByTaskNum, twoDepthAssign,
} from '../utils';

export default {
  name: 'XTasksParent',
  props: {
    inputLogDir: { required: false, type: String },
    inputFlameServer: { required: false, type: String },
    inputFireXId: { required: false, type: String },
  },
  data() {
    return {
      logDir: this.inputLogDir,
      updating: false,
      displayMessage: { content: '', color: '' },
      // Batch incoming task data in order to debounce incoming changes.
      newTaskDataToDispatch: {},
    };
  },
  computed: {
    logDirUid() {
      const matches = this.logDir.match(/.*(FireX-.*)\/?$/);
      if (matches.length) {
        return matches[1];
      }
      return 'Unknown';
    },
    rootUuid() {
      return this.$store.getters['tasks/rootUuid'];
    },
    logRunMetadata() {
      return {
        uid: this.logDirUid,
        logs_dir: this.logDir,
        root_uuid: this.rootUuid,
      };
    },
    flameServerUrl() {
      if (this.inputFlameServer) {
        return this.inputFlameServer;
      }
      if (!this.logDir) {
        // If we have neither an input flame server or a log dir (i.e. if we have no data source),
        // assume we're being hosted by a flame server and use the current origin.
        return window.location.origin;
      }
      return null;
    },
    hasTasks() {
      return this.$store.getters['tasks/hasTasks'];
    },
    hasIncompleteTasks() {
      return this.$store.getters['tasks/hasIncompleteTasks'];
    },
    canRevoke() {
      return this.$store.getters['tasks/canRevoke'];
    },
    liveUpdate() {
      return this.$store.state.graph.liveUpdate;
    },
    isApiConnected() {
      return this.$store.state.tasks.apiConnected;
    },
    uiConfig() {
      return this.$store.state.firexRunMetadata.uiConfig;
    },
  },
  created() {
    eventHub.$on('revoke-root', () => { this.revokeTask(this.rootUuid); });
    eventHub.$on('revoke-task', (uuid) => { this.revokeTask(uuid); });
    eventHub.$on('graph-refresh', () => { this.updateFullTasksState(this.liveUpdate); });
  },
  methods: {
    fetchNodesByUuidFromRecFile(recFileUrl) {
      return fetch(recFileUrl)
        .then(r => r.text())
        .then(recFileContent => parseRecFileContentsToNodesByUuid(recFileContent));
    },
    setNodesByUuid(newNodesByUuid) {
      this.newTaskDataToDispatch = {};
      this.$store.dispatch('tasks/setTasks', orderByTaskNum(newNodesByUuid));
    },
    debouncedDispatchTasksUpdate: _.debounce(
      // eslint-disable-next-line
      function () {
        this.$store.dispatch('tasks/addTasksData', this.newTaskDataToDispatch);
        this.newTaskDataToDispatch = {};
      // Wait at least 500ms of no events before updating tasks, up to a max of 1.5s.
      }, 500, { maxWait: 1500, leading: true, trailing: true },
    ),
    mergeNodesByUuid(newDataByUuid) {
      this.newTaskDataToDispatch = Object.freeze(
        twoDepthAssign(this.newTaskDataToDispatch, newDataByUuid),
      );
      this.debouncedDispatchTasksUpdate();
    },
    startLiveUpdate() {
      api.startLiveUpdate(this.mergeNodesByUuid);
    },
    updateFullTasksState(startListenForUpdates) {
      this.updating = true;
      // full state refresh plus subscribe to incremental updates.
      const taskGraphPromise = api.getTaskGraph();
      if (startListenForUpdates) {
        taskGraphPromise.then((nodesByUuid) => {
          this.startLiveUpdate();
          return nodesByUuid;
        });
      }
      taskGraphPromise.then((nodesByUuid) => {
        this.setNodesByUuid(nodesByUuid);
      });
      taskGraphPromise.finally(() => { this.updating = false; });
    },
    fetchAllTasksAndStartLiveUpdate() {
      this.updateFullTasksState(true);
      // TODO: going back to old flame isn't necessarily the right thing to do.
      // setTimeout(() => {
      //   if (!this.hasTasks && this.isApiConnected) {
      //     // How to handle no data? Fallback to rec?
      //     window.location.href = `${this.flameServerUrl}?noUpgrade=true`;
      //   }
      // }, 10000);
    },
    revokeTask(uuid) {
      if (!this.canRevoke) {
        return;
      }
      const isRoot = uuid === this.rootUuid;
      const messageDetail = isRoot ? 'this FireX run' : 'this task';

      // TODO: replace displayMessage display with toastr or similar.
      /* eslint-disable no-alert */
      const terminate = window.confirm(`Are you sure you want to terminate ${messageDetail}?`);
      if (terminate) {
        api.revokeTask(uuid).then(
          () => {
            const confirmationDetail = isRoot ? 'Run' : 'Task';
            this.displayMessage = { content: `${confirmationDetail} terminated`, color: '#F40' };
            setTimeout(() => { this.displayMessage = { content: '', color: '' }; }, 4000);
          },
          (errResponse) => {
            if (!_.isNil(errResponse) && _.get(errResponse, 'timeout', false)) {
              this.displayMessage = { content: 'No response from server.', color: '#BBB' };
            } else {
              // non-timeout failure.
              this.displayMessage = { content: 'UNSUCCESSFUL TERMINATION', color: '#BBB' };
              setTimeout(() => { this.displayMessage = { content: '', color: '' }; }, 8000);
            }
          },
        );
        this.displayMessage = { content: 'Waiting for celery...', color: 'deepskyblue' };
      }
    },
    setFlameRunMetadata() {
      api.getFireXRunMetadata().then((data) => {
        // TODO: is adding the server URL still necessary? This should be fetched from server.
        data.flameServerUrl = this.flameServerUrl;
        this.$store.commit('firexRunMetadata/setFlameRunMetadata', data);
      });
    },
    setApiAccessor(uiConfig) {
      if (uiConfig.access_mode === 'webserver-file') {
        const options = {
          modelPathTemplate: '/auto/firex-logs/<%- year %>/<%- month %>/'
            + '<%- day %>/<%- firex_id %>/flame_model/',
        };
        api.setAccessor('dump-files', this.inputFireXId, options);
      } else {
        // TODO: should probably timeout trying to reconnect after some time.
        api.setAccessor('socketio', this.flameServerUrl, {
          onConnect: () => this.$store.commit('tasks/setApiConnected', true),
          onDisconnect: () => this.$store.commit('tasks/setApiConnected', false),
        });
      }
      this.fetchAllTasksAndStartLiveUpdate();
      this.setFlameRunMetadata();
    },
    updateApiAccessor() {
      if (_.isNull(this.uiConfig)) {
        fetch('flame-ui-config.json')
          .then((r) => {
            if (r.ok) {
              return r.json();
            }
            return api.defaultUiConfig;
          },
          () => api.defaultUiConfig)
          .then(uiConfig => this.$store.commit('firexRunMetadata/setFlameUiConfig', uiConfig))
          .finally(() => this.setApiAccessor(this.uiConfig));
      } else {
        this.setApiAccessor(this.uiConfig);
      }
    },
  },
  watch: {
    liveUpdate(newLiveUpdate) {
      if (newLiveUpdate) {
        this.fetchAllTasksAndStartLiveUpdate();
      } else {
        api.stopLiveUpdate();
      }
    },
    flameServerUrl: {
      immediate: true,
      handler() {
        // Clear data from previous api accessor.
        this.setNodesByUuid({});
        this.updateApiAccessor();
      },
    },
  },
};
</script>

<style scoped>

.tasks-header {
  background-color: #EEE;
  border-bottom: 1px solid #000;
}

@keyframes spinner {
  to {
    transform: rotate(360deg);
  }
}

.spinner:before {
  content: '';
  box-sizing: border-box;
  position: absolute;
  top: 50%;
  left: 50%;
  width: 20px;
  height: 20px;
  margin-top: -10px;
  margin-left: -10px;
  border-radius: 50%;
  border-top: 2px solid #07d;
  border-right: 2px solid transparent;
  animation: spinner .6s linear infinite;
}

.notification {
  position: absolute;
  z-index: unset;
  display: inline-block;
  background: deepskyblue;
  border-bottom: 1px solid #000;
  width: 100%;
  text-align: center;
  height: 4em;
  line-height: 4em
}

</style>
