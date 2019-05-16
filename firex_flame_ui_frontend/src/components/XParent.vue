<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;">
    <div class="header">
      <!-- TODO: replace with toastr or similar -->
      <div v-if="displayMessage.content" class='notification'
           :style="'background: ' + displayMessage.color">
        <span style="top: 50%">{{displayMessage.content}}</span>
      </div>

      <div style="text-align: center; padding: 0 10px">
        <div :class="{spinner: updating}"></div>
      </div>
    </div>
    <!-- Only show main panel after data is loaded -->
    <router-view v-if="hasTasks"></router-view>
  </div>
</template>

<script>
import io from 'socket.io-client';
import {
  parseRecFileContentsToNodesByUuid, eventHub, socketRequestResponse,
  orderByTaskNum,
} from '../utils';

export default {
  name: 'XParent',
  props: {
    inputLogDir: { required: false, type: String },
    inputFlameServer: { required: false, type: String },
  },
  data() {
    return {
      logDir: this.inputLogDir,
      socketUpdateInProgress: false,
      displayMessage: { content: '', color: '' },
      socket: null,
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
    useRecFile() {
      return !this.flameServerUrl;
    },
    hasIncompleteTasks() {
      return this.$store.getters['tasks/hasIncompleteTasks'];
    },
    canRevoke() {
      return this.$store.getters['tasks/canRevoke'];
    },
    updating() {
      return this.$asyncComputed.recFileNodesByUuid.updating || this.socketUpdateInProgress;
    },
    liveUpdate() {
      return this.$store.state.graph.liveUpdate;
    },
  },
  asyncComputed: {
    recFileNodesByUuid: {
      get() {
        if (!this.useRecFile) {
          return null;
        }
        return this.fetchNodesByUuidFromRecFile(`${this.logDir}/flame.rec`);
      },
      // default: {},
    },
  },
  created() {
    eventHub.$on('revoke-root', () => { this.revokeTask(this.rootUuid); });
    eventHub.$on('revoke-task', (uuid) => { this.revokeTask(uuid); });
    eventHub.$on('graph-refresh', () => {
      if (this.useRecFile) {
        this.$asyncComputed.recFileNodesByUuid.update();
      } else {
        this.updateSocketFullState(this.socket, false);
      }
    });
    this.setFlameRunMetadata(this.socket);
  },
  methods: {
    fetchNodesByUuidFromRecFile(recFileUrl) {
      return fetch(recFileUrl)
        .then(r => r.text())
        .then(recFileContent => parseRecFileContentsToNodesByUuid(recFileContent));
    },
    setSocketNodesByUuid(newNodesByUuid) {
      this.$store.dispatch('tasks/setTasks', orderByTaskNum(newNodesByUuid));
    },
    mergeNodesByUuid(newDataByUuid) {
      this.$store.dispatch('tasks/addTasksData', newDataByUuid);
    },
    stopSocketListening(socket) {
      // Stop listening on everything.
      socket.off('graph-state');
      socket.off('tasks-update');
    },
    updateSocketFullState(socket, startListenForUpdates) {
      // full state refresh plus subscribe to incremental updates.
      socket.on('graph-state', (nodesByUuid) => {
        this.handleFullStateFromSocket(socket, nodesByUuid, startListenForUpdates);
      });
      socket.emit('send-graph-state');
    },
    startSocketListening(socket) {
      this.updateSocketFullState(socket, true);
      this.socketUpdateInProgress = true;
      // TODO: going back to old flame isn't necessarily the right thing to do.
      setTimeout(() => {
        if (!this.hasTasks && this.socket.connected) {
          // How to handle no data? Fallback to rec?
          window.location.href = `${this.flameServerUrl}?noUpgrade=true`;
        }
      }, 10000);
    },
    handleFullStateFromSocket(socket, nodesByUuid, startListenForUpdates) {
      this.setSocketNodesByUuid(nodesByUuid);
      this.socketUpdateInProgress = false;
      // Only start listening for incremental updates after we've processed the full state.
      if (startListenForUpdates && this.hasIncompleteTasks) {
        socket.on('tasks-update', this.mergeNodesByUuid);
      }
      socket.off('send-graph-state');
    },
    revokeTask(uuid) {
      if (!this.canRevoke) {
        return;
      }
      const isRoot = uuid === this.rootUuid;
      const messageDetail = isRoot ? 'this FireX run' : 'this task';

      // TODO: replace with toastr or similar.
      /* eslint-disable no-alert */
      const terminate = window.confirm(`Are you sure you want to terminate ${messageDetail}?`);
      if (terminate) {
        socketRequestResponse(
          this.socket,
          { name: 'revoke-task', data: uuid },
          {
            name: 'revoke-success',
            fn: () => {
              const confirmationDetail = isRoot ? 'Run' : 'Task';
              this.displayMessage = { content: `${confirmationDetail} terminated`, color: '#F40' };
              setTimeout(() => { this.displayMessage = { content: '', color: '' }; }, 4000);
            },
          },
          {
            name: 'revoke-failed',
            fn: () => {
              this.displayMessage = { content: 'UNSUCCESSFUL TERMINATION', color: '#BBB' };
              setTimeout(() => { this.displayMessage = { content: '', color: '' }; }, 6000);
            },
          },
          { waitTime: 5000, fn: () => { this.displayMessage = { content: 'No response from server.', color: '#BBB' }; } },
        );
        this.displayMessage = { content: 'Waiting for celery...', color: 'deepskyblue' };
      }
    },
    fetchTaskDetails(uuid) {
      if (this.useRecFile) {
        this.$store.dispatch('tasks/setDetailedTask', this.recFileNodesByUuid[uuid]);
      } else {
        const eventName = `task-details-${uuid}`;
        this.socket.on(eventName, (data) => {
          this.socket.off(eventName);
          this.$store.dispatch('tasks/setDetailedTask', data);
          // TODO: add timeout on failure and  handle already disconntected.
        });
        this.socket.emit('send-task-details', uuid);
      }
    },
    setFlameRunMetadata(socket) {
      if (!this.useRecFile) {
        socketRequestResponse(socket,
          { name: 'send-run-metadata' },
          {
            name: 'run-metadata',
            fn: (data) => {
              data.flameServerUrl = this.flameServerUrl;
              this.$store.commit('firexRunMetadata/setFlameRunMetadata', data);
            },
          });
      }
    },
  },
  beforeRouteEnter(to, from, next) {
    next((vm) => {
      if (to.name === 'XNodeAttributes') {
        // Add all data needed by XNodeAttributes, such as
        vm.fetchTaskDetails(to.params.uuid);
      }
    });
  },
  watch: {
    $route(to) {
      if (to.name === 'XNodeAttributes') {
        this.fetchTaskDetails(to.params.uuid);
      }
    },
    liveUpdate(newLiveUpdate) {
      if (newLiveUpdate) {
        this.startSocketListening(this.socket);
      } else {
        this.stopSocketListening(this.socket);
      }
    },
    flameServerUrl: {
      immediate: true,
      handler(newFlameServerUrl) {
        // Clear data from previous socket.
        this.setSocketNodesByUuid({});

        if (this.useRecFile) {
          this.socket = { connected: false };
        } else {
          // TODO: have UI indications of socket state (connected, connection lost, etc.).
          // TODO: should probably timeout trying to reconnect after some time.
          this.socket = io(newFlameServerUrl);

          this.startSocketListening(this.socket);
          this.setFlameRunMetadata(this.socket);
          this.socket.on('connect', () => {
            this.$store.commit('tasks/setSocketConnected', true);
          });
          this.socket.on('disconnect', () => {
            this.$store.commit('tasks/setSocketConnected', false);
          });
        }
      },
    },
  },
};
</script>

<style scoped>

.header {
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
