<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;">
    <div class="header">
      <!-- TODO: replace with toastr or similar -->
      <div v-if="displayMessage.content" class='notification'
           :style="'background: ' + displayMessage.color">
        <span style="top: 50%">{{displayMessage.content}}</span>
      </div>

      <div style="text-align: center; padding: 0 10px">
        <!--Flame Server:-->
        <!--<input type="text" size=20 :value="flameServer"-->
               <!--@keyup.enter="$router.push({ name: 'XGraph',
               query: { flameServer: $event.target.value.trim() } })"-->
               <!--:style="socketUpdateInProgress || socket.connected ?
               'border-color: lightgreen;' : 'border-color: red;'">-->

        <!--Logs Directory:-->
        <!--<input type="text" size="100" :value="inputLogDir"-->
               <!--:style="$asyncComputed.recFileNodesByUuid.error ?
               'border-color: red;' : ''"-->
               <!--@keyup.enter="$router.push({ name: 'XGraph',
               query: { logDir: $event.target.value.trim() } })">-->

        <!-- TODO: hack hack hack: this reference is necessary to populate the socket
        computed property -->
        <div v-show="false">{{socket.connected}}</div>
        <div :class="{spinner: updating}"></div>
      </div>
    </div>
    <!-- Only show main panel after data is loaded -->
    <!-- TODO: remove firexUid from downstream, replace with firexRunMetadata.
          Make sure metadata exists for all sources
    -->
    <!-- TODO: isConnected is too specific to supply to all children. Consider communicating
            this another way. -->
    <!-- TODO: will jump because UID is lazy loaded. Consider not rendering until
            we have the UID -->
    <router-view v-if="hasTasks"
                 :nodesByUuid="nodesByUuid"
                 :firexUid="uid"
                 :isConnected="socket.connected"
                 :runMetadata="firexRunMetadata"
                 :taskDetails="taskDetails"></router-view>
  </div>
</template>

<script>
import _ from 'lodash';
import io from 'socket.io-client';
import {
  parseRecFileContentsToNodesByUuid, eventHub, socketRequestResponse, hasIncompleteTasks,
  orderByTaskNum,
} from '../utils';

export default {
  name: 'XParent',
  props: {
    inputLogDir: { required: false, type: String },
    flameServer: { required: false, type: String },
  },
  data() {
    return {
      logDir: this.inputLogDir,
      socketNodesByUuid: {},
      socketUpdateInProgress: false,
      taskDetails: {},
      displayMessage: { content: '', color: '' },
      flameRunMetadata: { uid: '' },
    };
  },
  computed: {
    uid() {
      if (this.flameRunMetadata.uid) {
        return this.flameRunMetadata.uid;
      }
      if (this.logDir) {
        return this.logUid;
      }
      return 'Unknown';
    },
    logUid() {
      const matches = this.logDir.match(/.*(FireX-.*)\/?$/);
      if (matches.length) {
        return matches[1];
      }
      return 'Unknown';
    },
    logRunMetadata() {
      return {
        uid: this.logUid,
        logs_dir: this.logDir,
        root_uuid: this.rootUuid,
      };
    },
    firexRunMetadata() {
      return this.flameServer ? this.flameRunMetadata : this.logRunMetadata;
    },
    nodesByUuid() {
      if (this.useRecFile) {
        return this.recFileNodesByUuid;
      }
      return this.socketNodesByUuid;
    },
    hasTasks() {
      return !_.isEmpty(this.nodesByUuid);
    },
    useRecFile() {
      return _.isEmpty(this.flameServer);
    },
    socket() {
      // TODO: have UI indications of socket state (connected, connection lost, etc.).
      if (this.useRecFile) {
        return { connected: false };
      }
      // TODO: should probably timeout trying to reconnect after some time.
      const socket = io(this.flameServer, { reconnection: true });
      this.setSocketNodesByUuid({}); // Clear data from previous socket.
      this.startSocketListening(socket);
      // socket.on('disconnect', () => {
      //   console.log('Connection lost.')
      // })
      return socket;
    },
    hasIncompleteTasks() {
      return hasIncompleteTasks(this.nodesByUuid);
    },
    canRevoke() {
      return !this.useRecFile && this.hasIncompleteTasks && this.socket.connected;
    },
    updating() {
      return this.$asyncComputed.recFileNodesByUuid.updating || this.socketUpdateInProgress;
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
    eventHub.$on('task-search', (q) => {
      socketRequestResponse(
        this.socket,
        { name: 'task-search', data: q },
        {
          name: 'search-results',
          fn: (searchResult) => {
            eventHub.$emit('task-search-result', searchResult);
          },
        },
        null, null,
      );
    });

    eventHub.$on('set-live-update', this.setLiveUpdate);
    eventHub.$on('revoke-root', () => { this.revokeTask(this.rootUuid()); });
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
      // Order UUID keys by task_num.
      this.socketNodesByUuid = orderByTaskNum(newNodesByUuid);
    },
    mergeNodesByUuid(newDataByUuid) {
      _.each(newDataByUuid, (newData, uuid) => {
        // Note Vue can't deep watch for new properties, or watch nested objects automatically,
        // so it's necessary to use this.$set: https://vuejs.org/v2/api/#Vue-set
        if (!_.has(this.socketNodesByUuid, uuid)) {
          this.$set(this.socketNodesByUuid, uuid, newData);
        } else {
          _.each(newData, (v, k) => { this.$set(this.socketNodesByUuid[uuid], k, v); });
        }
      });
    },
    setLiveUpdate(val) {
      if (val) {
        this.startSocketListening(this.socket);
      } else {
        this.stopSocketListening(this.socket);
      }
    },
    stopSocketListening(socket) {
      // Stop listening on everything.
      socket.off('graph-state');
      socket.off('full-state');
      socket.off('tasks-update');
    },
    updateSocketFullState(socket, startListenForUpdates) {
      // full state refresh plus subscribe to incremental updates.
      socket.on('graph-state', (nodesByUuid) => {
        this.handleFullStateFromSocket(socket, nodesByUuid, startListenForUpdates);
      });
      // for backwards compatability with older flame servers. TODO: Delete in april 2018
      socket.on('full-state', (nodesByUuid) => {
        this.handleFullStateFromSocket(socket, nodesByUuid, startListenForUpdates);
      });
      socket.emit('send-graph-state');
      // for backwards comparability with older flame servers. TODO: Delete in april 2018
      socket.emit('send-full-state');
    },
    startSocketListening(socket) {
      this.updateSocketFullState(socket, true);
      this.socketUpdateInProgress = true;
      // TODO: going back to old flame isn't necessarily the right thing to do.
      setTimeout(() => {
        if (_.isEmpty(this.socketNodesByUuid) && this.socket.connected) {
          // How to handle no data? Fallback to rec?
          window.location.href = `${this.flameServer}?noUpgrade=true`;
        }
      }, 7000);
    },
    handleFullStateFromSocket(socket, nodesByUuid, startListenForUpdates) {
      this.setSocketNodesByUuid(nodesByUuid);
      this.socketUpdateInProgress = false;
      // Only start listening for incremental updates after we've processed the full state.
      if (startListenForUpdates && this.hasIncompleteTasks) {
        socket.on('tasks-update', this.mergeNodesByUuid);
      }
      socket.off('send-graph-state');
      socket.off('send-full-state');
    },
    revokeTask(uuid) {
      if (!this.canRevoke) {
        return;
      }
      const isRoot = uuid === this.rootUuid();
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
    rootUuid() {
      return _.head(_.filter(this.nodesByUuid, { parent_id: null })).uuid;
    },
    fetchTaskDetails(uuid) {
      if (this.useRecFile) {
        this.taskDetails = this.recFileNodesByUuid[uuid];
      } else {
        const eventName = `task-details-${uuid}`;
        this.socket.on(eventName, (data) => {
          this.taskDetails = data;
          this.socket.off(eventName);
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
              this.flameRunMetadata = data;
            },
          });
      }
    },
  },
  beforeRouteEnter(to, from, next) {
    next((vm) => {
      if (to.name === 'XNodeAttributes') {
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
    socket(newSocket) {
      this.flameRunMetadata = { uid: '' };
      if (newSocket) {
        this.setFlameRunMetadata(newSocket);
      }
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
