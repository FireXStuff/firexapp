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
    <router-view v-if="hasTasks"/>
    <x-error v-else-if="errorDetailMessage" :message="errorDetailMessage"/>
  </div>
</template>

<script>
import _ from 'lodash';
import { mapGetters, mapState } from 'vuex';

import * as api from '../api';
import {
  parseRecFileContentsToNodesByUuid, eventHub, twoDepthAssign, tasksViewKeyRouteChange,
} from '../utils';
import XError from './XError.vue';

export default {
  name: 'XTasksParent',
  components: { XError },
  data() {
    return {
      updating: false,
      displayMessage: { content: '', color: '' },
      // Batch incoming task data in order to debounce incoming changes.
      newTaskDataToDispatch: {},
      errorDetailMessage: null,
    };
  },
  computed: {
    ...mapState({
      liveUpdate: state => state.graph.liveUpdate,
      isApiConnected: state => state.tasks.apiConnected,
      uiConfig: state => state.header.uiConfig,
    }),
    ...mapGetters({
      rootUuid: 'tasks/rootUuid',
      hasTasks: 'tasks/hasTasks',
      hasIncompleteTasks: 'tasks/hasIncompleteTasks',
      canRevoke: 'tasks/canRevoke',
      // TODO: at one time there is exclusively either a server URL or a firex_id.
      // It'd be better if the store knew which type of key there was, and this component
      // reasoned in terms of a general key. This might mean moving some (or all) of the
      // API accessor config to the store (or wherever handles the different data key types).
      inputFlameServerUrl: 'header/inputFlameServerUrl',
      inputFireXId: 'header/inputFireXId',
    }),
    taskDataKey() {
      if (this.inputFireXId) {
        return this.inputFireXId;
      }
      return this.inputFlameServerUrl;
    },
    backfillFlameModelCommand() {
      const firexBin = _.get(this.uiConfig, 'firex_bin', 'firex');
      return `${firexBin} --chain BackfillFlameModel --firex_id_to_backfill ${this.inputFireXId}`;
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
      this.$store.dispatch('tasks/setTasks', newNodesByUuid);
    },
    debouncedDispatchTasksUpdate: _.debounce(
      // eslint-disable-next-line
      function () {
        this.$store.dispatch('tasks/addTasksData', this.newTaskDataToDispatch);
        this.newTaskDataToDispatch = {};
      // Wait at least 500ms of no events before updating tasks, up to a max of 3s.
      }, 500, { maxWait: 3000, leading: true, trailing: true },
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
      return taskGraphPromise.then(
        (nodesByUuid) => {
          this.setNodesByUuid(nodesByUuid);
        },
        () => {
          this.errorDetailMessage = `Failed to fetch tasks for ${this.taskDataKey}.
        You can attempt to reconstruct the data by running:
          ${this.backfillFlameModelCommand}`;
        },
      ).finally(() => {
        this.updating = false;
      });
    },
    fetchAllTasksAndStartLiveUpdate() {
      this.updateFullTasksState(true);
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
      // TODO: consider adding retries in file accessor since this is the first query per run.
      return api.getFireXRunMetadata().then(
        (runMetadata) => {
          this.$store.commit('firexRunMetadata/setFlameRunMetadata', runMetadata);
          return runMetadata;
        },
        (r) => {
          if (r.status === 401) {
            this.errorDetailMessage = 'Authentication failure; refresh your '
            + 'browser or login via private/incognito window to update '
            + 'authentication tokens.';
          } else {
            this.errorDetailMessage = `Failed to find Flame data for ${this.taskDataKey}`;
          }
          return Promise.reject(this.errorDetailMessage);
        },
      );
    },
    updateApiAccessor() {
      let resultPromise;
      if (this.uiConfig.access_mode === 'webserver-file') {
        this.setWebserverFileApiAccessor();
        // Get the run_metadata via the webserver-file accessor we just set, but change to a
        // socketio accessor to get live updates if the run is in-progress.
        resultPromise = this.setFlameRunMetadata().then((runMetadata) => {
          if (!runMetadata.run_complete) {
            this.setSocketIoApiAccessor(runMetadata.flame_url);
          }
        });
      } else if (_.includes(['socketio-origin', 'socketio-param'], this.uiConfig.access_mode)) {
        this.setSocketIoApiAccessor(this.inputFlameServerUrl);
        resultPromise = this.setFlameRunMetadata();
      } else {
        const error = `UI misconfiguration: unknown access_mode ${this.uiConfig.access_mode}`;
        this.errorDetailMessage = error;
        resultPromise = Promise.reject(error);
      }
      return resultPromise;
    },
    resetDataAndUpdateApiAccessor() {
      this.errorDetailMessage = null;
      if (_.isNull(this.uiConfig)) {
        // uiConfig is fetched from server and therefore lazy loaded. We can't initialize
        // an API accessor without the uiConfig, since the access_mode is needed.
        // This function will be re-executed once the uiConfig is non-null,
        // then the accessor will be created.
        return;
      }
      // Clear data from previous api accessor.
      this.setNodesByUuid({});
      this.updateApiAccessor().then(
        // Fetch data from the accessor we just set.
        () => this.fetchAllTasksAndStartLiveUpdate(),
      );
    },
    setSocketIoApiAccessor(flameServerUrl) {
      // TODO: should probably timeout trying to reconnect after some time.
      api.setAccessor('socketio', flameServerUrl, {
        onConnect: () => this.$store.commit('tasks/setApiConnected', true),
        onDisconnect: () => this.$store.commit('tasks/setApiConnected', false),
        onReconectFailed: () => {
          // Fallback from socketio to webserver file and re-fetch tasks.
          this.setWebserverFileApiAccessor();
          this.updateFullTasksState(false)
            .then(this.setFlameRunMetadata)
            .then((runMetadata) => {
              if (!runMetadata.run_complete) {
                // Run is not marked complete after failing to reconnect to socketio server.
                // All in-progress statuses will never be updated, so show them as incomplete.
                this.$store.dispatch('tasks/setInProgressTasksToIncomplete');
              }
            });
        },
        socketPathTemplate: _.get(this.uiConfig, 'flame_live_path_template', null),
      });
    },
    setWebserverFileApiAccessor() {
      const modelPathTemplate = this.uiConfig.model_path_template;
      api.setAccessor('dump-files', this.inputFireXId, { modelPathTemplate });
    },
    commitUiConfig(uiConfig) {
      this.$store.commit('header/setFlameUiConfig', uiConfig);
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
    // TODO: does it make sense for the 'update api accessor on data source change' logic to be
    // here? Should it be in the store itself?
    inputFlameServerUrl: {
      immediate: true,
      handler() {
        if (this.inputFlameServerUrl) {
          this.resetDataAndUpdateApiAccessor();
        }
      },
    },
    inputFireXId: {
      immediate: true,
      handler() {
        if (this.inputFireXId) {
          this.resetDataAndUpdateApiAccessor();
        }
      },
    },
    hasIncompleteTasks() {
      if (!this.hasIncompleteTasks && this.uiConfig.access_mode === 'webserver-file') {
        // All tasks are complete, change data source from socket (for live update)
        // to files (static data).
        this.setWebserverFileApiAccessor();
      }
    },
    // UI Config is lazy loaded and not valid initially, therefore don't set 'immediate'.
    uiConfig() { this.resetDataAndUpdateApiAccessor(); },
  },
  beforeRouteEnter(to, from, next) {
    tasksViewKeyRouteChange(to, from, next, (vm, uiConfig) => vm.commitUiConfig(uiConfig));
  },
  beforeRouteUpdate(to, from, next) {
    tasksViewKeyRouteChange(to, from, next, (vm, uiConfig) => vm.commitUiConfig(uiConfig));
  },
};
</script>

<style scoped>

.tasks-header {
  background-color: #EEE;
  border-bottom: 1px solid #000;
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
