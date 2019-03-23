<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;">
    <div class="header">

      <div v-if="displayMessage.content" class='notification' :style="'background: ' + displayMessage.color">
        <span style="top: 50%">{{displayMessage.content}}</span>
      </div>

      <div style="text-align: center; padding: 0 10px">
        <!--Flame Server:-->
        <!--<input type="text" size=20 :value="flameServer"-->
               <!--@keyup.enter="$router.push({ name: 'XGraph', query: { flameServer: $event.target.value.trim() } })"-->
               <!--:style="socketUpdateInProgress || socket.connected ? 'border-color: lightgreen;' : 'border-color: red;'">-->

        <!--Logs Directory:-->
        <!--<input type="text" size="100" :value="logDir"-->
               <!--:style="$asyncComputed.recFileNodesByUuid.error ? 'border-color: red;' : ''"-->
               <!--@keyup.enter="$router.push({ name: 'XGraph', query: { logDir: $event.target.value.trim() } })">-->

        <div v-show="false">{{socket.connected}}</div>
        <div :class="{spinner: $asyncComputed.recFileNodesByUuid.updating || socketUpdateInProgress}"></div>
      </div>
      <div style="display: flex; flex-direction: row;">
        <div>
          <router-link :to="{ name: 'XGraph', params: { nodesByUuid: nodesByUuid }}">
            <img style='height: 36px;' src="../assets/firex_logo.png">
          </router-link>
        </div>
        <div class="uid">{{title ? title : uid}}</div>

        <a :href="flameServer" class="flame-link">
          <font-awesome-icon icon="fire"></font-awesome-icon>
            Back to Legacy
          <font-awesome-icon icon="fire"></font-awesome-icon>
        </a>

        <div style="margin-left: auto; display: flex;">
          <div v-if="false" class="header-icon-button">
            <font-awesome-icon icon="search"></font-awesome-icon>
          </div>

          <div v-if="childSupportCenter" class="header-icon-button" v-on:click="eventHub.$emit('center')">
            <font-awesome-icon icon="bullseye"></font-awesome-icon>
          </div>

          <div v-if="liveUpdateAllowed" class="header-icon-button" :style="liveUpdate ? 'color: #2B2;' : ''"
               v-on:click="toggleLiveUpdate">
            <font-awesome-icon :icon="['far', 'eye']"></font-awesome-icon>
          </div>

          <div v-if="childSupportShowUuids" class="header-icon-button" :style="showUuids ? 'color: #2B2;' : ''"
               v-on:click="toggleShowUuids">
            <font-awesome-icon icon="plus-circle"></font-awesome-icon>
          </div>

          <div v-if="childSupportListLink" class="header-icon-button">
            <router-link :to="{ name: 'XList', params: { nodesByUuid: nodesByUuid }}">
              <font-awesome-icon icon="list-ul"></font-awesome-icon>
            </router-link>
          </div>
          <div v-if="childSupportGraphLink" class="header-icon-button">
            <router-link :to="{ name: 'XGraph', params: { nodesByUuid: nodesByUuid }}">
              <font-awesome-icon icon="sitemap"></font-awesome-icon>
            </router-link>
          </div>
          <!-- TODO: attribute view likely shouldn't be able to revoke entire run -->
          <div v-if="canRevoke" class="header-icon-button kill-button" v-on:click="revokeRoot">
              <font-awesome-icon icon="times"></font-awesome-icon>
          </div>
          <a :href="logsUrl" class="flame-link">View Logs</a>
          <a  v-if="supportLocation" :href="supportLocation" class="flame-link">Support</a>
          <router-link v-if="childSupportHelpLink" class="flame-link" :to="{ name: 'XHelp',
            query: {logDir: $route.query.logDir, flameServer: $route.query.flameServer}}">Help</router-link>
          <a v-if="codeUrl" class="flame-link" :href="codeUrl">
            <font-awesome-icon icon="file-code"></font-awesome-icon>
          </a>
        </div>
      </div>
    </div>
    <!-- Only show main panel after data is loaded -->
    <!-- TODO: is logDir still used by any children? If not, remove it.-->
    <router-view v-if="hasTasks"
                 :nodesByUuid="nodesByUuid"
                 :firexUid="uid"
                 :logDir="logDir"
                 :taskDetails="taskDetails"></router-view>
  </div>
</template>

<script>
import _ from 'lodash'
import {parseRecFileContentsToNodesByUuid, eventHub} from '../utils'
import io from 'socket.io-client'

export default {
  name: 'XParent',
  props: {
    logDir: {required: false, type: String},
    flameServer: {required: false, type: String},
  },
  data () {
    return {
      title: '',
      logsUrl: this.logDir,
      eventHub: eventHub,
      // TODO: clean this up by mapping event names, enablement variables, and components in a single structure.
      childSupportListLink: false,
      childSupportCenter: false,
      childSupportShowUuids: false,
      showUuids: false,
      childSupportLiveUpdate: false,
      liveUpdate: true,
      childSupportGraphLink: false,
      codeUrl: false,
      childSupportHelpLink: false,
      supportLocation: false,
      socketNodesByUuid: {},
      socketUpdateInProgress: false,
      taskDetails: {},
      displayMessage: {content: '', color: ''},
      receivedRevokeResponse: false,
    }
  },
  computed: {
    uid () {
      // TODO: super gross, get from server or as external param.
      return this.logDir.match(/.*(FireX-.*)\/?$/)[1]
    },
    nodesByUuid () {
      if (this.useRecFile) {
        return this.recFileNodesByUuid
      }
      return this.socketNodesByUuid
    },
    hasTasks () {
      return !_.isEmpty(this.nodesByUuid)
    },
    useRecFile () {
      return _.isEmpty(this.flameServer)
    },
    socket () {
      // TODO: have UI indications of socket state (connected, connection lost, etc.).
      if (this.useRecFile) {
        return {connected: false}
      }
      let socket = io(this.flameServer, {reconnection: false})
      this.setSocketNodesByUuid({}) // Clear data from previous socket.
      // TODO handle not connected after 5 seconds.
      this.startSocketListening(socket)
      // socket.on('disconnect', () => {
      //   console.log('Connection lost.')
      // })
      return socket
    },
    hasIncompleteTasks () {
      let incompleteStates = ['task-blocked', 'task-started', 'task-received']
      return _.some(this.nodesByUuid, n => _.includes(incompleteStates, n.state))
    },
    liveUpdateAllowed () {
      return this.childSupportLiveUpdate && !this.useRecFile && this.hasIncompleteTasks
    },
    canRevoke () {
      return !this.useRecFile && this.hasIncompleteTasks && this.socket.connected
    },
  },
  asyncComputed: {
    recFileNodesByUuid: {
      get () {
        if (!this.useRecFile) {
          return null
        }
        return this.fetchTreeData(this.logDir)
      },
      // default: {},
    },
  },
  created () {
    // TODO: clean this up by mapping event names, enablement variables, and components in a single structure.
    eventHub.$on('support-list-link', () => { this.childSupportListLink = true })
    eventHub.$on('support-graph-link', () => { this.childSupportGraphLink = true })
    eventHub.$on('support-help-link', () => { this.childSupportHelpLink = true })
    eventHub.$on('support-center', () => { this.childSupportCenter = true })
    eventHub.$on('support-add', () => { this.childSupportShowUuids = true })
    eventHub.$on('support-watch', () => { this.childSupportLiveUpdate = true })
    eventHub.$on('code_url', (c) => { this.codeUrl = c })
    eventHub.$on('support_location', (l) => { this.supportLocation = l })
    eventHub.$on('title', (t) => { this.title = t })
    eventHub.$on('logs_url', (l) => { this.logsUrl = l })
  },
  methods: {
    fetchTreeData (logsDir) {
      return fetch(logsDir + '/flame.rec')
        .then(function (r) {
          return r.text()
        })
        .then(function (recFileContent) {
          return parseRecFileContentsToNodesByUuid(recFileContent)
        })
    },
    setSocketNodesByUuid (newNodesByUuid) {
      // Order UUID keys by task_num.
      this.socketNodesByUuid = _.mapValues(_.groupBy(_.sortBy(newNodesByUuid, 'task_num'), 'uuid'), _.head)
    },
    mergeNodesByUuid (newDataByUuid) {
      _.each(newDataByUuid, (newData, uuid) => {
        // Note Vue can't deep watch for new properties, or watch nested objects automatically, so it's
        // necessary to use this.$set: https://vuejs.org/v2/api/#Vue-set
        if (!_.has(this.socketNodesByUuid, uuid)) {
          this.$set(this.socketNodesByUuid, uuid, newData)
        } else {
          _.each(newData, (v, k) => { this.$set(this.socketNodesByUuid[uuid], k, v) })
        }
      })
    },
    toggleLiveUpdate () {
      this.liveUpdate = !this.liveUpdate
      if (this.liveUpdate) {
        this.startSocketListening(this.socket)
      } else {
        this.stopSocketListening(this.socket)
      }
    },
    stopSocketListening (socket) {
      // Stop listening on everything.
      socket.off('graph-state')
      socket.off('full-state')
      socket.off('tasks-update')
    },
    startSocketListening (socket) {
      // full state refresh plus subscribe to incremental updates.
      socket.on('graph-state', (nodesByUuid) => {
        this.setSocketNodesByUuid(nodesByUuid)
        this.socketUpdateInProgress = false
        if (this.hasIncompleteTasks) {
          // Only start listening for incremental updates after we've processed the full state.
          socket.on('tasks-update', this.mergeNodesByUuid)
        }
      })
      socket.on('full-state', (nodesByUuid) => {
        this.setSocketNodesByUuid(nodesByUuid)
        this.socketUpdateInProgress = false
        if (this.hasIncompleteTasks) {
          // Only start listening for incremental updates after we've processed the full state.
          socket.on('tasks-update', this.mergeNodesByUuid)
        }
      })
      socket.emit('send-graph-state')
      socket.emit('send-full-state')
      this.socketUpdateInProgress = true
    },
    toggleShowUuids () {
      this.showUuids = !this.showUuids
      eventHub.$emit('toggle-uuids')
    },
    revokeRoot () {
      if (!this.canRevoke) {
        return
      }
      let terminate = confirm('Are you sure you want to terminate this FireX run?')
      if (terminate) {
        this.socket.on('revoke-success', () => {
          this.displayMessage = {content: 'Run terminated', color: '#F40'}
          this.receivedRevokeResponse = true
          this.socket.off('revoke-success')
          this.socket.off('revoke-failed')
          setTimeout(() => { this.displayMessage = {content: '', color: ''} }, 4000)
        })
        this.socket.on('revoke-failed', () => {
          this.displayMessage = {content: 'UNSUCCESSFUL TERMINATION', color: '#BBB'}
          this.receivedRevokeResponse = true
          this.socket.off('revoke-success')
          this.socket.off('revoke-failed')
          setTimeout(() => { this.displayMessage = {content: '', color: ''} }, 6000)
        })
        this.socket.emit('revoke-task', this.root())
        this.displayMessage = {content: 'Waiting for celery...', color: 'deepskyblue'}

        setTimeout(() => {
          if (!this.receivedRevokeResponse) {
            this.displayMessage = {content: 'No response from server.', color: '#BBB'}
          }
        }, 3000)
      }
    },
    root () {
      return _.head(_.filter(this.nodesByUuid, {'parent_id': null})).uuid
    },
    fetchTaskDetails (uuid) {
      if (this.useRecFile) {
        this.taskDetails = this.recFileNodesByUuid[uuid]
      } else {
        let eventName = 'task-details-' + uuid
        this.socket.on(eventName, (data) => {
          this.taskDetails = data
          this.socket.off(eventName)
          // TODO: add timeout on failure and  handle already disconntected.
        })
        this.socket.emit('send-task-details', uuid)
      }
    },
    clearSupportedChildInfo () {
      this.childSupportListLink = false
      this.childSupportCenter = false
      this.childSupportShowUuids = false
      this.childSupportLiveUpdate = false
      this.childSupportGraphLink = false
      this.childSupportHelpLink = false
      this.codeUrl = false
      this.supportLocation = false
      this.title = this.uid
      this.logsUrl = this.logDir
    },
  },
  beforeRouteEnter (to, from, next) {
    next(vm => {
      vm.clearSupportedChildInfo()
      if (to.name === 'XNodeAttributes') {
        vm.fetchTaskDetails(to.params.uuid)
      }
    })
  },
  watch: {
    '$route' (to, from) {
      this.clearSupportedChildInfo()
      // TODO: specify more supported actions in this way.
      if (to.meta.supportedActions) {
        to.meta.supportedActions.forEach(a => eventHub.$emit(a))
      }
      if (to.name === 'XNodeAttributes') {
        this.fetchTaskDetails(to.params.uuid)
      }
    },
  },
}
</script>

<style scoped>

.header {
  background-color: #EEE;
  border-bottom: 1px solid #000;
}

.uid {
  font-family: 'Source Sans Pro',sans-serif;
  margin: 0;
  padding: 0;
  margin-left: 6px;
  white-space: nowrap;
  font-size: 20px;
  line-height: 40px;
  font-weight: normal;
}

.flame-link {
  font-family: 'Source Sans Pro',sans-serif;
  vertical-align: top;
  border-left: 1px solid #000;
  line-height: 40px;
  text-align: center;
  padding: 0 8px;
  text-decoration: none;
  color: #000;
  border-radius: 0;
  font-size: 20px;
  justify-content: flex-end;
}

.header-icon-button {
  padding: 0 8px;
  border-left: 1px solid #000;
  justify-content: end;
  font-size: 20px;
  line-height: 40px;
  cursor: pointer;
  color: #000;
}

.header-icon-button:hover {
    color: #2980ff;
}

a {
  color: #000;
}

a:hover {
    color: #2980ff;
}

.kill-button {
  color: #900;
}

.kill-button:hover {
  color: #fff;
  background: #900;
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
