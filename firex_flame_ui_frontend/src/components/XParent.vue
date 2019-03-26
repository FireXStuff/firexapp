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
        <!--<input type="text" size="100" :value="inputLogDir"-->
               <!--:style="$asyncComputed.recFileNodesByUuid.error ? 'border-color: red;' : ''"-->
               <!--@keyup.enter="$router.push({ name: 'XGraph', query: { logDir: $event.target.value.trim() } })">-->

        <!-- TODO: hack hack hack: this reference is necessary to populate the socket computed property -->
        <div v-show="false">{{socket.connected}}</div>
        <div :class="{spinner: $asyncComputed.recFileNodesByUuid.updating || socketUpdateInProgress}"></div>
      </div>
    </div>
    <!-- Only show main panel after data is loaded -->
    <!-- TODO: remove firexUid from downstream, replace with firexRunMetadata. Make sure metadata exists for all sources
    -->
    <!-- TODO: canKill is too specific to supply to all children. Consider communicating this another way. -->
    <!-- TODO: will jump because UID is lazy loaded. Consider not rendering until we have the UID -->
    <router-view v-if="hasTasks"
                 :nodesByUuid="nodesByUuid"
                 :firexUid="uid"
                 :isConnected="socket.connected"
                 :runMetadata="flameRunMetadata"
                 :taskDetails="taskDetails"></router-view>
  </div>
</template>

<script>
import _ from 'lodash'
import {parseRecFileContentsToNodesByUuid, eventHub, socketRequestResponse, hasIncompleteTasks} from '../utils'
import io from 'socket.io-client'
import XTaskNodeSearch from './XTaskNodeSearch'

export default {
  name: 'XParent',
  components: {XTaskNodeSearch},
  props: {
    inputLogDir: {required: false, type: String},
    flameServer: {required: false, type: String},
  },
  data () {
    return {
      socketNodesByUuid: {},
      socketUpdateInProgress: false,
      taskDetails: {},
      displayMessage: {content: '', color: ''},
      flameRunMetadata: {uid: ''},
    }
  },
  computed: {
    uid () {
      if (this.flameRunMetadata.uid) {
        return this.flameRunMetadata.uid
      }
      if (this.logDir) {
        let matches = this.logDir.match(/.*(FireX-.*)\/?$/)
        if (matches.length) {
          return matches[1]
        }
      }
      return 'Unknown'
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
      // TODO: should probably timeout trying to reconnect after some time.
      let socket = io(this.flameServer, {reconnection: true})
      this.setSocketNodesByUuid({}) // Clear data from previous socket.
      // TODO handle not connected after 5 seconds.
      this.startSocketListening(socket)
      // socket.on('disconnect', () => {
      //   console.log('Connection lost.')
      // })
      return socket
    },
    hasIncompleteTasks () {
      return hasIncompleteTasks(this.nodesByUuid)
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
        return this.fetchNodesByUuidFromRecFile(this.logDir + '/flame.rec')
      },
      // default: {},
    },
  },
  created () {
    eventHub.$on('task-search', (q) => {
      socketRequestResponse(
        this.socket,
        {name: 'task-search', data: q},
        {
          name: 'search-results',
          fn: (searchResult) => {
            eventHub.$emit('task-search-result', searchResult)
          },
        },
        null, null)
    })

    eventHub.$on('set-live-update', this.setLiveUpdate)
    eventHub.$on('revoke-root', () => { this.revokeTask(this.rootUuid()) })
    eventHub.$on('revoke-task', (uuid) => { this.revokeTask(uuid) })
    this.setFlameRunMetadata(this.socket)
  },
  methods: {
    fetchNodesByUuidFromRecFile (recFileUrl) {
      return fetch(recFileUrl)
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
    setLiveUpdate (val) {
      this.liveUpdate = val
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
        this.handleFullStateFromSocket(socket, nodesByUuid)
      })
      // for backwards compatability with older flame servers. TODO: Delete in april 2018
      socket.on('full-state', (nodesByUuid) => {
        this.handleFullStateFromSocket(socket, nodesByUuid)
      })
      socket.emit('send-graph-state')
      // for backwards comparability with older flame servers. TODO: Delete in april 2018
      socket.emit('send-full-state')
      this.socketUpdateInProgress = true
      setTimeout(() => {
        if (_.isEmpty(this.socketNodesByUuid) && this.socket.connected) {
          // How to handle no data? Fallback to rec?
          window.location.href = this.flameServer + '?noUpgrade=true'
        }
      }, 4000)
    },
    handleFullStateFromSocket (socket, nodesByUuid) {
      this.setSocketNodesByUuid(nodesByUuid)
      this.socketUpdateInProgress = false
      if (this.hasIncompleteTasks) {
        // Only start listening for incremental updates after we've processed the full state.
        socket.on('tasks-update', this.mergeNodesByUuid)
      }
    },
    revokeTask (uuid) {
      if (!this.canRevoke) {
        return
      }
      let isRoot = uuid === this.rootUuid()
      let messageDetail = isRoot ? 'this FireX run' : 'this task'

      let terminate = confirm('Are you sure you want to terminate ' + messageDetail + '?')
      if (terminate) {
        // TODO: replace this messaging with something like toastr
        socketRequestResponse(
          this.socket,
          {name: 'revoke-task', data: uuid},
          {
            name: 'revoke-success',
            fn: () => {
              let confirmationDetail = isRoot ? 'Run' : 'Task'
              this.displayMessage = {content: confirmationDetail + ' terminated', color: '#F40'}
              setTimeout(() => { this.displayMessage = {content: '', color: ''} }, 4000)
            },
          },
          {
            name: 'revoke-failed',
            fn: () => {
              this.displayMessage = {content: 'UNSUCCESSFUL TERMINATION', color: '#BBB'}
              setTimeout(() => { this.displayMessage = {content: '', color: ''} }, 6000)
            },
          },
          {waitTime: 3000, fn: () => { this.displayMessage = {content: 'No response from server.', color: '#BBB'} }},
        )
        this.displayMessage = {content: 'Waiting for celery...', color: 'deepskyblue'}
      }
    },
    rootUuid () {
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
    setFlameRunMetadata (socket) {
      if (!this.useRecFile) {
        socketRequestResponse(socket,
          {name: 'send-run-metadata'},
          {
            name: 'run-metadata',
            fn: (data) => {
              this.flameRunMetadata = data
            },
          }
        )
      }
    },
  },
  beforeRouteEnter (to, from, next) {
    next(vm => {
      if (to.name === 'XNodeAttributes') {
        vm.fetchTaskDetails(to.params.uuid)
      }
    })
  },
  watch: {
    '$route' (to, from) {
      if (to.name === 'XNodeAttributes') {
        this.fetchTaskDetails(to.params.uuid)
      }
    },
    socket (newSocket, oldSocket) {
      this.flameRunMetadata = {uid: ''}
      if (newSocket) {
        this.setFlameRunMetadata(newSocket)
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
