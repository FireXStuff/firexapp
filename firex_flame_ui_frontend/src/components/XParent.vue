<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;">
    <div class="header">

      <div style="text-align: center" >
        Logs Directory:
        <input type="text" size=100 :value="logDir"
               :style="$asyncComputed.nodesByUuid.error ? 'border-color: red;' : ''"
               @keyup.enter="$router.push({ name: 'XGraph', query: { logDir: $event.target.value } })">
        <div :class="{spinner: $asyncComputed.nodesByUuid.updating}"></div>
      </div>
      <div style="display: flex; flex-direction: row;">
        <div>
          <router-link to="/">
            <img style='height: 36px;' src="../assets/firex_logo.png">
          </router-link>
        </div>
        <div class="uid">{{title ? title : uid}}</div>
        <div style="margin-left: auto; display: flex;">
          <div v-if="false" class="header-icon-button">
            <font-awesome-icon icon="search"></font-awesome-icon>
          </div>
          <div v-if="false" class="header-icon-button">
            <font-awesome-icon :icon="['far', 'eye']"></font-awesome-icon>
          </div>
            <div class="header-icon-button" v-on:click="eventHub.$emit('center')">
              <font-awesome-icon icon="bullseye"></font-awesome-icon>
            </div>

          <div v-if="false" class="header-icon-button">
            <font-awesome-icon icon="plus-circle"></font-awesome-icon>
          </div>
          <div class="header-icon-button">
            <!-- TODO: find a better way to always propagte the fetch key (i.e right now log dir, in general the UID -->
            <router-link :to="{ name: 'XList', params: { nodesByUuid: nodesByUuid }, query: {logDir: logDir}}">
              <font-awesome-icon icon="list-ul"></font-awesome-icon>
            </router-link>
          </div>
          <a :href="this.logsUrl" class="flame-link">View Logs</a>
          <a class="flame-link" href="help">Help</a>
        </div>
      </div>
    </div>
    <!-- Only show main panel after data is loaded -->
    <template v-if="$asyncComputed.nodesByUuid.success">
      <router-view v-on:title="title = $event" :nodesByUuid="nodesByUuid" :logDir="logDir"
                   v-on:logs_url="logsUrl = $event"></router-view>
    </template>
  </div>
</template>

<script>
import _ from 'lodash'
import {parseRecFileContentsToNodesByUuid, eventHub} from '../utils'

export default {
  name: 'XHeader',
  props: {
    logDir: {default: '/auto/firex-logs-sjc/djungic/FireX-djungic-190311-152310-63727'},
  },
  data () {
    return {
      title: '',
      logsUrl: this.logDir,
      eventHub: eventHub,
    }
  },
  computed: {
    uid () {
      let nodeWithUid = _.find(_.values(this.nodesByUuid), 'firex_bound_args.uid')
      if (nodeWithUid) {
        return nodeWithUid.firex_bound_args.uid
      }
      return ''
    },
  },
  asyncComputed: {
    nodesByUuid: {
      get () {
        return this.fetchTreeData(this.logDir)
      },
      default: {},
    },
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
  },
  watch: {
    '$route' (to, from) {
      // TODO: define log location per child route, not here.
      if (_.includes(['XGraph', 'XList'], to.name)) {
        this.logsUrl = 'http://firex.cisco.com' + this.logDir
      } else {
        this.logsUrl = ''
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

</style>
