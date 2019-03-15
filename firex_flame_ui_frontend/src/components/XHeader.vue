<template>
  <div>
    <div class="header">

      <div style="text-align: center">
          Logs Directory:
          <input type="text" size=100 :value="logDir" :style="fetchFailed ? 'border-color: red;' : ''"
                 @keyup.enter="$router.push({ name: 'XGraph', query: { logDir: $event.target.value } })">
      </div>
      <div class="header">
        <a href="/" class="logo"><img style='height: 36px;' src="../assets/firex_logo.png"></a>
        <div class="uid">{{title}}</div>
        <a class="flame-link" href="help">Help</a>
        <a :href="this.logsUrl" class="flame-link">View Logs</a>
        <div class="header-icon-button">
          <!-- Should send access to all nodes.-->
          <router-link :to="{ name: 'XList', props: true }">
            <font-awesome-icon icon="list-ul" size="1x"></font-awesome-icon>
          </router-link>
        </div>
        <div v-if="false" class="header-icon-button">
          <font-awesome-icon icon="plus-circle" size="1x"></font-awesome-icon>
        </div>
        <div v-if="false" class="header-icon-button" >
          <font-awesome-icon :icon="['far', 'eye']" size="1x"></font-awesome-icon>
        </div>
        <div v-if="false" class="header-icon-button">
          <font-awesome-icon icon="bullseye" size="1x"></font-awesome-icon>
        </div>
        <div v-if="false" class="header-icon-button" >
          <font-awesome-icon icon="search" size="1x"></font-awesome-icon>
        </div>
      </div>
    </div>
    <router-view v-on:title="title = $event" :nodesByUuid="nodesByUuid"
                 v-on:logs_url="logsUrl = $event"></router-view>
  </div>
</template>

<script>
/* eslint-disable */

import _ from 'lodash';
import {parseRecFileContentsToNodesByUuid} from '../parse_rec.js'

export default {
  name: 'XHeader',
  props: {
    logDir: {default: '/auto/firex-logs-sjc/djungic/FireX-djungic-190311-152310-63727'}
  },
  data () {
    return {title: '', logsUrl: this.logDir, fetchFailed: false}
  },
  asyncComputed: {
    nodesByUuid () {
      let vm = this
      vm.fetchFailed  = false
      return fetch(this.logDir + '/flame.rec')
        .then(function (r) {
          return r.text()
        })
        .then(function (rec_file_content) {
          let nodesByUuid = parseRecFileContentsToNodesByUuid(rec_file_content)
          vm.fetchFailed  = false
          return nodesByUuid
        })
      .catch(error => { vm.fetchFailed  = true; })
    }
  },
  watch: {
    '$route' (to, from) {
      if (_.includes(['XGraph', 'XList'], to.name)) {
        this.logsUrl = 'http://firex.cisco.com' + this.logDir
      }
      else {
        this.logsUrl = ''
      }
    }
  }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>

.header {
  width: 100%;
  background-color: #EEE;
  border-bottom: 1px solid #000;
  /*text-align: center;*/
}

.uid {
  font-family: 'Source Sans Pro',sans-serif;
  display: inline-block;
  margin: 0;
  padding: 0;
  margin-left: 6px;
  white-space: nowrap;
  font-size: 20px;
  line-height: 40px;
  position: absolute;
  font-weight: normal;
}

.flame-link {
  font-family: 'Source Sans Pro',sans-serif;
  vertical-align: top;
  border-left: 1px solid #000;
  display: inline-block;
  line-height: 40px;
  text-align: center;
  padding: 0 8px;
  text-decoration: none;
  color: #000;
  border-radius: 0;
  font-size: 20px;
  float: right;
}
.header-icon-button {
  padding: 0 8px;
  border-left: 1px solid #000;
  float: right;
  font-size: 20px;
  line-height: 40px;
}

</style>
