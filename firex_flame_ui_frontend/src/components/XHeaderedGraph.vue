<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;">
    <x-header :title="headerParams.title"
              :links="headerParams.links"
              :legacyPath="headerParams.legacyPath"
              :enableSearch="true"
    ></x-header>
    <x-graph :nodesByUuid="nodesByUuid" :firexUid="runMetadata.uid"></x-graph>
  </div>
</template>

<script>
import XGraph from './XGraph'
import XHeader from './XHeader'
import {eventHub, routeTo} from '../utils'
import _ from 'lodash'

export default {
  name: 'XHeaderedGraph',
  components: {XGraph, XHeader},
  props: {
    nodesByUuid: {required: true, type: Object},
    runMetadata: {required: true, type: Object},
    isConnected: {required: true, type: Boolean},
  },
  created () {
    let liveUpdate = _.find(this.headerParams.links, {'name': 'liveUpdate'})
    if (liveUpdate) {
      liveUpdate.on(true)
    }
  },
  computed: {
    isAlive () {
      return this.isConnected && this.hasIncompleteTasks
    },
    hasIncompleteTasks () {
      let incompleteStates = ['task-blocked', 'task-started', 'task-received', 'task-unblocked']
      return _.some(this.nodesByUuid, n => _.includes(incompleteStates, n.state))
    },
    headerParams () {
      let links = [

        //   <x-task-node-search></x-task-node-search>
        //
        {
          name: 'liveUpdate',
          on: (state) => eventHub.$emit('set-live-update', state),
          toggleState: true,
          initialState: true,
          icon: ['far', 'eye'],
        },
        {name: 'center', on: () => eventHub.$emit('center'), icon: 'bullseye'},
        {
          name: 'showTaskDetails',
          on: () => eventHub.$emit('toggle-uuids'),
          toggleState: true,
          initialState: false,
          icon: 'plus-circle',
        },
        {name: 'list', to: routeTo(this, 'XList'), icon: 'list-ul'},
        // <!-- TODO: attribute view likely shouldn't be able to revoke entire run -->
        // <div v-if="canRevoke" class="header-icon-button kill-button" v-on:click="revokeRoot">
        //     <font-awesome-icon icon="times"></font-awesome-icon>
        // </div>
        {name: 'kill', on: () => eventHub.$emit('revoke-root'), _class: 'kill-button', icon: 'times'},
        {name: 'logs', href: this.runMetadata.logs_dir, text: 'View logs'},
        {name: 'help', to: routeTo(this, 'XHelp'), text: 'Help'},
      ]
      if (!this.isAlive) {
        links = _.filter(links, l => !_.includes(['liveUpdate', 'kill'], l.name))
      }

      return {
        title: this.runMetadata.uid,
        legacyPath: '',
        links: links,
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
  margin: 0 8px;
  padding: 0;
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

</style>
