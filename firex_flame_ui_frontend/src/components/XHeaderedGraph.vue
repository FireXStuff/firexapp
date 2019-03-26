<template>
  <!-- ctrl.70 is ctrl-f. Need prevent to avoid default browser behaviour.  -->
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;"
       @keydown.ctrl.70.prevent="focusOnFind"  tabindex="0">
    <x-header :title="headerParams.title"
              :links="headerParams.links"
              :legacyPath="headerParams.legacyPath"
              :enableSearch="true"
    ></x-header>
    <x-graph
      :nodesByUuid="nodesByUuid"
      :firexUid="runMetadata.uid"></x-graph>
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
  methods: {
    focusOnFind (event) {
      eventHub.$emit('find-focus')
    },
  },
}
</script>

<style scoped>
</style>
