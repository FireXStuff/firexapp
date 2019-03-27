<template>
  <!-- Need prevent to avoid default browser behaviour.  -->
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;"
       @keydown.ctrl.f.prevent="focusOnFind"
       @keydown.ctrl.d.prevent="focusOnFind"
       @keydown.ctrl.u.prevent="focusOnFind"
       tabindex="0">
    <x-header :title="headerParams.title"
              :links="headerParams.links"
              :legacyPath="headerParams.legacyPath"
              :enableSearch="true"
    ></x-header>
    <!-- TODO: not sure where the best level to gate on UID is, but need UID to key on localStorage within x-graph-->
    <x-graph
      v-if="runMetadata.uid"
      :nodesByUuid="rootDescendantsByUuid"
      :firexUid="runMetadata.uid"></x-graph>
  </div>
</template>

<script>
import XGraph from './XGraph'
import XHeader from './XHeader'
import {eventHub, routeTo, hasIncompleteTasks, getDescendantUuids} from '../utils'
import _ from 'lodash'

export default {
  name: 'XHeaderedGraph',
  components: {XGraph, XHeader},
  props: {
    nodesByUuid: {required: true, type: Object},
    runMetadata: {required: true, type: Object},
    isConnected: {required: true, type: Boolean},
    rootUuid: {default: null},
  },
  created () {
    let liveUpdate = _.find(this.headerParams.links, {'name': 'liveUpdate'})
    if (liveUpdate) {
      liveUpdate.on(true)
    }
  },
  computed: {
    isUidValid () {
      if (!this.runMetadata.uid) {
        return false
      }
      return this.runMetadata.uid.startsWith('FireX-')
    },
    isAlive () {
      return this.isConnected && this.hasIncompleteTasks
    },
    hasIncompleteTasks () {
      return hasIncompleteTasks(this.rootDescendantsByUuid)
    },
    rootDescendantsByUuid () {
      if (this.rootUuid === null) {
        return this.nodesByUuid
      }
      let rootDescendantUuids = getDescendantUuids(this.rootUuid, this.nodesByUuid)
      return _.pick(this.nodesByUuid, [this.rootUuid].concat(rootDescendantUuids))
    },
    headerParams () {
      let links = [
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
        {name: 'kill', on: () => eventHub.$emit('revoke-root'), _class: 'kill-button', icon: 'times'},
        {name: 'logs', href: this.runMetadata.logs_dir, text: 'View logs'},
        {name: 'help', to: routeTo(this, 'XHelp'), text: 'Help'},
      ]
      // Remove live update and kill options if the run isn't alive.
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
  watch: {
    rootUuid () {
      this.$nextTick(() => { eventHub.$emit('center') })
    },
  },
}
</script>

<style scoped>
</style>
