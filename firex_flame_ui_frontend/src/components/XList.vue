<template>
  <div style="display:flex; flex-direction: column; margin-left: 5px">
    <div>Sort by:
      <div style="display:flex;">
        <div v-for="option in sortOptions" :key="option.value" style="margin: 0 15px;">
          <input type="radio" :id="option.value" name="list-order" :value="option.value" v-model="selectedSortOption">
          <label :for="option.value">{{option.text}}</label>
        </div>
      </div>
    </div>

    <div>Filter by task type:
      <div style="display:flex;">
        <div v-for="option in filterOptions" :key="option" style="display: inline-block; margin: 0 15px;">
          <input type="radio" :id="option" name="list-filter" :value="option" v-model="selectedFilterOption">
          <label :for="option">{{option}}</label>
        </div>
      </div>
    </div>

    <div>
      <hr>
    </div>

    <!-- TODO: Add no nodes matching filters result. -->
    <div class="list-container">
        <x-node v-for="n in displayNodesByUuid"
                :node="n" :state="n.state" :key="n.uuid" style="margin: 10px" :allowCollapse="false"></x-node>
    </div>
  </div>
</template>

<script>
import _ from 'lodash'

import XNode from './XNode'
import {eventHub} from '../utils'

export default {
  name: 'XList',
  components: {XNode},
  props: {
    nodesByUuid: {required: true, type: Object},
  },
  data () {
    return {
      // TODO: consider having these as URL parameters, so we can link to failures only.
      selectedSortOption: 'time-received',
      sortOptions: [
        {value: 'time-received', text: 'Time Received (default)'},
        {value: 'alphabetical', text: 'Alphabetical'},
        {value: 'runtime-ascending', text: 'Runtime (Asc)'},
        {value: 'runtime-descending', text: 'Runtime (Desc)'},
      ],
      selectedFilterOption: 'all',
      filterOptions: ['all', 'task-received', 'task-blocked', 'task-started', 'task-succeeded', 'task-shutdown',
        'task-failed', 'task-revoked', 'task-incomplete'],
    }
  },
  computed: {
    displayNodesByUuid () {
      this.nodes.forEach(function (n) {
        n.width = 'auto'
        n.height = 'auto'
      })
      let resultNodes = this.nodes
      if (this.selectedFilterOption !== 'all') {
        resultNodes = _.filter(resultNodes, {'state': this.selectedFilterOption})
      }
      let optionsToSortFields = {
        'time-received': 'task_num',
        'alphabetical': 'name',
        'runtime-ascending': 'actual_runtime', // TODO: what is this field isn't defined yet?
        'runtime-descending': 'actual_runtime',
      }
      let sortField = optionsToSortFields[this.selectedSortOption]
      let sortedNodes = _.sortBy(resultNodes, sortField)
      if (this.selectedSortOption === 'runtime-descending') {
        sortedNodes = _.reverse(sortedNodes)
      }
      return sortedNodes
    },
    nodes () {
      return _.values(this.nodesByUuid)
    },
  },
  created () {
    let supportedParentButtons = ['support-graph-link', 'support-help-link']
    supportedParentButtons.forEach(e => { eventHub.$emit(e) })
  },
}
</script>

<style scoped>
  .list-container {
    padding: 10px;
    flex: 1;
    overflow: auto;
  }
</style>
