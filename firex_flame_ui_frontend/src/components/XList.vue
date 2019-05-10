<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;">
    <x-header :title="title"
              :links="headerLinks"
              :legacyPath="headerParams.legacyPath"></x-header>

    <div style="display:flex; flex-direction: column; margin-left: 5px">
      <div>Sort by:
        <div style="display:flex; margin-left: 20px">
          <div v-for="option in sortOptions" :key="option.value" style="margin: 0 10px;">
            <input type="radio" :id="option.value" name="list-order" :value="option.value"
                   v-model="selectedSortOption">
            <label :for="option.value">{{option.text}}</label>
          </div>

          <div style="margin-left: 25px;">
            <input type="radio" id="ascending" name="list-order-direction" value="ascending"
                   v-model="selectedSortOptionDirection">
            <label for="ascending">Ascending</label>
          </div>
          <div>
            <input type="radio" id="descending" name="list-order-direction" value="descending"
                   v-model="selectedSortOptionDirection">
            <label for="descending">Descending</label>
          </div>
        </div>
      </div>

      <div>Filter by task type:
        <div style="display:flex; margin-left: 20px">
          <div style="display: inline-block; margin: 0 15px;">
            <input type="checkbox" id="all" v-on:change="toggleShowAll"
                   :checked="allFiltersSelected">
            <label for="all">all</label>
          </div>
          <div v-for="option in runStates" :key="option"
               style="display: inline-block; margin: 0 15px;">
            <input type="checkbox" :id="option" name="list-filter" :value="option"
                   v-model="selectedRunStates">
            <label :for="option">{{option}}</label>
          </div>
        </div>
      </div>

      <div>
        <hr>
      </div>

      <!-- TODO: Add no nodes matching filters result. -->
      <div class="list-container">
        <x-node v-for="uuid in displayTaskUuids" :key="uuid"
                :taskUuid="uuid"
                style="margin: 10px"
                :isLeaf="true"></x-node>
      </div>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';

import XNode from './nodes/XTaskNode.vue';
import XHeader from './XHeader.vue';
import { routeTo2 } from '../utils';

export default {
  name: 'XList',
  components: { XNode, XHeader },
  data() {
    const runStates = ['task-received', 'task-blocked', 'task-started', 'task-succeeded', 'task-shutdown',
      'task-failed', 'task-revoked', 'task-incomplete'];
    return {
      // TODO: consider having these as URL parameters, so we can link to failures only.
      selectedSortOption: 'time-received',
      selectedSortOptionDirection: 'ascending',
      sortOptions: [
        { value: 'time-received', text: 'Time Received' },
        { value: 'alphabetical', text: 'Alphabetical' },
        { value: 'runtime', text: 'Runtime' },
      ],
      selectedRunStates: _.clone(runStates),
      runStates,
      headerParams: {
        legacyPath: '/list',
      },
    };
  },
  computed: {
    title() {
      return this.$store.state.firexRunMetadata.uid;
    },
    headerLinks() {
      return [
        { name: 'graph', to: routeTo2(this.$route.query, 'XGraph'), icon: 'sitemap' },
        {
          name: 'logs',
          href: `http://firex.cisco.com${this.$store.state.firexRunMetadata.logs_dir}`,
          text: 'View Logs',
        },
        { name: 'help', to: routeTo2(this.$route.query, 'XHelp'), text: 'Help' },
      ];
    },
    displayTaskUuids() {
      let filteredNodes = _.values(this.$store.state.tasks.tasksByUuid);
      if (this.selectedRunStates !== 'all') {
        filteredNodes = _.filter(filteredNodes,
          n => _.includes(this.selectedRunStates, n.state));
      }
      const optionsToSortFields = {
        'time-received': 'task_num',
        alphabetical: 'name',
        runtime: 'actual_runtime', // TODO: what is this field isn't defined yet or in progress?
      };
      const sortField = optionsToSortFields[this.selectedSortOption];
      let sortedNodes = _.sortBy(filteredNodes, sortField);
      if (this.selectedSortOptionDirection === 'descending') {
        sortedNodes = _.reverse(sortedNodes);
      }
      return _.map(sortedNodes, 'uuid');
    },
    allFiltersSelected() {
      return _.difference(this.runStates, this.selectedRunStates).length === 0;
    },
  },
  methods: {
    toggleShowAll() {
      if (_.difference(this.runStates, this.selectedRunStates).length === 0) {
        this.selectedRunStates = [];
      } else {
        this.selectedRunStates = this.runStates;
      }
    },
  },
};
</script>

<style scoped>
  .list-container {
    padding: 10px;
    flex: 1;
    overflow: auto;
    align-items: center;
    display: flex;
    flex-direction: column;
  }
</style>
