<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;">
    <x-header :title="title"
              :links="headerLinks"
              :legacyPath="headerParams.legacyPath"></x-header>

    <div style="display:flex; flex-direction: column; margin-left: 5px">
      <div>Sort by:
        <div style="display:flex; margin-left: 20px">
          <div v-for="option in sortOptions" :key="option.value" style="margin: 0 3px;">
            <input type="radio" :id="option.value" name="list-order" :value="option.value"
                   v-model="selectedSortOption">
            <label :for="option.value">{{option.text}}</label>
          </div>

          <div style="margin-left: 25px;">
            <input type="radio" id="ascending" name="list-order-direction" value="ascending"
                   v-model="selectedSortOptionDirection">
            <label for="ascending">Ascending</label>
          </div>
          <div style="margin-left: 3px;">
            <input type="radio" id="descending" name="list-order-direction" value="descending"
                   v-model="selectedSortOptionDirection">
            <label for="descending">Descending</label>
          </div>
        </div>
      </div>

      <div>Filter by run state:
        <div style="display:flex; margin-left: 20px">
          <div style="display: inline-block; margin: 0 15px;">
            <input type="checkbox" id="all" v-on:change="toggleShowAll"
                   :checked="allRunStatesSelected">
            <label for="all">All</label>
          </div>
          <div v-for="(_, stateSelector) in runStateSelectorsToStates" :key="stateSelector"
               style="display: inline-block; margin: 0 15px;">
            <input type="checkbox" :id="stateSelector" name="list-filter" :value="stateSelector"
                   v-model="selectedStatesSelectors">
            <label :for="stateSelector">{{stateSelector}}</label>
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
import { routeTo2, containsAll } from '../utils';

export default {
  name: 'XList',
  components: { XNode, XHeader },
  // props: {
  //   sortOption: { default: 'alphabetical' },
  //   sortOptionDirection: { default: 'descending' },
  //   stateSelectors: { default: ['Completed', 'In-Progress'] },
  // },
  data() {
    const runStateSelectorsToStates = {
      Failed: ['task-failed'],
      // Note: task-incomplete is a server-side cludge to fix states that will never become
      // terminal.
      Completed: ['task-revoked', 'task-failed', 'task-incomplete', 'task-shutdown',
        'task-succeeded'],
      'In-Progress': ['task-received', 'task-blocked', 'task-started'],
      Revoked: ['task-revoked'],
    };
    return {
      // TODO: consider having these as URL parameters, so we can link to failures only.
      selectedSortOption: 'alphabetical',
      selectedSortOptionDirection: 'descending',
      sortOptions: [
        { value: 'time-received', text: 'Time Received' },
        { value: 'alphabetical', text: 'Alphabetical' },
        { value: 'runtime', text: 'Runtime' },
      ],
      selectedStatesSelectors: ['Completed', 'In-Progress'],
      runStateSelectorsToStates,
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
          href: this.$store.getters['firexRunMetadata/logsUrl'],
          text: 'View Logs',
        },
        { name: 'help', to: routeTo2(this.$route.query, 'XHelp'), text: 'Help' },
      ];
    },
    displayTaskUuids() {
      const filteredNodes = _.filter(
        this.$store.state.tasks.allTasksByUuid,
        n => _.includes(this.selectedRunStates, n.state),
      );

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
    selectedRunStates() {
      return _.flatten(_.values(_.pick(this.runStateSelectorsToStates,
        this.selectedStatesSelectors)));
    },
    allRunStatesSelected() {
      return containsAll(this.selectedRunStates, this.allRunStates);
    },
    allRunStates() {
      return _.uniq(_.flatten(_.values(this.runStateSelectorsToStates)));
    },
  },
  methods: {
    toggleShowAll() {
      if (this.allRunStatesSelected) {
        this.selectedStatesSelectors = [];
      } else {
        this.selectedStatesSelectors = ['Completed', 'In-Progress'];
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
