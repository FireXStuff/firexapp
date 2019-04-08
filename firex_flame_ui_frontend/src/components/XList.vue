<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;">
    <x-header :title="headerParams.title"
              :links="headerParams.links"
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
          <div v-for="option in filterOptions" :key="option"
               style="display: inline-block; margin: 0 15px;">
            <input type="checkbox" :id="option" name="list-filter" :value="option"
                   v-model="selectedFilterOptions">
            <label :for="option">{{option}}</label>
          </div>
        </div>
      </div>

      <div>
        <hr>
      </div>

      <!-- TODO: Add no nodes matching filters result. -->
      <div class="list-container">
        <x-node v-for="n in displayNodesByUuid" :key="n.uuid"
                :node="n" style="margin: 10px" :allowCollapse="false"></x-node>
      </div>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';

import XNode from './XTaskNode.vue';
import XHeader from './XHeader.vue';
import { routeTo } from '../utils';

export default {
  name: 'XList',
  components: { XNode, XHeader },
  props: {
    nodesByUuid: { required: true, type: Object },
    runMetadata: { default: () => ({ uid: '', logs_dir: '' }), type: Object },
  },
  data() {
    const filterOptions = ['task-received', 'task-blocked', 'task-started', 'task-succeeded', 'task-shutdown',
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
      selectedFilterOptions: _.clone(filterOptions),
      filterOptions,
      headerParams: {
        title: this.runMetadata.uid,
        legacyPath: '/list',
        links: [
          { name: 'graph', to: routeTo(this, 'XGraph'), icon: 'sitemap' },
          { name: 'logs', href: '#', text: 'View Logs' },
          { name: 'help', to: routeTo(this, 'XHelp'), text: 'Help' },
        ],
      },
    };
  },
  computed: {
    displayNodesByUuid() {
      let resultNodes = this.nodes;
      if (this.selectedFilterOptions !== 'all') {
        resultNodes = _.filter(resultNodes, n => _.includes(this.selectedFilterOptions, n.state));
      }
      const optionsToSortFields = {
        'time-received': 'task_num',
        alphabetical: 'name',
        runtime: 'actual_runtime', // TODO: what is this field isn't defined yet or in progress?
      };
      const sortField = optionsToSortFields[this.selectedSortOption];
      let sortedNodes = _.sortBy(resultNodes, sortField);
      if (this.selectedSortOptionDirection === 'descending') {
        sortedNodes = _.reverse(sortedNodes);
      }
      return sortedNodes;
    },
    nodes() {
      return _.values(this.nodesByUuid);
    },
    allFiltersSelected() {
      return _.difference(this.filterOptions, this.selectedFilterOptions).length === 0;
    },
  },
  methods: {
    toggleShowAll() {
      if (_.difference(this.filterOptions, this.selectedFilterOptions).length === 0) {
        this.selectedFilterOptions = [];
      } else {
        this.selectedFilterOptions = this.filterOptions;
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
