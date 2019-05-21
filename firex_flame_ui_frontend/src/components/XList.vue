<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column; overflow: auto;"
       @keydown.ctrl.f.prevent="focusOnFind"
       @keyup.191.prevent="focusOnFind"
       tabindex="0"
  >
    <x-header :title="title"
              :links="headerLinks"
              :enableSearch="true"
              legacyPath="/list"></x-header>

    <div style="display:flex; flex-direction: column; margin-left: 5px">
      <div>Sort by:
        <div style="display:flex; margin-left: 20px">
          <div v-for="option in sortOptions" :key="option.value" style="margin: 0 3px;">
            <input type="radio" :id="option.value" name="list-order" :value="option.value"
                   v-model="selectedOptions.sortOption">
            <label :for="option.value">{{option.text}}</label>
          </div>

          <div style="margin-left: 25px;">
            <input type="radio" id="ascending" name="list-order-direction" value="ascending"
                   v-model="selectedOptions.sortDirection">
            <label for="ascending">Ascending</label>
          </div>
          <div style="margin-left: 3px;">
            <input type="radio" id="descending" name="list-order-direction" value="descending"
                   v-model="selectedOptions.sortDirection">
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
                   v-model="selectedOptions.runStates">
            <label :for="stateSelector">{{stateSelector}}</label>
          </div>
        </div>
      </div>

      <div>
        <hr>
      </div>

      <!-- TODO: Add no nodes matching filters result. -->
      <div class="list-container" ref="task-list">
        <x-node v-for="uuid in displayTaskUuids" :key="uuid"
                :taskUuid="uuid"
                style="margin: 10px"
                :isLeaf="true"
                :ref="'task-' + uuid"
        ></x-node>
      </div>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';
import { mapState } from 'vuex';

import XNode from './nodes/XTaskNode.vue';
import XHeader from './XHeader.vue';
import { routeTo2, containsAll } from '../utils';

export default {
  name: 'XList',
  components: { XNode, XHeader },
  props: {
    // TODO: validate/prune for allowed values.
    sort: { required: true, type: String },
    sortDirection: {
      required: true,
      type: String,
      validator(value) {
        return _.includes(['ascending', 'descending'], value);
      },
    },
    // TODO: validate/prune for allowed values.
    runstates: { required: true, type: Array },
  },
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
      selectedOptions: {
        runStates: _.clone(this.runstates),
        sortOption: this.sort,
        sortDirection: this.sortDirection,
      },
      sortOptions: [
        { value: 'time-received', text: 'Time Received' },
        { value: 'alphabetical', text: 'Alphabetical' },
        { value: 'runtime', text: 'Runtime' },
      ],
      runStateSelectorsToStates,
    };
  },
  computed: {
    ...mapState({
      allTasksByUuid: state => state.tasks.allTasksByUuid,
      search: state => state.tasks.search,
    }),
    // Want to go over results based on selected sort order, not based on task_num.
    orderedSearchResultUuids() {
      return _.intersection(this.sortedTasksUuids, this.search.resultUuids);
    },
    focusedTaskUuid() {
      if (!this.search.isOpen || _.isEmpty(this.search.resultUuids)) {
        return null;
      }
      return this.orderedSearchResultUuids[this.search.selectedIndex];
    },
    hasSearchTerm() {
      return !_.isEmpty(this.search.term);
    },
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
    sortedTasksUuids() {
      const optionsToSortFields = {
        'time-received': 'task_num',
        alphabetical: 'name',
        runtime: 'actual_runtime', // TODO: what is this field isn't defined yet or in progress?
      };
      const sortField = optionsToSortFields[this.selectedOptions.sortOption];
      let sortedNodes = _.sortBy(this.allTasksByUuid, sortField);
      if (this.selectedOptions.sortDirection === 'descending') {
        sortedNodes = _.reverse(sortedNodes);
      }
      return _.map(sortedNodes, 'uuid');
    },
    filteredTaskUuids() {
      const runstateFilteredTasks = _.pickBy(
        this.allTasksByUuid,
        n => _.includes(this.selectedRunStates, n.state),
      );
      if (this.search.isOpen && this.hasSearchTerm) {
        return _.map(_.pick(runstateFilteredTasks, this.orderedSearchResultUuids), 'uuid');
      }
      return _.map(runstateFilteredTasks, 'uuid');
    },
    displayTaskUuids() {
      return _.intersection(this.sortedTasksUuids, this.filteredTaskUuids);
    },
    selectedRunStates() {
      return _.flatten(_.values(_.pick(this.runStateSelectorsToStates,
        this.selectedOptions.runStates)));
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
      this.selectedOptions.runStates = this.allRunStatesSelected
        ? [] : ['Completed', 'In-Progress'];
    },
    updateRouteQuery(key, value) {
      const newQuery = Object.assign({}, this.$route.query, { [[key]]: value });
      this.$router.replace({ query: newQuery });
    },
    focusOnFind() {
      this.$store.commit('tasks/toggleSearchOpen');
    },
  },
  beforeRouteEnter(to, from, next) {
    next((vm) => {
      // Restore scroll position when navigating to.
      if (_.has(to.meta, 'scrollY')) {
        vm.$refs['task-list'].scrollTop = to.meta.scrollY;
        delete to.meta.scrollY;
      }
    });
  },
  // Save scroll position when navigating away.
  beforeRouteLeave(to, from, next) {
    // TODO: this should be cleared on FireX UID change or keyed on by FireX UID.
    from.meta.scrollY = this.$refs['task-list'].scrollTop;
    this.$store.commit('tasks/closeSearch');
    next();
  },
  watch: {
    focusedTaskUuid(newValue) {
      // Note that the current search might hide the newly focused node, so scroll on
      // next tick (after the render that will cause the hidden node to show up).
      this.$nextTick(() => {
        this.$refs[`task-${newValue}`][0].$el.scrollIntoView();
      });
    },
    runstates(newRunstates) {
      if (!_.isEqual(newRunstates, this.selectedOptions.runStates)) {
        this.selectedOptions.runStates = newRunstates;
      }
    },
    sort(newSort) {
      if (!_.isEqual(newSort, this.selectedOptions.sortOption)) {
        this.selectedOptions.sortOption = newSort;
      }
    },
    sortDirection(newSortDirection) {
      if (!_.isEqual(newSortDirection, this.selectedOptions.sortDirection)) {
        this.selectedOptions.sortDirection = newSortDirection;
      }
    },
    selectedOptions: {
      handler(newValue) {
        const changedOptions = [];
        if (!_.isEqual(newValue.runStates, this.runStates)) {
          changedOptions.push({ key: 'runstates', value: _.join(newValue.runStates, ',') });
        }
        if (!_.isEqual(newValue.sortOption, this.sort)) {
          changedOptions.push({ key: 'sort', value: newValue.sortOption });
        }
        if (!_.isEqual(newValue.sortDirection, this.sortDirection)) {
          changedOptions.push({ key: 'sortDirection', value: newValue.sortDirection });
        }
        _.each(changedOptions, change => this.updateRouteQuery(change.key, change.value));
      },
      deep: true,
    },
  },
};
</script>

<style scoped>
  .list-container {
    /*padding: 10px;*/
    flex: 1;
    overflow: auto;
    align-items: center;
    display: flex;
    flex-direction: column;
  }
</style>
