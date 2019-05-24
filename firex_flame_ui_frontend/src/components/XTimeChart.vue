<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column; overflow: auto;"
       @keydown.ctrl.f.prevent="$store.commit('tasks/toggleSearchOpen')"
       @keyup.191.prevent="$store.commit('tasks/toggleSearchOpen')"
       tabindex="0"
       ref="time-table"
  >
    <x-header :title="title"
              :links="headerLinks"
              legacyPath="/list">
      <template v-slot:prebuttons>
        <x-task-node-search :ignoreCollapsed="false" class="header-icon-button">
        </x-task-node-search>
      </template>
    </x-header>

    <div style="display:flex; flex-direction: column; margin-left: 5px" class="time-table">
      <table>
        <thead>
          <tr style="text-align: center;">
            <th @click="sortOn('task_num')" class="sortable-header">
              #<template v-if="sort === 'task_num'">
                <font-awesome-icon v-if="isAsc" icon="caret-down"></font-awesome-icon>
                <font-awesome-icon v-else icon="caret-up"></font-awesome-icon>
              </template>
            </th>
            <th @click="sortOn('name')" class="sortable-header">
              Task
              <template v-if="sort === 'name'">
                <font-awesome-icon v-if="isAsc" icon="caret-down"></font-awesome-icon>
                <font-awesome-icon v-else icon="caret-up"></font-awesome-icon>
              </template>
            </th>
            <th @click="sortOn('hostname')"  class="sortable-header">
              Host
              <template v-if="sort === 'hostname'">
                <font-awesome-icon v-if="isAsc" icon="caret-down"></font-awesome-icon>
                <font-awesome-icon v-else icon="caret-up"></font-awesome-icon>
              </template>
            </th>
            <th style="display: flex; flex-direction: row;">
              <div style="align-self: start;">
                {{displayTasksStartTime ? formatTime(displayTasksStartTime): ''}}
              </div>
              <div style="align-self: center; flex: 1; text-align: center;" class="sortable-header"
                @click="sortOn('runtime')">
                Runtime: {{durationString(displayTasksDuration)}}
                <template v-if="sort === 'runtime'">
                  <font-awesome-icon v-if="isAsc" icon="caret-down"></font-awesome-icon>
                  <font-awesome-icon v-else icon="caret-up"></font-awesome-icon>
                </template>
              </div>
              <div style="align-self: end;">
                {{displayTasksEndTime ? formatShortTime(displayTasksEndTime, shortTime) : ''}}
              </div>
            </th>
            <!--<th>Links</th>-->
          </tr>
        </thead>
        <tbody>
          <tr v-for="task in displayTasks" :key="task.uuid">
            <td class="min">{{task.task_num}}</td>
            <td class="min">{{task.name}}</td>
            <td class="min">{{task.hostname}}</td>
            <td>
              <popper trigger="hover" :options="{ placement: 'top'}" >
                <div style="" class="popper popover-container">
                  <div class="popover-title"><b>{{task.name}}</b></div>
                  <div style="padding: 3px;">
                    Started: {{formatShortTime(task.first_started, shortTimeSec)}}
                  </div>
                  <div style="padding: 3px;">
                    Runtime: {{durationString(getRuntime(task))}}
                    ({{(100 * getRuntime(task) / displayTasksDuration).toFixed(2)}}%)
                  </div>
                </div>
                <div slot="reference"
                    :style="chartRectStyleByUuid[task.uuid]" style="height: 1em;">
                  <router-link :to="routeToAttribute(task.uuid)" class="task"
                               style="display: block; height: 100%;">
                  </router-link>
                </div>
              </popper>
            </td>
            <!--<td class="min">-->
              <!--<a style="padding: 3px;" title="logs">-->
                <!--<font-awesome-icon icon="file-alt"></font-awesome-icon>-->
              <!--</a>-->
              <!--<a style="padding: 3px;" title="subtree">-->
                <!--<font-awesome-icon icon="sitemap"></font-awesome-icon>-->
              <!--</a>-->
            <!--</td>-->
          </tr>
        </tbody>

      </table>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';
import { DateTime } from 'luxon';
import { mapState } from 'vuex';
import Popper from 'vue-popperjs';
import 'vue-popperjs/dist/vue-popper.css';

import XHeader from './XHeader.vue';
import XTaskNodeSearch from './XTaskNodeSearch.vue';
import {
  routeTo2, durationString, getNodeBackground, isTaskStateIncomplete,
} from '../utils';

export default {
  name: 'XList',
  components: { XHeader, XTaskNodeSearch, Popper },
  props: {
    sort: { required: true },
    sortDirection: { required: true },
  },
  data() {
    const shortTime = {
      month: 'long',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
    };
    return {
      shortTime,
      shortTimeSec: Object.assign({ second: '2-digit' }, shortTime),
    };
  },
  computed: {
    ...mapState({
      allTasksByUuid: state => state.tasks.allTasksByUuid,
      title: state => state.firexRunMetadata.uid,
      search: state => state.tasks.search,
    }),
    tasksWithRuntimeByUuid() {
      return _.mapValues(this.allTasksByUuid,
        t => Object.assign({ runtime: this.getRuntime(t) }, t));
    },
    displayTasks() {
      let tasks;
      if (this.search.isOpen && !_.isEmpty(this.search.term)) {
        tasks = _.pick(this.tasksWithRuntimeByUuid, this.search.resultUuids);
      } else {
        tasks = this.tasksWithRuntimeByUuid;
      }
      let sorted = _.sortBy(tasks, this.sort);
      if (this.sortDirection === 'desc') {
        sorted = _.reverse(sorted);
      }
      return _.keyBy(sorted, 'uuid');
    },
    headerLinks() {
      return [
        { name: 'graph', to: routeTo2(this.$route.query, 'XGraph'), icon: 'sitemap' },
        { name: 'list', to: routeTo2(this.$route.query, 'XList'), icon: 'list-ul' },
        {
          name: 'logs',
          href: this.$store.getters['firexRunMetadata/logsUrl'],
          text: 'View Logs',
        },
        { name: 'help', to: routeTo2(this.$route.query, 'XHelp'), text: 'Help' },
      ];
    },
    displayTasksStartTime() {
      return _.min(_.map(this.displayTasks, 'first_started'));
    },
    displayTasksEndTime() {
      return _.max(_.map(this.displayTasks, t => t.first_started + this.getRuntime(t)));
    },
    displayTasksDuration() {
      return this.displayTasksEndTime - this.displayTasksStartTime;
    },
    chartRectByUuid() {
      return _.mapValues(this.displayTasks,
        (t) => {
          const startOffsetPercentage = (t.first_started - this.displayTasksStartTime)
            / this.displayTasksDuration;
          const durationPercentage = this.getRuntime(t) / this.displayTasksDuration;
          return {
            offset: 100 * startOffsetPercentage,
            // show something even for very small durations.
            duration: _.max([100 * durationPercentage, 0.5]),
          };
        });
    },
    chartRectStyleByUuid() {
      return _.mapValues(this.displayTasks,
        t => ({
          'margin-left': `${this.chartRectByUuid[t.uuid].offset}%`,
          width: `${this.chartRectByUuid[t.uuid].duration}%`,
          background: getNodeBackground(t.exception, t.state),
        }));
    },
    isAsc() {
      return this.sortDirection === 'asc';
    },
  },
  methods: {
    getRuntime(task) {
      if (isTaskStateIncomplete(task.state)) {
        return (Date.now() / 1000) - task.first_started;
      }
      if (_.has(task, 'actual_runtime')) {
        return task.actual_runtime;
      }
      // hack since backend doesn't always fill in actual_runtime, even when runstate is terminal.
      // assume not filled in values ended when the entire run ended.
      const runEndTime = _.max(_.map(this.allTasksByUuid,
        t => t.first_started + _.get(t, 'actual_runtime', 0)));
      return runEndTime - task.first_started;
    },
    formatTime(unixTime) {
      return DateTime.fromSeconds(unixTime).toLocaleString(DateTime.DATETIME_FULL);
    },
    formatShortTime(unixTime, format) {
      return DateTime.fromSeconds(unixTime).toLocaleString(format);
    },
    routeToAttribute(uuid) {
      return routeTo2(this.$route.query, 'XNodeAttributes', { uuid });
    },
    durationString,
    updateRouteQuery(newParams) {
      const newQuery = Object.assign({}, this.$route.query, newParams);
      this.$router.replace({ query: newQuery });
    },

    sortOn(columnName) {
      if (columnName === this.sort) {
        this.updateRouteQuery({ sortDirection: this.sortDirection === 'asc' ? 'desc' : 'asc' });
      } else {
        this.updateRouteQuery({ sort: columnName, sortDirection: 'asc' });
      }
    },
  },
  beforeRouteLeave(to, from, next) {
    // TODO: this should be cleared on FireX UID change or keyed on by FireX UID.
    from.meta.scrollY = this.$refs['time-table'].scrollTop;
    this.$store.commit('tasks/closeSearch');
    next();
  },
  beforeRouteEnter(to, from, next) {
    next((vm) => {
      // Restore scroll position when navigating to.
      if (_.has(to.meta, 'scrollY')) {
        vm.$refs['time-table'].scrollTop = to.meta.scrollY;
        delete to.meta.scrollY;
      }
    });
  },
};
</script>

<style scoped>
  .time-table {
    font-family: 'Source Sans Pro', sans-serif;
  }

  table {border: none;}

  td {
    width: auto;
  }

  td.min {
    width: 1%;
    white-space: nowrap;
  }

  tr:hover {
    background-color: lightblue;
  }

  .sortable-header {
    white-space: nowrap;
  }

  .sortable-header:hover {
    cursor: pointer;
    color: #2980ff;
  }

  .task:hover {
    background: #000 !important;
  }

  .popover-container {
    background: white;
    padding: 0;
    border: 1px solid black;
    font-family: 'Source Sans Pro', sans-serif;
  }

  .popover-title {
    padding: 3px;
    text-align: center;
    border-bottom: 1px solid black;
    background-color: #EEE;
  }
</style>
