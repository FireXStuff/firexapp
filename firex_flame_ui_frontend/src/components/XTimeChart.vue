<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;"
       @keydown.ctrl.f.prevent="$store.commit('tasks/toggleSearchOpen')"
       @keyup.191.prevent="$store.commit('tasks/toggleSearchOpen')"
       tabindex="0"
       ref="time-table"
  >
    <x-header :title="title"
              :links="headerLinks">
      <template v-slot:prebuttons>
        <x-task-node-search :findUncollapsedAncestor="false">
        </x-task-node-search>
      </template>
    </x-header>

    <div class="time-table">
      <table>
        <thead>
          <tr style="text-align: center;">
            <th @click="sortOn('task_num')" class="sortable-header min">
              #<template v-if="sort === 'task_num'">
                <font-awesome-icon v-if="isAsc" icon="caret-down"></font-awesome-icon>
                <font-awesome-icon v-else icon="caret-up"></font-awesome-icon>
              </template>
            </th>
            <th @click="sortOn('name')" class="sortable-header min">
              Task
              <template v-if="sort === 'name'">
                <font-awesome-icon v-if="isAsc" icon="caret-down"></font-awesome-icon>
                <font-awesome-icon v-else icon="caret-up"></font-awesome-icon>
              </template>
            </th>
            <th @click="sortOn('hostname')"  class="sortable-header min">
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
            <th style="width: 5%;">Links</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="task in displayTasks" :key="task.uuid" style="height: 2em;">
            <td class="min">{{task.task_num}}</td>
            <td class="min">{{task.name}}</td>
            <td class="min">{{task.hostname}}</td>
            <td>
              <div style="display: flex; flex-direction: column; height: 1.5em;">
                <div style="height: 0.75em; display: flex; flex-direction: row;">
                <!-- Next div is offset.-->
                <div :style="{width: taskRectOffsetStyleByUuid[task.uuid]['padding-left']}"></div>

                <!-- Each runstate is represented by a div -->
                <div v-for="(stateData, uuid) in perStateRectByUuid[task.uuid]"
                     :key="uuid" :style="stateData" class="state-rect">
                  <popper trigger="hover"
                          :options="{ placement: 'top' }"
                          style="display: block;">
                    <div class="popper popover-container">
                      <div class="popover-title"><b>{{task.name}}</b></div>
                      <div style="padding: 3px;">
                        {{getRunstateDisplayName(stateData.state)}} at
                        {{formatShortTime(stateData.timestamp, shortTimeSec)}}
                      </div>
                      <div style="padding: 3px;">
                        {{getRunstateDisplayName(stateData.state)}}
                        for {{durationString(stateData.stateDuraion)}}
                        ({{(100 * stateData.stateDuraion
                            / displayTasksDuration).toFixed(2)}}% of run)
                      </div>
                    </div>
                    <span slot="reference" style="display: block; height: 0.75em">
                    </span>
                  </popper>
                </div>
                </div>
                <div v-if="!isTaskStateIncomplete(task.state)"
                     style="height: 0.75em; display: flex; flex-direction: row;">
                  <!-- next div is offset -->
                  <div :style="{width: taskRectOffsetStyleByUuid[task.uuid]['padding-left']}"></div>
                  <div :style="fullTaskRectStyleByUuid[task.uuid]" style="height: 0.75em;">
                    <popper trigger="hover"
                            :options="{ placement: 'bottom' }">
                      <div class="popper popover-container">
                        <div class="popover-title"><b>{{task.name}}</b></div>
                        <div style="padding: 3px;">
                          Started: {{formatShortTime(task.first_started, shortTimeSec)}}
                        </div>
                        <div style="padding: 3px;">
                          Runtime: {{durationString(getTaskRuntime(task))}}
                          ({{(100 * getTaskRuntime(task) / displayTasksDuration).toFixed(2)}}%)
                        </div>
                      </div>
                      <span slot="reference"
                            style="display: block; height: 0.75em">
                          <router-link :to="getTaskRoute(task.uuid)" class="task"
                                       style="display: block; height: 100%; display: block;">
                          </router-link>
                      </span>
                    </popper>
                  </div>
                </div>
              </div>
            </td>
            <td class="min">
              <div v-if="task.uuid in extraTaskFieldsByUuid" class="icon-links">
                <a :href="extraTaskFieldsByUuid[task.uuid].logs_url" title="logs">
                  <font-awesome-icon icon="file-alt"></font-awesome-icon>
                </a>
                <router-link :to="getCustomRootRoute(task.uuid)" title="subtree">
                  <font-awesome-icon icon="sitemap"></font-awesome-icon>
                </router-link>
                <a :href="extraTaskFieldsByUuid[task.uuid].code_url" title="code">
                  <font-awesome-icon icon="file-code"></font-awesome-icon>
                </a>
              </div>
              <!-- TODO: consider adding collapse-hidden icons, since lazy-loading causes
                  table expansion instead of column width re-calc.
              -->
            </td>
          </tr>
        </tbody>

      </table>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';
import { DateTime } from 'luxon';
import { mapState, mapGetters } from 'vuex';
import Popper from 'vue-popperjs';
import 'vue-popperjs/dist/vue-popper.css';

import XHeader from './XHeader.vue';
import XTaskNodeSearch from './XTaskNodeSearch.vue';
import * as api from '../api';
import {
  durationString, getNodeBackground, isTaskStateIncomplete, getRunstateDisplayName,
} from '../utils';

export default {
  name: 'XTimeChart',
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
      extraTaskFieldsByUuid: {},
    };
  },
  computed: {
    ...mapState({
      title: state => state.firexRunMetadata.uid,
      search: state => state.tasks.search,
    }),
    ...mapGetters({
      tasksByUuid: 'tasks/tasksByUuid',
      runEndTime: 'tasks/runEndTime',
      graphViewHeaderEntry: 'header/graphViewHeaderEntry',
      listViewHeaderEntry: 'header/listViewHeaderEntry',
      runLogsViewHeaderEntry: 'header/runLogsViewHeaderEntry',
      helpViewHeaderEntry: 'header/helpViewHeaderEntry',
      getTaskRoute: 'header/getTaskRoute',
      getCustomRootRoute: 'header/getCustomRootRoute',
    }),
    tasksWithRuntimeByUuid() {
      return _.mapValues(this.tasksByUuid,
        t => Object.assign({ runtime: this.getTaskRuntime(t) }, t));
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
        this.graphViewHeaderEntry,
        this.listViewHeaderEntry,
        this.runLogsViewHeaderEntry,
        this.helpViewHeaderEntry,
      ];
    },
    displayTasksStartTime() {
      return _.min(_.map(this.displayTasks, 'first_started'));
    },
    displayTasksEndTime() {
      return _.max(_.map(this.displayTasks, t => t.first_started + this.getTaskRuntime(t)));
    },
    displayTasksDuration() {
      return this.displayTasksEndTime - this.displayTasksStartTime;
    },
    fullTaskRectByUuid() {
      return _.mapValues(this.displayTasks,
        (t) => {
          const startOffsetPercentage = (t.first_started - this.displayTasksStartTime)
            / this.displayTasksDuration;
          const durationPercentage = this.getTaskRuntime(t) / this.displayTasksDuration;
          return {
            offset: 100 * startOffsetPercentage,
            // show something even for very small durations.
            duration: _.max([100 * durationPercentage, 0.5]),
          };
        });
    },
    perStateRectsByUuid() {
      return _.mapValues(this.displayTasks, (t, u) => _.map(
        _.get(this.extraTaskFieldsByUuid, [u, 'states'], []),
        (stateTransition, i, transitions) => {
          const startOffset = stateTransition.timestamp - this.displayTasksStartTime;
          const startStateOffsetPercentage = startOffset / this.displayTasksDuration;
          const endStateTimestamp = _.get(transitions, i + 1,
            { timestamp: startStateOffsetPercentage });
          const transitionDuration = endStateTimestamp.timestamp - stateTransition.timestamp;
          const durationPercentage = transitionDuration / this.displayTasksDuration;
          return {
            offset: 100 * startStateOffsetPercentage,
            // TODO: show something even for very small durations.
            durationPercentage: 100 * durationPercentage,
            duration: transitionDuration,
            state: stateTransition.state,
            timestamp: stateTransition.timestamp,
          };
        },
      ));
    },
    taskRectOffsetStyleByUuid() {
      return _.mapValues(this.displayTasks,
        t => ({
          'padding-left': `${this.fullTaskRectByUuid[t.uuid].offset}%`,
        }));
    },
    fullTaskRectStyleByUuid() {
      return _.mapValues(this.displayTasks,
        t => ({
          // 'margin-left': `${this.fullTaskRectByUuid[t.uuid].offset}%`,
          width: `${this.fullTaskRectByUuid[t.uuid].duration}%`,
          background: getNodeBackground(t.exception, t.state),
        }));
    },
    perStateRectByUuid() {
      return _.mapValues(this.perStateRectsByUuid, (statesRects, u) => _.map(
        statesRects, stateRect => ({
          width: `${stateRect.durationPercentage}%`,
          background: getNodeBackground(this.displayTasks[u].exception, stateRect.state),
          state: stateRect.state,
          timestamp: stateRect.timestamp,
          stateDuraion: stateRect.duration,
        }),
      ));
    },
    isAsc() {
      return this.sortDirection === 'asc';
    },
  },
  created() {
    api.fetchTaskFields(['states', 'logs_url', 'code_url']).then((extraTaskFieldsByUuid) => {
      this.extraTaskFieldsByUuid = extraTaskFieldsByUuid;
    });
  },
  methods: {
    getTaskRuntime(task) {
      if (isTaskStateIncomplete(task.state)) {
        return (Date.now() / 1000) - task.first_started;
      }
      if (_.has(task, 'actual_runtime')) {
        return task.actual_runtime;
      }
      // Hack since backend doesn't always fill in actual_runtime, even when runstate is
      // terminal. Assume not filled in actual_runtime tasks ended when the entire run ended.
      return this.runEndTime - task.first_started;
    },
    formatTime(unixTime) {
      return DateTime.fromSeconds(unixTime).toLocaleString(DateTime.DATETIME_FULL);
    },
    formatShortTime(unixTime, format) {
      if (_.isNil(unixTime)) {
        // TODO: Only necessary because kludge-states don't include timestamp. After fixed on
        //  server, this nil check can be performed.
        return 'Unknown';
      }
      return DateTime.fromSeconds(unixTime).toLocaleString(format);
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
    getRunstateDisplayName,
    isTaskStateIncomplete,
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
    flex: 1;
    overflow: auto;
    align-items: center;
    display: flex;
    flex-direction: column;
  }

  table {
    border: none;
    width: 100%;
    /*padding-left: 0.5em;*/
  }

  td {
    width: auto;
  }

  td.min {
    width: 1%;
    white-space: nowrap;
  }

  th.min {
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
    text-align: left;
    border: 1px solid black;
    font-family: 'Source Sans Pro', sans-serif;
  }

  .popover-title {
    padding: 3px;
    text-align: center;
    border-bottom: 1px solid black;
    background-color: #EEE;
  }

  .icon-links a {
    padding: 3px;
    color: black;
  }

  .icon-links a:hover {
    color: #2980ff;
  }

  .state-rect:hover {
    box-shadow: 0px 3px 3px rgb(58, 58, 58);
    z-index: 10;
  }

</style>
