<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;">
    <x-header :title="title"
              :links="headerLinks">
    </x-header>
    <div style="width: 100%; padding: 2em;">
      <div class="form-check">
        <input type="checkbox" id="deduplicate" class="form-check-input"
          v-model="deduplicateByName">
        <label class="form-check-label" for="deduplicate">
          Deduplicate by name
        </label>
      </div>
      <table style="width: 100%;" class="table">
        <thead>
          <tr>
            <th>Service Name</th>
            <th>Error Summary</th>
            <th>Execution Context</th>
            <th>Links</th>
          </tr>
        </thead>
        <tbody style="color: darkred;">
          <tr v-for="task in maybeDeDuplicatedFailedTasksByUuid" :key="task.uuid">
            <td style="text-align: center;" class="align-middle">
              <div>{{task.name}}{{ task.from_plugin ? ' (plugin)' : ''}}</div>
              <div>{{longNameToModulePath(task.long_name)}}</div>
              <div v-if="task.task_count">
                x{{task.task_count}}
              </div>
            </td>
            <td>
              <x-expandable-content
                                    button-class="btn-outline-danger" name=""
                                    :expand="shouldShowFull(displayTracebacksByUuid[task.uuid])"
                                    :unexpandedMaxHeight="collapseTracebackLinesThreshold">
              <pre style="white-space: pre-line; color: darkred;"
               v-html="createLinkedHtml(displayTracebacksByUuid[task.uuid], 'fake')"/>
              </x-expandable-content>
            </td>
            <td style="text-align: center;">
              {{errorContextByUuid[task.uuid]}}
            </td>
            <td style="text-align: center;">
              <div style="white-space: nowrap;">
                <a :href="task.logs_url">
                  <font-awesome-icon icon="file-alt"></font-awesome-icon>
                  View Log
                </a>
              </div>
              <div style="white-space: nowrap;">
                <router-link :to="taskRoute(task.uuid)">
                  <font-awesome-icon icon="sitemap"></font-awesome-icon>
                  View Task
                </router-link>
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';
import { mapState, mapGetters } from 'vuex';
import * as api from '../api';

import XHeader from './XHeader.vue';
import XExpandableContent from './XExpandableContent.vue';
import { isChainInterrupted } from '../utils';


export default {
  name: 'XErrorsTable',
  components: { XHeader, XExpandableContent },
  data() {
    return {
      failedTaskDetails: {},
      collapseTracebackLinesThreshold: 15,
      deduplicateByName: true,
    };
  },
  created() {
    this.fetchFailedTaskDetails(
      _.map(
        _.filter(this.tasksByUuid,
          t => t.state === 'task-failed' && !isChainInterrupted(t)),
        'uuid',
      ),
    );
  },
  computed: {
    ...mapState({
      title: state => state.firexRunMetadata.uid,
    }),
    ...mapGetters({
      tasksByUuid: 'graph/tasksIncludingAdditionalDescendantsByUuid',
      graphViewHeaderEntry: 'header/graphViewHeaderEntry',
      runLogsViewHeaderEntry: 'header/runLogsViewHeaderEntry',
      helpViewHeaderEntry: 'header/helpViewHeaderEntry',
      timeChartViewHeaderEntry: 'header/timeChartViewHeaderEntry',
      getTaskRoute: 'header/getTaskRoute',
      graphDataByUuid: 'graph/graphDataByUuid',
      createLinkedHtml: 'header/createLinkedHtml',
    }),
    title() {
      return this.$store.state.firexRunMetadata.uid;
    },
    headerLinks() {
      return [this.graphViewHeaderEntry, this.timeChartViewHeaderEntry,
        this.runLogsViewHeaderEntry, this.helpViewHeaderEntry];
    },
    maybeDeDuplicatedFailedTasksByUuid() {
      let displayTasks;
      if (this.deduplicateByName) {
        displayTasks = _.flatMap(_.groupBy(this.failedTaskDetails, 'name'), (sameNameTasks) => {
          // Never de-duplicate Warnings
          if (_.first(sameNameTasks).name === 'Warning') {
            return sameNameTasks;
          }
          const origTasks = _.filter(sameNameTasks, { from_plugin: false });
          const pluginTasks = _.filter(sameNameTasks, { from_plugin: true });

          let countedTasks;
          if (origTasks.length) {
            countedTasks = [_.assign({ task_count: origTasks.length }, _.first(origTasks))];
          } else {
            countedTasks = [];
          }

          // Keep all plugins & dedupe after we've fetched details and have long_name
          // Only keep first orig task.
          return _.concat(countedTasks, pluginTasks);
        });
      } else {
        displayTasks = this.failedTaskDetails;
      }
      return _.sortBy(displayTasks, 'name');
    },
    errorContextByUuid() {
      return _.mapValues(this.failedTaskDetails, (t) => {
        const { ancestorUuids } = this.graphDataByUuid[t.uuid];
        const errContextTaskUuid = _.find(
          ancestorUuids,
          u => _.has(this.tasksByUuid[u], 'error_context'),
        );
        if (errContextTaskUuid) {
          return this.tasksByUuid[errContextTaskUuid].error_context;
        }
        return '';
      });
    },
    displayTracebacksByUuid() {
      return _.mapValues(this.failedTaskDetails, t => this.getRootCauseMessageFromTraceback(
        t.traceback,
      ));
    },
  },
  methods: {
    fetchFailedTaskDetails(uuids) {
      this.failedTaskDetails = {};
      _.each(uuids, (uuid) => {
        api.fetchTaskDetails(uuid)
          .then((taskDetails) => {
            this.failedTaskDetails = _.assign(
              {}, this.failedTaskDetails, { [[uuid]]: taskDetails },
            );
          });
      });
    },
    getRootCauseMessageFromTraceback(traceback) {
      const causingSplitStr = 'The above exception was the direct cause of the following exception';

      let rootCauseMsg;
      if (_.includes(traceback, causingSplitStr)) {
        [rootCauseMsg] = traceback.split(causingSplitStr);
      } else if (traceback.startsWith('Traceback (most recent call last):')) {
        /**
         * Example"
         *Traceback (most recent call last):
         File "/some/path/trace.py", line 382, in trace_task
         R = retval = fun(*args, **kwargs)
         File "/some/other/path/file.py", line 170, in __call__
         return self._call(*args, **kwargs)
         <snip>
         Error message being extracted.
         Potentially multiple lines.
         */
        // root_cause_lines = [l for l in traceback_str.split('\n')[1:]
        //                   if not re.match(r"^\s.*", l)]
        const rootCauseLines = _.filter(_.tail(_.split(traceback, '\n')), l => !l.match('^\\s.*'));
        rootCauseMsg = _.join(rootCauseLines, '\n');
      } else {
        rootCauseMsg = traceback;
      }

      return _.trim(rootCauseMsg);
    },
    taskRoute(uuid) {
      return this.getTaskRoute(uuid);
    },
    longNameToModulePath(longName) {
      return _.join(_.dropRight(_.split(longName, '.')), '.');
    },
    shouldShowFull(traceback) {
      return traceback.split(/\n/).length < this.collapseTracebackLinesThreshold;
    },
  },
};
</script>

<style scoped>

  table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
    vertical-align: middle;
  }

  td {
    padding: 5px;
  }

  th {
    text-align: center;
  }

</style>
