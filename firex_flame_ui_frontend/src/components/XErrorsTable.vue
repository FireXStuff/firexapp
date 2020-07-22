<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;">
    <x-header :title="title"
              :links="headerLinks">
    </x-header>
    <div style="width: 100%; padding: 2em;">
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
          <tr v-for="(t, u) in failedTaskDetails" :key="u">
            <td style="text-align: center;">
              <div>{{t.name}}{{ t.from_plugin ? ' (plugin)' : ''}}</div>
              <div>{{longNameToModulePath(t.long_name)}}</div>
            </td>
            <td style="white-space: pre-line;"
                v-html="createLinkedHtml(getRootCauseMessageFromTraceback(t.traceback))"
            ></td>
            <td style="text-align: center;">
              {{errorContextByUuid[u]}}
            </td>
            <td style="text-align: center;">
              <div style="white-space: nowrap;">
                <a :href="t.logs_url">
                  <font-awesome-icon icon="file-alt"></font-awesome-icon>
                  View Log
                </a>
              </div>
              <div style="white-space: nowrap;">
                <router-link :to="taskRoute(u)">
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
import { isChainInterrupted } from '../utils';

export default {
  name: 'XErrorsTable',
  components: { XHeader },
  props: {

  },
  data() {
    return {
      failedTaskDetails: {},
    };
  },
  created() {
    this.fetchFailedTaskDetails(this.failedTaskUuidsSorted);
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
    failedTaskUuidsSorted() {
      const failedTasks = _.filter(this.tasksByUuid,
        t => t.state === 'task-failed' && !isChainInterrupted(t.exception));

      const dedupedFailedTasks = _.flatMap(_.groupBy(failedTasks, 'name'), (sameNameTasks) => {
        // Never de-duplicate Warnings
        if (_.first(sameNameTasks).name === 'Warning') {
          return sameNameTasks;
        }
        const origTasks = _.filter(sameNameTasks, { from_plugin: false });
        const pluginTasks = _.filter(sameNameTasks, { from_plugin: true });

        // Keep all plugins & dedupe after we've fetched details and have long_name
        // Only keep first orig task.
        return _.concat(_.first(origTasks) || [], pluginTasks);
      });

      return _.map(dedupedFailedTasks, 'uuid');
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

      return _.trim(_.truncate(rootCauseMsg, { length: 1000, omission: ' [...]' }));
    },
    taskRoute(uuid) {
      return this.getTaskRoute(uuid);
    },
    longNameToModulePath(longName) {
      return _.join(_.dropRight(_.split(longName, '.')), '.');
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

</style>
