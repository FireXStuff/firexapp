<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;">
    <x-header :title="headerParams.title"
              :main-title="headerParams.mainTitle"
              :links="headerParams.links"></x-header>

    <div class="node-attributes">
      <div v-for="(key, i) in sortedDisplayNodeKeys" :key="key"
           :style="{'background-color': i % 2 === 0 ? '#EEE': '#CCC', 'padding': '4px' }">
        <label style="font-weight: 700;">{{key}}:</label>

        <!-- Add parent {name, uuid} to store-->
        <div v-if="key === 'parent' && displayKeyNode[key]" style="display: inline">
          {{displayKeyNode[key].name}}
          <router-link :to="getTaskRoute(displayKeyNode[key].uuid)"
          >{{displayKeyNode[key].uuid}}</router-link>
        </div>
        <div v-else-if="key === 'children'">
          <div v-for="child in displayKeyNode[key]" :key="'child-' + child.uuid"
               style="margin-left: 25px; padding: 3px;">
            <strong>{{child.name}}: </strong>
            <router-link :to="getTaskRoute(child.uuid)">{{child.uuid}}</router-link>
          </div>
        </div>
        <div v-else-if="key === 'support_location'" style="display: inline">
          <a :href="displayKeyNode[key]"> {{displayKeyNode[key]}}</a>
        </div>
        <div v-else-if="key === 'traceback'" style="display: inline">
          <pre style="overflow: auto; color: darkred; margin-top: 0"
            >{{displayKeyNode[key].trim()}}</pre>
        </div>
        <div v-else-if="key === 'exception'" style="display: inline; color: darkred">
          {{displayKeyNode[key].trim()}}
        </div>
        <div v-else-if="key === 'replay command line'">
          <div style="margin-left: 30px;margin-top:10px">
            <div>
              {{replayCommandLine}}
            </div>
            <div style="margin-top:6px">
              <button type="button" @click="doCopy">
                <font-awesome-icon icon="clipboard"></font-awesome-icon> Copy
              </button>
            </div>
          </div>
        </div>
        <div v-else-if="isTimeKey(key)" style="display: inline">
          {{formatTime(displayKeyNode[key])}}
        </div>
        <div v-else-if="isObject(displayKeyNode[key])" style="overflow: auto">
          <div v-for="(arg_value, arg_key) in displayKeyNode[key]" :key="arg_key"
               style="margin-left: 25px; padding: 3px;">
            <strong>{{arg_key}}:
            </strong><pre v-if="shouldPrettyPrint(arg_value)" style="margin: 0 0 0 40px"
              >{{prettyPrint(arg_value)}}</pre>
            <template v-else>{{arg_value === null ? 'None' : arg_value}}</template>
          </div>
        </div>
        <span v-else>
          {{displayKeyNode[key]}}
        </span>
      </div>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';
import { DateTime } from 'luxon';
import { mapGetters, mapState } from 'vuex';

import * as api from '../../api';
import { eventHub, isTaskStateIncomplete, durationString } from '../../utils';
import XHeader from '../XHeader.vue';

export default {
  name: 'XNodeAttributes',
  components: { XHeader },
  props: {
    uuid: { required: true, type: String },
  },
  data() {
    return {
      showAllAttributes: false,
      taskAttributes: {},
    };
  },
  computed: {
    ...mapGetters({
      runDuration: 'tasks/runDuration',
      getTaskRoute: 'header/getTaskRoute',
    }),
    ...mapState({
      logsDir: state => state.firexRunMetadata.logs_dir,
      firexBin: state => _.get(state.header.uiConfig, 'firex_bin', 'firex'),
      uid: state => state.firexRunMetadata.uid,
    }),
    simpleTask() {
      return this.$store.getters['tasks/runStateByUuid'][this.uuid];
    },
    taskNameByUuid() {
      return this.$store.getters['tasks/taskNameByUuid'];
    },
    childrenUuids() {
      return this.$store.getters['graph/childrenUuidsByUuid'][this.uuid];
    },
    taskChildren() {
      return _.map(this.childrenUuids,
        uuid => ({ uuid, name: this.taskNameByUuid[uuid] }));
    },
    parentUuid() {
      return this.$store.getters['graph/parentUuidByUuid'][this.uuid];
    },
    taskParent() {
      let parent;
      if (!_.isNil(this.parentUuid)) {
        parent = {
          uuid: this.parentUuid,
          name: this.taskNameByUuid[this.parentUuid],
        };
      } else {
        parent = {};
      }
      return parent;
    },
    detailedTask() {
      const addedFields = { children: this.taskChildren, parent: this.taskParent };
      return _.assign(addedFields, this.taskAttributes);
    },
    displayNode() {
      // If we haven't fetched the details for some reason, just show the base properties.
      const task = _.merge({}, this.simpleTask, this.detailedTask);

      let attributeBlacklist;
      if (this.showAllAttributes) {
        attributeBlacklist = [];
        task.minPriorityCollapseOp = this.minPriorityOp;

        _.each(task.states, (state, i) => {
          if (_.has(task.states, i + 1)) {
            const nextState = task.states[i + 1];
            const stateDuration = nextState.timestamp - state.timestamp;
            state.duration = durationString(stateDuration);
            state['% of task'] = 100 * stateDuration / task.actual_runtime;
            state['% of run'] = 100 * stateDuration / this.runDuration;
          }
        });
      } else {
        attributeBlacklist = ['long_name', 'name', 'flame_additional_data',
          'from_plugin', 'depth', 'logs_url', 'task_num', 'code_url', 'flame_data',
          'parent_id', 'children_uuids', 'isLeaf', 'states', 'called_as_orig',
          'code_filepath',
        ];
      }
      if (task.actual_runtime) {
        const humanDuration = durationString(task.actual_runtime);
        task.actual_runtime = `${humanDuration} (orig: ${task.actual_runtime})`;
      }

      return _.omit(task, attributeBlacklist);
    },
    displayKeyNode() {
      const origKeysToDisplayKeys = {
        firex_bound_args: 'arguments',
        firex_default_bound_args: 'argument_defaults',
        firex_result: 'task_result',
      };
      return _.mapKeys(this.displayNode, (v, k) => _.get(origKeysToDisplayKeys, k, k));
    },
    sortedDisplayNodeKeys() {
      const displayKeys = _.sortBy(_.keys(this.displayKeyNode));
      displayKeys.push('replay command line');
      return displayKeys;
    },
    headerParams() {
      let links = [
        {
          name: 'showAllAttributes',
          // eslint-disable-next-line
          on: () => { this.showAllAttributes = !this.showAllAttributes; },
          toggleState: this.showAllAttributes,
          icon: 'plus-circle',
          title: 'Show All Attributes',
        },
        // TODO: use new 'code_filepath' & central_server instead of 'code_url'
        {
          name: 'code',
          href: this.detailedTask.code_url,
          icon: 'file-code',
          title: 'See Code',
        },
        // TODO: use new 'log_filepath' field & central_server instead of 'logs_url'.
        {
          name: 'logs',
          href: this.detailedTask.logs_url,
          text: 'Service Log',
          icon: 'file-alt',
        },
        {
          name: 'support',
          href: this.detailedTask.support_location,
          text: 'Support',
          icon: 'question-circle',
        },
      ];

      if (isTaskStateIncomplete(this.simpleTask.state)) {
        links = [
          {
            name: 'kill',
            on: () => eventHub.$emit('revoke-task', this.uuid),
            _class: 'kill-button',
            icon: 'times',
          },
        ].concat(links);
      }

      return {
        title: this.uid,
        mainTitle: this.detailedTask.long_name,
        links,
      };
    },
    minPriorityOp() {
      return this.$store.getters['graph/resolvedCollapseStateByUuid'][this.uuid].minPriorityOp;
    },
    replayCommandLine() {
      return `${this.firexBin} submit --chain Replay --uuid ${this.taskAttributes.uuid} --previous_logs_dir ${this.logsDir}`;
    },
  },
  methods: {
    fetchTaskAttributes() {
      api.fetchTaskDetails(this.uuid).then((taskAttributes) => {
        this.taskAttributes = taskAttributes;
      });
    },
    isObject(val) {
      return _.isObject(val);
    },
    // TODO: doesn't make sense to string twice, combine with prettyPrint.
    shouldPrettyPrint(val) {
      if (!_.isObject(val)) {
        return false;
      }
      const asJson = JSON.stringify(val, null, 2);
      // Don't bother pretty printing if the object is small.
      return asJson.length > 100;
    },
    prettyPrint(val) {
      if (val === null) {
        return 'None';
      }
      const prettyJson = JSON.stringify(val, null, 2);
      if (prettyJson.length < 40) {
        // If the string is short enough, don't show a pretty version.
        return JSON.stringify(val);
      }
      return prettyJson;
    },
    isTimeKey(key) {
      return _.includes(['first_started', 'started', 'failed', 'succeeded', 'revoked',
        'timestamp'], key);
    },
    formatTime(unixTime) {
      const humanTime = DateTime.fromSeconds(unixTime).toLocaleString(DateTime.DATETIME_FULL);
      return `${humanTime} (orig: ${unixTime})`;
    },
    doCopy() {
      this.$copyText(this.replayCommandLine);
    },
  },
  watch: {
    // eslint-disable-next-line
    '$route': {
      handler: 'fetchTaskAttributes',
      immediate: true,
    },
  },
};
</script>

<style scoped>

.node-attributes {
  font-family: 'Source Code Pro', monospace;
  font-size: 14px;
  margin: 10px;
  flex: 1;
  overflow: auto;
  display: flex;
  flex-direction: column;
}

</style>
