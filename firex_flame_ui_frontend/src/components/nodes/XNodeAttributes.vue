<template>
  <div style="width: 100%; height: 100%; display: flex; flex-direction: column;">
    <x-header :title="headerParams.title"
              :links="headerParams.links"
              :legacyPath="headerParams.legacyPath"></x-header>

    <div class="node-attributes">
      <div v-for="(key, i) in sortedDisplayNodeKeys" :key="key"
           :style="{'background-color': i % 2 === 0 ? '#EEE': '#CCC', 'padding': '4px' }">
        <label style="font-weight: 700;">{{key}}:</label>

        <div v-if="key === 'parent' && displayKeyNodes[key]" style="display: inline">
          {{nodesByUuid[displayKeyNodes[key]].name}}
          <router-link :to="linkToUuid(displayKeyNodes[key])">{{displayKeyNodes[key]}}</router-link>
        </div>
        <div v-else-if="key === 'children'">
          <div v-for="child_uuid in displayKeyNodes[key]" :key="child_uuid"
               style="margin-left: 25px; padding: 3px;">
            <strong>{{nodesByUuid[child_uuid].name}}: </strong>
            <router-link :to="linkToUuid(child_uuid)">{{child_uuid}}</router-link>
          </div>
        </div>
        <div v-else-if="key === 'support_location'" style="display: inline">
          <a :href="displayKeyNodes[key]"> {{displayKeyNodes[key]}}</a>
        </div>
        <div v-else-if="key === 'traceback'" style="display: inline">
          <pre style="overflow: auto; color: darkred; margin-top: 0"
            >{{displayKeyNodes[key].trim()}}</pre>
        </div>
        <div v-else-if="isTimeKey(key)" style="display: inline">
          {{formatTime(displayKeyNodes[key])}}
        </div>
        <div v-else-if="isObject(displayKeyNodes[key])" style="overflow: auto">
          <div v-for="(arg_value, arg_key) in displayKeyNodes[key]" :key="arg_key"
               style="margin-left: 25px; padding: 3px;">
            <strong>{{arg_key}}:
            </strong><pre v-if="shouldPrettyPrint(arg_value)" style="margin: 0 0 0 40px"
              >{{prettyPrint(arg_value)}}</pre>
            <template v-else>{{arg_value === null ? 'None' : arg_value}}</template>
          </div>
        </div>
        <span v-else>
          {{displayKeyNodes[key]}}
        </span>
      </div>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';
import { DateTime } from 'luxon';
import { routeTo, eventHub, isTaskStateIncomplete } from '../../utils';
import XHeader from '../XHeader.vue';

export default {
  name: 'XNodeAttributes',
  components: { XHeader },
  props: {
    uuid: { required: true, type: String },
    nodesByUuid: { required: true, type: Object },
    taskDetails: { required: true, type: Object },
    runMetadata: { default: () => ({ uid: '', logs_dir: '' }), type: Object },
  },
  computed: {
    displayNode() {
      // TODO: if an attribute is in nodesByUuid, that value should be rendered since
      //        it's auto-updated.
      // If we haven't fetched the details for some reason, just show the base properties.
      const base = _.isEmpty(this.taskDetails) ? this.nodesByUuid[this.uuid] : this.taskDetails;
      const node = _.clone(base);
      const attributeBlacklist = ['children', 'long_name', 'name', 'parent', 'flame_additional_data',
        'from_plugin', 'depth', 'logs_url', 'task_num', 'code_url'];
      return _.omit(node, attributeBlacklist);
    },
    displayKeyNodes() {
      const origKeysToDisplayKeys = {
        firex_bound_args: 'arguments',
        firex_default_bound_args: 'argument_defaults',
        firex_result: 'task_result',
        parent_id: 'parent',
        children_uuids: 'children',
      };
      return _.mapKeys(this.displayNode, (v, k) => _.get(origKeysToDisplayKeys, k, k));
    },
    sortedDisplayNodeKeys() {
      return _.sortBy(_.keys(this.displayKeyNodes));
    },
    headerParams() {
      let links = [
        { name: 'logs', href: this.taskDetails.logs_url, text: 'View Logs' },
        { name: 'support', href: this.taskDetails.support_location, text: 'Support' },
        { name: 'code', href: this.taskDetails.code_url, icon: 'file-code' },
      ];

      if (isTaskStateIncomplete(this.nodesByUuid[this.uuid].state)) {
        links = [
          {
            name: 'kill', on: () => eventHub.$emit('revoke-task', this.uuid), _class: 'kill-button', icon: 'times',
          },
        ].concat(links);
      }

      return {
        title: this.taskDetails.long_name,
        legacyPath: `/task/${this.uuid}`,
        links,
      };
    },
  },
  methods: {
    linkToUuid(uuid) {
      return routeTo(this, 'XNodeAttributes', { uuid });
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
        'local_received', 'timestamp'], key);
    },
    formatTime(unixTime) {
      const humanTime = DateTime.fromSeconds(unixTime).toLocaleString(DateTime.DATETIME_FULL);
      return `${humanTime} (orig: ${unixTime})`;
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
