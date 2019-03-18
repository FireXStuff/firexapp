<template>
  <div class="node-attributes">
    <div  v-for="(key, i) in sortedDisplayNodeKeys" :key="key"
          :style="{'background-color': i % 2 === 0 ? '#EEE': '#CCC', 'padding': '4px' }">
      <div v-if="key === 'firex_bound_args' || key === 'firex_default_bound_args'">
        <strong>arguments{{ key === 'firex_default_bound_args' ? '_defaults' : ''}}:</strong>
        <div v-for="(arg_value, arg_key) in displayNode[key]" :key="arg_key"
             style="margin-left: 25px; padding: 3px;">
          <strong>{{arg_key}}:</strong> {{arg_value}}
        </div>
      </div>
      <div v-else-if="key === 'firex_result'">
        <strong>task_result:</strong> {{displayNode[key]}}
      </div>
      <div v-else-if="key === 'support_location'">
        <strong>support_location:</strong> <a :href="displayNode[key]">{{displayNode[key]}}</a>
      </div>
      <span v-else>
        <strong>{{key}}:</strong> {{displayNode[key]}}
      </span>
    </div>
  </div>
</template>

<script>
/* eslint-disable */
import _ from 'lodash';
import {eventHub} from '../utils'

export default {
  name: 'XNodeAttributes',
  props: {
    uuid: {required: true, type: String},
    nodesByUuid: {}
  },
  computed: {
    displayNode () {
        let node =  _.clone(this.nodesByUuid[this.uuid])
        let attributeBlacklist = ['children', 'long_name', 'name', 'parent', 'flame_additional_data',
        'height', 'width', 'x', 'y', 'from_plugin', 'depth', 'logs_url', 'task_num', 'code_url']
        return _.omit(node, attributeBlacklist)
    },
    sortedDisplayNodeKeys () {
      return _.sortBy(_.keys(this.displayNode))
    }
  },
  created() {
    // TODO: this is super gross. Make it easier for children views to add buttons to the parent.
    if (this.nodesByUuid) {
      if (_.has(this.nodesByUuid[this.uuid], 'long_name')) {
        this.$emit('title', this.nodesByUuid[this.uuid].long_name)
      }
      if (_.has(this.nodesByUuid[this.uuid], 'logs_url')) {
        this.$emit('logs_url', this.nodesByUuid[this.uuid].logs_url)
      }
      if (_.has(this.nodesByUuid[this.uuid], 'code_url')) {
        eventHub.$emit('code_url', this.nodesByUuid[this.uuid].code_url)
      }
      if (_.has(this.nodesByUuid[this.uuid], 'support_location')) {
        eventHub.$emit('support_location', this.nodesByUuid[this.uuid].support_location)
      }
    }
  },
}
</script>

<style scoped>

.node-attributes {
  font-family: 'Source Code Pro',monospace;
  font-size: 14px;
  margin: 10px;
}

</style>
