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
      <div v-else-if="key === 'parent_id' && displayNode[key]">
        <strong>parent:</strong>
        {{nodesByUuid[displayNode[key]].name}}
        <router-link :to="linkToUuid(displayNode[key])">{{displayNode[key]}}</router-link>
      </div>
      <div v-else-if="key === 'children_uuids'">
        <strong>children:</strong>
        <div v-for="child_uuid in displayNode[key]" :key="child_uuid" style="margin-left: 25px">
          {{nodesByUuid[child_uuid].name}} <router-link :to="linkToUuid(child_uuid)">{{child_uuid}}</router-link>
        </div>
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
import _ from 'lodash'
import {eventHub, xNodeAttributeTo} from '../utils'

export default {
  name: 'XNodeAttributes',
  props: {
    uuid: {required: true, type: String},
    nodesByUuid: {required: true, type: Object},
    taskDetails: {required: true, type: Object},
  },
  computed: {
    displayNode () {
      let node = _.clone(this.taskDetails)
      let attributeBlacklist = ['children', 'long_name', 'name', 'parent', 'flame_additional_data',
        'height', 'width', 'x', 'y', 'from_plugin', 'depth', 'logs_url', 'task_num', 'code_url']
      return _.omit(node, attributeBlacklist)
    },
    sortedDisplayNodeKeys () {
      return _.sortBy(_.keys(this.displayNode))
    },
  },
  mounted () {
    // TODO: this is super gross. Make it easier for children views to add buttons to the parent.
    this.emitData()
  },
  methods: {
    linkToUuid (uuid) {
      return xNodeAttributeTo(uuid, this)
    },
    fetchAttributes (uuid) {
      return fetch('/flame.rec')
        .then(function (r) {
          return r.json()
        })
    },
    emitData () {
      let data = [
        {field: 'long_name', event: 'title'},
        {field: 'logs_url', event: 'logs_url'},
        {field: 'code_url', event: 'code_url'},
        {field: 'support_location', event: 'support_location'},
      ]
      data.forEach(d => {
        if (_.has(this.taskDetails, d.field)) {
          eventHub.$emit(d.event, this.taskDetails[d.field])
        }
      })
    },
  },
  // beforeRouteEnter (to, from, next) {
  //   next(vm => {
  //     vm.emitData()
  //   })
  // },
  // beforeRouteUpdate (to, from, next) {
  //   this.emitData()
  // },
  watch: {
    'taskDetails' (newVal, oldVal) {
      this.emitData()
    },
    // 'uuid' (newVal, oldVal) {
    //   this.emitData()
    // },
    '$route' (to, from) {
      this.emitData()
    },
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
