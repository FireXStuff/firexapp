<template>
  <div :style="topLevelStyle" class="node">
    <router-link :to="routeToAttribute(node.uuid)">
      <div style="overflow: hidden; text-overflow: ellipsis">
        <div v-if="allowCollapse && node.children_uuids.length" style="float: right;">
          <!-- Use prevent to avoid activating node-wide attribute link -->
          <i v-on:click.prevent="emit_collapse_toggle" style="cursor: pointer; padding: 2px;">
            <font-awesome-icon v-if="expanded" icon="window-minimize"></font-awesome-icon>
            <font-awesome-icon v-else icon="window-maximize"></font-awesome-icon>
          </i>
        </div>
        <div v-if="node.retries" style="float: right; position: relative;">
          <img src="../assets/retry.png" class="retries-img">
          <div title="Retries" class="retries">{{node.retries}}</div>
        </div>
        <div style="float: left; font-size: 12px">{{node.task_num}}</div>
        <div style="text-align: center; padding: 3px; ">
          <strong>{{node.name}}</strong>
        </div>
        <!-- Flame data might handle clicks in their own way, so we stop propagation to avoid navigating to
             task node attribute page. Should likely find a better way.-->
        <div class="flame-data" v-on:click="$event.stopPropagation()">
          <div v-if="showUuid">{{node.uuid}}</div>
          <!-- We're really trusting data from the server here (rendering raw HTML) -->
          <!-- TODO: verify flame_additional_data is always accumulative -->
          <div v-if="node.flame_additional_data" v-html="node.flame_additional_data"></div>
        </div>

        <div style="float: left; font-size: 12px; margin-top: 4px">{{node.hostname}} {{node.state}}</div>
        <div style="float: right; font-size: 12px; margin-top: 4px">{{duration}}</div>
      </div>
    </router-link>
  </div>
</template>

<script>
import _ from 'lodash'
import {xNodeAttributeTo} from '../utils'

export default {
  name: 'XNode',
  props: {
    node: {
      type: Object,
      required: true,
      validator: function (value) {
        // TODO: is it possible to validate node input? Since events are sent, nodes might always be partial.
        return true
      },
    },
    allowCollapse: {
      default: true,
    },
    emitDimensions: {default: false},
    showUuid: {default: false},
    width: {required: true},
    height: {required: true},
  },
  data () {
    return {
      expanded: true,
      statusToColour: {
        'task-received': '#888',
        'task-blocked': '#888',
        'task-started': 'cornflowerblue', // animate?
        'task-succeeded': '#2A2',
        'task-shutdown': '#2A2',
        'task-failed': '#900',
        'task-revoked': '#F40',
        'task-incomplete': 'repeating-linear-gradient(45deg,#888,#888 5px,#444 5px,#444 10px)',
        // TODO: is this correct? completed means incompleted??
        //  No, this isn't right. But events to happen this way (revoked then completed, even though we want to show
        //  revoked.) sometimes completed means incomplete! I hope this is only at the end of a run.
        'task-completed': '#2A2',
        'task-unblocked': 'cornflowerblue',
      },
      intrinsicDimensions: {width: null, height: null},
    }
  },
  computed: {
    topLevelStyle () {
      return {
        background: this.statusToColour[this.node.state],
        // width/height could be 'auto'
        width: _.isNumber(this.width) ? this.width + 'px' : this.width,
        height: _.isNumber(this.height) ? this.height + 'px' : this.height,
        'border-radius': !this.node.chain_depth ? '8px' : '',
        border: this.node.from_plugin ? '2px dashed #000' : '',
      }
    },
    duration () {
      let actualRuntime = this.node.actual_runtime
      if (!actualRuntime) {
        return ''
      }
      let hours = Math.floor(actualRuntime / (60 * 60))
      let hoursInSecs = hours * 60 * 60
      let mins = Math.floor((actualRuntime - hoursInSecs) / 60)
      let minsInSecs = mins * 60
      let secs = Math.floor(actualRuntime - hoursInSecs - minsInSecs)

      let result = 'time: '
      if (hours > 0) {
        result += hours + 'h '
      }
      if (mins > 0) {
        result += mins + 'm '
      }
      if (secs > 0) {
        result += secs + 's'
      }
      if (hours === 0 && mins === 0 && secs === 0) {
        result += '<1s'
      }
      return result
    },
  },
  mounted () {
    if (this.emitDimensions) {
      this.emit_dimensions()
    }
  },
  updated () {
    // TODO: this is pretty gross. Maybe check if the dimensions have changed before emitting.
    if (this.emitDimensions) {
      this.emit_dimensions()
    }
  },
  methods: {
    emit_collapse_toggle () {
      this.expanded = !this.expanded
      this.$emit('collapse-node')
      // Gross hack since if the event propagates, the node-wide link to NodeAttributes is followed.
    },
    emit_dimensions () {
      // TODO: this is gross. There must be a better way to get height and width dynamically.
      this.$nextTick(function () {
        let r = this.$el.getBoundingClientRect()
        let isFirst = this.intrinsicDimensions.width === null || this.intrinsicDimensions.height === null
        let dimensionChanged = this.intrinsicDimensions.width !== r.width || this.intrinsicDimensions.height !== r.height
        if (r && (isFirst || dimensionChanged)) {
          this.$emit('node-dimensions', {uuid: this.node.uuid, height: r.height, width: r.width})
          this.intrinsicDimensions = {width: r.width, height: r.height}
        }
      })
    },
    routeToAttribute (uuid) {
      return xNodeAttributeTo(uuid, this)
    },
  },
}
</script>

<style scoped>

/*@keyframes taskRunning {*/
  /*from {*/
      /*box-shadow: 0 0 10px #888;*/
  /*}*/
  /*50% {*/
      /*box-shadow: 0 0 50px #05F;*/
  /*}*/
  /*to {*/
      /*box-shadow: 0 0 10px #888;*/
  /*}*/
/*}*/

/*.progress {*/
  /*animation: taskRunning 1.5s ease 0s infinite normal none running;*/
/*}*/

.flame-data {
  background: white;
  text-align: center;
  border: 1px solid rgba(217,83,79,0.8);
  color: black;
  margin: 3px;
  min-width: 250px;
}

a {
  color: inherit;
  cursor: pointer;
  text-decoration: none;
}

.node {
  font-family: 'Source Sans Pro',sans-serif;
  font-weight: normal;
  font-size: 14px;
  color: white;
  padding: 3px;
}

.node:hover {
  background: #000 !important;
}

.flame-data a {
    display: inline-block;
}

 /* retries position is off for big numbers. TODO: find better solution. */
.retries {
  position: absolute;
  right: 6px;
  font-size: 11px;
  top: 2px;
}

.retries-img {
  position: absolute;
  right: 1px;
  height: 16px;
}

</style>
