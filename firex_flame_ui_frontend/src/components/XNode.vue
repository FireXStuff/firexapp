<template>
  <div :style="topLevelStyle" class="node">
    <router-link :to="allowClickToAttributes ? routeToAttribute(node.uuid) : currentRoute()">
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

        <div style="float: left; font-size: 12px; margin-top: 4px">{{node.hostname}}</div>
        <div style="float: right; font-size: 12px; margin-top: 4px">{{duration}}</div>
      </div>
    </router-link>
  </div>
</template>

<script>
import _ from 'lodash'
import {routeTo} from '../utils'

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
    allowClickToAttributes: {default: true},
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
    if (this.emitDimensions) {
      this.emit_dimensions()
    }
  },
  methods: {
    emit_collapse_toggle () {
      this.expanded = !this.expanded
      this.$emit('collapse-node')
    },
    emit_dimensions () {
      this.$nextTick(function () {
        // TODO: the bounding rect includes border-width and padding, but when we set the height and width
        //  of nodes these values get added again, meaning the SVG rendered size is different from this emitted
        // size. This makes some alignment off by the padding + border width (~10px)
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
      return routeTo(this, 'XNodeAttributes', {uuid: uuid})
    },
    currentRoute () {
      // The 'to' supplied to a router-link must be mutable for some reason.
      return _.clone(this.$router.currentRoute)
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
  width: auto;
  height: auto;
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
