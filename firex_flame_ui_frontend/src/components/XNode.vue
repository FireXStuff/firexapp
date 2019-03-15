<template>
  <div :style="topLevelStyle">
    <div style="overflow: hidden; text-overflow: ellipsis;">
      <div v-if="allowCollapse && node.children && node.children.length" style="float: right;">
        <i v-on:click="emit_collapse_toggle" style="cursor: pointer;">
          <font-awesome-icon v-if="expanded" icon="window-minimize"></font-awesome-icon>
          <font-awesome-icon v-else icon="window-maximize"></font-awesome-icon>
        </i>
      </div>
      <router-link :to="{name: 'XNodeAttributes', params: {'uuid': node.uuid}}">
        <div style="float: left; font-size: 12px; width: 0;">{{node.task_num}}</div>
        <div style="text-align: center; padding: 3px; ">
          <strong>{{node.name}}</strong>
        </div>
        <div class="flame-data">
          <!-- We're really trusting data from the server here (rendering raw HTML) -->
          <!-- TODO: verify flame_additional_data is always accumulative -->
          <div v-if="node.flame_additional_data" v-html="node.flame_additional_data" style="padding: 3px"></div>
        </div>

        <div style="float: left; font-size: 12px; margin-top: 4px">{{node.hostname}}</div>
        <div style="float: right; font-size: 12px; margin-top: 4px">{{duration}}</div>
       </router-link>
      <!-- TODO: the bottom-right isn't clickable to view task attributes. -->
    </div>
  </div>
</template>

<script>
import _ from 'lodash'

let nodeAttributes = [
  // 'x', 'y',
  'hostname',
  'task_num',
  // 'retries',
  'uuid',
  'name',
  'state',
  // 'width',
  // 'height',
]
// TODO: somewhere should be checking for node.rect

// let inProgressAnimationStyle = {
//   'animation-name': 'taskRunning',
//   'animation-duration': '1.5s',
//   'animation-timing-function': 'ease',
//   'animation-delay': '0s',
//   'animation-iteration-count': 'infinite',
//   'animation-direction': 'normal',
//   'animation-fill-mode': 'none',
//   'animation-play-state': 'running'
// }

export default {
  name: 'XNode',
  props: {
    node: {
      type: Object,
      required: true,
      validator: function (value) {
        return _.difference(nodeAttributes, _.keys(value)).length === 0
      },
    },
    allowCollapse: {
      default: true,
    },
    emitDimensions: {default: false},
  },
  data () {
    return {
      expanded: true,
      statusToColour: {
        'task-received': '#2A2',
        'task-blocked': '#888',
        'task-started': 'darkblue', // #888',
        'task-succeeded': '#2A2',
        'task-shutdown': '#2A2',
        'task-failed': '#900',
        'task-revoked': '#F40',
        'task-incomplete': '',
      },
    }
  },
  computed: {
    topLevelStyle () {
      let style = {
        'font-family': "'Source Sans Pro',sans-serif",
        'font-weight': 'normal',
        'font-size': '14px',
        background: this.statusToColour[this.node.state],
        color: 'white',
        width: _.isInteger(this.node.width) ? this.node.width + 'px' : this.node.width,
        height: _.isInteger(this.node.height) ? this.node.height + 'px' : this.node.height,
        'min-width': '250px', // TODO: externalize some of this.
        'border-radius': !this.node.chain_depth ? '8px' : '',
        border: this.node.from_plugin ? '2px dashed #000' : '',
        padding: '3px',
      }
      if (this.node.state === 'task-started') {
        // style = _.merge(style, inProgressAnimationStyle)
      }
      return style
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
      this.$nextTick(function () {
        this.emit_dimensions()
      })
    }
  },
  methods: {
    emit_collapse_toggle () {
      this.expanded = !this.expanded
      this.$emit('collapse-node')
    },
    emit_dimensions () {
      // TODO: this is gross. There must be a better way to get height and width dynamically.
      let h = this.$el.clientHeight
      let w = this.$el.clientWidth
      if (h && w) {
        this.$emit('node-dimensions', {uuid: this.node.uuid, height: h, width: w})
      }
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
  margin: 3px
}

a {
  color: inherit;
  cursor: pointer;
  text-decoration: none;
}

/*
TODO: fix.
div a:hover {*/
  /*background: #000;*/
/*}*/

</style>
