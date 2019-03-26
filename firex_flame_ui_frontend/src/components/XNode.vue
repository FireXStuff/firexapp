<template>
  <div style="display: block">
    <router-link :to="allowClickToAttributes ? routeToAttribute(node.uuid) : currentRoute()"
                 style="display: inline-block;">
      <div :style="topLevelStyle" class="node">

        <div style="overflow: hidden; text-overflow: ellipsis;">
          <div style="display: flex;">

            <div style="align-self: start; font-size: 12px">{{node.task_num}}</div>

            <div style="text-align: center; padding: 3px; align-self: center; flex: 1;">
              {{node.name}}
            </div>

            <div v-if="node.retries" style="align-self: end; position: relative;">
              <img src="../assets/retry.png" class="retries-img">
              <div title="Retries" class="retries">{{node.retries}}</div>
            </div>

            <!-- visibility: collapsed to include space for collapse button, even when allowCollapse is false. -->
            <div v-if="node.children_uuids.length && !isChained" style="align-self: end;"
                 :style="allowCollapse ? 'visibility: collapsed': ''">
              <!-- Use prevent to avoid activating node-wide attribute link -->
              <i v-on:click.prevent="emit_collapse_toggle" style="cursor: pointer; padding: 2px;">
                <font-awesome-icon v-if="expanded" icon="window-minimize"></font-awesome-icon>
                <font-awesome-icon v-else icon="window-maximize"></font-awesome-icon>
              </i>
            </div>
          </div>
          <!-- Flame data might handle clicks in their own way, so we stop propagation to avoid navigating to
               task node attribute page. Should likely find a better way.-->
          <div class="flame-data" v-on:click="flameDataClick">
            <div v-if="showUuid">{{node.uuid}}</div>
            <!-- We're really trusting data from the server here (rendering raw HTML) -->
            <!-- TODO: verify flame_additional_data is always accumulative -->
            <div v-if="node.flame_additional_data" v-html="node.flame_additional_data"></div>
          </div>

          <div style="float: left; font-size: 12px; margin-top: 4px">{{node.hostname}}</div>
          <div style="float: right; font-size: 12px; margin-top: 4px">{{duration}}</div>
        </div>

      </div>
    </router-link>
  </div>
</template>

<script>
import _ from 'lodash'
import {routeTo, isChainInterrupted} from '../utils'

let successGreen = '#2A2'

let statusToColour = {
  'task-received': '#888',
  'task-blocked': '#888',
  'task-started': 'cornflowerblue', // animate?
  'task-succeeded': successGreen,
  'task-shutdown': successGreen,
  'task-failed': '#900',
  'task-revoked': '#F40',
  'task-incomplete': 'repeating-linear-gradient(45deg,#888,#888 5px,#444 5px,#444 10px)',
  'task-completed': '#AAA',
  'task-unblocked': 'cornflowerblue',
}

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

      intrinsicDimensions: {width: null, height: null},
    }
  },
  computed: {
    background () {
      if (isChainInterrupted(this.node.exception)) {
        return 'repeating-linear-gradient(45deg,#888,#888 5px,#893C3C 5px,#893C3C  10px)'
      }
      return statusToColour[this.node.state]
    },
    isChained () {
      return Boolean(this.node.chain_depth)
    },
    topLevelStyle () {
      return {
        background: this.background,
        'border-radius': !this.isChained ? '8px' : '',
        border: this.node.from_plugin ? '2px dashed #000' : '',
      }
    },
    duration () {
      let runtime = this.node.actual_runtime

      if (!runtime && this.node.first_started) {
        runtime = (Date.now() / 1000) - this.node.local_received
      }
      if (!runtime && this.node.local_received) {
        runtime = (Date.now() / 1000) - this.node.local_received
      }
      if (!runtime) {
        return ''
      }

      let hours = Math.floor(runtime / (60 * 60))
      let hoursInSecs = hours * 60 * 60
      let mins = Math.floor((runtime - hoursInSecs) / 60)
      let minsInSecs = mins * 60
      let secs = Math.floor(runtime - hoursInSecs - minsInSecs)

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
    flameDataClick (event) {
      event.stopPropagation()
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
  font-style: normal;
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
