<template>
  <router-link :to="allowClickToAttributes ? routeToAttribute(node.uuid) : currentRoute()">
    <div :style="topLevelStyle" class="node" v-on:click.shift.prevent="nodeShiftClick">
      <div style="overflow: hidden; text-overflow: ellipsis;">
        <div style="display: flex;">

          <div style="align-self: start; font-size: 12px">{{node.task_num}}</div>

          <div style="text-align: center; padding: 3px; align-self: center; flex: 1;">
            {{node.name}}
          </div>

          <div v-if="node.retries" style="align-self: end; position: relative;">
            <img src="../../assets/retry.png" class="retries-img">
            <div title="Retries" class="retries">{{node.retries}}</div>
          </div>

          <!-- visibility: collapsed to include space for collapse button, even when allowCollapse
            is false. -->
          <div v-if="node.children_uuids.length && !isChained" style="align-self: end;"
               :style="allowCollapse ? '' : 'visibility: collapse;'">
            <!-- Use prevent to avoid activating node-wide attribute link -->
            <i v-on:click.prevent="emit_collapse_toggle" style="cursor: pointer; padding: 2px;">
              <font-awesome-icon v-if="!isAnyChildCollapsed" icon="window-minimize">
              </font-awesome-icon>
              <font-awesome-icon v-else icon="window-maximize"></font-awesome-icon>
            </i>
          </div>
        </div>
        <!-- Flame data might handle clicks in their own way, so we stop propagation to avoid
        navigating to task node attribute page. Should likely find a better way.-->
        <div class="flame-data" v-on:click="flameDataClick">
          <div v-if="showUuid">{{node.uuid}}</div>
          <div v-if="showUuid && displayDetails">{{displayDetails}}</div>
          <!-- We're really trusting data from the server here (rendering raw HTML) -->
          <!-- TODO: find out why <br /> is randomly in flame data.
                .replace(new RegExp('<br />', 'g'), '') -->
          <div v-if="displayFlameAdditionalData" v-html="displayFlameAdditionalData"></div>
        </div>

        <div style="display: flex; flex-direction: row; font-size: 12px; margin-top: 4px;">
          <div style="align-self: start; flex: 1;">{{node.hostname}}</div>
          <div style="align-self: end;">{{duration}}</div>
        </div>
      </div>
    </div>
  </router-link>

</template>

<script>
import _ from 'lodash';
import {
  routeTo, durationString, isTaskStateIncomplete, getNodeBackground,
} from '../../utils';

export default {
  name: 'XNode',
  props: {
    node: {
      type: Object,
      required: true,
      // TODO: is it possible to validate node input? Since events are sent,
      //  nodes might always be partial.
      // validator() { },
    },
    allowCollapse: {
      default: true,
    },
    showUuid: { default: false },
    liveUpdate: { default: false },
    allowClickToAttributes: { default: true },
    isAnyChildCollapsed: { default: false },
    emitDimensions: { default: false },
    displayDetails: { require: false },
  },
  data() {
    return {
      liveRunTime: 0,
    };
  },
  computed: {
    isChained() {
      return Boolean(this.node.chain_depth);
    },
    topLevelStyle() {
      return {
        background: getNodeBackground(this.node.exception, this.node.state),
        'border-radius': !this.isChained ? '8px' : '',
        border: this.node.from_plugin ? '2px dashed #000' : '',
      };
    },
    duration() {
      let runtime;
      if (!isTaskStateIncomplete(this.node.state) && this.node.actual_runtime) {
        runtime = this.node.actual_runtime;
      } else if (!runtime && (this.node.local_received || this.node.first_started)) {
        runtime = this.liveRunTime;
      } else {
        return '';
      }
      return durationString(runtime);
    },
    displayFlameAdditionalData() {
      if (!this.node.flame_additional_data) {
        return undefined;
      }
      return this.node.flame_additional_data.replace(/__start_dd.*__end_dd/g, '');
    },
  },
  created() {
    this.updateLiveRuntimeAndSchedule();
  },
  mounted() {
    this.emit_dimensions();
  },
  updated() {
    this.emit_dimensions();
  },
  methods: {
    emit_collapse_toggle() {
      this.$emit('collapse-node');
    },
    updateLiveRuntimeAndSchedule() {
      if (this.liveUpdate
        // task-complete occurs in-between retries.
        && (isTaskStateIncomplete(this.node.state) || this.node.state === 'task-completed')
        && (this.node.first_started || this.node.local_received)) {
        const start = this.node.first_started ? this.node.first_started : this.node.local_received;
        this.liveRunTime = (Date.now() / 1000) - start;
        setTimeout(() => {
          // Note liveUpdate may have changed since this timeout was set, so double check.
          if (this.liveUpdate) {
            this.updateLiveRuntimeAndSchedule();
          }
        }, 3000);
      }
    },
    routeToAttribute(uuid) {
      return routeTo(this, 'XNodeAttributes', { uuid });
    },
    currentRoute() {
      // The 'to' supplied to a router-link must be mutable for some reason.
      return _.clone(this.$router.currentRoute);
    },
    flameDataClick(event) {
      event.stopPropagation();
    },
    nodeShiftClick() {
      if (this.allowClickToAttributes) {
        this.$router.push(routeTo(this, 'custom-root', { rootUuid: this.node.uuid }));
      }
    },
    emit_dimensions() {
      if (this.emitDimensions) {
        this.$nextTick(() => {
          const r = this.$el.getBoundingClientRect();
          const renderedWidth = r.width; // this.$el.clientWidth
          const renderedHeight = r.height; // this.$el.clientHeight
          if (renderedWidth && renderedHeight) {
            const renderedDimensions = { width: r.width, height: r.height };
            if (!_.isEqual(this.latestEmittedDimensions, renderedDimensions)) {
              this.$emit('node-dimensions', _.merge({ uuid: this.node.uuid }, renderedDimensions));
              this.latestEmittedDimensions = renderedDimensions;
            }
          }
        });
      }
    },
  },
};
</script>

<style scoped>

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
  display: block;
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
