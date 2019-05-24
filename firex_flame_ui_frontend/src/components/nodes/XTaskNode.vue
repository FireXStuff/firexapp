<template>
  <router-link :to="allowClickToAttributes ? routeToAttribute() : currentRoute()">
    <div :style="topLevelStyle" class="node" v-on:click.shift.prevent="nodeShiftClick">
      <div style="overflow: hidden; text-overflow: ellipsis;">
        <div style="display: flex;">

          <div style="align-self: start; font-size: 12px">{{taskNumber}}</div>

          <div style="text-align: center; padding: 3px; align-self: center; flex: 1;">
            {{taskName}}
          </div>

          <div v-if="retries" style="align-self: end; position: relative;">
            <img src="../../assets/retry.png" class="retries-img">
            <div title="Retries" class="retries">{{retries}}</div>
          </div>

          <!-- visibility: collapsed to include space for collapse button, even when allowCollapse
            is false. -->
          <!-- TODO: seems odd both isLeaf and allowCollapse exist just to gate collapse. -->
          <div style="align-self: end;"
               :style="!isLeaf && !isChained && allowCollapse ? '' : 'visibility: collapse;'">
            <!-- Use prevent to avoid activating node-wide attribute link -->
            <i v-on:click.prevent="emitCollapseToggle" style="cursor: pointer; padding: 2px;">
              <font-awesome-icon v-if="toCollapse" icon="compress-arrows-alt"
                title="collapse"></font-awesome-icon>
              <font-awesome-icon v-else icon="expand-arrows-alt" title="expand">
              </font-awesome-icon>
            </i>
          </div>
        </div>
        <!-- Flame data might handle clicks in their own way, so we stop propagation to avoid
        navigating to task node attribute page. Should likely find a better way.-->
        <div class="flame-data" v-on:click="flameDataClick">
          <div v-if="showTaskDetails">{{taskUuid}}</div>
          <div v-if="showTaskDetails">{{minPriorityOp}}</div>
          <!-- We're really trusting data from the server here (rendering raw HTML) -->
          <div v-if="showLegacyFlameAdditionalData" v-html="flameAdditionalData"></div>
          <template v-else>
            <div v-for="(html, i) in flameDataHtmlContent" :key="i" v-html="html"></div>
          </template>
        </div>

        <div style="display: flex; flex-direction: row; font-size: 12px; margin-top: 4px;">
          <div style="align-self: start; flex: 1;">{{hostname}}</div>
          <x-duration :runState="runState"
                      :firstStarted="firstStarted"
                      :actualRuntime="actualRuntime"
                      style="align-self: end;"></x-duration>
        </div>
      </div>
    </div>
  </router-link>
</template>

<script>
import _ from 'lodash';
import {
  routeTo2, getNodeBackground, eventHub, getTaskNodeBorderRadius,
} from '../../utils';
import XDuration from './XDuration.vue';

export default {
  name: 'XNode',
  components: { XDuration },
  props: {
    allowCollapse: { default: true },
    taskUuid: { required: true },
    allowClickToAttributes: { default: true },
    toCollapse: { default: false },
    emitDimensions: { default: false, type: Boolean },
    isLeaf: { required: true, type: Boolean },
  },
  data() {
    return {
      latestEmittedDimensions: { width: -1, height: -1 },
    };
  },
  computed: {
    task() {
      return this.$store.state.tasks.allTasksByUuid[this.taskUuid];
    },
    showTaskDetails() {
      return this.$store.state.graph.showTaskDetails;
    },
    hostname() {
      return this.task.hostname;
    },
    flameAdditionalData() {
      return this.task.flame_additional_data;
    },
    retries() {
      return this.task.retries;
    },
    taskName() {
      return this.task.name;
    },
    taskNumber() {
      return this.task.task_num;
    },
    isChained() {
      return Boolean(this.chainDepth);
    },
    runState() {
      return this.$store.getters['tasks/runStateByUuid'][this.taskUuid].state;
    },
    exception() {
      return this.$store.getters['tasks/runStateByUuid'][this.taskUuid].exception;
    },
    actualRuntime() {
      return this.task.actual_runtime;
    },
    firstStarted() {
      return this.task.first_started;
    },
    chainDepth() {
      return this.task.chain_depth;
    },
    fromPlugin() {
      return this.task.from_plugin;
    },
    minPriorityOp() {
      return this.$store.getters['graph/resolvedCollapseStateByUuid'][this.taskUuid].minPriorityOp;
    },
    topLevelStyle() {
      const s = {
        background: getNodeBackground(this.exception, this.runState),
        'border-radius': getTaskNodeBorderRadius(this.chainDepth),
      };
      if (this.fromPlugin) {
        s.border = '2px dashed #000';
      }
      return s;
    },
    showLegacyFlameAdditionalData() {
      return !_.includes(_.map(_.get(this.task, 'flame_data', {}),
        'type'), 'html');
    },
    flameDataHtmlContent() {
      if (!_.has(this.task, 'flame_data')) {
        return {};
      }
      return _.map(
        _.reverse(_.sortBy(_.filter(
          this.task.flame_data,
          d => d.type === 'html',
        ), ['order'])),
        'value',
      );
    },
  },
  mounted() {
    this.emit_dimensions();
  },
  updated() {
    this.emit_dimensions();
  },
  methods: {
    emitCollapseToggle() {
      eventHub.$emit('toggle-task-collapse', this.taskUuid);
    },
    routeToAttribute() {
      return routeTo2(this.$route.query, 'XNodeAttributes', { uuid: this.taskUuid });
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
        this.$router.push(routeTo2(this.$route.query, 'custom-root', { rootUuid: this.taskUuid }));
      }
    },
    // TODO: debounce?
    emit_dimensions() {
      if (this.emitDimensions) {
        this.$nextTick(() => {
          const r = this.$el.getBoundingClientRect();
          const renderedWidth = r.width; // this.$el.clientWidth
          const renderedHeight = r.height; // this.$el.clientHeight
          if (renderedWidth && renderedHeight) {
            const renderedDimensions = { width: renderedWidth, height: renderedHeight };
            if (!_.isEqual(this.latestEmittedDimensions, renderedDimensions)) {
              this.latestEmittedDimensions = renderedDimensions;
              this.$emit('task-node-size', { [[this.taskUuid]]: renderedDimensions });
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
  border-right: 1px solid white;
  border-bottom: 1px solid white;
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
