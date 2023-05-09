<template>
  <div :style="topLevelStyle" class="node">
    <div class="node-body">
      <div class="node-header">

        <div class="task-num" title="task number">{{taskNumber}}</div>

        <div class="task-name">
          {{taskName}}{{fromPlugin ? ' (plugin)' : ''}}
        </div>

        <div v-if="retries" :title="'retried ' + retries + ' times'">
          <font-awesome-layers fixed-width >
            <font-awesome-icon :icon="['fal', 'redo']"/>
            <font-awesome-layers-text transform="shrink-3" :value="retries" />
          </font-awesome-layers>
        </div>

        <!-- visibility: collapsed to include space for collapse button, even when allowCollapse
          is false. -->
        <!-- TODO: seems odd both isLeaf and allowCollapse exist just to gate collapse. -->
        <div class="align-self-end"
             :style="!isLeaf && allowCollapse ? '' : 'visibility: collapse;'">

             <!-- Use prevent to avoid activating node-wide attribute link -->
          <i v-on:click.prevent="emitCollapseToggle" class="collapse-btn-container">
            <font-awesome-icon v-if="toCollapse"
                               icon="compress-arrows-alt"
                               title="collapse" fixed-width/>
            <font-awesome-icon v-else
                               icon="expand-arrows-alt"
                               title="expand" fixed-width/>
          </i>
        </div>
      </div>
      <!-- Flame data might handle clicks in their own way, so we stop propagation to avoid
      navigating to task node attribute page. Should likely find a better way. -->
      <div class="flame-data" v-on:click="flameDataClick" >
        <div v-if="showTaskDetails">{{taskUuid}}</div>

        <template v-if="showFlameData" >
          <!-- We're really trusting data from the server here (rendering raw HTML) -->
          <div v-for="(html, i) in flameDataHtmlContent" :key="i" v-html="html"></div>
        </template>
      </div>

      <div class="node-footer">
        <div class="task-host" title="queuename@hostname">{{displayHostname}}</div>
        <!-- FIXME: the decision if live time is necessary should be made here to reduce total #
          of components. If live update is not needed, no peer-comp-eval occurs, so the extra
          comp is pure overhead.-->
        <x-duration :runState="runState"
                    :firstStarted="firstStarted"
                    :approxRuntime="approxRuntime"
                    :actualRuntime="actualRuntime"
                    class="align-self-end"/>
      </div>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';
import { mapGetters } from 'vuex';

import {
  getNodeBackground, eventHub, getTaskNodeBorderRadius,
} from '../../utils';
import XDuration from './XDuration.vue';

export default {
  name: 'XCoreTaskNode',
  components: { XDuration },
  props: {
    allowCollapse: { default: true },
    taskUuid: { required: true },
    toCollapse: { default: false },
    isLeaf: { default: false, type: Boolean },
    emitDimensions: { default: false, type: Boolean },
    showFlameData: { default: true, type: Boolean },
  },
  data() {
    return {
      latestEmittedDimensions: { width: -1, height: -1 },
    };
  },
  computed: {
    ...mapGetters({
      getCustomRootRoute: 'header/getCustomRootRoute',
    }),
    task() {
      return this.$store.state.tasks.allTasksByUuid[this.taskUuid];
    },
    showTaskDetails() {
      return this.$store.state.graph.showTaskDetails;
    },
    displayHostname() {
      return _.truncate(this.task.hostname, { length: 22 });
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
    approxRuntime() {
      return this.task.approx_runtime;
    },
    chainDepth() {
      return this.task.chain_depth;
    },
    fromPlugin() {
      return this.task.from_plugin;
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
    flameDataHtmlContent() {
      return this.$store.getters['tasks/flameHtmlsByUuid'][this.taskUuid];
    },
  },
  mounted() {
    this.doEmitDimensions();
    // TODO: might still be worth considering in conjunction with other solutions.
    // Confirmed not affected by zoom, but still doesn't solve all flame-data node-sizing issues.
    // if (this.emitDimensions && typeof ResizeObserver !== 'undefined') {
    //   const resizeObserver = new ResizeObserver(() => {
    //     this.doEmitDimensions();
    //   });
    //   resizeObserver.observe(this.$el);
    // }
  },
  updated() {
    this.doEmitDimensions();
  },
  methods: {
    emitCollapseToggle() {
      eventHub.$emit('toggle-task-collapse', this.taskUuid);
    },
    flameDataClick(event) {
      event.stopPropagation();
    },
    // TODO: debounce?
    doEmitDimensions() {
      if (this.emitDimensions) {
        this.$nextTick(() => {
          const r = this.$el.getBoundingClientRect();
          const renderedWidth = r.width; // this.$el.clientWidth
          const renderedHeight = r.height; // this.$el.clientHeight
          if (renderedWidth && renderedHeight) {
            const renderedDimensions = { width: renderedWidth, height: renderedHeight };
            if (!_.isEqual(this.latestEmittedDimensions, renderedDimensions)) {
              this.latestEmittedDimensions = renderedDimensions;
              eventHub.$emit('task-node-size', { [[this.taskUuid]]: renderedDimensions });
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

.node-body {
  overflow: hidden;
  text-overflow: ellipsis;
}

.node-header {
  display: flex;
}

.task-num {
  align-self: start;
  font-size: 12px
}

.task-name {
  text-align: center;
  padding: 0 3px;
  align-self: center;
  flex: 1;
}

.node-footer {
  display: flex;
  flex-direction: row;
  font-size: 12px;
  margin-top: 4px;
}

.task-host {
  align-self: start;
  flex: 1;
}

.align-self-end {
  align-self: end;
}

.flame-data a {
  display: inline-block;
}

.collapse-btn-container {
  cursor: pointer;
  padding: 0 2px;
}

</style>
