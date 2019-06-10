<template>
  <!-- always have a margin to keep front nodes centered, regardless to if either front node
      has stacked boxes behind it. -->
  <div :style="{'margin-left': allStackOffset + 'px', 'display': 'inline-block'}">
    <div :style="frontBoxStyle">
        <x-task-node
          :taskUuid="taskUuid"
          :style="frontTaskStyle"
          :toCollapse="!areAllDescendantsCollapsed"
          :isLeaf="isLeaf"></x-task-node>
    </div>

    <div v-if="hasCollapsedChildren"
         :style="stacksContainerStyle" class="stacks-link" @click="expandAll">
      <div v-for="i in stackCount" :key="i"
           class="stacked-effect" :style="getNonFrontBoxStyle(i-1)">
      </div>
      <div class="stacks-count">
        {{collapsedUuidsCount}} {{collapsedUuidsCount === 1 ? 'Task' : 'Tasks'}}
      </div>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';
import {
  eventHub, containsAll,
} from '../../utils';
import {
  stackOffset, stackCount,
} from '../../collapse';

import XTaskNode from './XTaskNode.vue';

export default {
  name: 'XCollapseableTaskNode',
  components: { XTaskNode },
  props: {
    taskUuid: { required: true },
    width: { required: true },
    height: { required: true },
  },
  data() {
    return {
      stackCount,
    };
  },
  computed: {
    descendantUuids() {
      return this.$store.getters['graph/graphDataByUuid'][this.taskUuid].descendantUuids;
    },
    collapseDetails() {
      // TODO: figure out how it's possible for uncollapsedGraphByNodeUuid can not have
      // a node that has a layout. uncollapsedGraphByNodeUuid is used by the layout,
      // so everything in the layout must come from uncollapsedGraphByNodeUuid.

      return _.get(this.$store.getters['graph/uncollapsedGraphByNodeUuid'],
        this.taskUuid, { collapsedUuids: [] });
    },
    isLeaf() {
      return this.$store.getters['graph/graphDataByUuid'][this.taskUuid].isLeaf;
    },
    collapsedUuidsCount() {
      return this.collapseDetails.collapsedUuids.length;
    },
    areAllDescendantsCollapsed() {
      return containsAll(this.collapseDetails.collapsedUuids,
        this.descendantUuids);
    },
    hasCollapsedChildren() {
      return this.collapseDetails.collapsedUuids.length > 0;
    },
    allStackOffset() {
      return stackCount * stackOffset;
    },
    boxDimensions() {
      // 2x since we pad both sides of width.
      const width = this.width - this.allStackOffset * 2;
      // only subtract height if there is actually stacks to show.
      const height = this.height
        - (this.hasCollapsedChildren ? this.allStackOffset : 0);
      return { width, height };
    },
    chainDepth() {
      return this.$store.getters['tasks/runStateByUuid'][this.taskUuid].chain_depth;
    },
    frontBoxStyle() {
      return {
        width: `${this.boxDimensions.width}px`,
        height: `${this.boxDimensions.height}px`,
      };
    },
    frontTaskStyle() {
      return {
        width: `${this.boxDimensions.width}px`,
        height: `${this.boxDimensions.height}px`,
        display: 'inline-block',
      };
    },
    stacksContainerStyle() {
      const offsets = (stackCount - 1) * stackOffset;
      return _.merge(this.getNonFrontBoxMargins(1), {
        width: `${this.boxDimensions.width + offsets}px`,
        height: `${this.boxDimensions.height + offsets}px`,
      });
    },
  },
  methods: {
    getNonFrontBoxMargins(level) {
      return {
        // One pixel to offset for border.
        'margin-top': `${stackOffset * level}px`,
        'margin-left': `${stackOffset * level}px`,
      };
    },
    getNonFrontBoxStyle(level) {
      return _.merge(this.getNonFrontBoxMargins(level),
        {
          background: this.collapseDetails.background,
          'z-index': -(level + 1),
          width: `${this.boxDimensions.width}px`,
          height: `${this.boxDimensions.height}px`,
        });
    },
    expandAll() {
      eventHub.$emit('show-collapsed-tasks', { uuid: this.taskUuid });
    },
  },
};

</script>

<style scoped>

  .stacked-effect {
    border: 0.5px solid white;
    border-radius: 8px;
    position: absolute;
    top: 0;
  }

  .stacks-link:hover .stacked-effect {
    cursor: pointer;
    background: black !important;
  }

  .stacks-link {
    position: absolute;
    top: 0;
    z-index: -1;
    display: inline-block;
  }

  .stacks-link:hover .stacks-count {
    cursor: pointer;
  }

  .stacks-count {
    position: absolute;
    bottom: 1px; /* TODO: fix hack. should be calced from offset? */
    text-align: center;
    width: 100%;
    color: white;
    font-size: 13px;
    font-family: 'Source Sans Pro', sans-serif;
    height: 1em;
  }

</style>
