<template>
  <!-- always have a margin to keep front nodes centered, regardless to if either front node
      has stacked boxes behind it. -->
  <div :style="{'margin-left': behindBoxesCount*boxOffset + 'px', 'display': 'inline-block'}">
    <div :style="frontBoxStyle">
        <x-task-node :node="node"
                :showUuid="showUuid"
                :liveUpdate="liveUpdate"
                :style="frontTaskStyle"
                :areAllChildrenCollapsed="areAllChildrenCollapsed"
                :displayDetails="displayDetails"></x-task-node>
    </div>

    <div v-if="hasCollapsedChildren"
         :style="stacksContainerStyle" class="stacks-link" @click="expandAll">
      <div v-for="i in behindBoxesCount" :key="i"
           class="stacked-effect" :style="getNonFrontBoxStyle(i-1)">
      </div>
      <!-- TODO: put in terms of stack offset -->
      <div class="stacks-count">
        {{collapseNode.allRepresentedNodeUuids.length}}
        {{collapseNode.allRepresentedNodeUuids.length === 1 ? 'Task' : 'Tasks'}}
      </div>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';
import { eventHub, createCollapseEvent, containsAll } from '../../utils';
import XTaskNode from './XTaskNode.vue';

export default {
  name: 'XSvgCollapseNode',
  components: { XTaskNode },
  props: {
    node: { type: Object, required: true },
    showUuid: {},
    dimensions: { required: true, type: Object },
    liveUpdate: { required: true, type: Boolean },
    collapseNode: { required: true, type: Object },
    displayDetails: { required: true },
  },
  data() {
    return {
      // TODO: receive this value as input, since it's needed for layout calc.
      boxOffset: 12,
      behindBoxesCount: 3, // TODO: receive this value as input, since it's needed for layout calc.
    };
  },
  computed: {
    areAllChildrenCollapsed() {
      return containsAll(this.collapseNode.representedChildrenUuids,
        this.node.children_uuids);
    },
    hasCollapsedChildren() {
      return this.collapseNode.allRepresentedNodeUuids.length > 0;
    },
    boxDimensions() {
      const width = this.dimensions.width - this.behindBoxesCount * this.boxOffset * 2;
      const height = this.dimensions.height - this.behindBoxesCount * this.boxOffset;
      return { width, height };
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
      };
    },
    stacksContainerStyle() {
      const offsets = (this.behindBoxesCount - 1) * this.boxOffset;
      return _.merge(this.getNonFrontBoxMargins(1), {
        width: `${this.boxDimensions.width + offsets}px`,
        height: `${this.boxDimensions.height + offsets}px`,
      });
    },
  },
  methods: {
    emitExpandUuids(uuids) {
      const expandDescendantEvents = createCollapseEvent(uuids, 'expand', 'self');
      eventHub.$emit('ui-collapse', {
        keep_rel_position_task_uuid: this.collapseNode.parent_id,
        operationsByUuid: expandDescendantEvents,
      });
    },
    getNonFrontBoxMargins(level) {
      return {
        // One pixel to offset for border.
        'margin-top': `${this.boxOffset * level}px`,
        'margin-left': `${this.boxOffset * level}px`,
      };
    },
    getNonFrontBoxStyle(level) {
      let background;
      if (level < this.collapseNode.backgrounds.length) {
        background = this.collapseNode.backgrounds[level];
      } else {
        background = _.first(this.collapseNode.backgrounds);
      }

      return _.merge(this.getNonFrontBoxMargins(level),
        {
          background,
          'z-index': -(level + 1),
          width: `${this.boxDimensions.width}px`,
          height: `${this.boxDimensions.height}px`,
        });
    },
    expandAll() {
      this.emitExpandUuids(this.collapseNode.allRepresentedNodeUuids);
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
    bottom: 8px; /* hack. should be calced from offset?*/
    text-align: center;
    width: 100%;
    color: white;
    font-size: 14px;
  }

</style>
