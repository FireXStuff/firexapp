<template>
  <g :transform="transform">

    <foreignObject :width="collapseNode.width + 10" :height="collapseNode.height + 10">
      <div :style="style">
        <div v-if="!allDescendantsAreChildren" style="padding: 5px">
          <a class='collapse-action' href="#" @click.prevent="emitExpandChildren">&plus;
            {{collapseNode.representedChildrenUuids.length}} Top
            {{collapseNode.representedChildrenUuids.length === 1 ? 'Task': 'Tasks'}}</a>
        </div>
        <div style="padding: 5px">
          <a class='collapse-action' href="#" @click.prevent="emitExpandDescendants">&plus;&plus;
            {{collapseNode.allRepresentedNodeUuids.length}}
            {{collapseNode.allRepresentedNodeUuids.length === 1 ? 'Task': 'Total Tasks'}}</a>
        </div>
      </div>
    </foreignObject>

  </g>
</template>

<script>
import _ from 'lodash';
import { eventHub, createCollapseEvent } from '../../utils';

export default {
  name: 'XSvgCollapseNode',
  props: {
    position: { required: true, type: Object },
    // TODO: validate width == height
    collapseNode: { required: true, type: Object },
  },
  computed: {
    radius() {
      return this.collapseNode.width / 2;
    },
    xCenter() {
      return this.position.x + this.radius;
    },
    yCenter() {
      return this.position.y + this.radius;
    },
    style() {
      return {
        'border-radius': `${this.radius}px`,
        'font-size': '12px',
        background: this.collapseNode.background,
        width: `${this.collapseNode.width}px`,
        height: `${this.collapseNode.height}px`,
        display: 'flex',
        'align-items': 'center',
        'flex-direction': 'column',
        'justify-content': 'center',
        'text-align': 'center',
      };
    },
    transform() {
      return `translate(${this.position.x},${this.position.y})`;
    },
    allDescendantsAreChildren() {
      return this.collapseNode.representedChildrenUuids.length
        === this.collapseNode.allRepresentedNodeUuids.length;
    },
  },
  methods: {
    emitExpandDescendants() {
      // TODO: should there be a descendantsInclusive (selfAndDescendants) target?
      const expandChildrenEvents = createCollapseEvent(this.collapseNode.representedChildrenUuids,
        'expand', 'self');
      const expandDescendantEvents = createCollapseEvent(this.collapseNode.representedChildrenUuids, 'expand',
        'descendants');
      eventHub.$emit('ui-collapse', {
        keep_rel_position_task_uuid: this.collapseNode.parent_id,
        operationsByUuid: _.merge(expandChildrenEvents, expandDescendantEvents),
      });
    },
    emitExpandChildren() {
      const expandChildrenEvents = createCollapseEvent(this.collapseNode.representedChildrenUuids,
        'expand', 'self');
      const collapseNonChildrenDescendantEvents = {};
      // createCollapseEvent(
      //   this.collapseNode.representedChildrenUuids, 'collapse', 'descendants',
      // );
      // TODO: maybe this should be {[[parent_id]]: { grandchildren: 'collapse' }}
      eventHub.$emit('ui-collapse', {
        keep_rel_position_task_uuid: this.collapseNode.parent_id,
        operationsByUuid: _.merge(expandChildrenEvents, collapseNonChildrenDescendantEvents),
      });
    },
  },
};

</script>

<style scoped>

  .collapse-action {
    font-family: 'Source Sans Pro',sans-serif;
    color: white;
  }

  .collapse-action:hover {
    color: black;
  }

</style>
