<template>
  <foreignObject :x="this.position.x"
                 :y="this.position.y"
                 :width="collapseNode.width + 10"
                 :height="collapseNode.height + 10">
    <div :style="topStyle">
      <div :style="frontBoxStyle" class="stacked-effect">
        <div style="display: flex; flex-direction: row; flex-grow: 1; text-align: center;">
          <a  v-if="!allDescendantsAreChildren"
              class='collapse-action' href="#"
             :title="collapseNode.representedChildrenUuids.length + ' Top Tasks'"
             @click.prevent="emitExpandUuids(collapseNode.representedChildrenUuids)">
            {{collapseNode.representedChildrenUuids.length}}
            <font-awesome-icon icon="angle-down" size="lg"></font-awesome-icon>
          </a>
          <a class='collapse-action' href="#"
             :title="collapseNode.allRepresentedNodeUuids.length + ' Total Tasks'"
             @click.prevent="emitExpandUuids(collapseNode.allRepresentedNodeUuids)">
            {{collapseNode.allRepresentedNodeUuids.length}}
            <font-awesome-icon icon="angle-double-down" size="lg"></font-awesome-icon>
          </a>
        </div>
        <div style="display: flex; flex-direction: row; align-self: stretch; font-size: 12px; margin: 3px;">
          <div style="align-self: start; flex: 1;">{{displayHosts}}</div>
          <div style="align-self: end;">{{collapseNode.time}}</div>
        </div>
      </div>
      <div v-for="i in behindBoxesCount" :key="i"
            class="stacked-effect"  :style="getNonFrontBoxStyle(i)"></div>
    </div>
  </foreignObject>
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
  data() {
    return {
      boxOffset: 10,
      behindBoxesCount: 2,
    };
  },
  computed: {
    boxDimensions() {
      const width = this.collapseNode.width - this.behindBoxesCount * this.boxOffset;
      const height = this.collapseNode.height - this.behindBoxesCount * this.boxOffset;
      return { width: `${width}px`, height: `${height}px` };
    },
    displayHosts() {
      if (this.collapseNode.hosts.length === 1) {
        return this.collapseNode.hosts[0];
      }
      return `${this.collapseNode.hosts.length} hosts`;
    },
    topStyle() {
      return {
        width: `${this.collapseNode.width}px`,
        height: `${this.collapseNode.width}px`,
        'font-family': "'Source Sans Pro',sans-serif",
        color: 'white',
      };
    },
    frontBoxStyle() {
      return {
        'margin-left': '2px', // Make room for left shadow.
        'font-size': '18px',
        background: this.collapseNode.backgrounds[0],
        width: this.boxDimensions.width,
        height: this.boxDimensions.height,
        display: 'flex',
        'align-items': 'center',
        'flex-direction': 'column',
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
    emitExpandUuids(uuids) {
      const expandDescendantEvents = createCollapseEvent(uuids, 'expand', 'self');
      eventHub.$emit('ui-collapse', {
        keep_rel_position_task_uuid: this.collapseNode.parent_id,
        operationsByUuid: expandDescendantEvents,
      });
    },
    getNonFrontBoxStyle(level) {
      let background;
      if (level < this.collapseNode.backgrounds.length) {
        background = this.collapseNode.backgrounds[level];
      } else {
        background = _.last(this.collapseNode.backgrounds);
      }

      return {
        background,
        'margin-top': `${this.boxOffset * level}px`,
        'margin-left': `${this.boxOffset * level}px`,
        'z-index': -level,
        width: this.boxDimensions.width,
        height: this.boxDimensions.height,
      };
    },
  },
};

</script>

<style scoped>

  .collapse-action {
    padding: 5px 8px;
    color: white;
  }

  .collapse-action:hover {
    color: black;
  }

  .stacked-effect {
    box-shadow: 3px 3px 5px black;
    border-radius: 8px;
    position: absolute;
    top: 0;
  }

</style>
