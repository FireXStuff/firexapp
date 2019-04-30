<template>
  <g>
    <path class="link" v-for="l in displayLinks" :d="l.d" :key="l.id"></path>
  </g>
</template>

<script>
import _ from 'lodash';

export default {
  name: 'XLinks',
  props: {
    nodeDimensionsByUuid: { required: true, type: Object },
    nodeLayoutsByUuid: { required: true, type: Object },
  },
  computed: {
    nodesByUuid() {
      return _.mapValues(this.nodeDimensionsByUuid,
        n => _.merge({}, n, this.nodeLayoutsByUuid[n.uuid]));
    },
    displayLinks() {
      const links = [];
      // We only want to show links for nodes with a layout.
      _.each(this.nodeLayoutsByUuid, (__, cUuid) => {
        const c = this.nodesByUuid[cUuid];
        const parentId = c.parent_id;
        if (!_.isNull(parentId) && _.has(this.nodesByUuid, parentId)) {
          const p = this.nodesByUuid[parentId];
          const pXCenter = p.x + p.width / 2;
          // Horizontal line between the horizontal-centers of the two nodes.
          const horzDistance = (c.x + c.width / 2) - pXCenter;
          const pBottom = p.y + p.height;
          // Vertical line half the vertical distance between the two nodes.
          const halfVerticalDistance = (c.y - p.y - p.height) / 2;

          const d = `M${pXCenter} ${pBottom}`
            + ` v ${halfVerticalDistance}`
            + `h ${horzDistance}`
            + `v ${halfVerticalDistance}`;

          links.push({ d, id: `${p.uuid}->${c.uuid}` });
        }
      });
      return links;
    },
  },
};
</script>

<style scoped>

.link {
  fill: none;
  stroke: #000; /*#ccc; */
  stroke-width: 2px; /*4px;*/
}

</style>
