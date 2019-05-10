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
    parentUuidByUuid: { required: true, type: Object },
    nodeLayoutsByUuid: { required: true, type: Object },
  },
  computed: {
    displayLinks() {
      const links = [];
      // We only want to show links for nodes with a layout.
      _.each(this.nodeLayoutsByUuid, (childLayout, cUuid) => {
        const parentId = this.parentUuidByUuid[cUuid];
        if (!_.isNull(parentId) && _.has(this.nodeLayoutsByUuid, parentId)) {
          const parentLayout = this.nodeLayoutsByUuid[parentId];
          const pXCenter = parentLayout.x + parentLayout.width / 2;
          // Horizontal line between the horizontal-centers of the two nodes.
          const horzDistance = (childLayout.x + childLayout.width / 2) - pXCenter;
          const pBottom = parentLayout.y + parentLayout.height;
          // Vertical line half the vertical distance between the two nodes.
          const halfVerticalDistance = (childLayout.y - parentLayout.y - parentLayout.height) / 2;

          const d = `M${pXCenter} ${pBottom}`
            + ` v ${halfVerticalDistance}`
            + `h ${horzDistance}`
            + `v ${halfVerticalDistance}`;

          links.push({ d, id: `${parentId}->${cUuid}` });
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
