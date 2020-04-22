<template>
  <g>
    <path class="link" v-for="l in displayLinks" :d="l.d" :key="l.id">
    </path>
    <path class="link extra-link" v-for="l in displayExtraLinks" :d="l.d" :key="'extra-' + l.id" >
    </path>
  </g>
</template>

<script>
import _ from 'lodash';

export default {
  name: 'XLinks',
  props: {
    parentUuidByUuid: { required: true, type: Object },
    nodeLayoutsByUuid: { required: true, type: Object },
    additionalChildrenByUuid: { required: true, type: Object },
  },
  computed: {
    displayLinks() {
      const links = [];
      // We only want to show links for nodes with a layout.
      _.each(this.nodeLayoutsByUuid, (childLayout, cUuid) => {
        const parentId = this.parentUuidByUuid[cUuid];
        if (!_.isNull(parentId) && _.has(this.nodeLayoutsByUuid, parentId)) {
          const parentLayout = this.nodeLayoutsByUuid[parentId];
          const d = this.createPath(parentLayout, childLayout);
          links.push({ d, id: `${parentId}->${cUuid}` });
        }
      });
      return links;
    },
    displayExtraLinks() {
      const links = [];
      // We only want to show links for nodes with a layout.
      _.each(this.additionalChildrenByUuid, (extraChildren, pUuid) => {
        if (_.has(this.nodeLayoutsByUuid, pUuid)) {
          const parentLayout = this.nodeLayoutsByUuid[pUuid];
          _.each(extraChildren, (extraChildUuid) => {
            if (_.has(this.nodeLayoutsByUuid, extraChildUuid)) {
              const childLayout = this.nodeLayoutsByUuid[extraChildUuid];
              const d = this.createPath(parentLayout, childLayout);
              links.push({ d, id: `${pUuid}-extra->${extraChildUuid}` });
            }
          });
        }
      });
      return links;
    },
  },
  methods: {
    createPath(startLayout, endLayout) {
      const pXCenter = startLayout.x + startLayout.width / 2;
      // Horizontal line between the horizontal-centers of the two nodes.
      const horzDistance = (endLayout.x + endLayout.width / 2) - pXCenter;
      const pBottom = startLayout.y + startLayout.height;
      // Vertical line half the vertical distance between the two nodes.
      const halfVerticalDistance = (endLayout.y - startLayout.y - startLayout.height) / 2;

      return `M${pXCenter} ${pBottom}`
        + ` v ${halfVerticalDistance}`
        + `h ${horzDistance}`
        + `v ${halfVerticalDistance}`;
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

.extra-link {
  stroke-dasharray: 10 4;
  stroke-width: 2px;
}

.extra-link:hover {
  stroke: cornflowerblue;
}

</style>
