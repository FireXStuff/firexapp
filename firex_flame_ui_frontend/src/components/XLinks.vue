<template>
  <g>
    <path class="link" v-for="l in displayLinks" :d="l.d" :key="l.id"></path>
  </g>
</template>

<script>
import _ from 'lodash'

export default {
  name: 'XLinks',
  props: {
    onlyVisibleIntrinsicDimensionNodesByUuid: {required: true, type: Object},
    nodeLayoutsByUuid: {required: true, type: Object},
  },
  computed: {
    nodesByUuid () {
      return _.mapValues(this.onlyVisibleIntrinsicDimensionNodesByUuid, n => {
        return _.merge({}, n, this.nodeLayoutsByUuid[n.uuid])
      })
    },
    displayLinks () {
      let links = []
      _.each(this.nodesByUuid, c => {
        let parentId = c.parent_id
        if (!_.isNull(parentId) && _.has(this.nodesByUuid, parentId)) {
          let p = this.nodesByUuid[parentId]
          let d = 'M' + (p.x + p.width / 2) + ' ' + (p.y + p.height) +
            // Vertical line half the vertical distance between the two nodes.
            ' v ' + (c.y - p.y - p.height) / 2 +
            // Horizontal line between the horizontal-centers of the two nodes.
            ' h ' + ((c.x + c.width / 2) - (p.x + p.width / 2)) +
            ' v ' + (c.y - p.y - p.height) / 2
          links.push({d: d, id: p.uuid + '->' + c.uuid})
        }
      })
      return links
    },
  },
}
</script>

<style scoped>

.link {
  fill: none;
  stroke: #000; /*#ccc; */
  stroke-width: 2px; /*4px;*/
}

</style>
