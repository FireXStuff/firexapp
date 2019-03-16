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
    nodes: {required: true, type: Array},
  },
  computed: {
    displayLinks () {
      let links = []
      let uuids = _.map(this.nodes, 'uuid')
      this.nodes.forEach(p => {
        p.children.forEach(c => {
          // Only show links where both ends are in the input list of nodes.
          if (_.includes(uuids, c.uuid)) {
            let d = 'M' + (p.x + p.width / 2) + ' ' + (p.y + p.height) +
              // Vertical line half the vertical distance between the two nodes.
              ' v ' + (c.y - p.y - p.height) / 2 +
              // Horizontal line between the horizontal-centers of the two nodes.
              ' h ' + ((c.x + c.width / 2) - (p.x + p.width / 2)) +
              ' v ' + (c.y - p.y) / 2
            links.push({d: d, id: p.uuid + '->' + c.uuid})
          }
        })
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
