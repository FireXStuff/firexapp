<template>
  <div>
  <div id="list-container" style="margin-top:25px; display: inline-block;">
    <div style="margin: 0 50px">
      <x-node v-for="n in displayNodes" ref="list-node"
              :node="n" :key="n.uuid" style="margin: 10px;" class="node" :allowCollapse="false"></x-node>
    </div>
  </div>
  </div>
</template>

<script>
/* eslint-disable */
import _ from 'lodash';

import XNode from "./XNode";

export default {
  name: 'XList',
  components: {XNode},
  props: {
    root: {required: false}
  },
  computed: {
    displayNodes () {
      this.nodes.forEach(function(n) {
        n.width = 'auto'
        n.height = 'auto'
      })
      return _.sortBy(this.nodes, 'task_num')
    },
    // TODO: decide where this function should live.
    nodes () {
      if (!this.root) {
        return []
      }
      let node_list = []
      let nodes_to_check = [this.root]
      while (nodes_to_check.length > 0) {
        let node = nodes_to_check.pop()
        node_list.push(node)
        // TODO: where the hell are children getting deleted???
        if (_.has(node, 'children')) {
          nodes_to_check = nodes_to_check.concat(node['children'])
        }
      }
      return node_list
    }
  }
  // mounted () {
  //   // wait for all children to be mounted (have $el populated)
  //   let vm = this;
  //   // console.log(this.$el)
  //   this.$nextTick(function () {
  //     // TODO: why doesn't this work? On nextTick, all children should have $el populated. Accessing through $children
  //     //  works, but accessing through $refs (where we can track UUID) doesn't have $el populated????
  //     _.values(vm.$refs).forEach(function (comp) {
  //         console.log(comp.$el.clientHeight) //.$el.clientHeight) //.$el.height)
  //     })
  //   })
  // }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>


.link {
  fill: none;
  stroke: #000; /*#ccc; */
  stroke-width: 2px; /*4px;*/
}

#chart-container {
    position: absolute;
    top: 40px; /* Header Height */
    bottom: 20px; /* Footer Height */
    width: 100%;
}

</style>
