<template>
  <div>
    <!-- TODO: this div can eat in to the preceding one despite display: block. Figure out why and remove margin.  -->
  <div id="chart-container" ref="graph-parent" style="margin-top:25px">
    <!--.attr('viewBox','0 0 '+Math.min(width, height)+' '+Math.min(width, height))-->
    <svg width="100%" height="100%" viewBox="0 0 294 294" preserveAspectRatio="xMinYMin" ref="graph-svg">
      <rect width="1385" height="294" style="fill: none; pointer-events: all;"></rect>
      <g>
        <!--     .append("g").attr("transform", "translate(" + (margin.left + width/2) + "," + margin.top + ")") -->
        <g ref="inner-graph-svg">
          <!--  transform="translate(-137.6250000000001,-102.26371954479794)scale(1)translate(812.5,20)"-->
           <path class="link" v-for="l in displayLinks" :d="l.d" :key="l.source.uuid +'->' + l.target.uuid">
           </path>

             <x-svg-node v-for="n in displayNodes" :node="n" :key="n.uuid"
                     v-on:collapse-node="toggle_collapse_children(n.uuid)"
                     v-on:node-dimensions="updateNodeDimensions($event)">
             </x-svg-node>
        </g>
      </g>
    </svg>
  </div>
  </div>
</template>

<script>
/* eslint-disable */

//  TODO: specify what to import from d3 more precisely.
// TODO: use a more recent version of d3 (find where layout was moved to).
import * as d3 from "d3";
import XSvgNode from './XSvgNode';
import _ from 'lodash';

var margin = {top: 20, right: 120, bottom: 20, left: 120};
//var width = 3000 - margin.right - margin.left;
//var height = 1000 - margin.top - margin.bottom;

// TODO: Needs to be dynamic in case lots of flame data,
let nodeWidth = 250;
let nodeHeight = 100;

let d3Tree = d3.layout.tree()
//    .size([height, width])
  .separation(function(a, b) {return a.parent === b.parent ? 1.5 : 2;})
	.nodeSize([nodeWidth, nodeHeight]);

export default {
  name: 'XGraph',
  components: {XSvgNode},
  props: {
    root: {}
  },
  data () {
    return {
      hidden_node_ids: [],
      // very unfortunate we need to track this manually. TODO: look for a better way.
      dimensions_by_uuid: {},
    };
  },
  computed: {
    allNodesById () {
      if (this.root) {
        // let all_uuids = this.getUuids(this.root)
        // only actually do layout if we have all the dimensions of child nodes.
        // if (_.difference(all_uuids, _.keys(this.dimensions_by_uuid)).length === 0) {
          // Compute layout using de hierarical layout. This adds x,y attributes to node objects.
          let tree_nodes = d3Tree.nodes(this.root).reverse();

          // Force shared y coordinate for nodes at same depth.
          tree_nodes.forEach((d) => {
            let horizontal_node_spacing = 150;
            d.y = d.depth * horizontal_node_spacing

            if (_.has(this.dimensions_by_uuid, d.uuid)) {
              d.height = this.dimensions_by_uuid[d.uuid].height
              d.width = nodeWidth //this.dimensions_by_uuid[d.uuid].width
            } else {
              d.height = 'auto'
              d.width = nodeWidth //'auto'
            }
          });

          // There is only one node per uuid, so use _.head to get it.
          return _.mapValues(_.groupBy(tree_nodes, 'uuid'), _.head);
        // }
      }
      return {};
    },
    displayNodes () {
      return _.values(_.omit(this.allNodesById, this.hidden_node_ids))
    },
    displayLinks () {
      let vm = this;
      // Don't want to show links with one-end hidden.
      let withoutHiddenChildren = _.map(this.displayNodes, function(n){
        let tmp = _.clone(n)
        tmp.children = _.filter(n.children, c => !_.includes(vm.hidden_node_ids, c.uuid))
        return tmp
      });
      let links = d3Tree.links(withoutHiddenChildren)
      links.forEach(l => {
        let source_height = _.isNumber(l.source.height) ? l.source.height : nodeHeight
        let d = "M" + (l.source.x + nodeWidth/2) + ' ' + (l.source.y + source_height)
            + " v " + (l.target.y - l.source.y - source_height)/2
            + " h" + (l.target.x - l.source.x)
            + " v " + (l.target.y - l.source.y)/2
        l['d'] = d
      })
      return links
    },
    uid () {
      let nodeWithUid =  _.find(_.values(this.allNodesById), 'firex_bound_args.uid')
      if (nodeWithUid) {
        return nodeWithUid.firex_bound_args.uid
      }
      this.$emit('title', uid) // TODO: move out from computed
      return ''
    }
  },
  mounted () {
    let zoom = d3.behavior.zoom()
                          .scaleExtent([0.01, 1])
                          .on("zoom", this.zoomed);
    d3.select('div#chart-container svg').call(zoom).on("dblclick.zoom", null);
    // TODO: set initial zoom.
  },
  methods: {
    zoomed () {
      let t1 = "translate(" + d3.event.translate + ")";
      let s1 = "scale(" + d3.event.scale + ")";

      this.$refs['inner-graph-svg'].setAttribute("transform", t1 + s1)
    },
    toggle_collapse_children(parent_node_id) {
      let descendant_ids = this.getDescendantUuids(parent_node_id);
      if (_.intersection(this.hidden_node_ids, descendant_ids).length > 0) {
        // These IDs are currently hidden, so remove them.
        this.hidden_node_ids = _.difference(this.hidden_node_ids, descendant_ids);
      }
      else {
        this.hidden_node_ids = this.hidden_node_ids.concat(descendant_ids);
      }
    },
    updateNodeDimensions (event) {
        let new_entry = {}
        new_entry[event.uuid] = {width: event.width, height: event.height}
        // Vue doesn't deep watch, so create a new object.
        this.dimensions_by_uuid = _.merge({}, this.dimensions_by_uuid, new_entry)
    },
    // TODO: write or find generic tree-walking functions.
    getDescendantUuids(node_id) {
      let result_uuids = [];
      let uuids_to_check = _.map(this.allNodesById[node_id]['children'], 'uuid');
      while (uuids_to_check.length > 0) {
        let node_uuid = uuids_to_check.pop();
        if (!_.includes(result_uuids, node_uuid)) {
          let children_ids = _.map(this.allNodesById[node_uuid]['children'], 'uuid')
          uuids_to_check = uuids_to_check.concat(children_ids)
          result_uuids.push(node_uuid)
        }
      }
      return result_uuids;
    },
    getUuids(root) {
      let result_uuids = [root.uuid]
      root.children.forEach((c) => {
        result_uuids = result_uuids.concat(this.getUuids(c))
      })
      return result_uuids
    }
  }
}
</script>

<style scoped>

.node text {
  font: 12px sans-serif;
}

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
