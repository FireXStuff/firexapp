<template>
  <div>
    <!-- TODO: this div can eat in to the preceding one despite display: block. Figure out why and remove margin.  -->
  <div id="chart-container" style="margin-top:25px;">
    <!--.attr('viewBox','0 0 '+Math.min(width, height)+' '+Math.min(width, height))-->
    <svg width="100%" height="100%" viewBox="0 0 294 294" preserveAspectRatio="xMinYMin" ref="graph-svg">
      <rect width="1385" height="294" style="fill: none; pointer-events: all;"></rect>
      <g>
        <!--     .append("g").attr("transform", "translate(" + (margin.left + width/2) + "," + margin.top + ")") -->
        <g ref="inner-graph-svg">
           <path class="link" v-for="l in displayLinks" :d="l.d" :key="l.source.uuid +'->' + l.target.uuid">
           </path>
             <x-svg-node v-for="n in displayNodes" :node="n" :key="n.uuid"
                         v-on:collapse-node="toggleCollapseChildren(n.uuid)"></x-svg-node>
        </g>
      </g>
    </svg>
  </div>
  <div style="visibility: collapse; overflow: hidden;">
    <!-- This is very gross, but here the nodes that will be put on the graph are rendered invisibly or order
      for the browser to calculate their intrinsic size. Each node's size is then passed to the graph layout
      algorithm before the actual graph is rendered.-->
    <!-- need inline-block display per node to get each node's intrinsic width (i.e. don't want it to fill parent). -->
    <div v-for="n in defaultHeightWidthNodes" :key="n.uuid"
         style="display: inline-block; position: absolute; top: 0px;">
      <x-node :emitDimensions="true" :node="n" v-on:node-dimensions="updateNodeDimensions($event)"></x-node>
    </div>
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
import {flatGraphToTree} from '../parse_rec'
import XNode from './XNode'
import {flextree} from 'd3-flextree'

// var margin = {top: 20, right: 120, bottom: 20, left: 120};
//var width = 3000 - margin.right - margin.left;
//var height = 1000 - margin.top - margin.bottom;

// TODO: Needs to be dynamic in case lots of flame data,
let nodeWidth = 250;
let nodeHeight = 100;

let d3Tree = d3.layout.tree()
//    .size([height, width])
  .separation(function(a, b) {return a.parent === b.parent ? 1.5 : 2;})
	.nodeSize([nodeWidth, nodeHeight]);

// This calculates the layout (x, y per node) with dynamic node sizes.
const flextreeLayout = flextree({spacing: 75});

export default {
  name: 'XGraph',
  components: {XSvgNode, XNode},
  props: {
    nodesByUuid: {}
  },
  data () {
    return {
      hidden_node_ids: [],
      // very unfortunate we need to track this manually. TODO: look for a better way.
      dimensions_by_uuid: {},
    };
  },
  computed: {
    root () {
      // TODO: delay view creation until populated.
      if (_.isEmpty(this.nodesByUuid)) {
        return null;
      }
      return flatGraphToTree(this.nodesByUuid)
    },
    defaultHeightWidthNodes() {
      let nodes = _.values(this.nodesByUuid)
      nodes.forEach((n) => {n.width = 'auto'; n.height = 'auto'})
      return nodes;
    },
    intrinsicHeightWidthTree() {
        if (!this.root) {
          return {}
        }
        let all_uuids = _.keys(this.nodesByUuid)
        // only actually do layout if we have all the dimensions of child nodes.
        if (_.difference(all_uuids, _.keys(this.dimensions_by_uuid)).length === 0) {
          let newRoot = _.cloneDeep(this.root)
          this.invokePerNode(newRoot, (node) => {
            node.width = this.dimensions_by_uuid[node.uuid].width
            node.height = this.dimensions_by_uuid[node.uuid].height
          })
          return newRoot
        }
        return {}
    },
    fullyLaidOutNodes () {
      if (!_.isEmpty(this.intrinsicHeightWidthTree)){
        // Need to clone to trigger downstream updates (no deep watches).
        let newRootForLayout = _.cloneDeep(this.intrinsicHeightWidthTree)
        // TODO: change size accessor.
        this.invokePerNode(newRootForLayout, (node) => node.size = [node.width, node.height])
        let laidOutTree = flextreeLayout.hierarchy(newRootForLayout)
        flextreeLayout(laidOutTree)

        // This is a lot of cloning. Make sure they're all necessary.
        let resultNodes = _.cloneDeep(this.nodesByUuid)

        laidOutTree.each(laidOutNode => {
          resultNodes[laidOutNode.data.uuid].x = laidOutNode.left
          // Seperate each node by some fixed amount (.e.g 50).
          resultNodes[laidOutNode.data.uuid].y = laidOutNode.top + laidOutNode.depth * 50
          resultNodes[laidOutNode.data.uuid].width = laidOutNode.xSize
          resultNodes[laidOutNode.data.uuid].height = laidOutNode.ySize
        })
        return _.values(resultNodes);
      }
      return []
    },
    displayNodes () {
      return _.filter(this.fullyLaidOutNodes, n => !_.includes(this.hidden_node_ids, n.uuid))
    },
    // TODO: move to separate component.
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
        let source_width = _.isNumber(l.source.width) ? l.source.width : nodeWidth
        l['d'] = "M" + (l.source.x + source_width/2) + ' ' + (l.source.y + source_height)
            + " v " + (l.target.y - l.source.y - source_height)/2
            + " h" + ((l.target.x + l.target.width/2) - (l.source.x + l.source.width/2))
            + " v " + (l.target.y - l.source.y)/2
      })
      return links
    },
    uid () {
      let nodeWithUid =  _.find(_.values(this.nodesByUuid), 'firex_bound_args.uid')
      if (nodeWithUid) {
        return nodeWithUid.firex_bound_args.uid
      }
      return ''
    }
  },
  mounted () {
    let zoom = d3.behavior.zoom()
                          .scaleExtent([0.01, 1])
                          .on("zoom", this.zoomed);
    d3.select('div#chart-container svg').call(zoom).on("dblclick.zoom", null);
    // TODO: set initial zoom.
    this.$emit('title', this.uid)
  },
  methods: {
    zoomed () {
      let t1 = "translate(" + d3.event.translate + ")";
      let s1 = "scale(" + d3.event.scale + ")";

      this.$refs['inner-graph-svg'].setAttribute("transform", t1 + s1)
    },
    toggleCollapseChildren(parent_node_id) {
      let descendant_ids = this.getDescendantUuids(parent_node_id);
      if (_.difference(descendant_ids, this.hidden_node_ids).length === 0) {
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
        // Vue doesn't deep watch, so create a new object for every update.
        this.dimensions_by_uuid = _.merge({}, this.dimensions_by_uuid, new_entry)
    },
    // TODO: write in terms of invokePerNode
    getDescendantUuids(node_id) {
      let resultUuids = [];
      let uuidsToCheck = _.map(this.nodesByUuid[node_id]['children'], 'uuid');
      while (uuidsToCheck.length > 0) {
        let nodeUuid = uuidsToCheck.pop();
        if (!_.includes(resultUuids, nodeUuid)) {
          let children_ids = _.map(this.nodesByUuid[nodeUuid]['children'], 'uuid')
          uuidsToCheck = uuidsToCheck.concat(children_ids)
          resultUuids.push(nodeUuid)
        }
      }
      return resultUuids;
    },
    // TODO: move to generic graph utils file.
    invokePerNode(root, fn) {
      let doneUuids = [];
      let nodesToCheck = [root]
      while (nodesToCheck.length > 0) {
        let node = nodesToCheck.pop()
        // Avoid loops in graph.
        if (!_.includes(doneUuids, node.uuid)) {
          doneUuids.push(node.uuid)
          fn(node)
          nodesToCheck = nodesToCheck.concat(node.children)
        }
      }
    },
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
