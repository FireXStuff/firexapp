<template>
  <div style="width: 100%; height: 100%; overflow: hidden;">
    <div v-if="hiddenNodesCount > 0" class="user-message" style="background: lightblue; ">
      {{hiddenNodesCount}} tasks are hidden
      <a href="#" @click.prevent="hidden_node_ids = []"> (Show All)</a>
    </div>
    <div v-else-if="hasFailures" class="user-message" style="background: orange">
      Some tasks have failed.
      <a href="#" @click.prevent="hideSucessPaths()">
        Show only failed paths
      </a>
    </div>
    <div id="chart-container" style="width: 100%; height: 100%" ref="graph-svg">
      <svg width="100%" height="100%" preserveAspectRatio="xMinYMin"  style="background-color: white;">
        <g>
          <!--     .append("g").attr("transform", "translate(" + (margin.left + width/2) + "," + margin.top + ")") -->
          <g ref="inner-graph-svg">
            <x-link :nodes="displayNodes"></x-link>
            <x-svg-node v-for="n in displayNodes" :node="n" :key="n.uuid"
                        v-on:collapse-node="toggleCollapseChildren(n.uuid)"></x-svg-node>
          </g>
        </g>
      </svg>
    </div>
    <!-- TODO: FIND A BETTER WAY! visiblity:collapse prevents table height from being calculatd, so instead
    draw everything at z-index=-1000 and make sure the SVG covers it.-->
    <div style="/*visibility: collapse;*/ overflow: hidden;">
      <!-- This is very gross, but the nodes that will be put on the graph are rendered invisibly in order
        for the browser to calculate their intrinsic size. Each node's size is then passed to the graph layout
        algorithm before the actual graph is rendered.-->

      <!--
        Need inline-block display per node to get each node's intrinsic width (i.e. don't want it to force fill parent).
      -->
      <div v-for="n in defaultHeightWidthNodes" :key="n.uuid"
           style="display: inline-block; position: absolute; top: 0;  z-index: -1000;">
        <x-node :emitDimensions="true" :allowCollapse="false"
                :node="n" v-on:node-dimensions="updateNodeDimensions($event)"></x-node>
      </div>
    </div>
  </div>
</template>

<script>

//  TODO: specify what to import from d3 more precisely (select, zoom).
// TODO: use a more recent version of d3 (find where layout was moved to).
import * as d3 from 'd3'
import XSvgNode from './XSvgNode'
import _ from 'lodash'
import XNode from './XNode'
import {flextree} from 'd3-flextree'
import XLink from './XLinks'
import {invokePerNode, flatGraphToTree, eventHub, nodesWithAncestorOrDescendantFailure} from '../utils'

// This calculates the layout (x, y per node) with dynamic node sizes.
const flextreeLayout = flextree({spacing: 75})

export default {
  name: 'XGraph',
  components: {XSvgNode, XNode, XLink},
  props: {
    nodesByUuid: {required: true, type: Object},
  },
  data () {
    let zoom = d3.behavior.zoom()
      .scaleExtent([0.01, 1])
      .on('zoom', this.zoomed)
    return {
      // Default to hiding paths that don't include a failure by default.
      hidden_node_ids: nodesWithAncestorOrDescendantFailure(this.nodesByUuid),
      // very unfortunate we need to track this manually. TODO: look for a better way.
      dimensions_by_uuid: {},
      zoom: zoom,
      // TODO: this is gross. This state isn't necessary. Split this component in to two: one that does
      //  gross per-node intrinsic size calculation, and another that always has nodes with full rect defined.
      isFirstLoad: true,
    }
  },
  computed: {
    root () {
      return flatGraphToTree(this.nodesByUuid)
    },
    defaultHeightWidthNodes () {
      let nodes = _.values(this.nodesByUuid)
      nodes.forEach((n) => { n.width = 'auto'; n.height = 'auto' })
      return nodes
    },
    intrinsicHeightWidthTree () {
      let allUuids = _.keys(this.nodesByUuid)
      // only actually do layout if we have all the dimensions of child nodes.
      if (_.difference(allUuids, _.keys(this.dimensions_by_uuid)).length === 0) {
        let newRoot = _.cloneDeep(this.root)
        invokePerNode(newRoot, (node) => {
          node.width = this.dimensions_by_uuid[node.uuid].width
          node.height = this.dimensions_by_uuid[node.uuid].height
        })
        return newRoot
      }
      return {}
    },
    fullyLaidOutNodes () {
      if (!_.isEmpty(this.intrinsicHeightWidthTree)) {
        // Need to clone to trigger downstream updates (no deep watches).
        let newRootForLayout = _.cloneDeep(this.intrinsicHeightWidthTree)
        // TODO: change size accessor.
        invokePerNode(newRootForLayout, (node) => { node.size = [node.width, node.height] })
        let laidOutTree = flextreeLayout.hierarchy(newRootForLayout)
        flextreeLayout(laidOutTree)

        // This is a lot of cloning. Make sure they're all necessary.
        let resultNodesByUuid = _.cloneDeep(this.nodesByUuid)

        // TODO: consider if we want fixed-depth despite variable height per-node. Create a list where the index is
        //  the depth and the
        // value is the sum of each previous depth's tallest node.
        // let maxHeightByDepth = _.mapValues(_.groupBy(laidOutTree.nodes, 'depth'),
        //   nodes => _.max(_.map(nodes, 'size.1')))
        // let depthHeightArray = _.toArray(maxHeightByDepth) // orders based on input object keys.
        // let sumDepthHeightArray = [0].concat(_.map(depthHeightArray, (v, i, c) => _.sum(_.take(c, i)) + v))
        // console.log(maxHeightByDepth)
        // console.log(sumDepthHeightArray)
        // Since the flex-layout library messes with data, we'll just copy out the few parameters we need.
        laidOutTree.each(laidOutNode => {
          resultNodesByUuid[laidOutNode.data.uuid].x = laidOutNode.left
          // Separate each node by some fixed amount (.e.g 50).

          resultNodesByUuid[laidOutNode.data.uuid].y = laidOutNode.top + laidOutNode.depth * 50
          resultNodesByUuid[laidOutNode.data.uuid].width = laidOutNode.size[0]
          resultNodesByUuid[laidOutNode.data.uuid].height = laidOutNode.size[1]
        })
        // This is gross -- get rid of side effect in computed property.
        if (this.isFirstLoad) {
          this.isFirstLoad = false
          this.center()
        }
        return _.values(resultNodesByUuid)
      }
      return []
    },
    displayNodes () {
      return _.filter(this.fullyLaidOutNodes, n => !_.includes(this.hidden_node_ids, n.uuid))
    },
    visibleExtent () {
      return {
        top: _.min(_.map(this.displayNodes, 'y')),
        left: _.min(_.map(this.displayNodes, 'x')),
        right: _.max(_.map(this.displayNodes, n => n.x + n.width)),
        bottom: _.max(_.map(this.displayNodes, n => n.y + n.height)),
      }
    },
    hiddenNodesCount () {
      return this.hidden_node_ids.length
    },
    hasFailures () {
      return _.some(_.values(this.nodesByUuid), {'state': 'task-failed'})
    },
  },
  created () {
    eventHub.$on('center', this.center)
  },
  mounted () {
    d3.select('div#chart-container svg').call(this.zoom).on('dblclick.zoom', null)
  },
  methods: {
    zoomed () {
      this.setTransform(d3.event.translate, d3.event.scale)
    },
    setTransform (translate, scale) {
      let transform = ''
      if (translate) {
        transform += 'translate(' + translate + ')'
      }
      if (scale) {
        transform += 'scale(' + scale + ')'
      }
      this.$refs['inner-graph-svg'].setAttribute('transform', transform)
    },
    center () {
      // Not available during re-render.
      if (this.$refs['graph-svg']) {
        let boundingRect = this.$refs['graph-svg'].getBoundingClientRect()

        // TODO: padding as percentage of available area.
        let verticalPadding = 200
        let visibleExtentWidth = this.visibleExtent.right - this.visibleExtent.left
        let visibleExtentHeight = this.visibleExtent.bottom - this.visibleExtent.top + verticalPadding
        let xScale = boundingRect.width / visibleExtentWidth
        let yScale = boundingRect.height / visibleExtentHeight
        let scale = _.min([xScale, yScale])

        let scaledExtendWidth = visibleExtentWidth * scale
        let xTranslate = this.visibleExtent.left * scale
        // Center the graph based on (scaled) extra horizontal or vertical space.
        if (Math.round(boundingRect.width) > Math.round(scaledExtendWidth)) {
          let remainingHorizontal = boundingRect.width - scaledExtendWidth
          xTranslate = xTranslate - remainingHorizontal / 2
        }

        let scaledExtendHeight = visibleExtentHeight * scale
        let yTranslate = (this.visibleExtent.top - verticalPadding / 2) * scale
        if (Math.round(boundingRect.height) > Math.round(scaledExtendHeight)) {
          let remainingVertical = boundingRect.height - scaledExtendHeight
          yTranslate = yTranslate - remainingVertical / 2
        }
        let translate = [ -xTranslate, -(yTranslate) ]

        // MUST MAINTAIN ZOOM'S INTERNAL STATE! Otherwise, subsequent pan/zooms are inconsistent with current position.
        this.zoom.scale(scale)
        this.zoom.translate(translate)
        this.setTransform(_.join(translate, ','), scale)
      }
    },
    toggleCollapseChildren (parentNodeId) {
      let descendantIds = this.getDescendantUuids(parentNodeId)
      if (_.difference(descendantIds, this.hidden_node_ids).length === 0) {
        // These IDs are currently hidden, so remove them.
        this.hidden_node_ids = _.difference(this.hidden_node_ids, descendantIds)
      } else {
        this.hidden_node_ids = this.hidden_node_ids.concat(descendantIds)
      }
    },
    updateNodeDimensions (event) {
      let newEntry = {}
      newEntry[event.uuid] = {width: event.width, height: event.height}
      // Vue doesn't deep watch, so create a new object for every update.
      this.dimensions_by_uuid = _.merge({}, this.dimensions_by_uuid, newEntry)
    },
    // TODO: write in terms of invokePerNode
    getDescendantUuids (nodeId) {
      let resultUuids = []
      let uuidsToCheck = _.map(this.nodesByUuid[nodeId]['children'], 'uuid')
      while (uuidsToCheck.length > 0) {
        let nodeUuid = uuidsToCheck.pop()
        if (!_.includes(resultUuids, nodeUuid)) {
          let childrenIds = _.map(this.nodesByUuid[nodeUuid]['children'], 'uuid')
          uuidsToCheck = uuidsToCheck.concat(childrenIds)
          resultUuids.push(nodeUuid)
        }
      }
      return resultUuids
    },
    hideSucessPaths () {
      this.hidden_node_ids = this.hidden_node_ids.concat(nodesWithAncestorOrDescendantFailure(this.nodesByUuid))
    },
  },
}
</script>

<style scoped>
  * {
    box-sizing: border-box;
  }

  .user-message {
    text-align: center;
    font-size: 18px;
    position: absolute;
    z-index: 3;
    width: 100%;
  }
</style>
