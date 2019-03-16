<template>
  <div>
    <!-- TODO: this div can eat in to the preceding one despite display: block. Figure out why and remove margin.  -->
    <div id="chart-container" style="margin-top:25px;" ref="graph-svg">
      <svg width="100%" height="100%" preserveAspectRatio="xMinYMin" >
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
    <div style="visibility: collapse; overflow: hidden;">
      <!-- This is very gross, but the nodes that will be put on the graph are rendered invisibly or order
        for the browser to calculate their intrinsic size. Each node's size is then passed to the graph layout
        algorithm before the actual graph is rendered.-->

      <!-- Need inline-block display per node to get each node's intrinsic width (i.e. don't want it to fill parent).
      -->
      <div v-for="n in defaultHeightWidthNodes" :key="n.uuid"
           style="display: inline-block; position: absolute; top: 0;">
        <x-node :emitDimensions="true" :node="n" v-on:node-dimensions="updateNodeDimensions($event)"></x-node>
      </div>
    </div>
  </div>
</template>

<script>

//  TODO: specify what to import from d3 more precisely.
// TODO: use a more recent version of d3 (find where layout was moved to).
import * as d3 from 'd3'
import XSvgNode from './XSvgNode'
import _ from 'lodash'
import XNode from './XNode'
import {flextree} from 'd3-flextree'
import XLink from './XLinks'
import {invokePerNode, flatGraphToTree, eventHub} from '../utils'

// This calculates the layout (x, y per node) with dynamic node sizes.
const flextreeLayout = flextree({spacing: 75})

export default {
  name: 'XGraph',
  components: {XSvgNode, XNode, XLink},
  props: {
    nodesByUuid: {},
  },
  data () {
    let zoom = d3.behavior.zoom()
      .scaleExtent([0.01, 1])
      .on('zoom', this.zoomed)
    return {
      hidden_node_ids: [],
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

        // Since the flex-layout library messes with data, we'll just copy out the few parameters we need.
        laidOutTree.each(laidOutNode => {
          resultNodesByUuid[laidOutNode.data.uuid].x = laidOutNode.left
          // Separate each node by some fixed amount (.e.g 50).
          resultNodesByUuid[laidOutNode.data.uuid].y = laidOutNode.top + laidOutNode.depth * 50
          resultNodesByUuid[laidOutNode.data.uuid].width = laidOutNode.xSize
          resultNodesByUuid[laidOutNode.data.uuid].height = laidOutNode.ySize
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
      // TODO: figure out why this ref can be undefined even after initial render.
      if (this.$refs['graph-svg']) {
        let boundingRect = this.$refs['graph-svg'].getBoundingClientRect()

        let visibleExtentWidth = this.visibleExtent.right - this.visibleExtent.left
        let visibleExtentHeight = this.visibleExtent.bottom - this.visibleExtent.top
        let xScale = boundingRect.width / visibleExtentWidth
        let yScale = boundingRect.height / visibleExtentHeight
        let scale = _.min([xScale, yScale])

        let translate = [-(this.visibleExtent.left - visibleExtentWidth / 2) * scale, -this.visibleExtent.top * scale]

        // MUST MAINTAIN ZOOM'S INTERNAL STATE! Otherwise, subsequent pan/zooms are inconsistent with current position.
        this.zoom.scale(scale)
        this.zoom.translate(translate)
        this.setTransform(_.join(translate, ','), scale)
      } else {
        console.warn('svg-graph not initialized.')
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
  },
}
</script>

<style scoped>

#chart-container {
    position: absolute;
    top: 40px; /* Header Height */
    bottom: 20px; /* Footer Height */
    width: 100%;
}

</style>
