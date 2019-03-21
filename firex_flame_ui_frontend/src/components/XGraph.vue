<template>
  <div style="width: 100%; height: 100%; overflow: hidden;">
    <div v-if="hiddenNodesCount > 0" class="user-message" style="background: lightblue; ">
      {{hiddenNodesCount}} tasks are hidden
      <a href="#" @click.prevent="hiddenNodeIds = []"> (Show All)</a>
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
          <g :transform="svgGraphTransform">
            <x-link :nodesByUuid="displayNodesByUuid"></x-link>
            <x-svg-node v-for="n in displayNodesByUuid" :node="n" :key="n.uuid"
                        v-on:collapse-node="toggleCollapseChildren(n.uuid)"></x-svg-node>
            <path :d="'M' + (-transform.x / transform.scale) + ' ' + (30) + ' h' + (-lineLength)"
                  style="stroke: #000;stroke-width: 10px;"></path>
          </g>
        </g>
      </svg>
    </div>
    <!-- TODO: FIND A BETTER WAY! visiblity:collapse prevents table height from being calculated, so instead
    draw everything at z-index=-1000 and make sure the SVG & header cover these nodes.-->
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
import XLink from './XLinks'
import {eventHub, nodesWithAncestorOrDescendantFailure,
  calculateNodesPositionByUuid, flatGraphToTree} from '../utils'

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
      hiddenNodeIds: nodesWithAncestorOrDescendantFailure(this.nodesByUuid),
      // very unfortunate we need to track this manually. TODO: look for a better way.
      dimensionsByUuid: {},
      zoom: zoom,
      // TODO: this is gross. This state isn't necessary. Split this component in to two: one that does
      //  gross per-node intrinsic size calculation, and another that always has nodes with full rect defined.
      isFirstLoad: true,
      lineLength: 0,
      transform: {
        x: 0, y: 0, scale: 1,
      },
    }
  },
  computed: {
    defaultHeightWidthNodes () {
      let nodes = _.values(_.cloneDeep(this.nodesByUuid))
      nodes.forEach((n) => { n.width = 'auto'; n.height = 'auto' })
      return nodes
    },
    intrinsicDimensionNodesByUuid () {
      let allUuids = _.keys(this.nodesByUuid)
      // only actually do layout if we have all the dimensions of child nodes.
      if (_.difference(allUuids, _.keys(this.dimensionsByUuid)).length === 0) {
        let newNodesByUuid = _.cloneDeep(this.nodesByUuid)
        _.each(newNodesByUuid, (node) => {
          node.width = this.dimensionsByUuid[node.uuid].width
          node.height = this.dimensionsByUuid[node.uuid].height
        })
        return newNodesByUuid
      }
      return []
    },
    onlyVisibleIntrinsicDimensionNodesByUuid () {
      return _.omit(this.intrinsicDimensionNodesByUuid, this.hiddenNodeIds)
    },
    graphAsTree () {
      return flatGraphToTree(this.onlyVisibleIntrinsicDimensionNodesByUuid)
    },
    fullyLaidOutNodesByUuid () {
      if (!_.isEmpty(this.intrinsicDimensionNodesByUuid)) {
        let positionByUuid = calculateNodesPositionByUuid(this.graphAsTree)

        // This is a lot of cloning. Make sure they're all necessary.
        let resultNodesByUuid = _.cloneDeep(this.onlyVisibleIntrinsicDimensionNodesByUuid)
        _.each(resultNodesByUuid, n => {
          n.x = positionByUuid[n.uuid].x
          n.y = positionByUuid[n.uuid].y
          n.width = this.onlyVisibleIntrinsicDimensionNodesByUuid[n.uuid].width
          n.height = this.onlyVisibleIntrinsicDimensionNodesByUuid[n.uuid].height
        })
        return resultNodesByUuid
      }
      return {}
    },
    displayNodesByUuid () {
      // return _.filter(this.fullyLaidOutNodesByUuid, n => !_.includes(this.hiddenNodeIds, n.uuid))
      return this.fullyLaidOutNodesByUuid
    },
    visibleExtent () {
      return {
        top: _.min(_.map(_.values(this.displayNodesByUuid), 'y')),
        left: _.min(_.map(_.values(this.displayNodesByUuid), 'x')),
        right: _.max(_.map(_.values(this.displayNodesByUuid), n => n.x + n.width)),
        bottom: _.max(_.map(_.values(this.displayNodesByUuid), n => n.y + n.height)),
      }
    },
    hiddenNodesCount () {
      return this.hiddenNodeIds.length
    },
    hasFailures () {
      return _.some(_.values(this.nodesByUuid), {'state': 'task-failed'})
    },
    svgGraphTransform () {
      return 'translate(' + _.join([this.transform.x, this.transform.y], ',') + ')' +
        'scale(' + this.transform.scale + ')'
    },
  },
  created () {
    eventHub.$on('center', this.center)

    // TODO: clean up child route support communication by moving it to route definition.
    let supportedParentButtons = ['support-list-link', 'support-center', 'support-help-link', 'support-watch']
    supportedParentButtons.forEach(e => { eventHub.$emit(e) })
  },
  mounted () {
    d3.select('div#chart-container svg').call(this.zoom).on('dblclick.zoom', null)
  },
  methods: {
    zoomed () {
      this.setTransform(d3.event.translate, d3.event.scale)
    },
    setTransformUpdateZoom (translate, scale) {
      // MUST MAINTAIN ZOOM'S INTERNAL STATE! Otherwise, subsequent pan/zooms are inconsistent with current position.
      if (!_.isNil(scale)) {
        this.zoom.scale(scale)
      }
      if (!_.isNil(translate)) {
        this.zoom.translate(translate)
      }
      this.setTransform(translate, scale)
    },
    setTransform (translate, scale) {
      this.transform = {x: translate[0], y: translate[1], scale: scale}
    },
    getCurrentRelPos (nodeUuid) {
      let laidOutNode = this.fullyLaidOutNodesByUuid[nodeUuid]
      return {
        x: laidOutNode.x + this.transform.x,
        y: laidOutNode.y + this.transform.y,
      }
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
        let translate = [ -xTranslate, -yTranslate ]
        this.setTransformUpdateZoom(translate, scale)
      }
    },
    toggleCollapseChildren (parentNodeId) {
      let initialRelPos = this.getCurrentRelPos(parentNodeId)

      let descendantIds = this.getDescendantUuids(parentNodeId)
      if (_.difference(descendantIds, this.hiddenNodeIds).length === 0) {
        // These IDs are currently hidden, so remove them.
        this.hiddenNodeIds = _.difference(this.hiddenNodeIds, descendantIds)
      } else {
        this.hiddenNodeIds = this.hiddenNodeIds.concat(descendantIds)
      }

      // Since we're changing the nodes being displayed, the layout might drastically change. Maintain the
      // position of the node whose decendants have been added/removed so that the user remains oriented.
      this.$nextTick(() => {
        let nextRelPos = this.getCurrentRelPos(parentNodeId)
        let xShift = (initialRelPos.x - nextRelPos.x) * this.transform.scale
        let finalTranslateX = this.transform.x + xShift
        // Since we're viewing hierarchies, the y position shouldn't ever change when children are collapsed.
        let translated = [finalTranslateX, this.transform.y]
        this.setTransformUpdateZoom(translated, this.transform.scale)
      })
    },
    updateNodeDimensions (event) {
      let newEntry = {}
      newEntry[event.uuid] = {width: event.width, height: event.height}
      // Vue doesn't deep watch, so create a new object for every update.
      this.dimensionsByUuid = _.merge({}, this.dimensionsByUuid, newEntry)
    },
    // TODO: write in terms of invokePerNode
    getDescendantUuids (nodeId) {
      let resultUuids = []
      let uuidsToCheck = _.clone(this.nodesByUuid[nodeId]['children_uuids'])
      while (uuidsToCheck.length > 0) {
        let nodeUuid = uuidsToCheck.pop()
        if (!_.includes(resultUuids, nodeUuid)) {
          let childrenIds = this.nodesByUuid[nodeUuid]['children_uuids']
          uuidsToCheck = uuidsToCheck.concat(childrenIds)
          resultUuids.push(nodeUuid)
        }
      }
      return resultUuids
    },
    hideSucessPaths () {
      this.hiddenNodeIds = this.hiddenNodeIds.concat(nodesWithAncestorOrDescendantFailure(this.nodesByUuid))
    },
  },
  watch: {
    fullyLaidOutNodesByUuid: function (_, __) {
      // This is somewhat gross. Maybe there should be another component that does the SVG rendering that always
      // has dimensions populated. It could then center on mounted or similar.
      if (this.isFirstLoad) {
        this.isFirstLoad = false
        this.center()
      }
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
