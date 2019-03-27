<template>
  <div style="width: 100%; height: 100%; overflow: hidden;" tabindex="1"
       @keydown.c="center">
    <div v-if="hiddenNodeIds.length > 0" class="user-message" style="background: lightblue; ">
      {{hiddenNodeIds.length}} tasks are hidden
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
          <g :transform="svgGraphTransform">
            <x-link :onlyVisibleIntrinsicDimensionNodesByUuid="onlyVisibleIntrinsicDimensionNodesByUuid"
                    :nodeLayoutsByUuid="nodeLayoutsByUuid"></x-link>

            <x-svg-node v-for="(nodeLayout, uuid) in nodeLayoutsByUuid"
                        :node="nodesByUuid[uuid]"
                        :dimensions="dimensionsByUuid[uuid]"
                        :position="nodeLayout"
                        :key="uuid"
                        :showUuid="showUuids"
                        :liveUpdate="liveUpdate"
                        :opacity="!focusedNodeUuid || focusedNodeUuid === uuid ? 1: 0.3"
                        v-on:collapse-node="toggleCollapseChildren(uuid)"></x-svg-node>
        </g>
      </svg>
    </div>
    <!-- TODO: FIND A BETTER WAY! visiblity:collapse prevents table height from being calculated, so instead
    draw everything at z-index=-1000 and make sure the SVG & header cover these nodes.-->
    <div style="overflow: hidden;">
      <!-- This is very gross, but the nodes that will be put on the graph are rendered invisibly in order
        for the browser to calculate their intrinsic size. Each node's size is then passed to the graph layout
        algorithm before the actual graph is rendered.-->

      <!--
        Need inline-block display per node to get each node's intrinsic width (i.e. don't want it to force fill parent).
      -->
      <div v-for="n in nodesByUuid" :key="n.uuid"
           style="display: inline-block; position: absolute; top: 0;  z-index: -1000;">
        <x-node :emitDimensions="true" :allowCollapse="false" :showUuid="showUuids"
                :node="n"
                v-on:node-dimensions="updateNodeDimensions($event)"></x-node>
      </div>
    </div>
  </div>
</template>

<script>

import * as d3 from 'd3'
import XSvgNode from './XSvgNode'
import _ from 'lodash'
import XNode from './XNode'
import XLink from './XLinks'
import {eventHub, nodesWithAncestorOrDescendantFailure,
  calculateNodesPositionByUuid, getCenteringTransform, getDescendantUuids} from '../utils'

let scaleBounds = {max: 1.3, min: 0.01}

export default {
  name: 'XGraph',
  components: {XSvgNode, XNode, XLink},
  props: {
    nodesByUuid: {required: true, type: Object},
    firexUid: {required: true, type: String},
    showUuids: {default: false, type: Boolean},
    liveUpdate: {required: true, type: Boolean},
  },
  data () {
    let zoom = d3.zoom()
      .scaleExtent([scaleBounds.min, scaleBounds.max])
      .clickDistance(4) // Only consider a click a pan if it moves a couple pixels, since this blocks event prop.
      .on('zoom', this.zoomed)
    return {
      // Default to hiding paths that don't include a failure by default.
      hiddenNodeIds: nodesWithAncestorOrDescendantFailure(this.nodesByUuid),
      // very unfortunate we need to track this manually. TODO: look for a better way.
      dimensionsByUuid: {},
      zoom: zoom,
      transform: {x: 0, y: 0, scale: 1},
      focusedNodeUuid: null,
    }
  },
  computed: {
    allUuids () {
      return _.keys(this.nodesByUuid)
    },
    intrinsicNodeDimensionsByUuid () {
      // Note that since dimensionsByUuid is never deleted from, we need to filter by allUuids
      // in case nodesByUuid changes. this is a bit gross, maybe use a watcher instead.
      return _.mapValues(_.pick(this.dimensionsByUuid, this.allUuids), (node, uuid) => {
        return {
          uuid: uuid,
          width: this.dimensionsByUuid[uuid].width,
          height: this.dimensionsByUuid[uuid].height,
          parent_id: this.nodesByUuid[uuid].parent_id,
        }
      })
    },
    onlyVisibleIntrinsicDimensionNodesByUuid () {
      return _.omit(this.intrinsicNodeDimensionsByUuid, this.hiddenNodeIds)
    },
    nodeLayoutsByUuid () {
      if (!_.isEmpty(this.intrinsicNodeDimensionsByUuid)) {
        return calculateNodesPositionByUuid(this.onlyVisibleIntrinsicDimensionNodesByUuid)
      }
      return {}
    },
    nonHiddenNodesExtent () {
      return {
        top: _.min(_.map(this.nodeLayoutsByUuid, 'y')),
        left: _.min(_.map(this.nodeLayoutsByUuid, 'x')),
        // Note that if we've done the layout for a given UUID, we necessarily have the node dimensions.
        right: _.max(_.map(this.nodeLayoutsByUuid, (n, uuid) => n.x + this.dimensionsByUuid[uuid].width)),
        bottom: _.max(_.map(this.nodeLayoutsByUuid, (n, uuid) => n.y + this.dimensionsByUuid[uuid].height)),
      }
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
    eventHub.$on('node-focus', this.focusOnNode)
  },
  mounted () {
    d3.select('div#chart-container svg').call(this.zoom).on('dblclick.zoom', null)
    this.updateTransformViaZoom(this.getLocalStorageTransform())
  },
  methods: {
    zoomed () {
      // Null source events mean programatic zoom. We don't want to clear for programatic zooms.
      if (d3.event.sourceEvent !== null) {
        // Clear focus node on non-programatic pan/zoom.
        this.focusedNodeUuid = null
      }
      this.transform = {x: d3.event.transform.x, y: d3.event.transform.y, scale: d3.event.transform.k}
      this.setLocalStorageTransform(this.transform)
    },
    updateTransformViaZoom (transform) {
      // MUST MAINTAIN ZOOM'S INTERNAL STATE! Otherwise, subsequent pan/zooms are inconsistent with current position.
      let d3Transform = d3.zoomIdentity.translate(transform.x, transform.y).scale(transform.scale)
      // This call will create a d3.event and pipe it through, just like manual pan/zooms.
      d3.select('div#chart-container svg').call(this.zoom.transform, d3Transform)
    },
    getCurrentRelPos (nodeUuid) {
      let laidOutNode = this.nodeLayoutsByUuid[nodeUuid]
      return {
        x: laidOutNode.x + this.transform.x,
        y: laidOutNode.y + this.transform.y,
      }
    },
    center () {
      this.updateTransformViaZoom(this.getCenterTransform())
    },
    focusOnNode (uuid) {
      this.focusedNodeUuid = uuid
      this.updateTransformViaZoom(this.getCenterOnNodeTransform(uuid))
    },
    getCenterTransform () {
      // Not available during re-render.
      if (this.$refs['graph-svg']) {
        let boundingRect = this.$refs['graph-svg'].getBoundingClientRect()
        // Visible extent might not be initialized before first graph draw, so fall back here.
        if (_.every(this.nonHiddenNodesExtent, _.negate(_.isNil))) {
          return getCenteringTransform(this.nonHiddenNodesExtent, boundingRect, scaleBounds, 200)
        }
      }
      return {x: 0, y: 0, scale: 1}
    },
    getCenterOnNodeTransform (uuid) {
      // Not available during re-render.
      if (this.$refs['graph-svg']) {
        let boundingRect = this.$refs['graph-svg'].getBoundingClientRect()
        let nodeRect = {
          left: this.nodeLayoutsByUuid[uuid].x,
          right: this.nodeLayoutsByUuid[uuid].x + this.dimensionsByUuid[uuid].width,
          top: this.nodeLayoutsByUuid[uuid].y,
          bottom: this.nodeLayoutsByUuid[uuid].y + this.dimensionsByUuid[uuid].height,
        }
        return getCenteringTransform(nodeRect, boundingRect, scaleBounds, 0)
      }
      return {x: 0, y: 0, scale: 1}
    },
    toggleCollapseChildren (parentNodeId) {
      let initialRelPos = this.getCurrentRelPos(parentNodeId)

      let descendantIds = getDescendantUuids(parentNodeId, this.nodesByUuid)
      if (_.difference(descendantIds, this.hiddenNodeIds).length === 0) {
        // These UUIDs are currently hidden, so remove them.
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
        this.updateTransformViaZoom({x: finalTranslateX, y: this.transform.y, scale: this.transform.scale})
      })
    },
    updateNodeDimensions (event) {
      let dimensions = {width: event.width, height: event.height}
      // Vue doesn't deep watch, so create a new object for every update.
      this.$set(this.dimensionsByUuid, event.uuid, dimensions)
    },
    hideSucessPaths () {
      this.hiddenNodeIds = this.hiddenNodeIds.concat(nodesWithAncestorOrDescendantFailure(this.nodesByUuid))
    },
    isTransformValid (transform) {
      let vals = [transform.x, transform.y, transform.scale]
      return _.every(_.map(vals, v => !_.isNil(v) && _.isNumber(v)))
    },
    setLocalStorageTransform (newTransform) {
      localStorage[this.firexUid] = JSON.stringify(newTransform)
    },
    getLocalStorageTransform () {
      try {
        let storedTransform = JSON.parse(localStorage[this.firexUid])
        if (this.isTransformValid(storedTransform)) {
          return storedTransform
        }
      } catch (e) {
        localStorage.removeItem(this.firexUid)
      }
      return this.getCenterTransform()
    },
  },
  watch: {
    firexUid () {
      // TODO: this should be on firexRunMetadata change (keys on UID).
      this.updateTransformViaZoom(this.getLocalStorageTransform())
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
