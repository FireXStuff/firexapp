<template>
  <div style="width: 100%; height: 100%; overflow: hidden;">
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
        <g>
          <g :transform="svgGraphTransform">
            <x-link :onlyVisibleIntrinsicDimensionNodesByUuid="onlyVisibleIntrinsicDimensionNodesByUuid"
                    :nodeLayoutsByUuid="nodeLayoutsByUuid"></x-link>

            <x-svg-node v-for="(nodeLayout, uuid) in nodeLayoutsByUuid"
                        :node="nodesByUuid[uuid]"
                        :width="dimensionsByUuid[uuid].width"
                        :height="dimensionsByUuid[uuid].height"
                        :xPosition="nodeLayout.x"
                        :yPosition="nodeLayout.y"
                        :key="uuid"
                        :showUuid="showUuids"
                        v-on:collapse-node="toggleCollapseChildren(uuid)"></x-svg-node>
          </g>
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
                :node="n" width='auto' height='auto'
                v-on:node-dimensions="updateNodeDimensions($event)"></x-node>
      </div>
    </div>
  </div>
</template>

<script>

//  TODO: specify what to import from d3 more precisely (select, zoom).
// TODO: maybe use a more recent version of d3.
import * as d3 from 'd3'
import XSvgNode from './XSvgNode'
import _ from 'lodash'
import XNode from './XNode'
import XLink from './XLinks'
import {eventHub, nodesWithAncestorOrDescendantFailure,
  calculateNodesPositionByUuid} from '../utils'

export default {
  name: 'XGraph',
  components: {XSvgNode, XNode, XLink},
  props: {
    nodesByUuid: {required: true, type: Object},
    firexUid: {required: true, type: String},
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
      // TODO: read & write transform to local storage to save view port.
      transform: this.getLocalStorageTransform(),
      showUuids: false,
    }
  },
  computed: {
    intrinsicNodeDimensionsByUuid () {
      return _.mapValues(this.dimensionsByUuid, (node, uuid) => {
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
    eventHub.$on('toggle-uuids', this.toggleShowUuids)

    // TODO: clean up child route support communication by moving it to route definition.
    let supportedParentButtons = ['support-list-link', 'support-center', 'support-help-link', 'support-watch',
      'support-add']
    supportedParentButtons.forEach(e => { eventHub.$emit(e) })
  },
  mounted () {
    d3.select('div#chart-container svg').call(this.zoom).on('dblclick.zoom', null)
  },
  methods: {
    zoomed () {
      this.setTransform({x: d3.event.translate[0], y: d3.event.translate[1], scale: d3.event.scale})
    },
    setTransformUpdateZoom (transform) {
      // MUST MAINTAIN ZOOM'S INTERNAL STATE! Otherwise, subsequent pan/zooms are inconsistent with current position.
      if (!_.isNil(transform.scale)) {
        this.zoom.scale(transform.scale)
      }
      this.zoom.translate([transform.x, transform.y])
      this.setTransform(transform)
    },
    setTransform (transform) {
      this.transform = transform
      this.setLocalStorageTransform(this.transform)
    },
    getCurrentRelPos (nodeUuid) {
      let laidOutNode = this.nodeLayoutsByUuid[nodeUuid]
      return {
        x: laidOutNode.x + this.transform.x,
        y: laidOutNode.y + this.transform.y,
      }
    },
    toggleShowUuids () {
      this.showUuids = !this.showUuids
    },
    center () {
      this.setTransformUpdateZoom(this.getCenterTransform())
    },
    // TODO: externalize meat of this function as getCenterTransform(innerRect, outerRect).
    getCenterTransform () {
      // Not available during re-render.
      if (this.$refs['graph-svg']) {
        let boundingRect = this.$refs['graph-svg'].getBoundingClientRect()

        // TODO: padding as percentage of available area.
        let verticalPadding = 200
        let visibleExtentWidth = this.nonHiddenNodesExtent.right - this.nonHiddenNodesExtent.left
        let visibleExtentHeight = this.nonHiddenNodesExtent.bottom - this.nonHiddenNodesExtent.top + verticalPadding
        let xScale = boundingRect.width / visibleExtentWidth
        let yScale = boundingRect.height / visibleExtentHeight
        let scale = _.min([xScale, yScale]) // TODO: include absolute scale min.

        let scaledExtendWidth = visibleExtentWidth * scale
        let xTranslate = this.nonHiddenNodesExtent.left * scale

        // Center the graph based on (scaled) extra horizontal or vertical space.
        if (Math.round(boundingRect.width) > Math.round(scaledExtendWidth)) {
          let remainingHorizontal = boundingRect.width - scaledExtendWidth
          xTranslate = xTranslate - remainingHorizontal / 2
        }

        let scaledExtendHeight = visibleExtentHeight * scale
        let yTranslate = (this.nonHiddenNodesExtent.top - verticalPadding / 2) * scale
        if (Math.round(boundingRect.height) > Math.round(scaledExtendHeight)) {
          let remainingVertical = boundingRect.height - scaledExtendHeight
          yTranslate = yTranslate - remainingVertical / 2
        }
        return {x: -xTranslate, y: -yTranslate, scale: scale}
      }
      return {x: 0, y: 0, scale: 1}
    },
    toggleCollapseChildren (parentNodeId) {
      let initialRelPos = this.getCurrentRelPos(parentNodeId)

      let descendantIds = this.getDescendantUuids(parentNodeId)
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
        this.setTransformUpdateZoom({x: finalTranslateX, y: this.transform.y, scale: this.transform.scale})
      })
    },
    updateNodeDimensions (event) {
      let newEntry = {}
      newEntry[event.uuid] = {width: event.width, height: event.height}
      // Vue doesn't deep watch, so create a new object for every update.
      // this.$set(this.dimensionsByUuid, event.uuid, newEntry)
      this.dimensionsByUuid = _.merge({}, this.dimensionsByUuid, newEntry)
    },
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
    isTransformValid (transform) {
      let vals = [transform.x, transform.y, transform.scale]
      return _.every(_.map(vals, v => !_.isNaN(v) && _.isNumber(v)))
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
    nodeLayoutsByUuid (_, __) {
      // This is somewhat gross. Maybe there should be another component that does the SVG rendering that always
      // has dimensions populated. It could then center on mounted or similar.

      // if (this.isFirstLoad && !this.isTransformValid(this.getLocalStorageTransform())) {
      //   this.isFirstLoad = false
      // }
      this.setTransformUpdateZoom(this.getLocalStorageTransform())
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
