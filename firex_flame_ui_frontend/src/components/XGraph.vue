<template>
  <div style="width: 100%; height: 100%; overflow: hidden;" tabindex="1"
       @keydown.c="center">
    <div v-if="collapsedNodeUuids.length > 0" class="user-message" style="background: lightblue; ">
      {{collapsedNodeUuids.length}} tasks are hidden
      <a href="#" @click.prevent="clearAllCollapseFilters"> (Show All)</a>
    </div>
    <div v-else-if="hasFailures" class="user-message" style="background: orange">
      Some tasks have failed.
      <a href="#" @click.prevent="hideSucessPaths = true">
        Show only failed paths
      </a>
    </div>
    <div id="chart-container" style="width: 100%; height: 100%" ref="graph-svg">
      <svg width="100%" height="100%" preserveAspectRatio="xMinYMin"
           style="background-color: white;">
        <g :transform="svgGraphTransform">
          <x-link
            :uncollapsedIntrinsicDimensionNodesByUuid="uncollapsedIntrinsicDimensionNodesByUuid"
            :nodeLayoutsByUuid="nodeLayoutsByUuid"></x-link>
          <x-svg-task-node v-for="(nodeLayout, uuid) in nodeLayoutsByUuid"
                      :node="nodesByUuid[uuid]"
                      :dimensions="dimensionsByUuid[uuid]"
                      :position="nodeLayout"
                      :key="uuid"
                      :showUuid="showUuids"
                      :liveUpdate="liveUpdate"
                      :isAnyChildCollapsed="isAnyChildCollapsedByUuid[uuid]"
                      :opacity="!focusedNodeUuid || focusedNodeUuid === uuid ? 1: 0.3"
                      v-on:collapse-node="toggleCollapseDescendants(uuid)"></x-svg-task-node>
        </g>
      </svg>
    </div>

    <div style="overflow: hidden;">
      <!-- This is very gross, but the nodes that will be put on the graph are rendered
      invisibly in order for the browser to calculate their intrinsic size. Each node's size is
      then passed to the graph layout algorithm before the actual graph is rendered.-->
      <!--
        Need inline-block display per node to get each node's intrinsic width (i.e. don't want it
        to force fill parent).
      -->
      <div v-for="n in nodesByUuid" :key="n.uuid"
           style="display: inline-block; position: absolute; top: 0; z-index: -1000;">
        <x-task-node
          :node="n"
          :emitDimensions="true" :showUuid="showUuids"
          v-on:node-dimensions="updateTaskNodeDimensions($event)"></x-task-node>
      </div>
    </div>
  </div>
</template>

<script>

import * as d3 from 'd3';
import _ from 'lodash';
import XSvgTaskNode from './nodes/XSvgTaskNode.vue';
import XTaskNode from './nodes/XTaskNode.vue';
import XLink from './XLinks.vue';
import {
  eventHub, nodesInRootLeafPathWithFailureOrInProgress,
  calculateNodesPositionByUuid, getCenteringTransform, getAncestorUuids,
} from '../utils';

const scaleBounds = { max: 1.3, min: 0.01 };

export default {
  name: 'XGraph',
  components: {
    XSvgTaskNode, XLink, XTaskNode,
  },
  props: {
    // TODO: it might be worth making a computed property of just the graph structure
    //    (parent/child relationships),
    //  to avoid re-calcs in some context where only structure (not data content) is relied on.
    nodesByUuid: { required: true, type: Object },
    firexUid: { required: true, type: String },
    showUuids: { default: false, type: Boolean },
    liveUpdate: { required: true, type: Boolean },
  },
  data() {
    const zoom = d3.zoom()
      .scaleExtent([scaleBounds.min, scaleBounds.max])
      // Threshold for when a click is considered a pan, since this blocks event prop.
      .clickDistance(4)
      .on('zoom', this.zoomed);
    return {
      // very unfortunate we need to track this manually. TODO: look for a better way.
      dimensionsByUuid: {},
      zoom,
      transform: { x: 0, y: 0, scale: 1 },
      // When set, fades-out other nodes. Is reset by pan/zoom events.
      focusedNodeUuid: null,
      // Only want to center on first layout, then we'll rely on stored transform.
      isFirstLayout: true,
      // This is just a container for states that have been touched by the user -- it doesn't
      // contain an entry for every node's UUID (there is a computed property for that).
      uiCollapseDescendantStatesByUuid: {},
      // Default to hiding paths that don't include a failure or in progress by default.
      hideSucessPaths: true,
    };
  },
  computed: {
    allUuids() {
      return _.keys(this.nodesByUuid);
    },
    intrinsicNodeDimensionsByUuid() {
      // Note that since dimensionsByUuid is never deleted from, we need to filter by allUuids
      // in case nodesByUuid changes. this is a bit gross, maybe use a watcher instead.
      return _.mapValues(_.pick(this.dimensionsByUuid, this.allUuids), (node, uuid) => ({
        uuid,
        width: this.dimensionsByUuid[uuid].width,
        height: this.dimensionsByUuid[uuid].height,
        parent_id: this.nodesByUuid[uuid].parent_id,
      }));
    },
    uncollapsedIntrinsicDimensionNodesByUuid() {
      return _.omit(this.intrinsicNodeDimensionsByUuid, this.collapsedNodeUuids);
    },
    // TODO: should consider limiting layout recalculations to once per second instead of
    //      being reactive.
    //  This will probably slow down for large graphs (untested).
    nodeLayoutsByUuid() {
      if (!_.isEmpty(this.intrinsicNodeDimensionsByUuid)) {
        return calculateNodesPositionByUuid(this.uncollapsedIntrinsicDimensionNodesByUuid);
      }
      return {};
    },
    nonHiddenNodesExtent() {
      return {
        top: _.min(_.map(this.nodeLayoutsByUuid, 'y')),
        left: _.min(_.map(this.nodeLayoutsByUuid, 'x')),
        // Note that if we've done the layout for a given UUID, we necessarily have the
        // node dimensions.
        right: _.max(_.map(this.nodeLayoutsByUuid,
          (n, uuid) => n.x + this.dimensionsByUuid[uuid].width)),
        bottom: _.max(_.map(this.nodeLayoutsByUuid,
          (n, uuid) => n.y + this.dimensionsByUuid[uuid].height)),
      };
    },
    hasFailures() {
      return _.some(_.values(this.nodesByUuid), { state: 'task-failed' });
    },
    svgGraphTransform() {
      return `translate(${_.join([this.transform.x, this.transform.y], ',')})`
        + `scale(${this.transform.scale})`;
    },
    // uuid -> boolean, true means collapsed, false means uncollapsed (expanded)
    // Combines different data sources for collapse state (task-state, UI collapse/expand clicks)
    // in to the rendered state.
    resolvedCollapseStateByUuid() {
      let runStateShouldShowUuids = [];
      if (this.hideSucessPaths) {
        runStateShouldShowUuids = nodesInRootLeafPathWithFailureOrInProgress(this.nodesByUuid);
      }
      return _.mapValues(this.nodesByUuid, (node) => {
        // TODO: create computed property with just tree structure to avoid re-calcs on tree
        //    content data change.
        const nearestAncestorUiCollapsed = _.find(getAncestorUuids(node, this.nodesByUuid),
          ancestorUuid => _.has(this.uiCollapseDescendantStatesByUuid, ancestorUuid));
        if (!_.isUndefined(nearestAncestorUiCollapsed)) {
          return this.uiCollapseDescendantStatesByUuid[nearestAncestorUiCollapsed];
        }
        // Collapse this node if it isn't in the list of UUIDs to show due to their descendant's
        // state.
        return this.hideSucessPaths && !_.includes(runStateShouldShowUuids, node.uuid);
      });
    },
    collapsedNodeUuids() {
      // TODO: does it make any sense to store uuid -> boolean map? Just store the list of
      //  uuids intially.
      return _.keys(_.pickBy(this.resolvedCollapseStateByUuid));
    },
    // TODO: is this still necessary.
    allUuidsCollapseDescendantByUuid() {
      return _.mapValues(_.keyBy(this.allUuids),
        uuid => _.get(this.uiCollapseDescendantStatesByUuid, uuid, false));
    },
    // Is this really necessary?
    isAnyChildCollapsedByUuid() {
      return _.mapValues(this.nodesByUuid,
        node => _.some(node.children_uuids, cUuid => this.resolvedCollapseStateByUuid[cUuid]));
    },
  },
  created() {
    eventHub.$on('center', this.center);
    eventHub.$on('node-focus', this.focusOnNode);
  },
  mounted() {
    d3.select('div#chart-container svg').call(this.zoom).on('dblclick.zoom', null);
  },
  methods: {
    zoomed() {
      // Null source events mean programatic zoom. We don't want to clear for programatic zooms.
      if (d3.event.sourceEvent !== null) {
        // Clear focus node on non-programatic pan/zoom.
        this.focusedNodeUuid = null;
      }
      this.transform = {
        x: d3.event.transform.x,
        y: d3.event.transform.y,
        scale: d3.event.transform.k,
      };
      this.addLocalStorageData(this.transform);
    },
    updateTransformViaZoom(transform) {
      // MUST MAINTAIN ZOOM'S INTERNAL STATE! Otherwise, subsequent pan/zooms are inconsistent
      // with current position.
      const d3Transform = d3.zoomIdentity.translate(transform.x, transform.y)
        .scale(transform.scale);
      // This call will create a d3.event and pipe it through, just like manual pan/zooms.
      d3.select('div#chart-container svg').call(this.zoom.transform, d3Transform);
    },
    getCurrentRelPos(nodeUuid) {
      const laidOutNode = this.nodeLayoutsByUuid[nodeUuid];
      return {
        x: laidOutNode.x + this.transform.x,
        y: laidOutNode.y + this.transform.y,
      };
    },
    center() {
      this.updateTransformViaZoom(this.getCenterTransform());
    },
    focusOnNode(uuid) {
      this.focusedNodeUuid = uuid;
      this.updateTransformViaZoom(this.getCenterOnNodeTransform(uuid));
    },
    getCenterTransform() {
      // Not available during re-render.
      if (this.$refs['graph-svg']) {
        const boundingRect = this.$refs['graph-svg'].getBoundingClientRect();
        // Visible extent might not be initialized before first graph draw, so fall back here.
        if (_.every(this.nonHiddenNodesExtent, _.negate(_.isNil))) {
          return getCenteringTransform(this.nonHiddenNodesExtent, boundingRect, scaleBounds, 200);
        }
      }
      return { x: 0, y: 0, scale: 1 };
    },
    getCenterOnNodeTransform(uuid) {
      // Not available during re-render.
      if (this.$refs['graph-svg']) {
        const boundingRect = this.$refs['graph-svg'].getBoundingClientRect();
        const nodeRect = {
          left: this.nodeLayoutsByUuid[uuid].x,
          right: this.nodeLayoutsByUuid[uuid].x + this.dimensionsByUuid[uuid].width,
          top: this.nodeLayoutsByUuid[uuid].y,
          bottom: this.nodeLayoutsByUuid[uuid].y + this.dimensionsByUuid[uuid].height,
        };
        return getCenteringTransform(nodeRect, boundingRect, scaleBounds, 0);
      }
      return { x: 0, y: 0, scale: 1 };
    },
    toggleCollapseDescendants(parentNodeId) {
      const initialRelPos = this.getCurrentRelPos(parentNodeId);

      const someChildrenCollapsed = this.isAnyChildCollapsedByUuid[parentNodeId];

      let collapse;
      if (someChildrenCollapsed) {
        collapse = false;
      } else {
        // no children collapsed
        collapse = !_.get(this.uiCollapseDescendantStatesByUuid, parentNodeId, false);
      }

      this.$set(this.uiCollapseDescendantStatesByUuid, parentNodeId, collapse);

      // Since we're changing the nodes being displayed, the layout might drastically change.
      // Maintain the position of the node whose decendants have been added/removed so that
      // the user remains oriented.

      this.$nextTick(() => {
        const nextRelPos = this.getCurrentRelPos(parentNodeId);
        const xShift = (initialRelPos.x - nextRelPos.x) * this.transform.scale;
        const finalTranslateX = this.transform.x + xShift;
        // Since we're viewing hierarchies, the y position shouldn't ever change when
        // children are collapsed.
        this.updateTransformViaZoom({
          x: finalTranslateX,
          y: this.transform.y,
          scale: this.transform.scale,
        });
      });
    },
    updateTaskNodeDimensions(event) {
      // Vue doesn't deep watch, so use $set to maintain reactivity.
      this.$set(this.dimensionsByUuid, event.uuid, _.pick(event, ['width', 'height']));
    },
    isTransformValid(transform) {
      if (_.isNil(transform)) {
        return false;
      }
      const vals = [transform.x, transform.y, transform.scale];
      return _.every(_.map(vals, v => !_.isNil(v) && _.isNumber(v)));
    },
    addLocalStorageData(newData) {
      const storedData = this.readPathsFromLocalStorage('*');
      const toStoreData = _.assign(storedData, newData);
      localStorage[this.firexUid] = JSON.stringify(toStoreData);
    },
    readPathFromLocalStorage(path, def) {
      return _.get(this.readPathsFromLocalStorage([path]), path, def);
    },
    readPathsFromLocalStorage(paths) {
      try {
        const runLocalData = JSON.parse(localStorage[this.firexUid]);
        if (paths === '*') {
          return runLocalData;
        }
        return _.pick(runLocalData, paths);
      } catch (e) {
        // Delete bad persisted state, provide default.
        localStorage.removeItem(this.firexUid);
      }
      return {};
    },
    getLocalStorageTransform() {
      const storedTransform = this.readPathsFromLocalStorage(['x', 'y', 'scale']);
      if (this.isTransformValid(storedTransform)) {
        return storedTransform;
      }
      // Default to the centering transform.
      return this.getCenterTransform();
    },
    clearAllCollapseFilters() {
      this.hideSucessPaths = false;
      this.uiCollapseDescendantStatesByUuid = {};
      // TODO: applyFlameDataCollapseOps = false
    },
  },
  watch: {
    firexUid() {
      this.isFirstLayout = true;
    },
    nodeLayoutsByUuid() {
      if (this.isFirstLayout) {
        this.isFirstLayout = false;
        // TODO: combine localstorage reads, or cache below.
        this.updateTransformViaZoom(this.getLocalStorageTransform());
        this.hideSucessPaths = this.readPathFromLocalStorage('hideSucessPaths', false);
        this.uiCollapseDescendantStatesByUuid = this.readPathFromLocalStorage(
          'uiCollapseDescendantStatesByUuid', {},
        );
      }
    },
    hideSucessPaths() {
      this.addLocalStorageData({ hideSucessPaths: this.hideSucessPaths });
      if (this.hideSucessPaths) {
        // Since hiding success path usually excludes many nodes, we center the graph.
        this.center();
      }
    },
    uiCollapseDescendantStatesByUuid: {
      handler() {
        this.addLocalStorageData({
          uiCollapseDescendantStatesByUuid: this.uiCollapseDescendantStatesByUuid,
        });
      },
      deep: true,
    },
  },
};
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
