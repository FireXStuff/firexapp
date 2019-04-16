<template>
  <div style="width: 100%; height: 100%; overflow: hidden;" tabindex="1"
       @keydown.c="center">
    <div v-if="collapsedNodeUuids.length > 0" class="user-message" style="background: lightblue; ">
      {{collapsedNodeUuids.length}} tasks are collapsed
      <a href="#" @click.prevent="clearAllCollapseFilters">(Expand All)</a>
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
            :taskAndCollapseNodeDimensionsByUuid="taskAndCollapseNodeDimensionsByUuid"
            :nodeLayoutsByUuid="nodeLayoutsByUuid"></x-link>
          <template v-for="(nodeLayout, uuid) in nodeLayoutsByUuid">
            <x-svg-task-node v-if="nodesByUuid.hasOwnProperty(uuid)"
                        :node="nodesByUuid[uuid]"
                        :dimensions="dimensionsByUuid[uuid]"
                        :position="nodeLayout"
                        :key="uuid"
                        :showUuid="showUuids"
                        :liveUpdate="liveUpdate"
                        :isAnyChildCollapsed="anyChildCollapsedByUuid[uuid]"
                        :opacity="!focusedNodeUuid || focusedNodeUuid === uuid ? 1: 0.3"
                        v-on:collapse-node="toggleCollapseDescendants(uuid)"></x-svg-task-node>
            <!-- TODO: this is no longer correct -- collapseGraphByNodeUuid doesn't exclusively
                   contain collapsed nodes.-->
            <x-svg-collapse-node
              v-else-if="collapseGraphByNodeUuid.hasOwnProperty(uuid)"
              :key="uuid"
              :position="nodeLayout"
              :collapseNode="collapseGraphByNodeUuid[uuid]"></x-svg-collapse-node>
          </template>
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
  calculateNodesPositionByUuid, getCenteringTransform, getAncestorUuids, uuidv4,
  getAllDescendantsUuidsInclusive, rollupTaskStatesBackground, resolveCollapseStatusByUuid,
  getCollapsedGraphByNodeUuid, createCollapseEvent,
} from '../utils';
import XSvgCollapseNode from './nodes/XSvgCollapseNode.vue';

const scaleBounds = { max: 1.3, min: 0.01 };

export default {
  name: 'XGraph',
  components: {
    XSvgCollapseNode,
    XSvgTaskNode,
    XLink,
    XTaskNode,
  },
  props: {
    // TODO: it might be worth making a computed property of just the graph structure
    //    (parent/child relationships),
    //  to avoid re-calcs in some context were only structure (not data content) is relied on.
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
      // unfortunate we need to track this manually. TODO: look for a better way.
      dimensionsByUuid: {},
      zoom,
      transform: { x: 0, y: 0, scale: 1 },
      // When set, fades-out other nodes. Is reset by pan/zoom events.
      focusedNodeUuid: null,
      // Only want to center on first layout, then we'll rely on stored transform.
      isFirstLayout: true,
      // This is just a container for states that have been touched by the user -- it doesn't
      // contain an entry for every node's UUID (there is a computed property for that).
      // UUIDs map to a list of states: 'ui-{collapse|expand}-{children|grandChildren|ancestors}'
      uiCollapseStatesByUuid: {},
      // Default to hiding paths that don't include a failure or in progress by default.
      hideSucessPaths: true,
    };
  },
  computed: {
    allUuids() {
      return _.keys(this.nodesByUuid);
    },
    taskNodeDimensionsByUuid() {
      // Note that since dimensionsByUuid is never deleted from, we need to filter by allUuids
      // in case nodesByUuid changes. This is a bit gross, maybe use a watcher instead.
      return _.mapValues(_.pick(this.dimensionsByUuid, this.allUuids),
        (node, uuid) => ({
          uuid,
          width: this.dimensionsByUuid[uuid].width,
          height: this.dimensionsByUuid[uuid].height,
          parent_id: this.nodesByUuid[uuid].parent_id,
        }));
    },
    /**
     * Contains both collapse nodes and uncollapsed task nodes. Only contains tree structure
     * (.e.g parent_id) relationships for uncollapsed task nodes.
     * */
    collapseGraphByNodeUuid() {
      return _.mapValues(getCollapsedGraphByNodeUuid(this.resolvedCollapseStateByUuid, uuidv4),
        (n) => {
          if (!n.collapsed) {
            return n;
          }
          // Properies for collapse nodes.
          const collapsedCount = n.allRepresentedNodeUuids.length;
          let radius;
          if (collapsedCount === 1) {
            radius = 30;
          } else if (collapsedCount < 15) {
            radius = 50;
          } else if (collapsedCount < 50) {
            radius = 90;
          } else {
            radius = 120;
          }
          return _.merge(n, {
            background: rollupTaskStatesBackground(
              _.map(n.allRepresentedNodeUuids, u => _.get(this.nodesByUuid, [u, 'state'])),
            ),
            radius,
            width: radius * 2,
            height: radius * 2,
          });
        });
    },

    /**
     * Merges the task tree with the collapsed tree. This changes the graph structure,
     * Since sequential collapsed nodes are represented as a single collapse node.
     * */
    taskAndCollapseNodeDimensionsByUuid() {
      const sizedUncollapsedTaskNodes = _.omit(this.taskNodeDimensionsByUuid,
        this.collapsedNodeUuids);
      // Want parent relationship from collapseGraphByNodeUuid, so give it precedence
      // in the merge.
      const sizedTaskNodesAndCollapsedNodes = _.pickBy(this.collapseGraphByNodeUuid,
        n => n.collapsed || _.has(sizedUncollapsedTaskNodes, n.uuid));
      return _.merge(sizedUncollapsedTaskNodes, sizedTaskNodesAndCollapsedNodes);
    },
    // TODO: should consider limiting layout recalculations to once per second instead of
    //      being reactive.
    //  This will probably slow down for large graphs (untested).
    nodeLayoutsByUuid() {
      if (!_.isEmpty(this.taskNodeDimensionsByUuid)) {
        return calculateNodesPositionByUuid(this.taskAndCollapseNodeDimensionsByUuid);
      }
      return {};
    },
    nonCollapsedNodesExtent() {
      return {
        top: _.min(_.map(this.nodeLayoutsByUuid, 'y')),
        left: _.min(_.map(this.nodeLayoutsByUuid, 'x')),
        // Note that if we've done the layout for a given UUID, we necessarily have the
        // node dimensions.
        right: _.max(_.map(this.nodeLayoutsByUuid,
          (n, uuid) => n.x + this.taskAndCollapseNodeDimensionsByUuid[uuid].width)),
        bottom: _.max(_.map(this.nodeLayoutsByUuid,
          (n, uuid) => n.y + this.taskAndCollapseNodeDimensionsByUuid[uuid].height)),
      };
    },
    hasFailures() {
      return _.some(_.values(this.nodesByUuid), { state: 'task-failed' });
    },
    svgGraphTransform() {
      const xy = _.join([this.transform.x, this.transform.y], ',');
      return `translate(${xy})scale(${this.transform.scale})`;
    },
    mergedCollapseState() {
      return _.merge({}, this.flameDataDisplayOperationsByUuid, this.uiCollapseStatesByUuid);
    },
    // uuid -> boolean, true means collapsed, false means uncollapsed (expanded)
    // Combines different data sources for collapse state (task-state, UI collapse/expand clicks)
    // in to the rendered state.
    resolvedCollapseStateByUuid() {
      return resolveCollapseStatusByUuid(this.nodesByUuid, this.mergedCollapseState);
    },
    collapsedNodeUuids() {
      // TODO: does it make any sense to store uuid -> boolean map? Just store the list of
      //    uuids initially.
      return _.keys(_.pickBy(this.resolvedCollapseStateByUuid, 'collapsed'));
    },
    collapsedChildrenByUuid() {
      return _.mapValues(this.nodesByUuid,
        node => _.uniq(_.filter(node.children_uuids,
          cUuid => this.resolvedCollapseStateByUuid[cUuid].collapsed)));
    },
    // Is this really necessary?
    anyChildCollapsedByUuid() {
      return _.mapValues(this.collapsedChildrenByUuid,
        collapsedChildren => !_.isEmpty(collapsedChildren));
    },
    flameDataDisplayOperationsByUuid() {
      const displayFlameDataRegex = /__start_dd(.*)__end_dd/;
      const nodesWithDisplayFlameDataByUUid = _.pickBy(this.nodesByUuid,
        n => _.get(n, 'flame_additional_data', '').match(displayFlameDataRegex));
      const backendDefaultDisplayByUuid = _.mapValues(nodesWithDisplayFlameDataByUUid,
        n => JSON.parse(displayFlameDataRegex.exec(
          n.flame_additional_data.replace(/<br \/>/g, ''),
        )[1]));

      return _.mapValues(backendDefaultDisplayByUuid, (operationByTarget) => {
        return _.mapValues(operationByTarget, (op) => {
          return {
            priority: 5, // less than UI state priority.
            operation: op,
          };
        });
      });
    },
  },
  created() {
    eventHub.$on('center', this.center);
    eventHub.$on('node-focus', this.focusOnNode);
    eventHub.$on('ui-collapse', this.handleUiCollapseEvent);
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
        if (_.every(this.nonCollapsedNodesExtent, _.negate(_.isNil))) {
          return getCenteringTransform(this.nonCollapsedNodesExtent, boundingRect,
            scaleBounds, 200);
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
    /**
     * Collapse the descendants of the supplied node if no descendants are currently collapsed.
     * If any descendant is currently collapsed, show all.
     * @param parentNodeId
     */
    toggleCollapseDescendants(parentNodeId) {
      const allCollapsed = this.collapsedChildrenByUuid[parentNodeId].length
        === this.nodesByUuid[parentNodeId].children_uuids.length;
      this.handleUiCollapseEvent(
        {
          keep_rel_position_task_uuid: parentNodeId,
          operationsByUuid: createCollapseEvent(
            [parentNodeId], allCollapsed ? 'expand' : 'collapse',
            'descendants',
          ),
        },
      );
    },
    handleUiCollapseEvent(event) {
      // Specifying a node to maintain position realtive to is optional.
      const initialRelPos = event.keep_rel_position_task_uuid
        ? this.getCurrentRelPos(event.keep_rel_position_task_uuid) : undefined;

      _.each(event.operationsByUuid, (ops, uuid) => {
        const stateByTarget = _.get(this.uiCollapseStatesByUuid, uuid, {});
        // if incoming value is 'expand', that means 'remove any UI collapse'
        // collapse, on the other hand, adds a high-priority 'collapse' operation.
        // const newStateByTarget = _.pickBy(
        //  _.merge(stateByTarget, ops), s => s.operation !== 'expand');
        // Note this means that when a node is expanded, it stays expanded.

        // for incoming targets that already have a value, just drop the value (toggle).
        const droppedTogglingValues = _.pickBy(stateByTarget, (s, t) => !_.has(ops, t));
        // If it's a value for a new key, add it.
        const newValues = _.pickBy(ops, (s, t) => !_.has(stateByTarget, t));

        this.$set(this.uiCollapseStatesByUuid, uuid, _.merge(droppedTogglingValues, newValues));
      });

      if (initialRelPos) {
        // Since we're changing the nodes being displayed, the layout might drastically change.
        // Maintain the position of the node whose descendants have been added/removed so that
        // the user remains oriented.
        this.$nextTick(() => {
          const nextRelPos = this.getCurrentRelPos(event.keep_rel_position_task_uuid);
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
      }
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
      this.uiCollapseStatesByUuid = {};
      // TODO: applyFlameDataCollapseOps = false
    },
    loadLocalStorageState() {
      this.updateTransformViaZoom(this.getLocalStorageTransform());
      this.hideSucessPaths = this.readPathFromLocalStorage('hideSucessPaths', false);
      this.uiCollapseStatesByUuid = this.readPathFromLocalStorage(
        'uiCollapseStatesByUuid', {},
      );
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
        this.uiCollapseStatesByUuid = this.readPathFromLocalStorage(
          'uiCollapseStatesByUuid', {},
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
    uiCollapseStatesByUuid: {
      handler() {
        this.addLocalStorageData({
          uiCollapseStatesByUuid: this.uiCollapseStatesByUuid,
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
