<template>
  <div style="width: 100%; height: 100%; overflow: hidden;" tabindex="1"
       @keydown.c="center">
    <div class="user-message" style="background: lightblue; ">

      <span v-if="collapsedNodeUuids.length > 0">
        {{collapsedNodeUuids.length}} tasks are collapsed
      </span>

      <template v-if="collapsedNodeUuids.length > 0">
        (<a href="#" @click.prevent="setCollapseFilterState({})"
          >Expand All</a>)
      </template>

      <template v-if="canRestoreDefault">
        (<a  href="#" @click.prevent="setCollapseFilterState({applyDefaultCollapseOps: true})"
          >Restore Default</a>)
      </template>

      <template v-if="canShowOnlyFailed">
        (<a href="#" @click.prevent="setCollapseFilterState({ hideSuccessPaths: true })"
          >Show only failed</a>)
      </template>
    </div>
    <div id="chart-container" style="width: 100%; height: 100%" ref="graph-svg">
      <svg width="100%" height="100%" preserveAspectRatio="xMinYMin"
           style="background-color: white;">
        <g :transform="svgGraphTransform">
          <x-link
            :nodeDimensionsByUuid="uncollapsedTaskNodeDimensionsByUuid"
            :nodeLayoutsByUuid="nodeLayoutsByUuid"></x-link>
          <x-svg-task-node
            v-for="(nodeLayout, uuid) in nodeLayoutsByUuid"
            :key="uuid"
            :node="nodesByUuid[uuid]"
            :dimensions="uncollapsedTaskNodeDimensionsByUuid[uuid]"
            :position="nodeLayout"
            :showUuid="showUuids"
            :liveUpdate="liveUpdate"
            :collapseDetails="collapseGraphByNodeUuid[uuid]"
            :nodeGraphData="graphDataByUuid[uuid]"
            :opacity="!focusedNodeUuid || focusedNodeUuid === uuid ? 1: 0.3"
            :displayDetails="resolvedCollapseStateByUuid[uuid].minPriorityOp">
          </x-svg-task-node>
        </g>
      </svg>
    </div>

    <div style="overflow: hidden; width: 100%; height: 100%; position: absolute; top: 0;
      z-index: -10">
      <!-- This is very gross, but the nodes that will be put on the graph are rendered
      invisibly in order for the browser to calculate their intrinsic size. Each node's size is
      then passed to the graph layout algorithm before the actual SVG graph is rendered.-->
      <!--
        Need inline-block display per node to get each node's intrinsic width (i.e. don't want it
        to force fill parent).
      -->
      <div v-for="n in nodesByUuid" :key="n.uuid"
           style="display: inline-block; position: absolute; top: 0; z-index: -1000;">
        <!-- TODO: profile event performance with thousands of nodes. Might be worthwhile
            to catch all and send in one big event. -->
        <x-task-node
          :node="n"
          :emitDimensions="true"
          :showUuid="showUuids"
          :isLeaf="true"
          :displayDetails="getDisplayDetails(resolvedCollapseStateByUuid, n.uuid)"
          v-on:node-dimensions="updateTaskNodeDimensions($event)"></x-task-node>
      </div>
    </div>
  </div>
</template>

<script>

import { zoom as d3zoom, zoomIdentity } from 'd3-zoom';
import { select as d3select, event as d3event } from 'd3-selection';
import _ from 'lodash';

import XSvgTaskNode from './nodes/XSvgTaskNode.vue';
import XTaskNode from './nodes/XTaskNode.vue';
import XLink from './XLinks.vue';
import {
  eventHub, calculateNodesPositionByUuid, getCenteringTransform,
  resolveCollapseStatusByUuid,
  getCollapsedGraphByNodeUuid, createCollapseOpsByUuid, createRunStateExpandOperations,
  loadDisplayConfigs, concatArrayMergeCustomizer, containsAll,
  getPrioritizedTaskStateBackground,
} from '../utils';
import {
  prioritizeCollapseOps, resolveDisplayConfigsToOpsByUuid, resolveToggleOperation,
} from '../collapse';
import {
  getGraphDataByUuid, getChildrenUuidsByUuid,
} from '../graph-utils';

const scaleBounds = { max: 1.3, min: 0.01 };
export default {
  name: 'XGraph',
  components: {
    XSvgTaskNode, XLink, XTaskNode,
  },
  props: {
    nodesByUuid: { required: true, type: Object },
    runMetadata: { required: true, type: Object },
    // TODO: rename to 'showDetails'.
    showUuids: { default: false, type: Boolean },
    liveUpdate: { required: true, type: Boolean },
  },
  data() {
    const zoom = d3zoom()
      .scaleExtent([scaleBounds.min, scaleBounds.max])
      // Threshold for when a click is considered a pan, since this blocks event propagation.
      .clickDistance(4)
      .on('zoom', this.zoomed);
    return {
      // unfortunately we need to track this manually. TODO: look for a better way.
      dimensionsByUuid: {},
      zoom,
      transform: { x: 0, y: 0, scale: 1 },
      // When set, fades-out other nodes. Is reset by pan/zoom events.
      focusedNodeUuid: null,
      // Only want to center on first layout, then we'll rely on stored transform.
      isFirstLayout: true,
      collapseConfig: this.getLocalStorageCollapseConfig(),
    };
  },
  computed: {
    firexUid() {
      return this.runMetadata.uid;
    },
    allUuids() {
      return _.keys(this.nodesByUuid);
    },
    // TODO: could even push up this check to event processing.
    parentUuidByUuid() {
      return _.mapValues(this.nodesByUuid, 'parent_id');
    },
    childrenUuidsByUuid() {
      return getChildrenUuidsByUuid(this.parentUuidByUuid);
    },
    graphDataByUuid() {
      return getGraphDataByUuid(this.runMetadata.root_uuid, this.parentUuidByUuid,
        this.childrenUuidsByUuid);
    },
    runStateByUuid() {
      return _.mapValues(this.nodesByUuid,
        n => _.assign(
          { isLeaf: this.childrenUuidsByUuid[n.uuid].length === 0 },
          _.pick(n, ['state', 'exception']),
        ));
    },
    taskNodeDimensionsByUuid() {
      // Note that since dimensionsByUuid is never deleted from, we need to filter by allUuids
      // in case nodesByUuid changes. This is a bit gross, maybe use a watcher instead.
      const visibleUuids = _.difference(this.allUuids, this.collapsedNodeUuids);
      return _.pick(this.dimensionsByUuid, visibleUuids);
    },
    /**
     * Make collapse info available per UUID. Modifies tree structure when a sequence of collapsed
     * nodes exists.
     * */
    collapseGraphByNodeUuid() {
      const stackOffset = 12;
      const stackCount = 2; // Always have 2 stacked behind the front.
      return _.mapValues(getCollapsedGraphByNodeUuid(
        this.runMetadata.root_uuid,
        this.childrenUuidsByUuid,
        // TODO: getCollapsedGraphByNodeUuid could receive just uuid -> collapsed boolean map.
        this.resolvedCollapseStateByUuid,
      ),
      collapseData => ({
        background: getPrioritizedTaskStateBackground(
          _.map(collapseData.collapsedUuids, u => this.runStateByUuid[u].state),
        ),
        collapsedUuids: collapseData.collapsedUuids,
        // We don't want the width to vary if a node has collapsed children or not,
        // so pad the same either way.
        // x2 because we pad both left and right to keep front box centering.
        widthPadding: stackCount * stackOffset * 2,
        // Only pad vertical to make room for real stacked behind boxes.
        heightPadding: _.isEmpty(collapseData.collapsedUuids) ? 0 : stackCount * stackOffset,
        stackOffset,
        stackCount,
        parent_id: collapseData.parentId,
      }));
    },
    uncollapsedTaskNodeDimensionsByUuid() {
      // TODO: should collapseGraphByNodeUuid drop collapsed nodes? Then map over those
      //  (uncollapsed) keys?
      return _.mapValues(this.taskNodeDimensionsByUuid,
        (d, uuid) => _.merge({}, d,
          {
            uuid,
            width: d.width + this.collapseGraphByNodeUuid[uuid].widthPadding,
            height: d.height + this.collapseGraphByNodeUuid[uuid].heightPadding,
            parent_id: this.collapseGraphByNodeUuid[uuid].parent_id,
          }));
    },
    // TODO: should consider limiting layout recalculations to once per second instead of
    //      being reactive?
    //  This will probably slow down for large graphs (untested).
    nodeLayoutsByUuid() {
      if (!_.isEmpty(this.taskNodeDimensionsByUuid)) {
        return calculateNodesPositionByUuid(this.uncollapsedTaskNodeDimensionsByUuid);
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
          (n, uuid) => n.x + this.uncollapsedTaskNodeDimensionsByUuid[uuid].width)),
        bottom: _.max(_.map(this.nodeLayoutsByUuid,
          (n, uuid) => n.y + this.uncollapsedTaskNodeDimensionsByUuid[uuid].height)),
      };
    },
    hasFailures() {
      return _.some(_.values(this.runStateByUuid), { state: 'task-failed' });
    },
    svgGraphTransform() {
      const xy = _.join([this.transform.x, this.transform.y], ',');
      return `translate(${xy})scale(${this.transform.scale})`;
    },
    mergedCollapseStateSources() {
      const enabledCollapseStateSources = [];
      if (this.collapseConfig.applyDefaultCollapseOps) {
        enabledCollapseStateSources.push(
          prioritizeCollapseOps(this.flameDataDisplayOperationsByUuid, 'flameData'),
        );
        enabledCollapseStateSources.push(
          prioritizeCollapseOps(this.userDisplayConfigOperationsByUuid, 'userConfig'),
        );
        enabledCollapseStateSources.push(
          prioritizeCollapseOps(this.runStateExpandOperationsByUuid, 'runState'),
        );
      }
      if (this.collapseConfig.hideSuccessPaths) {
        enabledCollapseStateSources.push(
          prioritizeCollapseOps(this.showOnlyRunStateCollapseOperationsByUuid, 'runState'),
        );
      }
      enabledCollapseStateSources.push(this.collapseConfig.uiCollapseOperationsByUuid);
      return _.mergeWith({}, ...enabledCollapseStateSources, concatArrayMergeCustomizer);
    },
    resolvedCollapseStateByUuid() {
      return resolveCollapseStatusByUuid(
        this.runMetadata.root_uuid, this.graphDataByUuid, this.mergedCollapseStateSources,
      );
    },
    isCollapsedByUuid() {
      return _.mapValues(this.resolvedCollapseStateByUuid, 'collapsed');
    },
    collapsedNodeUuids() {
      return _.keys(_.pickBy(this.isCollapsedByUuid));
    },
    allDescendantsCollapsedByUuid() {
      return _.mapValues(this.collapseGraphByNodeUuid,
        (collapseData, uuid) => containsAll(collapseData.collapsedUuids,
          this.graphDataByUuid[uuid].descendantUuids));
    },
    // Avoid re-calculating display ops on every data change.
    flameDataAndNameByUuid() {
      return _.mapValues(this.nodesByUuid,
        // Needs parent_id & uuid for descendants calc. Should have a single one of those,
        // selectively updated.
        // TODO: further prune to flame_data._default_display
        n => _.pick(n, ['flame_data', 'name', 'parent_id', 'uuid']));
    },
    flameDataDisplayOperationsByUuid() {
      const displayPath = ['flame_data', '_default_display', 'value'];
      // TODO: Each task can send updates that should override previous op entries for that task.
      //  Do that filtering here.
      const ops = _.flatMap(this.flameDataAndNameByUuid, n => _.get(n, displayPath, []));
      return resolveDisplayConfigsToOpsByUuid(ops, this.flameDataAndNameByUuid);
    },
    userDisplayConfigOperationsByUuid() {
      const displayConfigs = loadDisplayConfigs();
      return resolveDisplayConfigsToOpsByUuid(displayConfigs, this.flameDataAndNameByUuid);
    },
    runStateExpandOperationsByUuid() {
      return createRunStateExpandOperations(this.runStateByUuid);
    },
    showOnlyRunStateCollapseOperationsByUuid() {
      const rootCollapse = {
        [[this.runMetadata.root_uuid]]: [{ targets: ['descendants'], operation: 'collapse' }],
      };
      return _.assign({}, rootCollapse, this.runStateExpandOperationsByUuid);
    },
    canRestoreDefault() {
      const otherConfigApplied = this.collapseConfig.hideSuccessPaths
        || !_.isEmpty(this.collapseConfig.uiCollapseOperationsByUuid);
      const hasDefaultAffectingOps = _.size(this.userDisplayConfigOperationsByUuid) > 0
        || _.size(this.flameDataDisplayOperationsByUuid) > 0;
      return hasDefaultAffectingOps
        && (otherConfigApplied || !this.collapseConfig.applyDefaultCollapseOps);
    },
    canShowOnlyFailed() {
      const alreadyApplied = this.collapseConfig.hideSuccessPaths;
      const userTouched = !_.isEmpty(this.collapseConfig.uiCollapseOperationsByUuid);
      return this.hasFailures && (!alreadyApplied || userTouched);
    },
  },
  mounted() {
    d3select('div#chart-container').call(this.zoom).on('dblclick.zoom', null);
    // this.$el.focus();

    // Registering listeners in 'created' some cause duplicate handlers to exist.
    eventHub.$on('center', this.center);
    eventHub.$on('node-focus', this.focusOnNode);
    eventHub.$on('ui-collapse', this.handleUiCollapseEvent);
    eventHub.$on('toggle-task-collapse', this.toggleCollapseDescendants);
  },
  methods: {
    zoomed() {
      // Null source events mean programatic zoom. We don't want to clear for programatic zooms.
      if (d3event.sourceEvent !== null) {
        // Clear focus node on non-programatic pan/zoom.
        this.focusedNodeUuid = null;
      }
      this.transform = {
        x: d3event.transform.x,
        y: d3event.transform.y,
        scale: d3event.transform.k,
      };
      // console.log(_.get(d3event, ['sourceEvent', 'type'], ''));
      // TODO: make transform top-level key per firexUid. This will avoid write slowdowns as other
      // per-run data grows.
      this.addLocalStorageData(this.transform);
    },
    updateTransformViaZoom(transform) {
      // MUST MAINTAIN ZOOM'S INTERNAL STATE! Otherwise, subsequent pan/zooms are inconsistent
      // with current position.
      const d3Transform = zoomIdentity.translate(transform.x, transform.y).scale(transform.scale);
      // This call will create a d3event and pipe it through, just like manual pan/zooms.
      d3select('div#chart-container').call(this.zoom.transform, d3Transform);
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
      // TODO: handle focusing on nodes that are collapsed.
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
      const allChildrenExpanded = _.every(this.childrenUuidsByUuid[parentNodeId],
        cuuid => this.resolvedCollapseStateByUuid[cuuid].collapsed);
      const allDescendantsCollapsed = this.allDescendantsCollapsedByUuid[parentNodeId];
      const resolvedOperation = resolveToggleOperation(parentNodeId,
        allDescendantsCollapsed, allChildrenExpanded,
        this.collapseConfig.uiCollapseOperationsByUuid);

      this.handleUiCollapseEvent(
        {
          keep_rel_pos_uuid: parentNodeId,
          operationsByUuid: createCollapseOpsByUuid(resolvedOperation.uuids,
            resolvedOperation.operation, resolvedOperation.target, parentNodeId),
        },
      );
    },
    handleUiCollapseEvent(event) {
      // Specifying a node to maintain position relative to is optional.
      const initialRelPos = event.keep_rel_pos_uuid
        ? this.getCurrentRelPos(event.keep_rel_pos_uuid) : undefined;

      _.each(event.operationsByUuid, (op, uuid) => {
        const existingOps = _.get(this.collapseConfig.uiCollapseOperationsByUuid, uuid, []);
        let resultOps;
        if (op.operation === 'clear') {
          resultOps = _.reject(existingOps,
            existingOp => _.isEqual(existingOp.targets, op.targets)
              && existingOp.sourceTaskUuid === op.sourceTaskUuid);
        } else {
          // All other operations are just accumulated.
          resultOps = existingOps.concat([op]);
        }
        this.$set(this.collapseConfig.uiCollapseOperationsByUuid, uuid, resultOps);
      });

      if (initialRelPos) {
        // Since we're changing the nodes being displayed, the layout might drastically change.
        // Maintain the position of the node whose descendants have been added/removed so that
        // the user remains oriented.
        this.$nextTick(() => {
          const nextRelPos = this.getCurrentRelPos(event.keep_rel_pos_uuid);
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
    getLocalStorageCollapseConfig() {
      const storedCollapseConfig = this.readPathFromLocalStorage('collapseConfig');
      const expectedKeys = ['hideSuccessPaths', 'uiCollapseOperationsByUuid',
        'applyDefaultCollapseOps'];
      // TODO: fill in remaining validation of collapseConfig.
      const containsAllRequired = containsAll(_.keys(storedCollapseConfig), expectedKeys);
      const collapseByUuidContainsAllRequired = _.every(
        _.values(_.get(storedCollapseConfig, 'uiCollapseOperationsByUuid', {})),
        ops => _.every(ops, op => containsAll(_.keys(op),
          ['operation', 'priority', 'targets', 'sourceTaskUuid'])),
      );
      if (containsAllRequired && collapseByUuidContainsAllRequired) {
        return storedCollapseConfig;
      }
      // Default collapse config.
      return {
        // This is just a container for states that have been touched by the user -- it doesn't
        // contain an entry for every node's UUID (there is a computed property for that).
        uiCollapseOperationsByUuid: {},
        // Hiding paths that don't include a failure or in progress.
        hideSuccessPaths: false,
        // Default to apply backend & user config display state.
        applyDefaultCollapseOps: true,
      };
    },
    setCollapseFilterState(obj) {
      // All keys not specified on object are disabled.
      this.collapseConfig = {
        hideSuccessPaths: _.get(obj, 'hideSuccessPaths', false),
        uiCollapseOperationsByUuid: _.get(obj, 'uiCollapseOperationsByUuid', {}),
        applyDefaultCollapseOps: _.get(obj, 'applyDefaultCollapseOps', false),
      };
      // Changing filter state can significantly change visible nodes, so we center.
      this.center();
    },
    getDisplayDetails(resolvedCollapseStateByUuid, uuid) {
      return _.get(resolvedCollapseStateByUuid, [uuid, 'minPriorityOp'], null);
    },
  },
  watch: {
    firexUid() {
      this.isFirstLayout = true;
      this.collapseConfig = this.getLocalStorageCollapseConfig();
    },
    nodeLayoutsByUuid() {
      // Need to load stored transform AFTER initial layout.
      if (this.isFirstLayout) {
        this.isFirstLayout = false;
        // TODO: combine localstorage reads, or cache at lower level.
        this.updateTransformViaZoom(this.getLocalStorageTransform());
      }
    },
    collapseConfig: {
      handler() {
        this.addLocalStorageData({
          collapseConfig: this.collapseConfig,
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
