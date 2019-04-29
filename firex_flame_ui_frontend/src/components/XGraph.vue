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
            :taskAndCollapseNodeDimensionsByUuid="taskAndCollapseNodeDimensionsByUuid"
            :nodeLayoutsByUuid="nodeLayoutsByUuid"></x-link>
          <x-svg-task-node
            v-for="(nodeLayout, uuid) in nodeLayoutsByUuid"
            :node="nodesByUuid[uuid]"
            :dimensions="taskAndCollapseNodeDimensionsByUuid[uuid]"
            :position="nodeLayout"
            :key="uuid"
            :showUuid="showUuids"
            :liveUpdate="liveUpdate"
            :collapseDetails="collapseGraphByNodeUuid[uuid]"
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
          :emitDimensions="true" :showUuid="showUuids"
          :displayDetails="getDisplayDetails(resolvedCollapseStateByUuid, n.uuid)"
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
  eventHub, calculateNodesPositionByUuid, getCenteringTransform,
  resolveCollapseStatusByUuid,
  getCollapsedGraphByNodeUuid, createCollapseEvent, createRunStateExpandOperations,
  loadDisplayConfigs, concatArrayMergeCustomizer, createCollapseRootOperation, containsAll,
  getPrioritizedTaskStateBackgrounds,
} from '../utils';
import {
  prioritizeCollapseOps, resolveDisplayConfigsToOpsByUuid,
} from '../collapse';

const scaleBounds = { max: 1.3, min: 0.01 };

export default {
  name: 'XGraph',
  components: {
    XSvgTaskNode, XLink, XTaskNode,
  },
  props: {
    // TODO: it might be worth making a computed property of just the graph structure
    //    (parent/child relationships)
    //  to avoid re-calcs in some context where only structure (not data content) is relied on.
    nodesByUuid: { required: true, type: Object },
    firexUid: { required: true, type: String },
    // TODO: rename to 'showDetails'.
    showUuids: { default: false, type: Boolean },
    liveUpdate: { required: true, type: Boolean },
  },
  data() {
    const zoom = d3.zoom()
      .scaleExtent([scaleBounds.min, scaleBounds.max])
      // Threshold for when a click is considered a pan, since this blocks event propagation.
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
      collapseConfig: this.getLocalStorageCollapseConfig(),
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
     * Contains both collapse nodes and uncollapsed task nodes.
     * */
    collapseGraphByNodeUuid() {
      const stackOffset = 12;
      const stackCount = 3; // Always have 3 stacked behind the front.
      return _.mapValues(getCollapsedGraphByNodeUuid(this.resolvedCollapseStateByUuid),
        collapsedUuids => ({
          backgrounds: getPrioritizedTaskStateBackgrounds(
            _.map(collapsedUuids, u => _.get(this.nodesByUuid, [u, 'state'])),
          ),
          collapsedUuids,
          // We don't want the width to vary if a node has collapsed children or not,
          // to pad the same either way.
          // x2 because we pad both left and right to keep front box centering.
          widthPadding: stackCount * stackOffset * 2,
          // Only pad vertical to make room for real stacked behind boxes.
          heightPadding: _.isEmpty(collapsedUuids) ? 0 : stackCount * stackOffset,
          stackOffset,
          stackCount,
        }));
    },
    /**
     * Merges the task tree with the collapsed tree. This changes the graph structure,
     * Since sequential collapsed nodes are represented as a single collapse node.
     * */
    taskAndCollapseNodeDimensionsByUuid() {
      const sizedUncollapsedTaskNodes = _.omit(this.taskNodeDimensionsByUuid,
        this.collapsedNodeUuids);

      return _.mapValues(sizedUncollapsedTaskNodes,
        (d, uuid) => _.merge({}, d,
          {
            width: d.width + this.collapseGraphByNodeUuid[uuid].widthPadding,
            height: d.height + this.collapseGraphByNodeUuid[uuid].heightPadding,
          }));
    },
    // TODO: should consider limiting layout recalculations to once per second instead of
    //      being reactive?
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
    // TODO: consider simplifying this to uuid -> boolean instead of encoding graph shape here too.
    resolvedCollapseStateByUuid() {
      return resolveCollapseStatusByUuid(this.nodesByUuid, this.mergedCollapseStateSources);
    },
    collapsedNodeUuids() {
      return _.keys(_.pickBy(this.resolvedCollapseStateByUuid, 'collapsed'));
    },
    collapsedChildrenByUuid() {
      return _.mapValues(this.nodesByUuid,
        node => _.uniq(_.filter(node.children_uuids,
          cUuid => this.resolvedCollapseStateByUuid[cUuid].collapsed)));
    },
    // Is this really necessary?
    allChildrenCollapsedByUuid() {
      return _.mapValues(this.collapsedChildrenByUuid,
        (collapsedChildren, uuid) => containsAll(collapsedChildren,
          this.nodesByUuid[uuid].children_uuids));
    },
    flameDataDisplayOperationsByUuid() {
      const displayPath = ['flame_data', '_default_display', 'value'];
      // TODO: Each task can send updates that should override previous op entries for that task.
      //  Do that filtering here.
      // TODO: computed property just for flame data to avoid recalc on any nodesByUuid change.
      const ops = _.flatMap(this.nodesByUuid, n => _.get(n, displayPath, []));
      return resolveDisplayConfigsToOpsByUuid(ops, this.nodesByUuid);
    },
    userDisplayConfigOperationsByUuid() {
      const displayConfigs = loadDisplayConfigs();
      return resolveDisplayConfigsToOpsByUuid(displayConfigs, this.nodesByUuid);
    },
    runStateExpandOperationsByUuid() {
      return createRunStateExpandOperations(this.nodesByUuid);
    },
    showOnlyRunStateCollapseOperationsByUuid() {
      // TODO: root almost never changes, so could avoid recalc.
      const rootCollapse = createCollapseRootOperation(this.nodesByUuid);
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
  created() {
    eventHub.$on('center', this.center);
    eventHub.$on('node-focus', this.focusOnNode);
    eventHub.$on('ui-collapse', this.handleUiCollapseEvent);
    eventHub.$on('toggle-task-collapse', this.toggleCollapseDescendants);
  },
  mounted() {
    d3.select('div#chart-container svg').call(this.zoom).on('dblclick.zoom', null);
    // this.$el.focus();
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
      // TODO: make transform top-level key per firexUid. This will avoid write slowdowns as other
      // per-run data grows.
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
      if (!laidOutNode) {
        console.log(`Missings ${nodeUuid}`);
        console.log(this.nodeLayoutsByUuid);
      }
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
      const allCollapsed = this.allChildrenCollapsedByUuid[parentNodeId];
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
        const existingOps = _.get(this.collapseConfig.uiCollapseOperationsByUuid, uuid, []);
        this.$set(this.collapseConfig.uiCollapseOperationsByUuid, uuid, existingOps.concat(ops));
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
    getLocalStorageCollapseConfig() {
      const storedCollapseConfig = this.readPathFromLocalStorage('collapseConfig');
      const expectedKeys = ['hideSuccessPaths', 'uiCollapseOperationsByUuid',
        'applyDefaultCollapseOps'];
      // TODO: fill in remaining validation of collapseConfig.
      const containsAllRequired = _.intersection(_.keys(storedCollapseConfig), expectedKeys).length
        === expectedKeys.length;
      if (containsAllRequired) {
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
      if (this.isFirstLayout) {
        this.isFirstLayout = false;
        // TODO: combine localstorage reads, or cache at lower level.
        this.updateTransformViaZoom(this.getLocalStorageTransform());
      }
      // console.log(_.has(this.nodeLayoutsByUuid, '4bccfbce-0620-4c8e-b3a6-e1bdc84f7df4'))
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
