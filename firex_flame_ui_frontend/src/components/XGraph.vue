<template>
  <div style="width: 100%; height: 100%; overflow: hidden;" tabindex="1"
       @keydown.c="center">
    <div class="user-message" style="background: lightblue; ">

      <span v-if="hasCollapsedNodes">
        {{collapsedNodeUuids.length}}/{{allUuids.length}} tasks are collapsed
      </span>

      <template v-if="hasCollapsedNodes">
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
    <div id="chart-container" ref="graph-svg">
        <svg preserveAspectRatio="xMinYMin" style="width: 100%; height: 100%;">
          <g :transform="svgGraphTransform">
            <x-link
              :parentUuidByUuid="parentUuidByUuid"
              :nodeLayoutsByUuid="nodeLayoutsByUuid"></x-link>
            <x-svg-task-nodes
              :nodeLayoutsByUuid="nodeLayoutsByUuid"></x-svg-task-nodes>
          </g>
        </svg>
    </div>
    <x-task-capturing-nodes></x-task-capturing-nodes>
  </div>
</template>

<script>

import { zoom as d3zoom, zoomIdentity } from 'd3-zoom';
import { select as d3select, event as d3event } from 'd3-selection';
import _ from 'lodash';

import XSvgTaskNodes from './nodes/XSvgTaskNodes.vue';
import XTaskCapturingNodes from './nodes/XSizeCapturingNodes.vue';
import XLink from './XLinks.vue';
import {
  eventHub, calculateNodesPositionByUuid, getCenteringTransform,
  createCollapseOpsByUuid, containsAll,
} from '../utils';
import {
  resolveToggleOperation,
} from '../collapse';
import {
  readPathsFromLocalStorage, addLocalStorageData, getLocalStorageCollapseConfig,
} from '../persistance';

function zoomed() {
  eventHub.$emit('zoom', {
    x: d3event.transform.x,
    y: d3event.transform.y,
    scale: d3event.transform.k,
  });
}
const scaleBounds = { max: 1, min: 0.01 };
const zoom = d3zoom()
  .scaleExtent([scaleBounds.min, scaleBounds.max])
  // Threshold for when a click is considered a pan, since this blocks event propagation.
  .clickDistance(4)
  .on('zoom', zoomed);


export default {
  name: 'XGraph',
  components: {
    XSvgTaskNodes, XLink, XTaskCapturingNodes,
  },
  data() {
    return {
      transform: { x: 0, y: 0, scale: 1 },
      // When set, fades-out other nodes. Is reset by pan/zoom events.
      focusedNodeUuid: null,
      // Only want to center on first layout, then we'll rely on stored transform.
      isFirstLayout: true,
      nodeLayoutsByUuid: {},
      layoutCalcs: 0,
      dimensionChanges: 0,
      uncollapsedTaskNodeDimensionsByUuidChanges: 0,
      parentUuidByUuidChanges: 0,
      transformX: 0,
      transformY: 0,
      transformScale: 1,
    };
  },
  computed: {
    collapseConfig() {
      return this.$store.state.graph.collapseConfig;
    },
    runMetadata() {
      return this.$store.state.firexRunMetadata;
    },
    firexUid() {
      return this.runMetadata.uid;
    },
    allUuids() {
      return this.$store.getters['tasks/allTaskUuids'];
    },
    parentUuidByUuid() {
      return this.$store.getters['graph/parentUuidByUuid'];
    },
    childrenUuidsByUuid() {
      return this.$store.getters['graph/childrenUuidsByUuid'];
    },
    graphDataByUuid() {
      return this.$store.getters['graph/graphDataByUuid'];
    },
    runStateByUuid() {
      return this.$store.getters['tasks/runStateByUuid'];
    },
    dimensionsByUuid() {
      return this.$store.state.tasks.taskNodeSizeByUuid;
    },
    /**
     * Make collapse info available per UUID. Modifies tree structure when a sequence of collapsed
     * nodes exists.
     * */
    uncollapsedGraphByNodeUuid() {
      return this.$store.getters['graph/uncollapsedGraphByNodeUuid'];
    },
    uncollapsedTaskNodeDimensionsByUuid() {
      // Need to wait for all node dimensions to be loaded before we do any calcs (e.g. layout)
      // with any dimensions.
      if (this.isFirstLayout
        && !containsAll(_.keys(this.dimensionsByUuid), _.keys(this.uncollapsedGraphByNodeUuid))) {
        return {};
      }
      // for non-first layouts, we are only calculating dimensions for nodes that have
      // reported their dimensions, not necessarily all uncollapsed nodes.
      const sizedUncollapsedNodes = _.pick(this.uncollapsedGraphByNodeUuid,
        _.keys(this.dimensionsByUuid));

      return _.mapValues(
        sizedUncollapsedNodes,
        (collapseData, uuid) => _.merge({}, this.dimensionsByUuid[uuid],
          {
            uuid,
            width: this.dimensionsByUuid[uuid].width + collapseData.widthPadding,
            height: this.dimensionsByUuid[uuid].height + collapseData.heightPadding,
            parent_id: collapseData.parent_id,
          }),
      );
    },
    nonCollapsedNodesExtent() {
      return {
        top: _.min(_.map(this.nodeLayoutsByUuid, 'y')),
        left: _.min(_.map(this.nodeLayoutsByUuid, 'x')),
        right: _.max(_.map(this.nodeLayoutsByUuid, n => n.x + n.width)),
        bottom: _.max(_.map(this.nodeLayoutsByUuid, n => n.y + n.height)),
      };
    },
    hasFailures() {
      return _.some(_.values(this.runStateByUuid), { state: 'task-failed' });
    },
    svgGraphTransform() {
      return `translate(${this.transform.x},${this.transform.y})scale(${this.transform.scale})`;
    },
    isCollapsedByUuid() {
      return this.$store.getters['graph/isCollapsedByUuid'];
    },
    collapsedNodeUuids() {
      return _.keys(_.pickBy(this.isCollapsedByUuid));
    },
    hasCollapsedNodes() {
      return this.collapsedNodeUuids.length > 0;
    },
    uncollapsedNodeUuids() {
      return this.$store.getters['graph/uncollapsedNodeUuids'];
    },
    allDescendantsCollapsedByUuid() {
      return _.mapValues(this.uncollapsedGraphByNodeUuid,
        (collapseData, uuid) => containsAll(collapseData.collapsedUuids,
          this.graphDataByUuid[uuid].descendantUuids));
    },
    // Avoid re-calculating display ops on every data change.
    flameDataAndNameByUuid() {
      return this.$store.getters['tasks/flameDataAndNameByUuid'];
    },
    flameDataDisplayOperationsByUuid() {
      return this.$store.getters['graph/flameDataDisplayOperationsByUuid'];
    },
    userDisplayConfigOperationsByUuid() {
      return this.$store.getters['graph/userDisplayConfigOperationsByUuid'];
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
    // d3select('div#chart-container').on('mousedown', () => {
    //   console.profile();
    // });
    d3select('div#chart-container').call(zoom).on('dblclick.zoom', null);
    // this.$el.focus();

    // Registering listeners in 'created' some cause duplicate handlers to exist.
    eventHub.$on('center', this.center);
    eventHub.$on('node-focus', this.focusOnNode);
    eventHub.$on('ui-collapse', this.handleUiCollapseEvent);
    eventHub.$on('toggle-task-collapse', this.toggleCollapseDescendants);
    eventHub.$on('zoom', this.zoomed);
  },
  methods: {
    zoomed(transform) {
      // Null source events mean programatic zoom. We don't want to clear for programatic zooms.
      if (d3event.sourceEvent !== null) {
        // Clear focus node on non-programatic pan/zoom.
        this.focusedNodeUuid = null;
      }
      this.transform = transform;
      this.transformX = transform.x;
      this.transformY = transform.y;
      this.transformScale = transform.scale;
      // this.transform = {
      //   x: d3event.transform.x,
      //   y: d3event.transform.y,
      //   scale: d3event.transform.k,
      // };
      // console.log(d3event);
      // console.log(_.get(d3event, ['sourceEvent', 'type'], ''));
      // TODO: make transform top-level key per firexUid. This will avoid write slowdowns as other
      // per-run data grows.
      addLocalStorageData(this.firexUid, this.transform);
    },
    updateTransformViaZoom(transform) {
      // MUST MAINTAIN ZOOM'S INTERNAL STATE! Otherwise, subsequent pan/zooms are inconsistent
      // with current position.
      const d3Transform = zoomIdentity.translate(transform.x, transform.y).scale(transform.scale);
      // This call will create a d3event and pipe it through, just like manual pan/zooms.
      d3select('div#chart-container').call(zoom.transform, d3Transform);
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
        const nodeLayout = this.nodeLayoutsByUuid[uuid];
        const nodeRect = {
          left: nodeLayout.x,
          right: nodeLayout.x + nodeLayout.width,
          top: nodeLayout.y,
          bottom: nodeLayout.y + nodeLayout.height,
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
        cuuid => this.isCollapsedByUuid[cuuid]);
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

      const resultOpsByUuid = _.mapValues(event.operationsByUuid, (op, uuid) => {
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
        return resultOps;
      });
      this.$store.dispatch('graph/setCollapseOpsByUuid', resultOpsByUuid);

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
    isTransformValid(transform) {
      if (_.isNil(transform)) {
        return false;
      }
      const vals = [transform.x, transform.y, transform.scale];
      return _.every(_.map(vals, v => !_.isNil(v) && _.isNumber(v)));
    },
    getLocalStorageTransform() {
      const storedTransform = readPathsFromLocalStorage(this.firexUid, ['x', 'y', 'scale']);
      if (this.isTransformValid(storedTransform)) {
        return storedTransform;
      }
      // Default to the centering transform.
      return this.getCenterTransform();
    },
    setCollapseFilterState(obj) {
      // All keys not specified on object are disabled.
      this.$store.commit('graph/setCollapseConfig', {
        hideSuccessPaths: _.get(obj, 'hideSuccessPaths', false),
        uiCollapseOperationsByUuid: _.get(obj, 'uiCollapseOperationsByUuid', {}),
        applyDefaultCollapseOps: _.get(obj, 'applyDefaultCollapseOps', false),
      });
      // Changing filter state can significantly change visible nodes, so we center.
      this.center();
    },
    // eslint-disable-next-line
    // updateLayout: _.debounce(function () {
    //   const positionsByUuid = calculateNodesPositionByUuid(
    //     this.uncollapsedTaskNodeDimensionsByUuid,
    //   );
    //   this.nodeLayoutsByUuid = Object.freeze(_.mapValues(positionsByUuid,
    //     (p, uuid) => _.assign(p, _.pick(
    //       this.uncollapsedTaskNodeDimensionsByUuid[uuid], 'width', 'height',
    //     ))));
    // }, 1000, { maxWait: 2000, leading: true, trailing: true }),
    updateLayout() {
      const positionsByUuid = calculateNodesPositionByUuid(
        this.uncollapsedTaskNodeDimensionsByUuid,
      );
      this.nodeLayoutsByUuid = Object.freeze(_.mapValues(positionsByUuid,
        (p, uuid) => _.assign(p, _.pick(
          this.uncollapsedTaskNodeDimensionsByUuid[uuid], 'width', 'height',
        ))));
    },
  },
  watch: {
    firexUid: {
      handler() {
        this.isFirstLayout = true;
        this.nodeLayoutsByUuid = {};
        // load collapse config on firexUid change.
        this.$store.commit('graph/setCollapseConfig', getLocalStorageCollapseConfig(this.firexUid));
      },
      immediate: true,
    },
    nodeLayoutsByUuid(newVal) {
      // Need to load stored transform AFTER initial layout.
      if (!_.isEmpty(newVal) && this.isFirstLayout) {
        this.isFirstLayout = false;
        // TODO: combine localstorage reads, or cache at lower level.
        this.updateTransformViaZoom(this.getLocalStorageTransform());
      }
      this.layoutCalcs += 1;
    },
    collapseConfig: {
      handler() {
        addLocalStorageData(this.firexUid, {
          collapseConfig: this.collapseConfig,
        });
      },
      deep: true,
    },
    dimensionsByUuid: {
      handler() {
        // seems to be updated a reasonable amount (about once per task).
        this.dimensionChanges += 1;
      },
      deep: true,
    },
    // TODO: why the hell is this updated so often???
    uncollapsedTaskNodeDimensionsByUuid: {
      handler(newUncollapsedTaskNodeDimensionsByUuid, oldVal) {
        // TODO: how do you explain this? It's a computed property, why would this watcher be
        // called if it hasn't changed???
        if (!_.isEqual(newUncollapsedTaskNodeDimensionsByUuid, oldVal)) {
          // Need to wait for all node dimensions to be loaded before we do any calcs (e.g. layout)
          // with any dimensions.
          if (!_.isEmpty(newUncollapsedTaskNodeDimensionsByUuid)) {
            this.updateLayout();
          }
        }
        this.uncollapsedTaskNodeDimensionsByUuidChanges += 1;
      },
      deep: true,
      immediate: true,
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

  #chart-container {
    background: white;
    display: flex;
    align-items: center;
    justify-content: center;
    height: 100%;
    width: 100%;
  }
</style>
