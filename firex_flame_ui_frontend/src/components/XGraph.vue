<template>
  <div style="width: 100%; height: 100%; overflow: hidden;" tabindex="1"
       @keydown.c="center"
       @keydown.up="translateBy(0, 30)"
       @keydown.down="translateBy(0, -30)"
       @keydown.left="translateBy(30, 0)"
       @keydown.right="translateBy(-30, 0)"
  >
    <div id="chart-container" ref="graph-svg">
        <svg preserveAspectRatio="xMinYMin" style="width: 100%; height: 100%;">
          <g :transform="svgGraphTransform">
            <x-link
              :parentUuidByUuid="uncollapsedParentUuidByUuid"
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
import { mapState } from 'vuex';

import XSvgTaskNodes from './nodes/XSvgTaskNodes.vue';
import XTaskCapturingNodes from './nodes/XSizeCapturingNodes.vue';
import XLink from './XLinks.vue';
import {
  eventHub, calculateNodesPositionByUuid, getCenteringTransform, containsAll,
} from '../utils';
import {
  createUiCollapseOp,
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
const scaleBounds = { max: 2, min: 0.05 };

// TODO: likely should attach this to the component after confirming no preformance impacts.
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
      // Only want to center on first layout, then we'll rely on stored transform.
      nodeLayoutsByUuid: {},
    };
  },
  computed: {
    ...mapState({
      isFirstLayout: state => state.graph.isFirstLayout,
    }),
    collapseConfig() {
      return this.$store.state.graph.collapseConfig;
    },
    uiCollapseStateByUuid() {
      return this.$store.state.graph.collapseConfig.uiCollapseStateByUuid;
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
    uncollapsedParentUuidByUuid() {
      return _.mapValues(this.uncollapsedTaskNodeDimensionsByUuid, 'parent_id');
    },
    nonCollapsedNodesExtent() {
      return {
        top: _.min(_.map(this.nodeLayoutsByUuid, 'y')),
        left: _.min(_.map(this.nodeLayoutsByUuid, 'x')),
        right: _.max(_.map(this.nodeLayoutsByUuid, n => n.x + n.width)),
        bottom: _.max(_.map(this.nodeLayoutsByUuid, n => n.y + n.height)),
      };
    },
    svgGraphTransform() {
      return `translate(${this.transform.x},${this.transform.y})scale(${this.transform.scale})`;
    },
    isCollapsedByUuid() {
      return this.$store.getters['graph/isCollapsedByUuid'];
    },
    uncollapsedNodeUuids() {
      return this.$store.getters['graph/uncollapsedNodeUuids'];
    },
    focusedNodeUuid() {
      return this.$store.state.tasks.focusedTaskUuid;
    },
  },
  created() {
    // TODO: replace with action listeners.
    eventHub.$on('center', this.center);
    eventHub.$on('show-collapsed-tasks', this.showTasksCollapsedTo);
    eventHub.$on('toggle-task-collapse', this.toggleCollapseDescendants);
    eventHub.$on('zoom', this.zoomed);
  },
  beforeDestroy() {
    eventHub.$off('show-collapsed-tasks');
    eventHub.$off('toggle-task-collapse');
    eventHub.$off('zoom');
    eventHub.$off('center');
  },
  mounted() {
    d3select('div#chart-container').call(zoom).on('dblclick.zoom', null);
    this.$el.focus();
  },
  methods: {
    zoomed(transform) {
      // Null source events mean programatic zoom. We don't want to clear for programatic zooms.
      if (d3event.sourceEvent !== null && this.focusedNodeUuid !== null) {
        // Clear focus node on non-programatic pan/zoom.
        this.$store.commit('tasks/setFocusedTaskUuid', null);
      }
      this.transform = transform;
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
    translateBy(x, y) {
      d3select('div#chart-container').call(zoom.translateBy, x, y);
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
     * @param uuid
     */
    toggleCollapseDescendants(uuid) {
      let uiCollapseEvent;
      if (this.allDescendantsCollapsed(uuid)) {
        // All descendants are collapsed.
        const isCollapsedByUiOp = _.has(this.uiCollapseStateByUuid, uuid);

        if (isCollapsedByUiOp) {
          // TODO: removing this op doesn't necessarily create the pressure to expand.
          // If default state is collapsed, then nothing happens, which is wrong.
          // console.log('all desc collapsed currently collapsed by UI op -- removing');
          uiCollapseEvent = { uuid, remove: true };
        } else {
          uiCollapseEvent = {
            uuid,
            remove: false,
            add_operation: {
              operation: 'expand',
              target: 'descendants',
            },
          };
        }
      } else if (this.noChildCollapsed(uuid)) {
        // There are a couple different reasons why no child is collapsed, and each
        // require a different action.

        // TODO: can deny_child_collapse be lumped in with 'has any UI op' case?
        const uuidOp = _.get(
          this.uiCollapseStateByUuid, [uuid, 'operation'], false,
        );
        const isChildCollapseDenied = uuidOp === 'deny_child_collapse';

        if (isChildCollapseDenied) {
          // clear child collapse denial operation.
          // console.log('child collapsed currently denied -- removing');
          uiCollapseEvent = { uuid, remove: true };
        } else {
          const isExpandedByUiOp = this.isExpandedByUiOp(uuid);

          if (isExpandedByUiOp) {
            // TODO: Just because children are exapnded by UI op doesn't mean removing the
            // op ADDs pressure to collapse.
            // console.log('all children expanded by UI op -- removing');
            uiCollapseEvent = { uuid, remove: true };
          } else {
            // Not caused by existing operation, create a collapse operation.
            uiCollapseEvent = {
              uuid,
              remove: false,
              add_operation: {
                operation: 'collapse',
                target: 'descendants',
              },
            };
          }
        }
      } else {
        // Partial collapse: collapse all descendants.
        const isExpandedByUiOp = this.isExpandedByUiOp(uuid);
        if (isExpandedByUiOp) {
          // console.log('partial collapse currently expanded by UI op -- removing');
          uiCollapseEvent = { uuid, remove: true };
        } else {
          uiCollapseEvent = {
            uuid,
            remove: false,
            add_operation: {
              operation: 'collapse',
              target: 'descendants',
            },
          };
        }
      }
      this.handleUiCollapseEvent(uiCollapseEvent);
    },
    isExpandedByUiOp(uuid) {
      if (_.has(this.uiCollapseStateByUuid, uuid)) {
        const op = this.uiCollapseStateByUuid[uuid].operation;
        return op === 'expand';
      }
      return false;
    },
    // isCollapsedByUiOp(uuid) {
    //   if (_.has(this.uiCollapseStateByUuid, uuid)) {
    //     const op = this.uiCollapseStateByUuid[uuid].operation;
    //     return op === 'collapse';
    //   }
    //   return false;
    // },
    noChildCollapsed(uuid) {
      return _.every(this.childrenUuidsByUuid[uuid], cuuid => !this.isCollapsedByUuid[cuuid]);
    },
    allDescendantsCollapsed(uuid) {
      const collapseData = this.uncollapsedGraphByNodeUuid[uuid];
      return containsAll(collapseData.collapsedUuids, this.graphDataByUuid[uuid].descendantUuids);
    },
    showTasksCollapsedTo({ uuid }) {
      let uiCollapseEvent;
      if (this.allDescendantsCollapsed(uuid)) {
        // All collapsed, just expand all.
        console.log('all desc collapsed -- expanding descs');
        uiCollapseEvent = {
          uuid,
          remove: false,
          add_operation: {
            operation: 'expand',
            target: 'descendants',
          },
        };

      // else if (this.noChildCollapsed(uuid)) {
      //   // Not possible -- that means nothing is collapsed to the supplied uuid.
      // }
      } else {
        // Partial collapse: showing collapsed tasks means preventing them from collapsing to the
        // supplied UUID.
        uiCollapseEvent = {
          uuid,
          remove: false,
          add_operation: {
            operation: 'deny_child_collapse',
            target: 'self',
          },
        };
      }
      this.handleUiCollapseEvent(uiCollapseEvent);
    },
    handleUiCollapseEvent(event) {
      const initialRelPos = this.getCurrentRelPos(event.uuid);

      console.log(event);

      let resultOpsByUuid;
      if (event.remove) {
        resultOpsByUuid = _.omit(this.uiCollapseStateByUuid,
          _.concat(this.graphDataByUuid[event.uuid].descendantUuids, event.uuid));
      } else {
        const toAdd = {
          [[event.uuid]]: createUiCollapseOp(
            event.add_operation.operation, event.add_operation.target,
          ),
        };
        resultOpsByUuid = Object.assign({}, this.uiCollapseStateByUuid, toAdd);
      }
      this.$store.dispatch('graph/setUiCollapseStateByUuid', resultOpsByUuid);

      // Since we're changing the nodes being displayed, the layout might drastically change.
      // Maintain the position of the node whose descendants have been added/removed so that
      // the user remains oriented.
      this.$nextTick(() => {
        const nextRelPos = this.getCurrentRelPos(event.uuid);
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
    updateLayout() {
      const positionsByUuid = calculateNodesPositionByUuid(
        this.uncollapsedTaskNodeDimensionsByUuid,
      );
      // TODO: make this a regular computed property?
      this.nodeLayoutsByUuid = Object.freeze(_.mapValues(positionsByUuid,
        (p, uuid) => _.assign(p, _.pick(
          this.uncollapsedTaskNodeDimensionsByUuid[uuid], 'width', 'height',
        ))));
    },
  },
  watch: {
    firexUid: {
      handler() {
        this.$store.commit('graph/setIsFirstLayout', true);
        this.nodeLayoutsByUuid = {};
        // load collapse config for the newly accessed FireX run.
        this.$store.commit('graph/setCollapseConfig', getLocalStorageCollapseConfig(this.firexUid));
      },
      immediate: true,
    },
    nodeLayoutsByUuid(newVal) {
      // Need to load stored transform AFTER initial layout.
      if (!_.isEmpty(newVal) && this.isFirstLayout) {
        // Want first render not to fade, but second render to fade-in new nodes.
        // Without next tick, every render has isFirstLayout false.
        this.$nextTick(() => this.$store.commit('graph/setIsFirstLayout', false));
        // TODO: combine localstorage reads, or cache at lower level.
        this.updateTransformViaZoom(this.getLocalStorageTransform());
      }
    },
    collapseConfig: {
      handler() {
        addLocalStorageData(this.firexUid, {
          collapseConfig: this.collapseConfig,
        });
      },
      deep: true,
    },
    // TODO: why the hell is this updated so often???
    uncollapsedTaskNodeDimensionsByUuid: {
      handler(newUncollapsedTaskNodeDimensionsByUuid, oldVal) {
        // TODO: figure out why this watcher as being called even when _.isEqual is false.
        // The layout is expensive and it's important to avoid unnecessary layouts.
        if (!_.isEqual(newUncollapsedTaskNodeDimensionsByUuid, oldVal)) {
          // Need to wait for all node dimensions to be loaded before we do any calcs (e.g. layout)
          // with any dimensions.
          if (!_.isEmpty(newUncollapsedTaskNodeDimensionsByUuid)) {
            this.updateLayout();
          }
        }
      },
      deep: true,
      immediate: true,
    },
    focusedNodeUuid(newValue) {
      if (!_.isNull(newValue)) {
        // TODO: handle focusing on nodes that are collapsed.
        this.updateTransformViaZoom(this.getCenterOnNodeTransform(newValue));
      }
    },
  },
};
</script>

<style scoped>
  * {
    box-sizing: border-box;
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
