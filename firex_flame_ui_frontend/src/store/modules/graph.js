import _ from 'lodash';
import { defineStore } from 'pinia';

import { getChildrenUuidsByUuid, getDescendantsByUuid, getGraphDataByUuid } from '../../graph-utils';
import {
  getPrioritizedTaskStateBackground, concatArrayMergeCustomizer, createRunStateExpandOperations, orderByTaskNum,
} from '../../utils';
import {
  prioritizeCollapseOps, resolveDisplayConfigsToOpsByUuid, stackOffset, stackCount,
  resolveCollapseStatusByUuid, getCollapsedGraphByNodeUuid, normalizeUiCollaseStateToOps,
} from '../../collapse';
import { loadDisplayConfigs } from '../../persistance';
import { useTasksStore } from './tasks';
import { useFirexRunMetadataStore } from './firexRunMetadata';

export const useGraphStore = defineStore('graph', {
  state: () => ({
    collapseConfig: {
      uiCollapseStateByUuid: {},
      hideSuccessPaths: false,
      applyDefaultCollapseOps: true,
    },
    liveUpdate: true,
    showTaskDetails: false,
    isFirstLayout: true,
  }),

  getters: {

    // TODO: update incrementally with each new parent/child relationship.
    graph() {
      const tasksStore = useTasksStore();
      // TODO: does it make sense to keep data for entire tree even when a custom root is selected?
      const parentUuidByUuid = _.mapValues(tasksStore.allTasksByUuid, 'parent_id');
      const childrenUuidsByUuid = getChildrenUuidsByUuid(parentUuidByUuid);
      const graphDataByUuid = getGraphDataByUuid(
        tasksStore.rootUuid,
        parentUuidByUuid,
        childrenUuidsByUuid,
        tasksStore.chainDepthByUuid,
      );
      return {
        parentUuidByUuid,
        childrenUuidsByUuid,
        graphDataByUuid,
      };
    },

    parentUuidByUuid() {
      return this.graph.parentUuidByUuid;
    },

    childrenUuidsByUuid() {
      return this.graph.childrenUuidsByUuid;
    },

    graphDataByUuid() {
      return this.graph.graphDataByUuid;
    },

    childrenAndAdditionalUuidsByUuid() {
      const tasksStore = useTasksStore();
      const unsortedChildrenAndAdditionalUuidsByUuid = _.mergeWith({},
        this.graph.childrenUuidsByUuid,
        tasksStore.additionalChildrenByUuid,
        concatArrayMergeCustomizer);
      return _.mapValues(unsortedChildrenAndAdditionalUuidsByUuid,
        childrenUuids => _.sortBy(childrenUuids, u => tasksStore.allTasksByUuid[u].task_num));
    },

    tasksIncludingAdditionalDescendantsByUuid() {
      const tasksStore = useTasksStore();
      const rootUuid = tasksStore.rootUuid;
      const childrenUuidsByUuid = this.childrenAndAdditionalUuidsByUuid;
      if (_.has(childrenUuidsByUuid, rootUuid)) {
        const descUuidsByUuid = getDescendantsByUuid(rootUuid, childrenUuidsByUuid);
        const uuids = _.concat(_.flatten(_.values(descUuidsByUuid)), rootUuid);
        return orderByTaskNum(_.pick(tasksStore.allTasksByUuid, uuids));
      }
      return {};
    },

    parentAndAdditionalUuidsByUuid() {
      const tasksStore = useTasksStore();
      // Initialize with an array containing 'real' (non-additional) parent.
      const additionalParentsByUuid = _.mapValues(this.graph.parentUuidByUuid,
        (uuid) => {
          if (uuid) {
            return [uuid];
          }
          // Do not include null parent (e.g. root's parent) in list of uuids.
          return [];
        });

      _.each(tasksStore.additionalChildrenByUuid, (additionalChildren, pUuid) => {
        _.each(additionalChildren, (childUuid) => {
          if (_.has(additionalParentsByUuid, childUuid)) {
            additionalParentsByUuid[childUuid].push(pUuid);
          }
        });
      });
      return _.mapValues(additionalParentsByUuid,
        parentUuids => _.sortBy(_.uniq(parentUuids),
          u => tasksStore.allTasksByUuid[u].task_num));
    },

    flameDataDisplayOperationsByUuid() {
      const flameDataAndNameByUuid = useTasksStore().flameDataAndNameByUuid;
      // TODO: Each task can send updates that should override previous op entries for that task.
      //  Do that filtering here.
      const flameDataOps = _.flatMap(flameDataAndNameByUuid,
        n => _.get(n, ['flame_data', '_default_display', 'value'], []));
      const resolvedOpsByUuid = resolveDisplayConfigsToOpsByUuid(flameDataOps,
        flameDataAndNameByUuid);

      // The tree that ops from flameData operate on are different thanthe UI hierarchy. Specifically,
      // flame collapse ops operate on a tree that ignores chains-to-descendant relationships.
      // We therefore transform 'descendants' to 'unchained-descendants' for all targets from
      // flame data ops.
      return _.mapValues(resolvedOpsByUuid, ops => _.map(ops, (op) => {
        const transformedTargets = _.map(op.targets,
          (t) => {
            if (t === 'descendants') {
              return 'unchained-descendants';
            }
            return t;
          });
        return _.assign({}, op, { targets: transformedTargets });
      }));
    },

    userDisplayConfigOperationsByUuid() {
      // TODO: note this state is coming from local storage, should use library for syncing
      // localStorage state with app (store) state.
      const displayConfigs = loadDisplayConfigs();
      return resolveDisplayConfigsToOpsByUuid(displayConfigs,
        useTasksStore().flameDataAndNameByUuid);
    },

    runStateExpandOperationsByUuid() {
      return createRunStateExpandOperations(useTasksStore().runStateByUuid);
    },

    showOnlyRunStateCollapseOperationsByUuid() {
      const rootCollapse = {
        [[useFirexRunMetadataStore().root_uuid]]:
          [{ targets: ['descendants'], operation: 'collapse' }],
      };
      return _.assign({}, rootCollapse, this.runStateExpandOperationsByUuid);
    },

    mergedCollapseStateSources(state) {
      const enabledCollapseStateSources = [];
      if (state.collapseConfig.applyDefaultCollapseOps) {
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
      if (state.collapseConfig.hideSuccessPaths) {
        enabledCollapseStateSources.push(
          prioritizeCollapseOps(
            this.showOnlyRunStateCollapseOperationsByUuid, 'runState',
          ),
        );
      }
      enabledCollapseStateSources.push(
        normalizeUiCollaseStateToOps(state.collapseConfig.uiCollapseStateByUuid),
      );
      return _.mergeWith({}, ...enabledCollapseStateSources, concatArrayMergeCustomizer);
    },

    resolvedCollapseStateByUuid() {
      const rootUuid = useTasksStore().rootUuid;
      if (!_.has(this.graphDataByUuid, rootUuid)) {
        return {};
      }
      return resolveCollapseStatusByUuid(
        rootUuid,
        this.graphDataByUuid,
        this.mergedCollapseStateSources,
      );
    },

    isCollapsedByUuid() {
      return _.mapValues(this.resolvedCollapseStateByUuid, 'collapsed');
    },

    collapsedNodeUuids() {
      return _.keys(_.pickBy(this.isCollapsedByUuid));
    },

    uncollapsedNodeUuids() {
      return _.keys(_.omitBy(this.isCollapsedByUuid));
    },

    uncollapsedGraphByNodeUuid() {
      const tasksStore = useTasksStore();
      const uncollapsedGraphByUuid = _.pick(
        getCollapsedGraphByNodeUuid(
          tasksStore.rootUuid,
          this.childrenUuidsByUuid,
          this.isCollapsedByUuid,
        ),
        this.uncollapsedNodeUuids,
      );
      const runStateByUuid = tasksStore.runStateByUuid;
      return _.mapValues(uncollapsedGraphByUuid,
        (collapseData, uuid) => ({
          background: getPrioritizedTaskStateBackground(
            _.map(collapseData.collapsedUuids,
              u => runStateByUuid[u].state),
          ),
          collapsedUuids: collapseData.collapsedUuids,
          // We don't want the width to vary if a node has collapsed children or not,
          // so pad the same either way.
          // x2 because we pad both left and right to keep front box centering.
          widthPadding: stackCount * stackOffset * 2,
          // Only pad vertical to make room for real stacked behind boxes.
          heightPadding: _.isEmpty(collapseData.collapsedUuids) ? 0 : stackCount * stackOffset,
          parent_id: collapseData.parentId,
          task_num: tasksStore.allTasksByUuid[uuid].task_num,
        }));
    },

  },

  actions: {
    toggleShowTaskDetails() {
      this.showTaskDetails = !this.showTaskDetails;
    },

    toggleLiveUpdate() {
      this.liveUpdate = !this.liveUpdate;
    },

    setUiCollapseStateByUuid(uiCollapseStateByUuid) {
      this.collapseConfig = Object.assign({},
        this.collapseConfig, { uiCollapseStateByUuid });
    },

    expandAll() {
      this.setCollapseConfigWithDefaults({});
    },

    restoreCollapseDefault() {
      this.setCollapseConfigWithDefaults({ applyDefaultCollapseOps: true });
    },

    collapseSuccessPaths() {
      this.setCollapseConfigWithDefaults({ hideSuccessPaths: true });
    },

    setCollapseConfigWithDefaults(partialConfig) {
      this.setCollapseConfig({
        hideSuccessPaths: _.get(partialConfig, 'hideSuccessPaths', false),
        uiCollapseStateByUuid: _.get(partialConfig, 'uiCollapseStateByUuid', {}),
        applyDefaultCollapseOps: _.get(partialConfig, 'applyDefaultCollapseOps', false),
      });
    },

    setCollapseConfig(collapseConfig) {
      this.collapseConfig = collapseConfig;
    },

    disableLiveUpdate() {
      this.setLiveUpdate(false);
    },

    enableLiveUpdate() {
      this.setLiveUpdate(true);
    },

    setLiveUpdate(newLiveUpdate) {
      this.liveUpdate = newLiveUpdate;
    },

    setIsFirstLayout(newIsFirstLayout) {
      this.isFirstLayout = newIsFirstLayout;
    },

  },
});

export default useGraphStore;
