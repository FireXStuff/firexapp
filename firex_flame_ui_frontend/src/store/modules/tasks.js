import _ from 'lodash';
import { defineStore } from 'pinia';

import {
  orderByTaskNum, hasIncompleteTasks, getDescendantUuids, twoDepthAssign, containsAny,
  isTaskStateIncomplete,
} from '../../utils';
import { useGraphStore } from './graph';
import { useFirexRunMetadataStore } from './firexRunMetadata';
import { useRouteStore } from './route';

export const useTasksStore = defineStore('tasks', {
  state: () => ({
    // Main task data structure.
    allTasksByUuid: {},
    apiConnected: false,
    // unfortunately we need to track this manually. TODO: look for a better way.
    taskNodeSizeByUuid: {},

    search: {
      term: '',
      selectedIndex: 0,
      resultUuids: [],
      isOpen: false,
    },
    // When set, fades-out other nodes. Is reset by pan/zoom events.
    focusedTaskUuid: null,
  }),

  getters: {

    selectedRoot: () => useRouteStore().params.rootUuid,

    tasksByUuid(state) {
      if (_.isNil(this.selectedRoot) || !_.has(state.allTasksByUuid, this.selectedRoot)) {
        return state.allTasksByUuid;
      }
      return this.descendantTasksByUuid(this.selectedRoot);
    },

    // TODO: could split this in to selected visible (via root) & literally all in order to
    //  cache layouts for all.
    allTaskUuids() {
      return _.keys(this.tasksByUuid);
    },

    runStateByUuid(state) {
      const childrenUuidsByUuid = useGraphStore().childrenUuidsByUuid;
      return _.mapValues(
        // note nodesByUuid can be updated before childrenUuidsByUuid, so add default.
        state.allTasksByUuid,
        n => _.assign(
          { isLeaf: _.get(childrenUuidsByUuid, n.uuid, []).length === 0 },
          _.pick(n, ['state', 'exception']),
        ),
      );
    },

    taskNameByUuid: state => _.mapValues(state.allTasksByUuid, 'name'),

    chainDepthByUuid: state => _.mapValues(state.allTasksByUuid, 'chain_depth'),

    flameHtmlsByUuid: state => _.mapValues(state.allTasksByUuid,
      task => _.map(
        _.reverse(_.sortBy(_.filter(
          _.get(task, 'flame_data', []),
          d => d.type === 'html',
        ), ['order'])),
        'value',
      )),

    // TODO: further prune to flame_data._default_display
    flameDataAndNameByUuid() {
      return _.mapValues(this.tasksByUuid,
        n => _.pick(n, ['flame_data', 'name', 'parent_id', 'uuid']));
    },

    additionalChildrenByUuid: state => _.mapValues(
      _.pickBy(state.allTasksByUuid, t => _.has(t, 'additional_children')),
      // Not all additional children exist, so exclude them since they can't be operated on.
      t => _.filter(t.additional_children,
        additionalChildUuid => _.has(state.allTasksByUuid, additionalChildUuid)),
    ),

    descendantTasksByUuid: state => (rootUuid) => {
      const descUuids = getDescendantUuids(rootUuid, state.allTasksByUuid);
      return orderByTaskNum(_.pick(state.allTasksByUuid, [rootUuid].concat(descUuids)));
    },

    // TODO: add support for custom root.
    hasIncompleteTasks: state => hasIncompleteTasks(state.allTasksByUuid),

    hasTasks: state => !_.isEmpty(state.allTasksByUuid),

    rootUuid(state) {
      if (
        !_.isNil(this.selectedRoot)
        && _.has(state.allTasksByUuid, this.selectedRoot)
      ) {
        return this.selectedRoot;
      }
      const metadataRootUuid = useFirexRunMetadataStore().root_uuid;
      if (!_.isNil(metadataRootUuid) && _.has(state.allTasksByUuid, metadataRootUuid)) {
        return metadataRootUuid;
      }
      // Server did not capture the root_uuid, fallback to searching graph.
      const nullParents = _.filter(state.allTasksByUuid, { parent_id: null });
      if (_.isEmpty(nullParents)) {
        let firstTask = _.minBy(_.values(state.allTasksByUuid), 'first_started');
        if (_.isNil(firstTask)) {
          return null; // no tasks, no root.
        }

        // Check if the server just missed the fact the root is the root, but have the task.
        while (
          firstTask.parent_id && _.has(state.allTasksByUuid, firstTask.parent_id)
        ) {
          firstTask = state.allTasksByUuid[firstTask.parent_id];
        }
        return firstTask.uuid;
      }
      return _.head(nullParents).uuid;
    },

    canRevoke(state) {
      return this.hasIncompleteTasks && state.apiConnected;
    },

    searchForUuids(state) {
      return (searchTerm) => {
        const lowerSearchTerm = searchTerm.toLowerCase();

        const searchFields = ['name', 'hostname', 'uuid'];
        const taskFieldMatchingUuids = _.keys(_.pickBy(state.allTasksByUuid,
          t => _.some(_.pick(t, searchFields),
            attr => _.includes(attr.toLowerCase(), lowerSearchTerm))));

        const flameHtmlMatchingUuids = _.keys(_.pickBy(this.flameHtmlsByUuid,
          flameDataHtmls => _.some(flameDataHtmls,
            flameDataHtml => _.includes(_.toLower(flameDataHtml), lowerSearchTerm))));

        return _.concat(taskFieldMatchingUuids, flameHtmlMatchingUuids);
      };
    },

    runStartTime(state) {
      return _.min(_.map(state.allTasksByUuid, 'first_started'));
    },

    runEndTime(state) {
      if (this.hasIncompleteTasks) {
        return Date.now() / 1000;
      }
      return _.max(_.map(state.allTasksByUuid,
        // Default to 0 since backend doesn't always fill in actual_runtime, even when runstate is
        // terminal.
        t => t.first_started + _.get(t, 'actual_runtime', 0)));
    },

    runDuration() {
      return this.runEndTime - this.runStartTime;
    },

  },

  actions: {

    // Avoid Vue dependency tracking by freezing tasksByUuid. This causes issues for large graphs.

    setTasks(tasksByUuid) {
      // Live update has performance issues for big runs. This should be fixed, but for now
      //  disable live update when there are many tasks.
      const taskCount = _.keys(tasksByUuid).length;
      if (taskCount > 3500) {
        useGraphStore().disableLiveUpdate();
      }
      this.allTasksByUuid = Object.freeze(tasksByUuid);
    },

    addTasksData(newDataByUuid) {
      // TODO: do other incremental updating (e.g. of graph structure, collapse data, etc) here
      // instead of doing a full re-calc every time a node is added.
      this.allTasksByUuid = Object.freeze(
        twoDepthAssign(this.allTasksByUuid, newDataByUuid),
      );
    },

    setInProgressTasksToIncomplete() {
      const incompleteTasksByUuid = _.pickBy(
        this.allTasksByUuid,
        t => isTaskStateIncomplete(t.state),
      );
      const approxRuntimesByUuid = _.mapValues(
        incompleteTasksByUuid,
        (t) => {
          if (t.first_started && t.latest_timestamp) {
            return t.latest_timestamp - t.first_started;
          }
          return undefined;
        },
      );
      const incompleteTaskUpdatesByUuid = _.mapValues(
        incompleteTasksByUuid,
        (t) => {
          // Always set the state to task-incomplete.
          const singleTaskUpdates = { state: 'task-incomplete' };
          if (
            !t.actual_runtime
            && t.first_started
            && t.latest_timestamp
          ) {
            const descendantEndTimes = _.map(
              this.descendantTasksByUuid(t.uuid),
              (descTask) => {
                const runtime = descTask.actual_runtime || _.get(approxRuntimesByUuid, descTask.uuid);
                if (runtime && descTask.first_started) {
                  return descTask.first_started + runtime;
                }
                return null;
              },
            );
            const maxEndTime = _.max(_.filter(descendantEndTimes));
            if (maxEndTime) {
              singleTaskUpdates.approx_runtime = maxEndTime - t.first_started;
            }
          }
          return singleTaskUpdates;
        },
      );
      this.addTasksData(incompleteTaskUpdatesByUuid);
    },

    clearTaskData() {
      this.setTasks({});
      this.clearTaskNodeSize();
    },

    addTaskNodeSize(taskNodeSizeByUuid) {
      this.taskNodeSizeByUuid = Object.freeze(
        _.assign({}, this.taskNodeSizeByUuid, taskNodeSizeByUuid),
      );
    },

    clearTaskNodeSize() {
      this.taskNodeSizeByUuid = {};
    },

    setApiConnected(apiConnected) {
      this.apiConnected = apiConnected;
    },

    setFocusedTaskUuid(newFocusedTaskUuid) {
      this.focusedTaskUuid = newFocusedTaskUuid;
    },

    // Named searchTasks, not search: Pinia puts state and actions in one namespace,
    // and `search` is already the search state object.
    searchTasks(searchSubmission) {
      if (searchSubmission.term !== this.search.term) {
        // New search term, submit new search.
        let resultUuids = this.searchForUuids(searchSubmission.term);
        if (searchSubmission.findUncollapsedAncestor) {
          // Find uncollapsed tasks that contain collapsed search results.
          const graphStore = useGraphStore();
          const collapsedUuids = graphStore.collapsedNodeUuids;
          const collapsedSearchResultUuids = _.intersection(resultUuids, collapsedUuids);
          const uncollapsedGraphByNodeUuid = graphStore.uncollapsedGraphByNodeUuid;
          const uncollapsedContainingResultUuids = _.keys(_.pickBy(uncollapsedGraphByNodeUuid,
            collapseDetails => containsAny(
              collapseDetails.collapsedUuids, collapsedSearchResultUuids,
            )));
          const uncollapsedSearchResultUuids = _.difference(resultUuids, collapsedUuids);
          const uncollapsedResultsAndCollapsedContaining = _.concat(uncollapsedContainingResultUuids,
            uncollapsedSearchResultUuids);
          // use all UUIDs intersection to maintain task_num order.
          resultUuids = _.intersection(this.allTaskUuids,
            uncollapsedResultsAndCollapsedContaining);
        }
        this.setTaskSearchResults({ term: searchSubmission.term, results: resultUuids });
      } else if (this.search.resultUuids.length > 0) {
        // Same search term as before, go from current result to the next.
        const resultCount = this.search.resultUuids.length;
        const nextIndex = (this.search.selectedIndex + 1) % resultCount;
        this.setSearchIndex(nextIndex);
      }
    },

    previousSearchResult() {
      const resultCount = this.search.resultUuids.length;
      if (resultCount > 0) {
        const nextIndex = (this.search.selectedIndex - 1 + resultCount) % resultCount;
        this.setSearchIndex(nextIndex);
      }
    },

    setTaskSearchResults(newSearch) {
      this.search = {
        term: newSearch.term,
        selectedIndex: 0,
        resultUuids: newSearch.results,
        isOpen: true,
      };
      if (newSearch.results.length > 0) {
        this.focusedTaskUuid = this.search.resultUuids[this.search.selectedIndex];
      }
    },

    setSearchIndex(newIndex) {
      this.search.selectedIndex = newIndex;
      this.focusedTaskUuid = this.search.resultUuids[newIndex];
    },

    closeSearch() {
      this.search.isOpen = false;
      this.focusedTaskUuid = null;
    },

    toggleSearchOpen() {
      this.search.isOpen = !this.search.isOpen;
    },

  },
});

export default useTasksStore;
