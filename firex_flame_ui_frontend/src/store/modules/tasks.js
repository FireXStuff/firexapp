import _ from 'lodash';

import {
  orderByTaskNum, hasIncompleteTasks, getDescendantUuids, twoDepthAssign, containsAny,
} from '../../utils';

const tasksState = {
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
};

// getters
const tasksGetters = {

  selectedRoot: (state, getters, rootState) => rootState.route.params.rootUuid,

  tasksByUuid(state, getters) {
    if (_.isNil(getters.selectedRoot) || !_.has(state.allTasksByUuid, getters.selectedRoot)) {
      return state.allTasksByUuid;
    }
    return getters.descendantTasksByUuid(getters.selectedRoot);
  },

  // TODO: could split this in to selected visible (via root) & literally all in order to
  //  cache layouts for all.
  allTaskUuids: (state, getters) => _.keys(getters.tasksByUuid),

  runStateByUuid: (state, getters, rootState, rootGetters) => _.mapValues(
    // note nodesByUuid can be updated before childrenUuidsByUuid, so add default.
    state.allTasksByUuid,
    n => _.assign(
      { isLeaf: _.get(rootGetters['graph/childrenUuidsByUuid'], n.uuid, []).length === 0 },
      _.pick(n, ['state', 'exception']),
    ),
  ),

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
  flameDataAndNameByUuid: state => _.mapValues(state.allTasksByUuid,
    n => _.pick(n, ['flame_data', 'name', 'parent_id', 'uuid'])),

  additionalChildrenByUuid: state => _.mapValues(
    _.pickBy(state.allTasksByUuid, t => _.has(t, 'additional_children')),
    'additional_children',
  ),

  descendantTasksByUuid: state => (rootUuid) => {
    const descUuids = getDescendantUuids(rootUuid, state.allTasksByUuid);
    return orderByTaskNum(_.pick(state.allTasksByUuid, [rootUuid].concat(descUuids)));
  },

  // TODO: add support for custom root.
  hasIncompleteTasks: state => hasIncompleteTasks(state.allTasksByUuid),

  hasTasks: state => !_.isEmpty(state.allTasksByUuid),

  rootUuid: (state, getters, rootState) => {
    if (!_.isNil(getters.selectedRoot)) {
      return getters.selectedRoot;
    }
    if (!_.isNil(rootState.firexRunMetadata.root_uuid)) {
      return rootState.firexRunMetadata.root_uuid;
    }
    // Fallback to searching graph.
    const nullParents = _.filter(state.allTasksByUuid, { parent_id: null });
    return _.isEmpty(nullParents) ? null : _.head(nullParents).uuid;
  },

  canRevoke: (state, getters) => getters.hasIncompleteTasks && state.apiConnected,

  searchForUuids: (state, getters) => (searchTerm) => {
    const lowerSearchTerm = searchTerm.toLowerCase();

    const searchFields = ['name', 'hostname', 'uuid'];
    const taskFieldMatchingUuids = _.keys(_.pickBy(state.allTasksByUuid,
      t => _.some(_.pick(t, searchFields),
        attr => _.includes(attr.toLowerCase(), lowerSearchTerm))));

    const flameHtmlMatchingUuids = _.keys(_.pickBy(getters.flameHtmlsByUuid,
      flameDataHtmls => _.some(flameDataHtmls,
        flameDataHtml => _.includes(_.toLower(flameDataHtml), lowerSearchTerm))));

    return _.concat(taskFieldMatchingUuids, flameHtmlMatchingUuids);
  },

  runStartTime(state) {
    return _.min(_.map(state.allTasksByUuid, 'first_started'));
  },

  runEndTime(state, getters) {
    if (getters.hasIncompleteTasks) {
      return Date.now() / 1000;
    }
    return _.max(_.map(state.allTasksByUuid,
      // Default to 0 since backend doesn't always fill in actual_runtime, even when runstate is
      // terminal.
      t => t.first_started + _.get(t, 'actual_runtime', 0)));
  },

  runDuration(state, getters) {
    return getters.runEndTime - getters.runStartTime;
  },

};

// actions
const actions = {

  setTasks(context, tasksByUuid) {
    context.commit('setTasks', Object.freeze(tasksByUuid));
  },

  addTasksData(context, newDataByUuid) {
    // TODO: do other incremental updating (e.g. of graph structure, collapse data, etc) here
    // instead of doing a full re-calc every time a node is added.
    context.commit('addTasksData', newDataByUuid);
  },

  clearTaskData(context) {
    context.commit('setTasks', {});
    context.commit('clearTaskNodeSize');
  },

  addTaskNodeSize(context, taskNodeSize) {
    context.commit('addTaskNodeSize', taskNodeSize);
  },

  selectRootUuid(context, newRootUuid) {
    context.commit('selectRootUuid', newRootUuid);
  },

  search(context, searchSubmission) {
    if (searchSubmission.term !== context.state.search.term) {
      // New search term, submit new search.
      let resultUuids = context.getters.searchForUuids(searchSubmission.term);
      if (searchSubmission.findUncollapsedAncestor) {
        // Find uncollapsed tasks that contain collapsed search results.
        const collapsedUuids = context.rootGetters['graph/collapsedNodeUuids'];
        const collapsedSearchResultUuids = _.intersection(resultUuids, collapsedUuids);
        const uncollapsedGraphByNodeUuid = context.getters['graph/uncollapsedGraphByNodeUuid'];
        const uncollapsedContainingResultUuids = _.keys(_.pickBy(uncollapsedGraphByNodeUuid,
          collapseDetails => containsAny(
            collapseDetails.collapsedUuids, collapsedSearchResultUuids,
          )));
        const uncollapsedSearchResultUuids = _.difference(resultUuids, collapsedUuids);
        const uncollapsedResultsAndCollapsedContaining = _.concat(uncollapsedContainingResultUuids,
          uncollapsedSearchResultUuids);
        // use all UUIDs intersection to maintain task_num order.
        resultUuids = _.intersection(context.getters.allTaskUuids,
          uncollapsedResultsAndCollapsedContaining);
      }
      context.commit('setTaskSearchResults',
        { term: searchSubmission.term, results: resultUuids });
    } else if (context.state.search.resultUuids.length > 0) {
      // Same search term as before, go from current result to the next.
      const resultCount = context.state.search.resultUuids.length;
      const nextIndex = (context.state.search.selectedIndex + 1) % resultCount;
      context.commit('setSearchIndex', nextIndex);
    }
  },

  previousSearchResult(context) {
    const resultCount = context.state.search.resultUuids.length;
    if (resultCount > 0) {
      const nextIndex = (context.state.search.selectedIndex - 1 + resultCount) % resultCount;
      context.commit('setSearchIndex', nextIndex);
    }
  },

};

// mutations
const mutations = {

  // Avoid Vue dependency tracking by freezing tasksByUuid. This causes issues for large graphs.

  addTasksData(state, newDataByUuid) {
    state.allTasksByUuid = Object.freeze(
      twoDepthAssign(state.allTasksByUuid, newDataByUuid),
    );
  },

  setTasks(state, tasksByUuid) {
    state.allTasksByUuid = tasksByUuid;
  },

  setApiConnected(state, apiConnected) {
    state.apiConnected = apiConnected;
  },

  addTaskNodeSize(state, taskNodeSizeByUuid) {
    state.taskNodeSizeByUuid = Object.freeze(
      _.assign({}, state.taskNodeSizeByUuid, taskNodeSizeByUuid),
    );
  },

  clearTaskNodeSize(state) {
    state.taskNodeSizeByUuid = {};
  },

  setFocusedTaskUuid(state, newFocusedTaskUuid) {
    state.focusedTaskUuid = newFocusedTaskUuid;
  },

  setTaskSearchResults(state, newSearch) {
    state.search = {
      term: newSearch.term,
      selectedIndex: 0,
      resultUuids: newSearch.results,
      isOpen: true,
    };
    if (newSearch.results.length > 0) {
      state.focusedTaskUuid = state.search.resultUuids[state.search.selectedIndex];
    }
  },

  setSearchIndex(state, newIndex) {
    state.search.selectedIndex = newIndex;
    state.focusedTaskUuid = state.search.resultUuids[newIndex];
  },

  closeSearch(state) {
    state.search.isOpen = false;
    state.focusedTaskUuid = null;
  },

  toggleSearchOpen(state) {
    state.search.isOpen = !state.search.isOpen;
  },

};

export default {
  namespaced: true,
  state: tasksState,
  getters: tasksGetters,
  actions,
  mutations,
};
