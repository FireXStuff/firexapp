import _ from 'lodash';

import { orderByTaskNum, hasIncompleteTasks, getDescendantUuids } from '../../utils';

const tasksState = {
  // Main task data structure.
  allTasksByUuid: {},
  selectedRoot: null,
  // TODO: this shouldn't be stored globally, but rather fetched by the attribute viewing
  // component. This requires API operations to be externalized from XParent.
  detailedTask: {},
  socketConnected: false,
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

  tasksByUuid(state, getters) {
    if (_.isNull(state.selectedRoot) || !_.has(state.allTasksByUuid, state.selectedRoot)) {
      return state.allTasksByUuid;
    }
    return getters.descendantTasksByUuid(state.selectedRoot);
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

  // TODO: further prune to flame_data._default_display
  flameDataAndNameByUuid: state => _.mapValues(state.allTasksByUuid,
    n => _.pick(n, ['flame_data', 'name', 'parent_id', 'uuid'])),

  descendantTasksByUuid: state => (rootUuid) => {
    const descUuids = getDescendantUuids(rootUuid, state.allTasksByUuid);
    return orderByTaskNum(_.pick(state.allTasksByUuid, [rootUuid].concat(descUuids)));
  },

  // TODO: add support for custom root.
  hasIncompleteTasks: state => hasIncompleteTasks(state.allTasksByUuid),

  hasTasks: state => !_.isEmpty(state.allTasksByUuid),

  rootUuid: (state, getters, rootState) => {
    if (!_.isNil(state.selectedRoot)) {
      return state.selectedRoot;
    }
    if (!_.isNil(rootState.firexRunMetadata.root_uuid)) {
      return rootState.firexRunMetadata.root_uuid;
    }
    // Fallback to searching graph.
    const nullParents = _.filter(state.allTasksByUuid, { parent_id: null });
    return _.isEmpty(nullParents) ? null : _.head(nullParents).uuid;
  },

  canRevoke: (state, getters) => getters.hasIncompleteTasks && state.socketConnected,

  searchForUuids: state => (searchTerm) => {
    const lowerSearchTerm = searchTerm.toLowerCase();
    const searchFields = ['name', 'hostname', 'flame_additional_data', 'uuid'];
    // TODO add support for flame_data (html entries only).
    return _.keys(_.pickBy(state.allTasksByUuid,
      t => _.some(_.pick(t, searchFields),
        attr => _.includes(attr.toLowerCase(), lowerSearchTerm))));
  },

};

// actions
const actions = {
  setDetailedTask(context, detailedTask) {
    const childrenUuids = context.rootGetters['graph/childrenUuidsByUuid'][detailedTask.uuid];
    const children = _.map(childrenUuids,
      uuid => ({ uuid, name: context.state.allTasksByUuid[uuid].name }));

    let parent;
    if (!_.isNil(detailedTask.parent_id)) {
      parent = {
        uuid: detailedTask.parent_id,
        name: context.state.allTasksByUuid[detailedTask.parent_id].name,
      };
    } else {
      parent = {};
    }
    context.commit('setDetailedTask', _.assign({ children, parent }, detailedTask));
  },

  setTasks(context, tasksByUuid) {
    context.commit('setTasks', Object.freeze(tasksByUuid));
  },

  addTasksData(context, newDataByUuid) {
    const newUuidData = _.pickBy(newDataByUuid,
      (t, u) => !_.has(context.state.allTasksByUuid, u));
    const updateUuidData = _.pickBy(newDataByUuid,
      (t, u) => _.has(context.state.allTasksByUuid, u));

    const newTasksByUuid = _.assign({}, context.state.allTasksByUuid, newUuidData);
    _.each(updateUuidData, (newTaskData, uuid) => {
      // New data fully replaces _attributes_ on a task, not entire tasks.
      newTasksByUuid[uuid] = _.assign({}, newTasksByUuid[uuid], newTaskData);
    });
    context.dispatch('setTasks', newTasksByUuid);
  },

  clearTaskData(context) {
    context.commit('setTasks', {});
    context.commit('clearTaskNodeSize');
    context.commit('setDetailedTask', {});
  },

  addTaskNodeSize(context, taskNodeSize) {
    context.commit('addTaskNodeSize', taskNodeSize);
  },

  selectRootUuid(context, newRootUuid) {
    context.commit('selectRootUuid', newRootUuid);
  },

  search(context, searchTerm) {
    if (searchTerm !== context.state.search.term) {
      // New search term, submit new search.
      const allResultUuids = context.getters.searchForUuids(searchTerm);
      // TODO: Find uncollapsed nodes that contain result collapsed nodes, don't just ignore!!!
      const uncollapsedUuids = context.rootGetters['graph/uncollapsedNodeUuids'];
      const uncollapsedResultUuids = _.intersection(allResultUuids, uncollapsedUuids);
      context.commit('setTaskSearchResults', { term: searchTerm, results: uncollapsedResultUuids });
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

  addTask(state, { task }) {
    state.allTasksByUuid = Object.freeze(_.assign({},
      state.allTasksByUuid, { [[task.uuid]]: task }));
  },

  setTasks(state, tasksByUuid) {
    state.allTasksByUuid = tasksByUuid;
  },

  setDetailedTask(state, detailedTask) {
    // TODO: add children [{name, uuid}] and parent {name, uuid} here.
    state.detailedTask = detailedTask;
  },

  setSocketConnected(state, newSocketConnected) {
    state.socketConnected = newSocketConnected;
  },

  addTaskNodeSize(state, taskNodeSizeByUuid) {
    state.taskNodeSizeByUuid = Object.freeze(
      _.assign({}, state.taskNodeSizeByUuid, taskNodeSizeByUuid),
    );
  },

  clearTaskNodeSize(state) {
    state.taskNodeSizeByUuid = {};
  },

  selectRootUuid(state, newRootUuid) {
    state.selectedRoot = newRootUuid;
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
