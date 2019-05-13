import _ from 'lodash';

import { orderByTaskNum, hasIncompleteTasks, getDescendantUuids } from '../../utils';

const tasksState = {
  items: [],
  checkoutStatus: null,
  allTasksByUuid: {},
  // TODO: populate from user selection.
  selectedRoot: null,
  // TODO: this shouldn't be stored in globally, but rather fetched by the attribute viewing
  // component. This requires API operations to be externalized from XParent.
  detailedTask: {},
  socketConnected: false,
  // unfortunately we need to track this manually. TODO: look for a better way.
  taskNodeSizeByUuid: {},
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

  getTaskById: state => uuid => state.allTasksByUuid[uuid],

  getTaskAttribute: state => (uuid, attr) => state.allTasksByUuid[uuid][attr],

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

  addTaskNodeSize(state, taskNodeSize) {
    const newData = { [taskNodeSize.uuid]: _.pick(taskNodeSize, ['width', 'height']) };
    state.taskNodeSizeByUuid = Object.freeze(_.assign({}, state.taskNodeSizeByUuid, newData));
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

};

export default {
  namespaced: true,
  state: tasksState,
  getters: tasksGetters,
  actions,
  mutations,
};
