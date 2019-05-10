import _ from 'lodash';
import Vue from 'vue';

import { orderByTaskNum, hasIncompleteTasks, getDescendantUuids } from '../../utils';

const tasksState = {
  items: [],
  checkoutStatus: null,
  tasksByUuid: {},
  // TODO: populate from user selection.
  selectedRoot: null,
  // TODO: this shouldn't be stored in globally, but rather fetched by the attribute viewing
  // component. This requires API operations to be externalized from XParent.
  detailedTask: {},
  socketConnected: false,
  // unfortunately we need to track this manually. TODO: look for a better way.
  taskNodeSizeByUuid: {},
};

// getters
const tasksGetters = {

  allTaskUuids: state => _.keys(state.tasksByUuid),

  getTaskById: state => uuid => state.tasksByUuid[uuid],

  getTaskAttribute: state => (uuid, attr) => state.tasksByUuid[uuid][attr],

  runStateByUuid: (state, getters, rootState, rootGetters) => _.mapValues(
    // note nodesByUuid can be updated before childrenUuidsByUuid, so add default.
    state.tasksByUuid,
    n => _.assign(
      { isLeaf: _.get(rootGetters['graph/childrenUuidsByUuid'], n.uuid, []).length === 0 },
      _.pick(n, ['state', 'exception']),
    ),
  ),

  // TODO: further prune to flame_data._default_display
  flameDataAndNameByUuid: state => _.mapValues(state.tasksByUuid,
    n => _.pick(n, ['flame_data', 'name', 'parent_id', 'uuid'])),

  descendantTasksByUuid: state => (rootUuid) => {
    const descUuids = getDescendantUuids(rootUuid, state.tasksByUuid);
    return orderByTaskNum(_.pick(state.tasksByUuid, [rootUuid].concat(descUuids)));
  },

  // TODO: add support for custom root.
  hasIncompleteTasks: state => hasIncompleteTasks(state.tasksByUuid),

  hasTasks: state => !_.isEmpty(state.tasksByUuid),

  // TODO: get rid of this, sent single root UUID, either user selected or from metadata.
  rootUuid: (state) => {
    const nullParents = _.filter(state.tasksByUuid, { parent_id: null });
    return _.isEmpty(nullParents) ? null : _.head(nullParents).uuid;
  },

  canRevoke: (state, getters) => getters.hasIncompleteTasks && state.socketConnected,

};

// actions
const actions = {
  setDetailedTask(context, detailedTask) {
    const childrenUuids = context.rootGetters['graph/childrenUuidsByUuid'][detailedTask.uuid];
    const children = _.map(childrenUuids,
      uuid => ({ uuid, name: context.state.tasksByUuid[uuid].name }));

    let parent;
    if (!_.isNil(detailedTask.parent_id)) {
      parent = {
        uuid: detailedTask.parent_id,
        name: context.state.tasksByUuid[detailedTask.parent_id].name,
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
      (t, u) => !_.has(context.state.tasksByUuid, u));
    const updateUuidData = _.pickBy(newDataByUuid,
      (t, u) => _.has(context.state.tasksByUuid, u));

    const newTasksByUuid = _.assign({}, context.state.tasksByUuid, newUuidData);
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
};

// mutations
const mutations = {

  // Avoid Vue dependency tracking by freezing tasksByUuid. This causes issues for large graphs.

  addTask(state, { task }) {
    state.tasksByUuid = Object.freeze(_.assign({}, state.tasksByUuid, { [[task.uuid]]: task }));
  },

  setTasks(state, tasksByUuid) {
    state.tasksByUuid = tasksByUuid;
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

};

export default {
  namespaced: true,
  state: tasksState,
  getters: tasksGetters,
  actions,
  mutations,
};
