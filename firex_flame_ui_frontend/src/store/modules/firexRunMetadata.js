import _ from 'lodash';

const metaDataState = {
  uid: null,
  logs_dir: null,
  root_uuid: null,
  centralServer: null,
  flameServerUrl: null,
};

// getters
const getters = {

};

// actions
const actions = {
  setFlameRunMetadata(context, firexRunMetadata) {
    context.dispatch('tasks/clearTaskNodeSize');
    context.commit('setFlameRunMetadata', firexRunMetadata);
  },
};

// mutations
const mutations = {
  setFlameRunMetadata(state, firexRunMetadata) {
    _.each(_.keys(state), (k) => { state[k] = firexRunMetadata[k]; });
  },
};

export default {
  namespaced: true,
  state: metaDataState,
  getters,
  actions,
  mutations,
};
