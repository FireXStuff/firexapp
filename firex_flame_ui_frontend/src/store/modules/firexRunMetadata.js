import _ from 'lodash';

const metaDataState = {
  uid: null,
  logs_dir: null,
  root_uuid: null,
  centralServer: null,
  flameServerUrl: null,
  chain: null,
  central_documentation_url: 'http://firex.cisco.com',
  centralServerUiPath: null,
};

// getters
const getters = {

  logsUrl(state) {
    // TODO: Is it even safe/reasonable to expect the central server to always serve the logs?
    //    Is it better to always serve relatively?
    const origin = state.centralServer;
    let logsUrl = '';
    if (!_.isNil(origin)) {
      logsUrl += origin;
    }
    logsUrl += state.logs_dir;
    return logsUrl;
  },
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
    _.each(_.keys(state), (k) => {
      if (_.has(firexRunMetadata, k)) {
        state[k] = firexRunMetadata[k];
      }
    });
  },
};

export default {
  namespaced: true,
  state: metaDataState,
  getters,
  actions,
  mutations,
};
