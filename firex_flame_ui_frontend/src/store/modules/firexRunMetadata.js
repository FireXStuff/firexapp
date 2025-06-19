import _ from 'lodash';

const metaDataState = {
  uid: null,
  logs_dir: null,
  root_uuid: null,
  chain: null,
  logs_server: null,
  flame_url: null,
  run_complete: null,
  revoke_reason: null,
  revoke_timestamp: null,
  revokedDetails: {},
};


// getters
const getters = {

  revokeDetails: state => {
    if (_.get(state.revokedDetails, 'revokeTimestamp')) {
      return state.revokedDetails;
    }
    return { // legacy.
      isRevoked: Boolean(state.revoke_timestamp),
      revokeReason: state.revoke_reason,
      revokeTimestamp: state.revoke_timestamp,
      revokingUser: null,
    }
  },

};

// actions
const actions = {
  setFlameRunMetadata(context, firexRunMetadata) {
    context.dispatch('tasks/clearTaskNodeSize');
    context.dispatch('graph/enableLiveUpdate', null, { root: true });
    context.commit('setFlameRunMetadata', firexRunMetadata);
  },
};

// mutations
const mutations = {
  setFlameRunMetadata(state, firexRunMetadata) {
    _.each(_.keys(metaDataState), (k) => {
      state[k] = _.get(firexRunMetadata, k, null);
    });
  },
  setRunJson(state, runJson) {
    // TODO: get run complete from run.json too?
    state.revokedDetails = {
      isRevoked: _.get(runJson, 'revoked'),
      revokeReason: _.get(runJson, 'revoked_details.reason'),
      revokeTimestamp: _.get(runJson, 'revoked_details.revoke_start_time'),
      revokingUser: _.get(runJson, 'revoked_details.revoking_user'),
    }
  }
};

export default {
  namespaced: true,
  state: metaDataState,
  getters,
  actions,
  mutations,
};
