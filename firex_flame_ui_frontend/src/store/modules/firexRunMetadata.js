import _ from 'lodash';

const EMPTY_RUN_JSON = Object.freeze(
  {
    firex_id: null,
    revoked: null,
    revoked_details: null,
  }
)

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
  run_json: EMPTY_RUN_JSON,
};


// getters
const getters = {

  revokeDetails: state => {
    const revoked_details = state.run_json.revoked_details;
    if (!_.isEmpty(revoked_details)) {
      return {
        isRevoked: state.run_json.revoked,
        revokeReason: revoked_details.reason,
        revokeTimestamp: revoked_details.revoke_start_time,
        revokingUser: revoked_details.revoking_user,
      }
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
    _.each(_.keys(firexRunMetadata), (k) => {
      state[k] = _.get(firexRunMetadata, k, null);
    });
  },
  setRunJson(state, runJson) {
    // TODO: get run complete from run.json too?
    state.run_json = {
      firex_id: _.get(runJson, 'firex_id'),
      revoked: _.get(runJson, 'revoked'),
      revoked_details: _.get(runJson, 'revoked_details'),
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
