import _ from 'lodash';
import { defineStore } from 'pinia';

const EMPTY_RUN_JSON = Object.freeze(
  {
    firex_id: null,
    revoked: null,
    revoked_details: null,
  }
)

export const useFirexRunMetadataStore = defineStore('firexRunMetadata', {
  state: () => ({
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
  }),

  getters: {

    revokeDetails: (state) => {
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

  },

  actions: {
    setFlameRunMetadata(firexRunMetadata) {
      _.each(_.keys(firexRunMetadata), (k) => {
        this[k] = _.get(firexRunMetadata, k, null);
      });
    },

    setRunJson(runJson) {
      // TODO: get run complete from run.json too?
      this.run_json = {
        firex_id: _.get(runJson, 'firex_id'),
        revoked: _.get(runJson, 'revoked'),
        revoked_details: _.get(runJson, 'revoked_details'),
      }
    },
  },
});

export default useFirexRunMetadataStore;
