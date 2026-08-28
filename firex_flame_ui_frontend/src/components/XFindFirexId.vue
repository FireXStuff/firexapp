<template>
  <div class="top-level">
    <div
      v-if="isRunDataMissing"
      class="alert alert-danger alert-dismissible fade show text-center"
      role="alert"
    >
      <h4>
        Failed to find {{ foundFirexId }}
      </h4>
      Run age may exceed retention policy. 
      <div>
        <a class="btn btn-primary" :href="restoreArchivedLogsUrl" role="button" style="margin: 0.5em;">
          <font-awesome-icon icon="box-open"/>
          Restore Archived Run
        </a>
      </div>
    </div>
    <div class="container">
      <div>
        <img style="height: 150px;" src="../assets/firex_logo.png" alt="firex logo">
      </div>
      <div style="width: 40%">
        <input type="text" v-model.trim="firexIdTextInput"
               style="width: 100%; text-align: center;"
               placeholder="Enter FireX ID like FireX-user-xxxxxx-xxxxxx-xxxx">
        <div v-if="errorMessage" class="error-message">
          {{ errorMessage }}
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import {
  fetchUiConfig, fetchRunModelMetadata, findRunPathSuffix, findFireXId,
} from '../utils';

export default {
  name: 'XFindFirexId',
  data() {
    return {
      firexIdTextInput: '',
      // Lazy loaded on route enter.
      uiConfig: null,
    };
  },
  asyncComputed: {
    runMetadata:
      {
        get() {
          if (!this.foundFirexId || !this.uiConfig) {
            return false;
          }
          return fetchRunModelMetadata(this.foundFirexId, this.uiConfig.model_path_template)
            .catch(() => false);
        },
        default: false,
      },
  },
  computed: {
    foundFirexId() {
      return this.firexIdTextInput ? findFireXId(this.firexIdTextInput) : null;
    },
    errorMessage() {
      if (this.firexIdTextInput) {
        if (!this.foundFirexId) {
          return 'The entered FireX ID is not valid.';
        }
        if (this.isRunDataMissing) {
          return 'No run data for FireX ID.';
        }
      }
      return null;
    },
    restoreArchivedLogsUrl() {
      return this.foundFirexId
        ? `${window.location.origin}/test_tracker/start/RestoreArchivedLogs?autoSubmit=true&_sfirex_id=${this.foundFirexId}`
        : null;
    },
    isRunDataMissing() {
      return this.foundFirexId && !this.runMetadata && !this.$asyncComputed.runMetadata.updating;
    },
  },
  methods: {
    routeToInputFirexId() {
      const pathSuffix = findRunPathSuffix(this.$route.path);
      this.$router.push({
        path: `/${this.foundFirexId}${pathSuffix}`,
      });
    },
  },
  watch: {
    runMetadata(runMetadata) {
      if (runMetadata) {
        this.routeToInputFirexId();
      }
    },
  },
  beforeRouteEnter(to, from, next) {
    fetchUiConfig().then(uiConfig => next((vm) => {
      vm.firexIdTextInput = to.params.inputFireXId;
      vm.uiConfig = uiConfig;
    }));
  },
};
</script>

<style scoped>
  .top-level {
    display: flex;
    flex-direction: column;
    height: 100vh;
    justify-content: flex-start; 
  }

  .container {
    flex: 1; 
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
  }

  .error-message {
    padding: 1px;
    background: salmon;
    width: 100%;
    text-align: center;
  }


</style>
