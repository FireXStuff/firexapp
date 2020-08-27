<template>
  <div class="container">
    <div>
      <img style='height: 150px;' src="../assets/firex_logo.png" alt="firex logo">
    </div>
    <div style="width: 40%">
      <input type="text" v-model.trim="firexIdTextInput"
             style="width: 100%; text-align: center;"
             placeholder="Enter FireX ID like FireX-user-xxxxxx-xxxxxx-xxxx">
      <div v-if="errorMessage" class="error-message">
        {{errorMessage}}
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
        if (!this.runMetadata && !this.$asyncComputed.runMetadata.updating) {
          return 'No run data for FireX ID.';
        }
      }
      return null;
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
  .container {
    width: 100%;
    height: 100%;
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
