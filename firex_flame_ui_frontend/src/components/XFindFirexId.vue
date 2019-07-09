<template>
  <div class="container">
    <div>
      <img style='height: 150px;' src="../assets/firex_logo.png" alt="firex logo">
    </div>
    <div style="width: 40%">
      <input type="text" v-model.trim="firexId"
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
  isFireXIdValid, fetchUiConfig, fetchRunModelMetadata, redirectToFlameIfAlive, findRunPathSuffix,
} from '../utils';

export default {
  name: 'XFindFirexId',
  data() {
    return {
      firexId: '',
      // Lazy loaded on route enter.
      uiConfig: null,
    };
  },
  asyncComputed: {
    runMetadata:
      {
        get() {
          if (!this.isFirexIdValid || !this.uiConfig) {
            return false;
          }
          return fetchRunModelMetadata(this.firexId, this.uiConfig.model_path_template)
            .catch(() => false);
        },
        default: false,
      },
  },
  computed: {
    isFirexIdValid() {
      return isFireXIdValid(this.firexId);
    },
    errorMessage() {
      if (this.firexId) {
        if (!this.isFirexIdValid) {
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
        path: `/${this.firexId}${pathSuffix}`,
      });
    },
  },
  watch: {
    runMetadata(runMetadata) {
      if (runMetadata) {
        if (this.uiConfig.redirect_to_alive_flame) {
          redirectToFlameIfAlive(runMetadata.flame_url, this.$route.path)
          // If the flame is not still alive, take the user to the graph for the selected run.
            .catch(this.routeToInputFirexId);
        } else {
          this.routeToInputFirexId();
        }
      }
    },
  },
  beforeRouteEnter(to, from, next) {
    fetchUiConfig().then(uiConfig => next((vm) => {
      vm.firexId = to.params.inputFireXId;
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
