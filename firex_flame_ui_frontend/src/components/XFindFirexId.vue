<template>
  <div class="container">
    <div>
      <img style='height: 150px;' src="../assets/firex_logo.png" alt="firex logo">
    </div>
    <div style="width: 40%">
      <input type="text" v-model.trim="inputFireXId"
             style="width: 100%; text-align: center;"
             placeholder="Enter FireX ID like FireX-user-xxxxxx-xxxxxx-xxxx">
      <div v-if="errorMessage" class="error-message">
        {{errorMessage}}
      </div>
    </div>
  </div>
</template>

<script>
import { isFireXIdValid, fetchUiConfig, templateFireXId } from '../utils';

export default {
  name: 'XFindFirexId',
  data() {
    return {
      inputFireXId: '',
      // Lazy loaded on route enter.
      uiConfig: null,
    };
  },
  asyncComputed: {
    runMetadata:
      {
        get() {
          if (!this.isFirexIdValid) {
            return false;
          }
          return fetch(this.runMetadataModePath)
            .then(r => r.json(), () => false)
            .catch(() => false);
        },
        default: false,
      },
  },
  computed: {
    isFirexIdValid() {
      return isFireXIdValid(this.inputFireXId);
    },
    errorMessage() {
      if (this.inputFireXId) {
        if (!this.isFirexIdValid) {
          return 'The entered FireX ID is not valid.';
        }
        if (!this.runMetadata && !this.$asyncComputed.runMetadata.updating) {
          return 'No run data for FireX ID.';
        }
      }
      return null;
    },
    runMetadataModePath() {
      if (!this.isFirexIdValid) {
        return null;
      }
      return templateFireXId(this.uiConfig.model_path_template, this.inputFireXId);
    },
  },
  methods: {
    routeToInputFirexId() {
      this.$router.push({ name: 'XGraph', params: { inputFireXId: this.inputFireXId } });
    },
  },
  watch: {
    runMetadata(runMetadata) {
      if (runMetadata) {
        fetch((new URL('/alive', runMetadata.flame_url)).toString(), { mode: 'no-cors' })
          .then(
            // If the flame for the selected run is still alive, redirect the user there.
            // TODO: move where this redirect happens to XTTaskParent and maintain
            //  path on redirect, so that users are sent to /tasks/<uuid>, for example.
            () => {
              if (this.uiConfig.redirect_to_alive_flame) {
                window.location.href = runMetadata.flame_url;
              } else {
                this.routeToInputFirexId();
              }
            },
            // If the flame is not still alive, take the user to the graph for the selected run.
            () => this.routeToInputFirexId(),
          );
      }
    },
  },
  beforeRouteEnter(to, from, next) {
    fetchUiConfig().then(uiConfig => next((vm) => { vm.uiConfig = uiConfig; }));
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
