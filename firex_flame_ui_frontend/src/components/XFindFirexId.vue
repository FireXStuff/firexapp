<template>
  <div class="container">
    <div>
      <img style='height: 150px;' src="../assets/firex_logo.png" alt="firex logo">
    </div>
    <div style="width: 40%">
      <input type="text" v-model.trim="inputFireXId" @keydown.enter="findFirexId"
             style="width: 100%; text-align: center;"
             placeholder="Enter FireX ID like FireX-user-xxxxxx-xxxxxx-xxxx">
      <div v-if="errorMessage" class="error-message">
        {{errorMessage}}
      </div>
    </div>
  </div>
</template>

<script>
import { isFireXIdValid, getFireXIdParts } from '../utils';

export default {
  name: 'XFindFirexId',
  data() {
    return {
      inputFireXId: '',
    };
  },
  asyncComputed: {
    runMetadata:
      {
        get() {
          if (!this.isFirexIdValid) {
            return false;
          }
          return fetch(this.runMetadataModePath).then(r => r.json(), () => false);
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
          return 'Could not find logs for FireX ID.';
        }
      }
      return null;
    },
    runMetadataModePath() {
      if (!this.isFirexIdValid) {
        return null;
      }
      const p = getFireXIdParts(this.inputFireXId);
      // TODO: get format from UI config stored on server.
      return `/auto/firex-logs/${p.year}/${p.month}/${p.day}/${p.firex_id}/flame_model/run-metadata.json`;
    },
  },
  watch: {
    runMetadata(runMetadata) {
      if (runMetadata) {
        fetch((new URL('/alive', runMetadata.flame_url)).toString(), { mode: 'no-cors' })
          .then(
            // If the flame for the selected run is still alive, redirect the user there.
            () => { window.location.href = runMetadata.flame_url; },
            // If the flame is not still alive, take the user to the graph for the selected run.
            () => this.$router.push({ name: 'XGraph', params: { inputFireXId: this.inputFireXId } }),
          );
      }
    },
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
