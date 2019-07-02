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

export default {
  name: 'XFindFirexId',
  data() {
    return {
      inputFireXId: '',
      firexIdRegex: new RegExp('FireX-.*-(\\d\\d)(\\d\\d)(\\d\\d)-\\d{6}-\\d+'),
    };
  },
  asyncComputed: {
    foundFirexIdLogs:
      {
        get() {
          if (!this.isFirexIdValid) {
            return false;
          }
          return fetch(this.logsPath).then(r => r.ok, () => false);
        },
        default: false,
      },
  },
  computed: {
    isFirexIdValid() {
      return this.firexIdRegex.test(this.inputFireXId);
    },
    errorMessage() {
      if (this.inputFireXId) {
        if (!this.isFirexIdValid) {
          return 'The entered FireX ID is not valid.';
        }
        if (!this.foundFirexIdLogs) {
          return 'Could not find logs for FireX ID.';
        }
      }
      return null;
    },
    logsPath() {
      const match = this.firexIdRegex.exec(this.inputFireXId);
      if (match == null) {
        return null;
      }
      return `/auto/firex-logs/${match[1]}/${match[2]}/${match[3]}/${this.inputFireXId}/`;
    },
  },
  methods: {
    findFirexId() {
      if (this.foundFirexIdLogs) {
        this.$router.push({ name: 'XGraph', params: { inputFireXId: this.inputFireXId } });
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
