<template>
  <div class="error-container">
    <div style="font-size: 250px;"> &#x1F525; </div>
    <div style="font-size: 5em;">Something is wrong.</div>
    <div v-if="storedMessage" style="font-size: 1.5em; white-space: pre;"
    >{{storedMessage}}</div>
  </div>
</template>

<script>

export default {
  name: 'XError',
  props: {
    message: { type: String, default: null },
  },
  data() {
    return {
      storedMessage: this.message,
    };
  },
  created() {
    // Remove message from query for aesthetic reasons,
    // relying on the initial value having already been stored.
    const query = Object.assign({}, this.$route.query);
    delete query.message;
    this.$router.replace({ query });
  },
};
</script>

<style scoped>
  .error-container {
    width: 100%;
    height: 100%;
    display: flex;
    flex-direction: column;
    text-align: center;
    font-family: 'Source Sans Pro',sans-serif;
  }
</style>
