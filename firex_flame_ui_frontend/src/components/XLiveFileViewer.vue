<template>
  <div style="margin: 1em; font-family: 'Source Sans Pro',sans-serif;">

    <template v-if="isLiveFileListenSupported">
      <h3>{{host}}:{{filepath}}</h3>
      <p v-for="(line, i) in lines" :key="i" class="thin">
        {{line}}
      </p>
    </template>
    <div v-else>
      Live file viewing is no longer available for this FireX run.
    </div>
  </div>
</template>

<script>
import * as api from '../api';

export default {
  name: 'XLifeFileViewer',
  props: {
    host: { type: String },
    filepath: { type: String },
  },
  data() {
    return {
      isLiveFileListenSupported: api.isLiveFileListenSupported(),
      lines: [],
    };
  },
  created() {
    api.startLiveFileListen(this.host, this.filepath, this.addLine);
    window.addEventListener('beforeunload', api.stopLiveFileListen);
  },
  destroyed() {
    window.removeEventListener('beforeunload', api.stopLiveFileListen);
  },
  methods: {
    addLine(newLine) {
      this.lines.push(newLine);
    },
  },
  beforeRouteLeave(to, from, next) {
    api.stopLiveFileListen();
    next();
  },
};
</script>

<style scoped>
.thin {
  margin: 0;
}
</style>
