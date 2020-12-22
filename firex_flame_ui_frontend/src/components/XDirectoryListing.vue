<template>
  <div class="flame-top">
    <x-header :title="inputFireXId"
              :main-title="logRelPath + ' logs'"
              :links="[]"></x-header>
    <div style="margin: 2em;">
      <div v-if="hasBucketListing" class="list-group">
        <router-link v-if="logRelPath.length" class="list-group-item"
                     :to="createLogRoute(getParentPathString(logRelPath))">
          <font-awesome-icon icon="level-up-alt"></font-awesome-icon>
          Parent Directory
        </router-link>
        <router-link v-for="d in bucketDirectories" class="list-group-item" :key="d"
                     :to="createLogRoute(appendPaths(logRelPath, d))">
          <font-awesome-icon icon="folder-open"></font-awesome-icon>
          {{d}}
        </router-link>
        <a v-for="file in bucketFiles" :href="file.link" :key="file.id"
           class="list-group-item">
          <font-awesome-icon icon="file-alt"></font-awesome-icon>
          {{file.name}}
        </a>
      </div>
      <div v-else>
        Found no logs for path {{logRelPath}}
      </div>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';
import { mapState } from 'vuex';
import XHeader from './XHeader.vue';
import {
  templateFireXId, normalizeGoogleBucketItems, arrayToPath, pathStringToArray,
  getParentArray,
} from '../utils';


export default {
  name: 'XDirectoryListing',
  components: { XHeader },
  props: {
    logRelPath: { type: String, default: '' },
    inputFireXId: { type: String },
  },
  asyncComputed: {
    bucketListing() {
      return fetch(this.googleBucketListRunLogsUrl).then(r => r.json())
        .then(bucketData => bucketData);
    },
  },
  computed: {
    ...mapState({
      uiConfig: state => state.header.uiConfig,
    }),
    normalizedLogRelPath() {
      if (this.logRelPath.endsWith('/')) {
        return _.trimEnd(this.logRelPath, '/');
      }
      return this.logRelPath;
    },
    bucketDirectories() {
      const dirPrefix = `${_.join(_.filter([this.inputFireXId, this.normalizedLogRelPath]), '/')}/`;
      return _.map(this.bucketListing.prefixes, p => _.split(p, dirPrefix, 2)[1]);
    },
    hasBucketListing() {
      return !_.isNil(this.bucketListing);
    },
    bucketFiles() {
      return normalizeGoogleBucketItems(this.bucketListing.items, this.inputFireXId);
    },
    googleBucketListRunLogsUrl() {
      return templateFireXId(this.uiConfig.logs_serving.list_url_template, this.inputFireXId,
        { log_entry_rel_run_root: this.ensureEndsWithSlash(this.logRelPath) });
    },
  },
  methods: {
    getParentPathString(path) {
      return this.ensureEndsWithSlash(arrayToPath(getParentArray(path)));
    },
    ensureEndsWithSlash(path) {
      return path && !path.endsWith('/') ? `${path}/` : path;
    },
    appendPaths(startPath, endPath) {
      return this.ensureEndsWithSlash(
        arrayToPath(_.concat(pathStringToArray(startPath), pathStringToArray(endPath))));
    },
    createLogRoute(path) {
      return `/${_.join([this.inputFireXId, 'logs', path], '/')}`;
    },
  },
};
</script>

<style scoped>
</style>
