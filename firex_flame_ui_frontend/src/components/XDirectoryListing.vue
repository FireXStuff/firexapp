<template>
  <div class="flame-top">
    <x-header :title="inputFireXId"
              :main-title="logRelPath + ' logs'"
              :links="[]"></x-header>
    <div style="margin: 2em;">
      <div v-if="selectedPathItems" class="list-group">
        <router-link v-if="logRelPath.length" class="list-group-item"
                     :to="createLogRoute(getParentPathString(logRelPath))">
          <font-awesome-icon icon="level-up-alt"></font-awesome-icon>
          Parent Directory
        </router-link>
        <router-link v-for="d in selectedPathItems.dirs" class="list-group-item" :key="d"
                     :to="createLogRoute(appendPaths(logRelPath, d))">
          <font-awesome-icon icon="folder-open"></font-awesome-icon>
          {{d}}
        </router-link>
        <a v-for="file in selectedPathItems.files" :href="file.link" :key="file.id"
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
  templateFireXId, flatGoogleBucketsListingToFilesByDir, arrayToPath, pathStringToArray,
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
        .then(bucketData => bucketData.items);
    },
  },
  computed: {
    ...mapState({
      uiConfig: state => state.header.uiConfig,
    }),
    googleBucketListRunLogsUrl() {
      return templateFireXId(this.uiConfig.logs_serving.list_url_template, this.inputFireXId);
    },
    directoryPathToFiles() {
      return flatGoogleBucketsListingToFilesByDir(this.bucketListing, this.inputFireXId);
    },
    selectedPathItems() {
      // TODO: handle path doesn't exist.
      return this.directoryPathToFiles[this.logRelPath];
    },
  },
  methods: {
    getParentPathString(path) {
      return arrayToPath(getParentArray(path));
    },
    appendPaths(startPath, endPath) {
      return arrayToPath(_.concat(pathStringToArray(startPath), pathStringToArray(endPath)));
    },
    createLogRoute(path) {
      return `/${_.join([this.inputFireXId, 'logs', path], '/')}`;
    },
  },
};
</script>

<style scoped>
</style>
