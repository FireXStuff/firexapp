<template>
  <div>
    <div v-for="(key, i) in displayOrderedKeys" :key="key"
         :style="{'background-color': i % 2 === 0 ? '#f5f5f5': '#fafafa', 'padding': '4px' }">
      <label class="node-attributes-label">{{key}}:</label>
      <a v-if="isLinkKey(key)" :href="keyValues[key]"> {{keyValues[key]}}</a>
      <div v-else style="overflow-y: hidden; overflow-x: auto; display: inline">
        <x-expandable-content v-if="shouldPrettyPrint(keyValues[key])"
                              button-class="btn-info-primary"
                              :name="key"
                              :expand="expandAll">
          <pre style="margin: 0 0 0 40px"> {{prettyPrint(keyValues[key])}} </pre>
        </x-expandable-content>
        <template v-else> {{keyValues[key] === null ? 'None' : keyValues[key]}}</template>
      </div>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';

import XExpandableContent from './XExpandableContent.vue';

export default {
  name: 'XKeyValueViewer',
  components: { XExpandableContent },
  props: {
    keyValues: { type: Object, required: true },
    expandAll: { default: false, type: Boolean },
    orderedKeys: { default: null, type: Array },
    linkKeys: { default: () => [], type: Array },
  },
  computed: {
    displayOrderedKeys() {
      if (this.orderedKeys) {
        return this.orderedKeys;
      }
      return _.sortBy(_.keys(this.keyValues));
    },
  },
  methods: {
    // TODO: doesn't make sense to string twice, combine with prettyPrint.
    shouldPrettyPrint(val) {
      if (!_.isObject(val)) {
        return false;
      }
      const asJson = JSON.stringify(val, null, 2);
      // Don't bother pretty printing if the object is small.
      return asJson.length > 100 && asJson.split(/\n/).length > 4;
    },
    prettyPrint(val) {
      if (val === null) {
        return 'None';
      }
      const prettyJson = JSON.stringify(val, null, 2);
      if (prettyJson.length < 40) {
        // If the string is short enough, don't show a pretty version.
        return JSON.stringify(val);
      }
      return prettyJson;
    },
    isLinkKey(key) {
      return _.includes(this.linkKeys, key);
    },
  },
};
</script>

<style scoped>

</style>
