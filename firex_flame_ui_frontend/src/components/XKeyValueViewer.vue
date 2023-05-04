<template>
  <div style="border: 1px solid #ddd;">
    <div v-for="(key, i) in displayOrderedKeys" :key="key"
         :style="{'background-color': i % 2 === 0 ? '#f5f5f5': '#fafafa', 'padding': '4px' }">
      <label style="margin-right: 1em;">{{key}}:</label>
      <a v-if="isLinkKey(key)" :href="keyValues[key]"> {{keyValues[key]}}</a>
      <div v-else style="overflow-y: hidden; overflow-x: auto; display: inline">
        <x-expandable-content v-if="prettyPrintedKeyValues[key]"
                              button-class="btn-info-primary"
                              :name="key"
                              :expand="expandAll">
          <pre style="margin: 0 0 0 40px; font-size: 14px;"
               v-html="createLinkedHtml(prettyPrintedKeyValues[key])">
          </pre>
        </x-expandable-content>
        <span v-else v-html="createLinkedHtml(formatValue(keyValues[key]))"></span>
      </div>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';
import { mapGetters } from 'vuex';

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
    ...mapGetters({
      createLinkedHtml: 'header/createLinkedHtml',
    }),
    displayOrderedKeys() {
      if (this.orderedKeys) {
        return this.orderedKeys;
      }
      return _.sortBy(_.keys(this.keyValues));
    },
    prettyPrintedKeyValues() {
      return _.mapValues(this.keyValues, (v) => {
        if (_.isObject(v)) {
          const prettyJson = JSON.stringify(v, null, 2);
          if (prettyJson.length > 100 && prettyJson.split(/\n/).length > 4) {
            return prettyJson;
          }
        }
        // Don't show a pretty printed version of this value.
        return false;
      });
    },
  },
  methods: {
    formatValue(val) {
      if (val === null) {
        return 'None';
      }
      if (_.isObject(val)) {
        return JSON.stringify(val);
      }
      return val;
    },
    isLinkKey(key) {
      return _.includes(this.linkKeys, key);
    },
  },
};
</script>

<style scoped>
</style>
