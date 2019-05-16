<template>
  <div style="display: flex">
    <div v-if="searchOpen" v-on:keydown.esc="closeSearch">
      <div class="search-pos">
        {{ totalResultsCount === 0 ? 0 : currentResultIndex + 1 }} / {{ totalResultsCount }}
      </div>
      <input ref='search-input' type="text" v-model.trim="currentSearchTerm"
             @keyup.enter="sendSearchRequest"
             class="search" placeholder="Search" style="margin-right: 8px">
    </div>
    <div class="header-icon-button"
         v-on:click="toggleSearchOpen"
         :style="searchOpen ? 'color: #2B2;' : ''">
      <font-awesome-icon icon="search"></font-awesome-icon>
    </div>
  </div>
</template>

<script>
import _ from 'lodash';
import { eventHub } from '../utils';

export default {
  name: 'XTaskNodeSearch',
  components: {},
  data() {
    return {
      searchOpen: false,
      currentResultIndex: 0,
      currentSearchTerm: '',
      latestSentSearchTerm: 0,
      searchResultUuids: [],
    };
  },
  created() {
    eventHub.$on('find-focus', this.toggleSearchOpen);
  },
  beforeDestroy() {
    eventHub.$off('find-focus');
  },
  computed: {
    uncollapsedSearchResultUuids() {
      // TODO: actually support collapsed nodes, don't just ignore!!!
      return _.intersection(this.searchResultUuids, this.uncollapsedNodeUuids);
    },
    totalResultsCount() {
      return this.uncollapsedSearchResultUuids.length;
    },
    uncollapsedNodeUuids() {
      return this.$store.getters['graph/uncollapsedNodeUuids'];
    },
  },
  methods: {
    sendSearchRequest() {
      this.$store.commit('tasks/setFocusedTaskUuid', null);
      if (this.currentSearchTerm !== this.latestSentSearchTerm) {
        // New search term, submit new search.
        this.latestSentSearchTerm = this.currentSearchTerm;
        this.currentResultIndex = 0;
        this.searchResultUuids = this.$store.getters['tasks/searchForUuids'](this.currentSearchTerm);
        if (this.searchResultUuids.length > 0) {
          this.emitFocusCurrentNode();
        }
      } else if (this.totalResultsCount > 0) {
        // Same search term as before, go from current result to the next
        this.currentResultIndex = (this.currentResultIndex + 1) % this.totalResultsCount;
        this.emitFocusCurrentNode();
      }
    },
    emitFocusCurrentNode() {
      const focusedUuid = this.uncollapsedSearchResultUuids[this.currentResultIndex];
      this.$store.commit('tasks/setFocusedTaskUuid', focusedUuid);
    },
    toggleSearchOpen() {
      this.searchOpen = !this.searchOpen;
      if (this.searchOpen) {
        this.$nextTick(() => { this.$refs['search-input'].focus(); });
      } else {
        this.closeSearch();
      }
    },
    closeSearch() {
      this.searchOpen = false;
      this.currentSearchTerm = '';
      this.searchResultUuids = [];
      this.currentResultIndex = 0;
    },
  },
};
</script>

<style scoped>
.search {
    font-family: 'Source Sans Pro',sans-serif;
    line-height: 36px;
    border: 2px solid #2980b9;
    outline: 0;
    display: inline-block;
    font-size: 24px;
    vertical-align: top;
    width: 300px;
    padding: 0 2px;
}

.search-pos {
  font-family: 'Source Sans Pro', sans-serif;
  display: inline-block;
  color: #000;
  line-height: 40px;
  font-size: 20px;
  cursor: default;
}

</style>
