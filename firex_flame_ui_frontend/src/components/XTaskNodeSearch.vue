<template>
  <div style="display: flex; border: none; border-left: 1px solid #000;">
    <div v-if="searchOpen" v-on:keydown.esc="$store.commit('tasks/closeSearch')">
      <div class="search-pos">
        {{ searchResultCount === 0 ? 0 : selectedIndex + 1 }} / {{ searchResultCount }}
      </div>
      <input ref='search-input' type="text" :value="searchTerm"
             @keyup.enter.exact="submitSearch($event.target.value.trim())"
             @keyup.enter.shift="$store.dispatch('tasks/previousSearchResult')"
             class="search" placeholder="Search" style="margin-right: 8px">
    </div>
    <!--<div class="header-icon-button" title="Search"-->
         <!--v-on:click="$store.commit('tasks/toggleSearchOpen')"-->
      <!--<font-awesome-icon icon="search" fixed-width></font-awesome-icon>-->
      <!--<x-header-button :link="buttonLink"></x-header-button>-->
    <!--</div>-->
    <x-header-button :link="buttonLink" :style="searchOpen ? 'color: #2B2;' : ''"></x-header-button>
  </div>
</template>

<script>
import { mapState } from 'vuex';
import XHeaderButton from './XHeaderButton.vue';

export default {
  name: 'XTaskNodeSearch',
  components: { XHeaderButton },
  props: {
    findUncollapsedAncestor: { default: true, type: Boolean },
  },
  computed: {
    ...mapState({
      selectedIndex: state => state.tasks.search.selectedIndex,
      searchResultCount: state => state.tasks.search.resultUuids.length,
      searchOpen: state => state.tasks.search.isOpen,
      searchTerm: state => state.tasks.search.term,
    }),
    buttonLink() {
      return {
        on: () => { this.$store.commit('tasks/toggleSearchOpen'); },
        icon: 'search',
        title: 'Search',
      };
    },
  },
  methods: {
    submitSearch(term) {
      this.$store.dispatch('tasks/search',
        { term, findUncollapsedAncestor: this.findUncollapsedAncestor });
    },
  },
  watch: {
    searchOpen(newIsOpen) {
      if (newIsOpen) {
        this.$nextTick(() => { this.$refs['search-input'].focus(); });
      }
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
  padding: 0 5px;
}

  /deep/ .flame-link {
    border: none;
  }

</style>
