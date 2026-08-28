<template>
  <!-- Use keyup.stop because search typing shouldn't cause outer keybindings to trigger. -->
  <div style="display: flex; height: 100%;" class="header-entry" @keyup.stop>
    <div v-if="searchOpen" style="height: 100%;"
         v-on:keydown.esc="$store.commit('tasks/closeSearch')">
      <div class="search-pos">
        {{ searchResultCount === 0 ? 0 : selectedIndex + 1 }} / {{ searchResultCount }}
        <input ref='search-input' type="text" :value="searchTerm"
             @keyup.enter.exact="submitSearch($event.target.value.trim())"
             @keyup.enter.shift="$store.dispatch('tasks/previousSearchResult')"
             class="search" placeholder="Search">
      </div>
    </div>
    <x-header-button :link="buttonLink" style="border-left: none;" ></x-header-button>
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
        _class: this.searchOpen ? 'active-icon' : null,
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
    max-width: 300px;
    padding: 0 2px;
    margin-left: 2px;
}

.search-pos {
  font-family: 'Source Sans Pro', sans-serif;
  color: #000;
  font-size: 20px;
  cursor: default;
  padding: 0 3px;
  display: inline-flex;
  align-items: center;
  height: 100%;
  text-wrap: nowrap;
}

  /deep/ .flame-link {
    border: none;
  }

</style>
