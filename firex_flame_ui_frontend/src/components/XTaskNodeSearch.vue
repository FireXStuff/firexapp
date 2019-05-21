<template>
  <div style="display: flex">
    <div v-if="searchOpen" v-on:keydown.esc="$store.commit('tasks/closeSearch')">
      <div class="search-pos">
        {{ searchResultCount === 0 ? 0 : selectedIndex + 1 }} / {{ searchResultCount }}
      </div>
      <input ref='search-input' type="text" :value="searchTerm"
             @keyup.enter.exact="$store.dispatch('tasks/search', $event.target.value.trim())"
             @keyup.enter.shift="$store.dispatch('tasks/previousSearchResult')"
             class="search" placeholder="Search" style="margin-right: 8px">
    </div>
    <div class="header-icon-button"
         v-on:click="$store.commit('tasks/toggleSearchOpen')"
         :style="searchOpen ? 'color: #2B2;' : ''">
      <font-awesome-icon icon="search"></font-awesome-icon>
    </div>
  </div>
</template>

<script>
import { mapState } from 'vuex';

export default {
  name: 'XTaskNodeSearch',
  components: {},
  computed: {
    ...mapState({
      selectedIndex: state => state.tasks.search.selectedIndex,
      searchResultCount: state => state.tasks.search.resultUuids.length,
      searchOpen: state => state.tasks.search.isOpen,
      searchTerm: state => state.tasks.search.term,
    }),
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
}

</style>
