<template>
  <div>
    <div :style="containerStyle">
      <slot></slot>
    </div>
    <div v-if="!isExpanded" class="fadeout-container">
      <div class="fadeout"></div>
      <button :class="'btn ' + buttonClass" style="margin: 0.5em 2em;"
              @click="isExplicitlyExpanded = !isExplicitlyExpanded">
        <font-awesome-icon icon="plus-circle"></font-awesome-icon>
        Show Full {{name}}
      </button>
    </div>
  </div>
</template>

<script>

export default {
  name: 'XExpandableContent',
  props: {
    buttonClass: { default: '', type: String },
    name: { default: '' },
    expand: { default: false, type: Boolean },
  },
  data() {
    return {
      isExplicitlyExpanded: false,
    };
  },
  computed: {
    isExpanded() {
      return this.isExplicitlyExpanded || this.expand;
    },
    containerStyle() {
      if (this.isExpanded) {
        return {};
      }
      return { 'max-height': '8em', overflow: 'hidden' };
    },
  },
  watch: {
  },
};
</script>

<style scoped>
  .fadeout-container {
    position: relative;
    bottom: 2em;
    margin-bottom: -2em;
  }

  .fadeout {
    height: 2em;
    background-image: linear-gradient(
        rgba(255, 255, 255, 0) 0%,
        rgba(255, 255, 255, 1) 100%
    );
  }

  .btn-outline-danger {
    color: #dc3545;
    border-color: #dc3545;
    background-color: white;
  }

  .btn-info-primary {
    color: #17a2b8;
    border-color: #17a2b8;
    background-color: white;
  }
</style>
