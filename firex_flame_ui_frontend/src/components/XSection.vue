<template>
  <div class="panel panel-default">
    <div class="panel-heading">
      <h3 class="panel-title">{{ heading }}</h3>
    </div>
    <div class="panel-body">
      <slot></slot>
    </div>
  </div>
</template>

<script>

export default {
  name: 'XSection',
  props: {
    heading: { type: String },
  },
};
</script>

<style scoped>
</style>
