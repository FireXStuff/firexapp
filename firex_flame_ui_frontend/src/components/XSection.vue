<template>
  <div class="panel panel-default">
    <div class="panel-heading">
      <slot name="header">
        <h3 class="panel-title">{{ heading }}</h3>
      </slot>
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
