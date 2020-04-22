<template>
  <font-awesome-icon
    :icon="iconDetails.icon"
    :style="{color: iconDetails.color}"
    :class="[iconDetails.class]"
  ></font-awesome-icon>
</template>

<script>

export default {
  name: 'XStatusIcon',
  props: {
    status: { required: true, type: String },
  },
  data() {
    return {
      type_to_icon_details: {
        success: { icon: ['far', 'check-circle'], color: 'green' },
        warning: { icon: 'exclamation-triangle', color: 'darkorange' },
        in_progress: { icon: 'circle-notch', color: 'cornflowerblue', class: 'fa-spin' },
        failed: { icon: ['far', 'times-circle'], color: 'darkred' },
        unknown: { icon: 'question-circle' },
      },
    };
  },
  computed: {
    iconDetails() {
      return this.type_to_icon_details[this.status];
    },
  },
};
</script>

<style scoped>
</style>
