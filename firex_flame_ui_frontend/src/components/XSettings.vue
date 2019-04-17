<template>
  <div style="margin: 10px">
    <div>
      <label for="auto-upgrade">Auto-upgrade:</label>
      <select id="auto-upgrade" :value="selectedAutoUpgrade"
              @input="setAutoUpgrade($event.target.value)">
        <option value="true">Central FireX Server</option>
        <option value="relative">Relative Flame Server</option>
        <option value="none">Disable Auto-Upgrade</option>
      </select>
    </div>
    {{ displayMsg }}
  </div>
</template>

<script>
import _ from 'lodash';

export default {
  name: 'XSettings',
  data() {
    const autoUpgradeKey = 'auto-flame-upgrade';
    return {
      displayMsg: '',
      autoUpgradeKey,
      selectedAutoUpgrade: this.readAutoUpgradeFromLocalStorage(autoUpgradeKey),
    };
  },
  created() {
    if (_.isNull(localStorage.getItem(this.autoUpgradeKey))) {
      this.setAutoUpgrade('relative');
    }
  },
  methods: {
    setAutoUpgrade(selectedValue) {
      this.selectedAutoUpgrade = selectedValue;
      localStorage.setItem(this.autoUpgradeKey, selectedValue);
      const displayString = {
        // To be backwards compatible, 'true' means central.
        true: 'enabled central FireX',
        relative: 'enabled relative Flame',
        none: 'disabled',
      }[selectedValue];

      this.displayMsg = `Successfully ${displayString} auto-upgrade.`;
    },
    readAutoUpgradeFromLocalStorage(autoUpgradeKey) {
      const storedValue = localStorage.getItem(autoUpgradeKey);
      if (_.includes(['true', 'relative'], storedValue)) {
        return storedValue;
      }
      return 'none';
    },
  },
};
</script>

<style>
</style>
