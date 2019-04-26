<template>
  <div class="settings">
    <h1>Settings</h1>
    <!-- TODO: replace with messaging library (e.g. toastr) -->
    <div style="background: lightgreen;">
      {{ successDisplayMsg }}
    </div>
    <div style="background: salmon;">
      {{ failureDisplayMsg }}
    </div>
    <div>
      <h3>Auto-upgrade</h3>
      <select id="auto-upgrade" :value="selectedAutoUpgrade"
              @input="setAutoUpgrade($event.target.value)">
        <option value="true">Central FireX Server</option>
        <option value="relative">Relative Flame Server</option>
        <option value="none">Disable Auto-Upgrade</option>
      </select>
    </div>
    <div>
      <h3>Display Configuration</h3>

      <button type="button"
              style="margin-bottom: 10px;"
              v-clipboard:copy="displayConfigsJson"
              v-clipboard:success="onCopySuccess"
              v-clipboard:error="onCopyFail"
        >Copy Display Configs</button>

      <table class="display-config">
        <thead>
          <tr>
            <td>Service Name</td>
            <td>Operation</td>
            <td>Targets</td>
            <td>Actions</td>
          </tr>
        </thead>
        <tbody>
          <tr v-if="displayConfigs.length === 0" >
            <td colspan="4" style="padding: 20px;">
              No display operations have been saved. Create operations below.
            </td>
          </tr>
          <tr v-else v-for="config in displayConfigs" :key="config.id">
            <td>{{ config.relative_to_nodes.value }}</td>
            <td>{{ config.operation }}</td>
            <td>{{ join(config.targets) }}</td>
            <td style="text-align: center;">
              <font-awesome-icon icon="trash" style="cursor: pointer;" title="delete"
                @click="deleteDisplayConfig(config.id)"></font-awesome-icon>
            </td>
          </tr>
        </tbody>
      </table>

      <div style="margin-top: 15px;">
        <input type="text" placeholder="Service Name" v-model="inputDisplayConfig.serviceName">
        <select style="margin: 10px;" v-model="inputDisplayConfig.operation">
          <option value="collapse">collapse</option>
          <option value="expand">expand</option>
        </select>

        <div style="display: inline; padding: 10px;">
          <div style="display: inline; padding: 3px;">
            <input type="checkbox" id="self" value="self"
                   v-model="inputDisplayConfig.targets">
            <label for="self">Self</label>
          </div>

          <div style="display: inline; padding: 3px;">
            <input type="checkbox" id="descendants" value="descendants"
                   v-model="inputDisplayConfig.targets">
            <label for="descendants">Descendants</label>
          </div>

          <div style="display: inline;">
            <input type="checkbox" id="grandchildren" value="grandchildren"
              v-model="inputDisplayConfig.targets">
            <label for="grandchildren">Grandchildren</label>
          </div>

          <div style="display: inline; padding: 3px;">
            <input type="checkbox" id="ancestors" value="ancestors"
                   v-model="inputDisplayConfig.targets">
            <label for="ancestors">Ancestors</label>
          </div>
        </div>
      </div>
      <!--TODO: use validation library -->
      <div style="background: salmon">
        {{displayConfigError}}
      </div>

      <button @click="saveDisplayConfigEntry">Save Display Config</button>

    </div>

  </div>
</template>

<script>
import _ from 'lodash';
import { uuidv4, loadDisplayConfigs } from '../utils';

export default {
  name: 'XSettings',
  data() {
    const autoUpgradeKey = 'auto-flame-upgrade';
    return {
      successDisplayMsg: '',
      autoUpgradeKey,
      selectedAutoUpgrade: this.readAutoUpgradeFromLocalStorage(autoUpgradeKey),
      displayConfigs: loadDisplayConfigs(),
      inputDisplayConfig: this.createEmptyDisplayConfigEntry(),
      displayConfigError: '',
      failureDisplayMsg: '',
    };
  },
  computed: {
    displayConfigsJson() {
      return JSON.stringify(this.displayConfigs, null, 2);
    },
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

      this.successDisplayMsg = `Successfully ${displayString} auto-upgrade.`;
    },
    readAutoUpgradeFromLocalStorage(autoUpgradeKey) {
      const storedValue = localStorage.getItem(autoUpgradeKey);
      if (_.includes(['true', 'relative'], storedValue)) {
        return storedValue;
      }
      return 'none';
    },
    createEmptyDisplayConfigEntry() {
      return {
        serviceName: '',
        operation: 'collapse',
        targets: [],
      };
    },
    saveDisplayConfigEntry() {
      this.displayConfigError = '';
      if (_.isEmpty(this.inputDisplayConfig.serviceName.trim())) {
        this.displayConfigError = 'Service name cannot be empty';
        return;
      }
      if (_.isEmpty(this.inputDisplayConfig.targets)) {
        this.displayConfigError = 'At least one target must be selected.';
        return;
      }

      const newEntry = {
        id: uuidv4(),
        relative_to_nodes: {
          type: 'task_name',
          value: this.inputDisplayConfig.serviceName,
        },
        operation: this.inputDisplayConfig.operation,
        targets: this.inputDisplayConfig.targets,
      };
      this.displayConfigs.push(newEntry);
      this.inputDisplayConfig = this.createEmptyDisplayConfigEntry();
      localStorage.setItem('displayConfigs', JSON.stringify(this.displayConfigs));
    },
    deleteDisplayConfig(id) {
      this.displayConfigs = _.filter(this.displayConfigs, c => c.id !== id);
      localStorage.setItem('displayConfigs', JSON.stringify(this.displayConfigs));
    },
    join(array) {
      return _.join(array, ', ');
    },
    onCopySuccess() {
      this.successDisplayMsg = 'Successfully copied display config to clipboard.';
    },
    onCopyFail() {
      this.failureDisplayMsg = 'Failed to copy to clipboard';
    },
  },
};
</script>

<style>

  .settings {
    font-family: 'Source Sans Pro',sans-serif;
    margin: 10px;
  }

  .display-config td {
    border: 1px solid #cbcbcb;
    border-spacing: 0;
    padding: 5px;
  }

  .display-config {
    border-spacing: 0;
  }

  .display-config thead {
    font-weight: bold;
  }

</style>
