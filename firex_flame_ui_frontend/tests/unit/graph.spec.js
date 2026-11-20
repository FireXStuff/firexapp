import _ from 'lodash';
import { mount } from '@vue/test-utils';
import { createPinia, setActivePinia } from 'pinia';

import router from '/src/router/index';
import XGraph from '/src/components/XGraph.vue';
import XCollapseableTaskNode from '/src/components/nodes/XCollapsableTaskNode.vue';
import XSizeCapturingNodes from '/src/components/nodes/XSizeCapturingNodes.vue';
import XCoreTaskNode from '/src/components/nodes/XCoreTaskNode.vue';
import { useTasksStore } from '/src/store/modules/tasks';
import { useFirexRunMetadataStore } from '/src/store/modules/firexRunMetadata';

const SIMPLE_TWO_NODE_CHAIN_BY_UUID = {
  1: {
    uuid: '1',
    name: 'rootName',
    parent_id: null,
  },
  2: {
    uuid: '2',
    name: 'childName',
    parent_id: '1',
  },
};

function fakeDimensionsByUuid(uuids) {
  return _.mapValues(_.keyBy(uuids), () => ({ width: 1, height: 2 }));
}

function createCollapseFlameData(uuid, targets) {
  return {
    _default_display: {
      order: 0,
      type: 'object',
      value: [
        {
          operation: 'collapse',
          relative_to_nodes: {
            type: 'task_uuid',
            value: uuid,
          },
          source_node: {
            type: 'task_uuid',
            value: uuid,
          },
          targets,
        },
      ],
    },
  };
}

function initGraphWrapper(tasksByUuid, pinia) {
  useFirexRunMetadataStore().setFlameRunMetadata(
    { uid: 'FireX-user-xxxxx', root_uuid: '1' },
  );

  const tasksStore = useTasksStore();
  tasksStore.setTasks(tasksByUuid);
  // Need to fake dimensions typically provided via the browser from getBoundingClientRect().
  tasksStore.addTaskNodeSize(fakeDimensionsByUuid(_.keys(tasksByUuid)));

  return mount(XGraph, {
    global: {
      plugins: [router, pinia],
      stubs: {
        'font-awesome-icon': true,
        'font-awesome-layers': true,
        'font-awesome-layers-text': true,
      },
    },
  });
}

describe('XGraph.vue', () => {
  let pinia;

  beforeEach(async () => {
    // Nothing under test fetches, but keep an inert fetch so an accidental one can't hit
    // the network.
    vi.stubGlobal('fetch', vi.fn(() => Promise.resolve({
      ok: true,
      json: () => Promise.resolve({}),
    })));
    // A fresh pinia per test replaces the deep-cloning the Vuex store options needed.
    pinia = createPinia();
    setActivePinia(pinia);
    router.push('/');
    await router.isReady();
  });

  it('renders simple tree', async () => {
    const wrapper = initGraphWrapper(SIMPLE_TWO_NODE_CHAIN_BY_UUID, pinia);

    // Make sure 2 nodes in visible tree exist
    expect(wrapper.findAllComponents(XCollapseableTaskNode)).toHaveLength(2);

    // Make sure two invisible nodes created for size-tracking exist but are not visible.
    const sizeCapturingComp = wrapper.findComponent(XSizeCapturingNodes);
    expect(sizeCapturingComp.attributes().style).toContain('z-index: -10');
    const sizeCapturingTaskNodes = sizeCapturingComp.findAllComponents(XCoreTaskNode);
    expect(sizeCapturingTaskNodes).toHaveLength(2);
  });

  it('updates a simple tree with a new task', async () => {
    const wrapper = initGraphWrapper(SIMPLE_TWO_NODE_CHAIN_BY_UUID, pinia);

    const tasksStore = useTasksStore();
    tasksStore.addTasksData({
      3: {
        uuid: '3',
        name: 'childChildName',
        parent_id: '2',
      },
    });
    tasksStore.addTaskNodeSize(fakeDimensionsByUuid(['3']));

    await wrapper.vm.$nextTick();
    expect(wrapper.findAllComponents(XCollapseableTaskNode)).toHaveLength(3);
  });

  it('updates a graph with collapsed tasks', async () => {
    const wrapper = initGraphWrapper(SIMPLE_TWO_NODE_CHAIN_BY_UUID, pinia);

    const tasksStore = useTasksStore();
    tasksStore.addTasksData({
      3: {
        uuid: '3',
        name: 'childChildName',
        parent_id: '2',
        flame_data: createCollapseFlameData('3', ['self', 'descendants']),
      },
      4: {
        uuid: '4',
        name: 'childChildChildName',
        parent_id: '3',
      },
    });
    tasksStore.addTaskNodeSize(fakeDimensionsByUuid(['3', '4']));
    await wrapper.vm.$nextTick();

    // Expecting 2 collapsed nodes, 2 uncollapsed.
    const collapseableTaskNodes = wrapper.findAllComponents(XCollapseableTaskNode);
    expect(collapseableTaskNodes).toHaveLength(2);
    expect(collapseableTaskNodes[1].props().taskUuid).toBe('2');
    expect(collapseableTaskNodes[1].html()).toContain('2 Tasks');

    // 4 total size capturing nodes.
    expect(wrapper.findComponent(XSizeCapturingNodes).findAllComponents(XCoreTaskNode))
      .toHaveLength(2);
  });
});
