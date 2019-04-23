import _ from 'lodash';

import { resolveCollapseStatusByUuid, getCollapsedGraphByNodeUuid } from '@/utils.js';


function getRandomInt() {
  return `${Math.floor(Math.random() * Math.floor(10000))}`;
}

describe('utils.js', () => {
  const trivialNodesByUuid = {
    1: {
      uuid: '1',
      parent_id: null,
    },
    2: {
      uuid: '2',
      parent_id: '1',
    },
    3: {
      uuid: '3',
      parent_id: '2',
    },
  };

  it('collapses trivial node via self', () => {
    const collapseOpsByUuid = {
      2: [{ operation: 'collapse', priority: 1, targets: ['self'] }],
    };

    const result = resolveCollapseStatusByUuid(trivialNodesByUuid, collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(_.find(result, n => n.parent_id === '1').collapsed).toBe(true);
    expect(result['1'].collapsed).toBe(false);
  });

  it('collapses trivial descendants', () => {
    const collapseOpsByUuid = {
      1: [{ operation: 'collapse', priority: 1, targets: ['descendants'] }],
    };

    const result = resolveCollapseStatusByUuid(trivialNodesByUuid, collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['2'].collapsed).toBe(true);
    expect(result['1'].collapsed).toBe(false);
  });

  it('expands self even when ancestor collapses descendants', () => {
    const collapseOpsByUuid = {
      1: [{ operation: 'collapse', priority: 1, targets: ['descendants'] }],
      3: [{ operation: 'expand', priority: 1, targets: ['self'] }],
    };

    const result = resolveCollapseStatusByUuid(trivialNodesByUuid, collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['1'].collapsed).toBe(false);
    expect(result['2'].collapsed).toBe(true);
    expect(result['3'].collapsed).toBe(false);
  });

  it('collapses ancestors and not self', () => {
    const collapseOpsByUuid = {
      3: [
        { operation: 'collapse', priority: 1, targets: ['ancestors'] },
        { operation: 'expand', priority: 1, targets: ['self'] },
      ],
    };

    const result = resolveCollapseStatusByUuid(trivialNodesByUuid, collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['1'].collapsed).toBe(true);
    expect(result['2'].collapsed).toBe(true);
    expect(result['3'].collapsed).toBe(false);
  });

  it('expands self even when descendant collapses ancestors', () => {
    const collapseOpsByUuid = {
      1: [{ operation: 'expand', priority: 1, targets: ['self'] }],
      3: [{ operation: 'collapse', priority: 1, targets: ['ancestors'] }],
    };

    const result = resolveCollapseStatusByUuid(trivialNodesByUuid, collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['1'].collapsed).toBe(false);
    expect(result['2'].collapsed).toBe(true);
    expect(result['3'].collapsed).toBe(true); // Note since 2 is collapsed, 3 will be collapsed.
  });

  it('ancestor trumps descendant on conflicting states', () => {
    const collapseOpsByUuid = {
      1: [{ operation: 'expand', priority: 1, targets: ['descendants'] }],
      3: [{ operation: 'collapse', priority: 1, targets: ['ancestors'] }],
    };

    const result = resolveCollapseStatusByUuid(trivialNodesByUuid, collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['1'].collapsed).toBe(true);
    expect(result['2'].collapsed).toBe(false);
    expect(result['3'].collapsed).toBe(false);
  });

  it('nearer descendant trumps farther descendant on conflicting states', () => {
    const collapseOpsByUuid = {
      // expect 1 to be expanded for this reason.
      2: [{ operation: 'expand', priority: 1, targets: ['ancestors'] }],
      3: [{ operation: 'collapse', priority: 1, targets: ['ancestors'] }],
    };

    const result = resolveCollapseStatusByUuid(trivialNodesByUuid, collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['1'].collapsed).toBe(false);
    expect(result['2'].collapsed).toBe(true);
    expect(result['3'].collapsed).toBe(true); // collapsed because parent (2) is collapsed.
  });

  it('children are collapsed if parent is collapsed', () => {
    const collapseOpsByUuid = {
      1: [{ operation: 'collapse', priority: 1, targets: ['self'] }],
    };
    const nodesByUuid = {
      1: { uuid: '1', parent_id: null },
      2: { uuid: '2', parent_id: '1' },
      3: { uuid: '3', parent_id: '1' },
    };

    const result = resolveCollapseStatusByUuid(nodesByUuid, collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['1'].collapsed).toBe(true);
    expect(result['2'].collapsed).toBe(true);
    expect(result['3'].collapsed).toBe(true);
  });

  it('considers priority trivial descendants', () => {
    const collapseOpsByUuid = {
      2: [{ operation: 'expand', priority: 1, targets: ['self'] }],
      3: [{ operation: 'collapse', priority: 10, targets: ['ancestors'] }],
    };

    const result = resolveCollapseStatusByUuid(trivialNodesByUuid, collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['1'].collapsed).toBe(true);
    expect(result['2'].collapsed).toBe(false);
    // since 2 isn't collapsed, there is no reason for 3 to be collapsed.
    expect(result['3'].collapsed).toBe(false);
  });

  it('considers priority trivial descendants', () => {
    const collapseOpsByUuid = {
      2: [{ operation: 'expand', priority: 1, targets: ['self'] }],
      3: [{ operation: 'collapse', priority: 10, targets: ['ancestors'] }],
    };

    const result = resolveCollapseStatusByUuid(trivialNodesByUuid, collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['1'].collapsed).toBe(true);
    expect(result['2'].collapsed).toBe(false);
    // since 2 isn't collapsed, there is no reason for 3 to be collapsed.
    expect(result['3'].collapsed).toBe(false);
  });

  it('collapses grandchildren', () => {
    const collapseOpsByUuid = {
      1: [{ operation: 'collapse', priority: 1, targets: ['grandchildren'] }],
    };

    const result = resolveCollapseStatusByUuid(trivialNodesByUuid, collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['1'].collapsed).toBe(false);
    expect(result['2'].collapsed).toBe(false);
    expect(result['3'].collapsed).toBe(true);
  });

  it('collapses root node', () => {
    const collapsedByUuid = {
      1: { uuid: '1', parent_id: null, collapsed: true },
      2: { uuid: '2', parent_id: '1', collapsed: false },
      3: { uuid: '3', parent_id: '1', collapsed: false },
    };

    const result = getCollapsedGraphByNodeUuid(collapsedByUuid, getRandomInt);
    expect(_.size(result)).toEqual(3);
    expect(result['2'].parent_id).toEqual(result['3'].parent_id);
    expect(_.has(result, '1')).toBe(false);
  });

  it('avoids creating new nodes when parent and children are all collapsed', () => {
    const collapsedByUuid = {
      1: { uuid: '1', parent_id: null, collapsed: true },
      2: { uuid: '2', parent_id: '1', collapsed: true },
      3: { uuid: '3', parent_id: '1', collapsed: true },
    };

    const result = getCollapsedGraphByNodeUuid(collapsedByUuid, getRandomInt);
    expect(_.size(result)).toEqual(1);

    const node = _.first(_.values(result));
    expect(node.collapsed).toBe(true);
    expect([node.uuid]).toEqual(
      expect.not.arrayContaining(['1', '2', '3']),
    );
  });

  it('collects sequential collapsed nodes', () => {
    const collapsedByUuid = {
      1: { uuid: '1', parent_id: null, collapsed: false },
      2: { uuid: '2', parent_id: '1', collapsed: true },
      3: { uuid: '3', parent_id: '2', collapsed: false },
    };

    const result = getCollapsedGraphByNodeUuid(collapsedByUuid, getRandomInt);
    expect(_.size(result)).toEqual(3);
    const newId = _.difference(_.keys(result), ['1', '3'])[0];
    expect(result[newId].parent_id).toEqual('1');
    expect(result['3'].parent_id).toEqual(newId);
    expect(_.has(result, '2')).toBe(false);
  });

  it('Collapses multi-level heirarchy', () => {
    //          1                       x
    //      2       4               2       4
    //      3     5   6     =>      x     5   x
    //                7
    const collapsedByUuid = {
      1: { uuid: '1', parent_id: null, collapsed: true },
      2: { uuid: '2', parent_id: '1', collapsed: false },
      3: { uuid: '3', parent_id: '2', collapsed: true },
      4: { uuid: '4', parent_id: '1', collapsed: false },
      5: { uuid: '5', parent_id: '4', collapsed: false },
      6: { uuid: '6', parent_id: '4', collapsed: true },
      7: { uuid: '7', parent_id: '6', collapsed: true },
    };

    const result = getCollapsedGraphByNodeUuid(collapsedByUuid, getRandomInt);
    expect(_.size(result)).toEqual(6);
    expect(_.intersection(_.keys(result), ['2', '4', '5']).length).toEqual(3);
  });

  it('Counts task descendants instead of collapse descendants', () => {
    const collapsedByUuid = {
      1: { uuid: '1', parent_id: null, collapsed: false },
      2: { uuid: '2', parent_id: '1', collapsed: true },
      3: { uuid: '3', parent_id: '1', collapsed: true },
      4: { uuid: '4', parent_id: '2', collapsed: true },
    };

    const result = getCollapsedGraphByNodeUuid(collapsedByUuid, getRandomInt);
    expect(_.size(result)).toEqual(2);
    const collapseNode = _.find(result, n => n.parent_id === '1');
    expect(collapseNode.allRepresentedNodeUuids.sort()).toEqual(['2', '3', '4']);
    expect(collapseNode.representedChildrenUuids.sort()).toEqual(['2', '3']);
  });

  it('Counts single collapsed leaf child.', () => {
    const collapsedByUuid = {
      1: { uuid: '1', parent_id: null, collapsed: false },
      2: { uuid: '2', parent_id: '1', collapsed: true },
    };

    const result = getCollapsedGraphByNodeUuid(collapsedByUuid, getRandomInt);
    expect(_.size(result)).toEqual(2);
    const collapseNode = _.find(result, n => n.parent_id === '1');
    expect(collapseNode.allRepresentedNodeUuids).toEqual(['2']);
    expect(collapseNode.representedChildrenUuids).toEqual(['2']);
  });

  it('Counts multiple collapsed leaf children as 1', () => {
    const collapsedByUuid = {
      1: { uuid: '1', parent_id: null, collapsed: false },
      2: { uuid: '2', parent_id: '1', collapsed: true },
      3: { uuid: '3', parent_id: '1', collapsed: true },
    };

    const result = getCollapsedGraphByNodeUuid(collapsedByUuid, getRandomInt);
    expect(_.size(result)).toEqual(2);
    const collapseNode = _.find(result, n => n.parent_id === '1');
    expect(collapseNode.allRepresentedNodeUuids.sort()).toEqual(['2', '3']);
    expect(collapseNode.representedChildrenUuids.sort()).toEqual(['2', '3']);
  });
});
