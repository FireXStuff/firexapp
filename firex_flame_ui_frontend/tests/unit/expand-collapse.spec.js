import _ from 'lodash';

import { getCollapsedGraphByNodeUuid } from '@/utils.js';
import { getGraphDataByUuid } from '@/graph-utils.js';
import { resolveCollapseStatusByUuid } from '@/collapse.js';

describe('utils.js', () => {

  const trivRoot = 1;
  const trivialGraph = getGraphDataByUuid(trivRoot, {
    1: null,
    2: '1',
    3: '2',
  });

  it('collapses trivial node via self', () => {
    const collapseOpsByUuid = {
      2: [{ operation: 'collapse', priority: 1, targets: ['self'] }],
    };
    const result = resolveCollapseStatusByUuid(trivRoot, trivialGraph, collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(_.find(result, n => n.parent_id === '1').collapsed).toBe(true);
    expect(result['1'].collapsed).toBe(false);
  });

  it('collapses trivial descendants', () => {
    const collapseOpsByUuid = {
      1: [{ operation: 'collapse', priority: 1, targets: ['descendants'] }],
    };

    const result = resolveCollapseStatusByUuid(trivRoot, trivialGraph, collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['2'].collapsed).toBe(true);
    expect(result['1'].collapsed).toBe(false);
  });

  it('expands self even when ancestor collapses descendants', () => {
    const collapseOpsByUuid = {
      1: [{ operation: 'collapse', priority: 1, targets: ['descendants'] }],
      3: [{ operation: 'expand', priority: 1, targets: ['self'] }],
    };

    const result = resolveCollapseStatusByUuid(trivRoot, trivialGraph, collapseOpsByUuid);
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

    const result = resolveCollapseStatusByUuid(trivRoot, trivialGraph, collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['1'].collapsed).toBe(false); // TODO: root never collapsed.
    expect(result['2'].collapsed).toBe(true);
    expect(result['3'].collapsed).toBe(false);
  });

  it('expands self even when descendant collapses ancestors', () => {
    const collapseOpsByUuid = {
      1: [{ operation: 'expand', priority: 1, targets: ['self'] }],
      3: [{ operation: 'collapse', priority: 1, targets: ['ancestors'] }],
    };

    const result = resolveCollapseStatusByUuid(trivRoot, trivialGraph, collapseOpsByUuid);
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

    const result = resolveCollapseStatusByUuid(trivRoot, trivialGraph, collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['1'].collapsed).toBe(false); // TODO: root never collapsed.
    expect(result['2'].collapsed).toBe(false);
    expect(result['3'].collapsed).toBe(false);
  });

  it('nearer descendant trumps farther descendant on conflicting states', () => {
    const collapseOpsByUuid = {
      // expect 1 to be expanded for this reason.
      2: [{ operation: 'expand', priority: 1, targets: ['ancestors'] }],
      3: [{ operation: 'collapse', priority: 1, targets: ['ancestors'] }],
    };

    const result = resolveCollapseStatusByUuid(trivRoot, trivialGraph, collapseOpsByUuid);
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
      1: null,
      2: '1',
      3: '1',
    };

    const result = resolveCollapseStatusByUuid('1', getGraphDataByUuid('1', nodesByUuid),
      collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['1'].collapsed).toBe(false); // TODO: root never collapsed.
    expect(result['2'].collapsed).toBe(true);
    expect(result['3'].collapsed).toBe(true);
  });

  it('considers priority trivial descendants', () => {
    const collapseOpsByUuid = {
      2: [{ operation: 'expand', priority: 1, targets: ['self'] }],
      3: [{ operation: 'collapse', priority: 10, targets: ['ancestors'] }],
    };

    const result = resolveCollapseStatusByUuid(trivRoot, trivialGraph, collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['1'].collapsed).toBe(false); // TODO: root never collapsed.
    expect(result['2'].collapsed).toBe(false);
    // since 2 isn't collapsed, there is no reason for 3 to be collapsed.
    expect(result['3'].collapsed).toBe(false);
  });

  it('considers priority trivial descendants', () => {
    const collapseOpsByUuid = {
      2: [{ operation: 'expand', priority: 1, targets: ['self'] }],
      3: [{ operation: 'collapse', priority: 10, targets: ['ancestors'] }],
    };

    const result = resolveCollapseStatusByUuid(trivRoot, trivialGraph, collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['1'].collapsed).toBe(false); // TODO: root never collapsed.
    expect(result['2'].collapsed).toBe(false);
    // since 2 isn't collapsed, there is no reason for 3 to be collapsed.
    expect(result['3'].collapsed).toBe(false);
  });

  it('collapses grandchildren', () => {
    const collapseOpsByUuid = {
      1: [{ operation: 'collapse', priority: 1, targets: ['grandchildren'] }],
    };

    const result = resolveCollapseStatusByUuid(trivRoot, trivialGraph, collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['1'].collapsed).toBe(false);
    expect(result['2'].collapsed).toBe(false);
    expect(result['3'].collapsed).toBe(true);
  });

  it('does not collapse root node', () => {
    const collapsedByUuid = {
      1: { uuid: '1', parent_id: null, collapsed: true },
      2: { uuid: '2', parent_id: '1', collapsed: false },
      3: { uuid: '3', parent_id: '1', collapsed: false },
    };

    const result = getCollapsedGraphByNodeUuid(collapsedByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['1'].collapsedUuids).toEqual([]);
    expect(result['2'].collapsedUuids).toEqual([]);
    expect(result['3'].collapsedUuids).toEqual([]);
  });

  it('Re-assigns parent ID when a collapsed child has an uncollapsed child', () => {
    const collapsedByUuid = {
      1: { uuid: '1', parent_id: null, collapsed: false },
      2: { uuid: '2', parent_id: '1', collapsed: true },
      3: { uuid: '3', parent_id: '2', collapsed: false },
    };

    const result = getCollapsedGraphByNodeUuid(collapsedByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['3'].parent_id).toEqual('1');
  });

  it('Collapses multi-level heirarchy', () => {
    //          1                       1
    //      2       4               x       4
    //      3     5   6     =>      3     5   x
    //                7                       x
    const collapsedByUuid = {
      1: { uuid: '1', parent_id: null, collapsed: false },
      2: { uuid: '2', parent_id: '1', collapsed: true },
      3: { uuid: '3', parent_id: '2', collapsed: false },
      4: { uuid: '4', parent_id: '1', collapsed: false },
      5: { uuid: '5', parent_id: '4', collapsed: false },
      6: { uuid: '6', parent_id: '4', collapsed: true },
      7: { uuid: '7', parent_id: '6', collapsed: true },
    };

    const result = getCollapsedGraphByNodeUuid(collapsedByUuid);
    expect(_.size(result)).toEqual(7);
    expect(result['1'].collapsedUuids).toEqual(['2']);
    expect(result['3'].parent_id).toEqual('1');
    expect(result['4'].collapsedUuids.sort()).toEqual(['6', '7']);
    expect(result['5'].parent_id).toEqual('4');
  });

  it('Counts task descendants instead of collapse descendants', () => {
    const collapsedByUuid = {
      1: { uuid: '1', parent_id: null, collapsed: false },
      2: { uuid: '2', parent_id: '1', collapsed: true },
      3: { uuid: '3', parent_id: '1', collapsed: true },
      4: { uuid: '4', parent_id: '2', collapsed: true },
    };

    const result = getCollapsedGraphByNodeUuid(collapsedByUuid);
    expect(_.size(result)).toEqual(4);
    expect(result[1].collapsedUuids.sort()).toEqual(['2', '3', '4']);
  });

  it('Counts multiple collapsed leaf children as 1', () => {
    const collapsedByUuid = {
      1: { uuid: '1', parent_id: null, collapsed: false },
      2: { uuid: '2', parent_id: '1', collapsed: true },
      3: { uuid: '3', parent_id: '1', collapsed: true },
    };

    const result = getCollapsedGraphByNodeUuid(collapsedByUuid);
    expect(_.size(result)).toEqual(3);

    expect(result['1'].collapsedUuids.sort()).toEqual(['2', '3']);
    expect(result['2'].collapsedUuids).toEqual([]);
    expect(result['3'].collapsedUuids).toEqual([]);
  });
});
