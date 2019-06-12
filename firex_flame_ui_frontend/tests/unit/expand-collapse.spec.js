import _ from 'lodash';

import { getGraphDataByUuid } from '@/graph-utils.js';
import { resolveCollapseStatusByUuid, getCollapsedGraphByNodeUuid } from '@/collapse.js';

describe('utils.js', () => {
  const trivRoot = 1;
  const trivChainDepthByUuid = _.keyBy(['1', '2', '3'], () => 0);
  const trivialGraph = getGraphDataByUuid(trivRoot, {
    1: null,
    2: '1',
    3: '2',
  },
  null,
  trivChainDepthByUuid);

  it('collapses trivial node via self', () => {
    const collapseOpsByUuid = {
      2: [{ operation: 'collapse', priority: 1, targets: ['self'] }],
    };
    const result = resolveCollapseStatusByUuid(trivRoot, trivialGraph, collapseOpsByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['2'].collapsed).toBe(true);
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
    expect(result['3'].collapsed).toBe(false);
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
    const childrenUuidsByUuid = {
      1: ['2', '3'],
      2: [],
      3: [],
    };
    const isCollapsedByUuid = {
      1: true,
      2: false,
      3: false,
    };

    const result = getCollapsedGraphByNodeUuid('1', childrenUuidsByUuid, isCollapsedByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['1'].collapsedUuids).toEqual([]);
    expect(result['2'].collapsedUuids).toEqual([]);
    expect(result['3'].collapsedUuids).toEqual([]);
  });

  it('Re-assigns parent ID when a collapsed child has an uncollapsed child', () => {
    const childrenUuidsByUuid = {
      1: ['2', '3'],
      2: [],
      3: [],
    };
    const isCollapsedByUuid = {
      1: false,
      2: true,
      3: false,
    };

    const result = getCollapsedGraphByNodeUuid('1', childrenUuidsByUuid, isCollapsedByUuid);
    expect(_.size(result)).toEqual(3);
    expect(result['3'].parentId).toEqual('1');
  });

  it('Collapses multi-level heirarchy', () => {
    //          1                       1
    //      2       4               x       4
    //      3     5   6     =>      3     5   x
    //                7                       x
    const childrenUuidsByUuid = {
      1: ['2', '4'],
      2: ['3'],
      3: [],
      4: ['5', '6'],
      5: [],
      6: ['7'],
      7: [],
    };
    const isCollapsedByUuid = {
      1: false,
      2: true,
      3: false,
      4: false,
      5: false,
      6: true,
      7: true,
    };

    const result = getCollapsedGraphByNodeUuid('1', childrenUuidsByUuid, isCollapsedByUuid);
    expect(_.size(result)).toEqual(7);
    expect(result['1'].collapsedUuids).toEqual(['2']);
    expect(result['3'].parentId).toEqual('1');
    expect(result['4'].collapsedUuids.sort()).toEqual(['6', '7']);
    expect(result['5'].parentId).toEqual('4');
  });

  it('Counts task descendants instead of collapse descendants', () => {
    const childrenUuidsByUuid = {
      1: ['2', '3'],
      2: ['4'],
      3: [],
      4: [],
    };
    const isCollapsedByUuid = {
      1: false,
      2: true,
      3: true,
      4: true,
    };

    const result = getCollapsedGraphByNodeUuid('1', childrenUuidsByUuid, isCollapsedByUuid);
    expect(_.size(result)).toEqual(4);
    expect(result[1].collapsedUuids.sort()).toEqual(['2', '3', '4']);
  });

  it('Counts multiple collapsed leaf children as 1', () => {
    const childrenUuidsByUuid = {
      1: ['2', '3'],
      2: [],
      3: [],
    };
    const isCollapsedByUuid = {
      1: false,
      2: true,
      3: true,
    };

    const result = getCollapsedGraphByNodeUuid('1', childrenUuidsByUuid, isCollapsedByUuid);
    expect(_.size(result)).toEqual(3);

    expect(result['1'].collapsedUuids.sort()).toEqual(['2', '3']);
    expect(result['2'].collapsedUuids).toEqual([]);
    expect(result['3'].collapsedUuids).toEqual([]);
  });
});
