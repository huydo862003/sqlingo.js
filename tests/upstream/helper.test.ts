import {
  describe, it, expect,
} from 'vitest';
import {
  mergeRanges, nameSequence, tsort,
} from '../../src/helper';

describe('TestHelper', () => {
  it('test_tsort', () => {
    expect(tsort(new Map([['a', new Set()]]))).toEqual(['a']);
    expect(tsort(new Map([['a', new Set(['b'])]]))).toEqual(['b', 'a']);
    expect(tsort(new Map([
      ['a', new Set(['c'])],
      ['b', new Set()],
      ['c', new Set()],
    ]))).toEqual([
      'b',
      'c',
      'a',
    ]);
    expect(tsort(new Map([
      ['a', new Set(['b', 'c'])],
      ['b', new Set(['c'])],
      ['c', new Set()],
      ['d', new Set(['a'])],
    ]))).toEqual([
      'c',
      'b',
      'a',
      'd',
    ]);

    expect(() => tsort(new Map([
      ['a', new Set(['b', 'c'])],
      ['b', new Set(['a'])],
      ['c', new Set()],
    ]))).toThrow();
  });

  it('test_name_sequence', () => {
    const s1 = nameSequence('a');
    const s2 = nameSequence('b');

    expect(s1()).toBe('a0');
    expect(s1()).toBe('a1');
    expect(s2()).toBe('b0');
    expect(s1()).toBe('a2');
    expect(s2()).toBe('b1');
    expect(s2()).toBe('b2');
  });

  it('test_merge_ranges', () => {
    expect(mergeRanges([])).toEqual([]);
    expect(mergeRanges([[0, 1]])).toEqual([[0, 1]]);
    expect(mergeRanges([[0, 1], [2, 3]])).toEqual([[0, 1], [2, 3]]);
    expect(mergeRanges([[0, 1], [1, 3]])).toEqual([[0, 3]]);
    expect(mergeRanges([
      [2, 3],
      [0, 1],
      [3, 4],
    ])).toEqual([[0, 1], [2, 4]]);
  });
});
