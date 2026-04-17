import {
  describe, expect, it,
} from 'vitest';
import {
  sqlToDbml,
} from '../index';

describe('records', () => {
  it('collects INSERT rows into a single record block per table', () => {
    const {
      schema,
    } = sqlToDbml(`
      CREATE TABLE t (a INT, b INT);
      INSERT INTO t (a, b) VALUES (1, 2), (3, 4);
      INSERT INTO t (a, b) VALUES (5, 6);
    `, 'postgres');
    expect(schema.records).toHaveLength(1);
    expect(schema.records[0].rows).toHaveLength(3);
    expect(schema.records[0].columns).toEqual([
      'a',
      'b',
    ]);
  });
});
