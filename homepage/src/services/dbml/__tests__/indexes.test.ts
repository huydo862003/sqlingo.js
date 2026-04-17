import {
  describe, expect, it,
} from 'vitest';
import {
  sqlToDbml,
} from '../index';

describe('indexes', () => {
  it('attaches CREATE INDEX to prior table', () => {
    const {
      schema,
    } = sqlToDbml(`
      CREATE TABLE t (a INT, b INT);
      CREATE UNIQUE INDEX idx_ab ON t USING btree (a, b);
    `, 'postgres');
    const idx = schema.tables[0].indexes?.[0];
    expect(idx).toMatchObject({
      unique: true,
      type: 'btree',
      name: 'idx_ab',
    });
    expect(idx?.columns.map((c) => c.expression)).toEqual([
      'a',
      'b',
    ]);
  });

  it('marks functional index columns as expression', () => {
    const {
      schema,
    } = sqlToDbml(`
      CREATE TABLE t (a INT, b TEXT);
      CREATE INDEX idx_f ON t (lower(b));
    `, 'postgres');
    const col = schema.tables[0].indexes?.[0].columns[0];
    expect(col?.isExpression).toBe(true);
  });
});
