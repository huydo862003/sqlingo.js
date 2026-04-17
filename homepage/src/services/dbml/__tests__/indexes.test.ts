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

  it('emits composite PRIMARY KEY as pk index', () => {
    const {
      schema,
    } = sqlToDbml(
      'CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b));',
      'postgres',
    );
    const idx = schema.tables[0].indexes?.[0];
    expect(idx?.pk).toBe(true);
    expect(idx?.columns.map((c) => c.expression)).toEqual([
      'a',
      'b',
    ]);
  });

  it('emits named CONSTRAINT PRIMARY KEY composite', () => {
    const {
      schema,
    } = sqlToDbml(
      'CREATE TABLE t (a INT, b INT, CONSTRAINT pk_ab PRIMARY KEY (a, b));',
      'postgres',
    );
    const idx = schema.tables[0].indexes?.[0];
    expect(idx).toMatchObject({
      pk: true,
      name: 'pk_ab',
    });
  });

  it('emits standalone UNIQUE composite as unique index', () => {
    const {
      schema,
    } = sqlToDbml(
      'CREATE TABLE t (a INT, b INT, UNIQUE (a, b));',
      'postgres',
    );
    const idx = schema.tables[0].indexes?.[0];
    expect(idx?.unique).toBe(true);
    expect(idx?.columns.map((c) => c.expression)).toEqual([
      'a',
      'b',
    ]);
  });

  it('emits named CONSTRAINT UNIQUE composite', () => {
    const {
      schema,
    } = sqlToDbml(
      'CREATE TABLE t (a INT, b INT, CONSTRAINT uq_ab UNIQUE (a, b));',
      'postgres',
    );
    const idx = schema.tables[0].indexes?.[0];
    expect(idx).toMatchObject({
      unique: true,
      name: 'uq_ab',
    });
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
