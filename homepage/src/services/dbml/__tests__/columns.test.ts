import {
  describe, expect, it,
} from 'vitest';
import {
  sqlToDbml,
} from '../index';

describe('columns', () => {
  it('maps pk, not null, unique, increment, default', () => {
    const {
      schema,
    } = sqlToDbml(`
      CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        email VARCHAR(255) NOT NULL UNIQUE,
        name VARCHAR(100),
        active BOOLEAN DEFAULT TRUE
      );
    `, 'postgres');
    const table = schema.tables[0];
    expect(table.columns[0]).toMatchObject({
      name: 'id',
      pk: true,
      notNull: true,
    });
    expect(table.columns[1]).toMatchObject({
      name: 'email',
      notNull: true,
      unique: true,
    });
    expect(table.columns[1].type.args).toEqual(['255']);
    expect(table.columns[3].default).toBeDefined();
  });

  it('maps INT[] to array=true', () => {
    const {
      schema,
    } = sqlToDbml('CREATE TABLE t (tags INT[]);', 'postgres');
    expect(schema.tables[0].columns[0].type.array).toBe(true);
  });

  it('parses composite primary key as index pk, not per-column pk', () => {
    const {
      schema,
    } = sqlToDbml('CREATE TABLE pair (a INT, b INT, PRIMARY KEY (a, b));', 'postgres');
    const table = schema.tables[0];
    expect(table.columns.every((col) => !col.pk)).toBe(true);
    const index = table.indexes?.[0];
    expect(index?.pk).toBe(true);
    expect(index?.columns.map((col) => col.expression)).toEqual([
      'a',
      'b',
    ]);
  });
});
