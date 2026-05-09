import {
  describe, expect, it,
} from 'vitest';
import {
  sqlToDbml,
} from '../index';

describe('tables', () => {
  it('parses schema-qualified table names', () => {
    const {
      schema,
    } = sqlToDbml('CREATE TABLE app.users (id INT);', 'postgres');

    expect(schema.tables[0]).toMatchObject({
      schema: 'app',
      name: 'users',
    });
  });

  it('parses catalog.schema.table as schema "catalog.schema"', () => {
    const {
      schema,
    } = sqlToDbml('CREATE TABLE db.app.users (id INT);', 'postgres');

    expect(schema.tables[0].schema).toBe('db.app');
    expect(schema.tables[0].name).toBe('users');
  });

  it('throws on unsupported syntax', () => {
    expect(() => sqlToDbml('GIBBERISH NOT SQL AT ALL @@@', 'postgres')).toThrow();
  });
});
