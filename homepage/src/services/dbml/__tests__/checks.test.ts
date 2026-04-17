import {
  describe, expect, it,
} from 'vitest';
import {
  sqlToDbml,
} from '../index';

describe('checks', () => {
  it('parses column-level CHECK as DbmlCheck on column', () => {
    const {
      schema,
    } = sqlToDbml(
      'CREATE TABLE t (age INT CHECK (age > 0));',
      'postgres',
    );
    const col = schema.tables[0].columns[0];
    expect(col.check?.expression).toContain('age');
  });

  it('parses table-level CHECK as DbmlCheck in table.checks', () => {
    const {
      schema,
    } = sqlToDbml(
      'CREATE TABLE t (a INT, b INT, CHECK (a < b));',
      'postgres',
    );
    expect(schema.tables[0].checks?.[0].expression).toContain('a < b');
  });

  it('emits column check and table Checks block', () => {
    const {
      dbml,
    } = sqlToDbml(
      'CREATE TABLE t (a INT CHECK (a > 0), b INT, CHECK (b < 100));',
      'postgres',
    );
    expect(dbml).toMatch(/a int \[check: `.*a.*`\]/);
    expect(dbml).toContain('Checks {');
    expect(dbml).toMatch(/`.*b.*< 100.*`/);
  });
});
