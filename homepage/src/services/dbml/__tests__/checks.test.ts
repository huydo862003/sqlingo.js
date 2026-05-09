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
    const column = schema.tables[0].columns[0];

    expect(column.check?.expression).toContain('age');
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

  it('parses named CONSTRAINT CHECK wrapper', () => {
    const {
      schema,
    } = sqlToDbml(
      'CREATE TABLE t (a INT, b INT, CONSTRAINT chk_ab CHECK (a < b));',
      'postgres',
    );
    const check = schema.tables[0].checks?.[0];

    expect(check?.name).toBe('chk_ab');
    expect(check?.expression).toContain('a < b');
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
