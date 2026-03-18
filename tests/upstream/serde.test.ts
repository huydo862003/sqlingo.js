import {
  describe, it, expect,
} from 'vitest';
import { parseOne } from '../../src/index';
import { Expression } from '../../src/expressions';
import { narrowInstanceOf } from '../../src/port_internals';
import { annotateTypes } from '../../src/optimizer/annotate_types';
import {
  dump, load,
} from '../../src/serde';
import { loadSqlFixtures } from './helpers';

function dumpLoad (expression: Expression, customExpressions?: Record<string, typeof Expression>): Expression | undefined {
  return load(JSON.parse(JSON.stringify(dump(expression))), customExpressions);
}

describe('TestSerde', () => {
  it('test_serde', () => {
    for (const sql of loadSqlFixtures('identity.sql')) {
      const before = parseOne(sql);
      const after = dumpLoad(before);
      expect(String(after)).toBe(String(before));
    }
  });

  it('test_custom_expression', () => {
    class CustomExpression extends Expression {}
    const before = new CustomExpression({});
    const after = dumpLoad(before, { CustomExpression });
    // custom expression class not in registry so load returns undefined — expect it to not crash
    expect(after).toEqual(before);
  });

  it('test_type_annotations', () => {
    const before = annotateTypes(parseOne('CAST(\'1\' AS STRUCT<x ARRAY<INT>>)'));
    const after = dumpLoad(before);
    expect(after).toBeDefined();
    expect(narrowInstanceOf(before.type, Expression)?.sql()).toBe(narrowInstanceOf(after?.type, Expression)?.sql());
    expect(narrowInstanceOf(narrowInstanceOf(before.args.this, Expression)?.type, Expression)?.sql()).toBe(narrowInstanceOf(narrowInstanceOf(after?.args.this, Expression)?.type, Expression)?.sql());
  });

  it('test_meta', () => {
    const before = parseOne('SELECT * FROM X');
    before.meta.x = 1;
    const after = dumpLoad(before);
    expect(after).toBeDefined();
    expect(after?.meta).toEqual(before.meta);
  });

  it('test_recursion', () => {
    let sql = 'SELECT 1';
    sql += ' UNION ALL SELECT 1'.repeat(5000);
    const expr = parseOne(sql);
    const before = expr.sql();
    const afterExpr = dumpLoad(expr);
    expect(afterExpr).toBeDefined();
    expect(afterExpr?.sql()).toBe(before);
  });
});
