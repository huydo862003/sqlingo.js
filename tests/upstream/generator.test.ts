import {
  describe, it, expect,
} from 'vitest';
import {
  parseOne, toColumn,
} from '../../src/index';
import {
  DataTypeExpr, DateTruncExpr, FuncExpr, VarExpr,
} from '../../src/expressions';
import { Parser } from '../../src/parser';
import { Tokenizer } from '../../src/tokens';

describe('TestGenerator', () => {
  it('test_fallback_function_sql', () => {
    class SpecialUDF extends FuncExpr {
      static requiredArgs = new Set(['a']);
      static availableArgs = new Set(['a', 'b']);
    }

    class NewParser extends Parser {
      static override FUNCTIONS = {
        ...Parser.FUNCTIONS,
        ...SpecialUDF.defaultParserMappings(),
      };
    }

    const tokens = new Tokenizer().tokenize('SELECT SPECIAL_UDF(a) FROM x');
    const expression = new NewParser().parse(tokens)[0];
    expect(expression?.sql()).toBe('SELECT SPECIAL_UDF(a) FROM x');
  });

  it('test_fallback_function_var_args_sql', () => {
    class SpecialUDF extends FuncExpr {
      static argTypes = {
        a: true,
        expressions: false,
      };

      static override isVarLenArgs = true;
    }

    class NewParser extends Parser {
      static override FUNCTIONS = {
        ...Parser.FUNCTIONS,
        ...SpecialUDF.defaultParserMappings(),
      };
    }

    const tokens = new Tokenizer().tokenize('SELECT SPECIAL_UDF(a, b, c, d + 1) FROM x');
    const expression = new NewParser().parse(tokens)[0];
    expect(expression?.sql()).toBe('SELECT SPECIAL_UDF(a, b, c, d + 1) FROM x');

    expect(
      new DateTruncExpr({
        this: toColumn('event_date'),
        unit: new VarExpr({ this: 'MONTH' }),
      }).sql(),
    ).toBe('DATE_TRUNC(\'MONTH\', event_date)');
  });

  it('test_identify', () => {
    expect(parseOne('x').sql({ identify: true })).toBe('"x"');
    expect(parseOne('x').sql({ identify: false })).toBe('x');
    expect(parseOne('X').sql({ identify: true })).toBe('"X"');
    expect(parseOne('"x"').sql({ identify: false })).toBe('"x"');
    expect(parseOne('x').sql({ identify: 'safe' })).toBe('"x"');
    expect(parseOne('X').sql({ identify: 'safe' })).toBe('X');
    expect(parseOne('x as 1').sql({ identify: 'safe' })).toBe('"x" AS "1"');
    expect(parseOne('X as 1').sql({ identify: 'safe' })).toBe('X AS "1"');
  });

  it('test_generate_nested_binary', () => {
    const sql = 'SELECT \'foo\'' + (' || \'foo\'').repeat(1000);
    expect(parseOne(sql).sql({ copy: false })).toBe(sql);
  });

  it('test_overlap_operator', () => {
    for (const op of ['&<', '&>']) {
      const inputSql = `SELECT '[1,10]'::int4range ${op} '[5,15]'::int4range`;
      const expectedSql = `SELECT CAST('[1,10]' AS INT4RANGE) ${op} CAST('[5,15]' AS INT4RANGE)`;
      const ast = parseOne(inputSql, { dialect: 'postgres' });
      expect(ast.sql()).toBe(expectedSql);
      expect(ast.sql({ dialect: 'postgres' })).toBe(expectedSql);
    }
  });

  it('test_pretty_nested_types', () => {
    function assertPrettyNested (
      datatype: DataTypeExpr | undefined,
      singleLine: string,
      pretty: string,
      maxTextWidth = 10,
      extraOpts: Record<string, unknown> = {},
    ): void {
      expect(datatype?.sql()).toBe(singleLine);
      expect(datatype?.sql({
        pretty: true,
        maxTextWidth,
        ...extraOpts,
      })).toBe(pretty);
    }

    // STRUCT
    let typeStr = 'STRUCT<a INT, b TEXT>';
    assertPrettyNested(
      DataTypeExpr.build(typeStr),
      typeStr,
      'STRUCT<\n  a INT,\n  b TEXT\n>',
    );

    // STRUCT - type def shorter than max text width so stays one line
    assertPrettyNested(
      DataTypeExpr.build(typeStr),
      typeStr,
      'STRUCT<a INT, b TEXT>',
      50,
    );

    // STRUCT, leadingComma = true
    assertPrettyNested(
      DataTypeExpr.build(typeStr),
      typeStr,
      'STRUCT<\n  a INT\n  , b TEXT\n>',
      10,
      { leadingComma: true },
    );

    // ARRAY
    typeStr = 'ARRAY<DECIMAL(38, 9)>';
    assertPrettyNested(
      DataTypeExpr.build(typeStr),
      typeStr,
      'ARRAY<\n  DECIMAL(38, 9)\n>',
    );

    // ARRAY nested STRUCT
    typeStr = 'ARRAY<STRUCT<a INT, b TEXT>>';
    assertPrettyNested(
      DataTypeExpr.build(typeStr),
      typeStr,
      'ARRAY<\n  STRUCT<\n    a INT,\n    b TEXT\n  >\n>',
    );

    // RANGE
    typeStr = 'RANGE<DECIMAL(38, 9)>';
    assertPrettyNested(
      DataTypeExpr.build(typeStr),
      typeStr,
      'RANGE<\n  DECIMAL(38, 9)\n>',
    );

    // LIST
    typeStr = 'LIST<INT, INT, TEXT>';
    assertPrettyNested(
      DataTypeExpr.build(typeStr),
      typeStr,
      'LIST<\n  INT,\n  INT,\n  TEXT\n>',
    );

    // MAP
    typeStr = 'MAP<INT, DECIMAL(38, 9)>';
    assertPrettyNested(
      DataTypeExpr.build(typeStr),
      typeStr,
      'MAP<\n  INT,\n  DECIMAL(38, 9)\n>',
    );
  });
});
