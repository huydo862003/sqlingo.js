import {
  describe, test, expect, vi,
} from 'vitest';
import {
  parse,
  parseOne,
  Parser,
  ParseError,
  ErrorLevel,
} from '../src/index';
import {
  Expression,
  TupleExpr,
  SelectExpr,
  JoinExpr,
  DataTypeExpr,
  TableExpr,
  WhensExpr,
  FromExpr,
  UnionExpr,
  LimitExpr,
  TransactionExpr,
  ColumnExpr,
  AliasExpr,
  UnnestExpr,
  LiteralExpr,
  VarExpr,
  HintExpr,
  SetExpr,
  SetItemExpr,
  EqExpr,
  CommandExpr,
  DotExpr,
  AnonymousExpr,
  YearExpr,
  CollateExpr,
  IdentifierExpr,
  DataTypeParamExpr,
  CoalesceExpr,
  IntervalExpr,
  OrExpr,
  PropertyEqExpr,
  GrantPrincipalExpr,
  GrantPrivilegeExpr,
  FuncExpr,
  StarExpr,
  DropExpr,
  CreateExpr,
  ColumnDefExpr,
} from '../src/expressions';
import { narrowInstanceOf } from '../src/port_internals';

// Subclass to expose protected errors for testing
class TestableParser extends Parser {
  getErrors (): ParseError[] {
    return this.errors;
  }
}

class TestParser {
  testParseEmpty () {
    expect(() => parseOne('')).toThrow(ParseError);
  }

  testParseInto () {
    expect(parseOne('(1)', { into: TupleExpr })).toBeInstanceOf(TupleExpr);
    expect(parseOne('(1,)', { into: TupleExpr })).toBeInstanceOf(TupleExpr);
    expect(parseOne('(x=1)', { into: TupleExpr })).toBeInstanceOf(TupleExpr);

    expect(parseOne('select * from t', { into: SelectExpr })).toBeInstanceOf(SelectExpr);
    expect(parseOne('select * from t limit 5', { into: SelectExpr })).toBeInstanceOf(SelectExpr);
    expect(parseOne('left join foo', { into: JoinExpr })).toBeInstanceOf(JoinExpr);
    expect(parseOne('int', { into: DataTypeExpr })).toBeInstanceOf(DataTypeExpr);
    expect(parseOne('array<int>', { into: DataTypeExpr })).toBeInstanceOf(DataTypeExpr);
    expect(parseOne('foo', { into: TableExpr })).toBeInstanceOf(TableExpr);
    expect(
      parseOne(
        'WHEN MATCHED THEN UPDATE SET target.salary = COALESCE(source.salary, target.salary)',
        { into: WhensExpr },
      ),
    ).toBeInstanceOf(WhensExpr);

    expect(() => parseOne('SELECT * FROM tbl', { into: TableExpr })).toThrow(ParseError);

    expect(parseOne('foo INT NOT NULL', { into: ColumnDefExpr })).toBeInstanceOf(ColumnDefExpr);
  }

  testParseIntoError () {
    try {
      parseOne('SELECT 1;', {
        read: 'sqlite',
        into: [FromExpr],
      });
      expect.unreachable();
    } catch (err) {
      expect(err).toBeInstanceOf(ParseError);
      expect(String((err as ParseError).message)).toContain(
        'Failed to parse \'SELECT 1;\' into [FromExpr]',
      );
    }
  }

  testParseIntoErrors () {
    try {
      parseOne('SELECT 1;', {
        read: 'sqlite',
        into: [FromExpr, JoinExpr],
      });
      expect.unreachable();
    } catch (err) {
      expect(err).toBeInstanceOf(ParseError);
      expect(String((err as ParseError).message)).toContain(
        'Failed to parse \'SELECT 1;\' into [FromExpr, JoinExpr]',
      );
    }
  }

  testColumn () {
    const columns = [...parseOne('select a, ARRAY[1] b, case when 1 then 1 end').findAll(ColumnExpr)];
    expect(columns.length).toBe(1);
    expect(parseOne('date').find(ColumnExpr)).not.toBeNull();
  }

  testTuple () {
    expect(parseOne('(a,)')).toBeInstanceOf(TupleExpr);
  }

  testStructs () {
    const cast1 = parseOne('cast(x as struct<int>)');
    const cast1To = cast1.getArgKey('to') as DataTypeExpr;
    const cast1Exprs = cast1To.getArgKey('expressions') as Expression[];
    expect(cast1Exprs[0]).toBeInstanceOf(DataTypeExpr);
    expect(cast1.sql()).toBe('CAST(x AS STRUCT<INT>)');

    const cast2 = parseOne('cast(x as struct<varchar(10)>)');
    const cast2To = cast2.getArgKey('to') as DataTypeExpr;
    const cast2Exprs = cast2To.getArgKey('expressions') as Expression[];
    expect(cast2Exprs[0]).toBeInstanceOf(DataTypeExpr);
    expect(cast2.sql()).toBe('CAST(x AS STRUCT<VARCHAR(10)>)');
  }

  testFloat () {
    expect(parseOne('.2').sql()).toBe(parseOne('0.2').sql());
  }

  testUnnest () {
    const unnestSql = 'UNNEST(foo)';
    const expr = parseOne(unnestSql);
    expect(expr).toBeInstanceOf(UnnestExpr);
    const unnest = expr as UnnestExpr;
    expect(Array.isArray(unnest.getArgKey('expressions'))).toBe(true);
    expect(expr.sql()).toBe(unnestSql);
  }

  testUnnestProjection () {
    const expr = parseOne('SELECT foo IN UNNEST(bla) AS bar') as SelectExpr;
    const selects = expr.args.expressions ?? [];
    expect(selects[0]).toBeInstanceOf(AliasExpr);
    expect((selects[0] as AliasExpr).outputName).toBe('bar');
    expect(parseOne('select unnest(x)').find(UnnestExpr)).not.toBeNull();
  }

  testUnaryPlus () {
    expect(parseOne('+15').sql()).toBe(LiteralExpr.number(15).sql());
  }

  testTable () {
    const tables = [...parseOne('select * from a, b.c, .d').findAll(TableExpr)].map((t) => t.sql());
    expect(new Set(tables)).toEqual(new Set([
      'a',
      'b.c',
      'd',
    ]));
  }

  testUnion () {
    expect(parseOne('SELECT * FROM (SELECT 1) UNION SELECT 2')).toBeInstanceOf(UnionExpr);
    expect(
      parseOne('SELECT x FROM y HAVING x > (SELECT 1) UNION SELECT 2'),
    ).toBeInstanceOf(UnionExpr);

    const singleUnion = 'SELECT x FROM t1 UNION ALL SELECT x FROM t2 LIMIT 1';
    const expr = parseOne(singleUnion) as UnionExpr;
    expect(expr).toBeInstanceOf(UnionExpr);
    const limit = expr.getArgKey('limit');
    expect(limit).toBeInstanceOf(LimitExpr);
    expect(expr.sql()).toBe(singleUnion);

    const twoUnions =
      'SELECT x FROM t1 UNION ALL SELECT x FROM t2 UNION ALL SELECT x FROM t3 LIMIT 1';
    const expr2 = parseOne(twoUnions) as UnionExpr;
    expect(expr2).toBeInstanceOf(UnionExpr);
    expect(expr2.getArgKey('limit')).toBeInstanceOf(LimitExpr);
    expect(expr2.sql()).toBe(twoUnions);

    const expr3 = parseOne(singleUnion, { read: 'clickhouse' }) as UnionExpr;
    expect(expr3.getArgKey('limit')).toBeUndefined();
    expect(expr3.sql({ dialect: 'clickhouse' })).toBe(singleUnion);
  }

  testSelect () {
    expect(parseOne('select 1 natural')).not.toBeNull();
    expect(
      (parseOne('select * from (select 1) x order by x.y') as SelectExpr).getArgKey('order'),
    ).not.toBeNull();
    expect(
      (parseOne('select * from x where a = (select 1) order by x.y') as SelectExpr).getArgKey(
        'order',
      ),
    ).not.toBeNull();
    const joins = (parseOne('select * from (select 1) x cross join y') as SelectExpr).getArgKey(
      'joins',
    ) as Expression[];
    expect(joins.length).toBe(1);
    expect(
      parseOne('SELECT * FROM x CROSS JOIN y, z LATERAL VIEW EXPLODE(y)').sql(),
    ).toBe('SELECT * FROM x CROSS JOIN y, z LATERAL VIEW EXPLODE(y)');
    expect(
      parseOne('create table a as (select b from c) index').find(AliasExpr),
    ).toBeUndefined();
  }

  testLambdaStruct () {
    const expression = parseOne('FILTER(a.b, x -> x.id = id)');
    const lambdaExpr = expression.getArgKey('expression') as Expression;
    expect(
      (lambdaExpr.getArgKey('this') as Expression).getArgKey('this'),
    ).toBeInstanceOf(DotExpr);
    expect(lambdaExpr.sql()).toBe('x -> x.id = id');
    expect(parseOne('FILTER([], x -> x)').find(ColumnExpr)).toBeUndefined();
  }

  testTransactions () {
    const expr1 = parseOne('BEGIN TRANSACTION') as TransactionExpr;
    expect(expr1.getArgKey('this')).toBeUndefined();
    expect(expr1.getArgKey('modes')).toEqual([]);
    expect(expr1.sql()).toBe('BEGIN');

    const expr2 = parseOne('START TRANSACTION', { read: 'mysql' }) as TransactionExpr;
    expect(expr2.getArgKey('this')).toBeUndefined();
    expect(expr2.getArgKey('modes')).toEqual([]);
    expect(expr2.sql()).toBe('BEGIN');

    const expr3 = parseOne('BEGIN DEFERRED TRANSACTION') as TransactionExpr;
    expect(expr3.getArgKey('this')).toBe('DEFERRED');
    expect(expr3.getArgKey('modes')).toEqual([]);
    expect(expr3.sql()).toBe('BEGIN');

    const expr4 = parseOne('START TRANSACTION READ WRITE, ISOLATION LEVEL SERIALIZABLE', {
      read: 'presto',
    }) as TransactionExpr;
    expect(expr4.getArgKey('this')).toBeUndefined();
    const modes4 = expr4.getArgKey('modes') as string[];
    expect(modes4[0]).toBe('READ WRITE');
    expect(modes4[1]).toBe('ISOLATION LEVEL SERIALIZABLE');
    expect(expr4.sql()).toBe('BEGIN READ WRITE, ISOLATION LEVEL SERIALIZABLE');

    const expr5 = parseOne('BEGIN', { read: 'bigquery' });
    expect(expr5).not.toBeInstanceOf(TransactionExpr);
    expect(expr5.getArgKey('expression')).toBeUndefined();
    expect(expr5.sql()).toBe('BEGIN');
  }

  testIdentify () {
    const expression = parseOne(`
      SELECT a, "b", c AS c, d AS "D", e AS "y|z'"
      FROM y."z"
    `) as SelectExpr;

    const exprs = expression.args.expressions ?? [];
    expect(exprs[0].name).toBe('a');
    expect(exprs[1].name).toBe('b');
    expect((exprs[2] as AliasExpr).alias).toBe('c');
    expect((exprs[3] as AliasExpr).alias).toBe('D');
    expect((exprs[4] as AliasExpr).alias).toBe('y|z\'');
    const fromExpr = expression.args.from as FromExpr;
    const tableExpr = fromExpr.getArgKey('this') as TableExpr;
    expect(tableExpr.name).toBe('z');
    expect((tableExpr.getArgKey('db') as IdentifierExpr).name).toBe('y');
  }

  testMulti () {
    const expressions = parse(`
      SELECT * FROM a; SELECT * FROM b;
    `);

    expect(expressions.length).toBe(2);
    const from0 = (expressions[0] as SelectExpr).args.from as FromExpr;
    expect(from0.name).toBe('a');
    const from1 = (expressions[1] as SelectExpr).args.from as FromExpr;
    expect(from1.name).toBe('b');

    const expressions2 = parse('SELECT 1; ; SELECT 2');
    expect(expressions2.length).toBe(3);
    expect(expressions2[1]).toBeUndefined();
  }

  testExpression () {
    const ignore = new TestableParser({ errorLevel: ErrorLevel.IGNORE });
    expect(ignore.expression(HintExpr, { expressions: [] })).toBeInstanceOf(HintExpr);
    expect(ignore.expression(HintExpr, { y: '' })).toBeInstanceOf(HintExpr);
    expect(ignore.expression(HintExpr)).toBeInstanceOf(HintExpr);

    // RAISE level collects errors without throwing immediately (equivalent to Python's RAISE)
    const immediate = new TestableParser({ errorLevel: ErrorLevel.RAISE });
    expect(immediate.expression(HintExpr, { expressions: [] })).toBeInstanceOf(HintExpr);

    const warn = new TestableParser({ errorLevel: ErrorLevel.WARN });
    warn.expression(HintExpr);
    expect(warn.getErrors().length).toBe(1);
  }

  testParseErrors () {
    expect(() => parseOne('IF(a > 0, a, b, c)')).toThrow(ParseError);
    expect(() => parseOne('IF(a > 0)')).toThrow(ParseError);
    expect(() => parseOne('SELECT CASE FROM x')).toThrow(ParseError);
    expect(() => parseOne('WITH cte AS (SELECT * FROM x)')).toThrow(ParseError);
    expect(() => parseOne('SELECT foo( FROM bar')).toThrow(ParseError);

    expect(
      parseOne(
        'CREATE TABLE t (i UInt8) ENGINE = AggregatingMergeTree() ORDER BY tuple()',
        {
          read: 'clickhouse',
          errorLevel: ErrorLevel.RAISE,
        },
      ).sql({ dialect: 'clickhouse' }),
    ).toBe('CREATE TABLE t (i UInt8) ENGINE=AggregatingMergeTree() ORDER BY tuple()');

    expect(() => parseOne('SELECT A[:')).toThrow(ParseError);

    expect(parseOne('as as', { errorLevel: ErrorLevel.IGNORE }).sql()).toBe('AS as');
  }

  testSpace () {
    expect(
      parseOne('SELECT ROW() OVER(PARTITION  BY x) FROM x GROUP  BY y').sql(),
    ).toBe('SELECT ROW() OVER (PARTITION BY x) FROM x GROUP BY y');

    expect(
      parseOne(`SELECT   * FROM x GROUP
            BY y`).sql(),
    ).toBe('SELECT * FROM x GROUP BY y');
  }

  testMissingBy () {
    expect(() => parseOne('SELECT FROM x ORDER BY')).toThrow(ParseError);
  }

  testParameter () {
    expect(parseOne('SELECT @x, @@x, @1').sql()).toBe('SELECT @x, @@x, @1');
  }

  testVar () {
    expect(parseOne('INTERVAL \'1\' DAY').getArgKey('unit')).toBeInstanceOf(VarExpr);
    expect(parseOne('SELECT @JOIN, @\'foo\'').sql()).toBe('SELECT @JOIN, @\'foo\'');
  }

  testCommentsSelect () {
    const expression = parseOne(`
      --comment1.1
      --comment1.2
      SELECT /*comment1.3*/
          a, --comment2
          b as B, --comment3:testing
          "test--annotation",
          c, --comment4 --foo
          e, --
          f -- space
      FROM foo
    `) as SelectExpr;

    expect(expression.comments).toEqual([
      'comment1.1',
      'comment1.2',
      'comment1.3',
    ]);
    const exprs = expression.args.expressions ?? [];
    expect(exprs[0].comments).toEqual(['comment2']);
    expect(exprs[1].comments).toEqual(['comment3:testing']);
    expect(exprs[2].comments).toBeUndefined();
    expect(exprs[3].comments).toEqual(['comment4 --foo']);
    expect(exprs[4].comments).toEqual(['']);
    expect(exprs[5].comments).toEqual([' space']);

    const expression2 = parseOne(`
      SELECT a.column_name --# Comment 1
             ,b.column_name2, --# Comment 2
             b.column_name3 AS NAME3 --# Comment 3
      FROM table_name a
      JOIN table_name2 b ON a.column_name = b.column_name
    `) as SelectExpr;

    const exprs2 = expression2.args.expressions ?? [];
    expect(exprs2[0].comments).toEqual(['# Comment 1']);
    expect(exprs2[1].comments).toEqual(['# Comment 2']);
    expect(exprs2[2].comments).toEqual(['# Comment 3']);
  }

  testCommentsSelectCte () {
    const expression = parseOne(`
      /*comment1.1*/
      /*comment1.2*/
      WITH a AS (SELECT 1)
      SELECT /*comment2*/
          a.*
      FROM /*comment3*/
          a
    `) as SelectExpr;

    expect(expression.comments).toEqual(['comment2']);
    expect((expression.args.from as FromExpr).comments).toEqual(['comment3']);
    expect((expression.args.with as Expression).comments).toEqual(['comment1.1', 'comment1.2']);
  }

  testCommentsInsert () {
    const expression = parseOne(`
      --comment1.1
      --comment1.2
      INSERT INTO /*comment1.3*/
          x       /*comment2*/
      VALUES      /*comment3*/
          (1, 'a', 2.0)
    `);

    expect(expression.comments).toEqual([
      'comment1.1',
      'comment1.2',
      'comment1.3',
    ]);
    expect((expression.getArgKey('this') as Expression).comments).toEqual(['comment2']);
  }

  testCommentsInsertCte () {
    const expression = parseOne(`
      /*comment1.1*/
      /*comment1.2*/
      WITH a AS (SELECT 1)
      INSERT INTO /*comment2*/
          b /*comment3*/
      SELECT * FROM a
    `);

    expect(expression.comments).toEqual(['comment2']);
    expect((expression.getArgKey('this') as Expression).comments).toEqual(['comment3']);
    expect((expression.getArgKey('with') as Expression).comments).toEqual(['comment1.1', 'comment1.2']);
  }

  testCommentsUpdate () {
    const expression = parseOne(`
      --comment1.1
      --comment1.2
      UPDATE  /*comment1.3*/
          tbl /*comment2*/
      SET     /*comment3*/
          x = 2
      WHERE /*comment4*/
          x <> 2
    `);

    expect(expression.comments).toEqual([
      'comment1.1',
      'comment1.2',
      'comment1.3',
    ]);
    expect((expression.getArgKey('this') as Expression).comments).toEqual(['comment2']);
    expect((expression.getArgKey('where') as Expression).comments).toEqual(['comment4']);
  }

  testCommentsUpdateCte () {
    const expression = parseOne(`
      /*comment1.1*/
      /*comment1.2*/
      WITH a AS (SELECT * FROM b)
      UPDATE /*comment2*/
          a  /*comment3*/
      SET col = 1
    `);

    expect(expression.comments).toEqual(['comment2']);
    expect((expression.getArgKey('this') as Expression).comments).toEqual(['comment3']);
    expect((expression.getArgKey('with') as Expression).comments).toEqual(['comment1.1', 'comment1.2']);
  }

  testCommentsDelete () {
    const expression = parseOne(`
      --comment1.1
      --comment1.2
      DELETE /*comment1.3*/
      FROM   /*comment2*/
          x  /*comment3*/
      WHERE  /*comment4*/
          y > 1
    `);

    expect(expression.comments).toEqual([
      'comment1.1',
      'comment1.2',
      'comment1.3',
    ]);
    expect((expression.getArgKey('this') as Expression).comments).toEqual(['comment3']);
    expect((expression.getArgKey('where') as Expression).comments).toEqual(['comment4']);
  }

  testCommentsDeleteCte () {
    const expression = parseOne(`
      /*comment1.1*/
      /*comment1.2*/
      WITH a AS (SELECT * FROM b)
      --comment2
      DELETE FROM a /*comment3*/
    `);

    expect(expression.comments).toEqual(['comment2']);
    expect((expression.getArgKey('this') as Expression).comments).toEqual(['comment3']);
    expect((expression.getArgKey('with') as Expression).comments).toEqual(['comment1.1', 'comment1.2']);
  }

  testTypeLiterals () {
    expect(parseOne('int 1').sql()).toBe(parseOne('CAST(1 AS INT)').sql());
    expect(parseOne('int.5').sql()).toBe(parseOne('CAST(0.5 AS INT)').sql());
    expect(parseOne('TIMESTAMP \'2022-01-01\'').sql()).toBe('CAST(\'2022-01-01\' AS TIMESTAMP)');
    expect(parseOne('TIMESTAMP(1) \'2022-01-01\'').sql()).toBe('CAST(\'2022-01-01\' AS TIMESTAMP(1))');
    expect(parseOne('TIMESTAMP WITH TIME ZONE \'2022-01-01\'').sql()).toBe(
      'CAST(\'2022-01-01\' AS TIMESTAMPTZ)',
    );
    expect(parseOne('TIMESTAMP WITH LOCAL TIME ZONE \'2022-01-01\'').sql()).toBe(
      'CAST(\'2022-01-01\' AS TIMESTAMPLTZ)',
    );
    expect(parseOne('TIMESTAMP WITHOUT TIME ZONE \'2022-01-01\'').sql()).toBe(
      'CAST(\'2022-01-01\' AS TIMESTAMP)',
    );
    expect(parseOne('TIMESTAMP(1) WITH TIME ZONE \'2022-01-01\'').sql()).toBe(
      'CAST(\'2022-01-01\' AS TIMESTAMPTZ(1))',
    );
    expect(parseOne('TIMESTAMP(1) WITH LOCAL TIME ZONE \'2022-01-01\'').sql()).toBe(
      'CAST(\'2022-01-01\' AS TIMESTAMPLTZ(1))',
    );
    expect(parseOne('TIMESTAMP(1) WITHOUT TIME ZONE \'2022-01-01\'').sql()).toBe(
      'CAST(\'2022-01-01\' AS TIMESTAMP(1))',
    );
    expect(parseOne('TIMESTAMP(1) WITH TIME ZONE').sql()).toBe('TIMESTAMPTZ(1)');
    expect(parseOne('TIMESTAMP(1) WITH LOCAL TIME ZONE').sql()).toBe('TIMESTAMPLTZ(1)');
    expect(parseOne('TIMESTAMP(1) WITHOUT TIME ZONE').sql()).toBe('TIMESTAMP(1)');
    expect(parseOne('JSON \'{"x":"y"}\'').sql()).toBe('PARSE_JSON(\'{"x":"y"}\')');
    expect(parseOne('TIMESTAMP(1)')).toBeInstanceOf(FuncExpr);
    expect(parseOne('TIMESTAMP(\'2022-01-01\')')).toBeInstanceOf(FuncExpr);
    expect(parseOne('TIMESTAMP()')).toBeInstanceOf(FuncExpr);
    expect(parseOne('map.x')).toBeInstanceOf(ColumnExpr);
    const castCharTo = parseOne('CAST(x AS CHAR(5))').getArgKey('to') as DataTypeExpr;
    const castCharExprs = castCharTo.getArgKey('expressions') as Expression[];
    expect(castCharExprs[0]).toBeInstanceOf(DataTypeParamExpr);
    expect(parseOne('1::int64', { dialect: 'bigquery' }).sql()).toBe(
      parseOne('CAST(1 AS BIGINT)').sql(),
    );
  }

  testSetExpression () {
    const set_ = parseOne('SET');
    expect(set_.sql()).toBe('SET');
    expect(set_).toBeInstanceOf(SetExpr);

    const setSession = parseOne('SET SESSION x = 1') as SetExpr;
    expect(setSession.sql()).toBe('SET SESSION x = 1');
    expect(setSession).toBeInstanceOf(SetExpr);

    const setExprs = setSession.args.expressions ?? [];
    const setItem = setExprs[0] as SetItemExpr;
    expect(setItem).toBeInstanceOf(SetItemExpr);
    expect(setItem.getArgKey('this')).toBeInstanceOf(EqExpr);
    expect((setItem.getArgKey('this') as EqExpr).getArgKey('this')).toBeInstanceOf(ColumnExpr);
    expect((setItem.getArgKey('this') as EqExpr).getArgKey('expression')).toBeInstanceOf(
      LiteralExpr,
    );
    expect(setItem.getArgKey('kind')).toBe('SESSION');

    const setTo = parseOne('SET x TO 1');
    expect(setTo.sql()).toBe('SET x = 1');
    expect(setTo).toBeInstanceOf(SetExpr);

    const setAsCommand = parseOne('SET DEFAULT ROLE ALL TO USER', { errorLevel: ErrorLevel.IGNORE });
    expect(setAsCommand.sql()).toBe('SET DEFAULT ROLE ALL TO USER');
    expect(setAsCommand).toBeInstanceOf(CommandExpr);
    expect((setAsCommand as CommandExpr).getArgKey('this')).toBe('SET');
    expect((setAsCommand as CommandExpr).getArgKey('expression')).toBe(' DEFAULT ROLE ALL TO USER');
  }

  testPrettyConfigOverride () {
    expect(parseOne('SELECT col FROM x').sql()).toBe('SELECT col FROM x');
    expect(parseOne('SELECT col FROM x').sql({ pretty: true })).toBe('SELECT\n  col\nFROM x');
  }

  testCommentErrorN () {
    const warnSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    parseOne(
      `SUM
(
-- test
)`,
      { errorLevel: ErrorLevel.WARN },
    );
    warnSpy.mockRestore();
  }

  testCommentErrorR () {
    const warnSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    parseOne('SUM(-- test\r)', { errorLevel: ErrorLevel.WARN });
    warnSpy.mockRestore();
  }

  testCreateTableError () {
    const warnSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    parseOne('CREATE TABLE SELECT', { errorLevel: ErrorLevel.WARN });
    warnSpy.mockRestore();
  }

  testPivotColumns () {
    const nothingAliased = `
      SELECT * FROM (
          SELECT partname, price FROM part
      ) PIVOT (AVG(price) FOR partname IN ('prop', 'rudder'))
    `;

    const everythingAliased = `
      SELECT * FROM (
          SELECT partname, price FROM part
      ) PIVOT (AVG(price) AS avg_price FOR partname IN ('prop' AS prop1, 'rudder' AS rudder1))
    `;

    const onlyPivotColumnsAliased = `
      SELECT * FROM (
          SELECT partname, price FROM part
      ) PIVOT (AVG(price) FOR partname IN ('prop' AS prop1, 'rudder' AS rudder1))
    `;

    const columnsPartiallyAliased = `
      SELECT * FROM (
          SELECT partname, price FROM part
      ) PIVOT (AVG(price) FOR partname IN ('prop' AS prop1, 'rudder'))
    `;

    const multipleAggregatesAliased = `
      SELECT * FROM (
          SELECT partname, price, quality FROM part
      ) PIVOT (AVG(price) AS p, MAX(quality) AS q FOR partname IN ('prop' AS prop1, 'rudder'))
    `;

    const multipleAggregatesNotAliased = `
      SELECT * FROM (
          SELECT partname, price, quality FROM part
      ) PIVOT (AVG(price), MAX(quality) FOR partname IN ('prop' AS prop1, 'rudder'))
    `;

    const multipleAggregatesNotAliasedWithQuotedIdentifierSpark = `
      SELECT * FROM (
          SELECT partname, price, quality FROM part
      ) PIVOT (AVG(\`PrIcE\`), MAX(quality) FOR partname IN ('prop' AS prop1, 'rudder'))
    `;

    const multipleAggregatesNotAliasedWithQuotedIdentifierDuckdb = `
      SELECT * FROM (
          SELECT partname, price, quality FROM part
      ) PIVOT (AVG("PrIcE"), MAX(quality) FOR partname IN ('prop' AS prop1, 'rudder'))
    `;

    const twoInClausesDuckdb = `
      SELECT * FROM cities PIVOT (
          sum(population) AS total,
          count(population) AS count
          FOR
              year IN (2000, 2010)
              country IN ('NL', 'US')
      )
    `;

    const threeInClausesDuckdb = `
      SELECT * FROM cities PIVOT (
          sum(population) AS total,
          count(population) AS count
          FOR
              year IN (2000, 2010)
              country IN ('NL', 'US')
              name IN ('Amsterdam', 'Seattle')
      )
    `;

    const queryToColumnNames: Array<[string, Record<string, string[]>]> = [
      [
        nothingAliased,
        {
          bigquery: ['prop', 'rudder'],
          duckdb: ['prop', 'rudder'],
          redshift: ['prop', 'rudder'],
          snowflake: ['"\'prop\'"', '"\'rudder\'"'],
          spark: ['prop', 'rudder'],
        },
      ],
      [
        everythingAliased,
        {
          bigquery: ['avg_price_prop1', 'avg_price_rudder1'],
          duckdb: ['prop1_avg_price', 'rudder1_avg_price'],
          redshift: ['prop1_avg_price', 'rudder1_avg_price'],
          spark: ['prop1', 'rudder1'],
        },
      ],
      [
        onlyPivotColumnsAliased,
        {
          bigquery: ['prop1', 'rudder1'],
          duckdb: ['prop1', 'rudder1'],
          redshift: ['prop1', 'rudder1'],
          spark: ['prop1', 'rudder1'],
        },
      ],
      [
        columnsPartiallyAliased,
        {
          bigquery: ['prop1', 'rudder'],
          duckdb: ['prop1', 'rudder'],
          redshift: ['prop1', 'rudder'],
          spark: ['prop1', 'rudder'],
        },
      ],
      [
        multipleAggregatesAliased,
        {
          bigquery: [
            'p_prop1',
            'q_prop1',
            'p_rudder',
            'q_rudder',
          ],
          duckdb: [
            'prop1_p',
            'prop1_q',
            'rudder_p',
            'rudder_q',
          ],
          spark: [
            'prop1_p',
            'prop1_q',
            'rudder_p',
            'rudder_q',
          ],
        },
      ],
      [
        multipleAggregatesNotAliased,
        {
          duckdb: [
            '"prop1_avg(price)"',
            '"prop1_max(quality)"',
            '"rudder_avg(price)"',
            '"rudder_max(quality)"',
          ],
          spark: [
            '`prop1_avg(price)`',
            '`prop1_max(quality)`',
            '`rudder_avg(price)`',
            '`rudder_max(quality)`',
          ],
        },
      ],
      [
        multipleAggregatesNotAliasedWithQuotedIdentifierSpark,
        {
          spark: [
            '`prop1_avg(PrIcE)`',
            '`prop1_max(quality)`',
            '`rudder_avg(PrIcE)`',
            '`rudder_max(quality)`',
          ],
        },
      ],
      [
        multipleAggregatesNotAliasedWithQuotedIdentifierDuckdb,
        {
          duckdb: [
            '"prop1_avg(PrIcE)"',
            '"prop1_max(quality)"',
            '"rudder_avg(PrIcE)"',
            '"rudder_max(quality)"',
          ],
        },
      ],
      [
        twoInClausesDuckdb,
        {
          duckdb: [
            '"2000_NL_total"',
            '"2000_NL_count"',
            '"2000_US_total"',
            '"2000_US_count"',
            '"2010_NL_total"',
            '"2010_NL_count"',
            '"2010_US_total"',
            '"2010_US_count"',
          ],
        },
      ],
      [
        threeInClausesDuckdb,
        {
          duckdb: [
            '"2000_NL_Amsterdam_total"',
            '"2000_NL_Amsterdam_count"',
            '"2000_NL_Seattle_total"',
            '"2000_NL_Seattle_count"',
            '"2000_US_Amsterdam_total"',
            '"2000_US_Amsterdam_count"',
            '"2000_US_Seattle_total"',
            '"2000_US_Seattle_count"',
            '"2010_NL_Amsterdam_total"',
            '"2010_NL_Amsterdam_count"',
            '"2010_NL_Seattle_total"',
            '"2010_NL_Seattle_count"',
            '"2010_US_Amsterdam_total"',
            '"2010_US_Amsterdam_count"',
            '"2010_US_Seattle_total"',
            '"2010_US_Seattle_count"',
          ],
        },
      ],
    ];

    for (const [query, dialectColumns] of queryToColumnNames) {
      for (const [dialect, expectedColumns] of Object.entries(dialectColumns)) {
        const expr = parseOne(query, { read: dialect }) as SelectExpr;
        const fromExpr = expr.args.from as FromExpr;
        const tableExpr = fromExpr.getArgKey('this') as Expression;
        const pivots = tableExpr.getArgKey('pivots') as Expression[];
        const columns = pivots[0].getArgKey('columns') as Expression[];
        expect(columns.map((col) => col.sql({ dialect }))).toEqual(expectedColumns);
      }
    }
  }

  testParseNested () {
    const warnOverThreshold = (query: string, maxThreshold = 0.5) => {
      const now = Date.now();
      const ast = parseOne(query);
      const elapsed = (Date.now() - now) / 1000;
      expect(ast).not.toBeNull();
      if (maxThreshold <= elapsed) {
        console.warn(
          `Query ${query.slice(0, 100)}... surpassed the time threshold of ${maxThreshold} seconds`,
        );
      }
    };

    warnOverThreshold('SELECT * FROM a ' + 'LEFT JOIN b ON a.id = b.id '.repeat(38));
    warnOverThreshold('SELECT * FROM a ' + 'LEFT JOIN UNNEST(ARRAY[]) '.repeat(15));
    warnOverThreshold('SELECT * FROM a ' + 'OUTER APPLY (SELECT * FROM b) '.repeat(30));
    warnOverThreshold('SELECT * FROM a ' + 'NATURAL FULL OUTER JOIN x '.repeat(30));
  }

  testParseProperties () {
    expect(parseOne('create materialized table x').sql()).toBe('CREATE MATERIALIZED TABLE x');
  }

  testParseFloats () {
    expect(parseOne('1. ').isNumber).toBe(true);
  }

  testParseTerseCoalesce () {
    expect(parseOne('SELECT x ?? y FROM z').find(CoalesceExpr)).not.toBeNull();
    expect(parseOne('SELECT a, b ?? \'No Data\' FROM z').sql()).toBe(
      'SELECT a, COALESCE(b, \'No Data\') FROM z',
    );
    expect(parseOne('SELECT a, b ?? c ?? \'No Data\' FROM z').sql()).toBe(
      'SELECT a, COALESCE(COALESCE(b, c), \'No Data\') FROM z',
    );
  }

  testParseIntervals () {
    const ast = parseOne(
      'SELECT a FROM tbl WHERE a <= DATE \'1998-12-01\' - INTERVAL \'71 days\' GROUP BY b',
    );
    const interval = ast.find(IntervalExpr) as IntervalExpr;
    expect(interval).not.toBeNull();
    expect((interval.getArgKey('this') as Expression).sql()).toBe('\'71\'');
    expect(interval.getArgKey('unit')).toBeInstanceOf(VarExpr);
    expect((interval.getArgKey('unit') as VarExpr).sql()).toBe('DAYS');
  }

  testParseConcatWs () {
    const ast = parseOne('CONCAT_WS(\' \', \'John\', \'Doe\')');
    expect(ast.sql()).toBe('CONCAT_WS(\' \', \'John\', \'Doe\')');
    const exprs = ast.getArgKey('expressions') as Expression[];
    expect(exprs[0].sql()).toBe('\' \'');
    expect(exprs[1].sql()).toBe('\'John\'');
    expect(exprs[2].sql()).toBe('\'Doe\'');

    const results = parse('CONCAT_WS()', { errorLevel: ErrorLevel.IGNORE });
    expect(results[0]?.sql()).toBe('CONCAT_WS()');
  }

  testParseDropSchema () {
    for (const dialect of [
      undefined,
      'bigquery',
      'snowflake',
    ] as (string | undefined)[]) {
      const ast = parseOne('DROP SCHEMA catalog.schema', { dialect });
      expect(ast).toBeInstanceOf(DropExpr);
      expect(ast.sql({ dialect })).toBe('DROP SCHEMA catalog.schema');
    }
  }

  testParseCreateSchema () {
    for (const dialect of [
      undefined,
      'bigquery',
      'snowflake',
    ] as (string | undefined)[]) {
      const ast = parseOne('CREATE SCHEMA catalog.schema', { dialect });
      expect(ast).toBeInstanceOf(CreateExpr);
      expect(ast.sql({ dialect })).toBe('CREATE SCHEMA catalog.schema');
    }
  }

  testValuesAsIdentifier () {
    const sql = 'SELECT values FROM t WHERE values + 1 > x';
    for (const dialect of [
      'bigquery',
      'clickhouse',
      'duckdb',
      'postgres',
      'redshift',
      'snowflake',
    ]) {
      expect(parseOne(sql, { dialect }).sql({ dialect })).toBe(sql);
    }
  }

  testAlterSet () {
    const sqls = [
      'ALTER TABLE tbl SET TBLPROPERTIES (\'x\'=\'1\', \'Z\'=\'2\')',
      'ALTER TABLE tbl SET SERDE \'test\' WITH SERDEPROPERTIES (\'k\'=\'v\', \'kay\'=\'vee\')',
      'ALTER TABLE tbl SET SERDEPROPERTIES (\'k\'=\'v\', \'kay\'=\'vee\')',
      'ALTER TABLE tbl SET LOCATION \'new_location\'',
      'ALTER TABLE tbl SET FILEFORMAT file_format',
      'ALTER TABLE tbl SET TAGS (\'tag1\' = \'t1\', \'tag2\' = \'t2\')',
    ];

    for (const dialect of [
      'hive',
      'spark2',
      'spark',
      'databricks',
    ]) {
      for (const sql of sqls) {
        expect(parseOne(sql, { dialect }).sql({ dialect })).toBe(sql);
      }
    }
  }

  testDistinctFrom () {
    expect(parseOne('a IS DISTINCT FROM b OR c IS DISTINCT FROM d')).toBeInstanceOf(OrExpr);
  }

  testTrailingComments () {
    const expressions = parse(`
      select * from x;
      -- my comment
    `);

    expect(
      expressions.map((e) => e?.sql() ?? '').join(';\n'),
    ).toBe('SELECT * FROM x;\n/* my comment */');
  }

  testParsePropEq () {
    const exprs = parseOne('x(a := b and c)').getArgKey('expressions') as Expression[];
    expect(exprs[0]).toBeInstanceOf(PropertyEqExpr);
  }

  testCollate () {
    const collatePairs: [string, new (...args: never[]) => Expression][] = [
      ['pg_catalog."default"', ColumnExpr],
      ['"en_DE"', IdentifierExpr],
      ['LATIN1_GENERAL_BIN', VarExpr],
      ['\'en\'', LiteralExpr],
    ];

    for (const [collatePart, expectedClass] of collatePairs) {
      const collateNode = parseOne(
        `SELECT * FROM t WHERE foo LIKE '%bar%' COLLATE ${collatePart}`,
      ).find(CollateExpr) as CollateExpr;
      expect(collateNode).toBeInstanceOf(CollateExpr);
      expect(collateNode.getArgKey('expression')).toBeInstanceOf(expectedClass);
    }
  }

  testDropColumn () {
    const ast = parseOne('ALTER TABLE tbl DROP COLUMN col');
    expect([...ast.findAll(TableExpr)].length).toBe(1);
    expect([...ast.findAll(ColumnExpr)].length).toBe(1);
  }

  testUdfMeta () {
    const ast1 = parseOne('YEAR(a) /* sqlglot.anonymous */');
    expect(ast1).toBeInstanceOf(AnonymousExpr);

    const ast2 = parseOne('YEAR(a) /* sqlglot.anONymous */');
    expect(ast2).toBeInstanceOf(YearExpr);

    const ast3 = parseOne('YEAR(a) /* sqlglot.anon */');
    expect(ast3).toBeInstanceOf(YearExpr);
  }

  testTokenPositionMeta () {
    const ast = parseOne(
      'SELECT a, b FROM test_schema.test_table_a UNION ALL SELECT c, d FROM test_catalog.test_schema.test_table_b',
    ) as UnionExpr;

    for (const identifier of ast.findAll(IdentifierExpr)) {
      expect(new Set(Object.keys(identifier.meta))).toEqual(
        new Set([
          'line',
          'col',
          'start',
          'end',
        ]),
      );
    }

    const leftSelect = ast.args.this as SelectExpr;
    const leftFrom = leftSelect.args.from as FromExpr;
    const leftTable = leftFrom.getArgKey('this') as TableExpr;
    expect(narrowInstanceOf(leftTable.args.this, Expression)?.meta).toEqual({
      line: 1,
      col: 41,
      start: 29,
      end: 40,
    });
    expect((leftTable.getArgKey('db') as IdentifierExpr).meta).toEqual({
      line: 1,
      col: 28,
      start: 17,
      end: 27,
    });

    const rightSelect = ast.args.expression as SelectExpr;
    const rightFrom = rightSelect.args.from as FromExpr;
    const rightTable = rightFrom.getArgKey('this') as TableExpr;
    expect(narrowInstanceOf(rightTable.args.this, Expression)?.meta).toEqual({
      line: 1,
      col: 106,
      start: 94,
      end: 105,
    });
    expect((rightTable.getArgKey('db') as IdentifierExpr).meta).toEqual({
      line: 1,
      col: 93,
      start: 82,
      end: 92,
    });
    expect((rightTable.getArgKey('catalog') as IdentifierExpr).meta).toEqual({
      line: 1,
      col: 81,
      start: 69,
      end: 80,
    });

    const ast2 = parseOne('SELECT FOO()');
    expect(ast2.find(AnonymousExpr)?.meta).toEqual({
      line: 1,
      col: 10,
      start: 7,
      end: 9,
    });

    const ast3 = parseOne('SELECT * FROM t');
    expect(ast3.find(StarExpr)?.meta).toEqual({
      line: 1,
      col: 8,
      start: 7,
      end: 7,
    });

    const ast4 = parseOne('SELECT t.* FROM t');
    expect(ast4.find(StarExpr)?.meta).toEqual({
      line: 1,
      col: 10,
      start: 9,
      end: 9,
    });

    const ast5 = parseOne('SELECT 1');
    expect(ast5.find(LiteralExpr)?.meta).toEqual({
      line: 1,
      col: 8,
      start: 7,
      end: 7,
    });

    expect(parseOne('max(1)').meta).toEqual({
      col: 3,
      end: 2,
      line: 1,
      start: 0,
    });
  }

  testQuotedIdentifierMeta () {
    const sql = 'SELECT "a" FROM "test_schema"."test_table_a"';
    const ast = parseOne(sql) as SelectExpr;

    const fromExpr = ast.args.from as FromExpr;
    const tableExpr = fromExpr.getArgKey('this') as TableExpr;
    const dbMeta = (tableExpr.getArgKey('db') as IdentifierExpr).meta;
    expect(sql.slice(dbMeta['start'] as number, (dbMeta['end'] as number) + 1)).toBe(
      '"test_schema"',
    );

    const tableMeta = narrowInstanceOf(tableExpr.args.this, Expression)?.meta;
    expect(sql.slice(tableMeta?.['start'] as number, (tableMeta?.['end'] as number) + 1)).toBe(
      '"test_table_a"',
    );
  }

  testQualifiedFunction () {
    const sql = 'a.b.c.d.e.f.g.foo()';
    const ast = parseOne(sql);
    expect([...ast.walk()].some((node) => node instanceof ColumnExpr)).toBe(false);
    expect([...ast.findAll(DotExpr)].length).toBe(7);
  }

  testPivotMissingAggFunc () {
    try {
      parseOne('select * from tbl pivot(col1 for col2 in (val1, val1))');
      expect.unreachable();
    } catch (err) {
      expect(err).toBeInstanceOf(ParseError);
      expect(String(err)).toContain('Expecting an aggregation function in PIVOT');
    }
  }

  testMultipleQueryModifiers () {
    const sql = 'SELECT * FROM a WHERE b = \'true\' AND c > 50 WHERE c = \'false\'';

    try {
      parseOne(sql);
      expect.unreachable();
    } catch (err) {
      expect(err).toBeInstanceOf(ParseError);
      expect(String(err)).toContain('Found multiple \'WHERE\' clauses. Line 1, Col: 49.');
    }

    expect(
      parseOne(sql, { errorLevel: ErrorLevel.IGNORE }).sql(),
    ).toBe('SELECT * FROM a WHERE c = \'false\'');
  }

  testParseIntoGrantPrincipal () {
    expect(parseOne('ROLE blah', { into: GrantPrincipalExpr })).toBeInstanceOf(GrantPrincipalExpr);
    expect(parseOne('GROUP blah', { into: GrantPrincipalExpr })).toBeInstanceOf(
      GrantPrincipalExpr,
    );
    expect(parseOne('blah', { into: GrantPrincipalExpr })).toBeInstanceOf(GrantPrincipalExpr);
    expect(
      parseOne('ROLE `blah`', {
        into: GrantPrincipalExpr,
        dialect: 'databricks',
      }),
    ).toBeInstanceOf(GrantPrincipalExpr);
    expect(
      parseOne('ROLE `blah`', {
        into: GrantPrincipalExpr,
        dialect: 'databricks',
      }).sql({
        dialect: 'databricks',
      }),
    ).toBe('ROLE `blah`');
  }

  testParseIntoGrantPrivilege () {
    expect(parseOne('SELECT', { into: GrantPrivilegeExpr })).toBeInstanceOf(GrantPrivilegeExpr);
    expect(parseOne('ALL PRIVILEGES', { into: GrantPrivilegeExpr })).toBeInstanceOf(
      GrantPrivilegeExpr,
    );
  }
}

const t = new TestParser();
describe('TestParser', () => {
  test('parse_empty', () => t.testParseEmpty());
  test('parse_into', () => t.testParseInto());
  test('parse_into_error', () => t.testParseIntoError());
  test('parse_into_errors', () => t.testParseIntoErrors());
  test('column', () => t.testColumn());
  test('tuple', () => t.testTuple());
  test('structs', () => t.testStructs());
  test('float', () => t.testFloat());
  test('unnest', () => t.testUnnest());
  test('unnest_projection', () => t.testUnnestProjection());
  test('unary_plus', () => t.testUnaryPlus());
  test('table', () => t.testTable());
  test('union', () => t.testUnion());
  test('select', () => t.testSelect());
  test('lambda_struct', () => t.testLambdaStruct());
  test('transactions', () => t.testTransactions());
  test('identify', () => t.testIdentify());
  test('multi', () => t.testMulti());
  test('expression', () => t.testExpression());
  test('parse_errors', () => t.testParseErrors());
  test('space', () => t.testSpace());
  test('missing_by', () => t.testMissingBy());
  test('parameter', () => t.testParameter());
  test('var', () => t.testVar());
  test('comments_select', () => t.testCommentsSelect());
  test('comments_select_cte', () => t.testCommentsSelectCte());
  test('comments_insert', () => t.testCommentsInsert());
  test('comments_insert_cte', () => t.testCommentsInsertCte());
  test('comments_update', () => t.testCommentsUpdate());
  test('comments_update_cte', () => t.testCommentsUpdateCte());
  test('comments_delete', () => t.testCommentsDelete());
  test('comments_delete_cte', () => t.testCommentsDeleteCte());
  test('type_literals', () => t.testTypeLiterals());
  test('set_expression', () => t.testSetExpression());
  test('pretty_config_override', () => t.testPrettyConfigOverride());
  test('comment_error_n', () => t.testCommentErrorN());
  test('comment_error_r', () => t.testCommentErrorR());
  test('create_table_error', () => t.testCreateTableError());
  test('pivot_columns', () => t.testPivotColumns());
  test('parse_nested', () => t.testParseNested());
  test('parse_properties', () => t.testParseProperties());
  test('parse_floats', () => t.testParseFloats());
  test('parse_terse_coalesce', () => t.testParseTerseCoalesce());
  test('parse_intervals', () => t.testParseIntervals());
  test('parse_concat_ws', () => t.testParseConcatWs());
  test('parse_drop_schema', () => t.testParseDropSchema());
  test('parse_create_schema', () => t.testParseCreateSchema());
  test('values_as_identifier', () => t.testValuesAsIdentifier());
  test('alter_set', () => t.testAlterSet());
  test('distinct_from', () => t.testDistinctFrom());
  test('trailing_comments', () => t.testTrailingComments());
  test('parse_prop_eq', () => t.testParsePropEq());
  test('collate', () => t.testCollate());
  test('drop_column', () => t.testDropColumn());
  test('udf_meta', () => t.testUdfMeta());
  test('token_position_meta', () => t.testTokenPositionMeta());
  test('quoted_identifier_meta', () => t.testQuotedIdentifierMeta());
  test('qualified_function', () => t.testQualifiedFunction());
  test('pivot_missing_agg_func', () => t.testPivotMissingAggFunc());
  test('multiple_query_modifiers', () => t.testMultipleQueryModifiers());
  test('parse_into_grant_principal', () => t.testParseIntoGrantPrincipal());
  test('parse_into_grant_privilege', () => t.testParseIntoGrantPrivilege());
});
