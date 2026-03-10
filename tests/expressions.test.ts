import {
  describe, test, expect,
} from 'vitest';
import {
  parseOne, ParseError, alias, toColumn, toIdentifier, toTable, cast, column,
} from '../src/index';
import type {
  IntervalExpr, QueryExpr,
} from '../src/expressions';
import {
  Expression,
  LiteralExpr,
  ColumnExpr,
  IdentifierExpr,
  SelectExpr,
  TableExpr,
  JoinExpr,
  WithExpr,
  CteExpr,
  SubqueryExpr,
  BracketExpr,
  DotExpr,
  StarExpr,
  VarExpr,
  WeekExpr,
  NegExpr,
  FilterExpr,
  WhereExpr,
  CastExpr,
  DataTypeExpr,
  DataTypeExprKind,
  OrderedExpr,
  TupleExpr,
  PropertiesExpr,
  FileFormatPropertyExpr,
  PartitionedByPropertyExpr,
  PropertyExpr,
  EnginePropertyExpr,
  CollatePropertyExpr,
  UnionExpr,
  StrPositionExpr,
  AbsExpr,
  ApproxDistinctExpr,
  ArrayExpr,
  ArrayAggExpr,
  ArrayContainsExpr,
  ArraySizeExpr,
  ArrayIntersectExpr,
  AvgExpr,
  TransactionExpr,
  CeilExpr,
  CoalesceExpr,
  CommitExpr,
  CountExpr,
  CountIfExpr,
  DateAddExpr,
  DateDiffExpr,
  DateStrToDateExpr,
  TsOrDsToTimeExpr,
  DayExpr,
  ExpExpr,
  FloorExpr,
  GenerateSeriesExpr,
  GlobExpr,
  GreatestExpr,
  IfExpr,
  InitcapExpr,
  LeastExpr,
  LikeExpr,
  ILikeExpr,
  LnExpr,
  LogExpr,
  MaxExpr,
  MinExpr,
  MonthExpr,
  QuarterExpr,
  PowExpr,
  QuantileExpr,
  RegexpLikeExpr,
  RegexpSplitExpr,
  RollbackExpr,
  RoundExpr,
  SplitExpr,
  StPointExpr,
  StDistanceExpr,
  StrToUnixExpr,
  StructExtractExpr,
  SubstringExpr,
  SumExpr,
  SqrtExpr,
  StddevExpr,
  StddevPopExpr,
  StddevSampExpr,
  TimeToStrExpr,
  TimeToUnixExpr,
  TimeStrToDateExpr,
  TimeStrToTimeExpr,
  TimeStrToUnixExpr,
  TrimExpr,
  TsOrDsAddExpr,
  TsOrDsToDateExpr,
  UnixToStrExpr,
  UnixToTimeExpr,
  UnixToTimeStrExpr,
  VarianceExpr,
  VariancePopExpr,
  YearExpr,
  HllExpr,
  StandardHashExpr,
  DateExpr,
  HexExpr,
  LowerHexExpr,
  Md5Expr,
  TransformExpr,
  AddMonthsExpr,
  CurrentTimestampExpr,
  TimeUnitExpr,
  table_,
  tableName,
  replaceTables,
  expand,
  replacePlaceholders,
  renameTable,
  parseIdentifier,
  convert,
  toInterval,
  values,
  null_,
  true_,
  var_,
  isType,
  JsonExtractExpr,
  JsonExtractScalarExpr,
} from '../src/expressions';
import { NormalizeFunctions } from '../src/dialects/dialect';

class TestExpressions {
  testArgKey () {
    const lit = parseOne('sum(1)').find(LiteralExpr);
    expect(lit instanceof LiteralExpr).toBe(true);
    expect((lit as LiteralExpr).argKey).toBe('this');
  }

  testDepth () {
    const lit = parseOne('x(1)').find(LiteralExpr);
    expect(lit instanceof LiteralExpr).toBe(true);
    expect((lit as LiteralExpr).depth).toBe(1);
  }

  testEq () {
    const query = parseOne('SELECT x FROM t');
    expect(query.sql()).toBe(query.copy().sql());

    expect(toIdentifier('a').sql()).not.toBe(toIdentifier('A').sql());

    const col1 = new ColumnExpr({
      table: toIdentifier('b'),
      this: toIdentifier('b'),
    });
    const col2 = new ColumnExpr({
      this: toIdentifier('b'),
      table: toIdentifier('b'),
    });
    expect(col1.sql()).toBe(col2.sql());

    expect(parseOne('\'x\'').sql()).not.toBe(parseOne('\'X\'').sql());
    expect(parseOne('\'1\'').sql()).not.toBe(parseOne('1').sql());
    expect(parseOne('`a`', { read: 'hive' }).sql()).toBe(parseOne('"a"').sql());
    expect(parseOne('`a`.`b`', { read: 'hive' }).sql()).toBe(parseOne('"a"."b"').sql());
    expect(parseOne('select a, b+1').sql()).toBe(parseOne('SELECT a, b + 1').sql());
    expect(parseOne('a.b.c.d', { read: 'hive' }).sql()).toBe(parseOne('a.b.c.d').sql());
    expect(parseOne('a + b * c - 1.0').sql()).toBe(parseOne('a+b*c-1.0').sql());
    expect(parseOne('a + b * c - 1.0').sql()).not.toBe(parseOne('a + b * c + 1.0').sql());
    expect(parseOne('a as b').sql()).toBe(parseOne('a AS b').sql());
    expect(parseOne('a as b').sql()).not.toBe(parseOne('a').sql());
    expect(new TableExpr({ pivots: [] }).sql()).toBe(new TableExpr({}).sql());
  }

  testFind () {
    const expression = parseOne('CREATE TABLE x STORED AS PARQUET AS SELECT * FROM y');
    expect(expression.find(SelectExpr)).toBeTruthy();
    expect(expression.find(JoinExpr)).toBeFalsy();
    expect(
      [...expression.findAll(TableExpr)].map((t) => t.name),
    ).toEqual(['x', 'y']);
  }

  testFindAll () {
    const expression = parseOne(`
      SELECT *
      FROM (
          SELECT b.*
          FROM a.b b
      ) x
      JOIN (
        SELECT c.foo
        FROM a.c c
        WHERE foo = 1
      ) y
        ON x.c = y.foo
      CROSS JOIN (
        SELECT *
        FROM (
          SELECT d.bar
          FROM d
        ) nested
      ) z
        ON x.c = y.foo
    `);

    expect(
      [...expression.findAll(TableExpr)].map((t) => t.name),
    ).toEqual([
      'b',
      'c',
      'd',
    ]);

    const addExpr = parseOne('select a + b + c + d');

    expect(
      [...addExpr.findAll(ColumnExpr)].map((c) => c.name),
    ).toEqual([
      'd',
      'c',
      'a',
      'b',
    ]);

    expect(
      [...addExpr.findAll(ColumnExpr, { bfs: false })].map((c) => c.name),
    ).toEqual([
      'a',
      'b',
      'c',
      'd',
    ]);
  }

  testFindAncestor () {
    const col = parseOne('select * from foo where (a + 1 > 2)').find(ColumnExpr);
    expect(col).toBeInstanceOf(ColumnExpr);
    expect((col as ColumnExpr).parentSelect).toBeInstanceOf(SelectExpr);
    expect((col as ColumnExpr).findAncestor(JoinExpr)).toBeUndefined();
  }

  testToDot () {
    const orig = parseOne('a.b.c."d".e.f');
    expect([...(orig as DotExpr).parts].map((p) => p.sql()).join('.')).toBe('a.b.c."d".e.f');

    const dot = DotExpr.build([
      toTable('a.b.c'),
      toIdentifier('d'),
      toIdentifier('e'),
      toIdentifier('f'),
    ]);
    expect([...dot.parts].map((p) => p.sql()).join('.')).toBe('a.b.c.d.e.f');

    const col = orig.find(ColumnExpr);
    expect(col).toBeTruthy();
    const toDot = (col as ColumnExpr).toDot();
    expect(toDot).toBeTruthy();
    expect((toDot as DotExpr).sql()).toBe('a.b.c."d".e.f');
  }

  testRoot () {
    const ast = parseOne('select * from (select a from x)');
    expect(ast.root()).toBe(ast);
    const col = ast.find(ColumnExpr);
    expect((col as ColumnExpr).root()).toBe(ast);
  }

  testAliasOrName () {
    const expression = parseOne(
      'SELECT a, b AS B, c + d AS e, *, \'zz\', \'zz\' AS z FROM foo as bar, baz',
    );
    expect(
      (expression as SelectExpr).selects.map((e) => e.aliasOrName),
    ).toEqual([
      'a',
      'B',
      'e',
      '*',
      'zz',
      'z',
    ]);

    expect(
      new Set([...expression.findAll(TableExpr)].map((t) => t.aliasOrName)),
    ).toEqual(new Set(['bar', 'baz']));

    const withExpr = parseOne(`
      WITH first AS (SELECT * FROM foo),
           second AS (SELECT * FROM bar)
      SELECT * FROM first, second, (SELECT * FROM baz) AS third
    `);

    expect(
      (withExpr.getArgKey('with') as WithExpr).args.expressions?.map((e) => (e as Expression).aliasOrName),
    ).toEqual(['first', 'second']);

    expect((withExpr.getArgKey('from') as Expression).aliasOrName).toBe('first');
    expect(
      (withExpr.getArgKey('joins') as Expression[]).map((e) => e.aliasOrName),
    ).toEqual(['second', 'third']);

    expect(parseOne('x.*').name).toBe('*');
    expect(parseOne('NULL').name).toBe('NULL');
    expect(parseOne('a.b.c').name).toBe('c');
  }

  testTableName () {
    const bqDashedTable = toTable('a-1.b.c', { dialect: 'bigquery' });
    expect(tableName(bqDashedTable)).toBe('"a-1".b.c');
    expect(tableName(bqDashedTable, { dialect: 'bigquery' })).toBe('`a-1`.b.c');
    expect(tableName('a-1.b.c', { dialect: 'bigquery' })).toBe('`a-1`.b.c');
    expect(tableName(parseOne('a', { into: TableExpr }))).toBe('a');
    expect(tableName(parseOne('a.b', { into: TableExpr }))).toBe('a.b');
    expect(tableName(parseOne('a.b.c', { into: TableExpr }))).toBe('a.b.c');
    expect(tableName('a.b.c')).toBe('a.b.c');
    expect(tableName(toTable('a.b.c.d.e', { dialect: 'bigquery' }))).toBe('a.b.c.d.e');
    expect(tableName(toTable('\'@foo\'', { dialect: 'snowflake' }))).toBe('\'@foo\'');
    expect(tableName(toTable('@foo', { dialect: 'snowflake' }))).toBe('@foo');
    expect(tableName(bqDashedTable, { identify: true })).toBe('"a-1"."b"."c"');
    expect(
      tableName(parseOne('/*c*/foo.bar', { into: TableExpr }), { identify: true }),
    ).toBe('"foo"."bar"');
  }

  testTable () {
    const fromTable = parseOne('select * from a b').find(TableExpr) as TableExpr;
    expect(table_('a', { alias: 'b' }).sql()).toBe(fromTable.sql());
    expect(table_('a', { db: '' }).sql()).toBe('a');
    expect(new TableExpr({ db: toIdentifier('a') }).sql()).toBe('a');
  }

  testReplaceTables () {
    expect(
      replaceTables(
        parseOne(
          'select * from a AS a, b, c.a, d.a cross join e.a cross join "f-F"."A" cross join G',
        ),
        {
          'a': 'a1',
          'b': 'b.a',
          'c.a': 'c.a2',
          'd.a': 'd2',
          '`f-F`.`A`': '"F"',
          'g': 'g1.a',
        },
        { dialect: 'bigquery' },
      ).sql(),
    ).toBe(
      'SELECT * FROM a1 AS a /* a */, b.a /* b */, c.a2 /* c.a */, d2 /* d.a */ CROSS JOIN e.a CROSS JOIN "F" /* f-F.A */ CROSS JOIN g1.a /* g */',
    );

    expect(
      replaceTables(
        parseOne('select * from example.table', { dialect: 'bigquery' }),
        { 'example.table': '`my-project.example.table`' },
        { dialect: 'bigquery' },
      ).sql(),
    ).toBe('SELECT * FROM "my-project"."example"."table" /* example.table */');

    expect(
      replaceTables(
        parseOne('select * from example.table /* sqlglot.meta replace=false */'),
        { 'example.table': 'a.b' },
      ).sql(),
    ).toBe('SELECT * FROM example.table /* sqlglot.meta replace=false */');
  }

  testExpand () {
    expect(
      expand(
        parseOne('select * from "a-b"."C" AS a'),
        {
          '`a-b`.c': parseOne('select 1') as QueryExpr,
        },
        { dialect: 'spark' },
      ).sql(),
    ).toBe('SELECT * FROM (SELECT 1) AS a /* source: a-b.c */');
  }

  testExpandWithLazySourceProvider () {
    expect(
      expand(
        parseOne('select * from "a-b"."C" AS a'),
        { '`a-b`.c': () => parseOne('select 1', { dialect: 'spark' }) },
        { dialect: 'spark' },
      ).sql(),
    ).toBe('SELECT * FROM (SELECT 1) AS a /* source: a-b.c */');
  }

  testReplacePlaceholders () {
    expect(
      replacePlaceholders(
        parseOne('select * from :tbl1 JOIN :tbl2 ON :col1 = :str1 WHERE :col2 > :int1'),
        [],
        {
          tbl1: toIdentifier('foo'),
          tbl2: toIdentifier('bar'),
          col1: toIdentifier('a'),
          col2: toIdentifier('c'),
          str1: 'b',
          int1: 100,
        },
      ).sql(),
    ).toBe('SELECT * FROM foo JOIN bar ON a = \'b\' WHERE c > 100');

    expect(
      replacePlaceholders(
        parseOne('select * from ? JOIN ? ON ? = ? WHERE ? = \'bla\''),
        [
          toIdentifier('foo'),
          toIdentifier('bar'),
          toIdentifier('a'),
          'b',
          'bla',
        ],
      ).sql(),
    ).toBe('SELECT * FROM foo JOIN bar ON a = \'b\' WHERE \'bla\' = \'bla\'');

    expect(
      replacePlaceholders(
        parseOne('select * from ? WHERE ? > 100'),
        [toIdentifier('foo')],
      ).sql(),
    ).toBe('SELECT * FROM foo WHERE ? > 100');

    expect(
      replacePlaceholders(
        parseOne('select * from :name WHERE ? > 100'),
        [],
        { another_name: 'bla' },
      ).sql(),
    ).toBe('SELECT * FROM :name WHERE ? > 100');

    expect(
      replacePlaceholders(
        parseOne('select * from (SELECT :col1 FROM ?) WHERE :col2 > ?'),
        [
          toIdentifier('tbl1'),
          100,
          'tbl3',
        ],
        {
          col1: toIdentifier('a'),
          col2: toIdentifier('b'),
          col3: 'c',
        },
      ).sql(),
    ).toBe('SELECT * FROM (SELECT a FROM tbl1) WHERE b > 100');

    expect(
      replacePlaceholders(
        parseOne('select * from foo WHERE x > ? AND y IS ?'),
        [0, false],
      ).sql(),
    ).toBe('SELECT * FROM foo WHERE x > 0 AND y IS FALSE');

    expect(
      replacePlaceholders(
        parseOne('select * from foo WHERE x > :int1 AND y IS :bool1'),
        [],
        {
          int1: 0,
          bool1: false,
        },
      ).sql(),
    ).toBe('SELECT * FROM foo WHERE x > 0 AND y IS FALSE');
  }

  testNamedSelects () {
    const expression = parseOne(
      'SELECT a, b AS B, c + d AS e, *, \'zz\', \'zz\' AS z FROM foo as bar, baz',
    );
    expect((expression as SelectExpr).namedSelects).toEqual([
      'a',
      'B',
      'e',
      '*',
      'zz',
      'z',
    ]);

    const withExpr = parseOne(`
      WITH first AS (SELECT * FROM foo)
      SELECT foo.bar, foo.baz as bazz, SUM(x) FROM first
    `);
    expect((withExpr as SelectExpr).namedSelects).toEqual(['bar', 'bazz']);

    const unionExpr = parseOne(`
      SELECT foo, bar FROM first
      UNION SELECT "ss" as foo, bar FROM second
      UNION ALL SELECT foo, bazz FROM third
    `);
    expect((unionExpr as UnionExpr).namedSelects).toEqual(['foo', 'bar']);
  }

  testSelects () {
    expect((parseOne('SELECT FROM x') as SelectExpr).selects).toEqual([]);

    const a = parseOne('SELECT a FROM x') as SelectExpr;
    expect(a.selects.map((s) => s.sql())).toEqual(['a']);

    const ab = parseOne('SELECT a, b FROM x') as SelectExpr;
    expect(ab.selects.map((s) => s.sql())).toEqual(['a', 'b']);

    const paren = parseOne('(SELECT a, b FROM x)') as SubqueryExpr;
    expect(paren.selects.map((s) => s.sql())).toEqual(['a', 'b']);
  }

  testAliasColumnNames () {
    let expression = parseOne('SELECT * FROM (SELECT * FROM x) AS y');
    let subquery = expression.find(SubqueryExpr);
    expect((subquery as SubqueryExpr).aliasColumnNames).toEqual([]);

    expression = parseOne('SELECT * FROM (SELECT * FROM x) AS y(a)');
    subquery = expression.find(SubqueryExpr);
    expect((subquery as SubqueryExpr).aliasColumnNames).toEqual(['a']);

    expression = parseOne('SELECT * FROM (SELECT * FROM x) AS y(a, b)');
    subquery = expression.find(SubqueryExpr);
    expect((subquery as SubqueryExpr).aliasColumnNames).toEqual(['a', 'b']);

    expression = parseOne('WITH y AS (SELECT * FROM x) SELECT * FROM y');
    const cte = expression.find(CteExpr);
    expect((cte as CteExpr).aliasColumnNames).toEqual([]);

    expression = parseOne('WITH y(a, b) AS (SELECT * FROM x) SELECT * FROM y');
    const cte2 = expression.find(CteExpr);
    expect((cte2 as CteExpr).aliasColumnNames).toEqual(['a', 'b']);

    expression = parseOne('SELECT * FROM tbl AS tbl(a, b)');
    const tbl = expression.find(TableExpr);
    expect((tbl as TableExpr).aliasColumnNames).toEqual(['a', 'b']);
  }

  testCast () {
    const expression = parseOne('CAST(x AS DATE)') as CastExpr;
    expect(expression.type).toBe(expression.getArgKey('to'));

    const selectExpr = parseOne('select cast(x as DATE)');
    const casts = [...selectExpr.findAll(CastExpr)];
    expect(casts.length).toBe(1);

    const castNode = casts[0] as CastExpr;
    expect((castNode.getArgKey('to') as DataTypeExpr).isType('DATE')).toBe(true);

    // check that already cast values aren't re-cast if wrapped in a cast to the same type
    const recast = cast(castNode, DataTypeExprKind.DATE);
    expect(recast.sql()).toBe(castNode.sql());
    expect(recast.sql()).toBe('CAST(x AS DATE)');

    // however, recasting is fine if the types are different
    const recast2 = cast(castNode, DataTypeExprKind.VARCHAR);
    expect(recast2.sql()).not.toBe(castNode.sql());
    expect([...recast2.findAll(CastExpr)].length).toBe(2);
    expect(recast2.sql()).toBe('CAST(CAST(x AS DATE) AS VARCHAR)');

    // check that dialect is used when casting strings
    expect(cast('x', 'regtype', { dialect: 'postgres' }).sql()).toBe('CAST(x AS REGTYPE)');
    expect(cast('`x`', 'date', { dialect: 'hive' }).sql()).toBe('CAST("x" AS DATE)');
  }

  testCtes () {
    const expression = parseOne('SELECT a FROM x');
    expect((expression as SelectExpr).ctes).toEqual([]);

    const withExpr = parseOne('WITH x AS (SELECT a FROM y) SELECT a FROM x');
    expect((withExpr as SelectExpr).ctes.map((s) => s.sql())).toEqual(['x AS (SELECT a FROM y)']);
  }

  testSql () {
    expect(parseOne('x + y * 2').sql()).toBe('x + y * 2');
    expect(parseOne('select "x"').sql({
      dialect: 'hive',
      pretty: true,
    })).toBe('SELECT\n  `x`');
    expect(parseOne('X + y').sql({
      identify: true,
      normalize: true,
    })).toBe('"x" + "y"');
    expect(parseOne('"X" + Y').sql({
      identify: true,
      normalize: true,
    })).toBe('"X" + "y"');
    expect(parseOne('SUM(X)').sql({
      identify: true,
      normalize: true,
    })).toBe('SUM("x")');
  }

  testTransformSimple () {
    const expression = parseOne('IF(a > 0, a, b)');

    const fun = (node: Expression) => {
      if (node instanceof ColumnExpr && node.name === 'a') {
        return parseOne('c - 2');
      }
      return node;
    };

    const actual1 = expression.transform(fun);
    expect(actual1.sql({ dialect: 'presto' })).toBe('IF(c - 2 > 0, c - 2, b)');
    expect(actual1 === expression).toBe(false);

    const actual2 = expression.transform(fun, { copy: false });
    expect(actual2.sql({ dialect: 'presto' })).toBe('IF(c - 2 > 0, c - 2, b)');
    expect(actual2 === expression).toBe(true);
  }

  testTransformNoInfiniteRecursion () {
    const expression = parseOne('a');

    const fun = (node: Expression) => {
      if (node instanceof ColumnExpr && node.name === 'a') {
        return parseOne('FUN(a)');
      }
      return node;
    };

    expect(expression.transform(fun).sql()).toBe('FUN(a)');
  }

  testTransformWithParentMutation () {
    const expression = parseOne('SELECT COUNT(1) FROM table');

    const fun = (node: Expression) => {
      if (node.sql() === 'COUNT(1)') {
        return new FilterExpr({
          this: node,
          expression: new WhereExpr({ this: true_() }),
        });
      }
      return node;
    };

    const transformed = expression.transform(fun);
    expect(transformed.sql()).toBe('SELECT COUNT(1) FILTER(WHERE TRUE) FROM table');
  }

  testTransformMultipleChildren () {
    const expression = parseOne('SELECT * FROM x');

    const fun = (node: Expression) => {
      if (node instanceof StarExpr) {
        return [parseOne('a'), parseOne('b')];
      }
      return node;
    };

    expect(expression.transform(fun).sql()).toBe('SELECT a, b FROM x');
  }

  testTransformNodeRemoval () {
    let expression = parseOne('SELECT a, b FROM x');

    const removeColumnB = (node: Expression) => {
      if (node instanceof ColumnExpr && node.name === 'b') {
        return undefined;
      }
      return node;
    };

    expect(expression.transform(removeColumnB).sql()).toBe('SELECT a FROM x');

    expression = parseOne('CAST(x AS FLOAT)');

    const removeNonListArg = (node: Expression) => {
      if (node instanceof DataTypeExpr) {
        return undefined;
      }
      return node;
    };

    expect(expression.transform(removeNonListArg).sql()).toBe('CAST(x AS)');

    expression = parseOne('SELECT a, b FROM x');

    const removeAllColumns = (node: Expression) => {
      if (node instanceof ColumnExpr) {
        return undefined;
      }
      return node;
    };

    expect(expression.transform(removeAllColumns).sql()).toBe('SELECT FROM x');
  }

  testReplace () {
    const expression = parseOne('SELECT a, b FROM x');
    (expression.find(ColumnExpr) as ColumnExpr).replace(parseOne('c'));
    expect(expression.sql()).toBe('SELECT c, b FROM x');
    (expression.find(TableExpr) as TableExpr).replace(parseOne('y'));
    expect(expression.sql()).toBe('SELECT c, b FROM y');

    const orderByExpr = parseOne('SELECT * FROM x ORDER BY a DESC, c');
    const ordered = orderByExpr.find(OrderedExpr) as OrderedExpr;
    (ordered.getArgKey('this') as Expression).replace([column({ col: 'a' }).asc(), column({ col: 'b' }).desc()]);
    expect(orderByExpr.sql()).toBe('SELECT * FROM x ORDER BY a, b DESC, c');
  }

  testArgDeletion () {
    let expression = parseOne('SELECT a, b FROM x');
    (expression.find(ColumnExpr) as ColumnExpr).pop();
    expect(expression.sql()).toBe('SELECT b FROM x');

    (expression.find(ColumnExpr) as ColumnExpr).pop();
    expect(expression.sql()).toBe('SELECT FROM x');

    expression.pop();
    expect(expression.sql()).toBe('SELECT FROM x');

    expression = parseOne('WITH x AS (SELECT a FROM x) SELECT * FROM x');
    (expression.find(WithExpr) as WithExpr).pop();
    expect(expression.sql()).toBe('SELECT * FROM x');

    expression = parseOne('SELECT * FROM foo JOIN bar');
    expect(((expression.args as Record<string, unknown>)['joins'] as unknown[] || []).length).toBe(1);

    expression.setArgKey('joins', undefined);
    expect(expression.sql()).toBe('SELECT * FROM foo');
    expect(((expression.args as Record<string, unknown>)['joins'] as unknown[]) ?? []).toEqual([]);
  }

  testWalk () {
    const expression = parseOne('SELECT * FROM (SELECT * FROM x)');
    expect([...expression.walk()].length).toBe(9);
    expect([...expression.walk({ bfs: false })].length).toBe(9);
    expect([...expression.walk()].every((e) => e instanceof Expression)).toBe(true);
    expect([...expression.walk({ bfs: false })].every((e) => e instanceof Expression)).toBe(true);
  }

  testStrPositionOrder () {
    const strPositionExp = parseOne('STR_POSITION(\'mytest\', \'test\')');
    expect(strPositionExp).toBeInstanceOf(StrPositionExpr);
    expect(((strPositionExp as StrPositionExpr).getArgKey('this') as LiteralExpr).getArgKey('this')).toBe('mytest');
    expect(((strPositionExp as StrPositionExpr).getArgKey('substr') as LiteralExpr).getArgKey('this')).toBe('test');
  }

  testFunctions () {
    expect(parseOne('x LIKE ANY (y)')).toBeInstanceOf(LikeExpr);
    expect(parseOne('x ILIKE ANY (y)')).toBeInstanceOf(ILikeExpr);
    expect(parseOne('ABS(a)')).toBeInstanceOf(AbsExpr);
    expect(parseOne('APPROX_DISTINCT(a)')).toBeInstanceOf(ApproxDistinctExpr);
    expect(parseOne('ARRAY(a)')).toBeInstanceOf(ArrayExpr);
    expect(parseOne('ARRAY_AGG(a)')).toBeInstanceOf(ArrayAggExpr);
    expect(parseOne('ARRAY_CONTAINS(a, \'a\')')).toBeInstanceOf(ArrayContainsExpr);
    expect(parseOne('ARRAY_SIZE(a)')).toBeInstanceOf(ArraySizeExpr);
    expect(parseOne('ARRAY_INTERSECTION([1, 2], [2, 3])')).toBeInstanceOf(ArrayIntersectExpr);
    expect(parseOne('ARRAY_INTERSECT([1, 2], [2, 3])')).toBeInstanceOf(ArrayIntersectExpr);
    expect(parseOne('AVG(a)')).toBeInstanceOf(AvgExpr);
    expect(parseOne('BEGIN DEFERRED TRANSACTION')).toBeInstanceOf(TransactionExpr);
    expect(parseOne('CEIL(a)')).toBeInstanceOf(CeilExpr);
    expect(parseOne('CEILING(a)')).toBeInstanceOf(CeilExpr);
    expect(parseOne('COALESCE(a, b)')).toBeInstanceOf(CoalesceExpr);
    expect(parseOne('COMMIT')).toBeInstanceOf(CommitExpr);
    expect(parseOne('COUNT(a)')).toBeInstanceOf(CountExpr);
    expect(parseOne('COUNT_IF(a > 0)')).toBeInstanceOf(CountIfExpr);
    expect(parseOne('DATE_ADD(a, 1)')).toBeInstanceOf(DateAddExpr);
    expect(parseOne('DATE_DIFF(a, 2)')).toBeInstanceOf(DateDiffExpr);
    expect(parseOne('DATE_STR_TO_DATE(a)')).toBeInstanceOf(DateStrToDateExpr);
    expect(parseOne('TS_OR_DS_TO_TIME(a)')).toBeInstanceOf(TsOrDsToTimeExpr);
    expect(parseOne('DAY(a)')).toBeInstanceOf(DayExpr);
    expect(parseOne('EXP(a)')).toBeInstanceOf(ExpExpr);
    expect(parseOne('FLOOR(a)')).toBeInstanceOf(FloorExpr);
    expect(parseOne('GENERATE_SERIES(a, b, c)')).toBeInstanceOf(GenerateSeriesExpr);
    expect(parseOne('GLOB(x, y)')).toBeInstanceOf(GlobExpr);
    expect(parseOne('GREATEST(a, b)')).toBeInstanceOf(GreatestExpr);
    expect(parseOne('IF(a, b, c)')).toBeInstanceOf(IfExpr);
    expect(parseOne('INITCAP(a)')).toBeInstanceOf(InitcapExpr);
    expect(parseOne('JSON_EXTRACT(a, \'$.name\')')).toBeInstanceOf(JsonExtractExpr);
    expect(parseOne('JSON_EXTRACT_SCALAR(a, \'$.name\')')).toBeInstanceOf(JsonExtractScalarExpr);
    expect(parseOne('LEAST(a, b)')).toBeInstanceOf(LeastExpr);
    expect(parseOne('LIKE(x, y)')).toBeInstanceOf(LikeExpr);
    expect(parseOne('LN(a)')).toBeInstanceOf(LnExpr);
    expect(parseOne('LOG(b, n)')).toBeInstanceOf(LogExpr);
    expect(parseOne('LOG2(a)')).toBeInstanceOf(LogExpr);
    expect(parseOne('LOG10(a)')).toBeInstanceOf(LogExpr);
    expect(parseOne('MAX(a)')).toBeInstanceOf(MaxExpr);
    expect(parseOne('MIN(a)')).toBeInstanceOf(MinExpr);
    expect(parseOne('MONTH(a)')).toBeInstanceOf(MonthExpr);
    expect(parseOne('QUARTER(a)')).toBeInstanceOf(QuarterExpr);
    expect(parseOne('POSITION(\' \' IN a)')).toBeInstanceOf(StrPositionExpr);
    expect(parseOne('POW(a, 2)')).toBeInstanceOf(PowExpr);
    expect(parseOne('POWER(a, 2)')).toBeInstanceOf(PowExpr);
    expect(parseOne('QUANTILE(a, 0.90)')).toBeInstanceOf(QuantileExpr);
    expect(parseOne('REGEXP_LIKE(a, \'test\')')).toBeInstanceOf(RegexpLikeExpr);
    expect(parseOne('REGEXP_SPLIT(a, \'test\')')).toBeInstanceOf(RegexpSplitExpr);
    expect(parseOne('ROLLBACK')).toBeInstanceOf(RollbackExpr);
    expect(parseOne('ROUND(a)')).toBeInstanceOf(RoundExpr);
    expect(parseOne('ROUND(a, 2)')).toBeInstanceOf(RoundExpr);
    expect(parseOne('SPLIT(a, \'test\')')).toBeInstanceOf(SplitExpr);
    expect(parseOne('ST_POINT(10, 20)')).toBeInstanceOf(StPointExpr);
    expect(parseOne('ST_DISTANCE(a, b)')).toBeInstanceOf(StDistanceExpr);
    expect(parseOne('STR_POSITION(a, \'test\')')).toBeInstanceOf(StrPositionExpr);
    expect(parseOne('STR_TO_UNIX(a, \'format\')')).toBeInstanceOf(StrToUnixExpr);
    expect(parseOne('STRUCT_EXTRACT(a, \'test\')')).toBeInstanceOf(StructExtractExpr);
    expect(parseOne('SUBSTR(\'a\', 1, 1)')).toBeInstanceOf(SubstringExpr);
    expect(parseOne('SUBSTRING(\'a\', 1, 1)')).toBeInstanceOf(SubstringExpr);
    expect(parseOne('SUM(a)')).toBeInstanceOf(SumExpr);
    expect(parseOne('SQRT(a)')).toBeInstanceOf(SqrtExpr);
    expect(parseOne('STDDEV(a)')).toBeInstanceOf(StddevExpr);
    expect(parseOne('STDDEV_POP(a)')).toBeInstanceOf(StddevPopExpr);
    expect(parseOne('STDDEV_SAMP(a)')).toBeInstanceOf(StddevSampExpr);
    expect(parseOne('TIME_TO_STR(a, \'format\')')).toBeInstanceOf(TimeToStrExpr);
    expect(parseOne('TIME_TO_TIME_STR(a)')).toBeInstanceOf(CastExpr);
    expect(parseOne('TIME_TO_UNIX(a)')).toBeInstanceOf(TimeToUnixExpr);
    expect(parseOne('TIME_STR_TO_DATE(a)')).toBeInstanceOf(TimeStrToDateExpr);
    expect(parseOne('TIME_STR_TO_TIME(a)')).toBeInstanceOf(TimeStrToTimeExpr);
    expect(parseOne('TIME_STR_TO_TIME(a, \'some_zone\')')).toBeInstanceOf(TimeStrToTimeExpr);
    expect(parseOne('TIME_STR_TO_UNIX(a)')).toBeInstanceOf(TimeStrToUnixExpr);
    expect(parseOne('TRIM(LEADING \'b\' FROM \'bla\')')).toBeInstanceOf(TrimExpr);
    expect(parseOne('TS_OR_DS_ADD(a, 1, \'day\')')).toBeInstanceOf(TsOrDsAddExpr);
    expect(parseOne('TS_OR_DS_TO_DATE(a)')).toBeInstanceOf(TsOrDsToDateExpr);
    expect(parseOne('TS_OR_DS_TO_DATE_STR(a)')).toBeInstanceOf(SubstringExpr);
    expect(parseOne('UNIX_TO_STR(a, \'format\')')).toBeInstanceOf(UnixToStrExpr);
    expect(parseOne('UNIX_TO_TIME(a)')).toBeInstanceOf(UnixToTimeExpr);
    expect(parseOne('UNIX_TO_TIME_STR(a)')).toBeInstanceOf(UnixToTimeStrExpr);
    expect(parseOne('VARIANCE(a)')).toBeInstanceOf(VarianceExpr);
    expect(parseOne('VARIANCE_POP(a)')).toBeInstanceOf(VariancePopExpr);
    expect(parseOne('YEAR(a)')).toBeInstanceOf(YearExpr);
    expect(parseOne('HLL(a)')).toBeInstanceOf(HllExpr);
    expect(parseOne('ARRAY(time, foo)')).toBeInstanceOf(ArrayExpr);
    expect(parseOne('STANDARD_HASH(\'hello\', \'sha256\')')).toBeInstanceOf(StandardHashExpr);
    expect(parseOne('DATE(foo)')).toBeInstanceOf(DateExpr);
    expect(parseOne('HEX(foo)')).toBeInstanceOf(HexExpr);
    expect(parseOne('LOWER(HEX(foo))')).toBeInstanceOf(LowerHexExpr);
    expect(parseOne('TO_HEX(foo)', { read: 'bigquery' })).toBeInstanceOf(LowerHexExpr);
    expect(parseOne('UPPER(TO_HEX(foo))', { read: 'bigquery' })).toBeInstanceOf(HexExpr);
    expect(parseOne('TO_HEX(MD5(foo))', { read: 'bigquery' })).toBeInstanceOf(Md5Expr);
    expect(parseOne('TRANSFORM(a, b)', { read: 'spark' })).toBeInstanceOf(TransformExpr);
    expect(parseOne('ADD_MONTHS(a, b)')).toBeInstanceOf(AddMonthsExpr);

    const ast = parseOne('GREATEST(a, b, c)') as GreatestExpr;
    expect(Array.isArray(ast.args.expressions)).toBe(true);
    expect(ast.args.expressions?.length).toBe(2);
  }

  testColumn () {
    const col0 = column({
      col: new StarExpr({}),
      table: 't',
    });
    expect(col0.sql()).toBe('t.*');

    const col1 = parseOne('a.b.c.d');
    expect((col1 as ColumnExpr).catalog).toBe('a');
    expect((col1 as ColumnExpr).db).toBe('b');
    expect((col1 as ColumnExpr).table).toBe('c');
    expect((col1 as ColumnExpr).name).toBe('d');

    const col2 = parseOne('a');
    expect((col2 as ColumnExpr).name).toBe('a');
    expect((col2 as ColumnExpr).table).toBe('');

    const fields = parseOne('a.b.c.d.e');
    expect(fields).toBeInstanceOf(DotExpr);
    expect((fields as DotExpr).text('expression')).toBe('e');
    const col3 = fields.find(ColumnExpr) as ColumnExpr;
    expect(col3.name).toBe('d');
    expect(col3.table).toBe('c');
    expect(col3.db).toBe('b');
    expect(col3.catalog).toBe('a');

    const col4 = parseOne('a[0].b');
    expect(col4).toBeInstanceOf(DotExpr);
    expect((col4 as DotExpr).args.this).toBeInstanceOf(BracketExpr);
    expect(((col4 as DotExpr).args.this as BracketExpr).args.this).toBeInstanceOf(ColumnExpr);

    const col5 = parseOne('a.*');
    expect(col5).toBeInstanceOf(ColumnExpr);
    expect((col5 as ColumnExpr).args.this).toBeInstanceOf(StarExpr);
    expect((col5 as ColumnExpr).getArgKey('table')).toBeInstanceOf(IdentifierExpr);
    expect((col5 as ColumnExpr).table).toBe('a');

    expect(parseOne('*')).toBeInstanceOf(StarExpr);
    expect(
      column({
        col: 'a',
        table: 'b',
        db: 'c',
        catalog: 'd',
      }).sql(),
    ).toBe(toColumn('d.c.b.a').sql());

    const dot = column({
      col: 'd',
      table: 'c',
      db: 'b',
      catalog: 'a',
    }, { fields: ['e', 'f'] });
    expect(dot).toBeInstanceOf(DotExpr);
    expect((dot as DotExpr).sql()).toBe('a.b.c.d.e.f');

    const dotQuoted = column({
      col: 'd',
      table: 'c',
      db: 'b',
      catalog: 'a',
    }, {
      fields: ['e', 'f'],
      quoted: true,
    });
    expect((dotQuoted as DotExpr).sql()).toBe('"a"."b"."c"."d"."e"."f"');
  }

  testText () {
    const col = parseOne('a.b.c.d.e');
    expect((col as DotExpr).text('expression')).toBe('e');
    expect((col as DotExpr).text('y')).toBe('');
    expect((parseOne('select * from x.y').find(TableExpr) as TableExpr).text('db')).toBe('x');
    expect(parseOne('select *').name).toBe('');
    expect(parseOne('1 + 1').name).toBe('1');
    expect(parseOne('\'a\'').name).toBe('a');
  }

  testAlias () {
    expect(alias('foo', 'bar').sql()).toBe('foo AS bar');
    expect(alias('foo', 'bar-1').sql()).toBe('foo AS "bar-1"');
    expect(alias('foo', 'bar_1').sql()).toBe('foo AS bar_1');
    expect(alias('foo * 2', '2bar').sql()).toBe('foo * 2 AS "2bar"');
    expect(alias('"foo"', '_bar').sql()).toBe('"foo" AS _bar');
    expect(alias('foo', 'bar', { quoted: true }).sql()).toBe('foo AS "bar"');
  }

  testUnit () {
    const unit = parseOne('timestamp_trunc(current_timestamp, week(thursday))');
    expect(unit.find(CurrentTimestampExpr)).toBeTruthy();
    const week = unit.find(WeekExpr) as WeekExpr;
    expect((week.args.this as VarExpr).sql()).toBe(var_('THURSDAY').sql());

    for (const [abbreviatedUnit, unabbreviatedUnit] of Object.entries(TimeUnitExpr.UNABBREVIATED_UNIT_NAME)) {
      const interval = parseOne(`interval '500 ${abbreviatedUnit}'`);
      expect((interval as IntervalExpr).unit).toBeInstanceOf(VarExpr);
      expect(((interval as IntervalExpr).unit as VarExpr).name).toBe(unabbreviatedUnit);
    }
  }

  testIdentifier () {
    expect(toIdentifier('"x"').args.quoted).toBe(true);
    expect(toIdentifier('x').args.quoted).toBeFalsy();
    expect(toIdentifier('foo ').args.quoted).toBe(true);
    expect(toIdentifier('_x').args.quoted).toBeFalsy();
  }

  testFunctionNormalizer () {
    expect(parseOne('HELLO()').sql({ normalizeFunctions: NormalizeFunctions.LOWER })).toBe('hello()');
    expect(parseOne('hello()').sql({ normalizeFunctions: NormalizeFunctions.UPPER })).toBe('HELLO()');
    expect(parseOne('heLLO()').sql({ normalizeFunctions: NormalizeFunctions.NONE })).toBe('heLLO()');
    expect(parseOne('SUM(x)').sql({ normalizeFunctions: NormalizeFunctions.LOWER })).toBe('sum(x)');
    expect(parseOne('sum(x)').sql({ normalizeFunctions: NormalizeFunctions.UPPER })).toBe('SUM(x)');
  }

  testPropertiesFromDict () {
    expect(
      PropertiesExpr.fromDict({
        FORMAT: 'parquet',
        PARTITIONED_BY: new TupleExpr({ expressions: [toIdentifier('a'), toIdentifier('b')] }),
        custom: 1,
        ENGINE: undefined,
        COLLATE: true,
      }).sql(),
    ).toBe(
      new PropertiesExpr({
        expressions: [
          new FileFormatPropertyExpr({ this: LiteralExpr.string('parquet') }),
          new PartitionedByPropertyExpr({
            this: new TupleExpr({ expressions: [toIdentifier('a'), toIdentifier('b')] }),
          }),
          new PropertyExpr({
            this: LiteralExpr.string('custom'),
            value: LiteralExpr.number(1),
          }),
          new EnginePropertyExpr({ this: null_() }),
          new CollatePropertyExpr({ this: true_() }),
        ],
      }).sql(),
    );

    expect(() => PropertiesExpr.fromDict({ FORMAT: Symbol() as unknown })).toThrow(Error);
  }

  testConvert () {
    const cases: [unknown, string][] = [
      [1, '1'],
      ['1', '\'1\''],
      [undefined, 'NULL'],
      [true, 'TRUE'],
    ];

    for (const [value, expected] of cases) {
      expect(convert(value).sql()).toBe(expected);
    }
  }

  testToInterval () {
    expect(toInterval('1day').sql()).toBe('INTERVAL \'1\' DAY');
    expect(toInterval('  5     months').sql()).toBe('INTERVAL \'5\' MONTHS');
    expect(toInterval('-2 day').sql()).toBe('INTERVAL \'-2\' DAY');

    expect(toInterval(LiteralExpr.string('1day')).sql()).toBe('INTERVAL \'1\' DAY');
    expect(toInterval(LiteralExpr.string('-2 day')).sql()).toBe('INTERVAL \'-2\' DAY');
    expect(toInterval(LiteralExpr.string('  5   months')).sql()).toBe('INTERVAL \'5\' MONTHS');
  }

  testToTable () {
    const tableOnly = toTable('table_name');
    expect(tableOnly.name).toBe('table_name');
    expect(tableOnly.getArgKey('db')).toBeUndefined();
    expect(tableOnly.getArgKey('catalog')).toBeUndefined();

    const dbAndTable = toTable('db.table_name');
    expect(dbAndTable.name).toBe('table_name');
    expect((dbAndTable.getArgKey('db') as IdentifierExpr).sql()).toBe(toIdentifier('db').sql());
    expect(dbAndTable.getArgKey('catalog')).toBeUndefined();

    const catalogDbAndTable = toTable('catalog.db.table_name');
    expect(catalogDbAndTable.name).toBe('table_name');
    expect((catalogDbAndTable.getArgKey('db') as IdentifierExpr).sql()).toBe(toIdentifier('db').sql());
    expect((catalogDbAndTable.getArgKey('catalog') as IdentifierExpr).sql()).toBe(toIdentifier('catalog').sql());

    const tableOnlyUnsafe = toTable('3e');
    expect(tableOnlyUnsafe.sql()).toBe('"3e"');
  }

  testToColumn () {
    const columnOnly = toColumn('column_name');
    expect(columnOnly.name).toBe('column_name');
    expect(columnOnly.getArgKey('table')).toBeUndefined();

    const tableAndColumn = toColumn('table_name.column_name');
    expect(tableAndColumn.name).toBe('column_name');
    expect((tableAndColumn.getArgKey('table') as IdentifierExpr).sql()).toBe(toIdentifier('table_name').sql());

    expect(toColumn('foo bar').sql()).toBe('"foo bar"');
    expect(toColumn('`column_name`', { dialect: 'spark' }).sql()).toBe('"column_name"');
    expect(toColumn('column_name', { quoted: true }).sql()).toBe('"column_name"');
    expect(
      toColumn('column_name', { table: toIdentifier('table_name') }).sql(),
    ).toBe('table_name.column_name');
  }

  testUnion () {
    const expression = parseOne('SELECT cola, colb UNION SELECT colx, coly') as UnionExpr;
    expect(expression).toBeInstanceOf(UnionExpr);
    expect(expression.namedSelects).toEqual(['cola', 'colb']);
    expect(
      expression.selects.map((s) => s.sql()),
    ).toEqual([new ColumnExpr({ this: toIdentifier('cola') }).sql(), new ColumnExpr({ this: toIdentifier('colb') }).sql()]);
  }

  testValues () {
    expect(
      values([[1, 2], [3, 4]], {
        alias: 't',
        columns: ['a', 'b'],
      }).sql(),
    ).toBe('(VALUES (1, 2), (3, 4)) AS t(a, b)');

    expect(() => values([[1, 2], [3, 4]], { columns: ['a'] })).toThrow(Error);
  }

  testDataTypeBuilder () {
    expect(DataTypeExpr.build('TEXT')?.sql()).toBe('TEXT');
    expect(DataTypeExpr.build('DECIMAL(10, 2)')?.sql()).toBe('DECIMAL(10, 2)');
    expect(DataTypeExpr.build('VARCHAR(255)')?.sql()).toBe('VARCHAR(255)');
    expect(DataTypeExpr.build('ARRAY<INT>')?.sql()).toBe('ARRAY<INT>');
    expect(DataTypeExpr.build('CHAR')?.sql()).toBe('CHAR');
    expect(DataTypeExpr.build('NCHAR')?.sql()).toBe('CHAR');
    expect(DataTypeExpr.build('VARCHAR')?.sql()).toBe('VARCHAR');
    expect(DataTypeExpr.build('NVARCHAR')?.sql()).toBe('VARCHAR');
    expect(DataTypeExpr.build('TEXT')?.sql()).toBe('TEXT');
    expect(DataTypeExpr.build('BINARY')?.sql()).toBe('BINARY');
    expect(DataTypeExpr.build('VARBINARY')?.sql()).toBe('VARBINARY');
    expect(DataTypeExpr.build('INT')?.sql()).toBe('INT');
    expect(DataTypeExpr.build('TINYINT')?.sql()).toBe('TINYINT');
    expect(DataTypeExpr.build('SMALLINT')?.sql()).toBe('SMALLINT');
    expect(DataTypeExpr.build('BIGINT')?.sql()).toBe('BIGINT');
    expect(DataTypeExpr.build('FLOAT')?.sql()).toBe('FLOAT');
    expect(DataTypeExpr.build('DOUBLE')?.sql()).toBe('DOUBLE');
    expect(DataTypeExpr.build('DECIMAL')?.sql()).toBe('DECIMAL');
    expect(DataTypeExpr.build('BOOLEAN')?.sql()).toBe('BOOLEAN');
    expect(DataTypeExpr.build('JSON')?.sql()).toBe('JSON');
    expect(DataTypeExpr.build('JSONB', { dialect: 'postgres' })?.sql()).toBe('JSONB');
    expect(DataTypeExpr.build('INTERVAL')?.sql()).toBe('INTERVAL');
    expect(DataTypeExpr.build('TIME')?.sql()).toBe('TIME');
    expect(DataTypeExpr.build('TIMESTAMP')?.sql()).toBe('TIMESTAMP');
    expect(DataTypeExpr.build('TIMESTAMPTZ')?.sql()).toBe('TIMESTAMPTZ');
    expect(DataTypeExpr.build('TIMESTAMPLTZ')?.sql()).toBe('TIMESTAMPLTZ');
    expect(DataTypeExpr.build('DATE')?.sql()).toBe('DATE');
    expect(DataTypeExpr.build('DATETIME')?.sql()).toBe('DATETIME');
    expect(DataTypeExpr.build('ARRAY')?.sql()).toBe('ARRAY');
    expect(DataTypeExpr.build('MAP')?.sql()).toBe('MAP');
    expect(DataTypeExpr.build('UUID')?.sql()).toBe('UUID');
    expect(DataTypeExpr.build('GEOGRAPHY')?.sql()).toBe('GEOGRAPHY');
    expect(DataTypeExpr.build('GEOMETRY')?.sql()).toBe('GEOMETRY');
    expect(DataTypeExpr.build('STRUCT')?.sql()).toBe('STRUCT');
    expect(DataTypeExpr.build('HLLSKETCH', { dialect: 'redshift' })?.sql()).toBe('HLLSKETCH');
    expect(DataTypeExpr.build('HSTORE', { dialect: 'postgres' })?.sql()).toBe('HSTORE');
    expect(DataTypeExpr.build('NULL')?.sql())?.toBe('NULL');
    expect(DataTypeExpr.build('NULL', { dialect: 'bigquery' })?.sql()).toBe('NULL');
    expect(DataTypeExpr.build('UNKNOWN')?.sql()).toBe('UNKNOWN');
    expect(DataTypeExpr.build('UNKNOWN', { dialect: 'bigquery' })?.sql()).toBe('UNKNOWN');
    expect(DataTypeExpr.build('UNKNOWN', { dialect: 'snowflake' })?.sql()).toBe('UNKNOWN');
    expect(DataTypeExpr.build('TIMESTAMP', { dialect: 'bigquery' })?.sql()).toBe('TIMESTAMPTZ');
    expect(DataTypeExpr.build('USER-DEFINED')?.sql()).toBe('USER-DEFINED');
    expect(DataTypeExpr.build('ARRAY<UNKNOWN>')?.sql()).toBe('ARRAY<UNKNOWN>');
    expect(DataTypeExpr.build('ARRAY<NULL>')?.sql()).toBe('ARRAY<NULL>');
    expect(DataTypeExpr.build('varchar(100) collate \'en-ci\'')?.sql()).toBe('VARCHAR(100)');
    expect(DataTypeExpr.build('int[3]')?.sql({ dialect: 'duckdb' })).toBe('INT[3]');
    expect(DataTypeExpr.build('int[3][3]')?.sql({ dialect: 'duckdb' })).toBe('INT[3][3]');
    expect(DataTypeExpr.build('time_ns', { dialect: 'duckdb' })?.sql()).toBe('TIME_NS');
    expect(DataTypeExpr.build('bignum', { dialect: 'duckdb' })?.sql()).toBe('BIGNUM');
    expect(DataTypeExpr.build('struct<x int>', { dialect: 'spark' })?.sql()).toBe('STRUCT<x INT>');

    expect(() => DataTypeExpr.build('varchar(')).toThrow(ParseError);
  }

  testRenameTable () {
    expect(renameTable('t1', 't2').sql()).toBe('ALTER TABLE t1 RENAME TO t2');
  }

  testIsInt () {
    expect(parseOne('- -1').isInteger).toBe(true);
  }

  testIsStar () {
    expect(parseOne('*').isStar).toBe(true);
    expect(parseOne('foo.*').isStar).toBe(true);
    expect(parseOne('SELECT * FROM foo').isStar).toBe(true);
    expect(parseOne('(SELECT * FROM foo)').isStar).toBe(true);
    expect(parseOne('SELECT *, 1 FROM foo').isStar).toBe(true);
    expect(parseOne('SELECT foo.* FROM foo').isStar).toBe(true);
    expect(parseOne('SELECT * EXCEPT (a, b) FROM foo').isStar).toBe(true);
    expect(parseOne('SELECT foo.* EXCEPT (foo.a, foo.b) FROM foo').isStar).toBe(true);
    expect(parseOne('SELECT * REPLACE (a AS b, b AS C)').isStar).toBe(true);
    expect(parseOne('SELECT * EXCEPT (a, b) REPLACE (a AS b, b AS C)').isStar).toBe(true);
    expect(parseOne('SELECT * INTO newevent FROM event').isStar).toBe(true);
    expect(parseOne('SELECT * FROM foo UNION SELECT * FROM bar').isStar).toBe(true);
    expect(parseOne('SELECT * FROM bla UNION SELECT 1 AS x').isStar).toBe(true);
    expect(parseOne('SELECT 1 AS x UNION SELECT * FROM bla').isStar).toBe(true);
    expect(parseOne('SELECT 1 AS x UNION SELECT 1 AS x UNION SELECT * FROM foo').isStar).toBe(true);
  }

  testSetMetadata () {
    const ast = parseOne('SELECT foo.col FROM foo');

    // meta is initialized as empty object in TypeScript (not lazily like Python)
    const userKeys = Object.keys(ast.meta).filter((k) => ![
      'line',
      'col',
      'start',
      'end',
    ].includes(k));
    expect(userKeys.length).toBe(0);

    ast.meta['some_meta_key'] = 'some_meta_value';
    expect(ast.meta['some_meta_key']).toBe('some_meta_value');
    expect(ast.meta['some_other_meta_key']).toBeUndefined();

    ast.meta['some_other_meta_key'] = 'some_other_meta_value';
    expect(ast.meta['some_other_meta_key']).toBe('some_other_meta_value');
  }

  testUnnest () {
    let ast = parseOne('SELECT (((1)))');
    expect((ast as SelectExpr).selects[0].unnest()).toBeInstanceOf(LiteralExpr);

    ast = parseOne('SELECT * FROM (((SELECT * FROM t)))');
    const fromThis = (ast.getArgKey('from') as Expression).getArgKey('this') as Expression;
    expect(fromThis.unnest()).toBeInstanceOf(SelectExpr);
  }

  testIsType () {
    let ast = parseOne('CAST(x AS VARCHAR)');
    expect(ast.isType('VARCHAR')).toBe(true);
    expect(ast.isType('VARCHAR(5)')).toBe(false);
    expect(ast.isType('FLOAT')).toBe(false);

    ast = parseOne('CAST(x AS VARCHAR(5))');
    expect(ast.isType('VARCHAR')).toBe(true);
    expect(ast.isType('VARCHAR(5)')).toBe(true);
    expect(ast.isType('VARCHAR(4)')).toBe(false);
    expect(ast.isType('FLOAT')).toBe(false);

    ast = parseOne('CAST(x AS ARRAY<INT>)');
    expect(ast.isType('ARRAY')).toBe(true);
    expect(ast.isType('ARRAY<INT>')).toBe(true);
    expect(ast.isType('ARRAY<FLOAT>')).toBe(false);
    expect(ast.isType('INT')).toBe(false);

    ast = parseOne('CAST(x AS ARRAY)');
    expect(ast.isType('ARRAY')).toBe(true);
    expect(ast.isType('ARRAY<INT>')).toBe(false);
    expect(ast.isType('ARRAY<FLOAT>')).toBe(false);
    expect(ast.isType('INT')).toBe(false);

    ast = parseOne('CAST(x AS STRUCT<a INT, b FLOAT>)');
    expect(ast.isType('STRUCT')).toBe(true);
    expect(ast.isType('STRUCT<a INT, b FLOAT>')).toBe(true);
    expect(ast.isType('STRUCT<a VARCHAR, b INT>')).toBe(false);

    const dtype = DataTypeExpr.build('foo', { udt: true });
    expect(isType(dtype, 'foo')).toBe(true);
    expect(isType(dtype, 'bar')).toBe(false);

    const dtype2 = DataTypeExpr.build('a.b.c', { udt: true });
    expect(isType(dtype2, 'a.b.c')).toBe(true);

    expect(() => DataTypeExpr.build('foo')).toThrow(ParseError);
  }

  testSetMeta () {
    const query = parseOne('SELECT * FROM foo /* sqlglot.meta x = 1, y = a, z */');
    expect((query.find(TableExpr) as TableExpr).meta).toMatchObject({
      x: true,
      y: 'a',
      z: true,
    });
    expect(query.sql()).toBe('SELECT * FROM foo /* sqlglot.meta x = 1, y = a, z */');
  }

  testParseIdentifier () {
    expect(parseIdentifier('a \' b').sql()).toBe(toIdentifier('a \' b').sql());
  }

  testLiteralNumber () {
    const numbers: (number | string)[] = [
      1,
      -1.1,
      1.1,
      0,
      '-1',
      '1',
      '1.1',
      '-1.1',
      '1e6',
      'inf',
      'binary_double_nan',
    ];

    for (const number of numbers) {
      const literal = LiteralExpr.number(number);

      expect(literal.isNumber).toBe(true);

      const isNegative = typeof number === 'string'
        ? number.startsWith('-')
        : number < 0;

      const expectedThis = typeof number === 'string'
        ? number.replace(/^-/, '')
        : String(Math.abs(number));

      if (isNegative) {
        expect(literal).toBeInstanceOf(NegExpr);
        expect((literal as NegExpr).args.this).toBeInstanceOf(LiteralExpr);
        const innerThis = ((literal as NegExpr).args.this as LiteralExpr).getArgKey('this');
        expect(innerThis).toBe(expectedThis);
      } else {
        expect(literal).toBeInstanceOf(LiteralExpr);
        expect((literal as LiteralExpr).getArgKey('this')).toBe(expectedThis);
      }
    }
  }

  testCommentAlias () {
    const sql = `
      SELECT
        a,
        b AS B,
        c, /*comment*/
        d AS D, -- another comment
        CAST(x AS INT), -- yet another comment
        y AND /* foo */ w AS E -- final comment
      FROM foo
    `;
    const expression = parseOne(sql) as SelectExpr;
    expect(
      expression.selects.map((e) => e.aliasOrName),
    ).toEqual([
      'a',
      'B',
      'c',
      'D',
      'x',
      'E',
    ]);

    expect(
      expression.sql(),
    ).toBe(
      'SELECT a, b AS B, c /* comment */, d AS D /* another comment */, CAST(x AS INT) /* yet another comment */, y AND /* foo */ w AS E /* final comment */ FROM foo',
    );

    expect(
      expression.sql({ comments: false }),
    ).toBe(
      'SELECT a, b AS B, c, d AS D, CAST(x AS INT), y AND w AS E FROM foo',
    );

    expect(parseOne('max(x) as "a b" -- comment').comments).toEqual([' comment']);
  }
}

const t = new TestExpressions();

describe('TestExpressions', () => {
  test('arg_key', () => t.testArgKey());
  test('depth', () => t.testDepth());
  test('eq', () => t.testEq());
  test('find', () => t.testFind());
  test('find_all', () => t.testFindAll());
  test('find_ancestor', () => t.testFindAncestor());
  test('to_dot', () => t.testToDot());
  test('root', () => t.testRoot());
  test('alias_or_name', () => t.testAliasOrName());
  test('table_name', () => t.testTableName());
  test('table', () => t.testTable());
  test('replace_tables', () => t.testReplaceTables());
  test('expand', () => t.testExpand());
  test('expand_with_lazy_source_provider', () => t.testExpandWithLazySourceProvider());
  test('replace_placeholders', () => t.testReplacePlaceholders());
  test('named_selects', () => t.testNamedSelects());
  test('selects', () => t.testSelects());
  test('alias_column_names', () => t.testAliasColumnNames());
  test('cast', () => t.testCast());
  test('ctes', () => t.testCtes());
  test('sql', () => t.testSql());
  test('transform_simple', () => t.testTransformSimple());
  test('transform_no_infinite_recursion', () => t.testTransformNoInfiniteRecursion());
  test('transform_with_parent_mutation', () => t.testTransformWithParentMutation());
  test('transform_multiple_children', () => t.testTransformMultipleChildren());
  test('transform_node_removal', () => t.testTransformNodeRemoval());
  test('replace', () => t.testReplace());
  test('arg_deletion', () => t.testArgDeletion());
  test('walk', () => t.testWalk());
  test('str_position_order', () => t.testStrPositionOrder());
  test('functions', () => t.testFunctions());
  test('column', () => t.testColumn());
  test('text', () => t.testText());
  test('alias', () => t.testAlias());
  test('unit', () => t.testUnit());
  test('identifier', () => t.testIdentifier());
  test('function_normalizer', () => t.testFunctionNormalizer());
  test('properties_from_dict', () => t.testPropertiesFromDict());
  test('convert', () => t.testConvert());
  test('to_interval', () => t.testToInterval());
  test('to_table', () => t.testToTable());
  test('to_column', () => t.testToColumn());
  test('union', () => t.testUnion());
  test('values', () => t.testValues());
  test('data_type_builder', () => t.testDataTypeBuilder());
  test('rename_table', () => t.testRenameTable());
  test('is_int', () => t.testIsInt());
  test('is_star', () => t.testIsStar());
  test('set_metadata', () => t.testSetMetadata());
  test('unnest', () => t.testUnnest());
  test('is_type', () => t.testIsType());
  test('set_meta', () => t.testSetMeta());
  test('parse_identifier', () => t.testParseIdentifier());
  test('literal_number', () => t.testLiteralNumber());
  test('comment_alias', () => t.testCommentAlias());
});
