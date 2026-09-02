import {
  describe, it, expect,
} from 'vitest';
import {
  parse, parseOne, transpile, optimize, tokenize,
  Generator, generate, Parser, Tokenizer,
  Schema, MappingSchema, Dialect,
  ErrorLevel, ParseError,
  dump, load, diff, lineage,
  version, pretty, setPretty,
} from '../src/index';
import {
  Expression, SelectExpr, ColumnExpr, TableExpr, LiteralExpr,
  CreateExpr, DropExpr, InsertExpr, UpdateExpr, DeleteExpr,
  AliasExpr, IdentifierExpr, StarExpr, WhereExpr, FromExpr,
  JoinExpr, OrderExpr, GroupExpr, HavingExpr, LimitExpr,
  UnionExpr, SubqueryExpr, CaseExpr, IfExpr, InExpr,
  FuncExpr, AnonymousExpr, CastExpr, DataTypeExpr,
  AddExpr, SubExpr, MulExpr, EqExpr, NeqExpr, GtExpr, LtExpr,
  AndExpr, OrExpr, NotExpr, NullExpr, BooleanExpr,
  BlockExpr, StoredProcedureExpr, ExecuteExpr, IfBlockExpr,
  TriggerPropertiesExpr, TriggerEventExpr,
  column, select, from, condition, and, or, not,
  func, case_, null_, true_, false_, alias, values,
  update, delete_, insert, subquery, union, intersect, except,
  table, toIdentifier, cast, maybeParse,
} from '../src/expressions';

describe('Bundle imports', () => {
  it('core functions are defined', () => {
    expect(parse).toBeTypeOf('function');
    expect(parseOne).toBeTypeOf('function');
    expect(transpile).toBeTypeOf('function');
    expect(optimize).toBeTypeOf('function');
    expect(tokenize).toBeTypeOf('function');
    expect(generate).toBeTypeOf('function');
    expect(dump).toBeTypeOf('function');
    expect(load).toBeTypeOf('function');
    expect(diff).toBeTypeOf('function');
    expect(lineage).toBeTypeOf('function');
    expect(setPretty).toBeTypeOf('function');
    expect(version).toBeTypeOf('string');
    expect(pretty).toBe(false);
  });

  it('core classes are defined', () => {
    expect(Generator).toBeTypeOf('function');
    expect(Parser).toBeTypeOf('function');
    expect(Tokenizer).toBeTypeOf('function');
    expect(Schema).toBeTypeOf('function');
    expect(MappingSchema).toBeTypeOf('function');
    expect(Dialect).toBeTypeOf('function');
    expect(ErrorLevel).toBeDefined();
    expect(ParseError).toBeTypeOf('function');
  });

  it('expression classes are defined', () => {
    const classes = [
      Expression,
      SelectExpr,
      ColumnExpr,
      TableExpr,
      LiteralExpr,
      CreateExpr,
      DropExpr,
      InsertExpr,
      UpdateExpr,
      DeleteExpr,
      AliasExpr,
      IdentifierExpr,
      StarExpr,
      WhereExpr,
      FromExpr,
      JoinExpr,
      OrderExpr,
      GroupExpr,
      HavingExpr,
      LimitExpr,
      UnionExpr,
      SubqueryExpr,
      CaseExpr,
      IfExpr,
      InExpr,
      FuncExpr,
      AnonymousExpr,
      CastExpr,
      DataTypeExpr,
      AddExpr,
      SubExpr,
      MulExpr,
      EqExpr,
      NeqExpr,
      GtExpr,
      LtExpr,
      AndExpr,
      OrExpr,
      NotExpr,
      NullExpr,
      BooleanExpr,
      BlockExpr,
      StoredProcedureExpr,
      ExecuteExpr,
      IfBlockExpr,
      TriggerPropertiesExpr,
      TriggerEventExpr,
    ];

    for (const cls of classes) {
      expect(cls).toBeTypeOf('function');
    }
  });

  it('expression builder helpers are defined', () => {
    const helpers = [
      column,
      select,
      from,
      condition,
      and,
      or,
      not,
      func,
      case_,
      null_,
      true_,
      false_,
      alias,
      values,
      update,
      delete_,
      insert,
      subquery,
      union,
      intersect,
      except,
      table,
      toIdentifier,
      cast,
      maybeParse,
    ];

    for (const fn of helpers) {
      expect(fn).toBeTypeOf('function');
    }
  });

  it('optimize works', () => {
    const result = optimize('SELECT 1 + 1');

    expect(result.sql()).toContain('2');
  });

  it('optimize works with schema', () => {
    const result = optimize('SELECT * FROM t WHERE 1 = 1 AND x > 2', {
      schema: {
        t: {
          x: 'INT',
        },
      },
    });

    expect(result.sql()).toContain('x');
  });

  it('all dialects register and parse', async () => {
    const dialects = [
      'athena',
      'bigquery',
      'clickhouse',
      'databricks',
      'doris',
      'dremio',
      'drill',
      'druid',
      'duckdb',
      'dune',
      'exasol',
      'fabric',
      'hive',
      'materialize',
      'mysql',
      'oracle',
      'postgres',
      'presto',
      'redshift',
      'risingwave',
      'singlestore',
      'snowflake',
      'solr',
      'spark',
      'spark2',
      'sqlite',
      'starrocks',
      'tableau',
      'teradata',
      'trino',
      'tsql',
    ];

    for (const d of dialects) {
      await import(`../src/dialects/${d}`);
    }

    for (const d of dialects) {
      const [ast] = parse('SELECT 1', {
        read: d,
      });

      expect(ast).toBeDefined();
      expect(ast.sql({
        dialect: d,
      })).toBe('SELECT 1');

      const [sql] = transpile('SELECT 1', {
        read: d,
        write: d,
      });

      expect(sql).toBe('SELECT 1');
    }
  });
});
