import {
  describe, it, expect, beforeEach, afterEach,
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
import {
  Athena,
} from '../src/dialects/athena';
import {
  BigQuery,
} from '../src/dialects/bigquery';
import {
  ClickHouse,
} from '../src/dialects/clickhouse';
import {
  Databricks,
} from '../src/dialects/databricks';
import {
  Doris,
} from '../src/dialects/doris';
import {
  Dremio,
} from '../src/dialects/dremio';
import {
  Drill,
} from '../src/dialects/drill';
import {
  Druid,
} from '../src/dialects/druid';
import {
  DuckDB,
} from '../src/dialects/duckdb';
import {
  Dune,
} from '../src/dialects/dune';
import {
  Exasol,
} from '../src/dialects/exasol';
import {
  Fabric,
} from '../src/dialects/fabric';
import {
  Hive,
} from '../src/dialects/hive';
import {
  Materialize,
} from '../src/dialects/materialize';
import {
  MySQL,
} from '../src/dialects/mysql';
import {
  Oracle,
} from '../src/dialects/oracle';
import {
  Postgres,
} from '../src/dialects/postgres';
import {
  Presto,
} from '../src/dialects/presto';
import {
  Redshift,
} from '../src/dialects/redshift';
import {
  RisingWave,
} from '../src/dialects/risingwave';
import {
  SingleStore,
} from '../src/dialects/singlestore';
import {
  Snowflake,
} from '../src/dialects/snowflake';
import {
  Solr,
} from '../src/dialects/solr';
import {
  Spark,
} from '../src/dialects/spark';
import {
  Spark2,
} from '../src/dialects/spark2';
import {
  SQLite,
} from '../src/dialects/sqlite';
import {
  StarRocks,
} from '../src/dialects/starrocks';
import {
  Tableau,
} from '../src/dialects/tableau';
import {
  Teradata,
} from '../src/dialects/teradata';
import {
  Trino,
} from '../src/dialects/trino';
import {
  TSQL,
} from '../src/dialects/tsql';

const ALL_DIALECT_CLASSES = [
  Athena,
  BigQuery,
  ClickHouse,
  Databricks,
  Doris,
  Dremio,
  Drill,
  Druid,
  DuckDB,
  Dune,
  Exasol,
  Fabric,
  Hive,
  Materialize,
  MySQL,
  Oracle,
  Postgres,
  Presto,
  Redshift,
  RisingWave,
  SingleStore,
  Snowflake,
  Solr,
  Spark,
  Spark2,
  SQLite,
  StarRocks,
  Tableau,
  Teradata,
  Trino,
  TSQL,
];

describe('Exports', () => {
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
});

describe('Dialect registration', () => {
  let savedRegistry: Map<string, typeof Dialect>;

  beforeEach(() => {
    savedRegistry = new Map(Dialect.registry);
    Dialect.registry.clear();
  });

  afterEach(() => {
    Dialect.registry.clear();

    for (const [
      k,
      v,
    ] of savedRegistry) {
      Dialect.registry.set(k, v);
    }
  });

  it('string-based lookup throws when dialect is not registered', () => {
    expect(() => parse('SELECT 1', {
      read: 'mysql',
    })).toThrow('Unknown dialect');
  });

  it('class-based works without any registration', () => {
    const [ast] = parse('SELECT 1', {
      read: MySQL,
    });

    expect(ast.sql()).toBe('SELECT 1');
  });

  it('transpile works with class-based dialects', () => {
    const [sql] = transpile('SELECT DATE_SUB(d, INTERVAL 1 DAY) FROM t', {
      read: MySQL,
      write: Postgres,
    });

    expect(sql).toBe('SELECT d - INTERVAL \'1 DAY\' FROM t');
  });

  it('string-based works after Dialect.register()', () => {
    Dialect.register(MySQL);

    const [ast] = parse('SELECT 1', {
      read: 'mysql',
    });

    expect(ast.sql()).toBe('SELECT 1');
  });

  it('registering one dialect does not affect others', () => {
    Dialect.register(MySQL);

    expect(() => parse('SELECT 1', {
      read: 'postgres',
    })).toThrow('Unknown dialect');

    const [ast] = parse('SELECT 1', {
      read: 'mysql',
    });

    expect(ast.sql()).toBe('SELECT 1');
  });

  it('all dialect classes parse SELECT 1', () => {
    for (const DialectClass of ALL_DIALECT_CLASSES) {
      const [ast] = parse('SELECT 1', {
        read: DialectClass,
      });

      expect(ast, `${DialectClass.DIALECT_NAME} parse failed`).toBeDefined();
      expect(
        ast.sql({
          dialect: DialectClass,
        }),
        `${DialectClass.DIALECT_NAME} sql() failed`,
      ).toBe('SELECT 1');
    }
  });

  it('all dialect classes transpile SELECT 1', () => {
    for (const DialectClass of ALL_DIALECT_CLASSES) {
      const [sql] = transpile('SELECT 1', {
        read: DialectClass,
        write: DialectClass,
      });

      expect(sql, `${DialectClass.DIALECT_NAME} transpile failed`).toBe('SELECT 1');
    }
  });

  it('Dialect.register() accepts variadic dialect classes', () => {
    Dialect.register(MySQL, Postgres, DuckDB);

    expect(parse('SELECT 1', {
      read: 'mysql',
    })[0].sql()).toBe('SELECT 1');
    expect(parse('SELECT 1', {
      read: 'postgres',
    })[0].sql()).toBe('SELECT 1');
    expect(parse('SELECT 1', {
      read: 'duckdb',
    })[0].sql()).toBe('SELECT 1');
    expect(() => parse('SELECT 1', {
      read: 'snowflake',
    })).toThrow('Unknown dialect');
  });
});

describe('Optimize', () => {
  it('constant folding', () => {
    expect(optimize('SELECT 1 + 1').sql()).toContain('2');
  });

  it('with schema', () => {
    const result = optimize('SELECT * FROM t WHERE 1 = 1 AND x > 2', {
      schema: {
        t: {
          x: 'INT',
        },
      },
    });

    expect(result.sql()).toContain('x');
  });
});
