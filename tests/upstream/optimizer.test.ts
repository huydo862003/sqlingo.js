import {
  describe, it, expect, beforeEach,
} from 'vitest';
import { parseOne } from '../../src/index';
import {
  Expression, SelectExpr,
  AnonymousExpr, DataTypeExpr, DataTypeExprKind, ColumnExpr, WhereExpr,
  BinaryExpr,
  CaseExpr,
} from '../../src/expressions';
import {
  OptimizeError,
} from '../../src/errors';
import type { Schema } from '../../src/schema';
import { MappingSchema } from '../../src/schema';
import { annotateTypes } from '../../src/optimizer/annotate_types';
import {
  normalize,
} from '../../src/optimizer/normalize';
import {
  buildScope, traverseScope, walkInScope,
} from '../../src/optimizer/scope';
import {
  optimize,
} from '../../src/optimizer/optimizer';
import {
  qualify,
} from '../../src/optimizer/qualify';
import {
  qualifyColumns, validateQualifyColumns, quoteIdentifiers, pushdownCteAliasColumns,
} from '../../src/optimizer/qualify_columns';
import {
  qualifyTables,
} from '../../src/optimizer/qualify_tables';
import {
  simplify,
} from '../../src/optimizer/simplify';
import {
  pushdownProjections,
} from '../../src/optimizer/pushdown_projections';
import {
  pushdownPredicates,
} from '../../src/optimizer/pushdown_predicates';
import {
  unnestSubqueries,
} from '../../src/optimizer/unnest_subqueries';
import {
  optimizeJoins,
} from '../../src/optimizer/optimize_joins';
import {
  eliminateJoins,
} from '../../src/optimizer/eliminate_joins';
import {
  eliminateCtes,
} from '../../src/optimizer/eliminate_ctes';
import {
  mergeSubqueries,
} from '../../src/optimizer/merge_subqueries';
import {
  eliminateSubqueries,
} from '../../src/optimizer/eliminate_subqueries';
import {
  canonicalize,
} from '../../src/optimizer/canonicalize';
import {
  normalizeIdentifiers,
} from '../../src/optimizer/normalize_identifiers';
import {
  isolateTableSelects,
} from '../../src/optimizer/isolate_table_selects';
import type { Dialects } from '../../src/dialects';
import { narrowInstanceOf } from '../../src/port_internals';
import {
  loadSqlFixturePairs, loadSqlFixtures, stringToBool, tpchSchema, tpcdsSchema,
} from './helpers';

function parseAndOptimize (
  func: (expr: Expression, opts?: Record<string, unknown>) => Expression,
  sql: string,
  readDialect?: string,
  kwargs: Record<string, unknown> = {},
): Expression {
  return func(parseOne(sql, { dialect: readDialect }), kwargs);
}

function qualifyColumnsHelper (
  expression: Expression,
  kwargs: {
    validateQualifyColumns?: boolean;
    [index: string]: unknown;
  } = {},
): Expression {
  const {
    validateQualifyColumns = true,
    ...restKwargs
  } = kwargs;
  expression = qualify(expression, {
    inferSchema: true,
    identify: false,
    validateQualifyColumns,
    ...restKwargs,
  });
  return expression;
}

function pushdownProjectionsHelper (
  expression: Expression,
  kwargs: Record<string, unknown> = {},
): Expression {
  expression = qualifyTables(expression);
  expression = qualifyColumns(expression, {
    inferSchema: true,
    ...kwargs,
  });
  expression = pushdownProjections(expression, kwargs);
  return expression;
}

function normalizeHelper (
  expression: Expression,
  options: {
    schema?: Schema | Record<string, unknown>;
    dialect?: Dialects;
    [index: string]: unknown;
  } = {},
): Expression {
  const schema = options.schema;
  expression = normalize(expression, { dnf: false });
  expression = annotateTypes(expression, { schema });
  return simplify(expression);
}

function simplifyHelper (
  expression: Expression,
  options: {
    schema?: Schema | Record<string, unknown>;
    dialect?: Dialects;
    [index: string]: unknown;
  } = {},
): Expression {
  const dialect = options.dialect;
  const schema = options.schema;
  expression = annotateTypes(expression, {
    schema,
    dialect,
  });
  return simplify(expression, {
    constantPropagation: true,
    coalesceSimplification: true,
    dialect,
  });
}

function pushdownCtesHelper (
  expression: Expression,
  _options: Record<string, unknown> = {},
): Expression {
  const scope = buildScope(expression);
  if (scope) {
    pushdownCteAliasColumns(scope);
  }
  return expression;
}

function annotateFunctionsHelper (
  expression: Expression,
  options: {
    schema?: Schema | Record<string, unknown>;
    dialect?: Dialects;
    [index: string]: unknown;
  } = {},
): Expression {
  const dialect = options.dialect;
  const schema = options.schema;
  const annotated = annotateTypes(expression, {
    dialect,
    schema,
  });
  return (annotated as SelectExpr).args.expressions?.[0] ?? annotated;
}

// Generic fixture test helper
function checkFile (
  file: string,
  func: (expr: Expression, opts?: Record<string, unknown>) => Expression,
  options: {
    pretty?: boolean;
    schema?: MappingSchema | Record<string, Record<string, string>>;
    db?: string;
    catalog?: string;
    dialect?: string;
    [key: string]: unknown;
  } = {},
): void {
  const {
    pretty = false,
    schema,
    ...restOptions
  } = options;

  for (const [
    meta,
    sql,
    expected,
  ] of loadSqlFixturePairs(`optimizer/${file}.sql`)) {
    const dialect = meta.dialect || options.dialect;
    const funcKwargs: Record<string, unknown> = { ...restOptions };

    if (schema !== undefined) funcKwargs.schema = schema;
    if (dialect) funcKwargs.dialect = dialect;

    if (meta.leaveTablesIsolated !== undefined) {
      funcKwargs.leaveTablesIsolated = stringToBool(meta.leaveTablesIsolated);
    }
    if (meta.validateQualifyColumns !== undefined) {
      funcKwargs.validateQualifyColumns = stringToBool(meta.validateQualifyColumns);
    }
    if (meta.canonicalizeTableAliases !== undefined) {
      funcKwargs.canonicalizeTableAliases = stringToBool(meta.canonicalizeTableAliases);
    }

    const optimized = parseAndOptimize(func, sql, dialect, funcKwargs);
    const actual = optimized.sql({
      pretty,
      dialect,
    });

    expect(actual, `${sql}`).toBe(expected);
  }
}

describe('TestOptimizer', () => {
  let schema: Record<string, Record<string, string>>;

  beforeEach(() => {
    schema = {
      x: {
        a: 'INT',
        b: 'INT',
      },
      y: {
        b: 'INT',
        c: 'INT',
      },
      z: {
        b: 'INT',
        c: 'INT',
      },
      w: {
        d: 'TEXT',
        e: 'TEXT',
      },
      temporal: {
        d: 'DATE',
        t: 'DATETIME',
      },
      structs: {
        one: 'STRUCT<a_1 INT, b_1 VARCHAR>',
        nested_0: 'STRUCT<a_1 INT, nested_1 STRUCT<a_2 INT, nested_2 STRUCT<a_3 INT>>>',
        quoted: 'STRUCT<"foo bar" INT>',
      },
      t_bool: {
        a: 'BOOLEAN',
      },
    };
  });

  it('test_optimize', () => {
    expect(optimize('x = 1 + 1', { identify: false }).sql()).toBe('x = 2');
  });

  it('test_isolate_table_selects', () => {
    checkFile('isolate_table_selects', (e, opts) => isolateTableSelects(e, opts), { schema });
  });

  it('test_qualify_tables', () => {
    const tables = new Set<string>();
    qualify(
      parseOne('with foo AS (select * from bar) select * from foo join baz'),
      {
        qualifyColumns: false,
        onQualify: (t) => tables.add(t.name),
      },
    );
    expect(tables).toEqual(new Set(['bar', 'baz']));

    expect(
      qualify(
        parseOne('WITH tesT AS (SELECT * FROM t1) SELECT * FROM test', { dialect: 'bigquery' }),
        {
          db: 'db',
          catalog: 'catalog',
          dialect: 'bigquery',
          quoteIdentifiers: false,
        },
      ).sql({ dialect: 'bigquery' }),
    ).toBe('WITH test AS (SELECT * FROM catalog.db.t1 AS t1) SELECT * FROM test AS test');

    expect(
      qualifyTables(
        parseOne('WITH cte AS (SELECT * FROM t) SELECT * FROM cte PIVOT(SUM(c) FOR v IN (\'x\', \'y\'))'),
        {
          db: 'db',
          catalog: 'catalog',
        },
      ).sql(),
    ).toBe('WITH cte AS (SELECT * FROM catalog.db.t AS t) SELECT * FROM cte AS cte PIVOT(SUM(c) FOR v IN (\'x\', \'y\')) AS _0');

    expect(
      qualifyTables(
        parseOne('WITH cte AS (SELECT * FROM t) SELECT * FROM cte PIVOT(SUM(c) FOR v IN (\'x\', \'y\')) AS pivot_alias'),
        {
          db: 'db',
          catalog: 'catalog',
        },
      ).sql(),
    ).toBe('WITH cte AS (SELECT * FROM catalog.db.t AS t) SELECT * FROM cte AS cte PIVOT(SUM(c) FOR v IN (\'x\', \'y\')) AS pivot_alias');

    expect(
      qualifyTables(parseOne('select a from b'), { catalog: 'catalog' }).sql(),
    ).toBe('SELECT a FROM b AS b');

    expect(
      qualifyTables(parseOne('select a from b'), { db: '"DB"' }).sql(),
    ).toBe('SELECT a FROM "DB".b AS b');

    checkFile('qualify_tables', (e, opts) => qualifyTables(e, opts), {
      db: 'db',
      catalog: 'c',
    });
  });

  it('test_normalize', () => {
    expect(
      normalize(parseOne('x AND (y OR z)'), { dnf: true }).sql(),
    ).toBe('(x AND y) OR (x AND z)');

    expect(
      normalize(parseOne('x AND (y OR z)')).sql(),
    ).toBe('x AND (y OR z)');

    checkFile('normalize', normalizeHelper, { schema });
  });

  it('test_qualify_columns', () => {
    expect(
      qualify(
        parseOne(`
          SELECT Teams.Name, count(*)
          FROM raw.TeamMemberships as TeamMemberships
          join raw.Teams
              on Teams.Id = TeamMemberships.TeamId
          GROUP BY 1
        `, { dialect: 'bigquery' }),
        {
          schema: {
            raw: {
              TeamMemberships: {
                Id: 'INTEGER',
                UserId: 'INTEGER',
                TeamId: 'INTEGER',
              },
              Teams: {
                Id: 'INTEGER',
                Name: 'STRING',
              },
            },
          },
          dialect: 'bigquery',
        },
      ).sql({ dialect: 'bigquery' }),
    ).toBe('SELECT `teams`.`name` AS `name`, count(*) AS `_col_1` FROM `raw`.`TeamMemberships` AS `teammemberships` JOIN `raw`.`Teams` AS `teams` ON `teams`.`id` = `teammemberships`.`teamid` GROUP BY `teams`.`name`');

    expect(
      qualify(
        parseOne('SELECT `my_db.my_table`.`my_column` FROM `my_db.my_table`', { dialect: 'bigquery' }),
        { dialect: 'bigquery' },
      ).sql({ dialect: 'bigquery' }),
    ).toBe('SELECT `my_table`.`my_column` AS `my_column` FROM `my_db.my_table` AS `my_table`');

    expect(
      qualifyColumns(
        parseOne('WITH RECURSIVE t AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM t AS child WHERE x < 10) SELECT * FROM t'),
        {
          schema: {},
          inferSchema: false,
        },
      ).sql(),
    ).toBe('WITH RECURSIVE t AS (SELECT 1 AS x UNION ALL SELECT child.x + 1 AS _col_0 FROM t AS child WHERE child.x < 10) SELECT t.x AS x FROM t');

    expect(
      qualifyColumns(
        parseOne('WITH x AS (SELECT a FROM db.y) SELECT * FROM db.x'),
        {
          schema: {
            db: {
              x: { z: 'int' },
              y: { a: 'int' },
            },
          },
          expandStars: false,
        },
      ).sql(),
    ).toBe('WITH x AS (SELECT y.a AS a FROM db.y) SELECT * FROM db.x');

    expect(
      qualifyColumns(
        parseOne('WITH x AS (SELECT a FROM db.y) SELECT z FROM db.x'),
        {
          schema: {
            db: {
              x: { z: 'int' },
              y: { a: 'int' },
            },
          },
          inferSchema: false,
        },
      ).sql(),
    ).toBe('WITH x AS (SELECT y.a AS a FROM db.y) SELECT x.z AS z FROM db.x');

    expect(
      qualifyColumns(
        parseOne('select y from x'),
        {
          schema: {},
          inferSchema: false,
        },
      ).sql(),
    ).toBe('SELECT y AS y FROM x');

    expect(
      qualify(
        parseOne('WITH X AS (SELECT Y.A FROM DB.y CROSS JOIN a.b.INFORMATION_SCHEMA.COLUMNS) SELECT `A` FROM X', { dialect: 'bigquery' }),
        { dialect: 'bigquery' },
      ).sql(),
    ).toBe('WITH "x" AS (SELECT "y"."a" AS "a" FROM "DB"."y" AS "y" CROSS JOIN "a"."b"."INFORMATION_SCHEMA.COLUMNS" AS "columns") SELECT "x"."a" AS "a" FROM "x" AS "x"');

    expect(
      qualify(
        parseOne('CREATE FUNCTION udfs.`myTest`(`x` FLOAT64) AS (1)', { dialect: 'bigquery' }),
        { dialect: 'bigquery' },
      ).sql({ dialect: 'bigquery' }),
    ).toBe('CREATE FUNCTION `udfs`.`myTest`(`x` FLOAT64) AS (1)');

    expect(
      qualify(
        parseOne('SELECT `bar_bazfoo_$id` FROM test', { dialect: 'spark' }),
        {
          schema: { test: { bar_bazFoo_$id: 'BIGINT' } },
          dialect: 'spark',
        },
      ).sql({ dialect: 'spark' }),
    ).toBe('SELECT `test`.`bar_bazfoo_$id` AS `bar_bazfoo_$id` FROM `test` AS `test`');

    const qualified = qualify(
      parseOne('WITH t AS (SELECT 1 AS c) (SELECT c FROM t)', { into: SelectExpr }),
    );
    expect(qualified.selects[0].parent).toBe(qualified);
    expect(qualified.sql()).toBe('WITH "t" AS (SELECT 1 AS "c") SELECT "t"."c" AS "c" FROM "t" AS "t"');

    // can't coalesce USING columns because they don't exist in every already-joined table
    expect(
      qualifyColumns(
        parseOne(
          'SELECT id, dt, v FROM (SELECT t1.id, t1.dt, sum(coalesce(t2.v, 0)) AS v FROM t1 AS t1 LEFT JOIN lkp AS lkp USING (id) LEFT JOIN t2 AS t2 USING (other_id, dt, common) WHERE t1.id > 10 GROUP BY 1, 2) AS `_0`',
          { dialect: 'bigquery' },
        ),
        {
          schema: new MappingSchema({
            schema: {
              t1: {
                id: 'int64',
                dt: 'date',
                common: 'int64',
              },
              lkp: {
                id: 'int64',
                other_id: 'int64',
                common: 'int64',
              },
              t2: {
                other_id: 'int64',
                dt: 'date',
                v: 'int64',
                common: 'int64',
              },
            },
            dialect: 'bigquery',
          }),
        },
      ).sql({ dialect: 'bigquery' }),
    ).toBe('SELECT `_0`.id AS id, `_0`.dt AS dt, `_0`.v AS v FROM (SELECT t1.id AS id, t1.dt AS dt, sum(coalesce(t2.v, 0)) AS v FROM t1 AS t1 LEFT JOIN lkp AS lkp ON t1.id = lkp.id LEFT JOIN t2 AS t2 ON lkp.other_id = t2.other_id AND t1.dt = t2.dt AND COALESCE(t1.common, lkp.common) = t2.common WHERE t1.id > 10 GROUP BY t1.id, t1.dt) AS `_0`');

    // Detection of correlation where columns are referenced in derived tables nested within subqueries
    expect(
      qualify(
        parseOne('SELECT a.g FROM a WHERE a.e < (SELECT MAX(u) FROM (SELECT SUM(c.b) AS u FROM c WHERE  c.d = f GROUP BY c.e) w)'),
        {
          schema: {
            a: {
              g: 'INT',
              e: 'INT',
              f: 'INT',
            },
            c: {
              d: 'INT',
              e: 'INT',
              b: 'INT',
            },
          },
          quoteIdentifiers: false,
        },
      ).sql(),
    ).toBe('SELECT a.g AS g FROM a AS a WHERE a.e < (SELECT MAX(w.u) AS _col_0 FROM (SELECT SUM(c.b) AS u FROM c AS c WHERE c.d = a.f GROUP BY c.e) AS w)');

    // Detection of correlation where columns are referenced in derived tables nested within lateral joins
    expect(
      qualify(
        parseOne('SELECT u.user_id, l.log_date FROM users AS u CROSS JOIN LATERAL (SELECT l1.log_date FROM (SELECT l.log_date FROM logs AS l WHERE l.user_id = u.user_id AND l.log_date <= 100 ORDER BY l.log_date LIMIT 1) AS l1) AS l', { dialect: 'postgres' }),
        {
          schema: {
            users: {
              user_id: 'text',
              log_date: 'date',
            },
            logs: {
              user_id: 'text',
              log_date: 'date',
            },
          },
          quoteIdentifiers: false,
        },
      ).sql({ dialect: 'postgres' }),
    ).toBe('SELECT u.user_id AS user_id, l.log_date AS log_date FROM users AS u CROSS JOIN LATERAL (SELECT l1.log_date AS log_date FROM (SELECT l.log_date AS log_date FROM logs AS l WHERE l.user_id = u.user_id AND l.log_date <= 100 ORDER BY l.log_date LIMIT 1) AS l1) AS l');

    expect(
      qualify(
        parseOne('SELECT A.b_id FROM A JOIN B ON A.b_id=B.b_id JOIN C USING(c_id)', { dialect: 'postgres' }),
        {
          schema: {
            A: { b_id: 'int' },
            B: {
              b_id: 'int',
              c_id: 'int',
            },
            C: { c_id: 'int' },
          },
          quoteIdentifiers: false,
        },
      ).sql({ dialect: 'postgres' }),
    ).toBe('SELECT a.b_id AS b_id FROM a AS a JOIN b AS b ON a.b_id = b.b_id JOIN c AS c ON b.c_id = c.c_id');

    expect(
      qualify(
        parseOne('SELECT A.b_id FROM A JOIN B ON A.b_id=B.b_id JOIN C ON B.b_id = C.b_id JOIN D USING(d_id)', { dialect: 'postgres' }),
        {
          schema: {
            A: { b_id: 'int' },
            B: {
              b_id: 'int',
              d_id: 'int',
            },
            C: { b_id: 'int' },
            D: { d_id: 'int' },
          },
          quoteIdentifiers: false,
        },
      ).sql({ dialect: 'postgres' }),
    ).toBe('SELECT a.b_id AS b_id FROM a AS a JOIN b AS b ON a.b_id = b.b_id JOIN c AS c ON b.b_id = c.b_id JOIN d AS d ON b.d_id = d.d_id');

    expect(
      qualify(
        parseOne(`
          SELECT
            (SELECT SUM(c.amount)
             FROM UNNEST(credits) AS c
             WHERE type != 'promotion') as total
          FROM billing
        `, { dialect: 'bigquery' }),
        {
          schema: { billing: { credits: 'ARRAY<STRUCT<amount FLOAT64, type STRING>>' } },
          dialect: 'bigquery',
        },
      ).sql({ dialect: 'bigquery' }),
    ).toBe('SELECT (SELECT SUM(`c`.`amount`) AS `_col_0` FROM UNNEST(`billing`.`credits`) AS `c` WHERE `type` <> \'promotion\') AS `total` FROM `billing` AS `billing`');

    expect(
      qualify(
        parseOne(`
          WITH cte AS (SELECT * FROM base_table)
          SELECT
            (SELECT SUM(item.price)
             FROM UNNEST(items) AS item
             WHERE category = 'electronics') as electronics_total
          FROM cte
        `, { dialect: 'bigquery' }),
        {
          schema: {
            base_table: {
              id: 'INT64',
              items: 'ARRAY<STRUCT<price FLOAT64, category STRING>>',
            },
          },
          dialect: 'bigquery',
        },
      ).sql({ dialect: 'bigquery' }),
    ).toBe('WITH `cte` AS (SELECT `base_table`.`id` AS `id`, `base_table`.`items` AS `items` FROM `base_table` AS `base_table`) SELECT (SELECT SUM(`item`.`price`) AS `_col_0` FROM UNNEST(`cte`.`items`) AS `item` WHERE `category` = \'electronics\') AS `electronics_total` FROM `cte` AS `cte`');

    checkFile('qualify_columns', qualifyColumnsHelper, { schema });
    checkFile('qualify_columns_ddl', qualifyColumnsHelper, { schema });

    expect(
      qualify(
        parseOne(`
          SELECT
          (
              SELECT
              col_st.value
              FROM UNNEST(col_st) AS col_st
          ) AS vcol1
          FROM t AS b
        `, { dialect: 'bigquery' }),
        {
          schema: {
            t: {
              col_st: 'ARRAY<STRUCT<key STRING, value INT>>',
            },
          },
          dialect: 'bigquery',
        },
      ).sql({ dialect: 'bigquery' }),
    ).toBe('SELECT (SELECT `col_st`.`value` AS `value` FROM UNNEST(`b`.`col_st`) AS `col_st`) AS `vcol1` FROM `t` AS `b`');
  });

  it('test_validate_columns', () => {
    expect(() => {
      qualify(
        parseOne('select foo from x'),
        { schema: { foo: { y: 'int' } } },
      );
    }).toThrow(OptimizeError);

    // Test ambiguous columns error with PIVOT
    expect(() => {
      const expression = parseOne('SELECT * FROM (SELECT a, b, c FROM x) PIVOT (SUM(b) FOR c IN (\'x\', \'y\'))');
      const q = qualifyColumns(expression, {
        schema: {
          x: {
            a: 'int',
            b: 'int',
            c: 'str',
          },
        },
      });
      validateQualifyColumns(q);
    }).toThrow(OptimizeError);
  });

  it('test_qualify_columns__with_invisible', () => {
    const s = new MappingSchema({
      schema: schema,
      visible: {
        x: new Set(['a']),
        y: new Set(['b']),
        z: new Set(['b']),
      },
    });
    checkFile('qualify_columns__with_invisible', qualifyColumnsHelper, { schema: s });
  });

  it('test_pushdown_cte_alias_columns', () => {
    checkFile('pushdown_cte_alias_columns', pushdownCtesHelper);
  });

  it('test_qualify_columns__invalid', () => {
    for (const sql of loadSqlFixtures('optimizer/qualify_columns__invalid.sql')) {
      expect(() => {
        const expression = qualifyColumns(
          parseOne(sql),
          { schema },
        );
        validateQualifyColumns(expression);
      }).toThrow(OptimizeError);
    }
  });

  it('test_optimize_error_highlighting', () => {
    const ANSI_UNDERLINE = '\x1b[4m';
    const ANSI_RESET = '\x1b[0m';
    const sql = 'SELECT nonexistent FROM x';

    expect(() => {
      optimize(sql, {
        schema,
        sql,
      });
    }).toThrow(OptimizeError);

    try {
      optimize(sql, {
        schema,
        sql,
      });
    } catch (e: unknown) {
      const msg = (e as Error).message;
      expect(msg).toContain('Column \'nonexistent\' could not be resolved');
      expect(msg).toContain(`${ANSI_UNDERLINE}nonexistent${ANSI_RESET}`);
    }

    // no highlighting when sql is undefined
    try {
      optimize(sql, { schema });
    } catch (e: unknown) {
      const msg = (e as Error).message;
      expect(msg).toContain('Column \'nonexistent\' could not be resolved');
      expect(msg).not.toContain(`${ANSI_UNDERLINE}nonexistent${ANSI_RESET}`);
    }
  });

  it('test_normalize_identifiers', () => {
    checkFile('normalize_identifiers', (e, opts) => normalizeIdentifiers(e, opts));
    expect(normalizeIdentifiers('a%').sql()).toBe('"a%"');
  });

  it('test_quote_identifiers', () => {
    checkFile('quote_identifiers', (e, opts) => quoteIdentifiers(e, opts));
  });

  it('test_pushdown_projection', () => {
    checkFile('pushdown_projections', pushdownProjectionsHelper, { schema });
  });

  it('test_simplify', () => {
    checkFile('simplify', simplifyHelper, { schema });

    // Stress test with huge union query
    const unionSql = 'SELECT 1 UNION ALL '.repeat(1000) + 'SELECT 1';
    const expression = parseOne(unionSql);
    expect(simplify(expression).sql()).toBe(unionSql);

    // Ensure simplify mutates the AST properly
    const expr2 = parseOne('SELECT 1 + 2') as SelectExpr;
    simplifyHelper(expr2.args.expressions![0]);
    expect(expr2.sql()).toBe('SELECT 3');

    const expr3 = parseOne('SELECT a, c, b FROM table1 WHERE 1 = 1') as SelectExpr;
    expect(simplifyHelper(simplifyHelper(expr3.args.where!)).sql()).toBe('WHERE TRUE');

    const expr4 = parseOne('TRUE AND TRUE AND TRUE');
    const simplified = simplify(expr4);
    expect(simplified.sql()).toBe('TRUE');

    // CONCAT type preservation
    const concat = parseOne('CONCAT(\'a\', x, \'b\', \'c\')', {
      dialect: 'presto',
    });
    const simplifiedConcat = simplify(concat);

    const safeConcat = parseOne('CONCAT(\'a\', x, \'b\', \'c\')');
    const simplifiedSafeConcat = simplify(safeConcat);

    expect(simplifiedConcat.args.safe).toBe(false);
    expect(simplifiedSafeConcat.args.safe).toBe(true);

    expect(simplifiedConcat.sql({ dialect: 'presto' })).toBe('CONCAT(\'a\', x, \'bc\')');
    expect(simplifiedSafeConcat.sql()).toBe('CONCAT(\'a\', x, \'bc\')');
  });

  it('test_simplify_nested', () => {
    const sql = `
    SELECT x, 1 + 1
    FROM foo
    WHERE x > (((select x + 1 + 1, sum(y + 1 + 1) FROM bar GROUP BY x + 1 + 1)))
    `;

    expect(
      simplify(parseOne(sql)).sql({ pretty: true }),
    ).toBe(parseOne(`
            SELECT x, 2
            FROM foo
            WHERE x > (((
                select x + 1 + 1, sum(y + 2)
                FROM bar
                GROUP BY x + 1 + 1
            )))`).sql({ pretty: true }));
  });

  it('test_unnest_subqueries', () => {
    checkFile('unnest_subqueries', (e) => unnestSubqueries(e));
  });

  it('test_pushdown_predicates', () => {
    checkFile('pushdown_predicates', (e, opts) => pushdownPredicates(e, opts));
  });

  it('test_expand_alias_refs', () => {
    expect(
      optimize('SELECT -99 AS e GROUP BY e').sql(),
    ).toBe('SELECT -99 AS "e" GROUP BY 1');

    expect(
      optimize('SELECT a + 1 AS d, d + 1 AS e FROM x WHERE e > 1 GROUP BY e').sql(),
    ).toBe('SELECT "x"."a" + 1 AS "d", "x"."a" + 1 + 1 AS "e" FROM "x" AS "x" WHERE ("x"."a" + 2) > 1 GROUP BY "x"."a" + 1 + 1');

    const unusedSchema = { l: { c: 'int' } };
    expect(
      qualifyColumns(
        parseOne('SELECT CAST(x AS INT) AS y FROM z AS z'),
        {
          schema: unusedSchema,
          inferSchema: false,
        },
      ).sql(),
    ).toBe('SELECT CAST(x AS INT) AS y FROM z AS z');

    // BigQuery expands overlapping alias only for GROUP BY + HAVING
    const sql = 'WITH data AS (SELECT 1 AS id, 2 AS my_id, \'a\' AS name, \'b\' AS full_name) SELECT id AS my_id, CONCAT(id, name) AS full_name FROM data WHERE my_id = 1 GROUP BY my_id, full_name HAVING my_id = 1';
    expect(
      qualifyColumns(
        parseOne(sql, { dialect: 'bigquery' }),
        {
          schema: new MappingSchema({
            schema: unusedSchema,
            dialect: 'bigquery',
          }),
        },
      ).sql(),
    ).toBe('WITH data AS (SELECT 1 AS id, 2 AS my_id, \'a\' AS name, \'b\' AS full_name) SELECT data.id AS my_id, CONCAT(data.id, data.name) AS full_name FROM data WHERE data.my_id = 1 GROUP BY data.id, CONCAT(data.id, data.name) HAVING data.id = 1');

    // Clickhouse expands overlapping alias across the entire query
    expect(
      qualifyColumns(
        parseOne(sql, { dialect: 'clickhouse' }),
        {
          schema: new MappingSchema({
            schema: unusedSchema,
            dialect: 'clickhouse',
          }),
        },
      ).sql(),
    ).toBe('WITH data AS (SELECT 1 AS id, 2 AS my_id, \'a\' AS name, \'b\' AS full_name) SELECT data.id AS my_id, CONCAT(data.id, data.name) AS full_name FROM data WHERE data.id = 1 GROUP BY data.id, CONCAT(data.id, data.name) HAVING data.id = 1');

    // Edge case: BigQuery shouldn't expand aliases in complex expressions
    const sql2 = 'WITH data AS (SELECT 1 AS id) SELECT FUNC(id) AS id FROM data GROUP BY FUNC(id)';
    expect(
      qualifyColumns(
        parseOne(sql2, { dialect: 'bigquery' }),
        {
          schema: new MappingSchema({
            schema: unusedSchema,
            dialect: 'bigquery',
          }),
        },
      ).sql(),
    ).toBe('WITH data AS (SELECT 1 AS id) SELECT FUNC(data.id) AS id FROM data GROUP BY FUNC(data.id)');

    const sql3 = 'SELECT x.a, max(x.b) as x FROM x AS x GROUP BY 1 HAVING x > 1';
    expect(
      qualifyColumns(
        parseOne(sql3, { dialect: 'bigquery' }),
        {
          schema: new MappingSchema({
            schema: unusedSchema,
            dialect: 'bigquery',
          }),
        },
      ).sql(),
    ).toBe('SELECT x.a AS a, MAX(x.b) AS x FROM x AS x GROUP BY 1 HAVING x > 1');
  });

  it('test_optimize_joins', () => {
    checkFile('optimize_joins', (e) => optimizeJoins(e));
  });

  it('test_eliminate_joins', () => {
    checkFile('eliminate_joins', (e) => eliminateJoins(e), { pretty: true });
  });

  it('test_eliminate_ctes', () => {
    checkFile('eliminate_ctes', (e) => eliminateCtes(e), { pretty: true });
  });

  it('test_merge_subqueries', () => {
    const mergeOptimize = (e: Expression, options?: Record<string, unknown>): Expression => {
      return optimize(e.sql(), {
        rules: [
          (expr: Expression, opts?: Record<string, unknown>) => qualifyTables(expr, opts),
          (expr: Expression, opts?: Record<string, unknown>) => qualifyColumns(expr, opts),
          (expr: Expression, opts?: Record<string, unknown>) => mergeSubqueries(expr, opts),
        ],
        ...options,
      });
    };
    checkFile('merge_subqueries', mergeOptimize, { schema });
  });

  it('test_eliminate_subqueries', () => {
    checkFile('eliminate_subqueries', (e) => eliminateSubqueries(e));
  });

  it('test_canonicalize', () => {
    const canonicalizeOptimize = (e: Expression, options?: Record<string, unknown>): Expression => {
      const {
        schema, dialect,
      } = options || {};
      return optimize(e, {
        rules: [
          (expr: Expression, options?: Record<string, unknown>) => qualify(expr, options),
          (expr: Expression, options?: Record<string, unknown>) => quoteIdentifiers(expr, options),
          (expr: Expression, options?: Record<string, unknown>) => annotateTypes(expr, options),
          (expr: Expression, options?: Record<string, unknown>) => canonicalize(expr, options),
        ],
        schema,
        dialect: dialect as string | undefined,
      });
    };
    checkFile('canonicalize', canonicalizeOptimize, { schema });

    // In T-SQL and Redshift, SELECT a + b can produce a NULL
    const ast = canonicalizeOptimize(parseOne('SELECT CAST(a AS TEXT) + CAST(b AS TEXT) FROM t', { dialect: 'tsql' }));
    expect(ast.sql({ dialect: 'postgres' })).toBe('SELECT CAST("t"."a" AS TEXT) || CAST("t"."b" AS TEXT) AS "_col_0" FROM "t" AS "t"');
  });

  it('test_tpch', () => {
    checkFile('tpc-h/tpc-h', (e, opts) => optimize(e, {
      schema: tpchSchema,
      ...opts,
    }), {
      pretty: true,
      schema: tpchSchema,
    });
  });

  it('test_tpcds', () => {
    checkFile('tpc-ds/tpc-ds', (e, opts) => optimize(e, {
      schema: tpcdsSchema,
      ...opts,
    }), {
      pretty: true,
      schema: tpcdsSchema,
    });
  }, 30000);

  it('test_scope', () => {
    const ast = parseOne('SELECT IF(a IN UNNEST(b), 1, 0) AS c FROM t', { dialect: 'bigquery' });
    const scope = buildScope(ast);
    expect(scope?.columns.map((c) => c.sql())).toEqual(['a', 'b']);

    const manyUnions = parseOne(Array.from({ length: 10000 }, () => 'SELECT x FROM t').join(' UNION ALL '));
    const scopesUsingTraverse = [...(buildScope(manyUnions)?.traverse() ?? [])];
    const scopesUsingTraverseScope = traverseScope(manyUnions);
    expect(scopesUsingTraverse.length).toBe(scopesUsingTraverseScope.length);

    const sql = `
      WITH q AS (
        SELECT x.b FROM x
      ), r AS (
        SELECT y.b FROM y
      ), z as (
        SELECT cola, colb FROM (VALUES(1, 'test')) AS tab(cola, colb)
      )
      SELECT
        r.b,
        s.b
      FROM r
      JOIN (
        SELECT y.c AS b FROM y
      ) s
      ON s.b = r.b
      WHERE s.b > (SELECT MAX(x.a) FROM x WHERE x.b = s.b)
    `;
    const expression = parseOne(sql);

    for (const scopes of [traverseScope(expression), [...(buildScope(expression)?.traverse() ?? [])]]) {
      expect(scopes.length).toBe(7);
      expect(scopes[0].expression.sql()).toBe('SELECT x.b FROM x');
      expect(scopes[1].expression.sql()).toBe('SELECT y.b FROM y');
      expect(scopes[2].expression.sql()).toBe('(VALUES (1, \'test\')) AS tab(cola, colb)');
      expect(scopes[3].expression.sql()).toBe('SELECT cola, colb FROM (VALUES (1, \'test\')) AS tab(cola, colb)');
      expect(scopes[4].expression.sql()).toBe('SELECT y.c AS b FROM y');
      expect(scopes[5].expression.sql()).toBe('SELECT MAX(x.a) FROM x WHERE x.b = s.b');
      expect(scopes[6].expression.sql()).toBe(parseOne(sql).sql());

      expect(new Set(scopes[6].sources.keys())).toEqual(new Set([
        'q',
        'z',
        'r',
        's',
      ]));
      expect(scopes[6].columns.length).toBe(6);
      expect(new Set(scopes[6].columns.map((c) => c.table))).toEqual(new Set(['r', 's']));
      expect(scopes[6].sourceColumns('q')).toEqual([]);
      expect(scopes[6].sourceColumns('r').length).toBe(2);
      expect(new Set(scopes[6].sourceColumns('r').map((c) => c.table))).toEqual(new Set(['r']));

      expect(new Set([...scopes[scopes.length - 1].findAll(ColumnExpr)].map((c) => c.sql()))).toEqual(new Set(['r.b', 's.b']));
      expect(scopes[scopes.length - 1].find(ColumnExpr)?.sql()).toBe('r.b');
      expect(new Set([...scopes[0].findAll(ColumnExpr)].map((c) => c.sql()))).toEqual(new Set(['x.b']));
    }

    // Check that we can walk in scope from an arbitrary node
    const whereNode = expression.find(WhereExpr);
    if (whereNode) {
      const colsInWhere = new Set(
        [...walkInScope(whereNode)]
          .filter((node): node is ColumnExpr => node instanceof ColumnExpr)
          .map((node) => node.sql()),
      );
      expect(colsInWhere).toEqual(new Set(['s.b']));
    }

    // Check that parentheses don't introduce a new scope unless an alias is attached
    const sql2 = 'SELECT * FROM (((SELECT * FROM (t1 JOIN t2) AS t3) JOIN (SELECT * FROM t4)))';
    const expr2 = parseOne(sql2);
    for (const scopes of [traverseScope(expr2), [...(buildScope(expr2)?.traverse() ?? [])]]) {
      expect(scopes.length).toBe(4);

      expect(scopes[0].expression.sql()).toBe('t1, t2');
      expect(new Set(scopes[0].sources.keys())).toEqual(new Set(['t1', 't2']));

      expect(scopes[1].expression.sql()).toBe('SELECT * FROM (t1, t2) AS t3');
      expect(new Set(scopes[1].sources.keys())).toEqual(new Set(['t3']));

      expect(scopes[2].expression.sql()).toBe('SELECT * FROM t4');
      expect(new Set(scopes[2].sources.keys())).toEqual(new Set(['t4']));

      expect(scopes[3].expression.sql()).toBe('SELECT * FROM (((SELECT * FROM (t1, t2) AS t3), (SELECT * FROM t4)))');
      expect(new Set(scopes[3].sources.keys())).toEqual(new Set(['']));
    }

    // UNNEST and LATERAL inner query
    const innerQuery = 'SELECT bar FROM baz';
    for (const udtf of [`UNNEST((${innerQuery}))`, `LATERAL (${innerQuery})`]) {
      const sqlU = `SELECT a FROM foo CROSS JOIN ${udtf}`;
      const exprU = parseOne(sqlU);

      for (const scopes of [traverseScope(exprU), [...(buildScope(exprU)?.traverse() ?? [])]]) {
        expect(scopes.length).toBe(3);
        expect(scopes[0].expression.sql()).toBe(innerQuery);
        expect(new Set(scopes[0].sources.keys())).toEqual(new Set(['baz']));
        expect(scopes[1].expression.sql()).toBe(udtf);
        expect(new Set(scopes[1].sources.keys())).toEqual(new Set(['', 'foo']));
        expect(scopes[2].expression.sql()).toBe(`SELECT a FROM foo CROSS JOIN ${udtf}`);
        expect(new Set(scopes[2].sources.keys())).toEqual(new Set(['', 'foo']));
      }
    }

    // DML statement scopes
    expect(
      traverseScope(parseOne('UPDATE customers SET total_spent = (SELECT 1 FROM t1) WHERE EXISTS (SELECT 1 FROM t2)')).length,
    ).toBe(3);

    expect(
      traverseScope(parseOne('UPDATE tbl1 SET col = 1 WHERE EXISTS (SELECT 1 FROM tbl2 WHERE tbl1.id = tbl2.id)')).length,
    ).toBe(1);

    expect(
      traverseScope(parseOne('UPDATE tbl1 SET col = 0')).length,
    ).toBe(0);

    const scope2 = buildScope(parseOne('SELECT * FROM t LEFT JOIN UNNEST(a) AS a1 LEFT JOIN UNNEST(a1.a) AS a2', { dialect: 'bigquery' }));
    expect(new Set(Object.keys(scope2?.selectedSources ?? {}))).toEqual(new Set([
      't',
      'a1',
      'a2',
    ]));
  });

  it('test_annotate_types', () => {
    for (const [
      meta,
      sql,
      expected,
    ] of loadSqlFixturePairs('optimizer/annotate_types.sql')) {
      const dialect = meta.dialect || undefined;
      const result = parseAndOptimize(
        (e, opts) => annotateTypes(e, opts),
        sql,
        dialect,
        { dialect },
      );
      const resultTypeSql = narrowInstanceOf(result.type, Expression)?.sql({ dialect });
      const expectedTypeSql = DataTypeExpr.build(expected, { dialect })?.sql({ dialect });
      expect(resultTypeSql, `${sql}`).toBe(expectedTypeSql);
    }
  });

  it('test_annotate_funcs', () => {
    const testSchema: Record<string, Record<string, string>> = {
      tbl: {
        bin_col: 'BINARY',
        str_col: 'STRING',
        bignum_col: 'BIGNUMERIC',
        date_col: 'DATE',
        decfloat_col: 'DECFLOAT',
        float_col: 'FLOAT',
        timestamp_col: 'TIMESTAMP',
        double_col: 'DOUBLE',
        bigint_col: 'BIGINT',
        obj_col: 'OBJECT',
        int_col: 'INT',
        bool_col: 'BOOLEAN',
        bytes_col: 'BYTES',
        interval_col: 'INTERVAL',
        array_col: 'ARRAY<STRING>',
      },
    };

    for (const [
      meta,
      sql,
      expected,
    ] of loadSqlFixturePairs('optimizer/annotate_functions.sql')) {
      const dialectStr = meta.dialect || '';
      const fullSql = `SELECT ${sql} FROM tbl`;

      for (const dialect of (dialectStr ? dialectStr.split(', ') : [''])) {
        const result = parseAndOptimize(
          (e, opts) => annotateFunctionsHelper(e, opts ?? {}),
          fullSql,
          dialect || undefined,
          {
            schema: testSchema,
            dialect: dialect || undefined,
          },
        );
        const resultTypeSql = narrowInstanceOf(result.type, Expression)?.sql({ dialect });
        const expectedTypeSql = DataTypeExpr.build(expected, { dialect: dialect })?.sql({ dialect });
        expect(resultTypeSql, `${sql}`).toBe(expectedTypeSql);
      }
    }
  });

  it('test_cast_type_annotation', () => {
    let expression = annotateTypes(parseOne('CAST(\'2020-01-01\' AS TIMESTAMPTZ(9))'));
    expect(expression.type?.toString()).toBe(DataTypeExprKind.TIMESTAMPTZ);
    expect(narrowInstanceOf(expression.args.this, Expression)?.type?.toString()).toBe(DataTypeExprKind.VARCHAR);
    expect(narrowInstanceOf(expression.args.to, Expression)?.type?.toString()).toBe(DataTypeExprKind.TIMESTAMPTZ);
    expect(
      narrowInstanceOf(narrowInstanceOf(narrowInstanceOf(expression.args.to, Expression)?.args.expressions?.[0], Expression)?.args.this, Expression)?.type?.toString(),
    ).toBe(DataTypeExprKind.INT);

    expression = annotateTypes(parseOne('ARRAY(1)::ARRAY<INT>'));
    expect(narrowInstanceOf(expression.type, Expression)?.sql()).toBe(parseOne('ARRAY<INT>', { into: DataTypeExpr }).sql());

    expression = annotateTypes(parseOne('CAST(x AS INTERVAL)'));
    expect(expression.type?.toString()).toBe(DataTypeExprKind.INTERVAL);
    expect(narrowInstanceOf(expression.args.this, Expression)?.type?.toString()).toBe(DataTypeExprKind.UNKNOWN);
    expect(narrowInstanceOf(expression.args.to, Expression)?.type?.toString()).toBe(DataTypeExprKind.INTERVAL);
  });

  it('test_binary_annotation', () => {
    const expression = narrowInstanceOf(annotateTypes(parseOne('SELECT 0.0 + (2 + 3)')).args.expressions?.[0], BinaryExpr);

    expect(expression?.type?.toString()).toBe(DataTypeExprKind.DOUBLE);
    expect(narrowInstanceOf(expression?.left, Expression)?.type?.toString()).toBe(DataTypeExprKind.DOUBLE);
    expect(narrowInstanceOf(expression?.right, Expression)?.type?.toString()).toBe(DataTypeExprKind.INT);
    expect(narrowInstanceOf(narrowInstanceOf(expression?.right, Expression)?.args.this, Expression)?.type?.toString()).toBe(DataTypeExprKind.INT);
    expect(narrowInstanceOf(narrowInstanceOf(narrowInstanceOf(expression?.right, Expression)?.args.this, Expression)?.args.this, Expression)?.type?.toString()).toBe(DataTypeExprKind.INT);
    expect(narrowInstanceOf(narrowInstanceOf(narrowInstanceOf(expression?.right, Expression)?.args.this, Expression)?.args.expression, Expression)?.type?.toString()).toBe(DataTypeExprKind.INT);

    for (const numericType of [
      'BIGINT',
      'DOUBLE',
      'INT',
    ]) {
      const q = `SELECT '1' + CAST(x AS ${numericType})`;
      const expr = narrowInstanceOf(annotateTypes(parseOne(q)).args.expressions?.[0], Expression);
      expect(narrowInstanceOf(expr?.type, Expression)?.sql()).toBe(DataTypeExpr.build(numericType)?.sql());
    }
  });

  it('test_typeddiv_annotation', () => {
    const expressions = annotateTypes(parseOne('SELECT 2 / 3, 2 / 3.0', { dialect: 'presto' })).args.expressions;
    expect(narrowInstanceOf(expressions?.[0], Expression)?.type?.toString()).toBe(DataTypeExprKind.BIGINT);
    expect(narrowInstanceOf(expressions?.[1], Expression)?.type?.toString()).toBe(DataTypeExprKind.DOUBLE);

    const expressions2 = annotateTypes(parseOne('SELECT SUM(2 / 3), CAST(2 AS DECIMAL) / 3', { dialect: 'mysql' })).args.expressions;
    expect(narrowInstanceOf(expressions2?.[0], Expression)?.type?.toString()).toBe(DataTypeExprKind.DOUBLE);
    expect(narrowInstanceOf(narrowInstanceOf(expressions2?.[0], Expression)?.args.this, Expression)?.type?.toString()).toBe(DataTypeExprKind.DOUBLE);
    expect(narrowInstanceOf(expressions2?.[1], Expression)?.type?.toString()).toBe(DataTypeExprKind.DECIMAL);
  });

  it('test_bracket_annotation', () => {
    let expression = narrowInstanceOf(annotateTypes(parseOne('SELECT A[:]')).args.expressions?.[0], Expression);
    expect(expression?.type?.toString()).toBe(DataTypeExprKind.UNKNOWN);
    expect((expression?.args.expressions as Expression[])?.[0]?.type?.toString()).toBe(DataTypeExprKind.UNKNOWN);

    expression = narrowInstanceOf(annotateTypes(parseOne('SELECT ARRAY[1, 2, 3][1]')).args.expressions?.[0], Expression);
    expect(narrowInstanceOf(narrowInstanceOf(expression?.args.this, Expression)?.type, Expression)?.sql()).toBe('ARRAY<INT>');
    expect(expression?.type?.toString()).toBe(DataTypeExprKind.INT);

    expression = narrowInstanceOf(annotateTypes(parseOne('SELECT ARRAY[1, 2, 3][1 : 2]')).args.expressions?.[0], Expression);
    expect(narrowInstanceOf(narrowInstanceOf(expression?.args.this, Expression)?.type, Expression)?.sql()).toBe('ARRAY<INT>');
    expect(narrowInstanceOf(expression?.type, Expression)?.sql()).toBe('ARRAY<INT>');

    expression = narrowInstanceOf(annotateTypes(parseOne('SELECT ARRAY[ARRAY[1], ARRAY[2], ARRAY[3]][1][2]')).args.expressions?.[0], Expression);
    expect(narrowInstanceOf(narrowInstanceOf(narrowInstanceOf(expression?.args.this, Expression)?.args.this, Expression)?.type, Expression)?.sql()).toBe('ARRAY<ARRAY<INT>>');
    expect(narrowInstanceOf(narrowInstanceOf(expression?.args.this, Expression)?.type, Expression)?.sql()).toBe('ARRAY<INT>');
    expect(expression?.type?.toString()).toBe(DataTypeExprKind.INT);

    expression = narrowInstanceOf(annotateTypes(parseOne('SELECT ARRAY[ARRAY[1], ARRAY[2], ARRAY[3]][1:2]')).args.expressions?.[0], Expression);
    expect(narrowInstanceOf(expression?.type, Expression)?.sql()).toBe('ARRAY<ARRAY<INT>>');

    expression = annotateTypes(parseOne('MAP(1.0, 2, \'2\', 3.0)[\'2\']', { dialect: 'spark' }));
    expect(expression.type?.toString()).toBe(DataTypeExprKind.DOUBLE);

    expression = annotateTypes(parseOne('MAP(1.0, 2, x, 3.0)[2]', { dialect: 'spark' }));
    expect(expression.type?.toString()).toBe(DataTypeExprKind.UNKNOWN);

    expression = annotateTypes(parseOne('MAP(ARRAY(1.0, x), ARRAY(2, 3.0))[x]'));
    expect(expression.type?.toString()).toBe(DataTypeExprKind.DOUBLE);

    expression = narrowInstanceOf(annotateTypes(
      parseOne('SELECT MAP(1.0, 2, 2, t.y)[2] FROM t', { dialect: 'spark' }),
      { schema: { t: { y: 'int' } } },
    ).args.expressions?.[0], Expression);
    expect(expression?.type?.toString()).toBe(DataTypeExprKind.INT);
  });

  it('test_interval_math_annotation', () => {
    const s = {
      x: {
        a: 'DATE',
        b: 'DATETIME',
      },
    };

    const tests: [string, DataTypeExprKind][] = [
      ['SELECT \'2023-01-01\' + INTERVAL \'1\' DAY', DataTypeExprKind.DATE],
      ['SELECT \'2023-01-01\' + INTERVAL \'1\' HOUR', DataTypeExprKind.DATETIME],
      ['SELECT \'2023-01-01 00:00:01\' + INTERVAL \'1\' HOUR', DataTypeExprKind.DATETIME],
      ['SELECT \'nonsense\' + INTERVAL \'1\' DAY', DataTypeExprKind.UNKNOWN],
      ['SELECT x.a + INTERVAL \'1\' DAY FROM x AS x', DataTypeExprKind.DATE],
      ['SELECT x.a + INTERVAL \'1\' HOUR FROM x AS x', DataTypeExprKind.DATETIME],
      ['SELECT x.b + INTERVAL \'1\' DAY FROM x AS x', DataTypeExprKind.DATETIME],
      ['SELECT x.b + INTERVAL \'1\' HOUR FROM x AS x', DataTypeExprKind.DATETIME],
      ['SELECT DATE_ADD(\'2023-01-01\', 1, \'DAY\')', DataTypeExprKind.DATE],
      ['SELECT DATE_ADD(\'2023-01-01 00:00:00\', 1, \'DAY\')', DataTypeExprKind.DATETIME],
      ['SELECT DATE_ADD(x.a, 1, \'DAY\') FROM x AS x', DataTypeExprKind.DATE],
      ['SELECT DATE_ADD(x.a, 1, \'HOUR\') FROM x AS x', DataTypeExprKind.DATETIME],
      ['SELECT DATE_ADD(x.b, 1, \'DAY\') FROM x AS x', DataTypeExprKind.DATETIME],
      ['SELECT DATE_TRUNC(\'DAY\', x.a) FROM x AS x', DataTypeExprKind.DATE],
      ['SELECT DATE_TRUNC(\'DAY\', x.b) FROM x AS x', DataTypeExprKind.DATETIME],
      ['SELECT DATE_TRUNC(\'SECOND\', x.a) FROM x AS x', DataTypeExprKind.DATETIME],
      ['SELECT DATE_TRUNC(\'DAY\', \'2023-01-01\') FROM x AS x', DataTypeExprKind.DATE],
      ['SELECT DATEDIFF(\'2023-01-01\', \'2023-01-02\', DAY) FROM x AS x', DataTypeExprKind.INT],
    ];

    for (const [sql, expectedType] of tests) {
      const expression = annotateTypes(parseOne(sql), { schema: s });
      expect(narrowInstanceOf(expression.args.expressions?.[0], Expression)?.type?.toString(), sql).toBe(expectedType);
      expect(expression.sql(), sql).toBe(sql);
    }
  });

  it('test_lateral_annotation', () => {
    const expression = narrowInstanceOf(optimize(
      parseOne('SELECT c FROM (select 1 a) as x LATERAL VIEW EXPLODE (a) AS c'),
    ).args.expressions?.[0], Expression);
    expect(expression?.type?.toString()).toBe(DataTypeExprKind.INT);
  });

  it('test_derived_tables_column_annotation', () => {
    const s = {
      x: { cola: 'INT' },
      y: { cola: 'FLOAT' },
    };
    const sql = `
      SELECT a.cola AS cola
      FROM (
          SELECT x.cola + y.cola AS cola
          FROM (
              SELECT x.cola AS cola
              FROM x AS x
          ) AS x
          JOIN (
              SELECT y.cola AS cola
              FROM y AS y
          ) AS y
      ) AS a
    `;

    const expression = annotateTypes(parseOne(sql), { schema: s });
    expect(narrowInstanceOf(expression.args.expressions?.[0], Expression)?.type?.toString()).toBe(DataTypeExprKind.FLOAT);

    const additionAlias = narrowInstanceOf(narrowInstanceOf(narrowInstanceOf(expression.args.from, Expression)?.args.this, Expression)?.args.this, Expression)?.args.expressions?.[0];
    expect(narrowInstanceOf(additionAlias, Expression)?.type?.toString()).toBe(DataTypeExprKind.FLOAT);

    const addition = narrowInstanceOf(narrowInstanceOf(additionAlias, Expression)?.args?.this, Expression);
    expect(addition?.type?.toString()).toBe(DataTypeExprKind.FLOAT);
    expect(narrowInstanceOf(addition?.args?.this, Expression)?.type?.toString()).toBe(DataTypeExprKind.INT);
    expect(narrowInstanceOf(addition?.args?.expression, Expression)?.type?.toString()).toBe(DataTypeExprKind.FLOAT);
  });

  it('test_cte_column_annotation', () => {
    const s = {
      x: { cola: 'CHAR' },
      y: {
        colb: 'TEXT',
        colc: 'BOOLEAN',
      },
    };
    const sql = `
      WITH tbl AS (
          SELECT x.cola + 'bla' AS cola, y.colb AS colb, y.colc AS colc
          FROM (
              SELECT x.cola AS cola
              FROM x AS x
          ) AS x
          JOIN (
              SELECT y.colb AS colb, y.colc AS colc
              FROM y AS y
          ) AS y
      )
      SELECT tbl.cola + tbl.colb + 'foo' AS col
      FROM tbl AS tbl
      WHERE tbl.colc = True
    `;

    const expression = annotateTypes(parseOne(sql), { schema: s });
    expect(narrowInstanceOf(expression.args.expressions?.[0], Expression)?.type?.toString()).toBe(DataTypeExprKind.TEXT);

    const outerAddition = narrowInstanceOf(narrowInstanceOf(expression.args.expressions?.[0], Expression)?.args.this, BinaryExpr);
    expect(outerAddition?.type?.toString()).toBe(DataTypeExprKind.TEXT);
    expect(outerAddition?.left?.type?.toString()).toBe(DataTypeExprKind.TEXT);
    expect(outerAddition?.right?.type?.toString()).toBe(DataTypeExprKind.VARCHAR);

    const innerAddition = (outerAddition)?.left;
    expect(narrowInstanceOf(innerAddition?.args?.this, Expression)?.type?.toString()).toBe(DataTypeExprKind.VARCHAR);
    expect(narrowInstanceOf(innerAddition?.args?.expression, Expression)?.type?.toString()).toBe(DataTypeExprKind.TEXT);

    expect(
      narrowInstanceOf(
        narrowInstanceOf(
          expression.getArgKey('where'),
          Expression,
        )?.args?.this,
        Expression,
      )?.type?.toString(),
    ).toBe(DataTypeExprKind.BOOLEAN);
  });

  it('test_function_annotation', () => {
    const s = {
      x: {
        cola: 'VARCHAR',
        colb: 'CHAR',
      },
    };
    const sql = 'SELECT x.cola || TRIM(x.colb) AS col, DATE(x.colb), DATEFROMPARTS(y, m, d) FROM x AS x';

    const expression = annotateTypes(parseOne(sql), { schema: s });
    const concatExprAlias = narrowInstanceOf(expression.args.expressions?.[0], Expression);
    expect(concatExprAlias?.type?.toString()).toBe(DataTypeExprKind.VARCHAR);

    const concatExpr = narrowInstanceOf(concatExprAlias?.args.this, BinaryExpr);
    expect(concatExpr?.type?.toString()).toBe(DataTypeExprKind.VARCHAR);
    expect(concatExpr?.left?.type?.toString()).toBe(DataTypeExprKind.VARCHAR);
    expect(concatExpr?.right?.type?.toString()).toBe(DataTypeExprKind.VARCHAR);
    expect(narrowInstanceOf(concatExpr?.right?.args?.this, Expression)?.type?.toString()).toBe(DataTypeExprKind.CHAR);

    const dateExpr = narrowInstanceOf(expression?.args.expressions?.[1], Expression);
    expect(dateExpr?.type?.toString()).toBe(DataTypeExprKind.DATE);

    const dateExpr2 = narrowInstanceOf(expression?.args.expressions?.[2], Expression);
    expect(dateExpr2?.type?.toString()).toBe(DataTypeExprKind.DATE);

    const sql2 = 'SELECT CASE WHEN 1=1 THEN x.cola ELSE x.colb END AS col FROM x AS x';
    const caseExprAlias = narrowInstanceOf(annotateTypes(parseOne(sql2), { schema: s }).args.expressions?.[0], Expression);
    expect(caseExprAlias?.type?.toString()).toBe(DataTypeExprKind.VARCHAR);

    const caseExpr = narrowInstanceOf(caseExprAlias?.args.this, CaseExpr);
    expect(caseExpr?.type?.toString()).toBe(DataTypeExprKind.VARCHAR);
    expect(narrowInstanceOf(caseExpr?.args?.default, Expression)?.type?.toString()).toBe(DataTypeExprKind.CHAR);

    const caseIfsExpr = caseExpr?.args?.ifs?.[0];
    expect(caseIfsExpr?.type?.toString()).toBe(DataTypeExprKind.VARCHAR);
    expect(narrowInstanceOf(caseIfsExpr?.getArgKey('true'), Expression)?.type?.toString()).toBe(DataTypeExprKind.VARCHAR);

    const timestamp = annotateTypes(parseOne('TIMESTAMP(x)'));
    expect(timestamp.type?.toString()).toBe(DataTypeExprKind.TIMESTAMP);

    const timestamptz = annotateTypes(parseOne('TIMESTAMP(x)', { dialect: 'bigquery' }));
    expect(timestamptz.type?.toString()).toBe(DataTypeExprKind.TIMESTAMPTZ);
  });

  it('test_unknown_annotation', () => {
    const s = { x: { cola: 'VARCHAR' } };
    const sql = 'SELECT x.cola + SOME_ANONYMOUS_FUNC(x.cola) AS col FROM x AS x';

    const concatExprAlias = narrowInstanceOf(annotateTypes(parseOne(sql), { schema: s }).args.expressions?.[0], Expression);
    expect(concatExprAlias?.type?.toString()).toBe(DataTypeExprKind.UNKNOWN);

    const concatExpr = narrowInstanceOf(concatExprAlias?.args.this, BinaryExpr);
    expect(concatExpr?.type?.toString()).toBe(DataTypeExprKind.UNKNOWN);
    expect(concatExpr?.left?.type?.toString()).toBe(DataTypeExprKind.VARCHAR);
    expect(concatExpr?.right?.type?.toString()).toBe(DataTypeExprKind.UNKNOWN);
    expect(narrowInstanceOf(concatExpr?.right?.args.expressions?.[0], Expression)?.type?.toString()).toBe(DataTypeExprKind.VARCHAR);

    // Ensures we don't raise if there are unqualified columns
    void annotateTypes(parseOne('select x from y lateral view explode(y) as x')).args.expressions?.[0];

    // NULL <op> UNKNOWN should yield UNKNOWN
    expect(
      annotateTypes((parseOne('SELECT NULL + ANONYMOUS_FUNC()')).args.expressions?.[0] as Expression).type?.toString(),
    ).toBe(DataTypeExprKind.UNKNOWN);
  });

  it('test_udf_annotation', () => {
    // Unqualified UDF
    let s = new MappingSchema({
      schema: { t: { col: 'INT' } },
      udfMapping: { my_func: 'VARCHAR' },
    });
    let expr = annotateTypes(parseOne('SELECT my_func(col) FROM t', { into: SelectExpr }), { schema: s });
    expect(expr.selects[0].type?.toString()).toBe(DataTypeExprKind.VARCHAR);

    // Qualified UDF (2-level)
    s = new MappingSchema({
      schema: { db: { t: { col: 'INT' } } },
      udfMapping: { db: { my_func: 'DOUBLE' } },
    });
    expr = annotateTypes(parseOne('SELECT db.my_func(col) FROM db.t', { into: SelectExpr }), { schema: s });
    const anon = expr.selects[0].find(AnonymousExpr);
    expect(anon?.type?.toString()).toBe(DataTypeExprKind.DOUBLE);
    expect(expr.selects[0].type?.toString()).toBe(DataTypeExprKind.DOUBLE);

    // Qualified UDF (3-level)
    s = new MappingSchema({
      schema: { cat: { db: { t: { col: 'INT' } } } },
      udfMapping: { cat: { db: { my_func: 'BOOLEAN' } } },
    });
    expr = annotateTypes(parseOne('SELECT cat.db.my_func(col) FROM cat.db.t', { into: SelectExpr }), { schema: s });
    const anon2 = expr.selects[0].find(AnonymousExpr);
    expect(anon2?.type?.toString()).toBe(DataTypeExprKind.BOOLEAN);

    // Unknown UDF returns UNKNOWN
    s = new MappingSchema({
      schema: { t: { col: 'INT' } },
      udfMapping: { known_func: 'DATE' },
    });
    expr = annotateTypes(parseOne('SELECT unknown_func(col) FROM t', { into: SelectExpr }), { schema: s });
    expect(expr.selects[0].type?.toString()).toBe(DataTypeExprKind.UNKNOWN);

    // Test get_udf_type with string input
    const s2 = new MappingSchema({ udfMapping: { my_func: 'INT' } });
    expect(s2.getUdfType('my_func(x)')?.toString()).toBe(DataTypeExprKind.INT);

    const s3 = new MappingSchema({ udfMapping: { db: { my_func: 'FLOAT' } } });
    expect(s3.getUdfType('db.my_func(x, y)')?.toString()).toBe(DataTypeExprKind.FLOAT);

    const s4 = new MappingSchema({ udfMapping: { cat: { db: { my_func: 'DATE' } } } });
    expect(s4.getUdfType('cat.db.my_func(a, b, c)')?.toString()).toBe(DataTypeExprKind.DATE);

    // Unknown UDF string returns UNKNOWN
    const s5 = new MappingSchema({ udfMapping: { known: 'INT' } });
    expect(s5.getUdfType('unknown(x)')?.toString()).toBe(DataTypeExprKind.UNKNOWN);
  });

  it('test_predicate_annotation', () => {
    let expression = annotateTypes(parseOne('x BETWEEN a AND b'));
    expect(expression.type?.toString()).toBe(DataTypeExprKind.BOOLEAN);

    expression = annotateTypes(parseOne('x IN (a, b, c, d)'));
    expect(expression.type?.toString()).toBe(DataTypeExprKind.BOOLEAN);
  });

  it('test_aggfunc_annotation', () => {
    const s = {
      x: {
        cola: 'SMALLINT',
        colb: 'FLOAT',
        colc: 'TEXT',
        cold: 'DATE',
      },
    };

    const tests: [[string, string], DataTypeExprKind][] = [
      [['AVG', 'cola'], DataTypeExprKind.DOUBLE],
      [['SUM', 'cola'], DataTypeExprKind.BIGINT],
      [['SUM', 'colb'], DataTypeExprKind.DOUBLE],
      [['MIN', 'cola'], DataTypeExprKind.SMALLINT],
      [['MIN', 'colb'], DataTypeExprKind.FLOAT],
      [['MAX', 'colc'], DataTypeExprKind.TEXT],
      [['MAX', 'cold'], DataTypeExprKind.DATE],
      [['COUNT', 'colb'], DataTypeExprKind.BIGINT],
      [['STDDEV', 'cola'], DataTypeExprKind.DOUBLE],
      [['ABS', 'cola'], DataTypeExprKind.SMALLINT],
      [['ABS', 'colb'], DataTypeExprKind.FLOAT],
    ];

    for (const [[func, col], targetType] of tests) {
      const expression = annotateTypes(
        parseOne(`SELECT ${func}(x.${col}) AS _col_0 FROM x AS x`, { into: SelectExpr }),
        { schema: s },
      );
      expect(narrowInstanceOf(expression.args.expressions?.[0], Expression)?.type?.toString(), `${func}(${col})`).toBe(targetType);
    }
  });

  it('test_concat_annotation', () => {
    const expression = annotateTypes(parseOne('CONCAT(\'A\', \'B\')'));
    expect(expression.type?.toString()).toBe(DataTypeExprKind.VARCHAR);
  });

  it('test_root_subquery_annotation', () => {
    const expression = annotateTypes(parseOne('(SELECT 1, 2 FROM x) LIMIT 0'));
    expect(expression.selects[0].type?.toString()).toBe(DataTypeExprKind.INT);
    expect(expression.selects[1].type?.toString()).toBe(DataTypeExprKind.INT);
  });

  it('test_nested_type_annotation', () => {
    const s = {
      order: {
        customer_id: 'bigint',
        item_id: 'bigint',
        item_price: 'numeric',
      },
    };
    const sql = `
      SELECT ARRAY_AGG(DISTINCT order.item_id) FILTER (WHERE order.item_price > 10) AS items,
      FROM order AS order
      GROUP BY order.customer_id
    `;
    const expression = annotateTypes(parseOne(sql, { into: SelectExpr }), { schema: s });
    expect(expression.selects[0].type?.toString()).toBe(DataTypeExprKind.ARRAY);
    expect(narrowInstanceOf(expression.selects[0].type, Expression)?.sql()).toBe('ARRAY<BIGINT>');

    const expr2 = annotateTypes(parseOne('SELECT ARRAY_CAT(ARRAY[1,2,3], ARRAY[4,5])', {
      into: SelectExpr,
      dialect: 'postgres',
    }));
    expect(expr2.selects[0].type?.toString()).toBe(DataTypeExprKind.ARRAY);
    expect(narrowInstanceOf(expr2.selects[0].type, Expression)?.sql()).toBe('ARRAY<INT>');

    const s2 = new MappingSchema({
      schema: { t: { c: 'STRUCT<`f` STRING>' } },
      dialect: 'bigquery',
    });
    const expr3 = annotateTypes(parseOne('SELECT t.c, [t.c] FROM t', { into: SelectExpr }), { schema: s2 });
    expect(narrowInstanceOf(expr3.selects[0].type, Expression)?.sql({ dialect: 'bigquery' })).toBe('STRUCT<`f` STRING>');
    expect(narrowInstanceOf(expr3.selects[1].type, Expression)?.sql({ dialect: 'bigquery' })).toBe('ARRAY<STRUCT<`f` STRING>>');

    const expr4 = annotateTypes(
      parseOne('SELECT unnest(t.x) FROM t AS t', {
        dialect: 'postgres',
        into: SelectExpr,
      }),
      { schema: { t: { x: 'array<int>' } } },
    );
    expect(expr4.selects[0].isType('int')).toBe(true);
  });

  it('test_type_annotation_cache', () => {
    const sql = 'SELECT 1 + 1';
    let expression = annotateTypes(parseOne(sql, { into: SelectExpr }));
    expect(expression.selects[0].type?.toString()).toBe(DataTypeExprKind.INT);

    narrowInstanceOf(expression.selects[0].args.this, Expression)?.replace(parseOne('1.2'));
    expression = annotateTypes(expression);
    expect(expression.selects[0].type?.toString()).toBe(DataTypeExprKind.DOUBLE);
  });

  it('test_user_defined_type_annotation', () => {
    const s = new MappingSchema({
      schema: { t: { x: 'int' } },
      dialect: 'postgres',
    });
    const expression = annotateTypes(parseOne('SELECT CAST(x AS IPADDRESS) FROM t', { into: SelectExpr }), { schema: s });
    expect(expression.selects[0].type?.toString()).toBe(DataTypeExprKind.USERDEFINED);
    expect(narrowInstanceOf(expression.selects[0].type, Expression)?.sql({ dialect: 'postgres' })).toBe('IPADDRESS');
  });

  it('test_unnest_annotation', () => {
    const expression = annotateTypes(
      qualify(
        parseOne(`
          SELECT a, a.b, a.b.c FROM x, UNNEST(x.a) AS a
        `, {
          dialect: 'bigquery',
          into: SelectExpr,
        }),
      ),
      { schema: { x: { a: 'ARRAY<STRUCT<b STRUCT<c int>>>' } } },
    );
    expect(narrowInstanceOf(expression.selects[0].type, Expression)?.sql()).toBe(DataTypeExpr.build('STRUCT<b STRUCT<c int>>')?.sql());
    expect(narrowInstanceOf(expression.selects[1].type, Expression)?.sql()).toBe(DataTypeExpr.build('STRUCT<c int>')?.sql());
    expect(narrowInstanceOf(expression.selects[2].type, Expression)?.sql()).toBe(DataTypeExpr.build('int')?.sql());

    expect(
      narrowInstanceOf(annotateTypes(
        qualify(
          parseOne('SELECT x FROM UNNEST(GENERATE_DATE_ARRAY(\'2021-01-01\', current_date(), interval 1 day)) AS x', { into: SelectExpr }),
        ),
      ).selects[0].type, Expression)?.sql(),
    ).toBe(DataTypeExpr.build('date')?.sql());

    expect(
      narrowInstanceOf(annotateTypes(
        qualify(
          parseOne('SELECT x FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(\'2016-10-05 00:00:00\', \'2016-10-06 02:00:00\', interval 1 day)) AS x', { into: SelectExpr }),
        ),
      ).selects[0].type, Expression)?.sql(),
    ).toBe(DataTypeExpr.build('timestamp')?.sql());
  });
});
