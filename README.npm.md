# sqlingo.js

[![npm version](https://img.shields.io/npm/v/@hdnax/sqlingo.js)](https://www.npmjs.com/package/@hdnax/sqlingo.js)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](https://github.com/huydo862003/sqlingo.js/blob/master/LICENSE)
![SQLGlot](https://img.shields.io/badge/SQLGlot-v28.10.0-blue)

WARNING: This package is still in alpha. Although the SQLGlot tests have all passed, but finding contrived failures may require me to use this package extensively myself, which I planned to in the future. You can also help me report bugs in the Github issue.

NOTICE: Done AI slop migration. Most of the code (documentation) and 99% of the library code are filtered and rewritten by me.

A JavaScript/TypeScript port of [SQLGlot](https://github.com/tobymao/sqlglot), which is a comprehensive SQL parser, transpiler, optimizer, and engine.

This package allows you to parse, transpile, optimize, and execute SQL across **33+ dialects** in JavaScript, with no other setup.

Supports TypeScript & CJS/ESM. Works in Node.js and the browser.

- [GitHub](https://github.com/huydo862003/sqlingo.js)
- [Issues](https://github.com/huydo862003/sqlingo.js/issues)
- [Changelog](https://github.com/huydo862003/sqlingo.js/blob/master/CHANGELOG.md)

## Features

- 33+ SQL dialects: Postgres, MySQL, BigQuery, Snowflake, DuckDB, ClickHouse, Redshift, Athena, Spark, and many more
- Full SQLGlot feature set: parsing, transpilation, optimization, column lineage, SQL diffing, and execution
- Pure JavaScript: no need for WASM or native dependencies
- TypeScript-first: full type definitions included

## Installation

```bash
npm install @hdnax/sqlingo.js
# or
pnpm add @hdnax/sqlingo.js
```

Peer dependency: [`luxon`](https://www.npmjs.com/package/luxon) (^3.7.2) is required for date/time operations.

## Quick Start

This example demonstrates transpiling a query from Spark to Postgres and then optimizing it.

```ts
import {
  transpile,
  parseOne,
  optimize,
  MappingSchema,
  Dialects,
} from "@hdnax/sqlingo.js";
// Note: The dialect file must be explicitly imported
// for the dialect to be registered to sqlingo.js
import "@hdnax/sqlingo.js/postgres";
import "@hdnax/sqlingo.js/spark";

// Transpile between dialects
const [pgSql] = transpile("SELECT APPROX_COUNT_DISTINCT(x) FROM table", {
  read: Dialects.Spark,
  write: Dialects.Postgres,
});
console.log(pgSql);
// Output: SELECT COUNT(DISTINCT x) FROM "table"

// Optimize an expression
const sql = "SELECT a, b FROM t WHERE a + 1 = 2";
const schema = new MappingSchema({ t: { a: "int", b: "int" } });

const optimized = optimize(parseOne(sql), { schema });
console.log(optimized.sql());
// Output: SELECT t.a AS a, t.b AS b FROM t AS t WHERE t.a = 1
```

## Core Usage

### Parsing

Parse SQL strings into expression trees (AST).

```ts
import { parse, parseOne } from "@hdnax/sqlingo.js";

// Parse multiple statements
const expressions = parse("SELECT 1; SELECT 2");

// Parse a single statement
const expr = parseOne("SELECT a, b FROM t WHERE a > 1");
```

### Transpiling

Convert SQL between different dialects.

```ts
import { transpile, Dialects } from "@hdnax/sqlingo.js";
import "@hdnax/sqlingo.js/duckdb";
import "@hdnax/sqlingo.js/hive";

const [result] = transpile("SELECT EPOCH_MS(1618088028295)", {
  read: Dialects.Duckdb,
  write: Dialects.Hive,
});
// Output: "SELECT FROM_UNIXTIME(1618088028295 / POW(10, 3))"
```

### Tokenizing

Extract tokens from a SQL string for lower-level analysis.

```ts
import { tokenize, Dialects } from "@hdnax/sqlingo.js";
import "@hdnax/sqlingo.js/postgres";

const tokens = tokenize("SELECT 1", {
  dialect: Dialects.Postgres,
});
```

## SQL Builder

Build queries programmatically using a fluent API.

```ts
import { select, column, condition, Dialects } from "@hdnax/sqlingo.js";
import "@hdnax/sqlingo.js/mysql";

const query = select("a", "b").from("t").where(condition("a > 1")).limit(10);

console.log(
  query.sql({
    dialect: Dialects.Mysql,
  }),
);
// Output: SELECT a, b FROM t WHERE a > 1 LIMIT 10
```

## Optimization & Analysis

### Optimization

Simplify and normalize queries based on schema information.

```ts
import { optimize, MappingSchema } from "@hdnax/sqlingo.js";

const schema = new MappingSchema({
  // define your schema
});

const optimized = optimize(parseOne("SELECT * FROM t"), { schema });
```

### Column Lineage

Trace the origin of columns through subqueries and joins.

```ts
import { lineage } from "@hdnax/sqlingo.js";

const node = lineage("b", "SELECT a AS b FROM (SELECT x AS a FROM y)");
console.log(node.source.name);
// Output: "y"
```

## Registering a Custom Dialect

You can extend the library by registering custom dialects or overriding existing behavior.

```ts
import { Dialect, Generator, transpile } from "@hdnax/sqlingo.js";

class MyDialect extends Dialect {
  static DIALECT_NAME = "my_dialect";

  static Generator = class extends Generator {
    // Override how specific expressions are generated
  };
}

// Register for use in transpile/parse
Dialect.register("my_dialect", MyDialect);

const [result] = transpile("SELECT 1", { write: "my_dialect" });
```

## Supported Dialects

Athena, BigQuery, ClickHouse, Databricks, Doris, Dremio, Drill, Druid, DuckDB, Dune, Exasol, Fabric, Hive, Materialize, MySQL, Oracle, Postgres, Presto, PRQL, Redshift, RisingWave, SingleStore, Snowflake, Solr, Spark, Spark2, SQLite, StarRocks, Tableau, Teradata, Trino, TSQL

## SQLGlot Compatibility

This package tracks [SQLGlot](https://github.com/tobymao/sqlglot) v28.10.0 (commit `264e95f`). The API surface mirrors SQLGlot's Python API, adapted to TypeScript conventions. See [CONVENTION.md](https://github.com/huydo862003/sqlingo.js/blob/master/CONVENTION.md) for details.

## License

MIT. See [LICENSE](https://github.com/huydo862003/sqlingo.js/blob/master/LICENSE).

Based on [SQLGlot](https://github.com/tobymao/sqlglot) by Toby Mao (MIT). See [COPYRIGHT_NOTICE](https://github.com/huydo862003/sqlingo.js/blob/master/COPYRIGHT_NOTICE).
