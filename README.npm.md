# sqlingo.js

[![npm version](https://img.shields.io/npm/v/@hdnax/sqlingo.js)](https://www.npmjs.com/package/@hdnax/sqlingo.js)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](https://github.com/huydo862003/sqlingo.js/blob/master/LICENSE)
[![Bundle Size](https://img.shields.io/bundlephobia/minzip/@hdnax/sqlingo.js)](https://bundlephobia.com/package/@hdnax/sqlingo.js)
![SQLGlot](https://img.shields.io/badge/SQLGlot-v28.10.0-blue)

A JavaScript/TypeScript port of [SQLGlot](https://github.com/tobymao/sqlglot), which is a comprehensive SQL parser, transpiler, optimizer, and engine.

This package allows you to parse, transpile, optimize, and execute SQL across **33+ dialects** in JavaScript, with no other setup.

Supports TypeScript & CJS/ESM. Works in Node.js and the browser.

- [GitHub](https://github.com/huydo862003/sqlingo.js)
- [Issues](https://github.com/huydo862003/sqlingo.js/issues)
- [Changelog](https://github.com/huydo862003/sqlingo.js/blob/master/CHANGELOG.md)

## Features

- 33+ SQL dialects: Postgres, MySQL, BigQuery, Snowflake, DuckDB, ClickHouse, Redshift, Athena, Spark, and many more
- Full SQLGlot feature set: parsing, transpilation, optimization, column lineage, SQL diffing, and execution
- Pure JavaScript: no need for WASM or anything.
- TypeScript-first: full type definitions included

## Installation

```bash
npm install @hdnax/sqlingo.js
# or
pnpm add @hdnax/sqlingo.js
# or
yarn add @hdnax/sqlingo.js
```

Peer dependency: [`luxon`](https://www.npmjs.com/package/luxon) (^3.7.2) is required for date/time operations.

## Usage

### Parsing

```ts
import { parse, parseOne } from "@hdnax/sqlingo.js";

// Parse one or more SQL statements
const expressions = parse("SELECT 1; SELECT 2");

// Parse a single statement
const expr = parseOne("SELECT a, b FROM t WHERE a > 1");
```

### Transpiling

```ts
import { transpile } from "@hdnax/sqlingo.js";

// Transpile between dialects
const [result] = transpile("SELECT EPOCH_MS(1618088028295)", {
  read: "duckdb",
  write: "hive",
});
// => "SELECT FROM_UNIXTIME(1618088028295 / POW(10, 3))"
```

### SQL Builder

```ts
import { select, column, condition, from } from "@hdnax/sqlingo.js";

const query = select(column("a"), column("b"))
  .from("t")
  .where(condition("a > 1"));
```

### Optimization

```ts
import { optimize } from "@hdnax/sqlingo.js";
import { MappingSchema } from "@hdnax/sqlingo.js";

const schema = new MappingSchema({
  // define your schema
});

const optimized = optimize(parseOne("SELECT * FROM t"), { schema });
```

### Tokenizing

```ts
import { tokenize } from "@hdnax/sqlingo.js";

const tokens = tokenize("SELECT 1", "postgres");
```

## Supported Dialects

Athena, BigQuery, ClickHouse, Databricks, Doris, Dremio, Drill, Druid, DuckDB, Dune, Exasol, Fabric, Hive, Materialize, MySQL, Oracle, Postgres, Presto, PRQL, Redshift, RisingWave, SingleStore, Snowflake, Solr, Spark, Spark2, SQLite, StarRocks, Tableau, Teradata, Trino, TSQL

## Public API

The main exports from `@hdnax/sqlingo.js`:

| Export                                                        | Description                             |
| ------------------------------------------------------------- | --------------------------------------- |
| `parse`, `parseOne`                                           | Parse SQL strings into expression trees |
| `transpile`                                                   | Parse and generate SQL across dialects  |
| `generate`                                                    | Generate SQL from an expression tree    |
| `tokenize`                                                    | Tokenize a SQL string                   |
| `optimize`                                                    | Optimize an expression tree             |
| `execute`                                                     | Execute SQL against in-memory tables    |
| `Dialect`, `Dialects`                                         | Dialect classes and enum                |
| `Expression`                                                  | Base expression class                   |
| `select`, `from`, `column`, `condition`, `table`, `func`, ... | Expression builder helpers              |
| `and`, `or`, `not`                                            | Logical combinators                     |
| `union`, `intersect`, `except`                                | Set operations                          |
| `cast`, `alias`, `case_`, `subquery`                          | SQL clause helpers                      |
| `Schema`, `MappingSchema`                                     | Schema definitions for optimizer        |
| `diff`                                                        | SQL diff utility                        |
| `lineage`                                                     | Column lineage tracing                  |
| `dump`, `load`                                                | Serialize/deserialize expression trees  |

## SQLGlot Compatibility

This package tracks [SQLGlot](https://github.com/tobymao/sqlglot) v28.10.0 (commit `264e95f`). The API surface mirrors SQLGlot's Python API, adapted to TypeScript conventions (camelCase, etc.). See [CONVENTION.md](https://github.com/huydo862003/sqlingo.js/blob/master/CONVENTION.md) for details on the mapping.

## License

MIT. See [LICENSE](https://github.com/huydo862003/sqlingo.js/blob/master/LICENSE).

Based on [SQLGlot](https://github.com/tobymao/sqlglot) by Toby Mao (MIT). See [COPYRIGHT_NOTICE](https://github.com/huydo862003/sqlingo.js/blob/master/COPYRIGHT_NOTICE).

## Copyright Notice

Check [License](./LICENSE).

## CDN Usage

Use sqlingo.js directly in the browser via CDN:

```html
<script type="module">
  import { transpile } from "https://esm.sh/@hdnax/sqlingo.js";
</script>
```
