# sqlingo.js

![Version](https://img.shields.io/badge/version-0.0.1-orange)
![Status](https://img.shields.io/badge/status-alpha-orange)
![License](https://img.shields.io/badge/license-MIT-green)
![SQLGlot](https://img.shields.io/badge/SQLGlot-264e95f-orange)

A JavaScript port of [SQLGlot](https://github.com/tobymao/sqlglot) (v28.10.0) — a SQL parser, transpiler, optimizer, and engine.

Supports TypeScript & CJS/ESM. Bundled size is around 1MB minified, gzipped.

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

```
This project incorporates code from the following sources:

===============================================================================

sqlingo.js
Copyright (c) 2026 Huy DNA
Licensed under the MIT License

===============================================================================

SQLGlot (https://github.com/tobymao/sqlglot)
Copyright (c) 2025 Toby Mao
Licensed under the MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## Links

- [GitHub](https://github.com/huydo862003/sqlingo.js)
- [Issues](https://github.com/huydo862003/sqlingo.js/issues)
