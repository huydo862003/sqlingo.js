# sqlingo.js

[![npm version](https://img.shields.io/npm/v/@hdnax/sqlingo.js)](https://www.npmjs.com/package/@hdnax/sqlingo.js)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
![SQLGlot](https://img.shields.io/badge/SQLGlot-v28.10.0-blue)

WARNING: This package is still in alpha. Although the SQLGlot tests have all passed, but finding contrived failures may require me to use this package extensively myself, which I planned to in the future. You can also help me report bugs in the Github issue.

A JavaScript/TypeScript port of [SQLGlot](https://github.com/tobymao/sqlglot), which is a comprehensive SQL parser, transpiler, optimizer, and engine.

This package allows you to parse, transpile, optimize, and execute SQL across **33+ dialects** in JavaScript, with no other setup.

Supports TypeScript & CJS/ESM. Works in Node.js and the browser.

> There's also an alternative [polyglot](https://github.com/tobilg/polyglot) library - check it out!

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
// Note: You must explictly import from a dialect
// for sqlingo.js to detect this dialect
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

See the [Usage Guide](./README.npm.md) for full API documentation and examples.

## Supported Dialects

Athena, BigQuery, ClickHouse, Databricks, Doris, Dremio, Drill, Druid, DuckDB, Dune, Exasol, Fabric, Hive, Materialize, MySQL, Oracle, Postgres, Presto, PRQL, Redshift, RisingWave, SingleStore, Snowflake, Solr, Spark, Spark2, SQLite, StarRocks, Tableau, Teradata, Trino, TSQL

## Goals (& Non-goals)

The main goal is that sqlingo.js should be a close mirror to SQLGlot. This way, it can quickly catch up with SQLGlot bug fixes and new releases.

Another goal is to stay true to Typescript convention (check [CONVENTION.md](./CONVENTION.md)).

Currently, these are non-goals:

- Optimized performance.
- Optimized bundle size.
- Compatibility with SQLGlot (but it should be trivial to make the two compatible)

## Backstory

I'm currenly maintaining [@dbml/core](https://github.com/holistics/dbml), a library that supports converting between DBML and SQL. Under the hood it uses ANTLR for parsing, and honestly it's been a mess:

- `@dbml/core` is **33MB**, which is quite insane to be honest. It actually broke our CI with OOM errors.
- We can't add more dialects without making the bundle even larger.
- The parser is feature-incomplete and spits out user-unfriendly error messages like `No viable alternative at...`.
- After all that, we only support **5 dialects**.

At a hackathon, I was poking around [Dagster](https://dagster.io/) and stumbled upon SQLGlot. I thought it was amazing that there was a library like this. SQLGlot seems to be trusted by a lots of tools in the Python ecosystems.

Since then, I was looking for an alternative in Javascript, because I want to run it on the browser. Sadly, at the time, there was none that I knew of.

I tried running SQLGlot through [Pyodide](https://pyodide.org/) as a hack, but the runtime is way too heavy to ship anywhere that matters.

Therefore, I decided to port it. At 2 weeks into my porting process, [polyglot](https://github.com/tobilg/polyglot) was announced (LOL!). However, I didn't want to waste my effort & also wanted full control - so I just continued anyways.

## Development Setup

### Prerequisites

Make sure these are installed on your machine:

- [`node`](https://nodejs.org/)@^20 - [Installation Guide](https://nodejs.org/en/download/package-manager)
- [`pnpm`](https://pnpm.io/)@^10.26.1 - [Installation Guide](https://pnpm.io/installation)

### Available Scripts

```bash
pnpm test          # Run tests
pnpm test:ui       # Run tests with UI
pnpm test:coverage # Run tests with coverage
pnpm build         # Build the project
pnpm dev           # Build in watch mode
pnpm typecheck     # Type check without emitting
pnpm lint          # Lint the code
pnpm lint:fix      # Lint and auto-fix issues
pnpm run doc       # Generate documentation
```

### Mirror Guide

Check [CONVENTION.md](./CONVENTION.md).

I have compiled our convention and lots of pitfalls there. You can use the knowledge there to allow easier debugging.

## License

sqlingo.js is licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Attribution

This project is based on [SQLGlot](https://github.com/tobymao/sqlglot) by Toby Mao, which is also licensed under the MIT License. The original SQLGlot source code is included as a submodule in this repository.

See [COPYRIGHT_NOTICE](COPYRIGHT_NOTICE) for full copyright information.
