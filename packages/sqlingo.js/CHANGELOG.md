# @hdnax/sqlingo.js

## 0.6.1

### Patch Changes

- e6fdbfa: Update new API in readme

## 0.6.0

### BREAKING CHANGES

- Dialect imports no longer auto-register via side effects. Pass dialect classes directly or call `Dialect.register()` explicitly:

  ```ts
  // Before (v0.5.0)
  import "@hdnax/sqlingo.js/mysql";
  parse("SELECT 1", { read: "mysql" });

  // After (v0.6.0) - preferred: pass class directly
  import { MySQL } from "@hdnax/sqlingo.js/mysql";
  parse("SELECT 1", { read: MySQL });

  // After (v0.6.0) - opt-in string lookup
  import { Dialect } from "@hdnax/sqlingo.js";
  import { MySQL } from "@hdnax/sqlingo.js/mysql";
  Dialect.register(MySQL);
  parse("SELECT 1", { read: "mysql" });
  ```

- `Dialect.register()` signature changed from `register(name, class)` to `register(...classes)`.

### Minor Changes

- 625fb15: Fix circular imports leading to unusability
- Side-effect-free API surface enables tree shaking
- Package marked with `"sideEffects": false`

### Patch Changes

- Resolve all 44 failing dialect tests ported from sqlglot v29.0.0 (MySQL, BigQuery, Trino, Databricks, SingleStore, Doris, StarRocks, Drill, Druid, Solr)
- Fix generator method dispatch casing (`dPipeSql`, `atTimeZoneSql`, `regexpExtractAllSql`, `startsWithSql`) and `ExpressionKey.STARTS_WITH` enum value to match camelCase convention
- Fix `ORIGINAL_KEYWORDS` convention in Spark/Drill tokenizers, preventing `@cache` inheritance bugs for subclass dialects (e.g. Databricks VOID keyword)
- Fix `renameFunc` to mirror sqlglot's `*flatten(expression.args.values())`
- Fix `groupConcatSql` dialect-aware generation and `sep: null` semantics (Python `None` vs JS `undefined`)
- Fix `pivotAliasSql` to use `toIdentifier()` for numeric alias quoting without mutating shared AST
- Fix `unixDateSql` to route through dialect generators via `DateDiffExpr` instead of raw string
- Fix DuckDB `trimSql` BLOB-to-TEXT wrapping, `unnestSql` AS keyword, `numberToStrSql` culture check
- Fix `explodeProjectionToUnnest`: correct `BracketExpr` indexing, `func()` expression building, `offset: 1` for BigQuery `SAFE_ORDINAL`
- Fix BigQuery `arrayContainsSql` (`LiteralExpr` not `IdentifierExpr`), ML function kwargs snake-to-camelCase, `buildJsonStripNulls` kwargs, jsonpath `VAR_TOKENS` for hyphenated keys
- Fix `analyzeSql` clause ordering, `introducerSql` prefix, `boolXorSql` template literal, `ShaLLOW` typo, `sqlName()` call
- Fix `annotateTypes` to preserve `CastExpr` natural type (not overwrite with `UNKNOWN`), add `INVERSE_FORMAT_TRIE` for format roundtrip
- Fix MySQL `ESCAPE_FOLLOW_CHARS` static, escape string handling, `timestampTruncSql` quotes, `TO_DAYS` paren, `CharacterSet` default
- Fix expression `argOrder` for `GapFillExpr`, `TimeStrToTimeExpr`, `TimeExpr`, `RegexpExtractAllExpr`
- Fix Snowflake inverse time mapping (`mmmm`/`mon`), SingleStore WEEKDAY/Repeat/IsAscii, StarRocks `createSql`, Doris partition parsing/property quoting, Oracle `ON_CONDITION_EMPTY_BEFORE_ERROR`
- Fix `subsecondPrecision` to use regex instead of Luxon for space-separated timestamps
- Add missing `register()` on `ExplodeOuterExpr`, `PosexplodeOuterExpr`, `ApproxQuantileExpr`, `ArrayExceptExpr`, `JsonArrayExpr.fromArgList`, `JsonbContainsExpr`, `JsonArrayContainsExpr`, `AndExpr`, `OrExpr`, `XorExpr`
- Add `TsOrDsAddExpr` type annotation in Spark2/Hive typing for `ADD_MONTHS` transpilation
- Fix test porting issues: BigQuery byte strings, MySQL escape sequences, raw string newlines, hex byte chars, bracket assertions, `from_` to `from` property access

## 0.5.0

### Minor Changes

- 920aef4: Sync to sqlglot 29.0.0

## 0.4.2

### Patch Changes

- cd3a34c: Bump deps to resolve dependabot security alerts: `vite` ^8.0.10 to ^8.1.0, `dompurify` override to ^3.4.11, add `ws` ^8.21.0 and `js-yaml` ^4.2.0 overrides
- f0d9075: Override read-yaml-file to fix changeset js-yaml compatibility

## 0.4.1

### Patch Changes

- 13e61b1: Fix various porting bugs in BigQuery, DuckDB, ClickHouse, and other dialects. All tests are now passing.

## 0.4.0

### Minor Changes

- ec190bb: Add missing snowflake, presto, clickhouse tests and fix various bugs related to them (undiscovered before). The work is still in progress as not 100% tests are passing yet
- 7ad6584: Annotate FORMAT_STRING(expr) for Spark ([upstream commit](https://github.com/tobymao/sqlglot/commit/1418494f777358f4b6bd1e05ee5cb02591d92c74))
- 7ad6584: Add numeric TRUNC output for additional dialects: ClickHouse, Presto, Hive, SQLite ([upstream commit](https://github.com/tobymao/sqlglot/commit/ff1fd521147cb66acc36f2da7b1590d9e7f8140f))

## 0.3.2

### Patch Changes

- 92777d6: Update README.md copywriting

## 0.3.1

### Patch Changes

- Pin upstream sqlglot to 87250100 to include exp.Trunc for numeric truncation

## 0.3.0

### Minor Changes

- cdd20a2: Bump vitest to 4.1.0 & pin vite to 7.3.5 (to support esm decorators)
- 067de4d: Reorganize the packages to split the lockfile of playground from the sqlingo.js package

## 0.2.3

### Patch Changes

- e9855e1: Sync with [sqlglot@28.10.1](https://github.com/tobymao/sqlglot/pull/7032)
  - Support missing meta when updating position metadata for an expression ([sqglot#7032](https://github.com/tobymao/sqlglot/pull/7032))

## 0.2.2

### Patch Changes

- 98abe58: Guard against prototype-polluting assignment in `Expression.setArgKey` [#2](https://github.com/huydo862003/sqlingo.js/pull/2)

## 0.2.1

### Patch Changes

- 7c03a7e:
  - Update `dompurify` from 3.2.7 to 3.4.2 to resolve vulnerability issues [#1](https://github.com/huydo862003/sqlingo.js/pull/1)
  - Update `picomatch` from 2.3.1 to 4.0.4 to resolve vulnerability issues [#1](https://github.com/huydo862003/sqlingo.js/pull/1)
  - Update `postcss` from 8.5.8 to 8.5.14 to resolve vulnerability issues [#1](https://github.com/huydo862003/sqlingo.js/pull/1)

## 0.2.0

### Minor Changes

- 62626fd: Complete AI migration. Most code are human-generated now.

## 0.1.7

### Patch Changes

- 29256e1: Fix vulnerability issues
- b4924ce: Bump flatted to 3.4.2 to fix vulnerability issue

## 0.1.6

### Patch Changes

- 8967932: Fix copywriting of README

## 0.1.5

### Patch Changes

- Redesign homepage with genuix design system; bump to v0.1.5.

## 0.1.4

### Patch Changes

- 1ad4d3e: (fck-AI-slop) Migrating from AI slops

## 0.1.3

### Patch Changes

- 6aeadaa: Add disclaimer about AI usage

## 0.1.2

### Patch Changes

- a0cbe60: Add warnings to npm doc page

## 0.1.1

### Patch Changes

- c90ac1e: Update API doc for npm package

## 0.1.0

### Minor Changes

- Add lazy dialect entrypoints (`@hdnax/sqlingo.js/postgres`, `/mysql`, `/tsql`, `/mssql`, etc.). Import core without loading any dialect; register only what you need.

## 0.0.5

### Patch Changes

- dc5780a: Update copywriting

## 0.0.4

### Patch Changes

- 4667ffe: Update README

## 0.0.3

### Patch Changes

- cdf377d: Include CHANGELOG.md to package

## 0.0.2

### Patch Changes

- Add GitHub repository info to package.json
- Add npm-specific README with usage guide and copyright notice

## 0.0.1

### Patch Changes

- First alpha version
