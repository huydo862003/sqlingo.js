# @hdnax/sqlingo.js

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
