---
_icon: icon.compass
---

A walkthrough of the main features: parsing, transpiling, building queries, column lineage, and optimization.

## Parsing

`parse` returns an array of AST expressions, one per statement. `parseOne` returns a single expression.

```typescript
import { parse, parseOne } from "@hdnax/sqlingo.js";

const expressions = parse("SELECT 1; SELECT 2");
console.log(expressions.length); // 2

const ast = parseOne("SELECT a, b FROM t WHERE a > 1");
console.log(ast.sql()); // "SELECT a, b FROM t WHERE a > 1"
```

Pass a dialect to parse dialect-specific syntax:

```typescript
import { MySQL } from "@hdnax/sqlingo.js/mysql";

const [ast] = parse("SELECT `name` FROM users", { read: MySQL });
```

## Transpiling

Convert SQL from one dialect to another with `transpile`:

```typescript
import { transpile } from "@hdnax/sqlingo.js";
import { DuckDB } from "@hdnax/sqlingo.js/duckdb";
import { Hive } from "@hdnax/sqlingo.js/hive";

const [result] = transpile("SELECT EPOCH_MS(1618088028295)", {
  read: DuckDB,
  write: Hive,
});
console.log(result);
// "SELECT FROM_UNIXTIME(1618088028295 / POW(10, 3))"
```

## Building queries

Build SQL programmatically with a fluent API instead of string concatenation:

```typescript
import { select, condition } from "@hdnax/sqlingo.js";
import { MySQL } from "@hdnax/sqlingo.js/mysql";

const query = select("a", "b")
  .from("t")
  .where(condition("a > 1"))
  .limit(10);

console.log(query.sql({ dialect: MySQL }));
// "SELECT a, b FROM t WHERE a > 1 LIMIT 10"
```

Compose conditions separately:

```typescript
import { condition } from "@hdnax/sqlingo.js";

const filter = condition("status = 'active'")
  .and("created_at > '2024-01-01'")
  .or("role = 'admin'");

console.log(filter.sql());
// "(status = 'active' AND created_at > '2024-01-01') OR role = 'admin'"
```

## Navigating the AST

Every parsed element is an `Expression`. Walk, search, and transform the tree:

```typescript
import { parseOne, ColumnExpr } from "@hdnax/sqlingo.js";

const ast = parseOne("SELECT a, b FROM t WHERE x > 1");

for (const col of ast.findAll(ColumnExpr)) {
  console.log(col.name);
}
// "a", "b", "x"
```

Transform nodes in place:

```typescript
const transformed = ast.transform((node) => {
  // Return a new node to replace, or undefined to keep as-is
  return undefined;
});
```

## Column lineage

Trace the origin of columns through subqueries and joins:

```typescript
import { lineage } from "@hdnax/sqlingo.js";

const node = lineage("b", "SELECT a AS b FROM (SELECT x AS a FROM y)");
console.log(node.source.name);
// "y"
```

## Tokenizing

Extract tokens from a SQL string for lower-level analysis:

```typescript
import { tokenize } from "@hdnax/sqlingo.js";
import { Postgres } from "@hdnax/sqlingo.js/postgres";

const tokens = tokenize("SELECT 1", { dialect: Postgres });
```

## Custom dialects

Extend `Dialect` to add your own SQL flavor:

```typescript
import { Dialect, Generator, transpile } from "@hdnax/sqlingo.js";

class MyDialect extends Dialect {
  static DIALECT_NAME = "my_dialect";

  static Generator = class extends Generator {
    // Override how specific expressions are generated
  };
}

Dialect.register(MyDialect);
const [result] = transpile("SELECT 1", { write: MyDialect });
```
