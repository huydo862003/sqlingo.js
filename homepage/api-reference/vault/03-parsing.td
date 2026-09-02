---
_icon: icon.file_code
---

## `parse(sql, options?)`

Parse SQL into an array of AST expressions, one per statement.

```typescript
import { parse } from "@hdnax/sqlingo.js";
import { MySQL } from "@hdnax/sqlingo.js/mysql";

const [ast] = parse("SELECT a, b FROM t", { read: MySQL });
```

| Name | Type | Description |
|------|------|-------------|
| `sql` | `string` | The SQL string to parse |
| `options.read` | `Dialect class or string` | Source dialect |
| `options.dialect` | `Dialect class or string` | Alias for `read` |

Returns `Expression[]`,one expression per statement.

---

## `parseOne(sql, options?)`

Parse a single SQL statement. Throws if zero statements are found. Returns a `BlockExpr` if multiple statements are found.

```typescript
import { parseOne } from "@hdnax/sqlingo.js";

const ast = parseOne("SELECT 1 + 1");
ast.sql(); // "SELECT 1 + 1"
```

Same parameters as `parse`. Returns a single `Expression`.

---

## `transpile(sql, options?)`

Parse SQL in one dialect and generate it in another.

```typescript
import { transpile } from "@hdnax/sqlingo.js";
import { MySQL } from "@hdnax/sqlingo.js/mysql";
import { Postgres } from "@hdnax/sqlingo.js/postgres";

const [sql] = transpile(
  "SELECT DATE_SUB(d, INTERVAL 1 DAY) FROM t",
  { read: MySQL, write: Postgres },
);
// "SELECT d - INTERVAL '1 DAY' FROM t"
```

| Name | Type | Description |
|------|------|-------------|
| `sql` | `string` | The SQL string to transpile |
| `options.read` | `Dialect class or string` | Source dialect |
| `options.write` | `Dialect class or string` | Target dialect |
| `options.pretty` | `boolean` | Pretty-print output |

Returns `string[]`,one SQL string per statement, in the target dialect.
