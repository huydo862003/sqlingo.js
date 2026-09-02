---
_icon: icon.sparkles
---

## `optimize(sql, options?)`

Canonicalize and optimize a SQL query. Removes dead conditions, qualifies columns with table names, and simplifies expressions.

```typescript
import { optimize } from "@hdnax/sqlingo.js";

const result = optimize(
  "SELECT * FROM t WHERE 1 = 1 AND x > 2",
  { schema: { t: { x: "INT" } } },
);
result.sql(); // 'SELECT "t"."x" AS "x" FROM "t" AS "t" WHERE "t"."x" > 2'
```

### Parameters

| Name | Type | Description |
|------|------|-------------|
| `sql` | `string` | SQL to optimize |
| `options.schema` | `object` | Table-to-columns mapping, e.g. `{ t: { x: "INT" } }` |
| `options.dialect` | `Dialect class or string` | Target dialect |

### Returns

`Expression` - The optimized AST. Call `.sql()` to get the SQL string.

You can also pass a `MappingSchema` for more control:

```typescript
import { optimize, parseOne, MappingSchema } from "@hdnax/sqlingo.js";

const schema = new MappingSchema({ users: { id: "INT", name: "TEXT" } });
const optimized = optimize(parseOne("SELECT * FROM users"), { schema });
```
