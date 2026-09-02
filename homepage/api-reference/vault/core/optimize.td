# Optimize

## `optimize(sql, options?)`

Canonicalize and optimize a SQL query.

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
| `options.schema` | `object` | Table schema mapping |
| `options.dialect` | `Dialect class or string` | Target dialect |

### Returns

`Expression` - The optimized AST. Call `.sql()` to get the SQL string.
