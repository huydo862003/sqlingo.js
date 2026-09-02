---
_icon: icon.rocket
---

## Install

```bash
npm install @hdnax/sqlingo.js
```

Peer dependency: [`luxon`](https://www.npmjs.com/package/luxon) (^3.7.2) is required for date/time operations.

## Parse SQL

```typescript
import { parseOne } from "@hdnax/sqlingo.js";

const ast = parseOne("SELECT a, b FROM users WHERE active = true");
console.log(ast.sql()); // "SELECT a, b FROM users WHERE active = true"
```

## Transpile between dialects

```typescript
import { transpile } from "@hdnax/sqlingo.js";
import { MySQL } from "@hdnax/sqlingo.js/mysql";
import { Postgres } from "@hdnax/sqlingo.js/postgres";

const [sql] = transpile(
  "SELECT DATE_SUB(d, INTERVAL 1 DAY) FROM t",
  { read: MySQL, write: Postgres },
);
console.log(sql);
// "SELECT d - INTERVAL '1 DAY' FROM t"
```

## Optimize a query

```typescript
import { optimize, parseOne, MappingSchema } from "@hdnax/sqlingo.js";

const schema = new MappingSchema({ t: { a: "int", b: "int" } });
const optimized = optimize(parseOne("SELECT a, b FROM t WHERE a + 1 = 2"), { schema });

console.log(optimized.sql());
// "SELECT t.a AS a, t.b AS b FROM t AS t WHERE t.a = 1"
```
