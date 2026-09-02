---
_icon: icon.tree
---

The base class for all AST nodes. Every parsed SQL element is an `Expression`.

## Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `.sql(options?)` | `string` | Generate SQL string |
| `.find(type)` | `Expression?` | Find first descendant of a given type |
| `.findAll(type)` | `Iterable<Expression>` | Find all descendants of a given type |
| `.transform(fn)` | `Expression` | Transform the AST recursively |
| `.walk()` | `Iterable<Expression>` | Walk all nodes depth-first |
| `.copy()` | `Expression` | Deep copy the subtree |

## Operators

| Method | Returns | Description |
|--------|---------|-------------|
| `.as(alias)` | `AliasExpr` | Alias the expression |
| `.eq(other)` | `EqExpr` | Equality comparison |
| `.and(other)` | `AndExpr` | Logical AND |
| `.or(other)` | `OrExpr` | Logical OR |

## Example

```typescript
import { parseOne, ColumnExpr } from "@hdnax/sqlingo.js";

const ast = parseOne("SELECT a, b FROM t WHERE x > 1");

for (const col of ast.findAll(ColumnExpr)) {
  console.log(col.name);
}
// "a", "b", "x"
```

Generate SQL in a specific dialect:

```typescript
import { Postgres } from "@hdnax/sqlingo.js/postgres";

ast.sql({ dialect: Postgres });
```
