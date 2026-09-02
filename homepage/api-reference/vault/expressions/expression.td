# Expression

The base class for all AST nodes. Every parsed SQL element is an `Expression`.

## Key methods

| Method | Returns | Description |
|--------|---------|-------------|
| `.sql(options?)` | `string` | Generate SQL string |
| `.find(type)` | `Expression?` | Find first descendant of type |
| `.findAll(type)` | `Iterable<Expression>` | Find all descendants of type |
| `.transform(fn)` | `Expression` | Transform the AST recursively |
| `.walk()` | `Iterable<Expression>` | Walk all nodes |
| `.copy()` | `Expression` | Deep copy |
| `.as(alias)` | `AliasExpr` | Create an alias |
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
