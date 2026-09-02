---
_icon: icon.hammer
---

Fluent API for building SQL programmatically instead of concatenating strings.

## `select(...columns)`

```typescript
import { select } from "@hdnax/sqlingo.js";

select("x", "y").from("t").where("x > 1").sql();
// "SELECT x, y FROM t WHERE x > 1"
```

## `condition(sql)`

```typescript
import { condition } from "@hdnax/sqlingo.js";

condition("x = 1").and("y = 2").or("z = 3").sql();
// "(x = 1 AND y = 2) OR z = 3"
```

## All helpers

| Function | Description | Example |
|----------|-------------|---------|
| `select(...cols)` | SELECT statement | `select("x", "y").from("t")` |
| `condition(sql)` | Composable condition | `condition("x = 1").and("y = 2")` |
| `column(opts)` | Column reference | `column({ col: "x", table: "t" })` |
| `table(name)` | Table reference | `table("users")` |
| `func(name, ...args)` | Function call | `func("COALESCE", "x", 1)` |
| `case_()` | CASE expression | `case_().when("x=1", "a").else("b")` |
| `and(conditions)` | AND multiple conditions | `and(["x=1", "y=2"])` |
| `or(conditions)` | OR multiple conditions | `or(["x=1", "y=2"])` |
| `not(condition)` | Negate a condition | `not("x=1")` |
| `null_()` | NULL literal | `null_()` |
| `true_()` / `false_()` | Boolean literals | `true_()` |
| `cast(expr, type)` | CAST expression | `cast("x", "INT")` |
| `insert(sql, table)` | INSERT statement | `insert("SELECT 1", "t")` |
| `update(table, sets)` | UPDATE statement | `update("t", { x: 1 })` |
| `delete_(table)` | DELETE statement | `delete_("t", { where: "x=1" })` |
| `union(selects)` | UNION | `union(["SELECT 1", "SELECT 2"])` |
| `intersect(selects)` | INTERSECT | `intersect(["SELECT 1", "SELECT 2"])` |
| `except(selects)` | EXCEPT | `except(["SELECT 1", "SELECT 2"])` |
