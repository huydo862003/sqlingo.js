## Class-based (recommended)

Pass dialect classes directly. No registration needed, fully tree-shakeable.

```typescript
import { parse, transpile } from "@hdnax/sqlingo.js";
import { MySQL } from "@hdnax/sqlingo.js/mysql";
import { Postgres } from "@hdnax/sqlingo.js/postgres";

parse("SELECT 1", { read: MySQL });
transpile("SELECT 1", { read: MySQL, write: Postgres });
```

## String-based (opt-in)

For apps that need string lookup (e.g. from config/dropdowns), register explicitly:

```typescript
import { Dialect, parse } from "@hdnax/sqlingo.js";
import { MySQL } from "@hdnax/sqlingo.js/mysql";
import { Postgres } from "@hdnax/sqlingo.js/postgres";

Dialect.register(MySQL, Postgres);

parse("SELECT 1", { read: "mysql" });
```

## `Dialect.register(...dialects)`

Register dialect classes for string-based lookup.

| Parameter | Type | Description |
|-----------|------|-------------|
| `dialects` | `...typeof Dialect` | Variadic dialect classes |
