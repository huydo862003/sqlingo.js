import { Dialect } from '../dialects/dialect';
import { ExecuteError } from '../errors';
import {
  AddExpr,
  AliasExpr,
  AnonymousExpr,
  AndExpr,
  ArrayExpr,
  BetweenExpr,
  BooleanExpr,
  CaseExpr,
  CastExpr,
  ColumnExpr,
  ConcatExpr,
  DistinctExpr,
  DivExpr,
  DotExpr,
  EqExpr,
  Expression,
  ExceptExpr,
  ExtractExpr,
  FuncExpr,
  InExpr,
  IntersectExpr,
  IntervalExpr,
  IsExpr,
  JsonExtractExpr,
  JsonPathExpr,
  JsonPathKeyExpr,
  JsonPathSubscriptExpr,
  LambdaExpr,
  LiteralExpr,
  ModExpr,
  MulExpr,
  NeqExpr,
  NotExpr,
  NullExpr,
  OrderedExpr,
  OrExpr,
  StarExpr,
  SubExpr,
  UnionExpr,
  JoinExprKind,
  type TableExpr,
} from '../expressions';
import { ALL_FUNCTIONS } from '../parser/function_registry';
import { Generator } from '../generator';
import type {
  Plan, Step,
} from '../planner';
import {
  Scan, Aggregate, Join, Sort, SetOperation,
} from '../planner';
import { Context } from './context';
import { ENV } from './env';
import {
  Table, RowReader,
} from './table';

/** Generates ORDERED(this, desc, nullsFirst) for use in sort key evaluation. */
function orderedJs (this: Generator, expression: OrderedExpr): string {
  const thisSql = this.sql(expression, 'this');
  const desc = expression.args.desc ? 'true' : 'false';
  const nullsFirst = expression.args.nullsFirst ? 'true' : 'false';
  return `ORDERED(${thisSql}, { desc: ${desc}, undefinedFirst: ${nullsFirst} })`;
}

/** Generates a function call using the expression's key as the function name. */
function rename (this: Generator, e: Expression): string {
  try {
    const values = Object.values(e.args);

    if (values.length === 1) {
      const val = values[0];
      if (!Array.isArray(val)) {
        return this.func(e._constructor.key, [val]);
      }
      return this.func(e._constructor.key, val);
    }

    if (e instanceof FuncExpr && (e._constructor as typeof FuncExpr).isVarLenArgs) {
      const args = [];
      for (const v of values) {
        if (Array.isArray(v)) {
          args.push(...v);
        } else if (v !== undefined) {
          args.push(v);
        }
      }
      return this.func(e._constructor.key, args);
    }

    return this.func(
      e._constructor.key,
      values.filter((v) => v !== undefined),
    );
  } catch (ex) {
    throw new Error(`Could not rename ${e}: ${ex}`);
  }
}

/** Generates a JS ternary chain from a CASE expression, building from the last branch inward. */
function caseJs (this: Generator, expression: CaseExpr): string {
  const thisStr = this.sql(expression, 'this');
  let chain = this.sql(expression, 'default') || 'null';

  const ifs = expression.args.ifs ?? [];
  for (const e of [...ifs].reverse()) {
    const trueStr = this.sql(e, 'true');
    let condition = this.sql(e, 'this');
    if (thisStr) {
      condition = `${thisStr} === (${condition})`;
    }
    chain = `(${condition} ? ${trueStr} : (${chain}))`;
  }

  return chain;
}

/** Generates a JS arrow function from a Lambda expression. */
function lambdaJs (this: Generator, e: LambdaExpr): string {
  return `(${this.expressions(e, { flat: true })}) => ${this.sql(e, 'this')}`;
}

/** Generates a DIV call, appending `|| undefined` for safe division and wrapping in Math.trunc for integer division. */
function divJs (this: Generator, e: DivExpr): string {
  let denominator = this.sql(e, 'expression');

  if (e.args.safe) {
    denominator += ' || undefined';
  }

  let sql = `DIV(${this.sql(e, 'this')}, ${denominator})`;

  if (e.args.typed) {
    sql = `Math.trunc(${sql})`;
  }

  return sql;
}

export class JavascriptGenerator extends Generator {
  static override get TRANSFORMS () {
    const parent = super.TRANSFORMS;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const overrides = new Map<typeof Expression, (this: Generator, e: any) => string>([
      ...parent.entries(),
      [
        AddExpr,
        function (this: Generator, e: AddExpr) {
          return `ADD(${this.sql(e, 'this')}, ${this.sql(e, 'expression')})`;
        },
      ],
      [
        AnonymousExpr,
        function (this: Generator, e: AnonymousExpr) {
          return this.func(e.name, e.args.expressions ?? []);
        },
      ],
      [
        AliasExpr,
        function (this: Generator, e: AliasExpr) {
          return this.sql(e.args.this as Expression);
        },
      ],
      [
        AndExpr,
        function (this: Generator, e: AndExpr) {
          return this.binary(e, '&&');
        },
      ],
      [
        ArrayExpr,
        function (this: Generator, e: ArrayExpr) {
          return `[${this.expressions(e, { flat: true })}]`;
        },
      ],
      [BetweenExpr, rename],
      [
        BooleanExpr,
        function (this: Generator, e: BooleanExpr) {
          return e.args.this ? 'true' : 'false';
        },
      ],
      [CaseExpr, caseJs],
      [
        CastExpr,
        function (this: Generator, e: CastExpr) {
          // Get the DataTypeExprKind as a lowercase string
          // DataTypeExprKind enum values are lowercase strings like 'text', 'bigint', etc.
          const toExpr = e.args.to;
          const toKind = typeof toExpr === 'string'
            ? toExpr.toLowerCase()
            : this.sql(toExpr as Expression).toLowerCase();
          return `CAST(${this.sql(e.args.this as Expression)}, '${toKind}')`;
        },
      ],
      [
        ColumnExpr,
        function (this: Generator, e: ColumnExpr) {
          const table = this.sql(e, 'table') || undefined;
          const col = this.sql(e.args.this as Expression);
          return `scope[${table}][${col}]`;
        },
      ],
      [
        ConcatExpr,
        function (this: Generator, e: ConcatExpr) {
          return this.func(
            e.args.safe ? 'SAFECONCAT' : 'CONCAT',
            e.args.expressions ?? [],
          );
        },
      ],
      [
        DistinctExpr,
        function (this: Generator, e: DistinctExpr) {
          return `new Set([${this.sql(e, 'this')}])`;
        },
      ],
      [
        DotExpr,
        function (this: Generator, e: DotExpr) {
          // DotExpr represents property access like .flavor
          // this is the left side (e.g., scope["i"]["attributes"])
          // expression is the right side (the property name)
          const left = this.sql(e.args.this as Expression);
          const right = this.sql(e.args.expression as Expression);
          // Convert .property to ["property"] notation
          return `${left}[${right}]`;
        },
      ],
      [DivExpr, divJs],
      [
        EqExpr,
        function (this: Generator, e: EqExpr) {
          return `EQ(${this.sql(e, 'this')}, ${this.sql(e, 'expression')})`;
        },
      ],
      [
        NeqExpr,
        function (this: Generator, e: NeqExpr) {
          return `NEQ(${this.sql(e, 'this')}, ${this.sql(e, 'expression')})`;
        },
      ],
      [
        ModExpr,
        function (this: Generator, e: ModExpr) {
          return `MOD(${this.sql(e, 'this')}, ${this.sql(e, 'expression')})`;
        },
      ],
      [
        MulExpr,
        function (this: Generator, e: MulExpr) {
          return `MUL(${this.sql(e, 'this')}, ${this.sql(e, 'expression')})`;
        },
      ],
      [
        SubExpr,
        function (this: Generator, e: SubExpr) {
          return `SUB(${this.sql(e, 'this')}, ${this.sql(e, 'expression')})`;
        },
      ],
      [
        ExtractExpr,
        function (this: Generator, e: ExtractExpr) {
          return `EXTRACT('${e.name.toLowerCase()}', ${this.sql(e, 'expression')})`;
        },
      ],
      [
        InExpr,
        function (this: Generator, e: InExpr) {
          return `[${this.expressions(e, { flat: true })}].includes(${this.sql(e, 'this')})`;
        },
      ],
      [
        IntervalExpr,
        function (this: Generator, e: IntervalExpr) {
          return `INTERVAL(${this.sql(e.args.this as Expression)}, '${this.sql(e.args.unit as Expression)}')`;
        },
      ],
      [
        IsExpr,
        function (this: Generator, e: IsExpr) {
          return e.args.this instanceof LiteralExpr ? this.binary(e, '===') : this.binary(e, '===');
        },
      ],
      [
        JsonExtractExpr,
        function (this: Generator, e: JsonExtractExpr) {
          return this.func(
            JsonExtractExpr.key,
            [
              e.args.this as Expression,
              e.args.expression as Expression,
              ...(e.args.expressions ?? []) as Expression[],
            ],
          );
        },
      ],
      [
        JsonPathExpr,
        function (this: Generator, e: JsonPathExpr) {
          const parts = e.args.expressions ?? [];
          return `[${parts.slice(1).map((p) => this.sql(p as Expression))
            .join(',')}]`;
        },
      ],
      [
        JsonPathKeyExpr,
        function (this: Generator, e: JsonPathKeyExpr) {
          return `'${this.sql(e.args.this as Expression)}'`;
        },
      ],
      [
        JsonPathSubscriptExpr,
        function (this: Generator, e: JsonPathSubscriptExpr) {
          return `'${e.args.this}'`;
        },
      ],
      [LambdaExpr, lambdaJs],
      [
        NotExpr,
        function (this: Generator, e: NotExpr) {
          return `!(${this.sql(e.args.this as Expression)})`;
        },
      ],
      [NullExpr, () => 'undefined'],
      [
        OrExpr,
        function (this: Generator, e: OrExpr) {
          return this.binary(e, '||');
        },
      ],
      [OrderedExpr, orderedJs],
      [StarExpr, () => '1'],
    ]);

    // Map all registered functions to rename (like Python's ALL_FUNCTIONS -> _rename)
    for (const cls of ALL_FUNCTIONS) {
      if (!overrides.has(cls as typeof Expression)) {
        overrides.set(cls as typeof Expression, rename);
      }
    }

    return overrides;
  }
}

export class JavascriptDialect extends Dialect {
  static Generator = JavascriptGenerator;
}

export class JavascriptExecutor {
  public generator: Generator;
  public env: Record<string, unknown>;
  public tables: Record<string, Table>;

  constructor (env?: Record<string, unknown>, tables?: Record<string, Table>) {
    this.generator = new JavascriptDialect().generator({
      identify: true,
      comments: false,
    });
    this.env = {
      ...ENV,
      ...(env || {}),
    };
    this.tables = tables || {};
  }

  /**
   * Traverses the logical execution plan DAG from leaves to root,
   * evaluating each step and passing the context upwards.
   */
  execute (plan: Plan): Table {
    const finished = new Set<Scan | Aggregate | Join | Sort | SetOperation>();
    const queue = new Set<Scan | Aggregate | Join | Sort | SetOperation>(plan.leaves as Iterable<Scan>);
    const contexts = new Map<Scan | Aggregate | Join | Sort | SetOperation, Context>();

    while (0 < queue.size) {
      const node = Array.from(queue).pop();
      if (!node) break;
      queue.delete(node);

      try {
        const contextTables = new Map<string, Table>();
        for (const dep of node.dependencies) {
          const depCtx = contexts.get(dep as Scan);
          if (depCtx) {
            for (const [name, table] of depCtx.tables) {
              contextTables.set(name, table);
            }
          }
        }

        const context = this.context(contextTables);

        if (node instanceof Scan) {
          contexts.set(node, this.scan(node, context));
        } else if (node instanceof Aggregate) {
          contexts.set(node, this.aggregate(node, context));
        } else if (node instanceof Join) {
          contexts.set(node, this.join(node, context));
        } else if (node instanceof Sort) {
          contexts.set(node, this.sort(node, context));
        } else if (node instanceof SetOperation) {
          contexts.set(node, this.setOperation(node, context));
        } else {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          throw new Error(`NotImplementedError: ${(node as any).constructor.name}`);
        }

        finished.add(node);

        for (const dep of node.dependents) {
          if (Array.from(dep.dependencies).every((d) => contexts.has(d as Scan))) {
            queue.add(dep as Scan);
          }
        }

        for (const dep of node.dependencies) {
          if (Array.from(dep.dependents).every((d) => finished.has(d as Scan))) {
            contexts.delete(dep as Scan);
          }
        }
      } catch (e: unknown) {
        throw new ExecuteError(`Step '${node.name}' failed: ${(e as Error).message}`);
      }
    }

    const root = plan.root;
    if (!root) return new Table([]);
    return contexts.get(root as Scan)?.tables.get(root.name) ?? new Table([]);
  }

  /** Convert a SQL expression into literal JS code string. */
  generate (expression: Expression | undefined): string | undefined {
    if (!expression) return undefined;
    return this.generator.generate(expression);
  }

  /** Convert an array of SQL expressions into an array of JS code strings. */
  generateTuple (expressions: Expression[]): string[] {
    if (!expressions || expressions.length === 0) return [];
    return expressions.map((e) => this.generate(e) ?? '');
  }

  context (tables: Map<string, Table> | Record<string, Table>): Context {
    const map = tables instanceof Map ? tables : new Map(Object.entries(tables));
    return new Context(map, this.env);
  }

  table (expressions: unknown[]): Table {
    const names = expressions.map((e) => (e instanceof Expression ? e.aliasOrName : e));
    return new Table(names as string[]);
  }

  scan (step: Scan, context: Context): Context {
    let source: string | undefined = undefined;

    if (step.source && step.source instanceof Expression) {
      source = step.source.name || step.source.alias;
    } else if (typeof step.source === 'string') {
      source = step.source;
    }

    let tableIter: Iterable<unknown>;

    if (source === undefined) {
      const staticResult = this.static();
      context = staticResult[0];
      tableIter = staticResult[1];
    } else if (context.has(source)) {
      if (!step.projections?.length && !step.condition) {
        const srcTable = context.tables.get(source);
        if (!srcTable) return context;
        return this.context({ [step.name]: srcTable });
      }
      tableIter = context.tableIter(source);
    } else {
      const scanResult = this.scanTable(step);
      context = scanResult[0];
      tableIter = scanResult[1];
    }

    return this.context({ [step.name]: this.projectAndFilter(context, step, tableIter) });
  }

  projectAndFilter (context: Context, step: Step, tableIter: Iterable<unknown>): Table {
    const sink = this.table(step.projections?.length ? step.projections : context.columns);
    const condition = this.generate(step.condition);
    const projections = this.generateTuple(step.projections || []);

    // Handle self-referential projections: if projections reference scope[stepName] but stepName
    // is not in the context, add an empty table with that name so scope references can resolve.
    // This happens when a Scan step has projections like scope["_0"]["x"] and is named "_0".
    if (0 < projections.length && projections[0].includes(`scope["${step.name}"]`) && !context.has(step.name)) {
      // Create empty table with sink columns to allow scope references to find it
      const contextTables: Record<string, Table> = {};
      for (const [k, v] of context.tables) {
        if (v) contextTables[k] = v;
      }
      contextTables[step.name] = new Table(sink.columns);
      context = this.context(new Map(Object.entries(contextTables)));
    }

    for (const reader of tableIter) {
      const r = Array.isArray(reader) ? reader[0] : reader;

      if (step.limit <= sink.length) {
        break;
      }

      if (condition && !context.eval(condition)) {
        continue;
      }

      if (0 < projections.length) {
        sink.append(context.evalTuple(projections));
      } else {
        sink.append((r as RowReader).row);
      }
    }

    return sink;
  }

  static (): [Context, RowReader[]] {
    return [this.context(new Map()), [new RowReader([])]];
  }

  scanTable (step: Scan): [Context, Iterable<unknown>] {
    const tables = this.tables;
    const source = step.source as TableExpr;

    // Build the nested path for table lookup (catalog.db.table)
    let table: unknown;

    if (source.args.catalog || source.args.db) {
      // Qualified table reference: try catalog.db.table path
      table = tables;
      if (source.args.catalog) {
        const catalogName = typeof source.args.catalog === 'string'
          ? source.args.catalog
          : (source.args.catalog as Expression).name;
        table = (table as Record<string, unknown>)[catalogName];
      }
      if (source.args.db && table !== undefined) {
        const dbName = typeof source.args.db === 'string'
          ? source.args.db
          : (source.args.db as Expression).name;
        table = (table as Record<string, unknown>)[dbName];
      }
      if (table !== undefined) {
        const tableName = source.name;
        table = (table as Record<string, unknown>)[tableName];
      }
    } else {
      // Unqualified table reference: try flat lookup first
      const tableName = source.name;
      table = (tables as Record<string, unknown>)[tableName];

      // If not found in flat structure, search in up to 2 levels deep for catalog.db.table structure
      if (!table || (typeof table === 'object' && !(table instanceof Table))) {
        for (const level1 of Object.values(tables)) {
          if (level1 && typeof level1 === 'object' && !(level1 instanceof Table)) {
            const candidate = (level1 as Record<string, unknown>)[tableName];
            if (candidate instanceof Table) {
              table = candidate;
              break;
            }
            // Try two levels deeper
            for (const level2 of Object.values(level1 as Record<string, unknown>)) {
              if (level2 && typeof level2 === 'object' && !(level2 instanceof Table)) {
                const candidate2 = (level2 as Record<string, unknown>)[tableName];
                if (candidate2 instanceof Table) {
                  table = candidate2;
                  break;
                }
              }
            }
          }
          if (table instanceof Table) break;
        }
      }
    }

    const resultTable = table instanceof Table ? table : new Table([]);

    const contextMap = new Map<string, Table>();
    contextMap.set(source.aliasOrName, resultTable);
    const context = this.context(contextMap);

    return [context, resultTable[Symbol.iterator]()];
  }

  join (step: Join, context: Context): Context {
    const source = step.sourceName;

    if (!source) return context;
    const sourceTable = context.tables.get(source) ?? new Table([]);
    let sourceContext = this.context(new Map([[source, sourceTable]]));

    const columnRanges = new Map<string, { start: number;
      stop: number; }>();
    columnRanges.set(source, {
      start: 0,
      stop: sourceTable.columns.length,
    });

    for (const [name, join] of Object.entries(step.joins || {})) {
      const table = context.tables.get(name) ?? new Table([]);
      const start = Math.max(...Array.from(columnRanges.values()).map((r) => r.stop));
      columnRanges.set(name, {
        start,
        stop: table.columns.length + start,
      });

      const joinContext = this.context(new Map([[name, table]]));
      let joinedTable: Table;

      const joinEntry = join;
      if (joinEntry.sourceKey.length) {
        joinedTable = this.hashJoin(joinEntry, sourceContext, joinContext);
      } else {
        joinedTable = this.nestedLoopJoin(joinEntry, sourceContext, joinContext);
      }

      const nextTables = new Map<string, Table>();
      for (const [n, cr] of columnRanges.entries()) {
        nextTables.set(n, new Table(joinedTable.columns, joinedTable.rows, cr));
      }
      sourceContext = this.context(nextTables);

      const condition = this.generate(joinEntry.condition);
      // Only apply condition filter for INNER joins; for LEFT/RIGHT joins, don't filter after the join
      // since the condition was already used to determine which rows match during the join
      const sideStr = String(joinEntry.side).toLowerCase();
      const isLeftOrRight = sideStr === JoinExprKind.LEFT || sideStr === JoinExprKind.RIGHT;
      if (condition && !isLeftOrRight) {
        sourceContext.filter(condition);
      }
    }

    if (!step.condition && !step.projections?.length) {
      return sourceContext;
    }

    const sink = this.projectAndFilter(
      sourceContext,
      step,
      (function* (ctx: Context) {
        for (const [reader] of ctx) {
          yield reader;
        }
      })(sourceContext),
    );

    if (step.projections?.length) {
      return this.context(new Map([[step.name ?? '', sink]]));
    } else {
      const finalTables = new Map<string, Table>();
      for (const [name, table] of sourceContext.tables.entries()) {
        finalTables.set(name, new Table(table.columns, sink.rows, table.columnRange));
      }
      return this.context(finalTables);
    }
  }

  nestedLoopJoin (_join: Join['joins'][string], sourceContext: Context, joinContext: Context): Table {
    const table = new Table([...sourceContext.columns, ...joinContext.columns]);

    for (const [readerA] of sourceContext) {
      for (const [readerB] of joinContext) {
        table.append([...readerA.row, ...readerB.row]);
      }
    }

    return table;
  }

  hashJoin (join: Join['joins'][string], sourceContext: Context, joinContext: Context): Table {
    const sourceKey = this.generateTuple(join.sourceKey as Expression[]);
    const joinKey = this.generateTuple(join.joinKey as Expression[]);
    const sideStr = String(join.side).toLowerCase();
    const left = sideStr === JoinExprKind.LEFT;
    const right = sideStr === JoinExprKind.RIGHT;

    const results = new Map<string, [unknown[][], unknown[][]]>();
    const leftRows: Array<[string, unknown[]]> = [];

    for (const [reader, ctx] of sourceContext) {
      const keyStr = JSON.stringify(ctx.evalTuple(sourceKey));
      if (!results.has(keyStr)) results.set(keyStr, [[], []]);
      // Copy the row to avoid shared reference issues
      results.get(keyStr)?.[0].push([...reader.row]);
      leftRows.push([keyStr, reader.row]);
    }

    for (const [reader, ctx] of joinContext) {
      const keyStr = JSON.stringify(ctx.evalTuple(joinKey));
      if (!results.has(keyStr)) results.set(keyStr, [[], []]);
      // Copy the row to avoid shared reference issues
      results.get(keyStr)?.[1].push([...reader.row]);
    }

    const table = new Table([...sourceContext.columns, ...joinContext.columns]);
    const leftNulls = [new Array(joinContext.columns.length).fill(undefined)];
    const rightNulls = [new Array(sourceContext.columns.length).fill(undefined)];

    if (left) {
      // For LEFT JOIN, output only keys that have left rows
      for (const [, [aGroup, bGroup]] of results.entries()) {
        if (aGroup.length === 0) continue; // Skip keys without left rows
        const finalBGroup = 0 < bGroup.length ? bGroup : leftNulls;
        for (const aRow of aGroup) {
          for (const bRow of finalBGroup) {
            table.append([...aRow, ...bRow]);
          }
        }
      }
    } else if (right) {
      // For RIGHT JOIN, output all right rows (with or without matches)
      for (const [, [aGroup, bGroup]] of results.entries()) {
        const finalAGroup = 0 < aGroup.length ? aGroup : rightNulls;
        for (const aRow of finalAGroup) {
          for (const bRow of bGroup) {
            table.append([...aRow, ...bRow]);
          }
        }
      }
    } else {
      // For INNER JOIN, output only matching rows
      for (const [, [aGroup, bGroup]] of results.entries()) {
        for (const aRow of aGroup) {
          for (const bRow of bGroup) {
            table.append([...aRow, ...bRow]);
          }
        }
      }
    }

    return table;
  }

  aggregate (step: Aggregate, context: Context): Context {
    const groupBy = this.generateTuple(Object.values(step.group || {}));
    const aggregations = this.generateTuple(step.aggregations || []);
    const operands = this.generateTuple(step.operands || []);

    if (0 < operands.length) {
      const operandTable = new Table(this.table(step.operands).columns);

      for (const [, ctx] of context) {
        operandTable.append(ctx.evalTuple(operands));
      }

      for (let i = 0; i < context.table.rows.length; i++) {
        context.table.rows[i] = [...context.table.rows[i], ...operandTable.rows[i]];
      }

      const width = context.columns.length;
      context.addColumns(...operandTable.columns);

      const scopedOperandTable = new Table(
        context.columns,
        context.table.rows,
        {
          start: width,
          stop: width + operandTable.columns.length,
        },
      );

      const newTables = new Map<string | undefined, Table>(
        [...context.tables.entries()].filter(([k, v]) => typeof k === 'string' && v instanceof Table) as [string, Table][],
      );
      newTables.set(undefined, scopedOperandTable);
      context = this.context(newTables as Map<string, Table>);
    }

    context.sort(groupBy);

    let group: string | undefined = undefined;
    let start = 0;
    let end = 1;
    const length = context.table.length;
    const groupKeyNames = Object.keys(step.group || {});
    const table = this.table([...groupKeyNames, ...(step.aggregations || [])]);

    const addRow = () => {
      const parsedGroup: unknown[] = group ? JSON.parse(group) : [];
      table.append([...parsedGroup, ...context.evalTuple(aggregations)]);
    };

    if (0 < length) {
      for (let i = 0; i < length; i++) {
        context.setIndex(i);
        const key = JSON.stringify(context.evalTuple(groupBy));
        group = group === undefined ? key : group;
        end += 1;

        if (key !== group) {
          context.setRange(start, end - 2);
          addRow();
          group = key;
          start = end - 2;
        }

        if (step.limit <= table.rows.length) {
          break;
        }

        if (i === length - 1) {
          context.setRange(start, end - 1);
          addRow();
        }
      }
    } else if (0 < step.limit && groupBy.length === 0) {
      context.setRange(0, 0);
      table.append(context.evalTuple(aggregations));
    }

    const nextTables = new Map<string, Table>();
    nextTables.set(step.name, table);
    for (const [name] of context.tables.entries()) {
      nextTables.set(name, table);
    }
    context = this.context(nextTables);

    if (step.projections?.length || step.condition) {
      return this.scan(step, context);
    }
    return context;
  }

  sort (step: Sort, context: Context): Context {
    const projections = this.generateTuple(step.projections || []);
    const projectionColumns: string[] = (step.projections || []).map((p: Expression) => p.aliasOrName);
    const allColumns = [...context.columns, ...projectionColumns];
    const sink = this.table(allColumns);

    const sortKeys = this.generateTuple(step.key || []);

    for (const [reader, ctx] of context) {
      const projVals = ctx.evalTuple(projections);
      sink.append([...reader.row, ...projVals]);
    }

    const sortTables = new Map<string | undefined, Table>();
    sortTables.set(undefined, sink);
    for (const name of context.tables.keys()) {
      sortTables.set(name, sink);
    }
    const sortContext = this.context(sortTables as Map<string, Table>);
    sortContext.sort(sortKeys);

    if (Number.isFinite(step.limit)) {
      sortContext.table.rows = sortContext.table.rows.slice(0, step.limit);
    }

    const colStart = context.columns.length;
    const colEnd = allColumns.length;
    const output = new Table(
      projectionColumns,
      sortContext.table.rows.map((r: unknown[]) => r.slice(colStart, colEnd)),
    );

    return this.context(new Map([[step.name ?? '', output]]));
  }

  setOperation (step: SetOperation, context: Context): Context {
    const left = context.tables.get(step.left ?? '') ?? new Table([]);
    const right = context.tables.get(step.right ?? '') ?? new Table([]);

    const sink = this.table(left.columns);

    if (step.op && (step.op === IntersectExpr || step.op.prototype instanceof IntersectExpr)) {
      const leftSet = new Set(left.rows.map((r) => JSON.stringify(r)));
      const rightSet = new Set(right.rows.map((r) => JSON.stringify(r)));
      sink.rows = [...leftSet].filter((r) => rightSet.has(r)).map((r) => JSON.parse(r));
    } else if (step.op && (step.op === ExceptExpr || step.op.prototype instanceof ExceptExpr)) {
      const rightSet = new Set(right.rows.map((r) => JSON.stringify(r)));
      sink.rows = left.rows.filter((r) => !rightSet.has(JSON.stringify(r)));
    } else if (step.op && (step.op === UnionExpr || step.op.prototype instanceof UnionExpr) && step.distinct) {
      const combined = new Set([...left.rows, ...right.rows].map((r) => JSON.stringify(r)));
      sink.rows = [...combined].map((r) => JSON.parse(r));
    } else {
      sink.rows = [...left.rows, ...right.rows];
    }

    if (Number.isFinite(step.limit)) {
      sink.rows = sink.rows.slice(0, step.limit);
    }

    return this.context(new Map([[step.name, sink]]));
  }
}
