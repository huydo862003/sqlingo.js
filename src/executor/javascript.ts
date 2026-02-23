import { Dialect } from '../dialects/dialect';
import { ExecuteError } from '../errors';
import {
  type Expression,
  AliasExpr,
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
  Expression as ExpressionClass,
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
  NotExpr,
  NullExpr,
  OrderedExpr,
  OrExpr,
  StarExpr,
  UnionExpr,
  JoinExprKind,
} from '../expressions';
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
function orderedJs (self: Generator, expression: OrderedExpr): string {
  const thisSql = self.sql(expression, 'this');
  const desc = expression.args.desc ? 'true' : 'false';
  const nullsFirst = expression.args.nullsFirst ? 'true' : 'false';
  return `ORDERED(${thisSql}, ${desc}, ${nullsFirst})`;
}

/** Generates a function call using the expression's key as the function name. */
function rename (self: Generator, e: Expression): string {
  try {
    const values = Object.values(e.args);

    if (values.length === 1) {
      const val = values[0];
      if (!Array.isArray(val)) {
        return self.func(e._constructor.key, [val]);
      }
      return self.func(e._constructor.key, val);
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
      return self.func(e._constructor.key, args);
    }

    return self.func(
      e._constructor.key,
      values.filter((v) => v !== undefined),
    );
  } catch (ex) {
    throw new Error(`Could not rename ${e}: ${ex}`);
  }
}

/** Generates a JS ternary chain from a CASE expression, building from the last branch inward. */
function caseJs (self: Generator, expression: CaseExpr): string {
  const thisStr = self.sql(expression, 'this');
  let chain = self.sql(expression, 'default') || 'null';

  const ifs = expression.args.ifs ?? [];
  for (const e of [...ifs].reverse()) {
    const trueStr = self.sql(e, 'true');
    let condition = self.sql(e, 'this');
    if (thisStr) {
      condition = `${thisStr} === (${condition})`;
    }
    chain = `(${condition} ? ${trueStr} : (${chain}))`;
  }

  return chain;
}

/** Generates a JS arrow function from a Lambda expression. */
function lambdaJs (self: Generator, e: LambdaExpr): string {
  return `(${self.expressions(e, { flat: true })}) => ${self.sql(e, 'this')}`;
}

/** Generates a DIV call, appending `|| null` for safe division and wrapping in Math.trunc for integer division. */
function divJs (self: Generator, e: DivExpr): string {
  let denominator = self.sql(e, 'expression');

  if (e.args.safe) {
    denominator += ' || null';
  }

  let sql = `DIV(${self.sql(e, 'this')}, ${denominator})`;

  if (e.args.typed) {
    sql = `Math.trunc(${sql})`;
  }

  return sql;
}

export class JavascriptGenerator extends Generator {
  static override get TRANSFORMS () {
    const parent = super.TRANSFORMS;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const overrides = new Map<typeof Expression, (self: Generator, e: any) => string>([
      ...parent.entries(),
      [AliasExpr, (_self: Generator, e: AliasExpr) => _self.sql(e.this as Expression)],
      [AndExpr, (s: Generator, e: AndExpr) => s.binary(e, '&&')],
      [ArrayExpr, (s: Generator, e: ArrayExpr) => `[${s.expressions(e, { flat: true })}]`],
      [BetweenExpr, rename],
      [BooleanExpr, (_s: Generator, e: BooleanExpr) => (e.this ? 'true' : 'false')],
      [CaseExpr, caseJs],
      [
        CastExpr,
        (s: Generator, e: CastExpr) =>
          `CAST(${s.sql(e.this as Expression)}, '${s.sql(e.args.to as Expression)}')`,
      ],
      [
        ColumnExpr,
        (s: Generator, e: ColumnExpr) =>
          `scope[${s.sql(e, 'table') || null}][${s.sql(e.this as Expression)}]`,
      ],
      [
        ConcatExpr,
        (s: Generator, e: ConcatExpr) =>
          s.func(
            e.args.safe ? 'SAFECONCAT' : 'CONCAT',
            e.args.expressions ?? [],
          ),
      ],
      [DistinctExpr, (s: Generator, e: DistinctExpr) => `new Set([${s.sql(e, 'this')}])`],
      [DivExpr, divJs],
      [
        ExtractExpr,
        (s: Generator, e: ExtractExpr) =>
          `EXTRACT('${e.name.toLowerCase()}', ${s.sql(e, 'expression')})`,
      ],
      [
        InExpr,
        (s: Generator, e: InExpr) =>
          `[${s.expressions(e, { flat: true })}].includes(${s.sql(e, 'this')})`,
      ],
      [
        IntervalExpr,
        (s: Generator, e: IntervalExpr) =>
          `INTERVAL(${s.sql(e.this as Expression)}, '${s.sql(e.args.unit as Expression)}')`,
      ],
      [
        IsExpr,
        (s: Generator, e: IsExpr) =>
          e.this instanceof LiteralExpr ? s.binary(e, '===') : s.binary(e, '==='),
      ],
      [
        JsonExtractExpr,
        (s: Generator, e: JsonExtractExpr) =>
          s.func(
            JsonExtractExpr.key,
            [
              e.this as Expression,
              e.args.expression as Expression,
              ...(e.args.expressions ?? []) as Expression[],
            ],
          ),
      ],
      [
        JsonPathExpr,
        (s: Generator, e: JsonPathExpr) => {
          const parts = e.args.expressions ?? [];
          return `[${parts.slice(1).map((p) => s.sql(p as Expression))
            .join(',')}]`;
        },
      ],
      [JsonPathKeyExpr, (s: Generator, e: JsonPathKeyExpr) => `'${s.sql(e.this as Expression)}'`],
      [JsonPathSubscriptExpr, (_s: Generator, e: JsonPathSubscriptExpr) => `'${e.this}'`],
      [LambdaExpr, lambdaJs],
      [NotExpr, (s: Generator, e: NotExpr) => `!(${s.sql(e.this as Expression)})`],
      [NullExpr, () => 'null'],
      [OrExpr, (s: Generator, e: OrExpr) => s.binary(e, '||')],
      [OrderedExpr, orderedJs],
      [StarExpr, () => '1'],
    ]);

    return overrides;
  }
}

export class JavascriptDialect extends Dialect {
  static Generator = JavascriptGenerator;
}

export class JavascriptExecutor {
  public generator: unknown;
  public env: Record<string, unknown>;
  public tables: Record<string, Table> | { find(source: Expression): Table };

  constructor (envOverride?: Record<string, unknown>, tables?: unknown) {
    this.generator = new JavascriptDialect().generator({
      identify: true,
      comments: false,
    });
    this.env = {
      ...ENV,
      ...(envOverride || {}),
    };
    this.tables = (tables || {}) as Record<string, Table> | { find(source: Expression): Table };
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
      const node = Array.from(queue).pop()!;
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
          throw new Error(`NotImplementedError: ${(node as { constructor: { name: string } }).constructor.name}`);
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
    return contexts.get(root as Scan)!.tables.get(root.name!)!;
  }

  /** Convert a SQL expression into literal JS code string. */
  generate (expression: ExpressionClass | undefined): string | undefined {
    if (!expression) return undefined;
    return (this.generator as { generate(e: Expression): string }).generate(expression);
  }

  /** Convert an array of SQL expressions into an array of JS code strings. */
  generateTuple (expressions: ExpressionClass[]): string[] {
    if (!expressions || expressions.length === 0) return [];
    return expressions.map((e) => this.generate(e)!);
  }

  context (tables: Map<string, Table> | Record<string, Table>): Context {
    const map = tables instanceof Map ? tables : new Map(Object.entries(tables));
    return new Context(map, this.env);
  }

  table (expressions: unknown[]): Table {
    const names = expressions.map((e) => (e instanceof ExpressionClass ? e.aliasOrName : e));
    return new Table(names as string[]);
  }

  scan (step: Scan, context: Context): Context {
    let source: string | undefined = undefined;

    if (step.source && step.source instanceof ExpressionClass) {
      source = step.source.name || step.source.alias;
    }

    let tableIter: Iterable<unknown>;

    if (source === undefined) {
      const staticResult = this.static();
      context = staticResult[0];
      tableIter = staticResult[1];
    } else if (context.has(source)) {
      if (!step.projections?.length && !step.condition) {
        return this.context({ [step.name!]: context.tables.get(source)! });
      }
      tableIter = context.tableIter(source);
    } else {
      const scanResult = this.scanTable(step);
      context = scanResult[0];
      tableIter = scanResult[1];
    }

    return this.context({ [step.name!]: this._projectAndFilter(context, step, tableIter) });
  }

  _projectAndFilter (context: Context, step: Step, tableIter: Iterable<unknown>): Table {
    const sink = this.table(step.projections?.length ? step.projections : context.columns);
    const condition = this.generate(step.condition);
    const projections = this.generateTuple(step.projections || []);

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
    const tables = this.tables as Record<string, Table> | { find(source: Expression): Table };
    const table = typeof (tables as { find?: unknown }).find === 'function'
      ? (tables as { find(source: Expression): Table }).find(step.source!)
      : (tables as Record<string, Table>)[(step.source as ExpressionClass).name!];

    const contextMap = new Map<string, Table>();
    contextMap.set((step.source as ExpressionClass).aliasOrName, table);
    const context = this.context(contextMap);

    return [context, table[Symbol.iterator]()];
  }

  join (step: Join, context: Context): Context {
    const source = step.sourceName;

    const sourceTable = context.tables.get(source!)!;
    let sourceContext = this.context(new Map([[source!, sourceTable]]));

    const columnRanges = new Map<string, { start: number;
      stop: number; }>();
    columnRanges.set(source!, {
      start: 0,
      stop: sourceTable.columns.length,
    });

    for (const [name, join] of Object.entries(step.joins || {})) {
      const table = context.tables.get(name)!;
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
      if (condition) {
        sourceContext.filter(condition);
      }
    }

    if (!step.condition && !step.projections?.length) {
      return sourceContext;
    }

    const sink = this._projectAndFilter(
      sourceContext,
      step,
      (function* (ctx: Context) {
        for (const [reader] of ctx) {
          yield reader;
        }
      })(sourceContext),
    );

    if (step.projections?.length) {
      return this.context(new Map([[step.name!, sink]]));
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
    const left = join.side === JoinExprKind.LEFT;
    const right = join.side === JoinExprKind.RIGHT;

    const results = new Map<string, [unknown[][], unknown[][]]>();

    for (const [reader, ctx] of sourceContext) {
      const keyStr = JSON.stringify(ctx.evalTuple(sourceKey));
      if (!results.has(keyStr)) results.set(keyStr, [[], []]);
      results.get(keyStr)![0].push(reader.row);
    }

    for (const [reader, ctx] of joinContext) {
      const keyStr = JSON.stringify(ctx.evalTuple(joinKey));
      if (!results.has(keyStr)) results.set(keyStr, [[], []]);
      results.get(keyStr)![1].push(reader.row);
    }

    const table = new Table([...sourceContext.columns, ...joinContext.columns]);
    const leftNulls = [new Array(joinContext.columns.length).fill(undefined)];
    const rightNulls = [new Array(sourceContext.columns.length).fill(undefined)];

    for (let [aGroup, bGroup] of results.values()) {
      if (left) {
        bGroup = 0 < bGroup.length ? bGroup : leftNulls;
      } else if (right) {
        aGroup = 0 < aGroup.length ? aGroup : rightNulls;
      }

      for (const aRow of aGroup) {
        for (const bRow of bGroup) {
          table.append([...aRow, ...bRow]);
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

      const newTables = new Map<string, Table>(
        [...context.tables.entries()].filter(([k]) => typeof k === 'string') as [string, Table][],
      );
      newTables.set('', scopedOperandTable);
      context = this.context(newTables);
    }

    context.sort(groupBy);

    let group: string | undefined = undefined;
    let start = 0;
    let end = 1;
    const length = context.table.length;
    const table = this.table([...Object.keys(step.group || {}), ...(step.aggregations || [])]);

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
    nextTables.set(step.name!, table);
    for (const [name] of context.tables.entries()) {
      nextTables.set(name, table);
    }
    context = this.context(nextTables);

    if (step.projections?.length || step.condition) {
      return this.scan(step as unknown as Scan, context);
    }
    return context;
  }

  sort (step: Sort, context: Context): Context {
    const projections = this.generateTuple(step.projections || []);
    const projectionColumns: string[] = (step.projections || []).map((p: Expression) => p.aliasOrName);
    const allColumns = [...context.columns, ...projectionColumns];
    const sink = this.table(allColumns);

    for (const [reader, ctx] of context) {
      sink.append([...reader.row, ...ctx.evalTuple(projections)]);
    }

    const sortTables = new Map<string, Table>();
    for (const name of context.tables.keys()) {
      sortTables.set(name, sink);
    }
    const sortContext = this.context(sortTables);
    sortContext.sort(this.generateTuple(step.key || []));

    if (Number.isFinite(step.limit)) {
      sortContext.table.rows = sortContext.table.rows.slice(0, step.limit);
    }

    const colStart = context.columns.length;
    const colEnd = allColumns.length;
    const output = new Table(
      projectionColumns,
      sortContext.table.rows.map((r: unknown[]) => r.slice(colStart, colEnd)),
    );

    return this.context(new Map([[step.name!, output]]));
  }

  setOperation (step: SetOperation, context: Context): Context {
    const left = context.tables.get(step.left!)!;
    const right = context.tables.get(step.right!)!;

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

    return this.context(new Map([[step.name!, sink]]));
  }
}
