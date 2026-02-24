import type { ExpressionOrString } from './expressions';
import {
  Expression,
  WithExpr,
  SelectExpr, SetOperationExpr, AggFuncExpr,
  ColumnExpr, IdentifierExpr,
  SubqueryExpr,
  column,
  alias,
  JoinExpr,
} from './expressions';
import {
  nameSequence,
} from './helper';
import {
  assertIsInstanceOf, filterInstanceOf, id, isInstanceOf,
} from './port_internals';
import { joinCondition } from './optimizer';

/**
 * Represents a query execution plan as a Directed Acyclic Graph (DAG) of Steps.
 */
export class Plan {
  public expression: Expression;
  public root: Step | undefined;
  private _dag: Map<Step, Set<Step>> | undefined = undefined;

  constructor (expression: Expression) {
    // Ensuring we work on a copy to prevent mutation of the original AST
    this.expression = expression.copy();
    this.root = Step.fromExpression(this.expression);
  }

  /**
   * Generates or retrieves the DAG of steps.
   */
  get dag (): Map<Step, Set<Step>> {
    if (this._dag) {
      return this._dag;
    }

    const dag = new Map<Step, Set<Step>>();
    const seen = new Set<Step>();

    const stack = [this.root];

    while (0 < stack.length) {
      const node = stack.pop()!;
      if (seen.has(node)) continue;

      seen.add(node);
      const dependencies = new Set<Step>();

      for (const dep of node.dependencies) {
        dependencies.add(dep);
        stack.push(dep);
      }

      dag.set(node, dependencies);
    }

    this._dag = dag;
    return this._dag;
  }

  /**
   * Returns an iterator over the leaf nodes (steps with no dependencies).
   */
  get leaves (): IterableIterator<Step> {
    const leafNodes: Step[] = [];
    for (const [node, deps] of this.dag.entries()) {
      if (deps.size === 0) {
        leafNodes.push(node);
      }
    }
    return leafNodes[Symbol.iterator]();
  }

  toString (): string {
    return `Plan\n----\n${this.root?.toString() ?? 'unnamed'}`;
  }
}

/**
 * Base class for an execution step in the query plan DAG.
 */
export class Step {
  public name: string = '';
  public dependencies: Set<Step> = new Set();
  public dependents: Set<Step> = new Set();
  public projections: Expression[] = [];
  public limit: number = Infinity;
  public condition: Expression | undefined = undefined;

  // Intermediate state for builders
  public operands: Expression[] = [];
  public aggregations: Expression[] = [];

  static fromExpression (
    expression?: Expression,
    ctes: Map<string, Step> = new Map(),
  ): Step | undefined {
    if (!expression) return undefined;
    const unnested = expression.unnest();
    const with_ = unnested.getArgKey('with');

    // Handle CTEs
    if (isInstanceOf(with_, WithExpr)) {
      ctes = new Map(ctes);
      for (const cte of with_.args.expressions || []) {
        assertIsInstanceOf(cte.args.this, Expression);
        const step = Step.fromExpression(cte.args.this, ctes);
        if (step) {
          step.name = cte.alias;
          ctes.set(step.name, step);
        }
      }
    }

    const from = unnested.args.from;
    let step: Step | undefined;

    if (unnested instanceof SelectExpr && from instanceof Expression) {
      assertIsInstanceOf(from.args.this, Expression);
      step = Scan.fromExpression(from.args.this, ctes);
    } else if (unnested instanceof SetOperationExpr) {
      step = SetOperation.fromExpression(unnested, ctes);
    } else {
      step = new Scan();
    }

    const joins = filterInstanceOf(unnested.args.joins || [], JoinExpr);
    if (joins && 0 < joins.length) {
      const joinStep = Join.fromJoins(joins, ctes);
      if (step) {
        joinStep.name = step.name;
        joinStep.sourceName = step.name;
        joinStep.addDependency(step);
      }
      step = joinStep;
    }

    const projections: Expression[] = [];
    const operands = new Map<Expression, string>();
    const aggregations = new Map<Expression, undefined>();
    const nextOperandName = nameSequence('_a_');

    const extractAggOperands = (expr: Expression): boolean => {
      const aggFuncs = Array.from(expr.findAll(AggFuncExpr));
      if (0 < aggFuncs.length) {
        aggregations.set(expr, undefined);
      }

      for (const agg of aggFuncs) {
        for (const operand of agg.unnestOperands()) {
          if (operand instanceof ColumnExpr) continue;
          if (!operands.has(operand)) {
            operands.set(operand, nextOperandName());
          }
          operand.replace(new ColumnExpr({
            this: new IdentifierExpr({
              this: operands.get(operand),
              quoted: true,
            }),
          }));
        }
      }
      return 0 < aggFuncs.length;
    };

    const setOpsAndAggs = (target: Step) => {
      target.operands = Array.from(operands.entries()).map(
        ([expr, aliasExpr]) => alias(
          expr,
          aliasExpr,
        ),
      );
      target.aggregations = Array.from(aggregations.keys());
    };

    for (const e of unnested.args.expressions || []) {
      assertIsInstanceOf(e, Expression);
      if (e.find(AggFuncExpr)) {
        projections.push(column(
          {
            col: e.aliasOrName,
            table: step?.name,
          },
          { quoted: true },
        ));
        extractAggOperands(e);
      } else {
        projections.push(e);
      }
    }

    const where = unnested.getArgKey('where');
    if (step && where instanceof Expression && where.args.this instanceof Expression) {
      step.condition = where.args.this;
    }

    const group = unnested.getArgKey('group');
    if (group || 0 < aggregations.size) {
      const aggregate = new Aggregate();
      if (step) {
        aggregate.source = step.name;
        aggregate.name = step.name;
      }

      const having = unnested.getArgKey('having');
      if (having instanceof Expression && isInstanceOf(having.args.this, 'string', Expression)) {
        if (extractAggOperands(alias(
          having.args.this,
          '_h',
          { quoted: true },
        ))) {
          aggregate.condition = column(
            {
              col: '_h',
              table: step?.name,
            },
            { quoted: true },
          );
        } else if (isInstanceOf(having.args.this, Expression)) {
          aggregate.condition = having.args.this;
        }
      }

      setOpsAndAggs(aggregate);

      const groupExprs = isInstanceOf(group, Expression) ? filterInstanceOf(group.args.expressions ?? [], Expression) : [];
      aggregate.group = Object.fromEntries(
        groupExprs.map((e, i) => [`_g${i}`, e]),
      );

      const intermediate = new Map<unknown, string>();
      Object.entries(aggregate.group).forEach(([k, v]) => {
        intermediate.set(v, k);
        if (v instanceof ColumnExpr) {
          intermediate.set(v.name, k);
        }
      });

      // Replace projections with group references
      for (const projection of projections) {
        for (const node of projection.walk()) {
          const name = intermediate.get(node);
          if (name) {
            node.replace(new ColumnExpr({
              this: name,
              table: step?.name,
            }));
          }
        }
      }

      aggregate.addDependency(step);
      step = aggregate;
    }

    const order = unnested.getArgKey('order');
    if (isInstanceOf(order, Expression)) {
      const sort = new Sort();
      sort.name = step?.name ?? '';
      sort.key = filterInstanceOf(order.args.expressions || [], Expression);
      sort.addDependency(step);
      step = sort;
    }

    if (step) step.projections = projections;

    const limit = unnested.getArgKey('limit');
    if (step && isInstanceOf(limit, Expression)) {
      step.limit = parseInt(limit.text('expression'));
    }

    return step;
  }

  addDependency (dependency?: Step): void {
    if (!dependency) return;
    this.dependencies.add(dependency);
    dependency.dependents.add(this);
  }

  /**
   * Returns a string representation of the execution step.
   */
  toString (): string {
    return this.toS();
  }

  /**
   * Recursive helper to build the formatted string tree.
   * @param level Indentation depth.
   */
  toS (level: number = 0): string {
    const indent = '  '.repeat(level);
    const nested = `${indent}    `;

    // Each subclass (Scan, Join, Aggregate) can provide specific context lines
    let context = this._toS(`${nested}  `);

    if (0 < context.length) {
      context = [`${nested}Context:`, ...context];
    }

    const lines: string[] = [
      `${indent}- ${this.id}`,
      ...context,
      `${nested}Projections:`,
    ];

    for (const expression of this.projections) {
      lines.push(`${nested}  - ${expression.sql()}`);
    }

    if (this.condition) {
      lines.push(`${nested}Condition: ${this.condition.sql()}`);
    }

    if (this.limit !== Infinity) {
      lines.push(`${nested}Limit: ${this.limit}`);
    }

    if (0 < this.dependencies.size) {
      lines.push(`${nested}Dependencies:`);
      for (const dependency of this.dependencies) {
        // Recursively call toS for each dependency with increased level
        lines.push(`  ${dependency.toS(level + 1)}`);
      }
    }

    return lines.join('\n');
  }

  /**
   * Property-like getter for the class name.
   */
  get typeName (): string {
    return this.constructor.name;
  }

  /**
   * Unique identifier for the step in the output.
   */
  get id (): string {
    const nameStr = this.name ? ` ${this.name}` : '';
    return `${this.typeName}:${nameStr} (${id(this)})`;
  }

  /**
   * Hook for subclasses to add extra descriptive lines (like Join conditions or Source tables).
   */
  _toS (_indent: string): string[] {
    return [];
  }
}

/**
 * Represents a leaf-level operation that reads from a table, subquery, or CTE.
 */
export class Scan extends Step {
  public source: ExpressionOrString | undefined = undefined;

  static fromExpression (
    expression: Expression,
    ctes: Map<string, Step> = new Map(),
  ): Step | undefined {
    const table = expression;
    const alias = expression.aliasOrName;

    if (expression instanceof SubqueryExpr) {
      const innerTable = expression.args.this;
      const step = innerTable !== undefined ? Step.fromExpression(innerTable, ctes) : undefined;
      if (step) step.name = alias;
      return step;
    }

    const step = new Scan();
    step.name = alias;
    step.source = expression;

    // If the table name matches a CTE, we add that CTE's step as a dependency
    const tableName = table.name;
    if (ctes.has(tableName)) {
      step.addDependency(ctes.get(tableName));
    }

    return step;
  }

  override _toS (indent: string): string[] {
    const source = this.source instanceof Expression ? this.source.sql() : this.source ?? '-static-';
    return [`${indent}Source: ${source}`];
  }
}

/**
 * Represents a join operation between a source step and one or more join targets.
 */
export class Join extends Step {
  public sourceName: string | undefined = undefined;
  public joins: Record<string, {
    side: string | undefined;
    joinKey: unknown[];
    sourceKey: unknown[];
    condition: Expression | undefined;
  }> = {};

  static fromJoins (
    joins: Iterable<JoinExpr>,
    ctes: Map<string, Step> = new Map(),
  ): Join {
    const step = new Join();

    for (const join of joins) {
      const {
        sourceKeys, joinKeys, on,
      } = joinCondition(join);

      step.joins[join.aliasOrName] = {
        side: join.side,
        joinKey: joinKeys || [],
        sourceKey: sourceKeys || [],
        condition: on || undefined,
      };

      // Each join target becomes a new dependency branch via a Scan
      if (join.args.this !== undefined) step.addDependency(Scan.fromExpression(join.args.this, ctes));
    }

    return step;
  }

  override _toS (indent: string): string[] {
    const lines = [`${indent}Source: ${this.sourceName || this.name}`];

    for (const [name, join] of Object.entries(this.joins)) {
      lines.push(`${indent}${name}: ${join.side || 'INNER'}`);

      const joinKeyStr = join.joinKey.map((key) => String(key)).join(', ');
      if (joinKeyStr) {
        lines.push(`${indent}Key: ${joinKeyStr}`);
      }

      if (join.condition) {
        lines.push(`${indent}On: ${join.condition.sql()}`);
      }
    }

    return lines;
  }
}

/**
 * Represents an aggregation step (GROUP BY / HAVING).
 */
export class Aggregate extends Step {
  public aggregations: Expression[] = [];
  public operands: Expression[] = [];
  public group: Record<string, Expression> = {};
  public source: ExpressionOrString | undefined = undefined;

  constructor () {
    super();
  }

  override _toS (indent: string): string[] {
    const lines: string[] = [`${indent}Aggregations:`];

    for (const expression of this.aggregations) {
      lines.push(`${indent}  - ${expression.sql()}`);
    }

    if (0 < Object.keys(this.group).length) {
      lines.push(`${indent}Group:`);
      for (const expression of Object.values(this.group)) {
        lines.push(`${indent}  - ${expression.sql()}`);
      }
    }

    if (this.condition) {
      lines.push(`${indent}Having:`);
      lines.push(`${indent}  - ${this.condition.sql()}`);
    }

    if (0 < this.operands.length) {
      lines.push(`${indent}Operands:`);
      for (const expression of this.operands) {
        lines.push(`${indent}  - ${expression.sql()}`);
      }
    }

    return lines;
  }
}

/**
 * Represents an ordering step (ORDER BY).
 */
export class Sort extends Step {
  public key: Expression[] = [];

  constructor () {
    super();
  }

  override _toS (indent: string): string[] {
    const lines: string[] = [`${indent}Key:`];

    for (const expression of this.key) {
      lines.push(`${indent}  - ${expression.sql()}`);
    }

    return lines;
  }
}

/**
 * Represents set operations like UNION, INTERSECT, and EXCEPT.
 */
export class SetOperation extends Step {
  public op: typeof SetOperationExpr; // Constructor for the specific SetOperation type
  public left: string | undefined;
  public right: string | undefined;
  public distinct: boolean;

  constructor (
    op: typeof SetOperationExpr,
    left: string | undefined,
    right: string | undefined,
    options: { distinct?: boolean } = {},
  ) {
    const { distinct = false } = options;
    super();
    this.op = op;
    this.left = left;
    this.right = right;
    this.distinct = distinct;
  }

  static fromExpression (
    expression?: SetOperationExpr,
    ctes: Map<string, Step> = new Map(),
  ): SetOperation | undefined {
    if (!(expression instanceof SetOperationExpr)) {
      throw new Error('Expected SetOperation expression');
    }

    const left = expression.args.this !== undefined ? Step.fromExpression(expression.args.this, ctes) : undefined;
    if (left) left.name = left.name || 'left';

    const right = expression.args.expression !== undefined ? Step.fromExpression(expression.args.expression, ctes) : undefined;
    if (right) right.name = right.name || 'right';

    const step = new SetOperation(
      expression._constructor as typeof SetOperationExpr,
      left?.name,
      right?.name,
      { distinct: Boolean(expression.args.distinct) },
    );

    if (left) step.addDependency(left);
    if (right) step.addDependency(right);

    const limit = expression.args.limit;
    if (isInstanceOf(limit, Expression)) {
      step.limit = parseInt(limit.text('expression'));
    }

    return step;
  }

  override _toS (indent: string): string[] {
    const lines: string[] = [];
    if (this.distinct) {
      lines.push(`${indent}Distinct: ${this.distinct}`);
    }
    return lines;
  }

  get typeName (): string {
    return this.op.name;
  }
}
