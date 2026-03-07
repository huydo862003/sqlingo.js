// https://github.com/tobymao/sqlglot/blob/main/sqlglot/expressions.py

import { DateTime } from 'luxon';
import {
  Dialect, type DialectType,
} from '../dialects/dialect';
import { Token } from '../tokens';
import {
  ensureIterable, splitNumWords,
} from '../helper';
import {
  assertIsInstanceOf, filterInstanceOf, isInstanceOf, narrowInstanceOf, isIterable, enumFromString,
  type Merge,
  multiInherit,
  type AddableObject, type RaddableObject,
  type SubtractableObject, type RsubtractableObject,
  type MultipliableObject, type RmultipliableObject,
  type TrueDivisibleObject, type RtrueDivisibleObject,
  type FloorDivisibleObject, type RfloorDivisibleObject,
  type ModableObject, type RmodableObject,
  type PowableObject, type RpowableObject,
  type NegatableObject,
  type InvertableObject,
  type IndexableObject,
} from '../port_internals';
import { traverseScope } from '../optimizer/scope';
import {
  ErrorLevel, ParseError,
} from '../errors';
import {
  parseOne, type ParseOptions,
} from '../parser';
import { registerFunc } from '../parser/function_registry';
import { normalizeIdentifiers } from '../optimizer';
import {
  dump, load,
} from '../serde';
import {
  ExpressionKey,
  CreateExprKind,
  JoinExprKind,
  DataTypeExprKind,
  AlterExprKind,
} from './types';
import type {
  RefreshExprKind,
  DescribeExprKind,
  KillExprKind,
  DeclareItemExprKind,
  SetItemExprKind,
  RecursiveWithSearchExprKind,
  ColumnDefExprKind,
  CommentExprKind,
  ColumnConstraintExprKind,
  DropExprKind,
  MultitableInsertsExprKind,
  GrantExprKind,
  GrantPrincipalExprKind,
  HistoricalDataExprKind,
  WindowSpecExprKind,
  AnalyzeExprKind,
  AnalyzeStatisticsExprKind,
  AnalyzeSampleExprKind,
  AnalyzeDeleteExprKind,
  AnalyzeValidateExprKind,
  JsonColumnDefExprKind,
  OpenJsonColumnDefExprKind,
  UseExprKind,
  IndexColumnConstraintExprKind,
  CopyExprKind,
  DistributedByPropertyExprKind,
  DictPropertyExprKind,
  LockingPropertyExprKind,
  RefreshTriggerPropertyExprKind,
  SetOperationExprKind,
  SelectExprKind,
  SessionParameterExprKind,
  PlaceholderExprKind,
  TimeSliceExprKind,
  TrimPosition,
} from './types';

export * from './types';

export const SQLGLOT_META = 'sqlglot.meta';
export const SQLGLOT_ANONYMOUS = 'sqlglot.anonymous';
export const TABLE_PARTS = [
  'this',
  'db',
  'catalog',
] as const;
export const COLUMN_PARTS = [
  'this',
  'table',
  'db',
  'catalog',
] as const;
export const POSITION_META_KEYS = [
  'line',
  'col',
  'start',
  'end',
] as const;

/**
 * Convert a value to boolean
 * @param value - Value to convert
 * @returns Boolean value
 */
function toBool (value: unknown): boolean {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    const lower = value.toLowerCase().trim();
    return lower === 'true' || lower === '1' || lower === 'yes';
  }
  return Boolean(value);
}

export type IntoType = string | typeof Expression | (string | typeof Expression)[];

/** Expression key enum */
export type PrimitiveExpressionValue = string | boolean | number;

export type ExpressionValue<E extends Expression = Expression> = E | PrimitiveExpressionValue;
export type ExpressionValueList<E extends Expression = Expression> = ExpressionValue<E>[];

export type ExpressionOrNumber<E extends Expression = Expression> = E | number;
export type ExpressionOrNumberList<E extends Expression = Expression> = ExpressionOrNumber<E>[];
export type ExpressionOrString<E extends Expression = Expression> = E | string;
export type ExpressionOrStringList<E extends Expression = Expression> = ExpressionOrString<E>[];

export type ExpressionOrBoolean<E extends Expression = Expression> = E | boolean;
export type ExpressionOrBooleanList<E extends Expression = Expression> = ExpressionOrBoolean<E>[];

/**
 * Base arguments that all Expression classes can accept.
 */
export interface BaseExpressionArgs {
  this?: ExpressionValue;
  expression?: ExpressionValue;
  expressions?: (ExpressionValue | ExpressionValueList)[];
  alias?: ExpressionOrString;
  isString?: boolean;
  to?: ExpressionOrString;
  from?: ExpressionOrString;
  joins?: Expression[];
  pivots?: Expression[];
  laterals?: Expression[];
}

/**
 * Base class for all SQL expressions in the AST.
 *
 * Expressions form a tree structure where each node can have:
 * - args: Named arguments (properties and child expressions)
 * - parent: Parent expression in the tree
 * - key: Expression type identifier
 *
 * @example
 * const col = new ColumnExpr({ this: new IdentifierExpr({ this: 'name' }) });
 */
export class Expression implements
  AddableObject<unknown, AddExpr>, RaddableObject<unknown, AddExpr>,
  SubtractableObject<unknown, SubExpr>, RsubtractableObject<unknown, SubExpr>,
  MultipliableObject<unknown, MulExpr>, RmultipliableObject<unknown, MulExpr>,
  TrueDivisibleObject<unknown, DivExpr>, RtrueDivisibleObject<unknown, DivExpr>,
  FloorDivisibleObject<unknown, IntDivExpr>, RfloorDivisibleObject<unknown, IntDivExpr>,
  ModableObject<unknown, ModExpr>, RmodableObject<unknown, ModExpr>,
  PowableObject<unknown, PowExpr>, RpowableObject<unknown, PowExpr>,
  NegatableObject<NegExpr>,
  InvertableObject<NotExpr>,
  IndexableObject<ExpressionValue, BracketExpr> {
  /** The key identifying this expression type */
  static key: ExpressionKey = ExpressionKey.EXPRESSION;

  /** Arguments/properties of this expression (child nodes, flags, etc.) */
  args: BaseExpressionArgs = {};

  /** Parent expression in the AST tree */
  parent?: Expression;

  /** The argument key this expression is stored under in its parent */
  argKey?: string;

  /** The index if this expression is in an array argument */
  index?: number;

  /** Comments associated with this expression */
  comments?: string[];

  /** Cached data type of this expression */
  private _type?: Expression;

  /** Metadata attached to this expression */
  meta: Record<string, unknown> = {};

  /** Cached hash value for this expression */
  private _hash?: string;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set(['this']);

  constructor (args: BaseExpressionArgs = {}) {
    this.args = args;
    for (const [argKey, value] of Object.entries(args)) {
      this.setParent(argKey, value);
    }
  }

  * [Symbol.iterator] (): Iterator<this['args']['expressions'] extends (infer U)[] | undefined ? U : never> {
    if ((this.constructor as typeof Expression).availableArgs.has('expressions')) {
      if (Array.isArray(this.args.expressions)) {
        for (const e of this.args.expressions) {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          yield e as any;
        }
      }
      return;
    }
    throw new Error(`'${this.constructor.name}' object is not iterable`);
  }

  /**
   * Extract text value from a named argument
   * @param key - The argument key to extract text from
   * @returns The text value, or empty string if not found
   */
  text (key: string): string {
    const field = this.getArgKey(key);
    if (typeof field === 'string') {
      return field;
    }
    if (field instanceof IdentifierExpr || field instanceof LiteralExpr || field instanceof VarExpr) {
      return typeof field.args.this === 'string'
        ? field.args.this
        : '';
    }
    if (field instanceof StarExpr || field instanceof NullExpr) {
      return field.name;
    }
    return '';
  }

  /**
   * Check if this expression is a string literal
   * @returns True if string literal
   */
  get isString (): boolean {
    return this instanceof LiteralExpr && this.args.isString === true;
  }

  /**
   * Check if this expression is a number literal
   * @returns True if number literal
   */
  get isNumber (): boolean {
    return (this instanceof LiteralExpr && !this.args.isString)
      || (this instanceof NegExpr && (this.args.this as Expression).isNumber);
  }

  /**
   * Returns a JavaScript value equivalent of the SQL node
   * @throws Error if the expression cannot be converted
   */
  toValue (): ExpressionValue | undefined {
    if (this instanceof LiteralExpr) {
      const value = this.args.this;
      if (this.isString) {
        return value;
      }
      if (typeof value === 'string') {
        return Number(value);
      }
      return value;
    }
    throw new Error(`${this.constructor.name} cannot be converted to a JavaScript value.`);
  }

  /**
   * Check if this expression is an integer literal
   * @returns True if integer literal
   */
  get isInteger (): boolean {
    if (!this.isNumber) {
      return false;
    }
    try {
      const value = this.toValue();
      return Number.isInteger(value);
    } catch {
      return false;
    }
  }

  /**
   * Check if this expression is a star (*) expression
   * @returns True if star expression
   */
  get isStar (): boolean {
    return this instanceof StarExpr
      || (this instanceof ColumnExpr && this.args.this instanceof StarExpr);
  }

  /**
   * Get the alias of this expression
   * @returns The alias name, or empty string if no alias
   */
  get alias (): string {
    if (this.args.alias instanceof TableAliasExpr) {
      return this.args.alias.name;
    }
    return this.text('alias');
  }

  /**
   * Get column names from table alias
   * @returns Array of column names
   */
  get aliasColumnNames (): string[] {
    const tableAlias = this.args.alias;
    if (!(tableAlias instanceof TableAliasExpr)) {
      return [];
    }
    const columns = tableAlias.args.columns;
    if (Array.isArray(columns)) {
      return columns.map((c: unknown) => (c instanceof Expression
        ? c.name
        : ''));
    }
    return [];
  }

  /**
   * Get the name of this expression (extracted from 'this' argument)
   * @returns The expression name
   */
  get name (): string {
    return this.text('this');
  }

  /**
   * Get the alias if present, otherwise the name
   * @returns Alias or name
   */
  get aliasOrName (): string {
    return this.alias || this.name;
  }

  /**
   * Get the output name (alias or name)
   * @returns The output name for this expression
   */
  get outputName (): string {
    return '';
  }

  /**
   * Get the data type of this expression
   * @returns DataType expression or undefined
   */
  get type (): string | Expression | undefined {
    if (this instanceof CastExpr) {
      return this._type || this.args.to;
    }
    return this._type;
  }

  /**
   * Set the data type for this expression
   * @param dtype - Data type
   */
  set type (dtype: Expression | string | undefined) {
    if (dtype !== undefined && !(dtype instanceof DataTypeExpr)) {
      dtype = DataTypeExpr.build(dtype);
    }
    this._type = dtype;
  }

  /**
   * Check if this expression has any of the specified data types
   * @param dtypes - Data type names to check
   * @returns True if expression has one of the specified types
   */
  isType (
    dtypes: string | DataTypeExprKind | Expression | Iterable<DataTypeExprKind | Expression | string>,
    _options?: { checkNullable?: boolean },
  ): boolean {
    if (!this._type) {
      return false;
    }
    return this._type.isType(ensureIterable(dtypes));
  }

  /**
   * Check if this expression is a leaf node (has no child expressions)
   * @returns True if this expression has no Expression or list children
   */
  get isLeaf (): boolean {
    return !Object.values(this.args).some((v) =>
      (v instanceof Expression || Array.isArray(v)) && v);
  }

  copy (): this {
    const root = new (this.constructor as new () => this)();
    const stack: [Expression, Expression][] = [[this, root]];

    while (0 < stack.length) {
      const [node, copy] = stack.pop()!;
      if (node.comments) {
        copy.comments = [...node.comments];
      }
      if (node._type) {
        copy._type = node._type.copy() as DataTypeExpr;
      }
      if (node.meta) {
        copy.meta = { ...node.meta };
      }
      if (node._hash) {
        copy._hash = node._hash;
      }

      for (const [k, vs] of Object.entries(node.args)) {
        if (vs instanceof Expression) {
          const childCopy = new (vs.constructor as new () => Expression)();
          stack.push([vs, childCopy]);
          copy.setArgKey(k, childCopy);
        } else if (Array.isArray(vs)) {
          (copy.args as Record<string, unknown>)[k] = [];
          for (const v of vs) {
            if (v instanceof Expression) {
              const childCopy = new (v.constructor as new () => Expression)();
              stack.push([v, childCopy]);
              copy.append(k, childCopy);
            } else {
              copy.append(k, v);
            }
          }
        } else {
          (copy.args as Record<string, unknown>)[k] = vs;
        }
      }
    }
    return root;
  }

  /**
   * Add comments to this expression
   * @param comments - Array of comment strings to add
   * @param options
   * @param options.prepend - If true, prepend comments instead of appending
   */
  addComments (comments?: Iterable<string>, options: { prepend?: boolean } = {}): void {
    const { prepend = false } = options;
    if (!this.comments) {
      this.comments = [];
    }

    if (comments) {
      for (const comment of comments) {
        const [_, ...meta] = comment.split(SQLGLOT_META);

        if (0 < meta.length) {
          for (const kv of meta.join('').split(',')) {
            const [key, ...valueParts] = kv.split('=');
            const value = 0 < valueParts.length
              ? valueParts[0].trim()
              : true;
            this.meta[key.trim()] = toBool(value);
          }
        }

        if (!prepend) {
          this.comments.push(comment);
        }
      }

      if (prepend) {
        this.comments = [...comments, ...this.comments];
      }
    }
  }

  /**
   * Remove and return all comments from this expression
   * @returns Array of comment strings
   */
  popComments (): string[] {
    const comments = this.comments || [];
    this.comments = undefined;
    return comments;
  }

  /**
   * Appends value to arg_key if it's a list or sets it as a new list
   * @param argKey - Name of the list expression arg
   * @param value - Value to append to the list
   */
  append (argKey: string, value: ExpressionValue | undefined): void {
    const args = this.args as Record<string, unknown>;
    if (!Array.isArray(args[argKey])) {
      args[argKey] = [];
    }
    this.setParent(argKey, value);
    const values = args[argKey] as unknown[];
    if (value instanceof Expression) {
      value.index = values.length;
    }
    values.push(value);
  }

  /**
   * Sets arg_key to value
   * @param argKey - Name of the expression arg
   * @param value - Value to set the arg to
   * @param index - If the arg is a list, this specifies what position to add the value in it
   * @param options
   * @param options.overwrite - If an index is given, determines whether to overwrite the list entry
   */
  setArgKey (
    argKey: string,
    value: ExpressionValue | (ExpressionValue | ExpressionValueList)[] | undefined,
    index?: number,
    options?: { overwrite?: boolean },
  ): void {
    const overwrite = options?.overwrite ?? true;
    // Clear hash cache up the tree
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let expression: Expression | undefined = this;
    while (expression && expression._hash !== undefined) {
      expression._hash = undefined;
      expression = expression.parent;
    }

    const args = this.args as Record<string, ExpressionValue | ExpressionValueList>;

    if (index !== undefined) {
      const expressions = (args[argKey] || []) as ExpressionValue[] | ExpressionValueList[];

      if (expressions[index] === undefined) {
        return;
      }

      if (value === undefined) {
        expressions.splice(index, 1);
        for (let i = index; i < expressions.length; i++) {
          const v = expressions[i];
          if (v instanceof Expression && v.index !== undefined) {
            v.index = v.index - 1;
          }
        }
        return;
      }

      if (Array.isArray(value)) {
        expressions.splice(index, 1);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        expressions.splice(index, 0, ...(value as any[]));
      } else if (overwrite) {
        expressions[index] = value;
      } else {
        expressions.splice(index, 0, value);
      }

      value = expressions;
    } else if (value === undefined) {
      delete args[argKey];
      return;
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    args[argKey] = value as any;
    this.setParent(argKey, value, index);
  }

  getArgKey (argKey?: string): ExpressionValue | ExpressionValueList | undefined {
    if (argKey === undefined) return undefined;
    return (this.args as Record<string, ExpressionValueList | ExpressionValue>)[argKey] ?? undefined;
  }

  private setParent (argKey: string, value: unknown, index?: number): void {
    if (value instanceof Expression) {
      value.parent = this;
      value.argKey = argKey;
      value.index = index;
    } else if (Array.isArray(value)) {
      value.forEach((v, i) => {
        if (v instanceof Expression) {
          v.parent = this;
          v.argKey = argKey;
          v.index = i;
        }
      });
    }
  }

  /**
   * Get the depth of this expression in the tree (distance from root)
   * @returns Depth level (0 = root)
   */
  get depth (): number {
    let depth = 0;
    let node: Expression | undefined = this.parent;
    while (node) {
      depth++;
      node = node.parent;
    }
    return depth;
  }

  * iterExpressions (options?: { reverse?: boolean }): Generator<Expression> {
    const reverse = options?.reverse ?? false;
    const argValues = reverse
      ? Object.values(this.args).reverse()
      : Object.values(this.args);
    for (const value of argValues) {
      if (value instanceof Expression) {
        yield value;
      } else if (Array.isArray(value)) {
        const items = reverse
          ? [...value].reverse()
          : value;
        for (const item of items) {
          if (item instanceof Expression) {
            yield item;
          }
        }
      }
    }
  }

  /**
   * Find the first expression of specified type(s) in the tree
   * @param expressionTypes - Array of expression class constructors to match
   * @param options - Options object
   * @param options.bfs - Use breadth-first search (default: true)
   * @returns First matching expression or undefined
   */
  find<T extends Expression>(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expressionTypes: (new (args: any) => T) | Readonly<Iterable<(new (args: any) => T)>>,
    options?: { bfs?: boolean },
  ): T | undefined {
    for (const expr of this.findAll(expressionTypes, options)) {
      return expr;
    }
    return undefined;
  }

  /**
   * Returns a generator object which visits all nodes in this tree and only
   * yields those that match at least one of the specified expression types.
   *
   * @param expressionTypes - the expression type(s) to match
   * @param options - Options object
   * @param options.bfs - whether to search the AST using the BFS algorithm (DFS is used if false)
   * @returns The generator object
   */
  * findAll<T extends Expression>(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expressionTypes: (new (args: any) => T) | Readonly<Iterable<(new (args: any) => T)>>,
    options?: { bfs?: boolean },
  ): Generator<T> {
    const types = Array.from(ensureIterable(expressionTypes));

    const bfs = options?.bfs ?? true;
    for (const expression of this.walk({ bfs })) {
      if (types.some((type) => expression instanceof type)) {
        yield expression as T;
      }
    }
  }

  /**
   * Find the nearest ancestor expression of specified type(s)
   * @param expressionTypes - Array of expression class constructors to match
   * @returns First matching ancestor or undefined
   */
  findAncestor<T extends Expression>(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ...expressionTypes: (new (args: any) => T)[]
  ): T | undefined {
    let node: Expression | undefined = this.parent;
    while (node) {
      if (expressionTypes.some((type) => node instanceof type)) {
        return node as T;
      }
      node = node.parent;
    }
    return undefined;
  }

  /**
   * Returns the parent select statement.
   */
  get parentSelect (): Expression | undefined {
    return this.findAncestor(SelectExpr);
  }

  /**
   * Returns if the parent is the same class as itself.
   */
  get sameParent (): boolean {
    return this.parent?.constructor === this.constructor;
  }

  sameParentAs (other: Expression): boolean {
    return this.parent === other.parent;
  }

  root (): Expression {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let node: Expression = this;
    while (node.parent) {
      node = node.parent;
    }
    return node;
  }

  /**
   * Generator that walks the expression tree using BFS or DFS.
   * @param options - Options object
   * @param options.bfs - whether to use breadth-first search (default: true), DFS if false
   * @param options.prune - optional function to determine if a node's children should be pruned
   * @returns Generator yielding all expressions in the tree
   */
  * walk (options?: { bfs?: boolean;
    prune?: (node: Expression) => boolean; }): Generator<Expression> {
    const bfs = options?.bfs ?? true;
    const prune = options?.prune;
    if (bfs) {
      yield* this.bfs({ prune });
    } else {
      yield* this.dfs({ prune });
    }
  }

  /**
   * Returns a generator object which visits all nodes in this tree in
   * the DFS (Depth-first) order.
   *
   * @param options - Options object
   * @param options.prune - optional function to determine if a node's children should be pruned
   * @returns The generator object
   */
  * dfs (options?: { prune?: (node: Expression) => boolean }): Generator<Expression> {
    const prune = options?.prune;
    const stack: Expression[] = [this];

    while (0 < stack.length) {
      const node = stack.pop()!;

      yield node;

      if (prune?.(node)) {
        continue;
      }

      for (const v of node.iterExpressions({ reverse: true })) {
        stack.push(v);
      }
    }
  }

  /**
   * Returns a generator object which visits all nodes in this tree in
   * the BFS (Breadth-first) order.
   *
   * @param options - Options object
   * @param options.prune - optional function to determine if a node's children should be pruned
   * @returns The generator object
   */
  * bfs (options?: { prune?: (node: Expression) => boolean }): Generator<Expression> {
    const prune = options?.prune;
    const queue: Expression[] = [this];

    while (0 < queue.length) {
      const node = queue.shift()!;

      yield node;

      if (prune?.(node)) {
        continue;
      }

      for (const v of node.iterExpressions()) {
        queue.push(v);
      }
    }
  }

  /**
   * Returns the first non-parenthesis child or self.
   */
  unnest (): Expression {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let expression: Expression = this;
    while (expression instanceof ParenExpr) {
      const thisArg = expression.args.this;
      if (thisArg instanceof Expression) {
        expression = thisArg;
      } else {
        break;
      }
    }
    return expression;
  }

  /**
   * Returns the inner expression if this is an Alias.
   */
  unalias (): Expression {
    if (this instanceof AliasExpr) {
      const thisArg = this.args.this;
      if (thisArg instanceof Expression) {
        return thisArg;
      }
    }
    return this;
  }

  /**
   * Returns unnested operands as a tuple.
   */
  unnestOperands (): Expression[] {
    return Array.from(this.iterExpressions()).map((arg) => arg.unnest());
  }

  /**
   * Returns a generator which yields child nodes whose parents are the same class.
   * A AND B AND C -> [A, B, C]
   *
   * @param options - Options object
   * @param options.unnest - whether to unwrap parentheses (default: true)
   */
  * flatten (options?: { unnest?: boolean }): Generator<Expression> {
    const unnest = options?.unnest ?? true;
    for (const node of this.dfs({ prune: (n) => n.parent !== undefined && n.constructor !== this.constructor })) {
      if (node.constructor !== this.constructor) {
        if (unnest && !(node instanceof SubqueryExpr)) {
          yield node.unnest();
        } else {
          yield node;
        }
      }
    }
  }

  sql (options: {
    dialect?: DialectType;
    [index: string]: unknown;
  } = {}): string {
    const {
      dialect, ...restOptions
    } = options;
    const dialectInstance = Dialect.getOrRaise(dialect);
    return dialectInstance.generate(this, restOptions);
  }

  /**
   * Visits all tree nodes (excluding already transformed ones)
   * and applies the given transformation function to each node.
   *
   * @param func - a function which takes a node and kwargs object, and returns a
   *               new transformed node or the same node without modifications. If the function
   *               returns undefined, then the corresponding node will be removed from the
   *               syntax tree.
   * @param options - Options object
   * @param options.copy - if set to true a new tree instance is constructed, otherwise the tree is
   *                       modified in place (default: true)
   * @returns The transformed tree
   */
  transform (
    func: (node: Expression, options: Record<string, unknown>) => string | Expression | Expression[] | undefined,
    options: {
      copy?: boolean;
      [index: string]: unknown;
    } = {},
  ): Expression {
    const {
      copy = true, ...restOptions
    } = options;

    let root: string | Expression | Expression[] | undefined;
    let newNode: string | Expression | Expression[] | undefined;

    const startNode = copy
      ? this.copy()
      : this;

    for (const node of startNode.dfs({ prune: (n) => n !== newNode })) {
      const parent = node.parent;
      const argKey = node.argKey;
      const index = node.index;

      newNode = func(node, {
        ...restOptions,
        copy,
      });

      if (!root) {
        root = newNode;
      } else if (parent && argKey && newNode !== node) {
        parent.setArgKey(argKey, newNode, index);
      }
    }

    if (!root) {
      throw new Error('Transform failed: no root node');
    }

    if (!(root instanceof Expression)) {
      throw new Error('Expected final root to be an Expression');
    }

    return root;
  }

  /**
   * Swap out this expression with a new expression.
   *
   * For example:
   *   const tree = new SelectExpr(...).select("x").from("tbl");
   *   tree.find([ColumnExpr]).replaceWith(new ColumnExpr({this: "y"}));
   *   tree.sql() // 'SELECT y FROM tbl'
   *
   * @param expression - new node (or undefined to remove)
   * @returns The new expression
   */
  replace<E extends Expression>(expression: E): E;
  replace<E extends Expression>(expression: ExpressionValue<E>): ExpressionValue<E>;
  replace<E extends Expression>(expression: ExpressionValue<E>[]): ExpressionValue<E>[];
  replace<E extends Expression>(expression: ExpressionValue<E> | undefined): ExpressionValue<E> | undefined;
  replace (expression: undefined): undefined;
  replace<E extends Expression>(expression: ExpressionValue<E> | ExpressionValue<E>[] | undefined): ExpressionValue<E> | ExpressionValue<E>[] | undefined {
    const parent = this.parent;

    if (!parent || parent === expression) {
      return expression;
    }

    const key = this.argKey;
    if (!key) {
      return expression;
    }

    const value = parent.getArgKey(key);

    if (Array.isArray(expression) && value instanceof Expression) {
      // We are trying to replace an Expression with a list, so it's assumed that
      // the intention was to really replace the parent of this expression.
      value.parent?.replace(expression);
    } else {
      parent.setArgKey(key, expression, this.index);
    }

    if (expression !== this as unknown) {
      this.parent = undefined;
      this.argKey = undefined;
      this.index = undefined;
    }

    return expression;
  }

  /**
   * Remove this expression from its AST.
   *
   * @returns The popped expression
   */
  pop (): this {
    this.replace(undefined);
    return this;
  }

  /**
   * Assert that this Expression is an instance of the specified type.
   *
   * If it is NOT an instance of type, this raises an assertion error.
   * Otherwise, this returns this expression.
   *
   * This is useful for type security in chained expressions:
   *
   * @example
   * ```typescript
   * parse_one("SELECT x from y").assertIs(SelectExpr).select("z").sql()
   * // 'SELECT x, z FROM y'
   * ```
   *
   * @param type - the class constructor to check against
   * @returns This expression, typed as the specified type
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  assertIs<T extends Expression>(type: new (args: any) => T): T {
    if (!(this instanceof type)) {
      throw new Error(`${this.constructor.name} is not ${type.name}.`);
    }
    return this as T;
  }

  /**
   * Checks if this expression is valid (e.g. all mandatory args are set).
   *
   * @param args - a sequence of values that were used to instantiate a Func expression.
   *               This is used to check that the provided arguments don't exceed the function
   *               argument limit.
   * @returns A list of error messages for all possible errors that were found
   */
  errorMessages (args?: unknown[]): string[] {
    const errors: string[] = [];

    // Check for required arguments
    const constructor = this.constructor as typeof Expression;
    for (const key of constructor.requiredArgs) {
      const v = (this.args as Record<string, ExpressionValue | ExpressionValueList>)[key];
      if (v === undefined || (Array.isArray(v) && v.length === 0)) {
        errors.push(`Required keyword: '${key}' missing for ${this.constructor.name}`);
      }
    }

    // Check for too many arguments in Func expressions
    if (args && this instanceof FuncExpr) {
      const argTypeCount = constructor.availableArgs.size;
      // Check if this function accepts variable-length arguments
      // (e.g., CONCAT, COALESCE can take any number of arguments)
      const isVarLen = (constructor as typeof FuncExpr).isVarLenArgs || false;
      if (argTypeCount < args.length && !isVarLen) {
        errors.push(
          `The number of provided arguments (${args.length}) is greater than `
          + `the maximum number of supported arguments (${argTypeCount})`,
        );
      }
    }

    return errors;
  }

  /**
   * Dump this Expression to a JSON-serializable list.
   */
  dump (): Record<string, unknown>[] {
    return dump(this);
  }

  /**
   * Load a list (as returned by Expression.dump) into an Expression instance.
   */
  static load (obj?: Record<string, unknown>[]): Expression | undefined {
    return load(obj);
  }

  /**
   * AND this condition with one or multiple expressions.
   *
   * @example
   * ```typescript
   * condition("x=1").and([condition("y=1")]).sql()
   * // 'x = 1 AND y = 1'
   *
   * // Without copying or wrapping
   * condition("x=1").and([condition("y=1")], { copy: false, wrap: false })
   * ```
   *
   * @param expressions - The expressions to AND with this condition
   * @param options - Options object
   * @param options.dialect - The dialect to use for parsing
   * @param options.copy - Whether to copy the involved expressions (default: true)
   * @param options.wrap - Whether to wrap operands in Parens to avoid precedence issues (default:
   * true)
   * @returns The new AND condition
   */
  and (
    expressions?: ExpressionValue | (ExpressionValue | undefined)[],
    options: {
      dialect?: DialectType;
      copy?: boolean;
      wrap?: boolean;
    } = {},
  ): ConditionExpr {
    const {
      copy = true, wrap = true, ...restOptions
    } = options;
    const expressionList = Array.from(ensureIterable(expressions));
    return and([this, ...expressionList] as (string | Expression | undefined)[], {
      ...restOptions,
      copy,
      wrap,
    });
  }

  /**
   * OR this condition with one or multiple expressions.
   *
   * @example
   * ```typescript
   * condition("x=1").or([condition("y=1")]).sql()
   * // 'x = 1 OR y = 1'
   *
   * // Without copying or wrapping
   * condition("x=1").or([condition("y=1")], { copy: false, wrap: false })
   * ```
   *
   * @param expressions - The expressions to OR with this condition
   * @param options - Options object
   * @param options.dialect - The dialect to use for parsing
   * @param options.copy - Whether to copy the involved expressions (default: true)
   * @param options.wrap - Whether to wrap operands in Parens to avoid precedence issues (default:
   * true)
   * @returns The new OR condition
   */
  or (
    expressions?: ExpressionValue | (ExpressionValue | undefined)[],
    options: {
      dialect?: DialectType;
      copy?: boolean;
      wrap?: boolean;
    } = {},
  ): ConditionExpr {
    const {
      copy = true, wrap = true, ...restOptions
    } = options;
    const expressionList = Array.from(ensureIterable(expressions));
    return or([this, ...expressionList] as (string | Expression | undefined)[], {
      ...restOptions,
      copy,
      wrap,
    });
  }

  /**
   * Wrap this condition with NOT.
   *
   * @example
   * ```typescript
   * condition("x=1").negate().sql()
   * // 'NOT x = 1'
   *
   * // Without copying
   * condition("x=1").negate({ copy: false })
   * ```
   *
   * @param options - Options object
   * @param options.copy - Whether to copy this object (default: true)
   * @returns The new NOT instance
   */
  not (options: { copy?: boolean } = {}): NotExpr {
    const {
      copy = true, ...restOptions
    } = options;
    return not(this, {
      ...restOptions,
      copy,
    });
  }

  /**
   * Update this expression with positions from a token or other expression.
   *
   * @param other - a token or expression to update this expression with
   * @param positions - position values to use if other is not provided
   * @param positions.line - the line number
   * @param positions.col - column number
   * @param positions.start - start char index
   * @param positions.end - end char index
   * @returns The updated expression
   */
  updatePositions (
    other?: Token | Expression,
    positions?: {
      line?: number;
      col?: number;
      start?: number;
      end?: number;
    },
  ): this {
    if (!other) {
      this.meta.line = positions?.line;
      this.meta.col = positions?.col;
      this.meta.start = positions?.start;
      this.meta.end = positions?.end;
    } else if (other instanceof Expression) {
      for (const key of POSITION_META_KEYS) {
        this.meta[key] = other.meta[key];
      }
    } else {
      // Copy from token-like object
      this.meta.line = other.line;
      this.meta.col = other.col;
      this.meta.start = other.start;
      this.meta.end = other.end;
    }
    return this;
  }

  /**
   * Create an alias for this expression.
   *
   * @example
   * ```typescript
   * column("x").as("y")  // "x AS y"
   * column("x").as("y", { quoted: true })  // "x AS "y""
   * ```
   *
   * @param alias - the alias name (string or Identifier expression)
   * @param options - Options object
   * @param options.quoted - whether to quote the alias
   * @param options.dialect - the dialect to use for parsing
   * @param options.copy - whether to copy this expression (default: true)
   * @param options.wrap - whether to wrap in parentheses (default: true)
   * @returns The Alias expression
   */
  as (
    _alias: PrimitiveExpressionValue | IdentifierExpr,
    options: {
      quoted?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      wrap?: boolean;
      [index: string]: unknown;
    } = {},
  ): AliasExpr {
    const {
      copy = true, ...restOptions
    } = options;
    const aliasName = _alias instanceof Expression
      ? _alias.name
      : _alias.toString();
    return alias(this, aliasName, {
      ...restOptions,
      copy,
    }) as AliasExpr; // NOTE: This is unsafe, needs verification
  }

  /**
   * Create a binary operation expression.
   * Internal helper for operator methods.
   *
   * @param klass - the binary expression class constructor
   * @param _other - the right-hand side operand
   * @param options - Options object
   * @param options.reverse - whether to reverse the operands (default: false)
   * @returns The binary expression
   */
  protected binop<T extends Expression>(
    klass: new (arg: {
      this?: Expression;
      expression?: Expression;
      [index: string]: unknown | undefined;
    }) => T,
    _other: unknown,
    options?: { reverse?: boolean },
  ): T {
    const reverse = options?.reverse ?? false;
    let self: Expression = this.copy();
    let other = convert(_other, { copy: true });
    if (!(self instanceof klass) && !(_other instanceof klass)) {
      const wrappedSelf = wrap(self, BinaryExpr);
      const wrappedOther = wrap(other, BinaryExpr);
      if (wrappedSelf) self = wrappedSelf;
      if (wrappedOther) other = wrappedOther;
    }
    if (reverse) {
      return new klass({
        this: other,
        expression: self,
      });
    }
    return new klass({
      this: self,
      expression: other,
    });
  }

  resetHash () {
    this._hash = undefined;
  }

  computeHash (): string {
    return this.hash();
  }

  hash (): string {
    if (this._hash !== undefined) {
      return this._hash;
    }

    const nodes: Expression[] = [];
    const queue: Expression[] = [this];

    while (0 < queue.length) {
      const node = queue.shift()!;
      nodes.push(node);
      for (const child of node.iterExpressions()) {
        if (child._hash === undefined) {
          queue.push(child);
        }
      }
    }

    for (let i = nodes.length - 1; 0 <= i; i--) {
      const node = nodes[i];
      let hash = this.hashString(node._constructor.key);

      if (node instanceof LiteralExpr || node instanceof IdentifierExpr) {
        const sortedEntries = Object.entries(node.args).sort();
        for (const [k, v] of sortedEntries) {
          if (v) {
            hash = this.hashString(hash + k + v.toString());
          }
        }
      } else {
        const sortedEntries = Object.entries(node.args);
        for (const [k, v] of sortedEntries) {
          if (Array.isArray(v)) {
            for (const x of v) {
              if (x !== undefined && x !== false) {
                const hashValue = typeof x === 'string'
                  ? x.toLowerCase()
                  : x;
                hash = this.hashString(hash + k + hashValue);
              } else {
                hash = this.hashString(hash + k);
              }
            }
          } else if (v !== undefined && v !== false) {
            const hashValue = typeof v === 'string'
              ? v.toLowerCase()
              : v;
            hash = this.hashString(hash + k + hashValue);
          }
        }
      }
      node._hash = hash;
    }
    return this._hash || '';
  }

  equals (other: unknown): boolean {
    if (this === other) return true;
    if (!(other instanceof Expression)) return false;
    if (this.constructor !== other.constructor) return false;
    return this.hash() === other.hash();
  }

  private hashString (str: string): string {
    return str.split('')
      .reduce((a, b) => {
        a = ((a << 5) - a) + b.charCodeAt(0);
        return a & a;
      }, 0)
      .toString();
  }

  toString (): string {
    return this.sql();
  }

  /**
   * Create an IN expression.
   *
   * @param expressions - The values to check against
   * @param options - Options object
   * @param options.query - Optional subquery expression
   * @param options.unnest - Optional unnest expression(s)
   * @param options.copy - Whether to copy this expression (default: true)
   * @returns The IN expression
   */
  in (
    expressions?: (ExpressionValue | ExpressionValueList)[],
    query?: ExpressionValue,
    options: {
      unnest?: ExpressionValue | ExpressionValueList;
      copy?: boolean;
    } = {},
  ): InExpr {
    const {
      copy = true, unnest, ...restOptions
    } = options;

    let subquery = query
      ? maybeParse(query)
      : undefined;

    // NOTE: The original sqlglot doesn't check that subquery is a QueryExpr. However, after a quick scan, only QueryExpr has a `subquery` method, so I added this check
    if (!(subquery instanceof SubqueryExpr) && subquery instanceof QueryExpr) {
      subquery = subquery.subquery(undefined, { copy: false });
    }

    return new InExpr({
      this: maybeCopy(this, copy),
      expressions: expressions?.map((e) => convert(e, { copy })),
      query: subquery,
      unnest: unnest
        ? new UnnestExpr({
          expressions: Array.from(ensureIterable(unnest)).map((e) => maybeParse(e as ExpressionValue, {
            ...restOptions,
            copy,
          })),
        })
        : undefined,
    });
  }

  /**
   * Create a BETWEEN expression.
   *
   * @param low - The lower bound
   * @param high - The upper bound
   * @param options - Options object
   * @param options.copy - Whether to copy this expression (default: true)
   * @param options.symmetric - Whether this is a symmetric between (optional)
   * @returns The BETWEEN expression
   */
  between (
    low: unknown,
    high: unknown,
    options: {
      copy?: boolean;
      symmetric?: boolean;
    } = {},
  ): BetweenExpr {
    const {
      copy = true, symmetric,
    } = options;

    const between = new BetweenExpr({
      this: maybeCopy(this, copy),
      low: convert(low, { copy }),
      high: convert(high, { copy }),
    });

    if (symmetric !== undefined) {
      between.setArgKey('symmetric', symmetric);
    }

    return between;
  }

  /**
   * Create an IS expression.
   */
  is (other?: ExpressionValue): IsExpr {
    return this.binop(IsExpr, other);
  }

  /**
   * Create a LIKE expression.
   */
  like (other?: ExpressionValue): LikeExpr {
    return this.binop(LikeExpr, other);
  }

  /**
   * Create an ILIKE expression.
   */
  ilike (other?: ExpressionValue): ILikeExpr {
    return this.binop(ILikeExpr, other);
  }

  /**
   * Create an EQ (equals) expression.
   */
  eq (other?: unknown): EqExpr {
    return this.binop(EqExpr, other);
  }

  /**
   * Create a NEQ (not equals) expression.
   */
  neq (other?: unknown): NeqExpr {
    return this.binop(NeqExpr, other);
  }

  /**
   * Create a REGEXP_LIKE expression.
   */
  rlike (other?: ExpressionValue): RegexpLikeExpr {
    return this.binop(RegexpLikeExpr, other);
  }

  /**
   * Create a DIV expression with optional typed and safe flags.
   */
  div (other?: ExpressionValue, options?: {
    typed?: boolean;
    safe?: boolean;
  }): DivExpr {
    const div = this.binop(DivExpr, other);
    div.setArgKey('typed', options?.typed ?? false);
    div.setArgKey('safe', options?.safe ?? false);
    return div;
  }

  /**
   * Create an ascending ORDER BY expression.
   */
  asc (options: { nullsFirst?: boolean } = {}): OrderedExpr {
    const {
      nullsFirst = true,
    } = options;
    return new OrderedExpr({
      this: this.copy(),
      nullsFirst: convert(nullsFirst),
    });
  }

  /**
   * Create a descending ORDER BY expression.
   */
  desc (options: { nullsFirst?: boolean } = {}): OrderedExpr {
    const {
      nullsFirst = false,
    } = options;
    return new OrderedExpr({
      this: this.copy(),
      desc: convert(true),
      nullsFirst: convert(nullsFirst),
    });
  }

  // Comparison operators

  /**
   * Create an LT (less than) expression.
   */
  lt (other?: ExpressionValue): LtExpr {
    return this.binop(LtExpr, other);
  }

  /**
   * Create an LTE (less than or equal) expression.
   */
  lte (other?: ExpressionValue): LteExpr {
    return this.binop(LteExpr, other);
  }

  /**
   * Create a GT (greater than) expression.
   */
  gt (other?: ExpressionValue): GtExpr {
    return this.binop(GtExpr, other);
  }

  /**
   * Create a GtE (greater than or equal) expression.
   */
  gte (other?: ExpressionValue): GteExpr {
    return this.binop(GteExpr, other);
  }

  // Arithmetic operators

  /**
   * Create an ADD expression.
   */
  add (other?: ExpressionValue): AddExpr {
    return this.binop(AddExpr, other);
  }

  /**
   * Create an ADD expression (reversed operands).
   */
  radd (other?: ExpressionValue): AddExpr {
    return this.binop(AddExpr, other, { reverse: true });
  }

  /**
   * Create a SUB expression.
   */
  sub (other?: ExpressionValue): SubExpr {
    return this.binop(SubExpr, other);
  }

  /**
   * Create a SUB expression (reversed operands).
   */
  rsub (other?: ExpressionValue): SubExpr {
    return this.binop(SubExpr, other, { reverse: true });
  }

  /**
   * Create a MUL expression.
   */
  mul (other?: ExpressionValue): MulExpr {
    return this.binop(MulExpr, other);
  }

  /**
   * Create a MUL expression (reversed operands).
   */
  rmul (other?: ExpressionValue): MulExpr {
    return this.binop(MulExpr, other, { reverse: true });
  }

  /**
   * Create a DIV expression.
   */
  truediv (other?: ExpressionValue): DivExpr {
    return this.binop(DivExpr, other);
  }

  /**
   * Create a DIV expression (reversed operands).
   */
  rtruediv (other?: ExpressionValue): DivExpr {
    return this.binop(DivExpr, other, { reverse: true });
  }

  /**
   * Create an INTDIV expression.
   */
  floorDiv (other?: ExpressionValue): IntDivExpr {
    return this.binop(IntDivExpr, other);
  }

  /**
   * Create an INTDIV expression (reversed operands).
   */
  rfloorDiv (other?: ExpressionValue): IntDivExpr {
    return this.binop(IntDivExpr, other, { reverse: true });
  }

  /**
   * Create a MOD expression.
   */
  mod (other?: ExpressionValue): ModExpr {
    return this.binop(ModExpr, other);
  }

  /**
   * Create a MOD expression (reversed operands).
   */
  rmod (other?: ExpressionValue): ModExpr {
    return this.binop(ModExpr, other, { reverse: true });
  }

  /**
   * Create a POW expression.
   */
  pow (other?: ExpressionValue): PowExpr {
    return this.binop(PowExpr, other);
  }

  /**
   * Create a POW expression (reversed operands).
   */
  rpow (other?: ExpressionValue): PowExpr {
    return this.binop(PowExpr, other, { reverse: true });
  }

  neg (): NegExpr {
    return new NegExpr({
      this: wrap(this.copy(), BinaryExpr),
    });
  }

  invert (): NotExpr {
    return not(this.copy());
  }

  getItem (...other: ExpressionValue[]): BracketExpr {
    return new BracketExpr({
      this: this.copy(),
      expressions: (Array.isArray(other) ? other : [other]).map((e) => convert(e, { copy: true })),
    });
  }

  get _constructor (): typeof Expression {
    return this.constructor as typeof Expression;
  }
}

export type ConditionExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class ConditionExpr extends Expression {
  static key = ExpressionKey.CONDITION;

  declare args: ConditionExprArgs;

  constructor (args: ConditionExprArgs = {}) {
    super(args);
  }
}

export type PredicateExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class PredicateExpr extends Expression {
  static key = ExpressionKey.PREDICATE;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: PredicateExprArgs;

  constructor (args: PredicateExprArgs = {}) {
    super(args);
  }
}

export type DerivedTableExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class DerivedTableExpr extends Expression {
  static key = ExpressionKey.DERIVED_TABLE;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: DerivedTableExprArgs;

  constructor (args: DerivedTableExprArgs = {}) {
    super(args);
  }

  /**
   * Gets the select expressions from the derived table.
   * Returns the select expressions if this is a QueryExpr, otherwise returns an empty array.
   *
   * @returns Array of Expression objects representing the SELECT clause expressions
   */
  get selects (): Expression[] {
    return this.args.this instanceof QueryExpr
      ? this.args.this.selects
      : [];
  }

  /**
   * Gets the output names of all select expressions in the derived table.
   * Maps each select expression to its output name (alias or column name).
   *
   * @returns Array of strings representing the names of the selected columns
   */
  get namedSelects (): string[] {
    return this.selects.map((s) => s.outputName);
  }
}

export type QueryExprArgs = Merge<[
  BaseExpressionArgs,
  { with?: Expression },
]>;

export class QueryExpr extends Expression {
  static key = ExpressionKey.QUERY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: QueryExprArgs;

  constructor (args: QueryExprArgs = {}) {
    super(args);
  }

  /**
   * Returns a `Subquery` that wraps around this query.
   *
   * Example:
   *     const subquery = select().select("x").from("tbl").subquery();
   *     select().select("x").from(subquery).sql();
   *     // 'SELECT x FROM (SELECT x FROM tbl)'
   *
   * @param alias - An optional alias for the subquery (string or Expression)
   * @param options - Options object with `copy` property
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns A Subquery expression wrapping this query
   */
  subquery (
    alias?: ExpressionOrString,
    options: {
      copy?: boolean;
      [index: string]: unknown;
    } = {},
  ): SubqueryExpr {
    const { copy = true } = options;
    const instance = maybeCopy(this, copy);
    let aliasExpr: TableAliasExpr | undefined;

    if (!(alias instanceof Expression)) {
      aliasExpr = new TableAliasExpr({
        this: alias
          ? toIdentifier(alias as string)
          : undefined,
      });
    }

    return new SubqueryExpr({
      this: instance,
      alias: aliasExpr,
    });
  }

  /**
   * Adds a LIMIT clause to this query.
   *
   * Example:
   *     select("1").union(select("1")).limit(1).sql();
   *     // 'SELECT 1 UNION SELECT 1 LIMIT 1'
   *
   * @param expression - The SQL code string to parse.
   *                     This can also be an integer.
   *                     If a `Limit` instance is passed, it will be used as-is.
   *                     If another `Expression` instance is passed, it will be wrapped in a
   *                     `Limit`.
   * @param options - Options object
   * @param options.dialect - The dialect used to parse the input expression
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns A limited query expression
   */
  limit (
    expression?: ExpressionOrNumber,
    options: {
      dialect?: DialectType;
      copy?: boolean;
      [index: string]: unknown;
    } = {},
  ): this {
    const {
      copy = true, ...restOptions
    } = options;
    return applyBuilder(expression, {
      instance: this,
      arg: 'limit',
      into: LimitExpr,
      prefix: 'LIMIT',
      intoArg: 'expression',
      ...restOptions,
      copy,
    }) as this;
  }

  /**
   * Set the OFFSET expression.
   *
   * Example:
   *     select().from("tbl").select("x").offset(10).sql();
   *     // 'SELECT x FROM tbl OFFSET 10'
   *
   * @param expression - The SQL code string to parse.
   *                     This can also be an integer.
   *                     If a `Offset` instance is passed, this is used as-is.
   *                     If another `Expression` instance is passed, it will be wrapped in a
   *                     `Offset`.
   * @param options - Options object
   * @param options.dialect - The dialect used to parse the input expression
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns The modified query expression
   */
  offset (
    expression?: ExpressionOrNumber,
    options: {
      dialect?: DialectType;
      copy?: boolean;
      [index: string]: unknown;
    } = {},
  ): this {
    const {
      copy = true, ...restOptions
    } = options;
    return applyBuilder(expression, {
      instance: this,
      arg: 'offset',
      into: OffsetExpr,
      prefix: 'OFFSET',
      intoArg: 'expression',
      ...restOptions,
      copy,
    }) as this;
  }

  /**
   * Set the ORDER BY expression.
   *
   * Example:
   *     select().from("tbl").select("x").orderBy("x DESC").sql();
   *     // 'SELECT x FROM tbl ORDER BY x DESC'
   *
   * @param expressions - The SQL code strings to parse.
   *                      If a `Group` instance is passed, this is used as-is.
   *                      If another `Expression` instance is passed, it will be wrapped in a
   *                      `Order`.
   * @param options - Options object
   * @param options.append - If `true`, add to any existing expressions. Otherwise, this flattens
   * all the `Order` expression into a single expression. Default is `true`.
   * @param options.dialect - The dialect used to parse the input expression
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns The modified query expression
   */
  orderBy (
    expressions?: ExpressionValue | (ExpressionValue | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [index: string]: unknown;
    } = {},
  ): this {
    const {
      append = true, copy = true, ...restOptions
    } = options;
    const expressionList = Array.from(ensureIterable(expressions)) as (string | Expression | undefined)[];
    return applyChildListBuilder(expressionList, {
      instance: this,
      arg: 'order',
      prefix: 'ORDER BY',
      into: OrderExpr,
      ...restOptions,
      append,
      copy,
    }) as this;
  }

  /**
   * Returns a list of all the CTEs attached to this query.
   *
   * @returns Array of CTE expressions
   */
  get ctes (): Expression[] {
    const withExpr = this.args.with;
    return filterInstanceOf(withExpr?.args.expressions ?? [], Expression); // sqlglot uses `Expression.expressions`, but I used $expressions for type safety
  }

  /**
   * Returns the query's projections.
   * Subclasses must implement this property.
   *
   * @returns Array of Expression objects representing the SELECT clause projections
   */
  get selects (): Expression[] {
    throw new Error('Query objects must implement `selects`');
  }

  /**
   * Returns the output names of the query's projections.
   * Subclasses must implement this property.
   *
   * @returns Array of strings representing the names of the projected columns
   */
  get namedSelects (): string[] {
    throw new Error('Query objects must implement `namedSelects`');
  }

  /**
   * Append to or set the SELECT expressions.
   *
   * Example:
   *     select().select(["x", "y"]).sql();
   *     // 'SELECT x, y'
   *
   * @param expressions - The SQL code strings to parse.
   *                      If an `Expression` instance is passed, it will be used as-is.
   * @param options - Options object
   * @param options.append - If `true`, add to any existing expressions. Otherwise, this resets the
   * expressions. Default is `true`.
   * @param options.dialect - The dialect used to parse the input expressions
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns The modified query expression
   */
  select (
    _expressions?: ExpressionValue | (ExpressionValue | undefined)[],
    _options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [index: string]: unknown;
    } = {},
  ): this {
    throw new Error('Query objects must implement `select`');
  }

  /**
   * Append to or set the WHERE expressions.
   *
   * Examples:
   *     select().select(["x"]).from("tbl").where(["x = 'a' OR x < 'b'"]).sql();
   *     // "SELECT x FROM tbl WHERE x = 'a' OR x < 'b'"
   *
   * @param expressions - The SQL code strings to parse.
   *                      If an `Expression` instance is passed, it will be used as-is.
   *                      Multiple expressions are combined with an AND operator.
   * @param options - Options object
   * @param options.append - If `true`, AND the new expressions to any existing expression.
   * Otherwise, this resets the expression. Default is `true`.
   * @param options.dialect - The dialect used to parse the input expressions
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns The modified expression
   */
  where (
    expressions?: ExpressionValue | ExpressionValueList,
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [index: string]: unknown;
    } = {},
  ): this {
    const {
      append = true, copy = true, ...restOptions
    } = options;
    const processedExpressions = Array.from(ensureIterable(expressions))
      .filter((expr): expr is string | Expression => typeof expr === 'string' || expr instanceof Expression)
      .map((expr): string | Expression | undefined =>
        expr instanceof WhereExpr
          ? expr.args.this
          : expr);

    return applyConjunctionBuilder(processedExpressions, {
      instance: this,
      arg: 'where',
      into: WhereExpr,
      ...restOptions,
      append,
      copy,
    }) as this;
  }

  /**
   * Append to or set the common table expressions.
   *
   * Example:
   *     select().with("tbl2", "SELECT * FROM tbl").select(["x"]).from("tbl2").sql();
   *     // 'WITH tbl2 AS (SELECT * FROM tbl) SELECT x FROM tbl2'
   *
   * @param alias - The SQL code string to parse as the table name.
   *                If an `Expression` instance is passed, this is used as-is.
   * @param as - The SQL code string to parse as the table expression.
   *             If an `Expression` instance is passed, it will be used as-is.
   * @param options - Options object
   * @param options.recursive - Set the RECURSIVE part of the expression. Defaults to `false`.
   * @param options.materialized - Set the MATERIALIZED part of the expression
   * @param options.append - If `true`, add to any existing expressions. Otherwise, this resets the
   * expressions. Default is `true`.
   * @param options.dialect - The dialect used to parse the input expression
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @param options.scalar - If `true`, this is a scalar common table expression
   * @returns The modified expression
   */
  with (
    alias: ExpressionOrString<IdentifierExpr | TableAliasExpr>,
    as: ExpressionOrString<QueryExpr>,
    options: {
      recursive?: boolean;
      materialized?: boolean;
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      scalar?: boolean;
      [index: string]: unknown;
    } = {},
  ): this {
    const {
      recursive = false, append = true, copy = true, ...restOptions
    } = options;
    return applyCteBuilder({
      instance: this,
      alias,
      as,
      ...restOptions,
      recursive,
      append,
      copy,
    }) as this;
  }

  /**
   * Builds a UNION expression.
   *
   * Example:
   *     select("1").union([select("1")]).sql();
   *     // 'SELECT 1 UNION SELECT 1'
   *
   * @param expressions - The SQL code strings to parse.
   *                      If an `Expression` instance is passed, it will be used as-is.
   * @param options - Options object
   * @param options.distinct - If `true`, uses UNION DISTINCT. Otherwise uses UNION ALL. Default is
   * `true`.
   * @param options.dialect - The dialect used to parse the input expressions
   * @returns A Union expression
   */
  union (
    expressions?: ExpressionValue | ExpressionValueList,
    options: {
      distinct?: boolean;
      dialect?: DialectType;
      [index: string]: unknown;
    } = {},
  ): UnionExpr | undefined {
    return union([this, ...(expressions !== undefined ? ensureIterable<ExpressionValue>(expressions) : [])], {
      ...options,
      distinct: options.distinct ?? true,
    });
  }

  /**
   * Builds an INTERSECT expression.
   *
   * Example:
   *     select("1").intersect([select("1")]).sql();
   *     // 'SELECT 1 INTERSECT SELECT 1'
   *
   * @param expressions - The SQL code strings to parse.
   *                      If an `Expression` instance is passed, it will be used as-is.
   * @param options - Options object
   * @param options.distinct - If `true`, uses INTERSECT DISTINCT. Otherwise uses INTERSECT ALL.
   * Default is `true`.
   * @param options.dialect - The dialect used to parse the input expressions
   * @returns An Intersect expression
   */
  intersect (
    expressions?: ExpressionOrString | ExpressionOrStringList,
    options: {
      distinct?: boolean;
      dialect?: DialectType;
      [index: string]: unknown;
    } = {},
  ): IntersectExpr | undefined {
    return intersect([this, ...ensureIterable<ExpressionOrString>(expressions)], {
      ...options,
      distinct: options.distinct ?? true,
    });
  }

  /**
   * Builds an EXCEPT expression.
   *
   * Example:
   *     select("1").except([select("2")]).sql();
   *     // 'SELECT 1 EXCEPT SELECT 2'
   *
   * @param expressions - The SQL code strings to parse.
   *                      If an `Expression` instance is passed, it will be used as-is.
   * @param options - Options object
   * @param options.distinct - If `true`, uses EXCEPT DISTINCT. Otherwise uses EXCEPT ALL. Default
   * is `true`.
   * @param options.dialect - The dialect used to parse the input expressions
   * @returns An Except expression
   */
  except (
    expressions?: ExpressionValue | ExpressionValueList,
    options: {
      distinct?: boolean;
      dialect?: DialectType;
      [index: string]: unknown;
    } = {},
  ): ExceptExpr | undefined {
    return except([this, ...(expressions !== undefined ? Array.from(ensureIterable<ExpressionValue>(expressions)) : [])], {
      ...options,
      distinct: options.distinct ?? true,
    });
  }
}

export type UdtfExprArgs = Merge<[
  DerivedTableExprArgs,
  { alias?: Expression },
]>;

export class UdtfExpr extends DerivedTableExpr {
  static key = ExpressionKey.UDTF;

  declare args: UdtfExprArgs;

  constructor (args: UdtfExprArgs = {}) {
    super(args);
  }

  get selects (): Expression[] {
    const alias = this.args.alias;
    return isInstanceOf(alias, TableAliasExpr)
      ? alias.columns
      : [];
  }
}

export type CacheExprArgs = Merge<[
  BaseExpressionArgs,
  {
    lazy?: Expression;
    options?: Expression[];
    this?: Expression;
    expression?: ExpressionValue;
  },
]>;

export class CacheExpr extends Expression {
  static key = ExpressionKey.CACHE;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'lazy',
    'options',
    'expression',
  ]);

  declare args: CacheExprArgs;

  constructor (args: CacheExprArgs = {}) {
    super(args);
  }
}

export type UncacheExprArgs = Merge<[
  BaseExpressionArgs,
  {
    exists?: boolean;
    this?: Expression;
  },
]>;

export class UncacheExpr extends Expression {
  static key = ExpressionKey.UNCACHE;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set(['this', 'exists']);

  declare args: UncacheExprArgs;

  constructor (args: UncacheExprArgs = {}) {
    super(args);
  }
}

/**
 * Enumeration of valid kind values for Refresh expressions.
 * Used to specify the variant or subtype of the expression.
 */
export type RefreshExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: RefreshExprKind;
    this?: Expression;
  },
]>;

export class RefreshExpr extends Expression {
  static key = ExpressionKey.REFRESH;

  static requiredArgs = new Set(['this', 'kind']);

  static availableArgs = new Set(['this', 'kind']);

  declare args: RefreshExprArgs;

  constructor (args: RefreshExprArgs = {}) {
    super(args);
  }
}

export type DdlExprArgs = Merge<[
  BaseExpressionArgs,
  {
    with?: Expression; // NOTE: sqlglot does not have this, but based on usage, I added this
    expression?: Expression; // NOTE: sqlglot does not have this, but based on usage, I added this
  },
]>;

export class DdlExpr extends Expression {
  static key = ExpressionKey.DDL;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: DdlExprArgs;

  constructor (args: DdlExprArgs = {}) {
    super(args);
  }

  /**
   * Returns a list of all the CTEs attached to this statement.
   *
   * @returns Array of CTE expressions
   */
  get ctes (): Expression[] {
    const withExpr = this.args.with;
    return filterInstanceOf(withExpr?.args.expressions ?? [], Expression); // NOTE: The original sqlglot uses `Expression.expressions`
  }

  /**
   * If this statement contains a query (e.g. a CTAS), this returns the query's projections.
   *
   * @returns Array of Expression objects representing the SELECT clause projections
   */
  get selects (): Expression[] {
    const expr = this.args.expression;
    return (expr instanceof QueryExpr)
      ? expr.selects
      : [];
  }

  /**
   * If this statement contains a query (e.g. a CTAS), this returns the output
   * names of the query's projections.
   *
   * @returns Array of strings representing the names of the projected columns
   */
  get namedSelects (): string[] {
    const expr = this.args.expression;
    return (expr instanceof QueryExpr)
      ? expr.namedSelects
      : [];
  }
}

export type LockingStatementExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class LockingStatementExpr extends Expression {
  static key = ExpressionKey.LOCKING_STATEMENT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: LockingStatementExprArgs;

  constructor (args: LockingStatementExprArgs = {}) {
    super(args);
  }
}

export type DmlExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class DmlExpr extends Expression {
  static key = ExpressionKey.DML;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: DmlExprArgs;

  constructor (args: DmlExprArgs = {}) {
    super(args);
  }

  /**
   * Set the RETURNING expression. Not supported by all dialects.
   *
   * Example:
   *     delete("tbl").returning("*", { dialect: "postgres" }).sql();
   *     // 'DELETE FROM tbl RETURNING *'
   *
   * @param expression - The SQL code string to parse.
   *                     If an `Expression` instance is passed, it will be used as-is.
   * @param options - Options object
   * @param options.dialect - The dialect used to parse the input expression
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns The modified expression
   */
  returning (
    expression?: ExpressionValue,
    options: {
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const {
      copy = true, ...restOptions
    } = options;
    return applyBuilder(expression as string | Expression | undefined, {
      instance: this,
      arg: 'returning',
      prefix: 'RETURNING',
      into: ReturningExpr,
      ...restOptions,
      copy,
    }) as this;
  }
}

/**
 * Enumeration of valid kind values for Create expressions.
 * Used to specify the variant or subtype of the expression.
 */
export type CreateExprArgs = Merge<[
  DdlExprArgs,
  {
    with?: Expression;
    kind?: CreateExprKind;
    exists?: boolean;
    properties?: Expression;
    replace?: boolean;
    refresh?: Expression;
    unique?: boolean;
    indexes?: Expression[];
    noSchemaBinding?: Expression;
    begin?: Expression;
    end?: Expression;
    clone?: Expression;
    concurrently?: Expression;
    clustered?: Expression;
    this?: Expression;
    expression?: Expression;
  },
]>;

export class CreateExpr extends DdlExpr {
  static key = ExpressionKey.CREATE;

  static requiredArgs = new Set(['this', 'kind']);

  static availableArgs = new Set([
    'with',
    'this',
    'kind',
    'expression',
    'exists',
    'properties',
    'replace',
    'refresh',
    'unique',
    'indexes',
    'noSchemaBinding',
    'begin',
    'clone',
    'concurrently',
    'clustered',
  ]);

  declare args: CreateExprArgs;

  constructor (args: CreateExprArgs = {}) {
    super(args);
  }

  get kind (): CreateExprKind | undefined {
    return this.args.kind;
  }
}

export type SequencePropertiesExprArgs = Merge<[
  BaseExpressionArgs,
  {
    increment?: Expression;
    minvalue?: string;
    maxvalue?: string;
    cache?: boolean;
    start?: Expression;
    owned?: Expression;
    options?: Expression[];
  },
]>;

export class SequencePropertiesExpr extends Expression {
  static key = ExpressionKey.SEQUENCE_PROPERTIES;

  static availableArgs = new Set([
    'increment',
    'minvalue',
    'maxvalue',
    'cache',
    'start',
    'owned',
    'options',
  ]);

  declare args: SequencePropertiesExprArgs;

  constructor (args: SequencePropertiesExprArgs = {}) {
    super(args);
  }
}

export type TruncateTableExprArgs = Merge<[
  BaseExpressionArgs,
  {
    isDatabase?: string;
    exists?: boolean;
    only?: boolean;
    cluster?: Expression;
    identity?: Expression;
    option?: Expression;
    partition?: Expression;
    expressions?: Expression[];
  },
]>;

export class TruncateTableExpr extends Expression {
  static key = ExpressionKey.TRUNCATE_TABLE;

  static requiredArgs = new Set(['expressions']);
  static availableArgs = new Set([
    'expressions',
    'isDatabase',
    'exists',
    'only',
    'cluster',
    'identity',
    'option',
    'partition',
  ]);

  declare args: TruncateTableExprArgs;

  constructor (args: TruncateTableExprArgs = {}) {
    super(args);
  }
}

export type CloneExprArgs = Merge<[
  BaseExpressionArgs,
  {
    shallow?: Expression;
    copy?: unknown;
    this?: Expression;
  },
]>;

export class CloneExpr extends Expression {
  static key = ExpressionKey.CLONE;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'shallow',
    'copy',
  ]);

  declare args: CloneExprArgs;

  constructor (args: CloneExprArgs = {}) {
    super(args);
  }
}

/**
 * Valid kind values for DESCRIBE statements
 */
export type DescribeExprArgs = Merge<[
  BaseExpressionArgs,
  {
    style?: Expression;
    kind?: DescribeExprKind;
    partition?: Expression;
    format?: string;
    asJson?: Expression;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class DescribeExpr extends Expression {
  static key = ExpressionKey.DESCRIBE;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'style',
    'kind',
    'expressions',
    'partition',
    'format',
    'asJson',
  ]);

  declare args: DescribeExprArgs;

  constructor (args: DescribeExprArgs = {}) {
    super(args);
  }
}

export type AttachExprArgs = Merge<[
  BaseExpressionArgs,
  {
    exists?: boolean;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class AttachExpr extends Expression {
  static key = ExpressionKey.ATTACH;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'exists',
    'expressions',
  ]);

  declare args: AttachExprArgs;

  constructor (args: AttachExprArgs = {}) {
    super(args);
  }
}

export type DetachExprArgs = Merge<[
  BaseExpressionArgs,
  {
    exists?: boolean;
    this?: Expression;
  },
]>;

export class DetachExpr extends Expression {
  static key = ExpressionKey.DETACH;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set(['this', 'exists']);

  declare args: DetachExprArgs;

  constructor (args: DetachExprArgs = {}) {
    super(args);
  }
}

export type InstallExprArgs = Merge<[
  BaseExpressionArgs,
  {
    from?: Expression;
    force?: Expression;
    this?: Expression;
  },
]>;

export class InstallExpr extends Expression {
  static key = ExpressionKey.INSTALL;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'from',
    'force',
  ]);

  declare args: InstallExprArgs;

  constructor (args: InstallExprArgs = {}) {
    super(args);
  }
}

export type SummarizeExprArgs = Merge<[
  BaseExpressionArgs,
  {
    table?: Expression;
    this?: Expression;
  },
]>;

export class SummarizeExpr extends Expression {
  static key = ExpressionKey.SUMMARIZE;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set(['this', 'table']);

  declare args: SummarizeExprArgs;

  constructor (args: SummarizeExprArgs = {}) {
    super(args);
  }
}

/**
 * Valid kind values for KILL statements
 */
export type KillExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: KillExprKind;
    this?: Expression;
  },
]>;

export class KillExpr extends Expression {
  static key = ExpressionKey.KILL;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set(['this', 'kind']);

  declare args: KillExprArgs;

  constructor (args: KillExprArgs = {}) {
    super(args);
  }
}

export type PragmaExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class PragmaExpr extends Expression {
  static key = ExpressionKey.PRAGMA;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: PragmaExprArgs;

  constructor (args: PragmaExprArgs = {}) {
    super(args);
  }
}

export type DeclareExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions?: Expression[] },
]>;

export class DeclareExpr extends Expression {
  static key = ExpressionKey.DECLARE;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: DeclareExprArgs;

  constructor (args: DeclareExprArgs = {}) {
    super(args);
  }
}

/**
 * Valid kind values for DECLARE items
 */
export type DeclareItemExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: DeclareItemExprKind | Expression;
    default?: Expression;
    this?: Expression;
  },
]>;

export class DeclareItemExpr extends Expression {
  static key = ExpressionKey.DECLARE_ITEM;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'kind',
    'default',
  ]);

  declare args: DeclareItemExprArgs;

  constructor (args: DeclareItemExprArgs = {}) {
    super(args);
  }
}

export type SetExprArgs = Merge<[
  BaseExpressionArgs,
  {
    unset?: Expression;
    tag?: Expression;
    expressions?: Expression[];
  },
]>;

export class SetExpr extends Expression {
  static key = ExpressionKey.SET;

  static availableArgs = new Set([
    'expressions',
    'unset',
    'tag',
  ]);

  declare args: SetExprArgs;

  constructor (args: SetExprArgs = {}) {
    super(args);
  }
}

export type HeredocExprArgs = Merge<[
  BaseExpressionArgs,
  {
    tag?: Expression;
    this?: Expression;
  },
]>;

export class HeredocExpr extends Expression {
  static key = ExpressionKey.HEREDOC;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set(['this', 'tag']);

  declare args: HeredocExprArgs;

  constructor (args: HeredocExprArgs = {}) {
    super(args);
  }
}

/**
 * Valid kind values for SET items
 */
export type SetItemExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: SetItemExprKind;
    collate?: string;
    global?: boolean;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class SetItemExpr extends Expression {
  static key = ExpressionKey.SET_ITEM;

  static availableArgs = new Set([
    'this',
    'expressions',
    'kind',
    'collate',
    'global',
  ]);

  declare args: SetItemExprArgs;

  constructor (args: SetItemExprArgs = {}) {
    super(args);
  }
}

export type QueryBandExprArgs = Merge<[
  BaseExpressionArgs,
  {
    scope?: Expression;
    update?: Expression;
    this?: Expression;
  },
]>;

export class QueryBandExpr extends Expression {
  static key = ExpressionKey.QUERY_BAND;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'scope',
    'update',
  ]);

  declare args: QueryBandExprArgs;

  constructor (args: QueryBandExprArgs = {}) {
    super(args);
  }
}

export type ShowExprArgs = Merge<[
  BaseExpressionArgs,
  {
    history?: Expression;
    terse?: Expression;
    target?: Expression;
    offset?: boolean;
    startsWith?: Expression;
    limit?: number | Expression;
    from?: Expression;
    like?: Expression;
    where?: Expression;
    db?: string;
    scope?: Expression;
    scopeKind?: string;
    full?: Expression;
    mutex?: Expression;
    query?: Expression;
    channel?: Expression;
    global?: boolean;
    log?: Expression;
    position?: Expression;
    types?: Expression[];
    privileges?: Expression[];
    forTable?: Expression;
    forGroup?: Expression;
    forUser?: Expression;
    forRole?: Expression;
    intoOutfile?: Expression;
    json?: Expression;
    this?: Expression;
  },
]>;

export class ShowExpr extends Expression {
  static key = ExpressionKey.SHOW;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'history',
    'terse',
    'target',
    'offset',
    'startsWith',
    'limit',
    'from',
    'like',
    'where',
    'db',
    'scope',
    'scopeKind',
    'full',
    'mutex',
    'query',
    'channel',
    'global',
    'log',
    'position',
    'types',
    'privileges',
    'forTable',
    'forGroup',
    'forUser',
    'forRole',
    'intoOutfile',
    'json',
  ]);

  declare args: ShowExprArgs;

  constructor (args: ShowExprArgs = {}) {
    super(args);
  }
}

export type UserDefinedFunctionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    wrapped?: Expression;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class UserDefinedFunctionExpr extends Expression {
  static key = ExpressionKey.USER_DEFINED_FUNCTION;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'expressions',
    'wrapped',
  ]);

  declare args: UserDefinedFunctionExprArgs;

  constructor (args: UserDefinedFunctionExprArgs = {}) {
    super(args);
  }
}

export type CharacterSetExprArgs = Merge<[
  BaseExpressionArgs,
  {
    default?: Expression;
    this?: Expression;
  },
]>;

export class CharacterSetExpr extends Expression {
  static key = ExpressionKey.CHARACTER_SET;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set(['this', 'default']);

  declare args: CharacterSetExprArgs;

  constructor (args: CharacterSetExprArgs = {}) {
    super(args);
  }
}

/**
 * Enumeration of valid kind values for RecursiveWithSearch expressions.
 * Used to specify the variant or subtype of the expression.
 */
export type RecursiveWithSearchExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: RecursiveWithSearchExprKind;
    using?: string;
    this?: Expression;
    expression?: Expression;
  },
]>;

export class RecursiveWithSearchExpr extends Expression {
  static key = ExpressionKey.RECURSIVE_WITH_SEARCH;

  static requiredArgs = new Set([
    'kind',
    'this',
    'expression',
  ]);

  static availableArgs = new Set([
    'kind',
    'this',
    'expression',
    'using',
  ]);

  declare args: RecursiveWithSearchExprArgs;

  constructor (args: RecursiveWithSearchExprArgs = {}) {
    super(args);
  }
}

export type WithExprArgs = Merge<[
  BaseExpressionArgs,
  {
    recursive?: boolean;
    search?: Expression;
    expressions?: Expression[];
  },
]>;

export class WithExpr extends Expression {
  static key = ExpressionKey.WITH;

  static requiredArgs = new Set(['expressions']);
  static availableArgs = new Set([
    'expressions',
    'recursive',
    'search',
  ]);

  declare args: WithExprArgs;

  constructor (args: WithExprArgs = {}) {
    super(args);
  }

  get recursive (): boolean | undefined {
    return this.args.recursive;
  }
}

export type WithinGroupExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: string | Expression;
    expression?: Expression;
  },
]>;
export class WithinGroupExpr extends Expression {
  static key = ExpressionKey.WITHIN_GROUP;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set(['this', 'expression']);

  declare args: WithinGroupExprArgs;

  constructor (args: WithinGroupExprArgs = {}) {
    super(args);
  }
}

export type ProjectionDefExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;
export class ProjectionDefExpr extends Expression {
  static key = ExpressionKey.PROJECTION_DEF;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: ProjectionDefExprArgs;

  constructor (args: ProjectionDefExprArgs = {}) {
    super(args);
  }
}

export type TableAliasExprArgs = Merge<[
  BaseExpressionArgs,
  {
    columns?: ExpressionValue<IdentifierExpr>[];
    this?: ExpressionValue<IdentifierExpr>;
  },
]>;

export class TableAliasExpr extends Expression {
  static key = ExpressionKey.TABLE_ALIAS;

  static availableArgs = new Set(['this', 'columns']);

  declare args: TableAliasExprArgs;

  constructor (args: TableAliasExprArgs = {}) {
    super(args);
  }

  get columns (): Expression[] {
    return (this.args.columns || []) as Expression[];
  }
}

export type ColumnPositionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    position?: Expression;
    this?: Expression;
  },
]>;

export class ColumnPositionExpr extends Expression {
  static key = ExpressionKey.COLUMN_POSITION;

  static requiredArgs = new Set(['position']);
  static availableArgs = new Set(['this', 'position']);

  declare args: ColumnPositionExprArgs;

  constructor (args: ColumnPositionExprArgs = {}) {
    super(args);
  }
}

/**
 * Enumeration of valid kind values for ColumnDef expressions.
 * Used to specify the variant or subtype of the expression.
 */
export type ColumnDefExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: Expression | ColumnDefExprKind;
    constraints?: Expression[];
    exists?: boolean;
    position?: Expression;
    default?: Expression;
    output?: Expression;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class ColumnDefExpr extends Expression {
  static key = ExpressionKey.COLUMN_DEF;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'kind',
    'constraints',
    'exists',
    'position',
    'default',
    'output',
  ]);

  declare args: ColumnDefExprArgs;

  constructor (args: ColumnDefExprArgs = {}) {
    super(args);
  }

  /**
   * Gets the data type of the column definition
   * @returns The DataType expression or undefined
   */
  get kind (): Expression | ColumnDefExprKind | undefined {
    return this.args.kind;
  }

  /**
   * Gets the column constraints
   * @returns Array of ColumnConstraint expressions
   */
  get constraints (): Expression[] {
    return this.args.constraints || [];
  }
}

export type AlterColumnExprArgs = Merge<[
  BaseExpressionArgs,
  {
    dtype?: Expression;
    collate?: string;
    using?: string;
    default?: Expression;
    drop?: Expression;
    comment?: string;
    allowNull?: Expression;
    visible?: Expression;
    renameTo?: string;
    this?: Expression;
  },
]>;

export class AlterColumnExpr extends Expression {
  static key = ExpressionKey.ALTER_COLUMN;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'dtype',
    'collate',
    'using',
    'default',
    'drop',
    'comment',
    'allowNull',
    'visible',
    'renameTo',
  ]);

  declare args: AlterColumnExprArgs;

  constructor (args: AlterColumnExprArgs = {}) {
    super(args);
  }
}

export type AlterIndexExprArgs = Merge<[
  BaseExpressionArgs,
  {
    visible?: Expression;
    this?: Expression;
  },
]>;

export class AlterIndexExpr extends Expression {
  static key = ExpressionKey.ALTER_INDEX;

  static requiredArgs = new Set(['this', 'visible']);

  static availableArgs = new Set(['this', 'visible']);

  declare args: AlterIndexExprArgs;

  constructor (args: AlterIndexExprArgs = {}) {
    super(args);
  }
}

export type AlterDistStyleExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class AlterDistStyleExpr extends Expression {
  static key = ExpressionKey.ALTER_DIST_STYLE;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: AlterDistStyleExprArgs;

  constructor (args: AlterDistStyleExprArgs = {}) {
    super(args);
  }
}

export type AlterSortKeyExprArgs = Merge<[
  BaseExpressionArgs,
  {
    compound?: Expression;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class AlterSortKeyExpr extends Expression {
  static key = ExpressionKey.ALTER_SORT_KEY;

  static availableArgs = new Set([
    'this',
    'expressions',
    'compound',
  ]);

  declare args: AlterSortKeyExprArgs;

  constructor (args: AlterSortKeyExprArgs = {}) {
    super(args);
  }
}

export type AlterSetExprArgs = Merge<[
  BaseExpressionArgs,
  {
    option?: Expression;
    tablespace?: Expression;
    accessMethod?: string;
    fileFormat?: string;
    copyOptions?: Expression[];
    tag?: Expression;
    location?: Expression;
    serde?: Expression;
    expressions?: Expression[];
  },
]>;

export class AlterSetExpr extends Expression {
  static key = ExpressionKey.ALTER_SET;

  static availableArgs = new Set([
    'expressions',
    'option',
    'tablespace',
    'accessMethod',
    'fileFormat',
    'copyOptions',
    'tag',
    'location',
    'serde',
  ]);

  declare args: AlterSetExprArgs;

  constructor (args: AlterSetExprArgs = {}) {
    super(args);
  }
}

export type RenameColumnExprArgs = Merge<[
  BaseExpressionArgs,
  {
    to?: Expression;
    exists?: boolean;
    this?: Expression;
  },
]>;

export class RenameColumnExpr extends Expression {
  static key = ExpressionKey.RENAME_COLUMN;

  static requiredArgs = new Set(['this', 'to']);

  static availableArgs = new Set([
    'this',
    'to',
    'exists',
  ]);

  declare args: RenameColumnExprArgs;

  constructor (args: RenameColumnExprArgs = {}) {
    super(args);
  }
}

export type AlterRenameExprArgs = Merge<[
  BaseExpressionArgs,
  { this?: Expression },
]>;

export class AlterRenameExpr extends Expression {
  static key = ExpressionKey.ALTER_RENAME;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: AlterRenameExprArgs;

  constructor (args: AlterRenameExprArgs = {}) {
    super(args);
  }
}

export type SwapTableExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class SwapTableExpr extends Expression {
  static key = ExpressionKey.SWAP_TABLE;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: SwapTableExprArgs;

  constructor (args: SwapTableExprArgs = {}) {
    super(args);
  }
}

/**
 * Enumeration of valid kind values for Comment expressions.
 * Used to specify the variant or subtype of the expression.
 */
export type CommentExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: CommentExprKind;
    exists?: boolean;
    materialized?: boolean;
    this?: Expression;
    expression?: Expression;
  },
]>;

export class CommentExpr extends Expression {
  static key = ExpressionKey.COMMENT;

  static requiredArgs = new Set([
    'this',
    'kind',
    'expression',
  ]);

  static availableArgs = new Set([
    'this',
    'kind',
    'expression',
    'exists',
    'materialized',
  ]);

  declare args: CommentExprArgs;

  constructor (args: CommentExprArgs = {}) {
    super(args);
  }
}

export type ComprehensionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    position?: Expression;
    iterator?: Expression;
    condition?: Expression;
    this?: Expression;
    expression?: Expression;
  },
]>;

export class ComprehensionExpr extends Expression {
  static key = ExpressionKey.COMPREHENSION;

  static requiredArgs = new Set([
    'this',
    'expression',
    'iterator',
  ]);

  static availableArgs = new Set([
    'this',
    'expression',
    'position',
    'iterator',
    'condition',
  ]);

  declare args: ComprehensionExprArgs;

  constructor (args: ComprehensionExprArgs = {}) {
    super(args);
  }
}

export type MergeTreeTtlActionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    delete?: Expression;
    recompress?: Expression[];
    toDisk?: Expression;
    toVolume?: Expression;
    this?: Expression;
  },
]>;

export class MergeTreeTtlActionExpr extends Expression {
  static key = ExpressionKey.MERGE_TREE_TTL_ACTION;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'delete',
    'recompress',
    'toDisk',
    'toVolume',
  ]);

  declare args: MergeTreeTtlActionExprArgs;

  constructor (args: MergeTreeTtlActionExprArgs = {}) {
    super(args);
  }
}

export type MergeTreeTtlExprArgs = Merge<[
  BaseExpressionArgs,
  {
    where?: Expression;
    group?: Expression;
    aggregates?: Expression[];
    expressions?: Expression[];
  },
]>;

export class MergeTreeTtlExpr extends Expression {
  static key = ExpressionKey.MERGE_TREE_TTL;

  static requiredArgs = new Set(['expressions']);
  static availableArgs = new Set([
    'expressions',
    'where',
    'group',
    'aggregates',
  ]);

  declare args: MergeTreeTtlExprArgs;

  constructor (args: MergeTreeTtlExprArgs = {}) {
    super(args);
  }
}

export type IndexConstraintOptionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    keyBlockSize?: number | Expression;
    using?: string;
    parser?: Expression;
    comment?: ExpressionValue;
    visible?: boolean | Expression;
    engineAttr?: ExpressionValue;
    secondaryEngineAttr?: ExpressionValue;
  },
]>;

export class IndexConstraintOptionExpr extends Expression {
  static key = ExpressionKey.INDEX_CONSTRAINT_OPTION;

  static availableArgs = new Set([
    'keyBlockSize',
    'using',
    'parser',
    'comment',
    'visible',
    'engineAttr',
    'secondaryEngineAttr',
  ]);

  declare args: IndexConstraintOptionExprArgs;

  constructor (args: IndexConstraintOptionExprArgs = {}) {
    super(args);
  }
}

/**
 * Enumeration of valid kind values for ColumnConstraint expressions.
 * Used to specify the variant or subtype of the expression.
 */
export type ColumnConstraintExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: Expression | ColumnConstraintExprKind;
    this?: Expression;
  },
]>;

export class ColumnConstraintExpr extends Expression {
  static key = ExpressionKey.COLUMN_CONSTRAINT;

  static requiredArgs = new Set(['kind']);
  static availableArgs = new Set(['this', 'kind']);

  declare args: ColumnConstraintExprArgs;

  constructor (args: ColumnConstraintExprArgs = {}) {
    super(args);
  }

  /**
   * Gets the kind of column constraint
   * @returns The ColumnConstraintKind expression
   */
  get kind (): Expression | ColumnConstraintExprKind | undefined {
    return this.args.kind;
  }
}

export type ColumnConstraintKindExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class ColumnConstraintKindExpr extends Expression {
  static key = ExpressionKey.COLUMN_CONSTRAINT_KIND;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: ColumnConstraintKindExprArgs;

  constructor (args: ColumnConstraintKindExprArgs = {}) {
    super(args);
  }
}

export type WithOperatorExprArgs = Merge<[
  BaseExpressionArgs,
  {
    op?: Expression;
    this?: Expression;
  },
]>;

export class WithOperatorExpr extends Expression {
  static key = ExpressionKey.WITH_OPERATOR;

  static requiredArgs = new Set(['this', 'op']);

  static availableArgs = new Set(['this', 'op']);

  declare args: WithOperatorExprArgs;

  constructor (args: WithOperatorExprArgs = {}) {
    super(args);
  }
}

export type WatermarkColumnConstraintExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class WatermarkColumnConstraintExpr extends Expression {
  static key = ExpressionKey.WATERMARK_COLUMN_CONSTRAINT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: WatermarkColumnConstraintExprArgs;

  constructor (args: WatermarkColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type ConstraintExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class ConstraintExpr extends Expression {
  static key = ExpressionKey.CONSTRAINT;

  static requiredArgs = new Set(['this', 'expressions']);

  static availableArgs = new Set(['this', 'expressions']);

  declare args: ConstraintExprArgs;

  constructor (args: ConstraintExprArgs = {}) {
    super(args);
  }
}

export type DeleteExprArgs = Merge<[
  DmlExprArgs,
  {
    with?: Expression;
    using?: string;
    where?: Expression;
    returning?: Expression;
    order?: Expression;
    limit?: number | Expression;
    tables?: Expression[];
    cluster?: Expression;
    this?: Expression;
  },
]>;

export class DeleteExpr extends DmlExpr {
  static key = ExpressionKey.DELETE;

  static availableArgs = new Set([
    'with',
    'this',
    'using',
    'where',
    'returning',
    'order',
    'limit',
    'tables',
    'cluster',
  ]);

  declare args: DeleteExprArgs;

  constructor (args: DeleteExprArgs = {}) {
    super(args);
  }

  /**
   * Create a DELETE expression or replace the table on an existing DELETE expression.
   *
   * Example:
   *     delete("tbl").sql();
   *     // 'DELETE FROM tbl'
   *
   * @param table - The table from which to delete
   * @param options - Options object
   * @param options.dialect - The dialect used to parse the input expression
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns The modified expression
   */
  delete (
    table?: ExpressionValue,
    options: {
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const {
      copy = true, ...restOptions
    } = options;
    return applyBuilder(table as string | Expression | undefined, {
      instance: this,
      arg: 'this',
      into: TableExpr,
      ...restOptions,
      copy,
    }) as this;
  }

  /**
   * Append to or set the WHERE expressions.
   *
   * Example:
   *     delete("tbl").where(["x = 'a' OR x < 'b'"]).sql();
   *     // "DELETE FROM tbl WHERE x = 'a' OR x < 'b'"
   *
   * @param expressions - The SQL code strings to parse.
   *                      If an `Expression` instance is passed, it will be used as-is.
   *                      Multiple expressions are combined with an AND operator.
   * @param options - Options object
   * @param options.append - If `true`, AND the new expressions to any existing expression.
   * Otherwise, this resets the expression. Default is `true`.
   * @param options.dialect - The dialect used to parse the input expressions
   * @param options.copy - If `false`, modify this expression instance in-place. Default is `true`.
   * @returns The modified expression
   */
  where (
    expressions?: ExpressionValue | ExpressionValueList,
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [index: string]: unknown;
    } = {},
  ): this {
    const {
      append = true, copy = true, ...restOptions
    } = options;
    return applyConjunctionBuilder(Array.from(ensureIterable(expressions)) as (string | Expression | undefined)[], {
      instance: this,
      arg: 'where',
      into: WhereExpr,
      ...restOptions,
      append,
      copy,
    }) as this;
  }
}

/**
 * Valid kind values for DROP statements
 */
export type DropExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: DropExprKind;
    exists?: boolean;
    temporary?: boolean;
    materialized?: boolean;
    cascade?: Expression;
    constraints?: Expression[];
    purge?: Expression;
    cluster?: Expression;
    concurrently?: Expression;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class DropExpr extends Expression {
  static key = ExpressionKey.DROP;

  static availableArgs = new Set([
    'this',
    'kind',
    'expressions',
    'exists',
    'temporary',
    'materialized',
    'cascade',
    'constraints',
    'purge',
    'cluster',
    'concurrently',
  ]);

  declare args: DropExprArgs;

  constructor (args: DropExprArgs = {}) {
    super(args);
  }

  /**
   * Gets the kind of DROP statement
   * @returns The kind as an uppercase string, or undefined
   */
  get kind (): DropExprKind | undefined {
    return this.args.kind;
  }
}

export type ExportExprArgs = Merge<[
  BaseExpressionArgs,
  {
    connection?: Expression;
    options?: Expression[];
    this?: Expression;
  },
]>;

export class ExportExpr extends Expression {
  static key = ExpressionKey.EXPORT;

  static requiredArgs = new Set(['this', 'options']);

  static availableArgs = new Set([
    'this',
    'connection',
    'options',
  ]);

  declare args: ExportExprArgs;

  constructor (args: ExportExprArgs = {}) {
    super(args);
  }
}

export type FilterExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class FilterExpr extends Expression {
  static key = ExpressionKey.FILTER;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: FilterExprArgs;

  constructor (args: FilterExprArgs = {}) {
    super(args);
  }
}

export type CheckExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class CheckExpr extends Expression {
  static key = ExpressionKey.CHECK;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: CheckExprArgs;

  constructor (args: CheckExprArgs = {}) {
    super(args);
  }
}

export type ChangesExprArgs = Merge<[
  BaseExpressionArgs,
  {
    information?: string;
    atBefore?: Expression;
    end?: Expression;
  },
]>;

export class ChangesExpr extends Expression {
  static key = ExpressionKey.CHANGES;

  static requiredArgs = new Set(['information']);
  static availableArgs = new Set([
    'information',
    'atBefore',
    'end',
  ]);

  declare args: ChangesExprArgs;

  constructor (args: ChangesExprArgs = {}) {
    super(args);
  }
}

export type ConnectExprArgs = Merge<[
  BaseExpressionArgs,
  {
    start?: Expression;
    connect?: Expression;
    nocycle?: Expression;
  },
]>;

export class ConnectExpr extends Expression {
  static key = ExpressionKey.CONNECT;

  static requiredArgs = new Set(['connect']);
  static availableArgs = new Set([
    'start',
    'connect',
    'nocycle',
  ]);

  declare args: ConnectExprArgs;

  constructor (args: ConnectExprArgs = {}) {
    super(args);
  }
}

export type CopyParameterExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression;
    expressions?: Expression[];
  },
]>;

export class CopyParameterExpr extends Expression {
  static key = ExpressionKey.COPY_PARAMETER;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'expression',
    'expressions',
  ]);

  declare args: CopyParameterExprArgs;

  constructor (args: CopyParameterExprArgs = {}) {
    super(args);
  }
}

export type CredentialsExprArgs = Merge<[
  BaseExpressionArgs,
  {
    credentials?: Expression[];
    encryption?: Expression;
    storage?: Expression;
    iamRole?: Expression;
    region?: Expression;
  },
]>;

export class CredentialsExpr extends Expression {
  static key = ExpressionKey.CREDENTIALS;

  static availableArgs = new Set([
    'credentials',
    'encryption',
    'storage',
    'iamRole',
    'region',
  ]);

  declare args: CredentialsExprArgs;

  constructor (args: CredentialsExprArgs = {}) {
    super(args);
  }
}

export type PriorExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class PriorExpr extends Expression {
  static key = ExpressionKey.PRIOR;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: PriorExprArgs;

  constructor (args: PriorExprArgs = {}) {
    super(args);
  }
}

export type DirectoryExprArgs = Merge<[
  BaseExpressionArgs,
  {
    local?: Expression;
    rowFormat?: string;
    this?: Expression;
  },
]>;

export class DirectoryExpr extends Expression {
  static key = ExpressionKey.DIRECTORY;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'local',
    'rowFormat',
  ]);

  declare args: DirectoryExprArgs;

  constructor (args: DirectoryExprArgs = {}) {
    super(args);
  }
}

export type DirectoryStageExprArgs = Merge<[
  BaseExpressionArgs,
  { this?: ExpressionValue },
]>;
export class DirectoryStageExpr extends Expression {
  static key = ExpressionKey.DIRECTORY_STAGE;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: DirectoryStageExprArgs;

  constructor (args: DirectoryStageExprArgs = {}) {
    super(args);
  }
}

export type ForeignKeyExprArgs = Merge<[
  BaseExpressionArgs,
  {
    reference?: Expression;
    delete?: Expression;
    update?: Expression;
    options?: Expression[];
    expressions?: Expression[];
  },
]>;

export class ForeignKeyExpr extends Expression {
  static key = ExpressionKey.FOREIGN_KEY;

  static availableArgs = new Set([
    'expressions',
    'reference',
    'delete',
    'update',
    'options',
  ]);

  declare args: ForeignKeyExprArgs;

  constructor (args: ForeignKeyExprArgs = {}) {
    super(args);
  }
}

export type ColumnPrefixExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class ColumnPrefixExpr extends Expression {
  static key = ExpressionKey.COLUMN_PREFIX;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: ColumnPrefixExprArgs;

  constructor (args: ColumnPrefixExprArgs = {}) {
    super(args);
  }
}

export type PrimaryKeyExprArgs = Merge<[
  BaseExpressionArgs,
  {
    options?: Expression[];
    include?: Expression;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class PrimaryKeyExpr extends Expression {
  static key = ExpressionKey.PRIMARY_KEY;

  static requiredArgs = new Set(['expressions']);
  static availableArgs = new Set([
    'this',
    'expressions',
    'options',
    'include',
  ]);

  declare args: PrimaryKeyExprArgs;

  constructor (args: PrimaryKeyExprArgs = {}) {
    super(args);
  }
}

export type IntoExprArgs = Merge<[
  BaseExpressionArgs,
  {
    temporary?: boolean;
    unlogged?: Expression;
    bulkCollect?: Expression;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class IntoExpr extends Expression {
  static key = ExpressionKey.INTO;

  static availableArgs = new Set([
    'this',
    'temporary',
    'unlogged',
    'bulkCollect',
    'expressions',
  ]);

  declare args: IntoExprArgs;

  constructor (args: IntoExprArgs = {}) {
    super(args);
  }
}

export type FromExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression; // NOTE: sqlglot does not have this, but based on usage, I added it;
  },
]>;

export class FromExpr extends Expression {
  static key = ExpressionKey.FROM;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: FromExprArgs;

  constructor (args: FromExprArgs = {}) {
    super(args);
  }

  /**
   * Gets the name of the FROM expression
   * @returns The name of the expression
   */
  get name (): string {
    return this.args.this?.name || '';
  }

  /**
   * Gets the alias or name of the FROM expression
   * @returns The alias if it exists, otherwise the name
   */
  get aliasOrName (): string {
    return this.args.this?.aliasOrName ?? '';
  }
}

export type HavingExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class HavingExpr extends Expression {
  static key = ExpressionKey.HAVING;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: HavingExprArgs;

  constructor (args: HavingExprArgs = {}) {
    super(args);
  }
}

export type HintExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions?: ExpressionValue[] },
]>;
export class HintExpr extends Expression {
  static key = ExpressionKey.HINT;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: HintExprArgs;

  constructor (args: HintExprArgs = {}) {
    super(args);
  }
}

export type JoinHintExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: string;
    expressions?: Expression[];
  },
]>;

export class JoinHintExpr extends Expression {
  static key = ExpressionKey.JOIN_HINT;

  static requiredArgs = new Set(['this', 'expressions']);

  static availableArgs = new Set(['this', 'expressions']);

  declare args: JoinHintExprArgs;

  constructor (args: JoinHintExprArgs = {}) {
    super(args);
  }
}

export type IdentifierExprArgs = Merge<[
  BaseExpressionArgs,
  {
    quoted?: boolean;
    global?: boolean;
    temporary?: boolean;
    this?: ExpressionValue;
    expressions?: Expression[];
  },
]>;

export class IdentifierExpr extends Expression {
  static key = ExpressionKey.IDENTIFIER;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'quoted',
    'global',
    'temporary',
  ]);

  declare args: IdentifierExprArgs;

  constructor (args: IdentifierExprArgs = {}) {
    super(args);
  }

  get outputName (): string {
    return this.name;
  }

  get quoted (): boolean {
    return Boolean(this.args.quoted);
  }
}

export type OpclassExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class OpclassExpr extends Expression {
  static key = ExpressionKey.OPCLASS;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: OpclassExprArgs;

  constructor (args: OpclassExprArgs = {}) {
    super(args);
  }
}

export type IndexExprArgs = Merge<[
  BaseExpressionArgs,
  {
    table?: Expression;
    unique?: boolean;
    primary?: boolean;
    amp?: Expression;
    params?: Expression[];
    this?: Expression;
  },
]>;

export class IndexExpr extends Expression {
  static key = ExpressionKey.INDEX;

  static availableArgs = new Set([
    'this',
    'table',
    'unique',
    'primary',
    'amp',
    'params',
  ]);

  declare args: IndexExprArgs;

  constructor (args: IndexExprArgs = {}) {
    super(args);
  }
}

export type IndexParametersExprArgs = Merge<[
  BaseExpressionArgs,
  {
    using?: string;
    include?: Expression;
    columns?: Expression[];
    withStorage?: Expression;
    partitionBy?: Expression;
    tablespace?: Expression;
    where?: Expression;
    on?: Expression;
  },
]>;

export class IndexParametersExpr extends Expression {
  static key = ExpressionKey.INDEX_PARAMETERS;

  static availableArgs = new Set([
    'using',
    'include',
    'columns',
    'withStorage',
    'partitionBy',
    'tablespace',
    'where',
    'on',
  ]);

  declare args: IndexParametersExprArgs;

  constructor (args: IndexParametersExprArgs = {}) {
    super(args);
  }
}

export type ConditionalInsertExprArgs = Merge<[
  BaseExpressionArgs,
  {
    else?: Expression;
    this?: Expression;
    expression?: Expression;
  },
]>;

export class ConditionalInsertExpr extends Expression {
  static key = ExpressionKey.CONDITIONAL_INSERT;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'expression',
    'else',
  ]);

  declare args: ConditionalInsertExprArgs;

  constructor (args: ConditionalInsertExprArgs = {}) {
    super(args);
  }
}

/**
 * Enumeration of valid kind values for MultitableInserts expressions.
 * Used to specify the variant or subtype of the expression.
 */
export type MultitableInsertsExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: MultitableInsertsExprKind;
    source?: Expression;
    expressions?: Expression[];
  },
]>;

export class MultitableInsertsExpr extends Expression {
  static key = ExpressionKey.MULTITABLE_INSERTS;

  static requiredArgs = new Set([
    'expressions',
    'kind',
    'source',
  ]);

  static availableArgs = new Set([
    'expressions',
    'kind',
    'source',
  ]);

  declare args: MultitableInsertsExprArgs;

  constructor (args: MultitableInsertsExprArgs = {}) {
    super(args);
  }
}

export type OnConflictExprArgs = Merge<[
  BaseExpressionArgs,
  {
    duplicate?: Expression;
    action?: Expression;
    conflictKeys?: Expression[];
    indexPredicate?: Expression;
    constraint?: Expression;
    where?: Expression;
    expressions?: Expression[];
  },
]>;

export class OnConflictExpr extends Expression {
  static key = ExpressionKey.ON_CONFLICT;

  static availableArgs = new Set([
    'duplicate',
    'expressions',
    'action',
    'conflictKeys',
    'indexPredicate',
    'constraint',
    'where',
  ]);

  declare args: OnConflictExprArgs;

  constructor (args: OnConflictExprArgs = {}) {
    super(args);
  }
}

export type OnConditionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    error?: Expression;
    empty?: Expression;
    null?: Expression;
  },
]>;

export class OnConditionExpr extends Expression {
  static key = ExpressionKey.ON_CONDITION;

  static availableArgs = new Set([
    'error',
    'empty',
    'null',
  ]);

  declare args: OnConditionExprArgs;

  constructor (args: OnConditionExprArgs = {}) {
    super(args);
  }
}

export type ReturningExprArgs = Merge<[
  BaseExpressionArgs,
  {
    into?: Expression;
    expressions?: Expression[];
  },
]>;

export class ReturningExpr extends Expression {
  static key = ExpressionKey.RETURNING;

  static requiredArgs = new Set(['expressions']);
  static availableArgs = new Set(['expressions', 'into']);

  declare args: ReturningExprArgs;

  constructor (args: ReturningExprArgs = {}) {
    super(args);
  }
}

export type IntroducerExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class IntroducerExpr extends Expression {
  static key = ExpressionKey.INTRODUCER;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: IntroducerExprArgs;

  constructor (args: IntroducerExprArgs = {}) {
    super(args);
  }
}

export type NationalExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class NationalExpr extends Expression {
  static key = ExpressionKey.NATIONAL;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: NationalExprArgs;

  constructor (args: NationalExprArgs = {}) {
    super(args);
  }
}

export type LoadDataExprArgs = Merge<[
  BaseExpressionArgs,
  {
    local?: Expression;
    overwrite?: Expression;
    inpath?: Expression;
    partition?: Expression;
    inputFormat?: string;
    serde?: Expression;
    this?: Expression;
  },
]>;

export class LoadDataExpr extends Expression {
  static key = ExpressionKey.LOAD_DATA;

  static requiredArgs = new Set(['this', 'inpath']);

  static availableArgs = new Set([
    'this',
    'local',
    'overwrite',
    'inpath',
    'partition',
    'inputFormat',
    'serde',
  ]);

  declare args: LoadDataExprArgs;

  constructor (args: LoadDataExprArgs = {}) {
    super(args);
  }
}

export type PartitionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    subpartition?: Expression;
    expressions?: Expression[];
  },
]>;

export class PartitionExpr extends Expression {
  static key = ExpressionKey.PARTITION;

  static requiredArgs = new Set(['expressions']);
  static availableArgs = new Set(['expressions', 'subpartition']);

  declare args: PartitionExprArgs;

  constructor (args: PartitionExprArgs = {}) {
    super(args);
  }
}

export type PartitionRangeExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression;
    expressions?: ExpressionValue[] | ExpressionValueList[];
  },
]>;

export class PartitionRangeExpr extends Expression {
  static key = ExpressionKey.PARTITION_RANGE;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set([
    'this',
    'expression',
    'expressions',
  ]);

  declare args: PartitionRangeExprArgs;

  constructor (args: PartitionRangeExprArgs = {}) {
    super(args);
  }
}

export type PartitionIdExprArgs = Merge<[
  BaseExpressionArgs,
  { this?: ExpressionValue },
]>;

export class PartitionIdExpr extends Expression {
  static key = ExpressionKey.PARTITION_ID;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: PartitionIdExprArgs;

  constructor (args: PartitionIdExprArgs = {}) {
    super(args);
  }
}

export type FetchExprArgs = Merge<[
  BaseExpressionArgs,
  {
    direction?: string;
    count?: Expression;
    limitOptions?: Expression[];
  },
]>;

export class FetchExpr extends Expression {
  static key = ExpressionKey.FETCH;

  static availableArgs = new Set([
    'direction',
    'count',
    'limitOptions',
  ]);

  declare args: FetchExprArgs;

  constructor (args: FetchExprArgs = {}) {
    super(args);
  }
}

/**
 * Valid kind values for GRANT statements
 */
export type GrantExprArgs = Merge<[
  BaseExpressionArgs,
  {
    privileges?: Expression[];
    kind?: GrantExprKind;
    securable?: Expression;
    principals?: Expression[];
    grantOption?: Expression;
  },
]>;

export class GrantExpr extends Expression {
  static key = ExpressionKey.GRANT;

  static requiredArgs = new Set([
    'privileges',
    'securable',
    'principals',
  ]);

  static availableArgs = new Set([
    'privileges',
    'kind',
    'securable',
    'principals',
    'grantOption',
  ]);

  declare args: GrantExprArgs;

  constructor (args: GrantExprArgs = {}) {
    super(args);
  }
}

export type RevokeExprArgs = Merge<[
  BaseExpressionArgs,
  {
    cascade?: Expression;
    privileges?: Expression[];
    kind?: string;
    securable?: Expression;
    principals?: Expression[];
    grantOption?: Expression;
  },
]>;

export class RevokeExpr extends Expression {
  static key = ExpressionKey.REVOKE;

  static requiredArgs = new Set([
    'privileges',
    'securable',
    'principals',
  ]);

  static availableArgs = new Set([
    'privileges',
    'kind',
    'securable',
    'principals',
    'grantOption',
    'cascade',
  ]);

  declare args: RevokeExprArgs;

  constructor (args: RevokeExprArgs = {}) {
    super(args);
  }
}

export type GroupExprArgs = Merge<[
  BaseExpressionArgs,
  {
    groupingSets?: Expression[];
    cube?: Expression;
    rollup?: Expression;
    totals?: Expression[];
    all?: boolean;
    expressions?: Expression[];
  },
]>;

export class GroupExpr extends Expression {
  static key = ExpressionKey.GROUP;

  static availableArgs = new Set([
    'expressions',
    'groupingSets',
    'cube',
    'rollup',
    'totals',
    'all',
  ]);

  declare args: GroupExprArgs;

  constructor (args: GroupExprArgs = {}) {
    super(args);
  }
}

export type CubeExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions?: Expression[] },
]>;

export class CubeExpr extends Expression {
  static key = ExpressionKey.CUBE;

  static availableArgs = new Set(['expressions']);

  declare args: CubeExprArgs;

  constructor (args: CubeExprArgs = {}) {
    super(args);
  }
}

export type RollupExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions?: Expression[] },
]>;

export class RollupExpr extends Expression {
  static key = ExpressionKey.ROLLUP;

  static availableArgs = new Set(['expressions']);

  declare args: RollupExprArgs;

  constructor (args: RollupExprArgs = {}) {
    super(args);
  }
}

export type GroupingSetsExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions?: Expression[] },
]>;

export class GroupingSetsExpr extends Expression {
  static key = ExpressionKey.GROUPING_SETS;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: GroupingSetsExprArgs;

  constructor (args: GroupingSetsExprArgs = {}) {
    super(args);
  }
}

export type LambdaExprArgs = Merge<[
  BaseExpressionArgs,
  {
    colon?: Expression;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class LambdaExpr extends Expression {
  static key = ExpressionKey.LAMBDA;

  static requiredArgs = new Set(['this', 'expressions']);

  static availableArgs = new Set([
    'this',
    'expressions',
    'colon',
  ]);

  declare args: LambdaExprArgs;

  constructor (args: LambdaExprArgs = {}) {
    super(args);
  }
}

export type LimitExprArgs = Merge<[
  BaseExpressionArgs,
  {
    offset?: number;
    limitOptions?: Expression[];
    this?: Expression;
    expression?: Expression;
    expressions?: Expression[];
  },
]>;

export class LimitExpr extends Expression {
  static key = ExpressionKey.LIMIT;

  static requiredArgs = new Set(['expression']);
  static availableArgs = new Set([
    'this',
    'expression',
    'offset',
    'limitOptions',
    'expressions',
  ]);

  declare args: LimitExprArgs;

  constructor (args: LimitExprArgs = {}) {
    super(args);
  }
}

export type LimitOptionsExprArgs = Merge<[
  BaseExpressionArgs,
  {
    percent?: Expression;
    rows?: Expression[];
    withTies?: Expression[];
  },
]>;

export class LimitOptionsExpr extends Expression {
  static key = ExpressionKey.LIMIT_OPTIONS;

  static availableArgs = new Set([
    'percent',
    'rows',
    'withTies',
  ]);

  declare args: LimitOptionsExprArgs;

  constructor (args: LimitOptionsExprArgs = {}) {
    super(args);
  }
}

/**
 * Enumeration of valid kind values for JOIN expressions.
 * Used to specify the variant or subtype of the expression.
 */
/**
 * Represents a JOIN clause in SQL.
 *
 * @example
 * // INNER JOIN users ON users.id = orders.user_id
 * const join = new JoinExpr({
 *   this: usersTable,
 *   on: joinCondition,
 *   kind: JoinExprKind.INNER
 * });
 */
export type JoinExprArgs = Merge<[
  BaseExpressionArgs,
  {
    on?: Expression;
    side?: JoinExprKind;
    kind?: JoinExprKind;
    using?: Expression[];
    method?: string;
    global?: boolean;
    hint?: string;
    matchCondition?: Expression;
    directed?: boolean;
    pivots?: Expression[];
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class JoinExpr extends Expression {
  static key = ExpressionKey.JOIN;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'on',
    'side',
    'kind',
    'using',
    'method',
    'global',
    'hint',
    'matchCondition',
    'directed',
    'expressions',
    'pivots',
  ]);

  declare args: JoinExprArgs;

  constructor (args: JoinExprArgs = {}) {
    super(args);
  }

  /**
   * Append to or set the ON expressions.
   *
   * @example
   * sqlglot.parseOne("JOIN x", Join).on("y = 1").sql()
   * // 'JOIN x ON y = 1'
   *
   * @param expressions - the SQL code strings to parse.
   *   If an Expression instance is passed, it will be used as-is.
   *   Multiple expressions are combined with an AND operator.
   * @param options - Configuration options
   * @param options.append - if true, AND the new expressions to any existing expression. Otherwise,
   * this resets the expression.
   * @param options.dialect - the dialect used to parse the input expressions.
   * @param options.copy - if false, modify this expression instance in-place.
   * @returns The modified Join expression.
   */
  on (
    expressions?: ExpressionValue | ExpressionValueList,
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const {
      append, dialect, copy, ...restOptions
    } = options;
    const expressionList = Array.from(ensureIterable(expressions)) as (string | Expression | undefined)[];
    const join = applyConjunctionBuilder(expressionList, {
      instance: this,
      arg: 'on',
      append,
      dialect,
      copy,
      ...restOptions,
    }) as this;

    if (join.args.kind === JoinExprKind.CROSS) {
      join.setArgKey('kind', undefined);
    }

    return join;
  }

  /**
   * Append to or set the USING expressions.
   *
   * @example
   * sqlglot.parseOne("JOIN x", Join).using("foo", "bla").sql()
   * // 'JOIN x USING (foo, bla)'
   *
   * @param expressions - the SQL code strings to parse.
   *   If an Expression instance is passed, it will be used as-is.
   * @param options - Configuration options
   * @param options.append - if true, concatenate the new expressions to the existing "using" list.
   * Otherwise, this resets the expression.
   * @param options.dialect - the dialect used to parse the input expressions.
   * @param options.copy - if false, modify this expression instance in-place.
   * @returns The modified Join expression.
   */
  using (
    expressions?: ExpressionValue | ExpressionValueList,
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const {
      append, dialect, copy, ...restOptions
    } = options;
    const expressionList = Array.from(ensureIterable(expressions)) as (string | Expression | undefined)[];
    const join = applyListBuilder(expressionList, {
      instance: this,
      arg: 'using',
      append,
      dialect,
      copy,
      ...restOptions,
    }) as this;

    if (join.args.kind === JoinExprKind.CROSS) {
      join.setArgKey('kind', undefined);
    }

    return join;
  }

  get method (): string {
    return this.text('method').toUpperCase();
  }

  get kind (): string {
    return this.text('kind').toUpperCase();
  }

  get side (): string {
    return this.text('side').toUpperCase();
  }

  get hint (): string {
    return this.text('hint').toUpperCase();
  }

  get aliasOrName (): string {
    return this.args.this?.aliasOrName ?? '';
  }

  get isSemiOrAntiJoin (): boolean {
    return this.args.kind !== undefined && [JoinExprKind.SEMI, JoinExprKind.ANTI].includes(this.args.kind);
  }
}

export type MatchRecognizeMeasureExprArgs = Merge<[
  BaseExpressionArgs,
  {
    windowFrame?: Expression;
    this?: Expression;
  },
]>;

export class MatchRecognizeMeasureExpr extends Expression {
  static key = ExpressionKey.MATCH_RECOGNIZE_MEASURE;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set(['this', 'windowFrame']);

  declare args: MatchRecognizeMeasureExprArgs;

  constructor (args: MatchRecognizeMeasureExprArgs = {}) {
    super(args);
  }
}

export type MatchRecognizeExprArgs = Merge<[
  BaseExpressionArgs,
  {
    partitionBy?: Expression;
    order?: Expression;
    measures?: Expression[];
    rows?: Expression[];
    after?: Expression;
    pattern?: Expression;
    define?: Expression[];
    alias?: Expression;
  },
]>;

export class MatchRecognizeExpr extends Expression {
  static key = ExpressionKey.MATCH_RECOGNIZE;

  static availableArgs = new Set([
    'partitionBy',
    'order',
    'measures',
    'rows',
    'after',
    'pattern',
    'define',
    'alias',
  ]);

  declare args: MatchRecognizeExprArgs;

  constructor (args: MatchRecognizeExprArgs = {}) {
    super(args);
  }
}

export type FinalExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class FinalExpr extends Expression {
  static key = ExpressionKey.FINAL;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: FinalExprArgs;

  constructor (args: FinalExprArgs = {}) {
    super(args);
  }
}

export type OffsetExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: number | Expression;
    expressions?: Expression[];
  },
]>;

export class OffsetExpr extends Expression {
  static key = ExpressionKey.OFFSET;

  static requiredArgs = new Set(['expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'expressions',
  ]);

  declare args: OffsetExprArgs;

  constructor (args: OffsetExprArgs = {}) {
    super(args);
  }
}

export type OrderExprArgs = Merge<[
  BaseExpressionArgs,
  {
    siblings?: Expression[];
    this?: Expression;
    expressions?: ExpressionValue[];
  },
]>;

export class OrderExpr extends Expression {
  static key = ExpressionKey.ORDER;

  static requiredArgs = new Set(['expressions']);
  static availableArgs = new Set([
    'this',
    'expressions',
    'siblings',
  ]);

  declare args: OrderExprArgs;

  constructor (args: OrderExprArgs = {}) {
    super(args);
  }
}

export type WithFillExprArgs = Merge<[
  BaseExpressionArgs,
  {
    from?: Expression;
    to?: Expression;
    step?: Expression;
    interpolate?: Expression[];
  },
]>;

export class WithFillExpr extends Expression {
  static key = ExpressionKey.WITH_FILL;

  static availableArgs = new Set([
    'from',
    'to',
    'step',
    'interpolate',
  ]);

  declare args: WithFillExprArgs;

  constructor (args: WithFillExprArgs = {}) {
    super(args);
  }
}

export type OrderedExprArgs = Merge<[
  BaseExpressionArgs,
  {
    desc?: boolean | Expression;
    nullsFirst?: boolean | Expression;
    withFill?: Expression;
    this?: Expression;
  },
]>;

export class OrderedExpr extends Expression {
  static key = ExpressionKey.ORDERED;

  static requiredArgs = new Set(['this', 'nullsFirst']);

  static availableArgs = new Set([
    'this',
    'desc',
    'nullsFirst',
    'withFill',
  ]);

  declare args: OrderedExprArgs;

  constructor (args: OrderedExprArgs = {}) {
    super(args);
  }

  get name (): string {
    return this.args.this?.name || '';
  }
}

export type PropertyExprArgs = Merge<[
  BaseExpressionArgs,
  {
    value?: ExpressionValue;
    this?: ExpressionValue; // NOTE: In argTypes, we set this to true
  },
]>;

export class PropertyExpr extends Expression {
  static key = ExpressionKey.PROPERTY;

  static requiredArgs = new Set(['this', 'value']);
  static availableArgs = new Set(['this', 'value']);

  declare args: PropertyExprArgs;

  constructor (args: PropertyExprArgs | BaseExpressionArgs = {}) {
    super(args);
  }
}

export type GrantPrivilegeExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class GrantPrivilegeExpr extends Expression {
  static key = ExpressionKey.GRANT_PRIVILEGE;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this', 'expressions']);

  declare args: GrantPrivilegeExprArgs;

  constructor (args: GrantPrivilegeExprArgs = {}) {
    super(args);
  }
}

/**
 * Valid kind values for GRANT principals
 */
export type GrantPrincipalExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: GrantPrincipalExprKind;
    this?: Expression;
  },
]>;

export class GrantPrincipalExpr extends Expression {
  static key = ExpressionKey.GRANT_PRINCIPAL;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set(['this', 'kind']);

  declare args: GrantPrincipalExprArgs;

  constructor (args: GrantPrincipalExprArgs = {}) {
    super(args);
  }
}

export type AllowedValuesPropertyExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions?: Expression[] },
]>;

export class AllowedValuesPropertyExpr extends Expression {
  static key = ExpressionKey.ALLOWED_VALUES_PROPERTY;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: AllowedValuesPropertyExprArgs;

  constructor (args: AllowedValuesPropertyExprArgs = {}) {
    super(args);
  }
}

export type PartitionByRangePropertyDynamicExprArgs = Merge<[
  BaseExpressionArgs,
  {
    start?: Expression;
    end?: Expression;
    every?: Expression;
    this?: Expression;
  },
]>;

export class PartitionByRangePropertyDynamicExpr extends Expression {
  static key = ExpressionKey.PARTITION_BY_RANGE_PROPERTY_DYNAMIC;

  static requiredArgs = new Set([
    'start',
    'end',
    'every',
  ]);

  static availableArgs = new Set([
    'this',
    'start',
    'end',
    'every',
  ]);

  declare args: PartitionByRangePropertyDynamicExprArgs;

  constructor (args: PartitionByRangePropertyDynamicExprArgs = {}) {
    super(args);
  }
}

export type RollupIndexExprArgs = Merge<[
  BaseExpressionArgs,
  {
    fromIndex?: Expression;
    properties?: Expression;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class RollupIndexExpr extends Expression {
  static key = ExpressionKey.ROLLUP_INDEX;

  static requiredArgs = new Set(['this', 'expressions']);

  static availableArgs = new Set([
    'this',
    'expressions',
    'fromIndex',
    'properties',
  ]);

  declare args: RollupIndexExprArgs;

  constructor (args: RollupIndexExprArgs = {}) {
    super(args);
  }
}

export type PartitionListExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class PartitionListExpr extends Expression {
  static key = ExpressionKey.PARTITION_LIST;

  static requiredArgs = new Set(['this', 'expressions']);

  static availableArgs = new Set(['this', 'expressions']);

  declare args: PartitionListExprArgs;

  constructor (args: PartitionListExprArgs = {}) {
    super(args);
  }
}

export type PartitionBoundSpecExprArgs = Merge<[
  BaseExpressionArgs,
  {
    fromExpressions?: Expression[];
    toExpressions?: Expression[];
    this?: Expression;
    expression?: Expression;
  },
]>;

export class PartitionBoundSpecExpr extends Expression {
  static key = ExpressionKey.PARTITION_BOUND_SPEC;

  static availableArgs = new Set([
    'this',
    'expression',
    'fromExpressions',
    'toExpressions',
  ]);

  declare args: PartitionBoundSpecExprArgs;

  constructor (args: PartitionBoundSpecExprArgs = {}) {
    super(args);
  }
}

export type QueryTransformExprArgs = Merge<[
  BaseExpressionArgs,
  {
    commandScript?: Expression;
    schema?: Expression;
    rowFormatBefore?: string;
    recordWriter?: Expression;
    rowFormatAfter?: string;
    recordReader?: Expression;
    expressions?: Expression[];
  },
]>;

export class QueryTransformExpr extends Expression {
  static key = ExpressionKey.QUERY_TRANSFORM;

  static requiredArgs = new Set(['expressions', 'commandScript']);
  static availableArgs = new Set([
    'expressions',
    'commandScript',
    'schema',
    'rowFormatBefore',
    'recordWriter',
    'rowFormatAfter',
    'recordReader',
  ]);

  declare args: QueryTransformExprArgs;

  constructor (args: QueryTransformExprArgs = {}) {
    super(args);
  }
}

export type SemanticViewExprArgs = Merge<[
  BaseExpressionArgs,
  {
    metrics?: Expression[];
    dimensions?: Expression[];
    facts?: Expression[];
    where?: Expression;
  },
]>;

export class SemanticViewExpr extends Expression {
  static key = ExpressionKey.SEMANTIC_VIEW;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'metrics',
    'dimensions',
    'facts',
    'where',
  ]);

  declare args: SemanticViewExprArgs;

  constructor (args: SemanticViewExprArgs = {}) {
    super(args);
  }
}

export type LocationExprArgs = Merge<[
  BaseExpressionArgs,
]>;
export class LocationExpr extends Expression {
  static key = ExpressionKey.LOCATION;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: LocationExprArgs;

  constructor (args: LocationExprArgs = {}) {
    super(args);
  }
}

export type QualifyExprArgs = Merge<[
  BaseExpressionArgs,
  { this?: Expression },
]>;

export class QualifyExpr extends Expression {
  static key = ExpressionKey.QUALIFY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: QualifyExprArgs;

  constructor (args: QualifyExprArgs = {}) {
    super(args);
  }
}

export type InputOutputFormatExprArgs = Merge<[
  BaseExpressionArgs,
  {
    inputFormat?: string;
    outputFormat?: string;
  },
]>;

export class InputOutputFormatExpr extends Expression {
  static key = ExpressionKey.INPUT_OUTPUT_FORMAT;

  static availableArgs = new Set(['inputFormat', 'outputFormat']);

  declare args: InputOutputFormatExprArgs;

  constructor (args: InputOutputFormatExprArgs = {}) {
    super(args);
  }
}

export type ReturnExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class ReturnExpr extends Expression {
  static key = ExpressionKey.RETURN;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: ReturnExprArgs;

  constructor (args: ReturnExprArgs = {}) {
    super(args);
  }
}

export type ReferenceExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expressions?: Expression[];
    options?: Expression[];
  },
]>;

export class ReferenceExpr extends Expression {
  static key = ExpressionKey.REFERENCE;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set([
    'this',
    'expressions',
    'options',
  ]);

  declare args: ReferenceExprArgs;

  constructor (args: ReferenceExprArgs = {}) {
    super(args);
  }
}

export type TupleExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions?: ExpressionValue[] },
]>;

export class TupleExpr extends Expression {
  static key = ExpressionKey.TUPLE;

  static availableArgs = new Set(['expressions']);

  declare args: TupleExprArgs;

  constructor (args: TupleExprArgs = {}) {
    super(args);
  }

  in (
    expressions?: ExpressionValue[],
    query?: ExpressionValue,
    options: {
      unnest?: ExpressionValue | ExpressionValueList;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): InExpr {
    const {
      copy = true, unnest, ...restOptions
    } = options;
    return new InExpr({
      this: maybeCopy(this, copy),
      expressions: expressions?.map((e) => convert(e, { copy })),
      query: query
        ? maybeParse(query, {
          ...restOptions,
          copy,
        })
        : undefined,
      unnest: unnest
        ? new UnnestExpr({
          expressions: Array.from(ensureIterable(unnest)).map((e) => maybeParse(e as ExpressionValue, {
            ...restOptions,
            copy,
          })),
        })
        : undefined,
    });
  }
}

export const QUERY_MODIFIERS = {
  match: false,
  laterals: false,
  joins: false,
  connect: false,
  pivots: false,
  prewhere: false,
  where: false,
  group: false,
  having: false,
  qualify: false,
  windows: false,
  distribute: false,
  sort: false,
  cluster: false,
  order: false,
  limit: false,
  offset: false,
  locks: false,
  sample: false,
  settings: false,
  format: false,
  options: false,
} as const;

// https://learn.microsoft.com/en-us/sql/t-sql/queries/option-clause-transact-sql?view=sql-server-ver16
// https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query?view=sql-server-ver16
export type QueryOptionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class QueryOptionExpr extends Expression {
  static key = ExpressionKey.QUERY_OPTION;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: QueryOptionExprArgs;

  constructor (args: QueryOptionExprArgs = {}) {
    super(args);
  }
}

// https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table?view=sql-server-ver16
export type WithTableHintExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions?: Expression[] },
]>;

export class WithTableHintExpr extends Expression {
  static key = ExpressionKey.WITH_TABLE_HINT;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: WithTableHintExprArgs;

  constructor (args: WithTableHintExprArgs = {}) {
    super(args);
  }
}

// https://dev.mysql.com/doc/refman/8.0/en/index-hints.html
export type IndexTableHintExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: string;
    expressions?: Expression[];
    target?: string;
  },
]>;

export class IndexTableHintExpr extends Expression {
  static key = ExpressionKey.INDEX_TABLE_HINT;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set([
    'this',
    'expressions',
    'target',
  ]);

  declare args: IndexTableHintExprArgs;

  constructor (args: IndexTableHintExprArgs = {}) {
    super(args);
  }
}

/**
 * Enumeration of valid kind values for HistoricalData expressions.
 * Used to specify the variant or subtype of the expression.
 */
// https://docs.snowflake.com/en/sql-reference/constructs/at-before
export type HistoricalDataExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    kind?: HistoricalDataExprKind;
    expression?: Expression;
  },
]>;

export class HistoricalDataExpr extends Expression {
  static key = ExpressionKey.HISTORICAL_DATA;

  static requiredArgs = new Set([
    'this',
    'kind',
    'expression',
  ]);

  static availableArgs = new Set([
    'this',
    'kind',
    'expression',
  ]);

  declare args: HistoricalDataExprArgs;

  constructor (args: HistoricalDataExprArgs = {}) {
    super(args);
  }
}

// https://docs.snowflake.com/en/sql-reference/sql/put
export type PutExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    target?: Expression;
    properties?: Expression;
  },
]>;

export class PutExpr extends Expression {
  static key = ExpressionKey.PUT;

  static requiredArgs = new Set(['this', 'target']);

  static availableArgs = new Set([
    'this',
    'target',
    'properties',
  ]);

  declare args: PutExprArgs;

  constructor (args: PutExprArgs = {}) {
    super(args);
  }
}

// https://docs.snowflake.com/en/sql-reference/sql/get
export type GetExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    target?: Expression;
    properties?: Expression;
  },
]>;

export class GetExpr extends Expression {
  static key = ExpressionKey.GET;

  static requiredArgs = new Set(['this', 'target']);

  static availableArgs = new Set([
    'this',
    'target',
    'properties',
  ]);

  declare args: GetExprArgs;

  constructor (args: GetExprArgs = {}) {
    super(args);
  }
}

export type TableExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: ExpressionOrString;
    db?: Expression;
    catalog?: Expression;
    alias?: Expression;
    laterals?: Expression[];
    joins?: Expression[];
    pivots?: Expression[];
    hints?: Expression[];
    systemTime?: Expression;
    version?: Expression;
    format?: string;
    pattern?: Expression;
    ordinality?: boolean;
    when?: Expression;
    only?: boolean;
    partition?: Expression;
    changes?: Expression[];
    rowsFrom?: number | Expression;
    sample?: number | Expression;
    indexed?: Expression;
  },
]>;

export class TableExpr extends Expression {
  static key = ExpressionKey.TABLE;

  static availableArgs = new Set([
    'this',
    'alias',
    'db',
    'catalog',
    'laterals',
    'joins',
    'pivots',
    'hints',
    'systemTime',
    'version',
    'format',
    'pattern',
    'ordinality',
    'when',
    'only',
    'partition',
    'changes',
    'rowsFrom',
    'sample',
    'indexed',
  ]);

  declare args: TableExprArgs;

  constructor (args: TableExprArgs = {}) {
    super(args);
  }

  /**
   * Returns the name of the table.
   * If `this` is missing or is a Func, returns empty string.
   */
  get name (): string {
    const thisArg = this.args.this;
    if (!thisArg || thisArg instanceof FuncExpr) {
      return '';
    }
    if (thisArg instanceof Expression) {
      return thisArg.name || '';
    }
    return thisArg.toString();
  }

  /**
   * Returns the database name as a string.
   */
  get db (): string {
    return this.text('db');
  }

  /**
   * Returns the catalog name as a string.
   */
  get catalog (): string {
    return this.text('catalog');
  }

  /**
   * Returns all Select expressions that reference this table.
   */
  get selects (): Expression[] {
    return [];
  }

  /**
   * Returns a list of named selects.
   */
  get namedSelects (): string[] {
    return [];
  }

  /**
   * Returns the parts of a table in order: [catalog, db, this].
   * Flattens Dot expressions into their constituent parts.
   */
  get parts (): Expression[] | [...Expression[], ColumnExpr] {
    const parts: Expression[] = [];

    for (const arg of [
      'catalog',
      'db',
      'this',
    ] as const) {
      const part = this.args[arg];

      if (part instanceof DotExpr) {
        parts.push(...part.flatten());
      } else if (part instanceof IdentifierExpr) {
        parts.push(part);
      }
    }

    return parts as IdentifierExpr[] | [...Expression[], ColumnExpr];
  }

  /**
   * Converts this table to a Column expression.
   */
  toColumn (options: { copy?: boolean } = {}): ColumnExpr | DotExpr | AliasExpr {
    const { copy = true } = options;

    const parts = this.parts;
    const lastPart = parts[parts.length - 1];

    let col: ColumnExpr | DotExpr | AliasExpr;
    if (lastPart instanceof IdentifierExpr) {
      // Build column from parts (reversed for catalog.db.table order)
      const columnParts = parts.slice(0, 4).reverse();
      const fields = parts.slice(4);
      col = column({
        col: columnParts[0] as IdentifierExpr,
        table: columnParts[1] as IdentifierExpr | undefined,
        db: columnParts[2] as IdentifierExpr | undefined,
        catalog: columnParts[3] as IdentifierExpr | undefined,
      }, {
        fields: fields as IdentifierExpr[],
        copy,
      });
    } else {
      // If last part is a function or array wrapped in Table
      col = lastPart as ColumnExpr;
    }

    const aliasArg = this.args.alias;
    if (aliasArg) {
      const aliasThis = aliasArg.args.this;
      const aliasName = typeof aliasThis === 'string' ? aliasThis : isInstanceOf(aliasThis, IdentifierExpr) ? aliasThis : undefined;
      col = alias(col, aliasName, { copy });
    }

    return col;
  }
}

export type VarExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class VarExpr extends Expression {
  static key = ExpressionKey.VAR;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: VarExprArgs;

  constructor (args: VarExprArgs = {}) {
    super(args);
  }
}

/**
 * Time travel expressions for Iceberg, BigQuery, DuckDB, etc.
 * @see {@link https://trino.io/docs/current/connector/iceberg.html | Trino Iceberg}
 * @see {@link https://www.databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html | Delta Time Travel}
 * @see {@link https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#for_system_time_as_of | BigQuery System Time}
 * @see {@link https://learn.microsoft.com/en-us/sql/relational-databases/tables/querying-data-in-a-system-versioned-temporal-table | SQL Server Temporal Tables}
 */
export type VersionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    kind?: string;
    expression?: Expression;
  },
]>;

export class VersionExpr extends Expression {
  static key = ExpressionKey.VERSION;

  static requiredArgs = new Set(['this', 'kind']);

  static availableArgs = new Set([
    'this',
    'kind',
    'expression',
  ]);

  declare args: VersionExprArgs;

  constructor (args: VersionExprArgs = {}) {
    super(args);
  }
}

export type SchemaExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class SchemaExpr extends Expression {
  static key = ExpressionKey.SCHEMA;

  static availableArgs = new Set(['this', 'expressions']);

  declare args: SchemaExprArgs;

  constructor (args: SchemaExprArgs = {}) {
    super(args);
  }
}

/**
 * Lock expressions for SELECT ... FOR UPDATE
 * @see {@link https://dev.mysql.com/doc/refman/8.0/en/select.html | MySQL SELECT}
 * @see {@link https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/SELECT.html | Oracle SELECT}
 */
export type LockExprArgs = Merge<[
  BaseExpressionArgs,
  {
    update?: Expression;
    expressions?: Expression[];
    wait?: Expression;
    key?: Expression;
  },
]>;

export class LockExpr extends Expression {
  static key = ExpressionKey.LOCK;

  static requiredArgs = new Set(['update']);

  static availableArgs = new Set([
    'update',
    'expressions',
    'wait',
    'key',
  ]);

  declare args: LockExprArgs;

  constructor (args: LockExprArgs = {}) {
    super(args);
  }
}

export type TableSampleExprArgs = Merge<[
  BaseExpressionArgs,
  {
    expressions?: Expression[];
    method?: ExpressionOrString;
    bucketNumerator?: Expression;
    bucketDenominator?: Expression;
    bucketField?: Expression;
    percent?: Expression;
    rows?: Expression[];
    size?: number | Expression;
    seed?: Expression;
    this?: ExpressionValue;
  },
]>;

export class TableSampleExpr extends Expression {
  static key = ExpressionKey.TABLE_SAMPLE;

  static availableArgs = new Set([
    'expressions',
    'method',
    'bucketNumerator',
    'bucketDenominator',
    'bucketField',
    'percent',
    'rows',
    'size',
    'seed',
  ]);

  declare args: TableSampleExprArgs;

  constructor (args: TableSampleExprArgs = {}) {
    super(args);
  }
}

export type TagExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    prefix?: string;
    postfix?: string;
  },
]>;

export class TagExpr extends Expression {
  static key = ExpressionKey.TAG;

  static availableArgs = new Set([
    'this',
    'prefix',
    'postfix',
  ]);

  declare args: TagExprArgs;

  constructor (args: TagExprArgs = {}) {
    super(args);
  }

  get this (): Expression | undefined {
    return this.args.this;
  }
}

export type PivotExprArgs = Merge<[
  BaseExpressionArgs,
  {
    fields?: Expression[];
    unpivot?: boolean;
    using?: string;
    group?: Expression;
    columns?: Expression[];
    includeNulls?: Expression[];
    defaultOnNull?: Expression;
    into?: Expression;
    with?: Expression;
    expressions?: Expression[];
  },
]>;

export class PivotExpr extends Expression {
  static key = ExpressionKey.PIVOT;

  static availableArgs = new Set([
    'this',
    'alias',
    'expressions',
    'fields',
    'unpivot',
    'using',
    'group',
    'columns',
    'includeNulls',
    'defaultOnNull',
    'into',
    'with',
  ]);

  declare args: PivotExprArgs;

  constructor (args: PivotExprArgs = {}) {
    super(args);
  }

  /**
   * Returns true if this is an UNPIVOT operation.
   */
  get unpivot (): boolean {
    return !!this.args.unpivot;
  }

  /**
   * Returns the pivot fields.
   */
  get fields (): Expression[] {
    return this.args.fields || [];
  }
}

export type UnpivotColumnsExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class UnpivotColumnsExpr extends Expression {
  static key = ExpressionKey.UNPIVOT_COLUMNS;

  static requiredArgs = new Set(['this', 'expressions']);

  static availableArgs = new Set(['this', 'expressions']);

  declare args: UnpivotColumnsExprArgs;

  constructor (args: UnpivotColumnsExprArgs = {}) {
    super(args);
  }
}

/**
 * Valid kind values for window frame specifications
 */
export type WindowSpecExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: WindowSpecExprKind;
    start?: Expression;
    startSide?: Expression;
    end?: Expression;
    endSide?: Expression;
    exclude?: Expression;
  },
]>;

export class WindowSpecExpr extends Expression {
  static key = ExpressionKey.WINDOW_SPEC;

  static availableArgs = new Set([
    'kind',
    'start',
    'startSide',
    'end',
    'endSide',
    'exclude',
  ]);

  declare args: WindowSpecExprArgs;

  constructor (args: WindowSpecExprArgs = {}) {
    super(args);
  }
}

export type PreWhereExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class PreWhereExpr extends Expression {
  static key = ExpressionKey.PRE_WHERE;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: PreWhereExprArgs;

  constructor (args: PreWhereExprArgs = {}) {
    super(args);
  }
}

export type WhereExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression; // NOTE: sqlglot does not have this, but based on Subquery.where(), I added this;
  },
]>;

export class WhereExpr extends Expression {
  static key = ExpressionKey.WHERE;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: WhereExprArgs;

  constructor (args: WhereExprArgs = {}) {
    super(args);
  }
}

export type StarExprArgs = Merge<[
  BaseExpressionArgs,
  {
    except?: Expression;
    replace?: boolean;
    rename?: string;
  },
]>;

export class StarExpr extends Expression {
  static key = ExpressionKey.STAR;

  static availableArgs = new Set([
    'except',
    'replace',
    'rename',
  ]);

  declare args: StarExprArgs;

  constructor (args: StarExprArgs = {}) {
    super(args);
  }

  /**
   * Returns the name of this star expression.
   */
  get name (): string {
    return '*';
  }

  /**
   * Returns the output name of this star expression.
   */
  get outputName (): string {
    return this.name;
  }
}

export type DataTypeParamExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class DataTypeParamExpr extends Expression {
  static key = ExpressionKey.DATA_TYPE_PARAM;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: DataTypeParamExprArgs;

  constructor (args: DataTypeParamExprArgs = {}) {
    super(args);
  }

  /**
   * Returns the name from the 'this' expression.
   */
  get name (): string {
    return this.args.this?.name ?? '';
  }
}

/**
 * Valid kind values for DataType expressions (SQL data types)
 */
export type DataTypeExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: string | DataTypeExprKind | Expression;
    expressions?: Expression[];
    nested?: boolean;
    values?: Expression[];
    prefix?: boolean | string;
    kind?: DataTypeExprKind | ExpressionOrString;
    nullable?: boolean | Expression;
  },
]>;

export class DataTypeExpr extends Expression {
  static key = ExpressionKey.DATA_TYPE;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'expressions',
    'nested',
    'values',
    'prefix',
    'kind',
    'nullable',
  ]);

  static STRUCT_TYPES = new Set<DataTypeExprKind>([
    DataTypeExprKind.FILE,
    DataTypeExprKind.NESTED,
    DataTypeExprKind.OBJECT,
    DataTypeExprKind.STRUCT,
    DataTypeExprKind.UNION,
  ]);

  static ARRAY_TYPES = new Set<DataTypeExprKind>([DataTypeExprKind.ARRAY, DataTypeExprKind.LIST]);

  static NESTED_TYPES = new Set<DataTypeExprKind>([
    ...DataTypeExpr.STRUCT_TYPES,
    ...DataTypeExpr.ARRAY_TYPES,
    DataTypeExprKind.MAP,
  ]);

  static TEXT_TYPES = new Set<DataTypeExprKind>([
    DataTypeExprKind.CHAR,
    DataTypeExprKind.NCHAR,
    DataTypeExprKind.NVARCHAR,
    DataTypeExprKind.TEXT,
    DataTypeExprKind.VARCHAR,
    DataTypeExprKind.NAME,
  ]);

  static SIGNED_INTEGER_TYPES = new Set<DataTypeExprKind>([
    DataTypeExprKind.BIGINT,
    DataTypeExprKind.INT,
    DataTypeExprKind.INT128,
    DataTypeExprKind.INT256,
    DataTypeExprKind.MEDIUMINT,
    DataTypeExprKind.SMALLINT,
    DataTypeExprKind.TINYINT,
  ]);

  static UNSIGNED_INTEGER_TYPES = new Set<DataTypeExprKind>([
    DataTypeExprKind.UBIGINT,
    DataTypeExprKind.UINT,
    DataTypeExprKind.UINT128,
    DataTypeExprKind.UINT256,
    DataTypeExprKind.UMEDIUMINT,
    DataTypeExprKind.USMALLINT,
    DataTypeExprKind.UTINYINT,
  ]);

  static INTEGER_TYPES = new Set<DataTypeExprKind>([
    ...DataTypeExpr.SIGNED_INTEGER_TYPES,
    ...DataTypeExpr.UNSIGNED_INTEGER_TYPES,
    DataTypeExprKind.BIT,
  ]);

  static FLOAT_TYPES = new Set<DataTypeExprKind>([DataTypeExprKind.DOUBLE, DataTypeExprKind.FLOAT]);

  static REAL_TYPES = new Set<DataTypeExprKind>([
    ...DataTypeExpr.FLOAT_TYPES,
    DataTypeExprKind.BIGDECIMAL,
    DataTypeExprKind.DECIMAL,
    DataTypeExprKind.DECIMAL32,
    DataTypeExprKind.DECIMAL64,
    DataTypeExprKind.DECIMAL128,
    DataTypeExprKind.DECIMAL256,
    DataTypeExprKind.DECFLOAT,
    DataTypeExprKind.MONEY,
    DataTypeExprKind.SMALLMONEY,
    DataTypeExprKind.UDECIMAL,
    DataTypeExprKind.UDOUBLE,
  ]);

  static NUMERIC_TYPES = new Set<string | DataTypeExprKind>([...DataTypeExpr.INTEGER_TYPES, ...DataTypeExpr.REAL_TYPES]);

  static TEMPORAL_TYPES = new Set<string | DataTypeExprKind>([
    DataTypeExprKind.DATE,
    DataTypeExprKind.DATE32,
    DataTypeExprKind.DATETIME,
    DataTypeExprKind.DATETIME2,
    DataTypeExprKind.DATETIME64,
    DataTypeExprKind.SMALLDATETIME,
    DataTypeExprKind.TIME,
    DataTypeExprKind.TIMESTAMP,
    DataTypeExprKind.TIMESTAMPNTZ,
    DataTypeExprKind.TIMESTAMPLTZ,
    DataTypeExprKind.TIMESTAMPTZ,
    DataTypeExprKind.TIMESTAMP_MS,
    DataTypeExprKind.TIMESTAMP_NS,
    DataTypeExprKind.TIMESTAMP_S,
    DataTypeExprKind.TIMETZ,
  ]);

  declare args: DataTypeExprArgs;

  constructor (args: DataTypeExprArgs = {}) {
    super(args);
  }

  /**
   * Constructs a DataTypeExpr object.
   */
  static build (
    dtype?: DataTypeExprKind | ExpressionOrString,
    options: {
      dialect?: DialectType;
      udt?: boolean;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): DataTypeExpr | undefined {
    const {
      udt = false, copy = true, dialect, ...kwargs
    } = options;

    let dataTypeExp;

    if (typeof dtype === 'string') {
      if (dtype === DataTypeExprKind.UNKNOWN) {
        return new DataTypeExpr({
          ...kwargs,
          this: DataTypeExprKind.UNKNOWN,
        });
      }

      try {
        dataTypeExp = parseOne(dtype, {
          read: dialect,
          into: DataTypeExpr,
          errorLevel: ErrorLevel.IGNORE,
        });
      } catch (e) {
        if (!(e instanceof ParseError)) {
          throw e;
        }
        if (udt) {
          return new DataTypeExpr({
            ...options,
            this: DataTypeExprKind.USERDEFINED,
            kind: dtype,
          });
        }
      }
    } else if ((dtype instanceof IdentifierExpr || dtype instanceof DotExpr) && udt) {
      return new DataTypeExpr({
        ...kwargs,
        this: DataTypeExprKind.USERDEFINED,
        kind: dtype,
      });
    } else if (typeof dtype === 'string' && Object.values(DataTypeExprKind).includes(dtype as DataTypeExprKind)) {
      dataTypeExp = new DataTypeExpr({
        ...kwargs,
        this: dtype,
      });
    } else if (dtype instanceof DataTypeExpr) {
      return maybeCopy(dtype, copy);
    } else {
      throw new Error(`Invalid data type: ${typeof dtype}. Expected string, DataTypeExprKind, or DataTypeExpr`);
    }

    for (const [k, v] of Object.entries(kwargs)) {
      dataTypeExp?.setArgKey(k, v as ExpressionValue);
    }
    return dataTypeExp;
  }

  /**
   * Checks whether this DataType matches one of the provided data types. Nested types or precision
   * will be compared using "structural equivalence" semantics, so e.g. array<int> != array<float>.
   *
   * @param dtypes - The data types to compare this DataType to.
   * @param options - Options for the comparison.
   * @param options.checkNullable - Whether to take the NULLABLE type constructor into account for the comparison.
   *                                 If false, it means that NULLABLE<INT> is equivalent to INT.
   * @returns True, if and only if there is a type in dtypes which is equal to this DataType.
   */
  isType (
    dtypes: DataTypeExprKind | ExpressionOrString<DataTypeExpr | IdentifierExpr | DotExpr> | Iterable<DataTypeExprKind | ExpressionOrString<DataTypeExpr | IdentifierExpr | DotExpr>>,
    options?: { checkNullable?: boolean },
  ): boolean {
    const checkNullable = options?.checkNullable ?? false;
    const selfIsNullable = this.args.nullable;

    for (const dtype of ensureIterable(dtypes)) {
      const otherType = DataTypeExpr.build(dtype, {
        copy: false,
        udt: true,
      });
      const otherIsNullable = otherType?.args.nullable;

      let matches: boolean;

      if (
        otherType?.args.expressions
        || (checkNullable && (selfIsNullable || otherIsNullable))
        || this.args.this === DataTypeExprKind.USERDEFINED
        || otherType?.args.this === DataTypeExprKind.USERDEFINED
      ) {
        matches = this.equals(otherType);
      } else {
        matches = this.args.this === otherType?.args.this;
      }

      if (matches) {
        return true;
      }
    }

    return false;
  }
}

export type TypeExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class TypeExpr extends Expression {
  static key = ExpressionKey.TYPE;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: TypeExprArgs;

  constructor (args: TypeExprArgs = {}) {
    super(args);
  }
}

export type CommandExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: string;
    expression?: string;
  },
]>;

export class CommandExpr extends Expression {
  static key = ExpressionKey.COMMAND;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: CommandExprArgs;

  constructor (args: CommandExprArgs = {}) {
    super(args);
  }
}

export type TransactionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    modes?: Expression[];
    mark?: Expression;
  },
]>;

export class TransactionExpr extends Expression {
  static key = ExpressionKey.TRANSACTION;

  static availableArgs = new Set([
    'this',
    'modes',
    'mark',
  ]);

  declare args: TransactionExprArgs;

  constructor (args: TransactionExprArgs = {}) {
    super(args);
  }
}

export type CommitExprArgs = Merge<[
  BaseExpressionArgs,
  {
    chain?: Expression;
    this?: Expression;
    durability?: Expression;
  },
]>;

export class CommitExpr extends Expression {
  static key = ExpressionKey.COMMIT;

  static availableArgs = new Set([
    'chain',
    'this',
    'durability',
  ]);

  declare args: CommitExprArgs;

  constructor (args: CommitExprArgs = {}) {
    super(args);
  }
}

export type RollbackExprArgs = Merge<[
  BaseExpressionArgs,
  {
    savepoint?: Expression;
    this?: Expression;
  },
]>;

export class RollbackExpr extends Expression {
  static key = ExpressionKey.ROLLBACK;

  static availableArgs = new Set(['savepoint', 'this']);

  declare args: RollbackExprArgs;

  constructor (args: RollbackExprArgs = {}) {
    super(args);
  }
}

/**
 * Enumeration of valid kind values for Alter expressions.
 * Used to specify the variant or subtype of the expression.
 */
export type AlterExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    kind?: AlterExprKind;
    actions?: Expression[];
    exists?: boolean;
    only?: boolean;
    options?: Expression[];
    cluster?: Expression;
    notValid?: Expression;
    check?: Expression;
    cascade?: Expression;
  },
]>;

export class AlterExpr extends Expression {
  static key = ExpressionKey.ALTER;

  static requiredArgs = new Set(['kind', 'actions']);

  static availableArgs = new Set([
    'this',
    'kind',
    'actions',
    'exists',
    'only',
    'options',
    'cluster',
    'notValid',
    'check',
    'cascade',
  ]);

  declare args: AlterExprArgs;

  constructor (args: AlterExprArgs = {}) {
    super(args);
  }

  /**
   * Returns the kind in uppercase.
   */
  get kind (): string | undefined {
    const kind = this.args.kind;
    return kind ? kind.toUpperCase() : undefined;
  }

  /**
   * Returns the actions array.
   */
  get actions (): Expression[] {
    return this.args.actions || [];
  }
}

export type AlterSessionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    expressions?: Expression[];
    unset?: Expression;
  },
]>;

export class AlterSessionExpr extends Expression {
  static key = ExpressionKey.ALTER_SESSION;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions', 'unset']);

  declare args: AlterSessionExprArgs;

  constructor (args: AlterSessionExprArgs = {}) {
    super(args);
  }
}

/**
 * Valid kind values for ANALYZE statements
 */
export type AnalyzeExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: AnalyzeExprKind;
    this?: Expression;
    options?: Expression[];
    mode?: Expression;
    partition?: Expression;
    expression?: Expression;
    properties?: Expression[];
  },
]>;

export class AnalyzeExpr extends Expression {
  static key = ExpressionKey.ANALYZE;

  static availableArgs = new Set([
    'kind',
    'this',
    'options',
    'mode',
    'partition',
    'expression',
    'properties',
  ]);

  declare args: AnalyzeExprArgs;

  constructor (args: AnalyzeExprArgs = {}) {
    super(args);
  }
}

/**
 * Enumeration of valid kind values for AnalyzeStatistics expressions.
 * Used to specify the variant or subtype of the expression.
 */
export type AnalyzeStatisticsExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: AnalyzeStatisticsExprKind;
    option?: Expression;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class AnalyzeStatisticsExpr extends Expression {
  static key = ExpressionKey.ANALYZE_STATISTICS;

  static requiredArgs = new Set(['kind']);
  static availableArgs = new Set([
    'kind',
    'option',
    'this',
    'expressions',
  ]);

  declare args: AnalyzeStatisticsExprArgs;

  constructor (args: AnalyzeStatisticsExprArgs = {}) {
    super(args);
  }
}

export type AnalyzeHistogramExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expressions?: Expression[];
    expression?: Expression;
    updateOptions?: Expression[];
  },
]>;

export class AnalyzeHistogramExpr extends Expression {
  static key = ExpressionKey.ANALYZE_HISTOGRAM;

  static requiredArgs = new Set(['this', 'expressions']);

  static availableArgs = new Set([
    'this',
    'expressions',
    'expression',
    'updateOptions',
  ]);

  declare args: AnalyzeHistogramExprArgs;

  constructor (args: AnalyzeHistogramExprArgs = {}) {
    super(args);
  }
}

/**
 * Enumeration of valid kind values for AnalyzeSample expressions.
 * Used to specify the variant or subtype of the expression.
 */
export type AnalyzeSampleExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: AnalyzeSampleExprKind;
    sample?: number | Expression;
  },
]>;

export class AnalyzeSampleExpr extends Expression {
  static key = ExpressionKey.ANALYZE_SAMPLE;

  static requiredArgs = new Set(['kind', 'sample']);

  static availableArgs = new Set(['kind', 'sample']);

  declare args: AnalyzeSampleExprArgs;

  constructor (args: AnalyzeSampleExprArgs = {}) {
    super(args);
  }
}

export type AnalyzeListChainedRowsExprArgs = Merge<[
  BaseExpressionArgs,
  { expression?: Expression },
]>;

export class AnalyzeListChainedRowsExpr extends Expression {
  static key = ExpressionKey.ANALYZE_LIST_CHAINED_ROWS;

  static availableArgs = new Set(['expression']);

  declare args: AnalyzeListChainedRowsExprArgs;

  constructor (args: AnalyzeListChainedRowsExprArgs = {}) {
    super(args);
  }
}

/**
 * Valid kind values for ANALYZE DELETE statements
 */
export type AnalyzeDeleteExprArgs = Merge<[
  BaseExpressionArgs,
  { kind?: AnalyzeDeleteExprKind },
]>;

export class AnalyzeDeleteExpr extends Expression {
  static key = ExpressionKey.ANALYZE_DELETE;

  static availableArgs = new Set(['kind']);

  declare args: AnalyzeDeleteExprArgs;

  constructor (args: AnalyzeDeleteExprArgs = {}) {
    super(args);
  }
}

export type AnalyzeWithExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions?: Expression[] },
]>;

export class AnalyzeWithExpr extends Expression {
  static key = ExpressionKey.ANALYZE_WITH;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: AnalyzeWithExprArgs;

  constructor (args: AnalyzeWithExprArgs = {}) {
    super(args);
  }
}

/**
 * Enumeration of valid kind values for AnalyzeValidate expressions.
 * Used to specify the variant or subtype of the expression.
 */
export type AnalyzeValidateExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: AnalyzeValidateExprKind;
    this?: Expression;
    expression?: Expression;
  },
]>;

export class AnalyzeValidateExpr extends Expression {
  static key = ExpressionKey.ANALYZE_VALIDATE;

  static requiredArgs = new Set(['kind']);

  static availableArgs = new Set([
    'kind',
    'this',
    'expression',
  ]);

  declare args: AnalyzeValidateExprArgs;

  constructor (args: AnalyzeValidateExprArgs = {}) {
    super(args);
  }
}

export type AnalyzeColumnsExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class AnalyzeColumnsExpr extends Expression {
  static key = ExpressionKey.ANALYZE_COLUMNS;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: AnalyzeColumnsExprArgs;

  constructor (args: AnalyzeColumnsExprArgs = {}) {
    super(args);
  }
}

export type UsingDataExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class UsingDataExpr extends Expression {
  static key = ExpressionKey.USING_DATA;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: UsingDataExprArgs;

  constructor (args: UsingDataExprArgs = {}) {
    super(args);
  }
}

export type AddConstraintExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions?: Expression[] },
]>;

export class AddConstraintExpr extends Expression {
  static key = ExpressionKey.ADD_CONSTRAINT;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: AddConstraintExprArgs;

  constructor (args: AddConstraintExprArgs = {}) {
    super(args);
  }
}

export type AddPartitionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    exists?: boolean;
    location?: Expression;
  },
]>;

export class AddPartitionExpr extends Expression {
  static key = ExpressionKey.ADD_PARTITION;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set([
    'this',
    'exists',
    'location',
  ]);

  declare args: AddPartitionExprArgs;

  constructor (args: AddPartitionExprArgs = {}) {
    super(args);
  }
}

export type AttachOptionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class AttachOptionExpr extends Expression {
  static key = ExpressionKey.ATTACH_OPTION;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: AttachOptionExprArgs;

  constructor (args: AttachOptionExprArgs = {}) {
    super(args);
  }
}

export type DropPartitionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    expressions?: Expression[];
    exists?: boolean;
  },
]>;

export class DropPartitionExpr extends Expression {
  static key = ExpressionKey.DROP_PARTITION;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions', 'exists']);

  declare args: DropPartitionExprArgs;

  constructor (args: DropPartitionExprArgs = {}) {
    super(args);
  }
}

export type ReplacePartitionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    expression?: Expression;
    source?: Expression;
  },
]>;

export class ReplacePartitionExpr extends Expression {
  static key = ExpressionKey.REPLACE_PARTITION;

  static requiredArgs = new Set(['expression', 'source']);

  static availableArgs = new Set(['expression', 'source']);

  declare args: ReplacePartitionExprArgs;

  constructor (args: ReplacePartitionExprArgs = {}) {
    super(args);
  }
}

export type AliasExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    alias?: ExpressionOrString;
  },
]>;

export class AliasExpr extends Expression {
  static key = ExpressionKey.ALIAS;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this', 'alias']);

  declare args: AliasExprArgs;

  constructor (args: AliasExprArgs = {}) {
    super(args);
  }

  get outputName (): string {
    if (typeof this.args.alias === 'string') {
      return this.args.alias;
    }
    return this.args.alias?.name ?? '';
  }
}

export type PivotAnyExprArgs = Merge<[
  BaseExpressionArgs,
  { this?: Expression },
]>;

/**
 * Represents Snowflake's ANY [ ORDER BY ... ] syntax
 * https://docs.snowflake.com/en/sql-reference/constructs/pivot
 */
export class PivotAnyExpr extends Expression {
  static key = ExpressionKey.PIVOT_ANY;

  static availableArgs = new Set(['this']);

  declare args: PivotAnyExprArgs;

  constructor (args: PivotAnyExprArgs = {}) {
    super(args);
  }
}

export type AliasesExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class AliasesExpr extends Expression {
  static key = ExpressionKey.ALIASES;

  static requiredArgs = new Set(['this', 'expressions']);

  static availableArgs = new Set(['this', 'expressions']);

  declare args: AliasesExprArgs;

  constructor (args: AliasesExprArgs = {}) {
    super(args);
  }

  get aliases (): Expression[] {
    return this.args.expressions ?? [];
  }
}

export type AtIndexExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

/**
 * https://docs.aws.amazon.com/redshift/latest/dg/query-super.html
 */
export class AtIndexExpr extends Expression {
  static key = ExpressionKey.AT_INDEX;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: AtIndexExprArgs;

  constructor (args: AtIndexExprArgs = {}) {
    super(args);
  }
}

export type AtTimeZoneExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: ExpressionValue;
    zone?: ExpressionValue;
  },
]>;

export class AtTimeZoneExpr extends Expression {
  static key = ExpressionKey.AT_TIME_ZONE;

  static requiredArgs = new Set(['this', 'zone']);

  static availableArgs = new Set(['this', 'zone']);

  declare args: AtTimeZoneExprArgs;

  constructor (args: AtTimeZoneExprArgs = {}) {
    super(args);
  }
}

export type FromTimeZoneExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    zone?: Expression;
  },
]>;

export class FromTimeZoneExpr extends Expression {
  static key = ExpressionKey.FROM_TIME_ZONE;

  static requiredArgs = new Set(['this', 'zone']);

  static availableArgs = new Set(['this', 'zone']);

  declare args: FromTimeZoneExprArgs;

  constructor (args: FromTimeZoneExprArgs = {}) {
    super(args);
  }
}

export type FormatPhraseExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    format?: Expression;
  },
]>;

/**
 * Format override for a column in Teradata.
 * Can be expanded to additional dialects as needed.
 *
 * https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Types-and-Literals/Data-Type-Formats-and-Format-Phrases/FORMAT
 */
export class FormatPhraseExpr extends Expression {
  static key = ExpressionKey.FORMAT_PHRASE;

  static requiredArgs = new Set(['this', 'format']);

  static availableArgs = new Set(['this', 'format']);

  declare args: FormatPhraseExprArgs;

  constructor (args: FormatPhraseExprArgs = {}) {
    super(args);
  }
}

export type DistinctExprArgs = Merge<[
  BaseExpressionArgs,
  {
    expressions?: Expression[];
    on?: Expression;
  },
]>;

export class DistinctExpr extends Expression {
  static key = ExpressionKey.DISTINCT;

  static availableArgs = new Set(['expressions', 'on']);

  declare args: DistinctExprArgs;

  constructor (args: DistinctExprArgs = {}) {
    super(args);
  }
}

export type ForInExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

/**
 * https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language#for-in
 */
export class ForInExpr extends Expression {
  static key = ExpressionKey.FOR_IN;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: ForInExprArgs;

  constructor (args: ForInExprArgs = {}) {
    super(args);
  }
}

export type TimeUnitExprArgs = Merge<[
  BaseExpressionArgs,
  {
    unit?: Expression;
    expression?: Expression;
    expressions?: ExpressionValue[];
    this?: ExpressionValue;
  },
]>;

/**
 * Automatically converts unit arg into a var.
 */
export class TimeUnitExpr extends Expression {
  static key = ExpressionKey.TIME_UNIT;

  static availableArgs = new Set(['unit']);

  static UNABBREVIATED_UNIT_NAME: Record<string, string> = {
    D: 'DAY',
    H: 'HOUR',
    M: 'MINUTE',
    MS: 'MILLISECOND',
    NS: 'NANOSECOND',
    Q: 'QUARTER',
    S: 'SECOND',
    US: 'MICROSECOND',
    W: 'WEEK',
    Y: 'YEAR',
  };

  static isVarLike (expr: unknown): expr is VarExpr | ColumnExpr | LiteralExpr {
    return expr instanceof VarExpr || expr instanceof ColumnExpr || expr instanceof LiteralExpr;
  }

  declare args: TimeUnitExprArgs;

  constructor (args: TimeUnitExprArgs = {}) {
    const unit = args.unit;

    if (
      unit
      && TimeUnitExpr.isVarLike(unit)
      && !(unit instanceof ColumnExpr && unit.parts.length !== 1)
    ) {
      args.unit = new VarExpr({
        this: (TimeUnitExpr.UNABBREVIATED_UNIT_NAME[unit.name] || unit.name).toUpperCase(),
      });
    } else if (unit instanceof WeekExpr) {
      const thisArg = unit.args.this;
      if (thisArg) {
        unit.setArgKey('this', new VarExpr({ this: thisArg.name.toUpperCase() }));
      }
    }

    super(args);
  }

  get unit (): Expression | undefined {
    return this.args.unit;
  }
}

export type IgnoreNullsExprArgs = Merge<[
  BaseExpressionArgs,
  { this?: Expression },
]>;

export class IgnoreNullsExpr extends Expression {
  static key = ExpressionKey.IGNORE_NULLS;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: IgnoreNullsExprArgs;

  constructor (args: IgnoreNullsExprArgs = {}) {
    super(args);
  }
}

export type RespectNullsExprArgs = Merge<[
  BaseExpressionArgs,
  { this?: Expression },
]>;

export class RespectNullsExpr extends Expression {
  static key = ExpressionKey.RESPECT_NULLS;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: RespectNullsExprArgs;

  constructor (args: RespectNullsExprArgs = {}) {
    super(args);
  }
}

/**
 * https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate-function-calls#max_min_clause
 */
export type HavingMaxExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression;
    max?: Expression;
  },
]>;

export class HavingMaxExpr extends Expression {
  static key = ExpressionKey.HAVING_MAX;

  static requiredArgs = new Set([
    'this',
    'expression',
    'max',
  ]);

  static availableArgs = new Set([
    'this',
    'expression',
    'max',
  ]);

  declare args: HavingMaxExprArgs;

  constructor (args: HavingMaxExprArgs = {}) {
    super(args);
  }
}

export type TranslateCharactersExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression;
    withError?: Expression;
  },
]>;

export class TranslateCharactersExpr extends Expression {
  static key = ExpressionKey.TRANSLATE_CHARACTERS;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'withError',
  ]);

  declare args: TranslateCharactersExprArgs;

  constructor (args: TranslateCharactersExprArgs = {}) {
    super(args);
  }
}

export type PositionalColumnExprArgs = Merge<[
  BaseExpressionArgs,
]>;
export class PositionalColumnExpr extends Expression {
  static key = ExpressionKey.POSITIONAL_COLUMN;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: PositionalColumnExprArgs;

  constructor (args: PositionalColumnExprArgs = {}) {
    super(args);
  }
}

export type OverflowTruncateBehaviorExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    withCount?: Expression;
  },
]>;

export class OverflowTruncateBehaviorExpr extends Expression {
  static key = ExpressionKey.OVERFLOW_TRUNCATE_BEHAVIOR;

  static requiredArgs = new Set(['withCount']);

  static availableArgs = new Set(['this', 'withCount']);

  declare args: OverflowTruncateBehaviorExprArgs;

  constructor (args: OverflowTruncateBehaviorExprArgs = {}) {
    super(args);
  }
}

export type JsonExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    with?: Expression;
    unique?: boolean;
  },
]>;

export class JsonExpr extends Expression {
  static key = ExpressionKey.JSON;

  static availableArgs = new Set([
    'this',
    'with',
    'unique',
  ]);

  declare args: JsonExprArgs;

  constructor (args: JsonExprArgs = {}) {
    super(args);
  }
}

export type JsonPathExprArgs = Merge<[
  BaseExpressionArgs,
  {
    expressions?: Expression[];
    escape?: Expression;
  },
]>;

export class JsonPathExpr extends Expression {
  static key = ExpressionKey.JSON_PATH;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions', 'escape']);

  declare args: JsonPathExprArgs;

  constructor (args: JsonPathExprArgs = {}) {
    super(args);
  }

  get outputName (): string {
    const lastSegment = this.args.expressions?.[this.args.expressions.length - 1];
    const thisValue = lastSegment?.args.this;
    return typeof thisValue === 'string' ? thisValue : '';
  }
}

export type JsonPathPartExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class JsonPathPartExpr extends Expression {
  static key = ExpressionKey.JSON_PATH_PART;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: JsonPathPartExprArgs;

  constructor (args: JsonPathPartExprArgs = {}) {
    super(args);
  }
}

export type FormatJsonExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class FormatJsonExpr extends Expression {
  static key = ExpressionKey.FORMAT_JSON;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: FormatJsonExprArgs;

  constructor (args: FormatJsonExprArgs = {}) {
    super(args);
  }
}

export type JsonKeyValueExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: ExpressionValue;
    expression?: ExpressionValue;
  },
]>;

export class JsonKeyValueExpr extends Expression {
  static key = ExpressionKey.JSON_KEY_VALUE;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: JsonKeyValueExprArgs;

  constructor (args: JsonKeyValueExprArgs = {}) {
    super(args);
  }
}

/**
 * Valid kind values for JSON column definitions
 */
export type JsonColumnDefExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    kind?: JsonColumnDefExprKind;
    path?: Expression;
    nestedSchema?: Expression;
    ordinality?: boolean;
  },
]>;

export class JsonColumnDefExpr extends Expression {
  static key = ExpressionKey.JSON_COLUMN_DEF;

  static availableArgs = new Set([
    'this',
    'kind',
    'path',
    'nestedSchema',
    'ordinality',
  ]);

  declare args: JsonColumnDefExprArgs;

  constructor (args: JsonColumnDefExprArgs = {}) {
    super(args);
  }
}

export type JsonSchemaExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions?: Expression[] },
]>;

export class JsonSchemaExpr extends Expression {
  static key = ExpressionKey.JSON_SCHEMA;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: JsonSchemaExprArgs;

  constructor (args: JsonSchemaExprArgs = {}) {
    super(args);
  }
}

export type JsonValueExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    path?: Expression;
    returning?: Expression;
    onCondition?: Expression;
  },
]>;

export class JsonValueExpr extends Expression {
  static key = ExpressionKey.JSON_VALUE;

  static requiredArgs = new Set(['this', 'path']);

  static availableArgs = new Set([
    'this',
    'path',
    'returning',
    'onCondition',
  ]);

  declare args: JsonValueExprArgs;

  constructor (args: JsonValueExprArgs = {}) {
    super(args);
  }
}

/**
 * Enumeration of valid kind values for OpenJsonColumnDef expressions.
 * Used to specify the variant or subtype of the expression.
 */
export type OpenJsonColumnDefExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    kind?: OpenJsonColumnDefExprKind;
    path?: Expression;
    asJson?: Expression;
  },
]>;

export class OpenJsonColumnDefExpr extends Expression {
  static key = ExpressionKey.OPEN_JSON_COLUMN_DEF;

  static requiredArgs = new Set(['this', 'kind']);

  static availableArgs = new Set([
    'this',
    'kind',
    'path',
    'asJson',
  ]);

  declare args: OpenJsonColumnDefExprArgs;

  constructor (args: OpenJsonColumnDefExprArgs = {}) {
    super(args);
  }
}

export type JsonExtractQuoteExprArgs = Merge<[
  BaseExpressionArgs,
  {
    option?: Expression;
    scalar?: boolean;
  },
]>;

export class JsonExtractQuoteExpr extends Expression {
  static key = ExpressionKey.JSON_EXTRACT_QUOTE;

  static requiredArgs = new Set(['option']);
  static availableArgs = new Set(['option', 'scalar']);

  declare args: JsonExtractQuoteExprArgs;

  constructor (args: JsonExtractQuoteExprArgs = {}) {
    super(args);
  }
}

export type ScopeResolutionExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class ScopeResolutionExpr extends Expression {
  static key = ExpressionKey.SCOPE_RESOLUTION;

  static requiredArgs = new Set(['expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: ScopeResolutionExprArgs;

  constructor (args: ScopeResolutionExprArgs = {}) {
    super(args);
  }
}

export type SliceExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: ExpressionOrNumber;
    step?: Expression;
  },
]>;

export class SliceExpr extends Expression {
  static key = ExpressionKey.SLICE;

  static availableArgs = new Set([
    'this',
    'expression',
    'step',
  ]);

  declare args: SliceExprArgs;

  constructor (args: SliceExprArgs = {}) {
    super(args);
  }
}

export type StreamExprArgs = Merge<[
  BaseExpressionArgs,
]>;
export class StreamExpr extends Expression {
  static key = ExpressionKey.STREAM;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: StreamExprArgs;

  constructor (args: StreamExprArgs = {}) {
    super(args);
  }
}

export type ModelAttributeExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class ModelAttributeExpr extends Expression {
  static key = ExpressionKey.MODEL_ATTRIBUTE;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: ModelAttributeExprArgs;

  constructor (args: ModelAttributeExprArgs = {}) {
    super(args);
  }
}

export type WeekStartExprArgs = Merge<[
  BaseExpressionArgs,
  { this?: Expression },
]>;
export class WeekStartExpr extends Expression {
  static key = ExpressionKey.WEEK_START;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: WeekStartExprArgs;

  constructor (args: WeekStartExprArgs = {}) {
    super(args);
  }
}

export type XmlNamespaceExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class XmlNamespaceExpr extends Expression {
  static key = ExpressionKey.XML_NAMESPACE;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: XmlNamespaceExprArgs;

  constructor (args: XmlNamespaceExprArgs = {}) {
    super(args);
  }
}

export type XmlKeyValueOptionExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class XmlKeyValueOptionExpr extends Expression {
  static key = ExpressionKey.XML_KEY_VALUE_OPTION;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: XmlKeyValueOptionExprArgs;

  constructor (args: XmlKeyValueOptionExprArgs = {}) {
    super(args);
  }
}

/**
 * Valid kind values for USE statements
 */
export type UseExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: UseExprKind;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class UseExpr extends Expression {
  static key = ExpressionKey.USE;

  static availableArgs = new Set([
    'this',
    'expressions',
    'kind',
  ]);

  declare args: UseExprArgs;

  constructor (args: UseExprArgs = {}) {
    super(args);
  }
}

export type WhenExprArgs = Merge<[
  BaseExpressionArgs,
  {
    matched?: ExpressionOrBoolean;
    source?: Expression;
    condition?: Expression;
    then?: Expression;
  },
]>;

export class WhenExpr extends Expression {
  static key = ExpressionKey.WHEN;

  static requiredArgs = new Set(['matched', 'then']);

  static availableArgs = new Set([
    'matched',
    'source',
    'condition',
    'then',
  ]);

  declare args: WhenExprArgs;

  constructor (args: WhenExprArgs = {}) {
    super(args);
  }
}

export type WhensExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions?: WhenExpr[] },
]>;

export class WhensExpr extends Expression {
  static key = ExpressionKey.WHENS;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: WhensExprArgs;

  constructor (args: WhensExprArgs = {}) {
    super(args);
  }
}

export type SemicolonExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class SemicolonExpr extends Expression {
  static key = ExpressionKey.SEMICOLON;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: SemicolonExprArgs;

  constructor (args: SemicolonExprArgs = {}) {
    super(args);
  }
}

export type TableColumnExprArgs = Merge<[
  BaseExpressionArgs,
  { this?: Expression },
]>;

export class TableColumnExpr extends Expression {
  static key = ExpressionKey.TABLE_COLUMN;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: TableColumnExprArgs;

  constructor (args: TableColumnExprArgs = {}) {
    super(args);
  }
}

export type VariadicExprArgs = Merge<[
  BaseExpressionArgs,
]>;

export class VariadicExpr extends Expression {
  static key = ExpressionKey.VARIADIC;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: VariadicExprArgs;

  constructor (args: VariadicExprArgs = {}) {
    super(args);
  }
}

export type CteExprArgs = Merge<[
  BaseExpressionArgs,
  {
    scalar?: boolean;
    materialized?: boolean;
    keyExpressions?: Expression[];
    alias?: Expression;
    this?: Expression;
  },
]>;

export class CteExpr extends DerivedTableExpr {
  static key = ExpressionKey.CTE;

  static requiredArgs = new Set(['this', 'alias']);

  static availableArgs = new Set([
    'this',
    'alias',
    'scalar',
    'materialized',
    'keyExpressions',
  ]);

  declare args: CteExprArgs;

  constructor (args: CteExprArgs = {}) {
    super(args);
  }
}

export type BitStringExprArgs = Merge<[
  ConditionExprArgs,
]>;

export class BitStringExpr extends ConditionExpr {
  static key = ExpressionKey.BIT_STRING;

  declare args: BitStringExprArgs;

  constructor (args: BitStringExprArgs = {}) {
    super(args);
  }
}

export type HexStringExprArgs = Merge<[
  BaseExpressionArgs,
  {
    isInteger?: boolean;
    this?: Expression;
  },
]>;

export class HexStringExpr extends ConditionExpr {
  static key = ExpressionKey.HEX_STRING;

  static availableArgs = new Set(['this', 'isInteger']);

  declare args: HexStringExprArgs;

  constructor (args: HexStringExprArgs = {}) {
    super(args);
  }
}

export type ByteStringExprArgs = Merge<[
  ConditionExprArgs,
  {
    isBytes?: boolean;
    this?: Expression;
  },
]>;

export class ByteStringExpr extends ConditionExpr {
  static key = ExpressionKey.BYTE_STRING;

  static availableArgs = new Set(['this', 'isBytes']);

  declare args: ByteStringExprArgs;

  constructor (args: ByteStringExprArgs = {}) {
    super(args);
  }
}

export type RawStringExprArgs = Merge<[
  ConditionExprArgs,
]>;

export class RawStringExpr extends ConditionExpr {
  static key = ExpressionKey.RAW_STRING;

  declare args: RawStringExprArgs;

  constructor (args: RawStringExprArgs = {}) {
    super(args);
  }
}

export type UnicodeStringExprArgs = Merge<[
  ConditionExprArgs,
  {
    escape?: Expression;
    this?: Expression;
  },
]>;

export class UnicodeStringExpr extends ConditionExpr {
  static key = ExpressionKey.UNICODE_STRING;

  static availableArgs = new Set(['this', 'escape']);

  declare args: UnicodeStringExprArgs;

  constructor (args: UnicodeStringExprArgs = {}) {
    super(args);
  }
}

/**
 * Represents a column reference (optionally qualified with table name).
 *
 * @example
 * // users.id
 * const col = column('id', 'users');
 */
export type ColumnExprArgs = Merge<[
  ConditionExprArgs,
  {
    table?: string | Expression;
    db?: string | Expression;
    catalog?: string | Expression;
    this?: ExpressionValue<IdentifierExpr | StarExpr>; // NOTE: sqlglot does not define `this` to also have type `StarExpr`, but based on the column function, I think it should also have this type
    joinMark?: Expression;
  },
]>;

export class ColumnExpr extends ConditionExpr {
  static key = ExpressionKey.COLUMN;

  static availableArgs = new Set([
    'this',
    'table',
    'db',
    'catalog',
    'joinMark',
  ]);

  declare args: ColumnExprArgs;

  constructor (args: ColumnExprArgs = {}) {
    super(args);
  }

  /**
   * Gets the table name as a string
   * @returns The table name
   */
  get table (): string {
    return this.text('table');
  }

  /**
   * Gets the database name as a string
   * @returns The database name
   */
  get db (): string {
    return this.text('db');
  }

  /**
   * Gets the catalog name as a string
   * @returns The catalog name
   */
  get catalog (): string {
    return this.text('catalog');
  }

  /**
   * Gets the output name of the column
   * @returns The column name
   */
  get outputName (): string {
    return this.name;
  }

  /**
   * Return the parts of a column in order catalog, db, table, name.
   * @returns Array of Identifier expressions for each part that exists
   */
  get parts (): [] | [...Expression[], StarExpr] {
    const result = [];
    for (const part of [
      'catalog',
      'db',
      'table',
      'this',
    ] as const) {
      const value = this.args[part];
      if (value) {
        result.push(value);
      }
    }
    return result as [] | [...IdentifierExpr[], StarExpr];
  }

  toDot (options: { includeDots?: boolean } = {}): DotExpr | IdentifierExpr | StarExpr {
    const { includeDots = true } = options;
    const parts: Expression[] = this.parts;
    let parent = this.parent;

    if (includeDots) {
      while (parent instanceof DotExpr && parent.args.expression !== undefined) {
        parts.push(parent.args.expression);
        parent = parent.parent;
      }
    }

    return 1 < parts.length ? DotExpr.build(parts.map((p) => p.copy())) : parts[0] as IdentifierExpr | StarExpr;
  }
}

export type PseudocolumnExprArgs = Merge<[
  ColumnExprArgs,
]>;

export class PseudocolumnExpr extends ColumnExpr {
  static key = ExpressionKey.PSEUDOCOLUMN;

  declare args: PseudocolumnExprArgs;

  constructor (args: PseudocolumnExprArgs = {}) {
    super(args);
  }
}

export type AutoIncrementColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class AutoIncrementColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.AUTO_INCREMENT_COLUMN_CONSTRAINT;

  declare args: AutoIncrementColumnConstraintExprArgs;

  constructor (args: AutoIncrementColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type ZeroFillColumnConstraintExprArgs = Merge<[
  ColumnConstraintExprArgs,
]>;

export class ZeroFillColumnConstraintExpr extends ColumnConstraintExpr {
  static key = ExpressionKey.ZERO_FILL_COLUMN_CONSTRAINT;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: ZeroFillColumnConstraintExprArgs;

  constructor (args: ZeroFillColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type PeriodForSystemTimeConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class PeriodForSystemTimeConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.PERIOD_FOR_SYSTEM_TIME_CONSTRAINT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: PeriodForSystemTimeConstraintExprArgs;

  constructor (args: PeriodForSystemTimeConstraintExprArgs = {}) {
    super(args);
  }
}

export type CaseSpecificColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  { not?: Expression },
]>;

export class CaseSpecificColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.CASE_SPECIFIC_COLUMN_CONSTRAINT;

  static requiredArgs = new Set(['not']);
  static availableArgs = new Set(['not']);

  declare args: CaseSpecificColumnConstraintExprArgs;

  constructor (args: CaseSpecificColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type CharacterSetColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  { this?: Expression },
]>;
export class CharacterSetColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.CHARACTER_SET_COLUMN_CONSTRAINT;

  declare args: CharacterSetColumnConstraintExprArgs;

  constructor (args: CharacterSetColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type CheckColumnConstraintExprArgs = Merge<[
  BaseExpressionArgs,
  {
    enforced?: Expression;
    this?: Expression;
  },
]>;

export class CheckColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.CHECK_COLUMN_CONSTRAINT;

  static availableArgs = new Set(['this', 'enforced']);

  declare args: CheckColumnConstraintExprArgs;

  constructor (args: CheckColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type ClusteredColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class ClusteredColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.CLUSTERED_COLUMN_CONSTRAINT;

  declare args: ClusteredColumnConstraintExprArgs;

  constructor (args: ClusteredColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type CollateColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class CollateColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.COLLATE_COLUMN_CONSTRAINT;

  declare args: CollateColumnConstraintExprArgs;

  constructor (args: CollateColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type CommentColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class CommentColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.COMMENT_COLUMN_CONSTRAINT;

  declare args: CommentColumnConstraintExprArgs;

  constructor (args: CommentColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type CompressColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  { this?: Expression },
]>;

export class CompressColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.COMPRESS_COLUMN_CONSTRAINT;

  declare args: CompressColumnConstraintExprArgs;

  constructor (args: CompressColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type DateFormatColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  { this?: Expression },
]>;

export class DateFormatColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.DATE_FORMAT_COLUMN_CONSTRAINT;

  declare args: DateFormatColumnConstraintExprArgs;

  constructor (args: DateFormatColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type DefaultColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class DefaultColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.DEFAULT_COLUMN_CONSTRAINT;

  declare args: DefaultColumnConstraintExprArgs;

  constructor (args: DefaultColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type EncodeColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class EncodeColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.ENCODE_COLUMN_CONSTRAINT;

  declare args: EncodeColumnConstraintExprArgs;

  constructor (args: EncodeColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type ExcludeColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class ExcludeColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.EXCLUDE_COLUMN_CONSTRAINT;

  declare args: ExcludeColumnConstraintExprArgs;

  constructor (args: ExcludeColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type EphemeralColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  { this?: Expression },
]>;

export class EphemeralColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.EPHEMERAL_COLUMN_CONSTRAINT;

  declare args: EphemeralColumnConstraintExprArgs;

  constructor (args: EphemeralColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type GeneratedAsIdentityColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  {
    onNull?: Expression;
    start?: Expression;
    increment?: Expression;
    minvalue?: string;
    maxvalue?: string;
    cycle?: Expression;
    order?: boolean;
    this?: boolean;
    expression?: Expression;
  },
]>;

export class GeneratedAsIdentityColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.GENERATED_AS_IDENTITY_COLUMN_CONSTRAINT;

  static availableArgs = new Set([
    'this',
    'expression',
    'onNull',
    'start',
    'increment',
    'minvalue',
    'maxvalue',
    'cycle',
    'order',
  ]);

  declare args: GeneratedAsIdentityColumnConstraintExprArgs;

  constructor (args: GeneratedAsIdentityColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type GeneratedAsRowColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  {
    start?: Expression;
    hidden?: Expression;
  },
]>;

export class GeneratedAsRowColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.GENERATED_AS_ROW_COLUMN_CONSTRAINT;

  static availableArgs = new Set(['start', 'hidden']);

  declare args: GeneratedAsRowColumnConstraintExprArgs;

  constructor (args: GeneratedAsRowColumnConstraintExprArgs = {}) {
    super(args);
  }
}

/**
 * Valid kind values for index column constraints
 */
export type IndexColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  {
    kind?: IndexColumnConstraintExprKind;
    indexType?: Expression;
    options?: Expression[];
    granularity?: Expression;
    this?: Expression;
    expressions?: Expression[];
    expression?: Expression;
  },
]>;

export class IndexColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.INDEX_COLUMN_CONSTRAINT;

  static availableArgs = new Set([
    'this',
    'expressions',
    'kind',
    'indexType',
    'options',
    'expression',
    'granularity',
  ]);

  declare args: IndexColumnConstraintExprArgs;

  constructor (args: IndexColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type InlineLengthColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class InlineLengthColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.INLINE_LENGTH_COLUMN_CONSTRAINT;

  declare args: InlineLengthColumnConstraintExprArgs;

  constructor (args: InlineLengthColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type NonClusteredColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class NonClusteredColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.NON_CLUSTERED_COLUMN_CONSTRAINT;

  declare args: NonClusteredColumnConstraintExprArgs;

  constructor (args: NonClusteredColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type NotForReplicationColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class NotForReplicationColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.NOT_FOR_REPLICATION_COLUMN_CONSTRAINT;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: NotForReplicationColumnConstraintExprArgs;

  constructor (args: NotForReplicationColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type MaskingPolicyColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class MaskingPolicyColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.MASKING_POLICY_COLUMN_CONSTRAINT;

  static availableArgs = new Set(['this', 'expressions']);

  declare args: MaskingPolicyColumnConstraintExprArgs;

  constructor (args: MaskingPolicyColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type NotNullColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  { allowNull?: Expression },
]>;

export class NotNullColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.NOT_NULL_COLUMN_CONSTRAINT;

  static availableArgs = new Set(['allowNull']);

  declare args: NotNullColumnConstraintExprArgs;

  constructor (args: NotNullColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type OnUpdateColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class OnUpdateColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.ON_UPDATE_COLUMN_CONSTRAINT;

  declare args: OnUpdateColumnConstraintExprArgs;

  constructor (args: OnUpdateColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type PrimaryKeyColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  {
    desc?: Expression;
    options?: Expression[];
  },
]>;

export class PrimaryKeyColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.PRIMARY_KEY_COLUMN_CONSTRAINT;

  static availableArgs = new Set(['desc', 'options']);

  declare args: PrimaryKeyColumnConstraintExprArgs;

  constructor (args: PrimaryKeyColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type TitleColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class TitleColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.TITLE_COLUMN_CONSTRAINT;

  declare args: TitleColumnConstraintExprArgs;

  constructor (args: TitleColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type UniqueColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  {
    indexType?: Expression;
    onConflict?: Expression;
    nulls?: Expression[];
    options?: Expression[];
    this?: Expression;
  },
]>;

export class UniqueColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.UNIQUE_COLUMN_CONSTRAINT;

  static availableArgs = new Set([
    'this',
    'indexType',
    'onConflict',
    'nulls',
    'options',
  ]);

  declare args: UniqueColumnConstraintExprArgs;

  constructor (args: UniqueColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type UppercaseColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class UppercaseColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.UPPERCASE_COLUMN_CONSTRAINT;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: UppercaseColumnConstraintExprArgs;

  constructor (args: UppercaseColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type PathColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class PathColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.PATH_COLUMN_CONSTRAINT;

  declare args: PathColumnConstraintExprArgs;

  constructor (args: PathColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type ProjectionPolicyColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
]>;

export class ProjectionPolicyColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.PROJECTION_POLICY_COLUMN_CONSTRAINT;

  declare args: ProjectionPolicyColumnConstraintExprArgs;

  constructor (args: ProjectionPolicyColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type ComputedColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  {
    persisted?: boolean;
    notNull?: boolean;
    dataType?: Expression;
    this?: Expression;
  },
]>;

export class ComputedColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.COMPUTED_COLUMN_CONSTRAINT;

  static availableArgs = new Set([
    'this',
    'persisted',
    'notNull',
    'dataType',
  ]);

  declare args: ComputedColumnConstraintExprArgs;

  constructor (args: ComputedColumnConstraintExprArgs = {}) {
    super(args);
  }
}

export type InOutColumnConstraintExprArgs = Merge<[
  ColumnConstraintKindExprArgs,
  {
    input?: Expression;
    output?: Expression;
    variadic?: Expression;
  },
]>;

export class InOutColumnConstraintExpr extends ColumnConstraintKindExpr {
  static key = ExpressionKey.IN_OUT_COLUMN_CONSTRAINT;

  static availableArgs = new Set([
    'input',
    'output',
    'variadic',
  ]);

  declare args: InOutColumnConstraintExprArgs;

  constructor (args: InOutColumnConstraintExprArgs = {}) {
    super(args);
  }
}

/**
 * Enumeration of valid kind values for Copy expressions.
 * Used to specify the variant or subtype of the expression.
 */
export type CopyExprArgs = Merge<[
  BaseExpressionArgs,
  {
    kind?: CopyExprKind;
    files?: Expression[];
    credentials?: Expression[];
    format?: string;
    params?: Expression[];
    this?: Expression;
  },
]>;

export class CopyExpr extends DmlExpr {
  static key = ExpressionKey.COPY;

  static requiredArgs = new Set(['this', 'kind']);

  static availableArgs = new Set([
    'this',
    'kind',
    'files',
    'credentials',
    'format',
    'params',
  ]);

  declare args: CopyExprArgs;

  constructor (args: CopyExprArgs = {}) {
    super(args);
  }
}

export type InsertExprArgs = Merge<[
  DmlExprArgs,
  DdlExprArgs,
  {
    hint?: Expression;
    with?: Expression;
    isFunction?: boolean;
    conflict?: Expression;
    returning?: Expression;
    overwrite?: boolean;
    exists?: boolean;
    alternative?: Expression;
    where?: Expression;
    ignore?: Expression;
    byName?: string;
    stored?: Expression;
    partition?: Expression;
    settings?: Expression[];
    source?: Expression;
    default?: Expression;
    this?: Expression;
    expression?: Expression;
  },
]>;

export class InsertExpr extends multiInherit(DdlExpr, DmlExpr, Expression) {
  static key = ExpressionKey.INSERT;

  static availableArgs = new Set([
    'hint',
    'with',
    'isFunction',
    'this',
    'expression',
    'conflict',
    'returning',
    'overwrite',
    'exists',
    'alternative',
    'where',
    'ignore',
    'byName',
    'stored',
    'partition',
    'settings',
    'source',
    'default',
  ]);

  declare args: InsertExprArgs;

  constructor (args: InsertExprArgs = {}) {
    super(args);
  }

  /**
   * Append to or set the common table expressions.
   *
   * @example
   * insert("SELECT x FROM cte", "t").with("cte", "SELECT * FROM tbl").sql()
   * // 'WITH cte AS (SELECT * FROM tbl) INSERT INTO t SELECT x FROM cte'
   *
   * @param alias - the SQL code string to parse as the table name.
   *   If an Expression instance is passed, this is used as-is.
   * @param as - the SQL code string to parse as the table expression.
   *   If an Expression instance is passed, it will be used as-is.
   * @param options - Configuration options
   * @param options.recursive - set the RECURSIVE part of the expression. Defaults to false.
   * @param options.materialized - set the MATERIALIZED part of the expression.
   * @param options.append - if true, add to any existing expressions. Otherwise, this resets the
   * expressions.
   * @param options.dialect - the dialect used to parse the input expression.
   * @param options.copy - if false, modify this expression instance in-place.
   * @returns The modified expression.
   */
  with (
    alias?: ExpressionOrString<IdentifierExpr>,
    as?: ExpressionOrString<QueryExpr>,
    options: {
      recursive?: boolean;
      materialized?: boolean;
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const {
      recursive, materialized, append, dialect, copy, ...restOptions
    } = options;
    return applyCteBuilder({
      instance: this,
      alias,
      as,
      recursive,
      materialized,
      append,
      dialect,
      copy,
      ...restOptions,
    }) as this;
  }
}

/**
 * Represents a literal value (string, number, boolean).
 *
 * @example
 * const str = new LiteralExpr({ this: 'hello', isString: true });
 * const num = new LiteralExpr({ this: '42', isString: false });
 */
export type LiteralExprArgs = Merge<[
  BaseExpressionArgs,
  {
    isString?: boolean;
    this?: string;
  },
]>;

export class LiteralExpr extends ConditionExpr {
  static key = ExpressionKey.LITERAL;

  static requiredArgs = new Set(['this', 'isString']);

  static availableArgs = new Set(['this', 'isString']);

  declare args: LiteralExprArgs;

  /**
   * Create a numeric literal expression
   * @param number - The number value
   * @returns A literal expression (or negative expression for negative numbers)
   */
  static number (number: number | string): LiteralExpr | NegExpr {
    let expr: LiteralExpr | NegExpr = new LiteralExpr({
      this: String(number),
      isString: false,
    });

    const numValue = typeof number === 'number'
      ? number
      : parseFloat(String(number));

    if (!isNaN(numValue) && numValue < 0) {
      expr = new LiteralExpr({
        this: String(Math.abs(numValue)),
        isString: false,
      });
      expr = new NegExpr({ this: expr });
    }

    return expr;
  }

  /**
   * Create a string literal expression
   * @param string - The string value
   * @returns A literal expression
   */
  static string (string: unknown): LiteralExpr {
    return new LiteralExpr({
      this: String(string),
      isString: true,
    });
  }

  constructor (args: LiteralExprArgs = {}) {
    super(args);
  }

  get outputName (): string {
    return this.name;
  }

  /**
   * Convert the literal to a Javascript value.
   * Returns a number (int or float) for numeric literals, or string for string literals.
   */
  toValue (): number | string {
    if (this.isNumber) {
      const parsed = parseInt(this.args.this as string, 10);
      if (!isNaN(parsed)) {
        return parsed;
      }
      const floatParsed = parseFloat(this.args.this as string);
      if (!isNaN(floatParsed)) {
        return floatParsed;
      }
    }
    return this.args.this as string;
  }
}

export type ClusterExprArgs = Merge<[
  OrderExprArgs,
]>;

export class ClusterExpr extends OrderExpr {
  static key = ExpressionKey.CLUSTER;

  declare args: ClusterExprArgs;

  constructor (args: ClusterExprArgs = {}) {
    super(args);
  }
}

export type DistributeExprArgs = Merge<[
  OrderExprArgs,
]>;

export class DistributeExpr extends OrderExpr {
  static key = ExpressionKey.DISTRIBUTE;

  declare args: DistributeExprArgs;

  constructor (args: DistributeExprArgs = {}) {
    super(args);
  }
}

export type SortExprArgs = Merge<[
  OrderExprArgs,
]>;

export class SortExpr extends OrderExpr {
  static key = ExpressionKey.SORT;

  declare args: SortExprArgs;

  constructor (args: SortExprArgs = {}) {
    super(args);
  }
}

export type AlgorithmPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class AlgorithmPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.ALGORITHM_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: AlgorithmPropertyExprArgs;

  constructor (args: AlgorithmPropertyExprArgs = {}) {
    super(args);
  }
}

export type AutoIncrementPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class AutoIncrementPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.AUTO_INCREMENT_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: AutoIncrementPropertyExprArgs;

  constructor (args: AutoIncrementPropertyExprArgs = {}) {
    super(args);
  }
}

export type AutoRefreshPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class AutoRefreshPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.AUTO_REFRESH_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: AutoRefreshPropertyExprArgs;

  constructor (args: AutoRefreshPropertyExprArgs = {}) {
    super(args);
  }
}

export type BackupPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class BackupPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.BACKUP_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: BackupPropertyExprArgs;

  constructor (args: BackupPropertyExprArgs = {}) {
    super(args);
  }
}

export type BuildPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class BuildPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.BUILD_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: BuildPropertyExprArgs;

  constructor (args: BuildPropertyExprArgs = {}) {
    super(args);
  }
}

export type BlockCompressionPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    autotemp?: Expression;
    always?: Expression[];
    default?: Expression;
    manual?: Expression;
    never?: Expression;
  },
]>;

export class BlockCompressionPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.BLOCK_COMPRESSION_PROPERTY;

  static availableArgs = new Set([
    'autotemp',
    'always',
    'default',
    'manual',
    'never',
  ]);

  declare args: BlockCompressionPropertyExprArgs;

  constructor (args: BlockCompressionPropertyExprArgs = {}) {
    super(args);
  }
}

export type CharacterSetPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    default?: Expression;
    this?: Expression;
  },
]>;

export class CharacterSetPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.CHARACTER_SET_PROPERTY;

  static requiredArgs = new Set(['this', 'default']);

  static availableArgs = new Set(['this', 'default']);

  declare args: CharacterSetPropertyExprArgs;

  constructor (args: CharacterSetPropertyExprArgs = {}) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }
}

export type ChecksumPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    on?: Expression;
    default?: Expression;
  },
]>;

export class ChecksumPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.CHECKSUM_PROPERTY;

  static availableArgs = new Set(['on', 'default']);

  declare args: ChecksumPropertyExprArgs;

  constructor (args: ChecksumPropertyExprArgs = {}) {
    super(args);
  }
}

export type CollatePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    default?: Expression;
    this?: Expression;
  },
]>;

export class CollatePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.COLLATE_PROPERTY;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set(['this', 'default']);

  declare args: CollatePropertyExprArgs;

  constructor (args: CollatePropertyExprArgs = {}) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }
}

export type CopyGrantsPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class CopyGrantsPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.COPY_GRANTS_PROPERTY;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: CopyGrantsPropertyExprArgs;

  constructor (args: CopyGrantsPropertyExprArgs = {}) {
    super(args);
  }
}

export type DataBlocksizePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    size?: number | Expression;
    units?: Expression[];
    minimum?: Expression;
    maximum?: Expression;
    default?: Expression;
  },
]>;

export class DataBlocksizePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.DATA_BLOCKSIZE_PROPERTY;

  static availableArgs = new Set([
    'size',
    'units',
    'minimum',
    'maximum',
    'default',
  ]);

  declare args: DataBlocksizePropertyExprArgs;

  constructor (args: DataBlocksizePropertyExprArgs = {}) {
    super(args);
  }
}

export type DataDeletionPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    on?: Expression;
    filterColumn?: Expression;
    retentionPeriod?: Expression;
  },
]>;

export class DataDeletionPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.DATA_DELETION_PROPERTY;

  static requiredArgs = new Set(['on']);
  static availableArgs = new Set([
    'on',
    'filterColumn',
    'retentionPeriod',
  ]);

  declare args: DataDeletionPropertyExprArgs;

  constructor (args: DataDeletionPropertyExprArgs = {}) {
    super(args);
  }
}

export type DefinerPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: string },
]>;

export class DefinerPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.DEFINER_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: DefinerPropertyExprArgs;

  constructor (args: DefinerPropertyExprArgs = {}) {
    super(args);
  }
}

export type DistKeyPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class DistKeyPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.DIST_KEY_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: DistKeyPropertyExprArgs;

  constructor (args: DistKeyPropertyExprArgs = {}) {
    super(args);
  }
}

/**
 * Enumeration of valid kind values for DistributedByProperty expressions.
 * Used to specify the variant or subtype of the expression.
 */
export type DistributedByPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    kind?: DistributedByPropertyExprKind;
    buckets?: Expression[];
    order?: Expression;
    expressions?: Expression[];
  },
]>;

export class DistributedByPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.DISTRIBUTED_BY_PROPERTY;

  static requiredArgs = new Set(['kind']);
  static availableArgs = new Set([
    'expressions',
    'kind',
    'buckets',
    'order',
  ]);

  declare args: DistributedByPropertyExprArgs;

  constructor (args: DistributedByPropertyExprArgs = {}) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }
}

export type DistStylePropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class DistStylePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.DIST_STYLE_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: DistStylePropertyExprArgs;

  constructor (args: DistStylePropertyExprArgs = {}) {
    super(args);
  }
}

export type DuplicateKeyPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { expressions?: Expression[] },
]>;

export class DuplicateKeyPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.DUPLICATE_KEY_PROPERTY;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: DuplicateKeyPropertyExprArgs;

  constructor (args: DuplicateKeyPropertyExprArgs = {}) {
    super(args);
  }
}

export type EnginePropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class EnginePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.ENGINE_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: EnginePropertyExprArgs;

  constructor (args: EnginePropertyExprArgs = {}) {
    super(args);
  }
}

export type HeapPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class HeapPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.HEAP_PROPERTY;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: HeapPropertyExprArgs;

  constructor (args: HeapPropertyExprArgs = {}) {
    super(args);
  }
}

export type ToTablePropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class ToTablePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.TO_TABLE_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: ToTablePropertyExprArgs;

  constructor (args: ToTablePropertyExprArgs = {}) {
    super(args);
  }
}

export type ExecuteAsPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class ExecuteAsPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.EXECUTE_AS_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: ExecuteAsPropertyExprArgs;

  constructor (args: ExecuteAsPropertyExprArgs = {}) {
    super(args);
  }
}

export type ExternalPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    this?: Expression;
  },
]>;

export class ExternalPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.EXTERNAL_PROPERTY;

  static availableArgs = new Set(['this']);

  declare args: ExternalPropertyExprArgs;

  constructor (args: ExternalPropertyExprArgs = {}) {
    super(args);
  }
}

export type FallbackPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    no?: Expression;
    protection?: Expression;
  },
]>;

export class FallbackPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.FALLBACK_PROPERTY;

  static requiredArgs = new Set(['no']);
  static availableArgs = new Set(['no', 'protection']);

  declare args: FallbackPropertyExprArgs;

  constructor (args: FallbackPropertyExprArgs = {}) {
    super(args);
  }
}

export type FileFormatPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    hiveFormat?: string;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class FileFormatPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.FILE_FORMAT_PROPERTY;

  static availableArgs = new Set([
    'this',
    'expressions',
    'hiveFormat',
  ]);

  declare args: FileFormatPropertyExprArgs;

  constructor (args: FileFormatPropertyExprArgs = {}) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }
}

export type CredentialsPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { expressions?: Expression[] },
]>;

export class CredentialsPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.CREDENTIALS_PROPERTY;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: CredentialsPropertyExprArgs;

  constructor (args: CredentialsPropertyExprArgs = {}) {
    super(args);
  }
}

export type FreespacePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    this?: Expression;
    percent?: Expression;
  },
]>;

export class FreespacePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.FREESPACE_PROPERTY;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set(['this', 'percent']);

  declare args: FreespacePropertyExprArgs;

  constructor (args: FreespacePropertyExprArgs = {}) {
    super(args);
  }
}

export type GlobalPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class GlobalPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.GLOBAL_PROPERTY;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: GlobalPropertyExprArgs;

  constructor (args: GlobalPropertyExprArgs = {}) {
    super(args);
  }
}

export type IcebergPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class IcebergPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.ICEBERG_PROPERTY;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: IcebergPropertyExprArgs;

  constructor (args: IcebergPropertyExprArgs = {}) {
    super(args);
  }
}

export type InheritsPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { expressions?: Expression[] },
]>;

export class InheritsPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.INHERITS_PROPERTY;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: InheritsPropertyExprArgs;

  constructor (args: InheritsPropertyExprArgs = {}) {
    super(args);
  }
}

export type InputModelPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class InputModelPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.INPUT_MODEL_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: InputModelPropertyExprArgs;

  constructor (args: InputModelPropertyExprArgs = {}) {
    super(args);
  }
}

export type OutputModelPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class OutputModelPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.OUTPUT_MODEL_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: OutputModelPropertyExprArgs;

  constructor (args: OutputModelPropertyExprArgs = {}) {
    super(args);
  }
}

export type IsolatedLoadingPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    no?: Expression;
    concurrent?: Expression;
    target?: Expression;
  },
]>;

export class IsolatedLoadingPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.ISOLATED_LOADING_PROPERTY;

  static availableArgs = new Set([
    'no',
    'concurrent',
    'target',
  ]);

  declare args: IsolatedLoadingPropertyExprArgs;

  constructor (args: IsolatedLoadingPropertyExprArgs = {}) {
    super(args);
  }
}

export type JournalPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    no?: Expression;
    dual?: Expression;
    before?: Expression;
    local?: Expression;
    after?: Expression;
  },
]>;

export class JournalPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.JOURNAL_PROPERTY;

  static availableArgs = new Set([
    'no',
    'dual',
    'before',
    'local',
    'after',
  ]);

  declare args: JournalPropertyExprArgs;

  constructor (args: JournalPropertyExprArgs = {}) {
    super(args);
  }
}

export type LanguagePropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class LanguagePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.LANGUAGE_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: LanguagePropertyExprArgs;

  constructor (args: LanguagePropertyExprArgs = {}) {
    super(args);
  }
}

export type EnviromentPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { expressions?: Expression[] },
]>;

export class EnviromentPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.ENVIROMENT_PROPERTY;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: EnviromentPropertyExprArgs;

  constructor (args: EnviromentPropertyExprArgs = {}) {
    super(args);
  }
}

export type ClusteredByPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    expressions?: Expression[];
    sortedBy?: string;
    buckets?: Expression[];
  },
]>;

export class ClusteredByPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.CLUSTERED_BY_PROPERTY;

  static requiredArgs = new Set(['expressions', 'buckets']);

  static availableArgs = new Set([
    'expressions',
    'sortedBy',
    'buckets',
  ]);

  declare args: ClusteredByPropertyExprArgs;

  constructor (args: ClusteredByPropertyExprArgs = {}) {
    super(args);
  }
}

/**
 * Enumeration of valid kind values for DictProperty expressions.
 * Used to specify the variant or subtype of the expression.
 */
export type DictPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    this?: Expression;
    kind?: DictPropertyExprKind;
    settings?: Expression[];
  },
]>;

export class DictPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.DICT_PROPERTY;

  static requiredArgs = new Set(['this', 'kind']);

  static availableArgs = new Set([
    'this',
    'kind',
    'settings',
  ]);

  declare args: DictPropertyExprArgs;

  constructor (args: DictPropertyExprArgs = {}) {
    super(args);
  }
}

export type DictSubPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class DictSubPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.DICT_SUB_PROPERTY;

  declare args: DictSubPropertyExprArgs;

  constructor (args: DictSubPropertyExprArgs = {}) {
    super(args);
  }
}

export type DictRangeExprArgs = Merge<[
  PropertyExprArgs,
  {
    min?: Expression;
    max?: Expression;
    this?: Expression;
  },
]>;

export class DictRangeExpr extends PropertyExpr {
  static key = ExpressionKey.DICT_RANGE;

  static requiredArgs = new Set([
    'this',
    'min',
    'max',
  ]);

  static availableArgs = new Set([
    'this',
    'min',
    'max',
  ]);

  declare args: DictRangeExprArgs;

  constructor (args: DictRangeExprArgs = {}) {
    super(args);
  }
}

export type DynamicPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class DynamicPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.DYNAMIC_PROPERTY;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: DynamicPropertyExprArgs;

  constructor (args: DynamicPropertyExprArgs = {}) {
    super(args);
  }
}

export type OnClusterExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class OnClusterExpr extends PropertyExpr {
  static key = ExpressionKey.ON_CLUSTER;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: OnClusterExprArgs;

  constructor (args: OnClusterExprArgs = {}) {
    super(args);
  }
}

export type EmptyPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class EmptyPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.EMPTY_PROPERTY;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: EmptyPropertyExprArgs;

  constructor (args: EmptyPropertyExprArgs = {}) {
    super(args);
  }
}

export type LikePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class LikePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.LIKE_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this', 'expressions']);

  declare args: LikePropertyExprArgs;

  constructor (args: LikePropertyExprArgs = {}) {
    super(args);
  }
}

export type LocationPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class LocationPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.LOCATION_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: LocationPropertyExprArgs;

  constructor (args: LocationPropertyExprArgs = {}) {
    super(args);
  }
}

export type LockPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class LockPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.LOCK_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: LockPropertyExprArgs;

  constructor (args: LockPropertyExprArgs = {}) {
    super(args);
  }
}

/**
 * Enumeration of valid kind values for LockingProperty expressions.
 * Used to specify the variant or subtype of the expression.
 */
export type LockingPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    kind?: LockingPropertyExprKind;
    forOrIn?: Expression;
    lockType?: Expression;
    override?: Expression;
    this?: Expression;
  },
]>;

export class LockingPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.LOCKING_PROPERTY;

  static requiredArgs = new Set(['kind', 'lockType']);

  static availableArgs = new Set([
    'this',
    'kind',
    'forOrIn',
    'lockType',
    'override',
  ]);

  declare args: LockingPropertyExprArgs;

  constructor (args: LockingPropertyExprArgs = {}) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }
}

export type LogPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { no?: Expression },
]>;

export class LogPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.LOG_PROPERTY;

  static requiredArgs = new Set(['no']);
  static availableArgs = new Set(['no']);

  declare args: LogPropertyExprArgs;

  constructor (args: LogPropertyExprArgs = {}) {
    super(args);
  }
}

export type MaterializedPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    this?: Expression;
  },
]>;

export class MaterializedPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.MATERIALIZED_PROPERTY;

  static availableArgs = new Set(['this']);

  declare args: MaterializedPropertyExprArgs;

  constructor (args: MaterializedPropertyExprArgs = {}) {
    super(args);
  }
}

export type MergeBlockRatioPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    no?: Expression;
    default?: Expression;
    percent?: Expression;
    this?: Expression;
  },
]>;

export class MergeBlockRatioPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.MERGE_BLOCK_RATIO_PROPERTY;

  static availableArgs = new Set([
    'this',
    'no',
    'default',
    'percent',
  ]);

  declare args: MergeBlockRatioPropertyExprArgs;

  constructor (args: MergeBlockRatioPropertyExprArgs = {}) {
    super(args);
  }

  get value (): string {
    return this.args.value as string;
  }
}

export type NoPrimaryIndexPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class NoPrimaryIndexPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.NO_PRIMARY_INDEX_PROPERTY;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: NoPrimaryIndexPropertyExprArgs;

  constructor (args: NoPrimaryIndexPropertyExprArgs = {}) {
    super(args);
  }
}

export type OnPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;
export class OnPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.ON_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: OnPropertyExprArgs;

  constructor (args: OnPropertyExprArgs = {}) {
    super(args);
  }
}

export type OnCommitPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { delete?: boolean },
]>;

export class OnCommitPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.ON_COMMIT_PROPERTY;

  static availableArgs = new Set(['delete']);

  declare args: OnCommitPropertyExprArgs;

  constructor (args: OnCommitPropertyExprArgs = {}) {
    super(args);
  }
}

export type PartitionedByPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class PartitionedByPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.PARTITIONED_BY_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: PartitionedByPropertyExprArgs;

  constructor (args: PartitionedByPropertyExprArgs = {}) {
    super(args);
  }
}

export type PartitionedByBucketExprArgs = Merge<[
  PropertyExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class PartitionedByBucketExpr extends PropertyExpr {
  static key = ExpressionKey.PARTITIONED_BY_BUCKET;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: PartitionedByBucketExprArgs;

  constructor (args: PartitionedByBucketExprArgs = {}) {
    super(args);
  }
}

export type PartitionByTruncateExprArgs = Merge<[
  PropertyExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class PartitionByTruncateExpr extends PropertyExpr {
  static key = ExpressionKey.PARTITION_BY_TRUNCATE;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: PartitionByTruncateExprArgs;

  constructor (args: PartitionByTruncateExprArgs = {}) {
    super(args);
  }
}

export type PartitionByRangePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    partitionExpressions?: Expression[];
    createExpressions?: Expression[];
  },
]>;

export class PartitionByRangePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.PARTITION_BY_RANGE_PROPERTY;

  static requiredArgs = new Set(['partitionExpressions', 'createExpressions']);

  static availableArgs = new Set(['partitionExpressions', 'createExpressions']);

  declare args: PartitionByRangePropertyExprArgs;

  constructor (args: PartitionByRangePropertyExprArgs = {}) {
    super(args);
  }
}

export type RollupPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { expressions?: Expression[] },
]>;

export class RollupPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.ROLLUP_PROPERTY;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: RollupPropertyExprArgs;

  constructor (args: RollupPropertyExprArgs = {}) {
    super(args);
  }
}

export type PartitionByListPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    partitionExpressions?: Expression[];
    createExpressions?: Expression[];
  },
]>;

export class PartitionByListPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.PARTITION_BY_LIST_PROPERTY;

  static requiredArgs = new Set(['partitionExpressions', 'createExpressions']);

  static availableArgs = new Set(['partitionExpressions', 'createExpressions']);

  declare args: PartitionByListPropertyExprArgs;

  constructor (args: PartitionByListPropertyExprArgs = {}) {
    super(args);
  }
}

/**
 * Valid kind values for refresh trigger properties
 */
export type RefreshTriggerPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    method?: string;
    kind?: RefreshTriggerPropertyExprKind;
    every?: Expression;
    unit?: Expression;
    starts?: Expression[];
  },
]>;

export class RefreshTriggerPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.REFRESH_TRIGGER_PROPERTY;

  static availableArgs = new Set([
    'method',
    'kind',
    'every',
    'unit',
    'starts',
  ]);

  declare args: RefreshTriggerPropertyExprArgs;

  constructor (args: RefreshTriggerPropertyExprArgs = {}) {
    super(args);
  }
}

export type UniqueKeyPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { expressions?: Expression[] },
]>;

export class UniqueKeyPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.UNIQUE_KEY_PROPERTY;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: UniqueKeyPropertyExprArgs;

  constructor (args: UniqueKeyPropertyExprArgs = {}) {
    super(args);
  }
}

export type PartitionedOfPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class PartitionedOfPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.PARTITIONED_OF_PROPERTY;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: PartitionedOfPropertyExprArgs;

  constructor (args: PartitionedOfPropertyExprArgs = {}) {
    super(args);
  }
}

export type StreamingTablePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class StreamingTablePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.STREAMING_TABLE_PROPERTY;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: StreamingTablePropertyExprArgs;

  constructor (args: StreamingTablePropertyExprArgs = {}) {
    super(args);
  }
}

export type RemoteWithConnectionModelPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    this?: Expression;
  },
]>;

export class RemoteWithConnectionModelPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.REMOTE_WITH_CONNECTION_MODEL_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: RemoteWithConnectionModelPropertyExprArgs;

  constructor (args: RemoteWithConnectionModelPropertyExprArgs = {}) {
    super(args);
  }
}

export type ReturnsPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    this?: Expression;
    isTable?: Expression;
    table?: Expression;
    null?: Expression;
  },
]>;

export class ReturnsPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.RETURNS_PROPERTY;

  static availableArgs = new Set([
    'this',
    'isTable',
    'table',
    'null',
  ]);

  declare args: ReturnsPropertyExprArgs;

  constructor (args: ReturnsPropertyExprArgs = {}) {
    super(args);
  }
}

export type StrictPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class StrictPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.STRICT_PROPERTY;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: StrictPropertyExprArgs;

  constructor (args: StrictPropertyExprArgs = {}) {
    super(args);
  }
}

export type RowFormatPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class RowFormatPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.ROW_FORMAT_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: RowFormatPropertyExprArgs;

  constructor (args: RowFormatPropertyExprArgs = {}) {
    super(args);
  }
}

export type RowFormatDelimitedPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    fields?: Expression[];
    escaped?: Expression;
    collectionItems?: Expression[];
    mapKeys?: Expression[];
    lines?: Expression[];
    null?: Expression;
    serde?: Expression;
  },
]>;

export class RowFormatDelimitedPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.ROW_FORMAT_DELIMITED_PROPERTY;

  static availableArgs = new Set([
    'fields',
    'escaped',
    'collectionItems',
    'mapKeys',
    'lines',
    'null',
    'serde',
  ]);

  declare args: RowFormatDelimitedPropertyExprArgs;

  constructor (args: RowFormatDelimitedPropertyExprArgs = {}) {
    super(args);
  }
}

export type RowFormatSerdePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    this?: Expression;
    serdeProperties?: Expression[];
  },
]>;

export class RowFormatSerdePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.ROW_FORMAT_SERDE_PROPERTY;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set(['this', 'serdeProperties']);

  declare args: RowFormatSerdePropertyExprArgs;

  constructor (args: RowFormatSerdePropertyExprArgs = {}) {
    super(args);
  }
}

export type SamplePropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class SamplePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.SAMPLE_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: SamplePropertyExprArgs;

  constructor (args: SamplePropertyExprArgs = {}) {
    super(args);
  }
}

export type SecurityPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class SecurityPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.SECURITY_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: SecurityPropertyExprArgs;

  constructor (args: SecurityPropertyExprArgs = {}) {
    super(args);
  }
}

export type SchemaCommentPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class SchemaCommentPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.SCHEMA_COMMENT_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: SchemaCommentPropertyExprArgs;

  constructor (args: SchemaCommentPropertyExprArgs = {}) {
    super(args);
  }
}

export type SerdePropertiesExprArgs = Merge<[
  PropertyExprArgs,
  {
    expressions?: Expression[];
    with?: Expression;
  },
]>;

export class SerdePropertiesExpr extends PropertyExpr {
  static key = ExpressionKey.SERDE_PROPERTIES;

  static requiredArgs = new Set(['expressions']);
  static availableArgs = new Set(['expressions', 'with']);

  declare args: SerdePropertiesExprArgs;

  constructor (args: SerdePropertiesExprArgs = {}) {
    super(args);
  }
}

export type SetPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { multi?: Expression },
]>;

export class SetPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.SET_PROPERTY;

  static requiredArgs = new Set(['multi']);

  static availableArgs = new Set(['multi']);

  declare args: SetPropertyExprArgs;

  constructor (args: SetPropertyExprArgs = {}) {
    super(args);
  }
}

export type SharingPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    this?: Expression;
  },
]>;

export class SharingPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.SHARING_PROPERTY;

  static availableArgs = new Set(['this']);

  declare args: SharingPropertyExprArgs;

  constructor (args: SetPropertyExprArgs = {}) {
    super(args);
  }
}

export type SetConfigPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class SetConfigPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.SET_CONFIG_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: SetConfigPropertyExprArgs;

  constructor (args: SetConfigPropertyExprArgs = {}) {
    super(args);
  }
}

export type SettingsPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { expressions?: Expression[] },
]>;

export class SettingsPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.SETTINGS_PROPERTY;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: SettingsPropertyExprArgs;

  constructor (args: SettingsPropertyExprArgs = {}) {
    super(args);
  }
}

export type SortKeyPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    this?: Expression;
    compound?: Expression;
  },
]>;

export class SortKeyPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.SORT_KEY_PROPERTY;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set(['this', 'compound']);

  declare args: SortKeyPropertyExprArgs;

  constructor (args: SortKeyPropertyExprArgs = {}) {
    super(args);
  }
}

export type SqlReadWritePropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class SqlReadWritePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.SQL_READ_WRITE_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: SqlReadWritePropertyExprArgs;

  constructor (args: SqlReadWritePropertyExprArgs = {}) {
    super(args);
  }
}

export type SqlSecurityPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class SqlSecurityPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.SQL_SECURITY_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: SqlSecurityPropertyExprArgs;

  constructor (args: SqlSecurityPropertyExprArgs = {}) {
    super(args);
  }
}

export type StabilityPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class StabilityPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.STABILITY_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: StabilityPropertyExprArgs;

  constructor (args: StabilityPropertyExprArgs = {}) {
    super(args);
  }
}

export type StorageHandlerPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class StorageHandlerPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.STORAGE_HANDLER_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: StorageHandlerPropertyExprArgs;

  constructor (args: StorageHandlerPropertyExprArgs = {}) {
    super(args);
  }
}

export type TemporaryPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    this?: Expression;
  },
]>;

export class TemporaryPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.TEMPORARY_PROPERTY;

  static availableArgs = new Set(['this']);

  declare args: TemporaryPropertyExprArgs;

  constructor (args: TemporaryPropertyExprArgs = {}) {
    super(args);
  }
}

export type SecurePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class SecurePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.SECURE_PROPERTY;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: SecurePropertyExprArgs;

  constructor (args: SecurePropertyExprArgs = {}) {
    super(args);
  }
}

export type TagsExprArgs = Merge<[
  PropertyExprArgs,
  ColumnConstraintKindExprArgs,
  {
    expressions?: Expression[];
    this?: Expression;
    value?: string | Expression;
  },
]>;

export class TagsExpr extends multiInherit(ColumnConstraintKindExpr, PropertyExpr) {
  static key = ExpressionKey.TAGS;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: TagsExprArgs;

  constructor (args: TagsExprArgs = {}) {
    super(args);
  }
}

export type TransformModelPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { expressions?: Expression[] },
]>;

export class TransformModelPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.TRANSFORM_MODEL_PROPERTY;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: TransformModelPropertyExprArgs;

  constructor (args: TransformModelPropertyExprArgs = {}) {
    super(args);
  }
}

export type TransientPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    this?: Expression;
  },
]>;

export class TransientPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.TRANSIENT_PROPERTY;

  static availableArgs = new Set(['this']);

  declare args: TransientPropertyExprArgs;

  constructor (args: TransientPropertyExprArgs = {}) {
    super(args);
  }
}

export type UnloggedPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class UnloggedPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.UNLOGGED_PROPERTY;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: UnloggedPropertyExprArgs;

  constructor (args: UnloggedPropertyExprArgs = {}) {
    super(args);
  }
}

export type UsingTemplatePropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class UsingTemplatePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.USING_TEMPLATE_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: UsingTemplatePropertyExprArgs;

  constructor (args: UsingTemplatePropertyExprArgs = {}) {
    super(args);
  }
}

export type ViewAttributePropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class ViewAttributePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.VIEW_ATTRIBUTE_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: ViewAttributePropertyExprArgs;

  constructor (args: ViewAttributePropertyExprArgs = {}) {
    super(args);
  }
}

export type VolatilePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    this?: Expression;
  },
]>;

export class VolatilePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.VOLATILE_PROPERTY;

  static availableArgs = new Set(['this']);

  declare args: VolatilePropertyExprArgs;

  constructor (args: VolatilePropertyExprArgs = {}) {
    super(args);
  }
}

export type WithDataPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    no?: Expression;
    statistics?: Expression[];
  },
]>;

export class WithDataPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.WITH_DATA_PROPERTY;

  static requiredArgs = new Set(['no']);
  static availableArgs = new Set(['no', 'statistics']);

  declare args: WithDataPropertyExprArgs;

  constructor (args: WithDataPropertyExprArgs = {}) {
    super(args);
  }
}

export type WithJournalTablePropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class WithJournalTablePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.WITH_JOURNAL_TABLE_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: WithJournalTablePropertyExprArgs;

  constructor (args: WithJournalTablePropertyExprArgs = {}) {
    super(args);
  }
}

export type WithSchemaBindingPropertyExprArgs = Merge<[
  PropertyExprArgs,
  { this?: Expression },
]>;

export class WithSchemaBindingPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.WITH_SCHEMA_BINDING_PROPERTY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: WithSchemaBindingPropertyExprArgs;

  constructor (args: WithSchemaBindingPropertyExprArgs = {}) {
    super(args);
  }
}

export type WithSystemVersioningPropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    on?: Expression;
    dataConsistency?: Expression;
    retentionPeriod?: Expression;
    with?: Expression;
    this?: Expression;
  },
]>;

export class WithSystemVersioningPropertyExpr extends PropertyExpr {
  static key = ExpressionKey.WITH_SYSTEM_VERSIONING_PROPERTY;

  static requiredArgs = new Set(['with']);
  static availableArgs = new Set([
    'on',
    'this',
    'dataConsistency',
    'retentionPeriod',
    'with',
  ]);

  declare args: WithSystemVersioningPropertyExprArgs;

  constructor (args: WithSystemVersioningPropertyExprArgs = {}) {
    super(args);
  }
}

export type WithProcedureOptionsExprArgs = Merge<[
  PropertyExprArgs,
  { expressions?: Expression[] },
]>;

export class WithProcedureOptionsExpr extends PropertyExpr {
  static key = ExpressionKey.WITH_PROCEDURE_OPTIONS;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: WithProcedureOptionsExprArgs;

  constructor (args: WithProcedureOptionsExprArgs = {}) {
    super(args);
  }
}

export type EncodePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    this?: Expression;
    properties?: Expression;
    key?: Expression;
  },
]>;

export class EncodePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.ENCODE_PROPERTY;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'properties',
    'key',
  ]);

  declare args: EncodePropertyExprArgs;

  constructor (args: EncodePropertyExprArgs = {}) {
    super(args);
  }
}

export type IncludePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
    this?: Expression;
    alias?: ExpressionOrString;
    columnDef?: Expression;
  },
]>;

export class IncludePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.INCLUDE_PROPERTY;

  static requiredArgs = new Set(['this']);
  static availableArgs = new Set([
    'this',
    'alias',
    'columnDef',
  ]);

  declare args: IncludePropertyExprArgs;

  constructor (args: IncludePropertyExprArgs = {}) {
    super(args);
  }
}

export type ForcePropertyExprArgs = Merge<[
  PropertyExprArgs,
  {
    value?: string;
  },
]>;

export class ForcePropertyExpr extends PropertyExpr {
  static key = ExpressionKey.FORCE_PROPERTY;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: ForcePropertyExprArgs;

  constructor (args: ForcePropertyExprArgs = {}) {
    super(args);
  }
}

/**
 * Enumeration of CREATE property locations
 * Form: schema specified
 *   create [POST_CREATE]
 *     table a [POST_NAME]
 *     (b int) [POST_SCHEMA]
 *     with ([POST_WITH])
 *     index (b) [POST_INDEX]
 *
 * Form: alias selection
 *   create [POST_CREATE]
 *     table a [POST_NAME]
 *     as [POST_ALIAS] (select * from b) [POST_EXPRESSION]
 *     index (c) [POST_INDEX]
 */
export type PropertiesExprArgs = Merge<[
  BaseExpressionArgs,
  { expressions?: Expression[] },
]>;

export class PropertiesExpr extends Expression {
  static key = ExpressionKey.PROPERTIES;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: PropertiesExprArgs;

  constructor (args: PropertiesExprArgs = {}) {
    super(args);
  }

  static NAME_TO_PROPERTY = {
    'ALGORITHM': AlgorithmPropertyExpr,
    'AUTO_INCREMENT': AutoIncrementPropertyExpr,
    'CHARACTER SET': CharacterSetPropertyExpr,
    'CLUSTERED_BY': ClusteredByPropertyExpr,
    'COLLATE': CollatePropertyExpr,
    'COMMENT': SchemaCommentPropertyExpr,
    'CREDENTIALS': CredentialsPropertyExpr,
    'DEFINER': DefinerPropertyExpr,
    'DISTKEY': DistKeyPropertyExpr,
    'DISTRIBUTED_BY': DistributedByPropertyExpr,
    'DISTSTYLE': DistStylePropertyExpr,
    'ENGINE': EnginePropertyExpr,
    'EXECUTE AS': ExecuteAsPropertyExpr,
    'FORMAT': FileFormatPropertyExpr,
    'LANGUAGE': LanguagePropertyExpr,
    'LOCATION': LocationPropertyExpr,
    'LOCK': LockPropertyExpr,
    'PARTITIONED_BY': PartitionedByPropertyExpr,
    'RETURNS': ReturnsPropertyExpr,
    'ROW_FORMAT': RowFormatPropertyExpr,
    'SORTKEY': SortKeyPropertyExpr,
    'ENCODE': EncodePropertyExpr,
    'INCLUDE': IncludePropertyExpr,
  } as const;

  static PROPERTY_TO_NAME: Record<string, string> = Object.fromEntries(
    Object.entries(PropertiesExpr.NAME_TO_PROPERTY).map(([k, v]) => [v.key, k]),
  );

  /**
   * Creates a Properties expression from a dictionary of property key-value pairs.
   *
   * @param propertiesDict - Dictionary mapping property names to their values
   * @returns A Properties expression containing the property expressions
   */
  static fromDict (propertiesDict: Record<string, unknown>): PropertiesExpr {
    const expressions: Expression[] = [];

    for (const [key, value] of Object.entries(propertiesDict)) {
      const propertyClass = PropertiesExpr.NAME_TO_PROPERTY[key.toUpperCase() as keyof typeof PropertiesExpr.NAME_TO_PROPERTY];
      if (propertyClass) {
        // @ts-expect-error "sqlglot is intrinsically type-unsafe here"
        expressions.push(new propertyClass({ this: convert(value) }));
      } else {
        expressions.push(new PropertyExpr({
          this: LiteralExpr.string(key),
          value: convert(value),
        }));
      }
    }

    return new PropertiesExpr({ expressions });
  }
}

/**
 * Enumeration of valid kind values for SetOperation expressions.
 * Used to specify the variant or subtype of the expression.
 */
export type SetOperationExprArgs = Merge<[
  QueryExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    distinct?: boolean;
    byName?: string;
    side?: string;
    kind?: SetOperationExprKind;
    on?: Expression[];
    match?: Expression;
    laterals?: Expression[];
    joins?: Expression[];
    connect?: Expression;
    pivots?: Expression[];
    prewhere?: Expression;
    where?: Expression;
    group?: Expression;
    having?: Expression;
    qualify?: Expression;
    windows?: Expression[];
    distribute?: Expression;
    sort?: Expression;
    cluster?: Expression;
    order?: Expression;
    limit?: ExpressionOrNumber;
    offset?: ExpressionOrNumber;
    locks?: Expression[];
    sample?: ExpressionOrNumber;
  },
]>;

export class SetOperationExpr extends QueryExpr {
  static key = ExpressionKey.SET_OPERATION;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'with',
    'this',
    'expression',
    'distinct',
    'byName',
    'side',
    'kind',
    'on',
    'match',
    'laterals',
    'joins',
    'connect',
    'pivots',
    'prewhere',
    'where',
    'group',
    'having',
    'qualify',
    'windows',
    'distribute',
    'sort',
    'cluster',
    'order',
    'limit',
    'offset',
    'locks',
    'sample',
    'settings',
    'format',
    'options',
  ]);

  declare args: SetOperationExprArgs;

  constructor (args: SetOperationExprArgs = {}) {
    super(args);
  }

  /**
   * Applies select expressions to both sides of the set operation.
   */
  select (
    expressions?: ExpressionValue | (ExpressionValue | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const {
      copy = true, ...restOptions
    } = options;
    const expressionList = Array.from(ensureIterable(expressions)) as (string | Expression | undefined)[];
    const self = maybeCopy(this, copy);
    const leftSide = self.args.this;
    assertIsInstanceOf(leftSide, QueryExpr);
    narrowInstanceOf(leftSide.unnest(), QueryExpr)?.select(expressionList, {
      ...restOptions,
      copy: false,
    });
    const rightSide = self.args.expression;
    assertIsInstanceOf(rightSide, QueryExpr);
    narrowInstanceOf(rightSide.unnest(), QueryExpr)?.select(expressionList, {
      ...restOptions,
      copy: false,
      append: options?.append ?? true,
    });

    return self;
  }

  /**
   * Returns named selects from the underlying query.
   * Walks up the SetOperation chain to find the base query.
   */
  get namedSelects (): string[] {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let expression: QueryExpr = this;
    while (expression instanceof SetOperationExpr) {
      const next = expression.args.this?.unnest();
      assertIsInstanceOf(next, QueryExpr);
      expression = next;
    }
    return expression.namedSelects;
  }

  /**
   * Returns true if either side of the set operation is a star select.
   */
  get isStar (): boolean {
    const leftIsStar = this.args.this?.isStar;
    const rightIsStar = this.args.expression?.isStar;
    return leftIsStar || rightIsStar || false;
  }

  /**
   * Returns selects from the underlying query.
   * Walks up the SetOperation chain to find the base query.
   */
  get selects (): Expression[] {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let expression: QueryExpr = this;
    while (expression instanceof SetOperationExpr) {
      const next = expression.args.this?.unnest();
      assertIsInstanceOf(next, QueryExpr);
      expression = next;
    }
    return expression.selects;
  }

  /**
   * Returns the left side of the set operation.
   */
  get left (): Expression | undefined {
    return this.args.this;
  }

  /**
   * Returns the right side of the set operation.
   */
  get right (): Expression | undefined {
    return this.args.expression;
  }

  /**
   * Returns the kind of set operation as uppercase string.
   */
  get kind (): SetOperationExprKind | undefined {
    return this.args.kind;
  }

  /**
   * Returns the side as uppercase string.
   */
  get side (): string {
    return this.text('side').toUpperCase();
  }
}

export type UpdateExprArgs = Merge<[
  DmlExprArgs,
  {
    with?: Expression;
    this?: Expression;
    expressions?: Expression[];
    from?: Expression;
    where?: Expression;
    returning?: Expression;
    order?: Expression;
    limit?: number | Expression;
    options?: Expression[];
  },
]>;

export class UpdateExpr extends DmlExpr {
  static key = ExpressionKey.UPDATE;

  static availableArgs = new Set([
    'with',
    'this',
    'expressions',
    'from',
    'where',
    'returning',
    'order',
    'limit',
    'options',
  ]);

  declare args: UpdateExprArgs;

  constructor (args: UpdateExprArgs = {}) {
    super(args);
  }

  /**
   * Set the table to update.
   *
   * @example
   * update().table("my_table").set("x = 1").sql()
   * // 'UPDATE my_table SET x = 1'
   *
   * @param expression - The SQL code string to parse or Expression instance
   * @param options - Options for parsing and copying
   * @returns The modified Update expression
   */
  table (
    expression?: ExpressionOrString,
    options: {
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const {
      dialect, copy = true,
    } = options;
    return applyBuilder(expression, {
      instance: this,
      arg: 'this',
      into: TableExpr,
      prefix: undefined,
      dialect,
      copy,
    });
  }

  /**
   * Append to or set the SET expressions.
   *
   * @example
   * update().table("my_table").setExpressions("x = 1").sql()
   * // 'UPDATE my_table SET x = 1'
   *
   * @param expressions - The SQL code strings to parse or Expression instances
   * @param options - Options for parsing, appending, and copying
   * @returns The modified Update expression
   */
  set (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const {
      append = true, dialect, copy = true,
    } = options;
    const expressionList = Array.from(ensureIterable(expressions)) as (string | Expression | undefined)[];
    return applyListBuilder(expressionList, {
      instance: this,
      arg: 'expressions',
      append,
      into: Expression,
      prefix: undefined,
      dialect,
      copy,
    });
  }

  /**
   * Append to or set the WHERE expressions.
   *
   * @example
   * update().table("tbl").set("x = 1").where("x = 'a' OR x < 'b'").sql()
   * // "UPDATE tbl SET x = 1 WHERE x = 'a' OR x < 'b'"
   *
   * @param expressions - The SQL code strings to parse or Expression instances
   * @param options - Options for parsing, appending, and copying
   * @returns The modified Update expression
   */
  where (
    expressions?: ExpressionValue | (ExpressionValue | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this | undefined {
    const {
      append = true, dialect, copy = true,
    } = options;
    return applyConjunctionBuilder(expressions, {
      instance: this,
      arg: 'where',
      append,
      into: WhereExpr,
      dialect,
      copy,
    });
  }

  /**
   * Set the FROM expression.
   *
   * @example
   * update().table("my_table").set("x = 1").from("baz").sql()
   * // 'UPDATE my_table SET x = 1 FROM baz'
   *
   * @param expression - The SQL code string to parse or Expression instance
   * @param options - Options for parsing and copying
   * @returns The modified Update expression
   */
  from (
    expression?: string | Expression,
    options: {
      dialect?: DialectType;
      copy?: boolean;
    } = {},
  ): this {
    const {
      dialect, copy = true,
    } = options;
    if (!expression) {
      return this;
    }

    return applyBuilder(expression, {
      instance: this,
      arg: 'from',
      into: FromExpr,
      prefix: undefined,
      dialect,
      copy,
    });
  }

  /**
   * Append to or set the common table expressions.
   *
   * @example
   * update().table("my_table").set(["x = 1"]).from("baz").with("baz", "SELECT id FROM foo").sql()
   * // 'WITH baz AS (SELECT id FROM foo) UPDATE my_table SET x = 1 FROM baz'
   *
   * @param alias - The SQL code string to parse as the table name
   * @param as - The SQL code string to parse as the table expression
   * @param options - Options object
   * @returns The modified Update expression
   */
  with (
    alias: string | IdentifierExpr,
    as: string | QueryExpr,
    options: {
      recursive?: boolean;
      materialized?: boolean;
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      scalar?: boolean;
    } = {},
  ): this {
    const {
      recursive = false, append = true, copy = true, ...restOptions
    } = options;
    return applyCteBuilder({
      instance: this,
      alias,
      as,
      ...restOptions,
      recursive,
      append,
      copy,
    });
  }
}

/**
 * Enumeration of valid kind values for SELECT expressions.
 * Used to specify the variant or subtype of the expression.
 */
/**
 * Represents a SELECT statement.
 *
 * @example
 * // SELECT col1, col2 FROM table WHERE id > 10
 * const select = new SelectExpr({
 *   expressions: [col1, col2],
 *   from: fromExpr,
 *   where: whereCondition
 * });
 */
export type SelectExprArgs = Merge<[
  QueryExprArgs,
  {
    with?: Expression;
    kind?: SelectExprKind;
    expressions?: Expression[];
    hint?: Expression;
    distinct?: Expression;
    into?: Expression;
    from?: Expression;
    operationModifiers?: Expression[];
    match?: Expression;
    laterals?: Expression[];
    joins?: Expression[];
    connect?: Expression;
    pivots?: Expression[];
    prewhere?: Expression;
    where?: Expression;
    group?: Expression;
    having?: Expression;
    qualify?: Expression;
    windows?: Expression[];
    distribute?: Expression;
    sort?: Expression;
    cluster?: Expression;
    order?: Expression;
    limit?: number | Expression;
    offset?: number | Expression;
    locks?: Expression[];
    sample?: number | Expression;
  },
]>;

export class SelectExpr extends QueryExpr {
  static key = ExpressionKey.SELECT;

  static availableArgs = new Set([
    'with',
    'kind',
    'expressions',
    'hint',
    'distinct',
    'into',
    'from',
    'operationModifiers',
    'exclude',
    'match',
    'laterals',
    'joins',
    'connect',
    'pivots',
    'prewhere',
    'where',
    'group',
    'having',
    'qualify',
    'windows',
    'distribute',
    'sort',
    'cluster',
    'order',
    'limit',
    'offset',
    'locks',
    'sample',
    'settings',
    'format',
    'options',
  ]);

  declare args: SelectExprArgs;

  constructor (args: SelectExprArgs = {}) {
    super(args);
  }

  /**
   * Returns a list of output names from the select expressions.
   */
  get namedSelects (): string[] {
    const selects: string[] = [];

    for (const e of (this.args.expressions || [])) {
      if (e.aliasOrName) {
        selects.push(e.outputName);
      } else if (e instanceof AliasesExpr) {
        const aliases = e.args.expressions || [];
        selects.push(...aliases.map((a) => a instanceof Expression ? a.name : '').filter((n) => n));
      }
    }

    return selects;
  }

  /**
   * Returns true if any expression is a star expression.
   */
  get isStar (): boolean {
    return this.args.expressions?.some((expression) => typeof expression === 'object' && 'isStar' in expression && expression.isStar) ?? false;
  }

  /**
   * Returns the SELECT expressions.
   */
  get selects (): Expression[] {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return this.args.expressions || [] as any;
  }

  /**
   * Set the FROM expression.
   *
   * @example
   * select().from("tbl").select(["x"]).sql()
   * // 'SELECT x FROM tbl'
   */
  from (
    expression?: ExpressionOrString,
    options: {
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      dialect, copy = true, ...restOptions
    } = options;
    return applyBuilder(expression, {
      ...restOptions,
      instance: this,
      arg: 'from',
      into: FromExpr,
      prefix: 'FROM',
      dialect,
      copy,
    });
  }

  /**
   * Set the GROUP BY expression.
   *
   * @example
   * select().from("tbl").select(["x", "COUNT(1)"]).groupBy(["x"]).sql()
   * // 'SELECT x, COUNT(1) FROM tbl GROUP BY x'
   */
  groupBy (
    expressions?: ExpressionOrString | (ExpressionOrString | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      append = true, dialect, copy = true, ...restOptions
    } = options;
    const expressionList = Array.from(ensureIterable(expressions)) as (string | Expression | undefined)[];
    if (expressionList.length === 0) {
      return copy ? (this.copy() as this) : this;
    }

    return applyChildListBuilder(expressionList, {
      ...restOptions,
      instance: this,
      arg: 'group',
      append,
      copy,
      prefix: 'GROUP BY',
      into: GroupExpr,
      dialect,
    });
  }

  /**
   * Set the SORT BY expression.
   *
   * @example
   * select().from("tbl").select(["x"]).sortBy(["x DESC"]).sql({ dialect: "hive" })
   * // 'SELECT x FROM tbl SORT BY x DESC'
   */
  sortBy (
    expressions?: ExpressionOrString | (ExpressionOrString | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      append = true, dialect, copy = true, ...restOptions
    } = options;
    return applyChildListBuilder(expressions, {
      ...restOptions,
      instance: this,
      arg: 'sort',
      append,
      copy,
      prefix: 'SORT BY',
      into: SortExpr,
      dialect,
    });
  }

  /**
   * Set the CLUSTER BY expression.
   *
   * @example
   * select().from("tbl").select(["x"]).clusterBy(["x"]).sql({ dialect: "hive" })
   * // 'SELECT x FROM tbl CLUSTER BY x'
   */
  clusterBy (
    expressions?: ExpressionOrString | (ExpressionOrString | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      append = true, dialect, copy = true, ...restOptions
    } = options;
    return applyChildListBuilder(expressions, {
      ...restOptions,
      instance: this,
      arg: 'cluster',
      append,
      copy,
      prefix: 'CLUSTER BY',
      into: ClusterExpr,
      dialect,
    });
  }

  /**
   * Append to or set the HAVING expressions.
   *
   * @example
   * Select().select("x", "COUNT(y)").from("tbl").groupBy("x").having("COUNT(y) > 3").sql()
   * // 'SELECT x, COUNT(y) FROM tbl GROUP BY x HAVING COUNT(y) > 3'
   *
   * @param expressions - The SQL code strings to parse or Expression instances.
   * Multiple expressions are combined with an AND operator.
   * @param options - Optional configuration for the builder.
   * @returns The modified Select expression.
   */
  having (
    expressions: ExpressionOrString | (ExpressionOrString | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): SelectExpr {
    const {
      append = true,
      dialect = undefined,
      copy = true,
      ...opts
    } = options;

    return applyConjunctionBuilder(expressions, {
      instance: this,
      arg: 'having',
      append,
      into: HavingExpr,
      dialect,
      copy,
      ...opts,
    })!;
  }

  /**
   * Set the ORDER BY expression.
   *
   * @example
   * Select().from("tbl").select("x").orderBy("x DESC").sql()
   * // 'SELECT x FROM tbl ORDER BY x DESC'
   *
   * @param expressions - The SQL code strings to parse.
   * If an Expression instance is passed, it will be wrapped in an Order expression.
   * @param options - Optional configuration for the builder.
   * @returns The modified Select expression.
   */
  orderBy (
    expressions?: ExpressionValue | (ExpressionValue | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      append = true,
      dialect = undefined,
      copy = true,
      ...opts
    } = options;

    return applyChildListBuilder(expressions, {
      instance: this,
      arg: 'order',
      append,
      copy,
      prefix: 'ORDER BY',
      into: OrderExpr,
      dialect,
      ...opts,
    });
  }

  /**
   * Append to or set the SELECT expressions.
   *
   * @example
   * select().select(["x", "y"]).from("tbl").sql()
   * // 'SELECT x, y FROM tbl'
   */
  select (
    expressions?: ExpressionValue | (ExpressionValue | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      append = true, dialect, copy = true, ...restOptions
    } = options;
    return applyListBuilder(expressions, {
      ...restOptions,
      instance: this,
      arg: 'expressions',
      append,
      dialect,
      into: Expression,
      copy,
    });
  }

  /**
   * Append to or set the LATERAL expressions.
   *
   * @example
   * select().select(["x"]).lateral(["OUTER explode(y) tbl2 AS z"]).from("tbl").sql()
   * // 'SELECT x FROM tbl LATERAL VIEW OUTER EXPLODE(y) tbl2 AS z'
   */
  lateral (
    expressions?: ExpressionOrString | (ExpressionOrString | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      append = true, dialect, copy = true, ...restOptions
    } = options;
    return applyListBuilder(expressions, {
      ...restOptions,
      instance: this,
      arg: 'laterals',
      append,
      into: LateralExpr,
      prefix: 'LATERAL VIEW',
      dialect,
      copy,
    });
  }

  /**
   * Append to or set the JOIN expressions.
   *
   * @example
   * select().select(["*"]).from("tbl").join("tbl2", { on: "tbl1.y = tbl2.y" }).sql()
   * // 'SELECT * FROM tbl JOIN tbl2 ON tbl1.y = tbl2.y'
   *
   * @example
   * select().select(["1"]).from("a").join("b", { using: ["x", "y", "z"] }).sql()
   * // 'SELECT 1 FROM a JOIN b USING (x, y, z)'
   *
   * @example
   * select().select(["*"]).from("tbl").join("tbl2", { on: "tbl1.y = tbl2.y", joinType: "left outer" }).sql()
   * // 'SELECT * FROM tbl LEFT OUTER JOIN tbl2 ON tbl1.y = tbl2.y'
   */
  join (
    expression?: ExpressionOrString,
    options: {
      on?: ExpressionOrString | ExpressionOrStringList;
      using?: ExpressionOrString | ExpressionOrStringList;
      append?: boolean;
      joinType?: JoinExprKind;
      joinAlias?: ExpressionOrString<IdentifierExpr>;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      on, using: usingOpt, append = true, joinType, joinAlias, dialect, copy = true, ..._restOptions
    } = options;
    const parseArgs = {
      dialect,
    };

    let expr: Expression;
    try {
      expr = maybeParse(expression, {
        ...parseArgs,
        into: JoinExpr,
        prefix: 'JOIN',
      });
    } catch {
      expr = maybeParse(expression, {
        ...parseArgs,
      });
    }

    let join = expr instanceof JoinExpr ? expr : new JoinExpr({ this: expr });

    // If joining a Select, wrap it in a subquery
    if (join.args.this instanceof SelectExpr) {
      join.args.this?.replace(join.args.this?.subquery());
    }

    // Set join type (method, side, kind)
    if (joinType) {
      const [
        method,
        side,
        kind,
      ] = maybeParse(joinType, {
        ...parseArgs,
        into: 'JOIN_TYPE', // FIXME: What is this in sqlglot??
      });
      if (method instanceof Token) {
        join.setArgKey('method', method.text);
      }
      if (side instanceof Token) {
        join.setArgKey('side', side.text);
      }
      if (kind instanceof Token) {
        join.setArgKey('kind', kind.text);
      }
    }

    // Set ON condition
    if (on) {
      const onExpr = and(Array.from(ensureIterable<ExpressionValue>(on)), {
        dialect,
        copy,
      });
      join.setArgKey('on', onExpr);
    }

    // Set USING
    if (usingOpt) {
      join = applyListBuilder(Array.from(ensureIterable(usingOpt)) as (string | Expression | undefined)[], {
        instance: join,
        arg: 'using',
        append,
        copy,
        into: IdentifierExpr,
      }) as JoinExpr;
    }

    // Set join alias
    if (joinAlias) {
      join.setArgKey('this', alias(join.args.this, joinAlias, { table: true }));
    }

    return applyListBuilder([join], {
      instance: this,
      arg: 'joins',
      append,
      copy,
    });
  }

  /**
   * Append to or set the WINDOW expressions.
   *
   * @example
   * select().select(["x"]).from("tbl").window(["w AS (PARTITION BY x)"]).sql()
   * // 'SELECT x FROM tbl WINDOW w AS (PARTITION BY x)'
   */
  window (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      append = true, dialect, copy = true, ...restOptions
    } = options;
    return applyListBuilder(expressions, {
      ...restOptions,
      instance: this,
      arg: 'windows',
      append,
      into: WindowExpr,
      dialect,
      copy,
    });
  }

  /**
   * Append to or set the QUALIFY expressions.
   *
   * @example
   * select().select(["x"]).from("tbl").qualify(["ROW_NUMBER() OVER (PARTITION BY x) = 1"]).sql()
   * // 'SELECT x FROM tbl QUALIFY ROW_NUMBER() OVER (PARTITION BY x) = 1'
   */
  qualify (
    expressions?: ExpressionValue | (ExpressionValue | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this | undefined {
    const {
      append = true, dialect, copy = true, ...restOptions
    } = options;
    return applyConjunctionBuilder(expressions, {
      ...restOptions,
      instance: this,
      arg: 'qualify',
      append,
      into: QualifyExpr,
      dialect,
      copy,
    });
  }

  /**
   * Set the DISTINCT clause.
   *
   * @example
   * select().from("tbl").select(["x"]).distinct().sql()
   * // 'SELECT DISTINCT x FROM tbl'
   */
  distinct (
    ons?: (ExpressionValue | undefined)[],
    options: {
      distinct?: boolean;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      distinct: distinctValue = true, copy = true, ..._restOptions
    } = options;
    const instance = maybeCopy(this, copy);

    if (ons && 0 < ons.length) {
      const onExprs = ons.filter((on): on is string | Expression => on !== undefined)
        .map((on) => maybeParse(on, {
          copy,
          into: Expression,
        }));
      const tupleExpr = new TupleExpr({ expressions: onExprs });
      instance.setArgKey('distinct', distinctValue ? new DistinctExpr({ on: tupleExpr }) : undefined);
    } else {
      instance.setArgKey('distinct', distinctValue ? new DistinctExpr({}) : undefined);
    }

    return instance as this;
  }

  /**
   * Convert this expression to a CREATE TABLE AS statement.
   *
   * @example
   * select().select(["*"]).from("tbl").ctas("x").sql()
   * // 'CREATE TABLE x AS SELECT * FROM tbl'
   */
  ctas (
    table?: ExpressionOrString,
    options: {
      properties?: Record<string, unknown>;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): CreateExpr {
    const {
      properties, dialect, copy = true,
    } = options;
    const instance = maybeCopy(this, copy);
    const tableExpr = maybeParse(table, {
      dialect,
      into: TableExpr,
    });

    let propertiesExpr: PropertiesExpr | undefined;
    if (properties) {
      propertiesExpr = PropertiesExpr.fromDict(properties);
    }

    return new CreateExpr({
      this: tableExpr,
      kind: CreateExprKind.TABLE,
      expression: instance,
      properties: propertiesExpr,
    });
  }

  /**
   * Set the locking read mode for this expression.
   *
   * @example
   * select().select(["x"]).from("tbl").where(["x = 'a'"]).lock().sql({ dialect: "mysql" })
   * // "SELECT x FROM tbl WHERE x = 'a' FOR UPDATE"
   *
   * @example
   * select().select(["x"]).from("tbl").where(["x = 'a'"]).lock({ update: false }).sql({ dialect: "mysql" })
   * // "SELECT x FROM tbl WHERE x = 'a' FOR SHARE"
   */
  lock (
    options: {
      update?: boolean;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      update = true, copy = true,
    } = options;
    const inst = maybeCopy(this, copy);
    inst.setArgKey('locks', [
      new LockExpr({
        update: new LiteralExpr({
          this: String(Number(update)),
          isString: false,
        }),
      }),
    ]);
    return inst as this;
  }

  /**
   * Set hints for this expression.
   *
   * @example
   * select().select(["x"]).from("tbl").hint(["BROADCAST(y)"]).sql({ dialect: "spark" })
   * // 'SELECT /*+ BROADCAST(y) *\/ x FROM tbl'
   */
  hint (
    _hints?: ExpressionValue | ExpressionValue[],
    options: {
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      dialect, copy = true,
    } = options;
    const hints = _hints !== undefined ? Array.from(ensureIterable<ExpressionValue>(_hints)) : [];
    const hintExprs = hints.map((h) =>
      maybeParse(h, {
        dialect,
        into: Expression,
      }));
    const inst = maybeCopy(this, copy);
    inst.setArgKey('hint', new HintExpr({ expressions: hintExprs }));
    return inst as this;
  }
}

export type SubqueryExprArgs = Merge<[
  DerivedTableExprArgs,
  QueryExprArgs,
  {
    with?: Expression;
    alias?: Expression;
    this?: Expression;
  },
]>;

export class SubqueryExpr extends multiInherit(DerivedTableExpr, QueryExpr) {
  static key = ExpressionKey.SUBQUERY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set([
    'this',
    'alias',
    'with',
    'match',
    'laterals',
    'joins',
    'connect',
    'pivots',
    'prewhere',
    'where',
    'group',
    'having',
    'qualify',
    'windows',
    'distribute',
    'sort',
    'cluster',
    'order',
    'limit',
    'offset',
    'locks',
    'sample',
    'settings',
    'format',
    'options',
  ]);

  declare args: SubqueryExprArgs;

  constructor (args: SubqueryExprArgs = {}) {
    super(args);
  }

  /**
   * Returns the first non-subquery expression.
   */
  unnest (): QueryExpr {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let expression: QueryExpr = this;
    while (expression instanceof SubqueryExpr) {
      const next = expression.args.this;
      if (!next) break;
      assertIsInstanceOf(next, QueryExpr);
      expression = next;
      if (expression === this) break;
    }
    return expression;
  }

  /**
   * Returns the outermost wrapper subquery.
   */
  unwrap (): SubqueryExpr {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let expression: SubqueryExpr = this;
    while (expression.sameParent && expression.isWrapper) {
      const parent = expression.parent;
      if (!(parent instanceof SubqueryExpr)) break;
      expression = parent;
    }
    return expression;
  }

  /**
   * Append to or set the SELECT expressions on the unnested query.
   */
  select (
    expressions?: string | Expression | (string | Expression | undefined)[],
    options: {
      append?: boolean;
      dialect?: DialectType;
      copy?: boolean;
      [key: string]: unknown;
    } = {},
  ): this {
    const {
      copy = true, ...restOptions
    } = options;
    const instance = maybeCopy(this, copy);
    const unnested = instance?.unnest();

    if (unnested instanceof SelectExpr) {
      unnested.select(expressions, {
        ...restOptions,
        copy: false,
      });
    }

    return instance;
  }

  /**
   * Whether this Subquery acts as a simple wrapper around another expression.
   *
   * SELECT * FROM (((SELECT * FROM t)))
   *               ^
   *               This corresponds to a "wrapper" Subquery node
   */
  get isWrapper (): boolean {
    return Object.entries(this.args).every(
      ([k, v]) => k === 'this' || v === undefined,
    );
  }

  /**
   * Returns true if the inner query is a star expression.
   */
  get isStar (): boolean {
    const thisArg = this.args.this;
    return thisArg ? thisArg.isStar : false;
  }

  /**
   * Returns the alias of this subquery.
   */
  get outputName (): string {
    return this.alias;
  }
}

export type WindowExprArgs = Merge<[
  ConditionExprArgs,
  {
    this?: Expression;
    partitionBy?: ExpressionValue[];
    order?: Expression;
    spec?: Expression;
    alias?: Expression;
    over?: Expression;
    first?: Expression;
  },
]>;

export class WindowExpr extends ConditionExpr {
  static key = ExpressionKey.WINDOW;

  static availableArgs = new Set([
    'this',
    'partitionBy',
    'order',
    'spec',
    'alias',
    'over',
    'first',
  ]);

  declare args: WindowExprArgs;

  constructor (args: WindowExprArgs = {}) {
    super(args);
  }
}

export type ParameterExprArgs = Merge<[
  ConditionExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class ParameterExpr extends ConditionExpr {
  static key = ExpressionKey.PARAMETER;

  static availableArgs = new Set(['this', 'expression']);

  declare args: ParameterExprArgs;

  constructor (args: ParameterExprArgs = {}) {
    super(args);
  }
}

/**
 * Valid kind values for session parameters
 */
export type SessionParameterExprArgs = Merge<[
  ConditionExprArgs,
  {
    this?: Expression;
    kind?: SessionParameterExprKind;
  },
]>;

export class SessionParameterExpr extends ConditionExpr {
  static key = ExpressionKey.SESSION_PARAMETER;

  static availableArgs = new Set(['this', 'kind']);

  declare args: SessionParameterExprArgs;

  constructor (args: SessionParameterExprArgs = {}) {
    super(args);
  }
}

/**
 * Valid kind values for placeholders
 */
export type PlaceholderExprArgs = Merge<[
  ConditionExprArgs,
  {
    this?: Expression;
    kind?: PlaceholderExprKind;
    widget?: Expression;
    jdbc?: boolean;
  },
]>;

export class PlaceholderExpr extends ConditionExpr {
  static key = ExpressionKey.PLACEHOLDER;

  static availableArgs = new Set([
    'this',
    'kind',
    'widget',
    'jdbc',
  ]);

  declare args: PlaceholderExprArgs;

  constructor (args: PlaceholderExprArgs = {}) {
    super(args);
  }

  /**
   * Returns the name of this placeholder.
   */
  get name (): string {
    return this.args.this?.name || '?';
  }
}

export type NullExprArgs = Merge<[
  ConditionExprArgs,
]>;

export class NullExpr extends ConditionExpr {
  static key = ExpressionKey.NULL;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: NullExprArgs;

  constructor (args: NullExprArgs = {}) {
    super(args);
  }

  /**
   * Returns the name of this undefined expression.
   */
  get name (): string {
    return 'NULL';
  }

  /**
   * Converts this to a Python undefined value.
   */
  toValue (): undefined {
    return undefined;
  }
}

export type BooleanExprArgs = Merge<[
  ConditionExprArgs,
  {
    this?: boolean;
  },
]>;

export class BooleanExpr extends ConditionExpr {
  static key = ExpressionKey.BOOLEAN;

  declare args: BooleanExprArgs;

  constructor (args: BooleanExprArgs = {}) {
    super(args);
  }

  /**
   * Converts this to a boolean value.
   */
  toValue (): boolean {
    return Boolean(this.args.this);
  }
}

export type PseudoTypeExprArgs = Merge<[
  DataTypeExprArgs,
  { this?: DataTypeExprKind },
]>;

export class PseudoTypeExpr extends DataTypeExpr {
  static key = ExpressionKey.PSEUDO_TYPE;

  static availableArgs = new Set(['this']);

  declare args: PseudoTypeExprArgs;

  constructor (args: PseudoTypeExprArgs = {}) {
    super(args);
  }
}

export type ObjectIdentifierExprArgs = Merge<[
  DataTypeExprArgs,
  { this?: DataTypeExprKind },
]>;

export class ObjectIdentifierExpr extends DataTypeExpr {
  static key = ExpressionKey.OBJECT_IDENTIFIER;

  static availableArgs = new Set(['this']);

  declare args: ObjectIdentifierExprArgs;

  constructor (args: ObjectIdentifierExprArgs = {}) {
    super(args);
  }
}

export type BinaryExprArgs = Merge<[
  ConditionExprArgs,
  {
    this?: ExpressionValue;
    expression?: ExpressionValue;
    operator?: ExpressionOrString;
    expressions?: Expression[];
  },
]>;

export class BinaryExpr extends ConditionExpr {
  static key = ExpressionKey.BINARY;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: BinaryExprArgs;

  constructor (args: BinaryExprArgs = {}) {
    super(args);
  }

  get left (): Expression | undefined {
    return this.args.this as Expression | undefined;
  }

  get right (): Expression | undefined {
    return this.args.expression as Expression | undefined;
  }
}

export type UnaryExprArgs = Merge<[
  BaseExpressionArgs,
  { this?: ExpressionOrString },
]>;
export class UnaryExpr extends Expression {
  static key = ExpressionKey.UNARY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: UnaryExprArgs;

  constructor (args: UnaryExprArgs = {}) {
    super(args);
  }
}

export type PivotAliasExprArgs = Merge<[
  AliasExprArgs,
]>;

export class PivotAliasExpr extends AliasExpr {
  static key = ExpressionKey.PIVOT_ALIAS;

  declare args: PivotAliasExprArgs;

  constructor (args: PivotAliasExprArgs = {}) {
    super(args);
  }
}

export type BracketExprArgs = Merge<[
  ConditionExprArgs,
  {
    this?: ExpressionValue;
    expressions?: Expression[];
    offset?: number;
    safe?: boolean;
    returnsListForMaps?: Expression[];
  },
]>;

/**
 * https://cloud.google.com/bigquery/docs/reference/standard-sql/operators#array_subscript_operator
 */
export class BracketExpr extends ConditionExpr {
  static key = ExpressionKey.BRACKET;

  static requiredArgs = new Set(['this', 'expressions']);

  static availableArgs = new Set([
    'this',
    'expressions',
    'offset',
    'safe',
    'returnsListForMaps',
  ]);

  declare args: BracketExprArgs;

  constructor (args: BracketExprArgs = {}) {
    super(args);
  }

  get outputName (): string {
    if (this.args.expressions?.length === 1) {
      return this.args.expressions[0].outputName;
    }
    return super.outputName;
  }
}

export type IntervalOpExprArgs = Merge<[
  TimeUnitExprArgs,
  {
    unit?: Expression;
    expression?: Expression;
  },
]>;

export class IntervalOpExpr extends TimeUnitExpr {
  static key = ExpressionKey.INTERVAL_OP;

  static requiredArgs = new Set(['expression']);

  static availableArgs = new Set(['unit', 'expression']);

  declare args: IntervalOpExprArgs;

  constructor (args: IntervalOpExprArgs = {}) {
    super(args);
  }

  interval (): IntervalExpr {
    return new IntervalExpr({
      this: this.args.expression?.copy(),
      unit: this.unit?.copy(),
    });
  }
}

/**
 * https://www.oracletutorial.com/oracle-basics/oracle-interval/
 * https://trino.io/docs/current/language/types.html#interval-day-to-second
 * https://docs.databricks.com/en/sql/language-manual/data-types/interval-type.html
 */
export type IntervalSpanExprArgs = Merge<[
  BaseExpressionArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class IntervalSpanExpr extends Expression {
  static key = ExpressionKey.INTERVAL_SPAN;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: IntervalSpanExprArgs;

  constructor (args: IntervalSpanExprArgs = {}) {
    super(args);
  }
}

export type IntervalExprArgs = Merge<[
  TimeUnitExprArgs,
  {
    this?: Expression;
    unit?: Expression;
  },
]>;

export class IntervalExpr extends TimeUnitExpr {
  static key = ExpressionKey.INTERVAL;

  static availableArgs = new Set(['this', 'unit']);

  declare args: IntervalExprArgs;

  constructor (args: IntervalExprArgs = {}) {
    super(args);
  }
}

/**
 * The base class for all function expressions.
 *
 * Attributes:
 *   isVarLenArgs: if set to true the last argument defined in argTypes will be
 *     treated as a variable length argument and the argument's value will be stored as a list.
 *   sqlNames: the SQL name (1st item in the list) and aliases (subsequent items) for this
 *     function expression. These values are used to map this node to a name during parsing as
 *     well as to provide the function's name during SQL string generation. By default the SQL
 *     name is set to the expression's class name transformed to snake case.
 */
export type FuncExprArgs = Merge<[
  ConditionExprArgs,
  {
    expression?: ExpressionValue;
    expressions?: ExpressionValue[];
    this?: ExpressionValue;
  },
]>;

export class FuncExpr extends ConditionExpr {
  static key = ExpressionKey.FUNC;

  declare args: FuncExprArgs;

  constructor (args: FuncExprArgs = {}) {
    super(args);
  }

  static isVarLenArgs = false;

  /**
   * Explicit ordering of arguments for fromArgList.
   *
   * In Python, dict keys maintain insertion order (Python 3.7+), so Object.keys(argTypes)
   * reliably reflects the order arguments were defined. However, in JavaScript, while
   * modern engines do maintain insertion order for string keys, relying on this behavior
   * can lead to subtle bugs, especially with numeric keys or in edge cases.
   *
   * By explicitly defining argOrder, we make the argument ordering:
   * - Reliable and predictable across all JavaScript engines
   * - Self-documenting (readers immediately see the expected argument order)
   * - Protected from future refactoring that might change argTypes definition order
   *
   * Subclasses with specific arguments should override this array.
   */
  static argOrder: string[] = [];

  /**
   * Create a function instance from a list of arguments
   */
  static fromArgList<T extends typeof FuncExpr> (this: T, args: unknown[]): InstanceType<T> {
    const allArgKeys = this.argOrder;

    if (this.isVarLenArgs) {
      const nonVarLenArgKeys = allArgKeys.slice(0, -1);
      const numNonVar = nonVarLenArgKeys.length;

      const argsDict: Record<string, unknown> = {};
      for (let i = 0; i < nonVarLenArgKeys.length; i++) {
        argsDict[nonVarLenArgKeys[i]] = args[i];
      }
      argsDict[allArgKeys[allArgKeys.length - 1]] = args.slice(numNonVar);

      return new this(argsDict as FuncExprArgs) as InstanceType<T>;
    } else {
      const argsDict: Record<string, unknown> = {};
      for (let i = 0; i < allArgKeys.length; i++) {
        argsDict[allArgKeys[i]] = args[i];
      }

      return new this(argsDict as FuncExprArgs) as InstanceType<T>;
    }
  }

  static _sqlNames: string[] = [];
  /**
   * Get the SQL names for this function class
   * @returns Array of SQL names (primary name first, then aliases)
   */
  static sqlNames (): string[] {
    if (this === FuncExpr) {
      throw new Error('SQL name is only supported by concrete function implementations');
    }

    if (!Object.hasOwn(this, '_sqlNames')) {
      const className = this.name.replace(/Expr$/, '');
      const snakeCase = className
        .replace(/([A-Z]+)([A-Z][a-z])/g, '$1_$2')
        .replace(/([a-z\d])([A-Z])/g, '$1_$2')
        .toLowerCase();

      this._sqlNames = [snakeCase];
    }

    return this._sqlNames;
  }

  /**
   * Get the primary SQL name for this function
   * @returns The primary SQL name (first item from sqlNames)
   */
  static sqlName (): string {
    const sqlNames = this.sqlNames();
    if (!sqlNames.length) {
      throw new Error(`Expected non-empty 'sql_names' for Func: ${this.name}.`);
    }
    return sqlNames[0];
  }

  /**
   * Get default parser mappings for this function
   * @returns Object mapping SQL names to the fromArgList parser
   */
  static defaultParserMappings (): Record<string, (args: Expression[]) => FuncExpr> {
    const mappings: Record<string, (args: Expression[]) => FuncExpr> = {};
    for (const name of this.sqlNames()) {
      mappings[name] = this.fromArgList.bind(this);
    }
    return mappings;
  }

  /**
   * Register this function class in the global registry
   * Called automatically by subclasses using static initialization blocks
   */
  static register (): void {
    if (this === FuncExpr || this === AggFuncExpr) {
      return;
    }
    registerFunc(this);
  }
}

export type JsonPathFilterExprArgs = Merge<[
  JsonPathPartExprArgs,
  { this?: string },
]>;
export class JsonPathFilterExpr extends JsonPathPartExpr {
  static key = ExpressionKey.JSON_PATH_FILTER;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: JsonPathFilterExprArgs;

  constructor (args: JsonPathFilterExprArgs = {}) {
    super(args);
  }
}

export type JsonPathKeyExprArgs = Merge<[
  JsonPathPartExprArgs,
  { this?: string | JsonPathWildcardExpr | JsonPathScriptExpr | JsonPathFilterExpr | JsonPathSliceExpr | number },
]>;
export class JsonPathKeyExpr extends JsonPathPartExpr {
  static key = ExpressionKey.JSON_PATH_KEY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: JsonPathKeyExprArgs;

  constructor (args: JsonPathKeyExprArgs = {}) {
    super(args);
  }
}

export type JsonPathRecursiveExprArgs = Merge<[
  JsonPathPartExprArgs,
  { this?: string | Expression },
]>;

export class JsonPathRecursiveExpr extends JsonPathPartExpr {
  static key = ExpressionKey.JSON_PATH_RECURSIVE;

  static availableArgs = new Set(['this']);

  declare args: JsonPathRecursiveExprArgs;

  constructor (args: JsonPathRecursiveExprArgs = {}) {
    super(args);
  }
}

export type JsonPathRootExprArgs = Merge<[
  JsonPathPartExprArgs,
]>;

export class JsonPathRootExpr extends JsonPathPartExpr {
  static key = ExpressionKey.JSON_PATH_ROOT;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: JsonPathRootExprArgs;

  constructor (args: JsonPathRootExprArgs = {}) {
    super(args);
  }
}

export type JsonPathScriptExprArgs = Merge<[
  JsonPathPartExprArgs,
  { this?: string },
]>;

export class JsonPathScriptExpr extends JsonPathPartExpr {
  static key = ExpressionKey.JSON_PATH_SCRIPT;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: JsonPathScriptExprArgs;

  constructor (args: JsonPathScriptExprArgs = {}) {
    super(args);
  }
}

export type JsonPathSliceExprArgs = Merge<[
  JsonPathPartExprArgs,
  {
    start?: string | ExpressionOrNumber;
    end?: string | ExpressionOrNumber;
    step?: string | ExpressionOrNumber;
  },
]>;

export class JsonPathSliceExpr extends JsonPathPartExpr {
  static key = ExpressionKey.JSON_PATH_SLICE;

  static availableArgs = new Set([
    'start',
    'end',
    'step',
  ]);

  declare args: JsonPathSliceExprArgs;

  constructor (args: JsonPathSliceExprArgs = {}) {
    super(args);
  }
}

export type JsonPathSelectorExprArgs = Merge<[
  JsonPathPartExprArgs,
  { this?: string | JsonPathWildcardExpr | JsonPathScriptExpr | JsonPathFilterExpr | JsonPathSliceExpr | number },
]>;

export class JsonPathSelectorExpr extends JsonPathPartExpr {
  static key = ExpressionKey.JSON_PATH_SELECTOR;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: JsonPathSelectorExprArgs;

  constructor (args: JsonPathSelectorExprArgs = {}) {
    super(args);
  }
}

export type JsonPathSubscriptExprArgs = Merge<[
  JsonPathPartExprArgs,
  { this?: string | JsonPathWildcardExpr | JsonPathScriptExpr | JsonPathFilterExpr | JsonPathSliceExpr | number },
]>;

export class JsonPathSubscriptExpr extends JsonPathPartExpr {
  static key = ExpressionKey.JSON_PATH_SUBSCRIPT;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this']);

  declare args: JsonPathSubscriptExprArgs;

  constructor (args: JsonPathSubscriptExprArgs = {}) {
    super(args);
  }
}

export type JsonPathUnionExprArgs = Merge<[
  JsonPathPartExprArgs,
  { expressions?: (string | JsonPathWildcardExpr | JsonPathScriptExpr | JsonPathFilterExpr | JsonPathSliceExpr | number)[] },
]>;

export class JsonPathUnionExpr extends JsonPathPartExpr {
  static key = ExpressionKey.JSON_PATH_UNION;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: JsonPathUnionExprArgs;

  constructor (args: JsonPathUnionExprArgs = {}) {
    super(args);
  }
}

export type JsonPathWildcardExprArgs = Merge<[
  JsonPathPartExprArgs,
]>;

export class JsonPathWildcardExpr extends JsonPathPartExpr {
  static key = ExpressionKey.JSON_PATH_WILDCARD;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  declare args: JsonPathWildcardExprArgs;

  constructor (args: JsonPathWildcardExprArgs = {}) {
    super(args);
  }
}

export type MergeExprArgs = Merge<[
  DmlExprArgs,
  {
    using?: Expression;
    on?: Expression;
    usingCond?: string;
    whens?: Expression;
    with?: Expression;
    returning?: Expression;
    this?: Expression;
  },
]>;

export class MergeExpr extends DmlExpr {
  static key = ExpressionKey.MERGE;

  static requiredArgs = new Set([
    'this',
    'using',
    'whens',
  ]);

  static availableArgs = new Set([
    'this',
    'using',
    'on',
    'usingCond',
    'whens',
    'with',
    'returning',
  ]);

  declare args: MergeExprArgs;

  constructor (args: MergeExprArgs = {}) {
    super(args);
  }
}

export type LateralExprArgs = Merge<[
  BaseExpressionArgs,
  {
    view?: boolean;
    outer?: Expression;
    crossApply?: boolean;
    ordinality?: boolean;
    alias?: Expression;
    this?: Expression;
  },
]>;

export class LateralExpr extends UdtfExpr {
  static key = ExpressionKey.LATERAL;

  static availableArgs = new Set([
    'this',
    'view',
    'outer',
    'alias',
    'crossApply',
    'ordinality',
  ]);

  declare args: LateralExprArgs;

  constructor (args: LateralExprArgs = {}) {
    super(args);
  }
}

export type TableFromRowsExprArgs = Merge<[
  UdtfExprArgs,
  {
    joins?: Expression[];
    pivots?: Expression[];
    sample?: number | Expression;
    alias?: Expression;
    this?: Expression;
  },
]>;

export class TableFromRowsExpr extends UdtfExpr {
  static key = ExpressionKey.TABLE_FROM_ROWS;

  static availableArgs = new Set([
    'this',
    'alias',
    'joins',
    'pivots',
    'sample',
  ]);

  declare args: TableFromRowsExprArgs;

  constructor (args: TableFromRowsExprArgs = {}) {
    super(args);
  }
}

export type UnionExprArgs = Merge<[
  SetOperationExprArgs,
]>;

export class UnionExpr extends SetOperationExpr {
  static key = ExpressionKey.UNION;

  declare args: UnionExprArgs;

  constructor (args: UnionExprArgs = {}) {
    super(args);
  }
}

export type ExceptExprArgs = Merge<[
  SetOperationExprArgs,
]>;

export class ExceptExpr extends SetOperationExpr {
  static key = ExpressionKey.EXCEPT;

  declare args: ExceptExprArgs;

  constructor (args: ExceptExprArgs = {}) {
    super(args);
  }
}

export type IntersectExprArgs = Merge<[
  SetOperationExprArgs,
]>;

export class IntersectExpr extends SetOperationExpr {
  static key = ExpressionKey.INTERSECT;

  declare args: IntersectExprArgs;

  constructor (args: IntersectExprArgs = {}) {
    super(args);
  }
}

/**
 * VALUES clause with DuckDB support for ORDER BY, LIMIT, OFFSET
 * @see {@link https://duckdb.org/docs/stable/sql/query_syntax/limit | DuckDB LIMIT}
 */
export type ValuesExprArgs = Merge<[
  UdtfExprArgs,
  {
    expressions?: Expression[];
    alias?: TableAliasExpr;
    order?: Expression;
    limit?: number | Expression;
    offset?: number | Expression;
  },
]>;

export class ValuesExpr extends UdtfExpr {
  static key = ExpressionKey.VALUES;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set([
    'expressions',
    'alias',
    'order',
    'limit',
    'offset',
  ]);

  declare args: ValuesExprArgs;

  constructor (args: ValuesExprArgs = {}) {
    super(args);
  }
}

export type SubqueryPredicateExprArgs = Merge<[
  PredicateExprArgs,
]>;

export class SubqueryPredicateExpr extends PredicateExpr {
  static key = ExpressionKey.SUBQUERY_PREDICATE;

  declare args: SubqueryPredicateExprArgs;

  constructor (args: SubqueryPredicateExprArgs = {}) {
    super(args);
  }
}

export type AddExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class AddExpr extends BinaryExpr {
  static key = ExpressionKey.ADD;

  declare args: AddExprArgs;

  constructor (args: AddExprArgs = {}) {
    super(args);
  }
}

export type ConnectorExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class ConnectorExpr extends BinaryExpr {
  static key = ExpressionKey.CONNECTOR;

  declare args: ConnectorExprArgs;

  constructor (args: ConnectorExprArgs = {}) {
    super(args);
  }
}

export type BitwiseAndExprArgs = Merge<[
  BinaryExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    padside?: Expression;
  },
]>;

export class BitwiseAndExpr extends BinaryExpr {
  static key = ExpressionKey.BITWISE_AND;

  static availableArgs = new Set([
    'this',
    'expression',
    'padside',
  ]);

  declare args: BitwiseAndExprArgs;

  constructor (args: BitwiseAndExprArgs = {}) {
    super(args);
  }
}

export type BitwiseLeftShiftExprArgs = Merge<[
  BinaryExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    requiresInt128?: Expression;
  },
]>;

export class BitwiseLeftShiftExpr extends BinaryExpr {
  static key = ExpressionKey.BITWISE_LEFT_SHIFT;

  static availableArgs = new Set([
    'this',
    'expression',
    'requiresInt128',
  ]);

  declare args: BitwiseLeftShiftExprArgs;

  constructor (args: BitwiseLeftShiftExprArgs = {}) {
    super(args);
  }
}

export type BitwiseOrExprArgs = Merge<[
  BinaryExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    padside?: Expression;
  },
]>;

export class BitwiseOrExpr extends BinaryExpr {
  static key = ExpressionKey.BITWISE_OR;

  static availableArgs = new Set([
    'this',
    'expression',
    'padside',
  ]);

  declare args: BitwiseOrExprArgs;

  constructor (args: BitwiseOrExprArgs = {}) {
    super(args);
  }
}

export type BitwiseRightShiftExprArgs = Merge<[
  BinaryExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    requiresInt128?: Expression;
  },
]>;

export class BitwiseRightShiftExpr extends BinaryExpr {
  static key = ExpressionKey.BITWISE_RIGHT_SHIFT;

  static availableArgs = new Set([
    'this',
    'expression',
    'requiresInt128',
  ]);

  declare args: BitwiseRightShiftExprArgs;

  constructor (args: BitwiseRightShiftExprArgs = {}) {
    super(args);
  }
}

export type BitwiseXorExprArgs = Merge<[
  BinaryExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    padside?: Expression;
  },
]>;

export class BitwiseXorExpr extends BinaryExpr {
  static key = ExpressionKey.BITWISE_XOR;

  static availableArgs = new Set([
    'this',
    'expression',
    'padside',
  ]);

  declare args: BitwiseXorExprArgs;

  constructor (args: BitwiseXorExprArgs = {}) {
    super(args);
  }
}

export type DivExprArgs = Merge<[
  BinaryExprArgs,
  {
    this?: ExpressionValue;
    expression?: ExpressionValue;
    typed?: Expression;
    safe?: boolean;
  },
]>;

export class DivExpr extends BinaryExpr {
  static key = ExpressionKey.DIV;

  static availableArgs = new Set([
    'this',
    'expression',
    'typed',
    'safe',
  ]);

  declare args: DivExprArgs;

  constructor (args: DivExprArgs = {}) {
    super(args);
  }

  get left (): Expression | undefined {
    return this.args.this as Expression | undefined;
  }

  get right (): Expression | undefined {
    return this.args.expression as Expression | undefined;
  }
}

export type OverlapsExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class OverlapsExpr extends BinaryExpr {
  static key = ExpressionKey.OVERLAPS;

  declare args: OverlapsExprArgs;

  constructor (args: OverlapsExprArgs = {}) {
    super(args);
  }
}

export type ExtendsLeftExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class ExtendsLeftExpr extends BinaryExpr {
  static key = ExpressionKey.EXTENDS_LEFT;

  declare args: ExtendsLeftExprArgs;

  constructor (args: ExtendsLeftExprArgs = {}) {
    super(args);
  }
}

export type ExtendsRightExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class ExtendsRightExpr extends BinaryExpr {
  static key = ExpressionKey.EXTENDS_RIGHT;

  declare args: ExtendsRightExprArgs;

  constructor (args: ExtendsRightExprArgs = {}) {
    super(args);
  }
}

export type DotExprArgs = Merge<[
  BinaryExprArgs,
  {
    expression?: Expression;
    expressions?: Expression[];
  },
]>;

export class DotExpr extends BinaryExpr {
  static key = ExpressionKey.DOT;

  declare args: DotExprArgs;

  constructor (args: DotExprArgs = {}) {
    super(args);
  }

  get isStar (): boolean {
    return this.args.expression?.isStar ?? false;
  }

  get name (): string {
    return this.args.expression?.name ?? '';
  }

  get outputName (): string {
    return this.name;
  }

  /**
   * Build a Dot object with a sequence of expressions.
   */
  static build (expressions: Expression[]): DotExpr {
    if (expressions.length < 2) {
      throw new Error('Dot requires >= 2 expressions.');
    }

    return expressions.reduce(
      (x, y) => new DotExpr({
        this: x,
        expression: y,
      }),
    ) as DotExpr;
  }

  /**
   * Return the parts of a table / column in order catalog, db, table.
   */
  get parts (): Expression[] {
    const [thisExpr, ...restParts] = this.flatten();
    const parts = [...restParts];

    parts.reverse();

    for (const arg of COLUMN_PARTS) {
      const part = (thisExpr.args as Record<string, ExpressionValue | ExpressionValueList>)[arg];

      if (part instanceof Expression) {
        parts.push(part);
      }
    }

    parts.reverse();
    return parts;
  }
}

export type DPipeExprArgs = Merge<[
  BinaryExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    safe?: boolean;
  },
]>;

export class DPipeExpr extends BinaryExpr {
  static key = ExpressionKey.D_PIPE;

  static availableArgs = new Set([
    'this',
    'expression',
    'safe',
  ]);

  declare args: DPipeExprArgs;

  constructor (args: DPipeExprArgs = {}) {
    super(args);
  }
}

export type EqExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class EqExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  static key = ExpressionKey.EQ;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: EqExprArgs;

  constructor (args: EqExprArgs = {}) {
    super(args);
  }
}

export type NullSafeEqExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class NullSafeEqExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  static key = ExpressionKey.NULL_SAFE_EQ;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: NullSafeEqExprArgs;

  constructor (args: NullSafeEqExprArgs = {}) {
    super(args);
  }
}

export type NullSafeNeqExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class NullSafeNeqExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  static key = ExpressionKey.NULL_SAFE_NEQ;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: NullSafeNeqExprArgs;

  constructor (args: NullSafeNeqExprArgs = {}) {
    super(args);
  }
}

export type PropertyEqExprArgs = Merge<[
  BinaryExprArgs,
  {
    expression?: ExpressionValue;
    this?: ExpressionValue;
  },
]>;

export class PropertyEqExpr extends BinaryExpr {
  static key = ExpressionKey.PROPERTY_EQ;

  declare args: PropertyEqExprArgs;

  constructor (args: PropertyEqExprArgs = {}) {
    super(args);
  }
}

export type DistanceExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class DistanceExpr extends BinaryExpr {
  static key = ExpressionKey.DISTANCE;

  declare args: DistanceExprArgs;

  constructor (args: DistanceExprArgs = {}) {
    super(args);
  }
}

export type EscapeExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class EscapeExpr extends BinaryExpr {
  static key = ExpressionKey.ESCAPE;

  declare args: EscapeExprArgs;

  constructor (args: EscapeExprArgs = {}) {
    super(args);
  }
}

export type GlobExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class GlobExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  static key = ExpressionKey.GLOB;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: GlobExprArgs;

  constructor (args: GlobExprArgs = {}) {
    super(args);
  }
}

export type GtExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class GtExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  static key = ExpressionKey.GT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: GtExprArgs;

  constructor (args: GtExprArgs = {}) {
    super(args);
  }
}

export type GteExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class GteExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  static key = ExpressionKey.GtE;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: GteExprArgs;

  constructor (args: GteExprArgs = {}) {
    super(args);
  }
}

export type ILikeExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class ILikeExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  static key = ExpressionKey.ILIKE;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: ILikeExprArgs;

  constructor (args: ILikeExprArgs = {}) {
    super(args);
  }
}

export type IntDivExprArgs = Merge<[
  BinaryExprArgs,
  {
    this?: Expression;
  },
]>;

export class IntDivExpr extends BinaryExpr {
  static key = ExpressionKey.INT_DIV;

  declare args: IntDivExprArgs;

  constructor (args: IntDivExprArgs = {}) {
    super(args);
  }
}

export type IsExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class IsExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  static key = ExpressionKey.IS;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: IsExprArgs;

  constructor (args: IsExprArgs = {}) {
    super(args);
  }
}

export type KwargExprArgs = Merge<[
  BinaryExprArgs,
  { this?: Expression },
]>;

/**
 * Kwarg in special functions like func(kwarg => y).
 */
export class KwargExpr extends BinaryExpr {
  static key = ExpressionKey.KWARG;

  declare args: KwargExprArgs;

  constructor (args: KwargExprArgs = {}) {
    super(args);
  }
}

export type LikeExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class LikeExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  static key = ExpressionKey.LIKE;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: LikeExprArgs;

  constructor (args: LikeExprArgs = {}) {
    super(args);
  }
}

export type MatchExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class MatchExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  static key = ExpressionKey.MATCH;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: MatchExprArgs;

  constructor (args: MatchExprArgs = {}) {
    super(args);
  }
}

export type LtExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class LtExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  static key = ExpressionKey.LT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: LtExprArgs;

  constructor (args: LtExprArgs = {}) {
    super(args);
  }
}

export type LteExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class LteExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  static key = ExpressionKey.LTE;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: LteExprArgs;

  constructor (args: LteExprArgs = {}) {
    super(args);
  }
}

export type ModExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class ModExpr extends BinaryExpr {
  static key = ExpressionKey.MOD;

  declare args: ModExprArgs;

  constructor (args: ModExprArgs = {}) {
    super(args);
  }
}

export type MulExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class MulExpr extends BinaryExpr {
  static key = ExpressionKey.MUL;

  declare args: MulExprArgs;

  constructor (args: MulExprArgs = {}) {
    super(args);
  }
}

export type NeqExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class NeqExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  static key = ExpressionKey.NEQ;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: NeqExprArgs;

  constructor (args: NeqExprArgs = {}) {
    super(args);
  }
}

export type OperatorExprArgs = Merge<[
  BinaryExprArgs,
  {
    this?: Expression;
    operator?: Expression;
    expression?: Expression;
  },
]>;

export class OperatorExpr extends BinaryExpr {
  static key = ExpressionKey.OPERATOR;

  static requiredArgs = new Set([
    'this',
    'operator',
    'expression',
  ]);

  static availableArgs = new Set([
    'this',
    'operator',
    'expression',
  ]);

  declare args: OperatorExprArgs;

  constructor (args: OperatorExprArgs = {}) {
    super(args);
  }
}

export type SimilarToExprArgs = Merge<[
  PredicateExprArgs,
  BinaryExprArgs,
]>;

export class SimilarToExpr extends multiInherit(BinaryExpr, PredicateExpr) {
  static key = ExpressionKey.SIMILAR_TO;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: SimilarToExprArgs;

  constructor (args: SimilarToExprArgs = {}) {
    super(args);
  }
}

export type SubExprArgs = Merge<[
  BinaryExprArgs,
  { expression?: string | Expression },
]>;

export class SubExpr extends BinaryExpr {
  static key = ExpressionKey.SUB;

  declare args: SubExprArgs;

  constructor (args: SubExprArgs = {}) {
    super(args);
  }
}

export type AdjacentExprArgs = Merge<[
  BinaryExprArgs,
]>;

export class AdjacentExpr extends BinaryExpr {
  static key = ExpressionKey.ADJACENT;

  declare args: AdjacentExprArgs;

  constructor (args: AdjacentExprArgs = {}) {
    super(args);
  }
}

export type BitwiseNotExprArgs = Merge<[
  UnaryExprArgs,
]>;

export class BitwiseNotExpr extends UnaryExpr {
  static key = ExpressionKey.BITWISE_NOT;

  declare args: BitwiseNotExprArgs;

  constructor (args: BitwiseNotExprArgs = {}) {
    super(args);
  }
}

export type NotExprArgs = Merge<[
  UnaryExprArgs,
  { this?: Expression },
]>;

export class NotExpr extends UnaryExpr {
  static key = ExpressionKey.NOT;

  declare args: NotExprArgs;

  constructor (args: NotExprArgs = {}) {
    super(args);
  }
}

export type ParenExprArgs = Merge<[
  UnaryExprArgs,
  { this?: Expression },
]>;

export class ParenExpr extends UnaryExpr {
  static key = ExpressionKey.PAREN;

  declare args: ParenExprArgs;

  constructor (args: ParenExprArgs = {}) {
    super(args);
  }

  get outputName (): string {
    return this.args.this?.name ?? '';
  }
}

export type NegExprArgs = Merge<[
  UnaryExprArgs,
  { this?: Expression }, // NOTE: sqlglot does not have this
]>;

export class NegExpr extends UnaryExpr {
  static key = ExpressionKey.NEG;

  declare args: NegExprArgs;

  constructor (args: NegExprArgs = {}) {
    super(args);
  }

  toValue (): ExpressionValue | undefined {
    if (this.isNumber) {
      return ((this.args.this?.toValue() as number) ?? 0) * -1;
    }
    return super.toValue();
  }
}

export type BetweenExprArgs = Merge<[
  PredicateExprArgs,
  {
    this?: Expression;
    low?: Expression;
    high?: Expression;
    symmetric?: Expression;
  },
]>;

export class BetweenExpr extends PredicateExpr {
  static key = ExpressionKey.BETWEEN;

  static requiredArgs = new Set([
    'this',
    'low',
    'high',
  ]);

  static availableArgs = new Set([
    'this',
    'low',
    'high',
    'symmetric',
  ]);

  declare args: BetweenExprArgs;

  constructor (args: BetweenExprArgs = {}) {
    super(args);
  }
}

export type InExprArgs = Merge<[
  PredicateExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
    query?: Expression;
    unnest?: Expression;
    field?: Expression;
    isGlobal?: boolean;
  },
]>;

export class InExpr extends PredicateExpr {
  static key = ExpressionKey.IN;

  static availableArgs = new Set([
    'this',
    'expressions',
    'query',
    'unnest',
    'field',
    'isGlobal',
  ]);

  declare args: InExprArgs;

  constructor (args: InExprArgs = {}) {
    super(args);
  }
}

/**
 * Function returns NULL instead of error
 * https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/functions-reference#safe_prefix
 */
export type SafeFuncExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SafeFuncExpr extends FuncExpr {
  static key = ExpressionKey.SAFE_FUNC;

  static argOrder = ['this'];

  declare args: SafeFuncExprArgs;

  constructor (args: SafeFuncExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TypeofExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TypeofExpr extends FuncExpr {
  static key = ExpressionKey.TYPEOF;

  static argOrder = ['this'];

  declare args: TypeofExprArgs;

  constructor (args: TypeofExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AcosExprArgs = Merge<[
  FuncExprArgs,
]>;

export class AcosExpr extends FuncExpr {
  static key = ExpressionKey.ACOS;

  static argOrder = ['this'];

  declare args: AcosExprArgs;

  constructor (args: AcosExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AcoshExprArgs = Merge<[
  FuncExprArgs,
]>;

export class AcoshExpr extends FuncExpr {
  static key = ExpressionKey.ACOSH;

  static argOrder = ['this'];

  declare args: AcoshExprArgs;

  constructor (args: AcoshExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AsinExprArgs = Merge<[
  FuncExprArgs,
]>;

export class AsinExpr extends FuncExpr {
  static key = ExpressionKey.ASIN;

  static argOrder = ['this'];

  declare args: AsinExprArgs;

  constructor (args: AsinExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AsinhExprArgs = Merge<[
  FuncExprArgs,
]>;

export class AsinhExpr extends FuncExpr {
  static key = ExpressionKey.ASINH;

  static argOrder = ['this'];

  declare args: AsinhExprArgs;

  constructor (args: AsinhExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AtanExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
  },
]>;

export class AtanExpr extends FuncExpr {
  static key = ExpressionKey.ATAN;

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: AtanExprArgs;

  constructor (args: AtanExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AtanhExprArgs = Merge<[
  FuncExprArgs,
]>;

export class AtanhExpr extends FuncExpr {
  static key = ExpressionKey.ATANH;

  static argOrder = ['this'];

  declare args: AtanhExprArgs;

  constructor (args: AtanhExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Atan2ExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class Atan2Expr extends FuncExpr {
  static key = ExpressionKey.ATAN2;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: Atan2ExprArgs;

  constructor (args: Atan2ExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CotExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CotExpr extends FuncExpr {
  static key = ExpressionKey.COT;

  static argOrder = ['this'];

  declare args: CotExprArgs;

  constructor (args: CotExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CothExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CothExpr extends FuncExpr {
  static key = ExpressionKey.COTH;

  static argOrder = ['this'];

  declare args: CothExprArgs;

  constructor (args: CothExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CosExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CosExpr extends FuncExpr {
  static key = ExpressionKey.COS;

  static argOrder = ['this'];

  declare args: CosExprArgs;

  constructor (args: CosExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CscExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CscExpr extends FuncExpr {
  static key = ExpressionKey.CSC;

  static argOrder = ['this'];

  declare args: CscExprArgs;

  constructor (args: CscExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CschExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CschExpr extends FuncExpr {
  static key = ExpressionKey.CSCH;

  static argOrder = ['this'];

  declare args: CschExprArgs;

  constructor (args: CschExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SecExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SecExpr extends FuncExpr {
  static key = ExpressionKey.SEC;

  static argOrder = ['this'];

  declare args: SecExprArgs;

  constructor (args: SecExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SechExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SechExpr extends FuncExpr {
  static key = ExpressionKey.SECH;

  static argOrder = ['this'];

  declare args: SechExprArgs;

  constructor (args: SechExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SinExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SinExpr extends FuncExpr {
  static key = ExpressionKey.SIN;

  static argOrder = ['this'];

  declare args: SinExprArgs;

  constructor (args: SinExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SinhExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SinhExpr extends FuncExpr {
  static key = ExpressionKey.SINH;

  static argOrder = ['this'];

  declare args: SinhExprArgs;

  constructor (args: SinhExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TanExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TanExpr extends FuncExpr {
  static key = ExpressionKey.TAN;

  static argOrder = ['this'];

  declare args: TanExprArgs;

  constructor (args: TanExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TanhExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TanhExpr extends FuncExpr {
  static key = ExpressionKey.TANH;

  static argOrder = ['this'];

  declare args: TanhExprArgs;

  constructor (args: TanhExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DegreesExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DegreesExpr extends FuncExpr {
  static key = ExpressionKey.DEGREES;

  static argOrder = ['this'];

  declare args: DegreesExprArgs;

  constructor (args: DegreesExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CoshExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CoshExpr extends FuncExpr {
  static key = ExpressionKey.COSH;

  static argOrder = ['this'];

  declare args: CoshExprArgs;

  constructor (args: CoshExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CosineDistanceExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class CosineDistanceExpr extends FuncExpr {
  static key = ExpressionKey.COSINE_DISTANCE;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: CosineDistanceExprArgs;

  constructor (args: CosineDistanceExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DotProductExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class DotProductExpr extends FuncExpr {
  static key = ExpressionKey.DOT_PRODUCT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: DotProductExprArgs;

  constructor (args: DotProductExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type EuclideanDistanceExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class EuclideanDistanceExpr extends FuncExpr {
  static key = ExpressionKey.EUCLIDEAN_DISTANCE;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: EuclideanDistanceExprArgs;

  constructor (args: EuclideanDistanceExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ManhattanDistanceExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class ManhattanDistanceExpr extends FuncExpr {
  static key = ExpressionKey.MANHATTAN_DISTANCE;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: ManhattanDistanceExprArgs;

  constructor (args: ManhattanDistanceExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JarowinklerSimilarityExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class JarowinklerSimilarityExpr extends FuncExpr {
  static key = ExpressionKey.JAROWINKLER_SIMILARITY;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'caseInsensitive',
  ]);

  static argOrder = ['this', 'expression'];

  declare args: JarowinklerSimilarityExprArgs;

  constructor (args: JarowinklerSimilarityExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AggFuncExprArgs = Merge<[
  FuncExprArgs,
  { this?: ExpressionValue },
]>;

export class AggFuncExpr extends FuncExpr {
  static key = ExpressionKey.AGG_FUNC;

  static argOrder = ['this'];

  declare args: AggFuncExprArgs;

  constructor (args: AggFuncExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitwiseCountExprArgs = Merge<[
  FuncExprArgs,
]>;

export class BitwiseCountExpr extends FuncExpr {
  static key = ExpressionKey.BITWISE_COUNT;

  static argOrder = ['this'];

  declare args: BitwiseCountExprArgs;

  constructor (args: BitwiseCountExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitmapBucketNumberExprArgs = Merge<[
  FuncExprArgs,
]>;

export class BitmapBucketNumberExpr extends FuncExpr {
  static key = ExpressionKey.BITMAP_BUCKET_NUMBER;

  static argOrder = ['this'];

  declare args: BitmapBucketNumberExprArgs;

  constructor (args: BitmapBucketNumberExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitmapCountExprArgs = Merge<[
  FuncExprArgs,
]>;

export class BitmapCountExpr extends FuncExpr {
  static key = ExpressionKey.BITMAP_COUNT;

  static argOrder = ['this'];

  declare args: BitmapCountExprArgs;

  constructor (args: BitmapCountExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitmapBitPositionExprArgs = Merge<[
  FuncExprArgs,
]>;

export class BitmapBitPositionExpr extends FuncExpr {
  static key = ExpressionKey.BITMAP_BIT_POSITION;

  static argOrder = ['this'];

  declare args: BitmapBitPositionExprArgs;

  constructor (args: BitmapBitPositionExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ByteLengthExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ByteLengthExpr extends FuncExpr {
  static key = ExpressionKey.BYTE_LENGTH;

  static argOrder = ['this'];

  declare args: ByteLengthExprArgs;

  constructor (args: ByteLengthExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BoolnotExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    roundInput?: boolean | Expression;
  },
]>;

export class BoolnotExpr extends FuncExpr {
  static key = ExpressionKey.BOOLNOT;

  static availableArgs = new Set(['this', 'roundInput']);

  static argOrder = ['this', 'roundInput'];

  declare args: BoolnotExprArgs;

  constructor (args: BoolnotExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BoolandExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    roundInput?: boolean | Expression;
  },
]>;

export class BoolandExpr extends FuncExpr {
  static key = ExpressionKey.BOOLAND;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'roundInput',
  ]);

  static argOrder = [
    'this',
    'expression',
    'roundInput',
  ];

  declare args: BoolandExprArgs;

  constructor (args: BoolandExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BoolorExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    roundInput?: boolean | Expression;
  },
]>;

export class BoolorExpr extends FuncExpr {
  static key = ExpressionKey.BOOLOR;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'roundInput',
  ]);

  static argOrder = [
    'this',
    'expression',
    'roundInput',
  ];

  declare args: BoolorExprArgs;

  constructor (args: BoolorExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#bool_for_json
 */
export type JsonBoolExprArgs = Merge<[
  FuncExprArgs,
]>;

export class JsonBoolExpr extends FuncExpr {
  static key = ExpressionKey.JSON_BOOL;

  static argOrder = ['this'];

  declare args: JsonBoolExprArgs;

  constructor (args: JsonBoolExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayRemoveExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    nullPropagation?: boolean;
  },
]>;

export class ArrayRemoveExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_REMOVE;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'nullPropagation',
  ]);

  static argOrder = [
    'this',
    'expression',
    'nullPropagation',
  ];

  declare args: ArrayRemoveExprArgs;

  constructor (args: ArrayRemoveExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AbsExprArgs = Merge<[
  FuncExprArgs,
]>;

export class AbsExpr extends FuncExpr {
  static key = ExpressionKey.ABS;

  static argOrder = ['this'];

  declare args: AbsExprArgs;

  constructor (args: AbsExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ApproxTopKEstimateExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
  },
]>;

export class ApproxTopKEstimateExpr extends FuncExpr {
  static key = ExpressionKey.APPROX_TOP_K_ESTIMATE;

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: ApproxTopKEstimateExprArgs;

  constructor (args: ApproxTopKEstimateExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type FarmFingerprintExprArgs = Merge<[
  FuncExprArgs,
  { expressions?: Expression[] },
]>;

export class FarmFingerprintExpr extends FuncExpr {
  static key = ExpressionKey.FARM_FINGERPRINT;

  static isVarLenArgs = true;

  static _sqlNames = ['FARM_FINGERPRINT', 'FARMFINGERPRINT64'];

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  static argOrder = ['expressions'];

  declare args: FarmFingerprintExprArgs;

  constructor (args: FarmFingerprintExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type FlattenExprArgs = Merge<[
  FuncExprArgs,
]>;

export class FlattenExpr extends FuncExpr {
  static key = ExpressionKey.FLATTEN;

  static argOrder = ['this'];

  declare args: FlattenExprArgs;

  constructor (args: FlattenExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Float64ExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
  },
]>;

export class Float64Expr extends FuncExpr {
  static key = ExpressionKey.FLOAT64;

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: Float64ExprArgs;

  constructor (args: Float64ExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * https://spark.apache.org/docs/latest/api/sql/index.html#transform
 */
export type TransformExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class TransformExpr extends FuncExpr {
  static key = ExpressionKey.TRANSFORM;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: TransformExprArgs;

  constructor (args: TransformExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TranslateExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    from?: Expression;
    to?: Expression;
  },
]>;

export class TranslateExpr extends FuncExpr {
  static key = ExpressionKey.TRANSLATE;

  static requiredArgs = new Set([
    'this',
    'from',
    'to',
  ]);

  static availableArgs = new Set([
    'this',
    'from',
    'to',
  ]);

  static argOrder = [
    'this',
    'fromStr',
    'to',
  ];

  declare args: TranslateExprArgs;

  constructor (args: TranslateExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AnonymousExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: ExpressionValue;
    expressions?: ExpressionValueList;
  },
]>;

export class AnonymousExpr extends FuncExpr {
  static key = ExpressionKey.ANONYMOUS;

  static isVarLenArgs = true;

  static availableArgs = new Set(['this', 'expressions']);

  static argOrder = ['this', 'expressions'];

  declare args: AnonymousExprArgs;

  constructor (args: AnonymousExprArgs = {}) {
    super(args);
  }

  get name (): string {
    const thisExpr = this.args.this;
    if (thisExpr === undefined) {
      return '';
    }
    if (thisExpr instanceof Expression) {
      return thisExpr.name;
    }
    return thisExpr.toString();
  }

  static {
    this.register();
  }
}

export type ApplyExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class ApplyExpr extends FuncExpr {
  static key = ExpressionKey.APPLY;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: ApplyExprArgs;

  constructor (args: ApplyExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayExprArgs = Merge<[
  FuncExprArgs,
  {
    expressions?: (string | Expression)[];
    bracketNotation?: Expression;
    structNameInheritance?: boolean | string;
  },
]>;

export class ArrayExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY;

  static isVarLenArgs = true;

  static availableArgs = new Set([
    'expressions',
    'bracketNotation',
    'structNameInheritance',
  ]);

  static argOrder = [
    'expressions',
    'bracketNotation',
    'structNameInheritance',
  ];

  declare args: ArrayExprArgs;

  constructor (args: ArrayExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AsciiExprArgs = Merge<[
  FuncExprArgs,
]>;

export class AsciiExpr extends FuncExpr {
  static key = ExpressionKey.ASCII;

  static argOrder = ['this'];

  declare args: AsciiExprArgs;

  constructor (args: AsciiExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToArrayExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class ToArrayExpr extends FuncExpr {
  static key = ExpressionKey.TO_ARRAY;

  static argOrder = ['this'];

  declare args: ToArrayExprArgs;

  constructor (args: ToArrayExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToBooleanExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    safe?: boolean;
  },
]>;

export class ToBooleanExpr extends FuncExpr {
  static key = ExpressionKey.TO_BOOLEAN;

  static availableArgs = new Set(['this', 'safe']);

  static argOrder = ['this', 'safe'];

  declare args: ToBooleanExprArgs;

  constructor (args: ToBooleanExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ListExprArgs = Merge<[
  FuncExprArgs,
  { expressions?: Expression[] },
]>;

export class ListExpr extends FuncExpr {
  static key = ExpressionKey.LIST;

  static isVarLenArgs = true;

  static availableArgs = new Set(['expressions']);

  static argOrder = ['expressions'];

  declare args: ListExprArgs;

  constructor (args: ListExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * String pad, kind True -> LPAD, False -> RPAD
 */
export type PadExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    fillPattern?: Expression;
    isLeft?: boolean;
  },
]>;

export class PadExpr extends FuncExpr {
  static key = ExpressionKey.PAD;

  static requiredArgs = new Set([
    'this',
    'expression',
    'isLeft',
  ]);

  static availableArgs = new Set([
    'this',
    'expression',
    'fillPattern',
    'isLeft',
  ]);

  static argOrder = [
    'this',
    'expression',
    'fillPattern',
    'isLeft',
  ];

  declare args: PadExprArgs;

  constructor (args: PadExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/to_char
 * https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/TO_CHAR-number.html
 */
export type ToCharExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    format?: Expression;
    nlsparam?: Expression;
    isNumeric?: Expression;
  },
]>;

export class ToCharExpr extends FuncExpr {
  static key = ExpressionKey.TO_CHAR;

  static availableArgs = new Set([
    'this',
    'format',
    'nlsparam',
    'isNumeric',
  ]);

  static argOrder = [
    'this',
    'format',
    'nlsparam',
    'isNumeric',
  ];

  declare args: ToCharExprArgs;

  constructor (args: ToCharExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToCodePointsExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ToCodePointsExpr extends FuncExpr {
  static key = ExpressionKey.TO_CODE_POINTS;

  static argOrder = ['this'];

  declare args: ToCodePointsExprArgs;

  constructor (args: ToCodePointsExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/to_decimal
 * https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/TO_NUMBER.html
 */
export type ToNumberExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    format?: Expression;
    nlsparam?: Expression;
    precision?: Expression;
    scale?: Expression;
    safe?: boolean;
    safeName?: string;
  },
]>;

export class ToNumberExpr extends FuncExpr {
  static key = ExpressionKey.TO_NUMBER;

  static availableArgs = new Set([
    'this',
    'format',
    'nlsparam',
    'precision',
    'scale',
    'safe',
    'safeName',
  ]);

  static argOrder = [
    'this',
    'format',
    'nlsparam',
    'precision',
    'scale',
    'safe',
    'safeName',
  ];

  declare args: ToNumberExprArgs;

  constructor (args: ToNumberExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToDoubleExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    format?: Expression;
    safe?: boolean;
  },
]>;

export class ToDoubleExpr extends FuncExpr {
  static key = ExpressionKey.TO_DOUBLE;

  static availableArgs = new Set([
    'this',
    'format',
    'safe',
  ]);

  static argOrder = [
    'this',
    'format',
    'safe',
  ];

  declare args: ToDoubleExprArgs;

  constructor (args: ToDoubleExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToDecfloatExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    format?: Expression;
  },
]>;

export class ToDecfloatExpr extends FuncExpr {
  static key = ExpressionKey.TO_DECFLOAT;

  static availableArgs = new Set(['this', 'format']);

  static argOrder = ['this', 'format'];

  declare args: ToDecfloatExprArgs;

  constructor (args: ToDecfloatExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TryToDecfloatExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    format?: Expression;
  },
]>;

export class TryToDecfloatExpr extends FuncExpr {
  static key = ExpressionKey.TRY_TO_DECFLOAT;

  static availableArgs = new Set(['this', 'format']);

  static argOrder = ['this', 'format'];

  declare args: TryToDecfloatExprArgs;

  constructor (args: TryToDecfloatExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToFileExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    path?: Expression;
    safe?: boolean;
  },
]>;

export class ToFileExpr extends FuncExpr {
  static key = ExpressionKey.TO_FILE;

  static availableArgs = new Set([
    'this',
    'path',
    'safe',
  ]);

  static argOrder = [
    'this',
    'path',
    'safe',
  ];

  declare args: ToFileExprArgs;

  constructor (args: ToFileExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CodePointsToBytesExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CodePointsToBytesExpr extends FuncExpr {
  static key = ExpressionKey.CODE_POINTS_TO_BYTES;

  static argOrder = ['this'];

  declare args: CodePointsToBytesExprArgs;

  constructor (args: CodePointsToBytesExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ColumnsExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    unpack?: Expression;
  },
]>;

export class ColumnsExpr extends FuncExpr {
  static key = ExpressionKey.COLUMNS;

  static availableArgs = new Set(['this', 'unpack']);

  static argOrder = ['this', 'unpack'];

  declare args: ColumnsExprArgs;

  constructor (args: ColumnsExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ConvertExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    style?: Expression;
    safe?: boolean;
  },
]>;

export class ConvertExpr extends FuncExpr {
  static key = ExpressionKey.CONVERT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'style',
    'safe',
  ]);

  static argOrder = [
    'this',
    'expression',
    'style',
    'safe',
  ];

  declare args: ConvertExprArgs;

  constructor (args: ConvertExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ConvertToCharsetExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    dest?: Expression;
    source?: Expression;
  },
]>;

export class ConvertToCharsetExpr extends FuncExpr {
  static key = ExpressionKey.CONVERT_TO_CHARSET;

  static requiredArgs = new Set(['this', 'dest']);

  static availableArgs = new Set([
    'this',
    'dest',
    'source',
  ]);

  static argOrder = [
    'this',
    'dest',
    'source',
  ];

  declare args: ConvertToCharsetExprArgs;

  constructor (args: ConvertToCharsetExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ConvertTimezoneExprArgs = Merge<[
  FuncExprArgs,
  {
    sourceTz?: string | Expression;
    targetTz?: string | Expression;
    timestamp?: Expression;
    options?: Expression[];
  },
]>;

export class ConvertTimezoneExpr extends FuncExpr {
  static key = ExpressionKey.CONVERT_TIMEZONE;

  static requiredArgs = new Set(['targetTz', 'timestamp']);

  static availableArgs = new Set([
    'sourceTz',
    'targetTz',
    'timestamp',
    'options',
  ]);

  static argOrder = [
    'sourceTz',
    'targetTz',
    'timestamp',
    'options',
  ];

  declare args: ConvertTimezoneExprArgs;

  constructor (args: ConvertTimezoneExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CodePointsToStringExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CodePointsToStringExpr extends FuncExpr {
  static key = ExpressionKey.CODE_POINTS_TO_STRING;

  static argOrder = ['this'];

  declare args: CodePointsToStringExprArgs;

  constructor (args: CodePointsToStringExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type GenerateSeriesExprArgs = Merge<[
  FuncExprArgs,
  {
    start?: Expression;
    end?: Expression;
    step?: Expression;
    isEndExclusive?: Expression;
  },
]>;

export class GenerateSeriesExpr extends FuncExpr {
  static key = ExpressionKey.GENERATE_SERIES;

  static requiredArgs = new Set(['start', 'end']);

  static availableArgs = new Set([
    'start',
    'end',
    'step',
    'isEndExclusive',
  ]);

  static argOrder = [
    'start',
    'end',
    'step',
    'isEndExclusive',
  ];

  declare args: GenerateSeriesExprArgs;

  constructor (args: GenerateSeriesExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/generator
 */
export type GeneratorExprArgs = Merge<[
  UdtfExprArgs,
  FuncExprArgs,
  {
    rowcount?: Expression;
    timeLimit?: Expression;
    alias?: Expression;
  },
]>;

export class GeneratorExpr extends multiInherit(FuncExpr, UdtfExpr) {
  static key = ExpressionKey.GENERATOR;

  static availableArgs = new Set(['rowcount', 'timelimit']);

  declare args: GeneratorExprArgs;

  constructor (args: GeneratorExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AiClassifyExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    categories?: Expression;
    config?: Expression;
  },
]>;

export class AiClassifyExpr extends FuncExpr {
  static key = ExpressionKey.AI_CLASSIFY;

  static _sqlNames = ['AI_CLASSIFY'];

  static requiredArgs = new Set(['this', 'categories']);

  static availableArgs = new Set([
    'this',
    'categories',
    'config',
  ]);

  static argOrder = [
    'this',
    'categories',
    'config',
  ];

  declare args: AiClassifyExprArgs;

  constructor (args: AiClassifyExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayAllExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class ArrayAllExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_ALL;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: ArrayAllExprArgs;

  constructor (args: ArrayAllExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * Represents Python's `any(f(x) for x in array)`, where `array` is `this` and `f` is `expression`
 */
export type ArrayAnyExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class ArrayAnyExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_ANY;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: ArrayAnyExprArgs;

  constructor (args: ArrayAnyExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayAppendExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    nullPropagation?: boolean;
  },
]>;

export class ArrayAppendExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_APPEND;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'nullPropagation',
  ]);

  static argOrder = [
    'this',
    'expression',
    'nullPropagation',
  ];

  declare args: ArrayAppendExprArgs;

  constructor (args: ArrayAppendExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayPrependExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    nullPropagation?: boolean;
  },
]>;

export class ArrayPrependExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_PREPEND;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'nullPropagation',
  ]);

  static argOrder = [
    'this',
    'expression',
    'nullPropagation',
  ];

  declare args: ArrayPrependExprArgs;

  constructor (args: ArrayPrependExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayConcatExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
    nullPropagation?: boolean;
  },
]>;

export class ArrayConcatExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_CONCAT;

  static _sqlNames = ['ARRAY_CONCAT', 'ARRAY_CAT'];

  static isVarLenArgs = true;

  static availableArgs = new Set([
    'this',
    'expressions',
    'nullPropagation',
  ]);

  static argOrder = [
    'this',
    'expressions',
    'nullPropagation',
  ];

  declare args: ArrayConcatExprArgs;

  constructor (args: ArrayConcatExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayCompactExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class ArrayCompactExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_COMPACT;

  static argOrder = ['this'];

  declare args: ArrayCompactExprArgs;

  constructor (args: ArrayCompactExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayInsertExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    position?: Expression;
    expression?: Expression;
    offset?: number;
  },
]>;

export class ArrayInsertExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_INSERT;

  static requiredArgs = new Set([
    'this',
    'position',
    'expression',
  ]);

  static availableArgs = new Set([
    'this',
    'position',
    'expression',
    'offset',
  ]);

  static argOrder = [
    'this',
    'position',
    'expression',
    'offset',
  ];

  declare args: ArrayInsertExprArgs;

  constructor (args: ArrayInsertExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayRemoveAtExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    position?: Expression;
  },
]>;

export class ArrayRemoveAtExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_REMOVE_AT;

  static requiredArgs = new Set(['this', 'position']);

  static availableArgs = new Set(['this', 'position']);

  static argOrder = ['this', 'position'];

  declare args: ArrayRemoveAtExprArgs;

  constructor (args: ArrayRemoveAtExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayConstructCompactExprArgs = Merge<[
  FuncExprArgs,
  { expressions?: Expression[] },
]>;

export class ArrayConstructCompactExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_CONSTRUCT_COMPACT;

  static isVarLenArgs = true;

  static availableArgs = new Set(['expressions']);

  static argOrder = ['expressions'];

  declare args: ArrayConstructCompactExprArgs;

  constructor (args: ArrayConstructCompactExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayContainsExprArgs = Merge<[
  FuncExprArgs,
  BinaryExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    ensureVariant?: boolean | Expression;
  },
]>;

export class ArrayContainsExpr extends multiInherit(BinaryExpr, FuncExpr) {
  static key = ExpressionKey.ARRAY_CONTAINS;

  static _sqlNames = ['ARRAY_CONTAINS', 'ARRAY_HAS'];

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'ensureVariant',
    'checkNull',
  ]);

  declare args: ArrayContainsExprArgs;

  constructor (args: ArrayContainsExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayContainsAllExprArgs = Merge<[
  FuncExprArgs,
  BinaryExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    expressions?: Expression[];
  },
]>;

export class ArrayContainsAllExpr extends multiInherit(BinaryExpr, FuncExpr) {
  static key = ExpressionKey.ARRAY_CONTAINS_ALL;

  static _sqlNames = ['ARRAY_CONTAINS_ALL', 'ARRAY_HAS_ALL'];

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: ArrayContainsAllExprArgs;

  constructor (args: ArrayContainsAllExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayFilterExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class ArrayFilterExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_FILTER;

  static _sqlNames = ['FILTER', 'ARRAY_FILTER'];

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: ArrayFilterExprArgs;

  constructor (args: ArrayFilterExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayFirstExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ArrayFirstExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_FIRST;

  static argOrder = ['this'];

  declare args: ArrayFirstExprArgs;

  constructor (args: ArrayFirstExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayLastExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ArrayLastExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_LAST;

  static argOrder = ['this'];

  declare args: ArrayLastExprArgs;

  constructor (args: ArrayLastExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayReverseExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ArrayReverseExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_REVERSE;

  static argOrder = ['this'];

  declare args: ArrayReverseExprArgs;

  constructor (args: ArrayReverseExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArraySliceExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    start?: Expression;
    end?: Expression;
    step?: Expression;
  },
]>;

export class ArraySliceExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_SLICE;

  static requiredArgs = new Set(['this', 'start']);

  static availableArgs = new Set([
    'this',
    'start',
    'end',
    'step',
  ]);

  static argOrder = [
    'this',
    'start',
    'end',
    'step',
  ];

  declare args: ArraySliceExprArgs;

  constructor (args: ArraySliceExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayToStringExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
    null?: Expression;
  },
]>;

export class ArrayToStringExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_TO_STRING;

  static _sqlNames = ['ARRAY_TO_STRING', 'ARRAY_JOIN'];

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'null',
  ]);

  static argOrder = [
    'this',
    'expression',
    'null',
  ];

  declare args: ArrayToStringExprArgs;

  constructor (args: ArrayToStringExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayIntersectExprArgs = Merge<[
  FuncExprArgs,
  { expressions?: Expression[] },
]>;

export class ArrayIntersectExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_INTERSECT;

  static isVarLenArgs = true;

  static _sqlNames = ['ARRAY_INTERSECT', 'ARRAY_INTERSECTION'];

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  static argOrder = ['expressions'];

  declare args: ArrayIntersectExprArgs;

  constructor (args: ArrayIntersectExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StPointExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    null?: Expression;
  },
]>;

export class StPointExpr extends FuncExpr {
  static key = ExpressionKey.ST_POINT;

  static _sqlNames = ['ST_POINT', 'ST_MAKEPOINT'];

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'null',
  ]);

  static argOrder = [
    'this',
    'expression',
    'null',
  ];

  declare args: StPointExprArgs;

  constructor (args: StPointExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StDistanceExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    useSpheroid?: Expression;
  },
]>;

export class StDistanceExpr extends FuncExpr {
  static key = ExpressionKey.ST_DISTANCE;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'useSpheroid',
  ]);

  static argOrder = [
    'this',
    'expression',
    'useSpheroid',
  ];

  declare args: StDistanceExprArgs;

  constructor (args: StDistanceExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#string
 */
export type StringExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: string;
    zone?: Expression;
  },
]>;

export class StringExpr extends FuncExpr {
  static key = ExpressionKey.STRING;

  static availableArgs = new Set(['this', 'zone']);

  static argOrder = ['this', 'zone'];

  declare args: StringExprArgs;

  constructor (args: StringExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StringToArrayExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
    null?: Expression;
  },
]>;

export class StringToArrayExpr extends FuncExpr {
  static key = ExpressionKey.STRING_TO_ARRAY;

  static _sqlNames = [
    'STRING_TO_ARRAY',
    'SPLIT_BY_STRING',
    'STRTOK_TO_ARRAY',
  ];

  static availableArgs = new Set([
    'this',
    'expression',
    'null',
  ]);

  static argOrder = [
    'this',
    'expression',
    'null',
  ];

  declare args: StringToArrayExprArgs;

  constructor (args: StringToArrayExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayOverlapsExprArgs = Merge<[
  FuncExprArgs,
  BinaryExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    expressions?: Expression[];
  },
]>;

export class ArrayOverlapsExpr extends multiInherit(BinaryExpr, FuncExpr) {
  static key = ExpressionKey.ARRAY_OVERLAPS;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: ArrayOverlapsExprArgs;

  constructor (args: ArrayOverlapsExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArraySizeExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: ExpressionValue;
    expression?: Expression;
  },
]>;

export class ArraySizeExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_SIZE;

  static _sqlNames = ['ARRAY_SIZE', 'ARRAY_LENGTH'];

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: ArraySizeExprArgs;

  constructor (args: ArraySizeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArraySortExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
  },
]>;

export class ArraySortExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_SORT;

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: ArraySortExprArgs;

  constructor (args: ArraySortExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArraySumExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
  },
]>;

export class ArraySumExpr extends FuncExpr {
  static key = ExpressionKey.ARRAY_SUM;

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: ArraySumExprArgs;

  constructor (args: ArraySumExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArraysZipExprArgs = Merge<[
  FuncExprArgs,
  { expressions?: Expression[] },
]>;

export class ArraysZipExpr extends FuncExpr {
  static key = ExpressionKey.ARRAYS_ZIP;

  static isVarLenArgs = true;

  static availableArgs = new Set(['expressions']);

  static argOrder = ['expressions'];

  declare args: ArraysZipExprArgs;

  constructor (args: ArraysZipExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CaseExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    ifs?: Expression[];
    default?: string | Expression;
  },
]>;

export class CaseExpr extends FuncExpr {
  static key = ExpressionKey.CASE;

  static requiredArgs = new Set(['ifs']);
  static availableArgs = new Set([
    'this',
    'ifs',
    'default',
  ]);

  static argOrder = [
    'this',
    'ifs',
    'default',
  ];

  declare args: CaseExprArgs;

  constructor (args: CaseExprArgs = {}) {
    super(args);
  }

  when (
    condition: string | Expression,
    then: string | Expression,
    options: {
      copy?: boolean;
      dialect?: DialectType;
      prefix?: string;
    } = {},
  ): CaseExpr {
    const { copy = true } = options;
    const instance = maybeCopy(this, copy);
    instance.append(
      'ifs',
      new IfExpr({
        this: maybeParse(condition, options),
        true: maybeParse(then, options),
      }),
    );
    return instance;
  }

  else (
    condition: string | Expression,
    options: {
      copy?: boolean;
      dialect?: DialectType;
      prefix?: string;
    } = {},
  ): CaseExpr {
    const { copy = true } = options;
    const instance = maybeCopy(this, copy);
    instance.setArgKey('default', maybeParse(condition, options));
    return instance;
  }

  static {
    this.register();
  }
}

export type CastExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: ExpressionValue;
    to?: string | Expression;
    format?: string;
    safe?: boolean;
    action?: Expression;
    default?: Expression;
  },
]>;

export class CastExpr extends FuncExpr {
  static key = ExpressionKey.CAST;

  static requiredArgs = new Set(['this', 'to']);

  static availableArgs = new Set([
    'this',
    'to',
    'format',
    'safe',
    'action',
    'default',
  ]);

  static argOrder = [
    'this',
    'to',
    'format',
    'safe',
    'action',
    'default',
  ];

  declare args: CastExprArgs;

  constructor (args: CastExprArgs = {}) {
    super(args);
  }

  get name (): string {
    return (this.args.this as Expression).name || '';
  }

  get to (): Expression {
    return this.args.to as Expression;
  }

  get outputName (): string {
    return this.name;
  }

  isType (
    dtypes: DataTypeExprKind | DataTypeExpr | IdentifierExpr | DotExpr | string | Iterable<DataTypeExprKind | DataTypeExpr | IdentifierExpr | DotExpr | string>,
    _options: { checkNullable?: boolean } = {},
  ): boolean {
    const toExpr = this.args.to;
    if (!toExpr) return false;
    if (toExpr instanceof DataTypeExpr) {
      return toExpr.isType(dtypes);
    }
    return false;
  }

  static {
    this.register();
  }
}

export type JustifyDaysExprArgs = Merge<[
  FuncExprArgs,
]>;

export class JustifyDaysExpr extends FuncExpr {
  static key = ExpressionKey.JUSTIFY_DAYS;

  static argOrder = ['this'];

  declare args: JustifyDaysExprArgs;

  constructor (args: JustifyDaysExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JustifyHoursExprArgs = Merge<[
  FuncExprArgs,
]>;

export class JustifyHoursExpr extends FuncExpr {
  static key = ExpressionKey.JUSTIFY_HOURS;

  static argOrder = ['this'];

  declare args: JustifyHoursExprArgs;

  constructor (args: JustifyHoursExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JustifyIntervalExprArgs = Merge<[
  FuncExprArgs,
]>;

export class JustifyIntervalExpr extends FuncExpr {
  static key = ExpressionKey.JUSTIFY_INTERVAL;

  static argOrder = ['this'];

  declare args: JustifyIntervalExprArgs;

  constructor (args: JustifyIntervalExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TryExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TryExpr extends FuncExpr {
  static key = ExpressionKey.TRY;

  static argOrder = ['this'];

  declare args: TryExprArgs;

  constructor (args: TryExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CastToStrTypeExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    to?: Expression;
  },
]>;

export class CastToStrTypeExpr extends FuncExpr {
  static key = ExpressionKey.CAST_TO_STR_TYPE;

  static requiredArgs = new Set(['this', 'to']);

  static availableArgs = new Set(['this', 'to']);

  static argOrder = ['this', 'to'];

  declare args: CastToStrTypeExprArgs;

  constructor (args: CastToStrTypeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CheckJsonExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class CheckJsonExpr extends FuncExpr {
  static key = ExpressionKey.CHECK_JSON;

  static argOrder = ['this'];

  declare args: CheckJsonExprArgs;

  constructor (args: CheckJsonExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CheckXmlExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    disableAutoConvert?: Expression;
  },
]>;

export class CheckXmlExpr extends FuncExpr {
  static key = ExpressionKey.CHECK_XML;

  static availableArgs = new Set(['this', 'disableAutoConvert']);

  static argOrder = ['this', 'disableAutoConvert'];

  declare args: CheckXmlExprArgs;

  constructor (args: CheckXmlExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CollateExprArgs = Merge<[
  FuncExprArgs,
  BinaryExprArgs,
]>;

export class CollateExpr extends multiInherit(BinaryExpr, FuncExpr) {
  static key = ExpressionKey.COLLATE;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: CollateExprArgs;

  constructor (args: CollateExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CollationExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CollationExpr extends FuncExpr {
  static key = ExpressionKey.COLLATION;

  static argOrder = ['this'];

  declare args: CollationExprArgs;

  constructor (args: CollationExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CeilExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    decimals?: Expression;
    to?: Expression;
  },
]>;

export class CeilExpr extends FuncExpr {
  static key = ExpressionKey.CEIL;

  static _sqlNames = ['CEIL', 'CEILING'];

  static availableArgs = new Set([
    'this',
    'decimals',
    'to',
  ]);

  static argOrder = [
    'this',
    'decimals',
    'to',
  ];

  declare args: CeilExprArgs;

  // Auto-register this class when the module loads
  static {
    this.register();
  }

  constructor (args: CeilExprArgs = {}) {
    super(args);
  }
}

export type CoalesceExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expressions?: ExpressionValue[];
    isNvl?: boolean;
    isNull?: boolean;
  },
]>;

export class CoalesceExpr extends FuncExpr {
  static key = ExpressionKey.COALESCE;

  static _sqlNames = [
    'COALESCE',
    'IFNULL',
    'NVL',
  ];

  static isVarLenArgs = true;

  static availableArgs = new Set([
    'this',
    'expressions',
    'isNvl',
    'isNull',
  ]);

  static argOrder = [
    'this',
    'expressions',
    'isNvl',
    'isNull',
  ];

  declare args: CoalesceExprArgs;

  constructor (args: CoalesceExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ChrExprArgs = Merge<[
  FuncExprArgs,
  {
    expressions?: Expression[];
    charset?: string;
  },
]>;

export class ChrExpr extends FuncExpr {
  static key = ExpressionKey.CHR;

  static _sqlNames = ['CHR', 'CHAR'];

  static isVarLenArgs = true;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions', 'charset']);

  static argOrder = ['expressions', 'charset'];

  declare args: ChrExprArgs;

  constructor (args: ChrExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ConcatExprArgs = Merge<[
  FuncExprArgs,
  {
    expressions?: Expression[];
    safe?: boolean;
    coalesce?: boolean;
  },
]>;

export class ConcatExpr extends FuncExpr {
  static key = ExpressionKey.CONCAT;

  static isVarLenArgs = true;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set([
    'expressions',
    'safe',
    'coalesce',
  ]);

  static argOrder = [
    'expressions',
    'safe',
    'coalesce',
  ];

  declare args: ConcatExprArgs;

  constructor (args: ConcatExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ContainsExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    jsonScope?: Expression;
  },
]>;

export class ContainsExpr extends FuncExpr {
  static key = ExpressionKey.CONTAINS;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'jsonScope',
  ]);

  static argOrder = [
    'this',
    'expression',
    'jsonScope',
  ];

  declare args: ContainsExprArgs;

  constructor (args: ContainsExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ConnectByRootExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ConnectByRootExpr extends FuncExpr {
  static key = ExpressionKey.CONNECT_BY_ROOT;

  static argOrder = ['this'];

  declare args: ConnectByRootExprArgs;

  constructor (args: ConnectByRootExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CbrtExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CbrtExpr extends FuncExpr {
  static key = ExpressionKey.CBRT;

  static argOrder = ['this'];

  declare args: CbrtExprArgs;

  constructor (args: CbrtExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentAccountExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentAccountExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_ACCOUNT;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentAccountExprArgs;

  constructor (args: CurrentAccountExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentAccountNameExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentAccountNameExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_ACCOUNT_NAME;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentAccountNameExprArgs;

  constructor (args: CurrentAccountNameExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentAvailableRolesExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentAvailableRolesExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_AVAILABLE_ROLES;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentAvailableRolesExprArgs;

  constructor (args: CurrentAvailableRolesExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentClientExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentClientExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_CLIENT;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentClientExprArgs;

  constructor (args: CurrentClientExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentIpAddressExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentIpAddressExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_IP_ADDRESS;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentIpAddressExprArgs;

  constructor (args: CurrentIpAddressExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentDatabaseExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentDatabaseExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_DATABASE;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentDatabaseExprArgs;

  constructor (args: CurrentDatabaseExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentSchemasExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class CurrentSchemasExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_SCHEMAS;

  static argOrder = ['this'];

  declare args: CurrentSchemasExprArgs;

  constructor (args: CurrentSchemasExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentSecondaryRolesExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentSecondaryRolesExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_SECONDARY_ROLES;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentSecondaryRolesExprArgs;

  constructor (args: CurrentSecondaryRolesExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentSessionExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentSessionExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_SESSION;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentSessionExprArgs;

  constructor (args: CurrentSessionExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentStatementExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentStatementExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_STATEMENT;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentStatementExprArgs;

  constructor (args: CurrentStatementExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentVersionExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentVersionExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_VERSION;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentVersionExprArgs;

  constructor (args: CurrentVersionExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentTransactionExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentTransactionExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_TRANSACTION;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentTransactionExprArgs;

  constructor (args: CurrentTransactionExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentWarehouseExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentWarehouseExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_WAREHOUSE;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentWarehouseExprArgs;

  constructor (args: CurrentWarehouseExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentDateExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class CurrentDateExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_DATE;

  static argOrder = ['this'];

  declare args: CurrentDateExprArgs;

  constructor (args: CurrentDateExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentDatetimeExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class CurrentDatetimeExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_DATETIME;

  static argOrder = ['this'];

  declare args: CurrentDatetimeExprArgs;

  constructor (args: CurrentDatetimeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentTimeExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class CurrentTimeExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_TIME;

  static argOrder = ['this'];

  declare args: CurrentTimeExprArgs;

  constructor (args: CurrentTimeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LocaltimeExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class LocaltimeExpr extends FuncExpr {
  static key = ExpressionKey.LOCALTIME;

  static argOrder = ['this'];

  declare args: LocaltimeExprArgs;

  constructor (args: LocaltimeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LocaltimestampExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class LocaltimestampExpr extends FuncExpr {
  static key = ExpressionKey.LOCALTIMESTAMP;

  static argOrder = ['this'];

  declare args: LocaltimestampExprArgs;

  constructor (args: LocaltimestampExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SystimestampExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class SystimestampExpr extends FuncExpr {
  static key = ExpressionKey.SYSTIMESTAMP;

  static argOrder = ['this'];

  declare args: SystimestampExprArgs;

  constructor (args: SystimestampExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentTimestampExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    sysdate?: boolean | Expression;
  },
]>;

export class CurrentTimestampExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_TIMESTAMP;

  static availableArgs = new Set(['this', 'sysdate']);

  static argOrder = ['this', 'sysdate'];

  declare args: CurrentTimestampExprArgs;

  constructor (args: CurrentTimestampExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentTimestampLtzExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentTimestampLtzExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_TIMESTAMP_LTZ;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentTimestampLtzExprArgs;

  constructor (args: CurrentTimestampLtzExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentTimezoneExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentTimezoneExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_TIMEZONE;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentTimezoneExprArgs;

  constructor (args: CurrentTimezoneExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentOrganizationNameExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentOrganizationNameExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_ORGANIZATION_NAME;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentOrganizationNameExprArgs;

  constructor (args: CurrentOrganizationNameExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentSchemaExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class CurrentSchemaExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_SCHEMA;

  static argOrder = ['this'];

  declare args: CurrentSchemaExprArgs;

  constructor (args: CurrentSchemaExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentUserExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class CurrentUserExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_USER;

  static argOrder = ['this'];

  declare args: CurrentUserExprArgs;

  constructor (args: CurrentUserExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentCatalogExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentCatalogExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_CATALOG;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentCatalogExprArgs;

  constructor (args: CurrentCatalogExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentRegionExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentRegionExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_REGION;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentRegionExprArgs;

  constructor (args: CurrentRegionExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentRoleExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentRoleExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_ROLE;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentRoleExprArgs;

  constructor (args: CurrentRoleExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentRoleTypeExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentRoleTypeExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_ROLE_TYPE;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentRoleTypeExprArgs;

  constructor (args: CurrentRoleTypeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CurrentOrganizationUserExprArgs = Merge<[
  FuncExprArgs,
]>;

export class CurrentOrganizationUserExpr extends FuncExpr {
  static key = ExpressionKey.CURRENT_ORGANIZATION_USER;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: CurrentOrganizationUserExprArgs;

  constructor (args: CurrentOrganizationUserExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SessionUserExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SessionUserExpr extends FuncExpr {
  static key = ExpressionKey.SESSION_USER;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: SessionUserExprArgs;

  constructor (args: SessionUserExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UtcDateExprArgs = Merge<[
  FuncExprArgs,
]>;

export class UtcDateExpr extends FuncExpr {
  static key = ExpressionKey.UTC_DATE;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: UtcDateExprArgs;

  constructor (args: UtcDateExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UtcTimeExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class UtcTimeExpr extends FuncExpr {
  static key = ExpressionKey.UTC_TIME;

  static argOrder = ['this'];

  declare args: UtcTimeExprArgs;

  constructor (args: UtcTimeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UtcTimestampExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class UtcTimestampExpr extends FuncExpr {
  static key = ExpressionKey.UTC_TIMESTAMP;

  static argOrder = ['this'];

  declare args: UtcTimestampExprArgs;

  constructor (args: UtcTimestampExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DateAddExprArgs = Merge<[
  FuncExprArgs,
  IntervalOpExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    unit?: Expression;
    expressions?: ExpressionOrStringList;
  },
]>;

export class DateAddExpr extends multiInherit(FuncExpr, IntervalOpExpr) {
  static key = ExpressionKey.DATE_ADD;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'unit',
  ]);

  declare args: DateAddExprArgs;

  constructor (args: DateAddExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DateBinExprArgs = Merge<[
  FuncExprArgs,
  IntervalOpExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    unit?: Expression;
    zone?: Expression;
    origin?: Expression;
    expressions?: ExpressionOrStringList;
  },
]>;

export class DateBinExpr extends multiInherit(FuncExpr, IntervalOpExpr) {
  static key = ExpressionKey.DATE_BIN;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'unit',
    'zone',
    'origin',
  ]);

  declare args: DateBinExprArgs;

  constructor (args: DateBinExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DateSubExprArgs = Merge<[
  FuncExprArgs,
  IntervalOpExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    unit?: Expression;
    expressions?: ExpressionOrStringList;
  },
]>;

export class DateSubExpr extends multiInherit(FuncExpr, IntervalOpExpr) {
  static key = ExpressionKey.DATE_SUB;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'unit',
  ]);

  declare args: DateSubExprArgs;

  constructor (args: DateSubExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DateDiffExprArgs = Merge<[
  FuncExprArgs,
  TimeUnitExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    unit?: Expression;
    zone?: Expression;
    bigInt?: Expression;
    datePartBoundary?: boolean | Expression;
    expressions?: ExpressionOrStringList;
  },
]>;

export class DateDiffExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  static key = ExpressionKey.DATE_DIFF;

  static _sqlNames = ['DATEDIFF', 'DATE_DIFF'];

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'unit',
    'zone',
    'bigInt',
    'datePartBoundary',
  ]);

  declare args: DateDiffExprArgs;

  constructor (args: DateDiffExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DateTruncExprArgs = Merge<[
  FuncExprArgs,
  {
    unit?: Expression;
    this?: Expression;
    zone?: Expression;
    inputTypePreserved?: Expression;
    unabbreviate?: boolean;
  },
]>;

export class DateTruncExpr extends FuncExpr {
  static key = ExpressionKey.DATE_TRUNC;

  static requiredArgs = new Set(['unit', 'this']);

  static availableArgs = new Set([
    'unit',
    'this',
    'zone',
    'inputTypePreserved',
  ]);

  static argOrder = [
    'unit',
    'this',
    'zone',
    'inputTypePreserved',
  ];

  declare args: DateTruncExprArgs;

  constructor (args: DateTruncExprArgs = {}) {
    const unabbreviate = args.unabbreviate ?? true;
    const unit = args.unit;

    if (
      TimeUnitExpr.isVarLike(unit)
      && !(unit instanceof ColumnExpr && unit.parts.length !== 1)
    ) {
      let unitName = unit.name.toUpperCase();
      if (unabbreviate && unitName in TimeUnitExpr.UNABBREVIATED_UNIT_NAME) {
        unitName = TimeUnitExpr.UNABBREVIATED_UNIT_NAME[unitName];
      }
      args.unit = LiteralExpr.string(unitName);
    }

    delete args.unabbreviate;
    super(args);
  }

  get unit (): Expression | undefined {
    return this.args.unit;
  }

  static {
    this.register();
  }
}

export type DatetimeExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression | undefined;
  },
]>;

export class DatetimeExpr extends FuncExpr {
  static key = ExpressionKey.DATETIME;

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: DatetimeExprArgs;

  constructor (args: DatetimeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DatetimeAddExprArgs = Merge<[
  FuncExprArgs,
  IntervalOpExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    unit?: Expression;
    expressions?: ExpressionOrStringList;
  },
]>;

export class DatetimeAddExpr extends multiInherit(FuncExpr, IntervalOpExpr) {
  static key = ExpressionKey.DATETIME_ADD;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'unit',
  ]);

  declare args: DatetimeAddExprArgs;

  constructor (args: DatetimeAddExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DatetimeSubExprArgs = Merge<[
  FuncExprArgs,
  IntervalOpExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    unit?: Expression;
    expressions?: ExpressionOrStringList;
  },
]>;

export class DatetimeSubExpr extends multiInherit(FuncExpr, IntervalOpExpr) {
  static key = ExpressionKey.DATETIME_SUB;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'unit',
  ]);

  declare args: DatetimeSubExprArgs;

  constructor (args: DatetimeSubExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DatetimeDiffExprArgs = Merge<[
  FuncExprArgs,
  TimeUnitExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    unit?: Expression;
    expressions?: ExpressionOrStringList;
  },
]>;

export class DatetimeDiffExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  static key = ExpressionKey.DATETIME_DIFF;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'unit',
  ]);

  declare args: DatetimeDiffExprArgs;

  constructor (args: DatetimeDiffExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DatetimeTruncExprArgs = Merge<[
  FuncExprArgs,
  TimeUnitExprArgs,
  {
    this?: Expression;
    unit?: Expression;
    zone?: Expression;
    expressions?: ExpressionOrStringList;
  },
]>;

export class DatetimeTruncExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  static key = ExpressionKey.DATETIME_TRUNC;

  static requiredArgs = new Set(['this', 'unit']);

  static availableArgs = new Set([
    'this',
    'unit',
    'zone',
  ]);

  declare args: DatetimeTruncExprArgs;

  constructor (args: DatetimeTruncExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DateFromUnixDateExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DateFromUnixDateExpr extends FuncExpr {
  static key = ExpressionKey.DATE_FROM_UNIX_DATE;

  static argOrder = ['this'];

  declare args: DateFromUnixDateExprArgs;

  constructor (args: DateFromUnixDateExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DayOfWeekExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DayOfWeekExpr extends FuncExpr {
  static key = ExpressionKey.DAY_OF_WEEK;

  static argOrder = ['this'];

  declare args: DayOfWeekExprArgs;

  constructor (args: DayOfWeekExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DayOfWeekIsoExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DayOfWeekIsoExpr extends FuncExpr {
  static key = ExpressionKey.DAY_OF_WEEK_ISO;

  static argOrder = ['this'];

  declare args: DayOfWeekIsoExprArgs;

  constructor (args: DayOfWeekIsoExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DayOfMonthExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DayOfMonthExpr extends FuncExpr {
  static key = ExpressionKey.DAY_OF_MONTH;

  static argOrder = ['this'];

  declare args: DayOfMonthExprArgs;

  constructor (args: DayOfMonthExprArgs = {}) {
    super(args);
  }

  static _sqlNames = ['DAY_OF_MONTH', 'DAYOFMONTH'];

  static {
    this.register();
  }
}

export type DayOfYearExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DayOfYearExpr extends FuncExpr {
  static key = ExpressionKey.DAY_OF_YEAR;

  static argOrder = ['this'];

  declare args: DayOfYearExprArgs;

  constructor (args: DayOfYearExprArgs = {}) {
    super(args);
  }

  static _sqlNames = ['DAY_OF_YEAR', 'DAYOFYEAR'];

  static {
    this.register();
  }
}

export type DaynameExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    abbreviated?: boolean | Expression;
  },
]>;

export class DaynameExpr extends FuncExpr {
  static key = ExpressionKey.DAYNAME;

  static availableArgs = new Set(['this', 'abbreviated']);

  static argOrder = ['this', 'abbreviated'];

  declare args: DaynameExprArgs;

  constructor (args: DaynameExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToDaysExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ToDaysExpr extends FuncExpr {
  static key = ExpressionKey.TO_DAYS;

  static argOrder = ['this'];

  declare args: ToDaysExprArgs;

  constructor (args: ToDaysExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type WeekOfYearExprArgs = Merge<[
  FuncExprArgs,
]>;

export class WeekOfYearExpr extends FuncExpr {
  static key = ExpressionKey.WEEK_OF_YEAR;

  static argOrder = ['this'];

  declare args: WeekOfYearExprArgs;

  constructor (args: WeekOfYearExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type YearOfWeekExprArgs = Merge<[
  FuncExprArgs,
]>;

export class YearOfWeekExpr extends FuncExpr {
  static key = ExpressionKey.YEAR_OF_WEEK;

  static argOrder = ['this'];

  declare args: YearOfWeekExprArgs;

  constructor (args: YearOfWeekExprArgs = {}) {
    super(args);
  }

  static _sqlNames = ['YEAR_OF_WEEK', 'YEAROFWEEK'];

  static {
    this.register();
  }
}

export type YearOfWeekIsoExprArgs = Merge<[
  FuncExprArgs,
]>;

export class YearOfWeekIsoExpr extends FuncExpr {
  static key = ExpressionKey.YEAR_OF_WEEK_ISO;

  static argOrder = ['this'];

  declare args: YearOfWeekIsoExprArgs;

  constructor (args: YearOfWeekIsoExprArgs = {}) {
    super(args);
  }

  static _sqlNames = ['YEAR_OF_WEEK_ISO', 'YEAROFWEEKISO'];

  static {
    this.register();
  }
}

export type MonthsBetweenExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    roundoff?: Expression;
  },
]>;

export class MonthsBetweenExpr extends FuncExpr {
  static key = ExpressionKey.MONTHS_BETWEEN;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'roundoff',
  ]);

  static argOrder = [
    'this',
    'expression',
    'roundoff',
  ];

  declare args: MonthsBetweenExprArgs;

  static {
    this.register();
  }

  constructor (args: MonthsBetweenExprArgs = {}) {
    super(args);
  }
}

export type MakeIntervalExprArgs = Merge<[
  FuncExprArgs,
  {
    year?: Expression;
    month?: Expression;
    week?: Expression;
    day?: Expression;
    hour?: Expression;
    minute?: Expression;
    second?: Expression;
  },
]>;

export class MakeIntervalExpr extends FuncExpr {
  static key = ExpressionKey.MAKE_INTERVAL;

  static availableArgs = new Set([
    'year',
    'month',
    'week',
    'day',
    'hour',
    'minute',
    'second',
  ]);

  static argOrder = [
    'year',
    'month',
    'week',
    'day',
    'hour',
    'minute',
    'second',
  ];

  declare args: MakeIntervalExprArgs;

  constructor (args: MakeIntervalExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LastDayExprArgs = Merge<[
  TimeUnitExprArgs,
  FuncExprArgs,
  {
    this?: Expression;
    unit?: Expression;
    expression?: Expression;
  },
]>;

export class LastDayExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  static key = ExpressionKey.LAST_DAY;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this', 'unit']);

  declare args: LastDayExprArgs;

  constructor (args: LastDayExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type PreviousDayExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class PreviousDayExpr extends FuncExpr {
  static key = ExpressionKey.PREVIOUS_DAY;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: PreviousDayExprArgs;

  constructor (args: PreviousDayExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LaxBoolExprArgs = Merge<[
  FuncExprArgs,
]>;

export class LaxBoolExpr extends FuncExpr {
  static key = ExpressionKey.LAX_BOOL;

  static argOrder = ['this'];

  declare args: LaxBoolExprArgs;

  constructor (args: LaxBoolExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LaxFloat64ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class LaxFloat64Expr extends FuncExpr {
  static key = ExpressionKey.LAX_FLOAT64;

  static argOrder = ['this'];

  declare args: LaxFloat64ExprArgs;

  constructor (args: LaxFloat64ExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LaxInt64ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class LaxInt64Expr extends FuncExpr {
  static key = ExpressionKey.LAX_INT64;

  static argOrder = ['this'];

  declare args: LaxInt64ExprArgs;

  constructor (args: LaxInt64ExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LaxStringExprArgs = Merge<[
  FuncExprArgs,
]>;

export class LaxStringExpr extends FuncExpr {
  static key = ExpressionKey.LAX_STRING;

  static argOrder = ['this'];

  declare args: LaxStringExprArgs;

  constructor (args: LaxStringExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ExtractExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: string | Expression;
    expression?: Expression;
  },
]>;

export class ExtractExpr extends FuncExpr {
  static key = ExpressionKey.EXTRACT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: ExtractExprArgs;

  constructor (args: ExtractExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ExistsExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
  },
]>;
export class ExistsExpr extends multiInherit(FuncExpr, SubqueryPredicateExpr) {
  static key = ExpressionKey.EXISTS;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: ExistsExprArgs;

  constructor (args: ExistsExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type EltExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class EltExpr extends FuncExpr {
  static key = ExpressionKey.ELT;

  static isVarLenArgs = true;
  static requiredArgs = new Set(['this', 'expressions']);

  static availableArgs = new Set(['this', 'expressions']);

  static argOrder = ['this', 'expressions'];

  declare args: EltExprArgs;

  constructor (args: EltExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimestampExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    zone?: Expression;
    withTz?: Expression;
  },
]>;

export class TimestampExpr extends FuncExpr {
  static key = ExpressionKey.TIMESTAMP;

  static availableArgs = new Set([
    'this',
    'zone',
    'withTz',
  ]);

  static argOrder = [
    'this',
    'zone',
    'withTz',
  ];

  declare args: TimestampExprArgs;

  constructor (args: TimestampExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimestampAddExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    unit?: Expression;
  },
]>;

export class TimestampAddExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  static key = ExpressionKey.TIMESTAMP_ADD;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'unit',
  ]);

  declare args: TimestampAddExprArgs;

  constructor (args: TimestampAddExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimestampSubExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    unit?: Expression;
  },
]>;

export class TimestampSubExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  static key = ExpressionKey.TIMESTAMP_SUB;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'unit',
  ]);

  declare args: TimestampSubExprArgs;

  constructor (args: TimestampSubExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimestampDiffExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    unit?: Expression;
  },
]>;

export class TimestampDiffExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  static key = ExpressionKey.TIMESTAMP_DIFF;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'unit',
  ]);

  declare args: TimestampDiffExprArgs;

  constructor (args: TimestampDiffExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimestampTruncExprArgs = Merge<[
  TimeUnitExprArgs,
  FuncExprArgs,
  {
    this?: Expression;
    unit?: Expression;
    zone?: Expression;
    inputTypePreserved?: Expression;
    expression?: Expression;
  },
]>;

export class TimestampTruncExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  static key = ExpressionKey.TIMESTAMP_TRUNC;

  static requiredArgs = new Set(['this', 'unit']);

  static availableArgs = new Set([
    'this',
    'unit',
    'zone',
    'inputTypePreserved',
  ]);

  declare args: TimestampTruncExprArgs;

  constructor (args: TimestampTruncExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * Valid kind values for time slice expressions
 */
export type TimeSliceExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    unit?: Expression;
    kind?: TimeSliceExprKind;
  },
]>;

export class TimeSliceExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  static key = ExpressionKey.TIME_SLICE;

  static requiredArgs = new Set([
    'this',
    'expression',
    'unit',
  ]);

  static availableArgs = new Set([
    'this',
    'expression',
    'unit',
    'kind',
  ]);

  declare args: TimeSliceExprArgs;

  constructor (args: TimeSliceExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeAddExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    unit?: Expression;
  },
]>;

export class TimeAddExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  static key = ExpressionKey.TIME_ADD;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'unit',
  ]);

  declare args: TimeAddExprArgs;

  constructor (args: TimeAddExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeSubExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    unit?: Expression;
  },
]>;

export class TimeSubExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  static key = ExpressionKey.TIME_SUB;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'unit',
  ]);

  declare args: TimeSubExprArgs;

  constructor (args: TimeSubExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeDiffExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    unit?: Expression;
  },
]>;

export class TimeDiffExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  static key = ExpressionKey.TIME_DIFF;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'unit',
  ]);

  declare args: TimeDiffExprArgs;

  constructor (args: TimeDiffExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeTruncExprArgs = Merge<[
  TimeUnitExprArgs,
  FuncExprArgs,
  {
    this?: Expression;
    unit?: Expression;
    zone?: Expression;
    expression?: Expression;
  },
]>;

export class TimeTruncExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  static key = ExpressionKey.TIME_TRUNC;

  static requiredArgs = new Set(['this', 'unit']);

  static availableArgs = new Set([
    'this',
    'unit',
    'zone',
  ]);

  declare args: TimeTruncExprArgs;

  constructor (args: TimeTruncExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DateFromPartsExprArgs = Merge<[
  FuncExprArgs,
  {
    year?: Expression;
    month?: Expression;
    day?: Expression;
    allowOverflow?: boolean | Expression;
  },
]>;

export class DateFromPartsExpr extends FuncExpr {
  static key = ExpressionKey.DATE_FROM_PARTS;

  static requiredArgs = new Set(['year']);
  static availableArgs = new Set([
    'year',
    'month',
    'day',
    'allowOverflow',
  ]);

  static argOrder = [
    'year',
    'month',
    'day',
    'allowOverflow',
  ];

  declare args: DateFromPartsExprArgs;

  constructor (args: DateFromPartsExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeFromPartsExprArgs = Merge<[
  FuncExprArgs,
  {
    hour?: Expression;
    min?: Expression;
    sec?: Expression;
    nano?: Expression;
    fractions?: Expression[];
    precision?: number | Expression;
    overflow?: boolean | Expression;
  },
]>;

export class TimeFromPartsExpr extends FuncExpr {
  static key = ExpressionKey.TIME_FROM_PARTS;

  static requiredArgs = new Set([
    'hour',
    'min',
    'sec',
  ]);

  static availableArgs = new Set([
    'hour',
    'min',
    'sec',
    'nano',
    'fractions',
    'precision',
    'overflow',
  ]);

  static argOrder = [
    'hour',
    'min',
    'sec',
    'nano',
    'fractions',
    'precision',
    'overflow',
  ];

  declare args: TimeFromPartsExprArgs;

  constructor (args: TimeFromPartsExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DateStrToDateExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DateStrToDateExpr extends FuncExpr {
  static key = ExpressionKey.DATE_STR_TO_DATE;

  static argOrder = ['this'];

  declare args: DateStrToDateExprArgs;

  constructor (args: DateStrToDateExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DateToDateStrExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DateToDateStrExpr extends FuncExpr {
  static key = ExpressionKey.DATE_TO_DATE_STR;

  static argOrder = ['this'];

  declare args: DateToDateStrExprArgs;

  constructor (args: DateToDateStrExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DateToDiExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DateToDiExpr extends FuncExpr {
  static key = ExpressionKey.DATE_TO_DI;

  static argOrder = ['this'];

  declare args: DateToDiExprArgs;

  constructor (args: DateToDiExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DateExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
    zone?: Expression;
  },
]>;

export class DateExpr extends FuncExpr {
  static key = ExpressionKey.DATE;

  static isVarLenArgs = true;

  static availableArgs = new Set([
    'this',
    'zone',
    'expressions',
  ]);

  static argOrder = [
    'this',
    'expressions',
    'zone',
  ];

  declare args: DateExprArgs;

  constructor (args: DateExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DayExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DayExpr extends FuncExpr {
  static key = ExpressionKey.DAY;

  static argOrder = ['this'];

  declare args: DayExprArgs;

  constructor (args: DayExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DecodeExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    charset?: Expression;
    replace?: boolean;
  },
]>;

export class DecodeExpr extends FuncExpr {
  static key = ExpressionKey.DECODE;

  static requiredArgs = new Set(['this', 'charset']);

  static availableArgs = new Set([
    'this',
    'charset',
    'replace',
  ]);

  static argOrder = [
    'this',
    'charset',
    'replace',
  ];

  declare args: DecodeExprArgs;

  constructor (args: DecodeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DecodeCaseExprArgs = Merge<[
  FuncExprArgs,
  { expressions?: Expression[] },
]>;

export class DecodeCaseExpr extends FuncExpr {
  static key = ExpressionKey.DECODE_CASE;

  static isVarLenArgs = true;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  static argOrder = ['expressions'];

  declare args: DecodeCaseExprArgs;

  constructor (args: DecodeCaseExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DecryptExprArgs = Merge<[
  FuncExprArgs,
  {
    passphrase?: Expression;
    aad?: Expression;
    encryptionMethod?: string | Expression;
    safe?: boolean;
  },
]>;

export class DecryptExpr extends FuncExpr {
  static key = ExpressionKey.DECRYPT;

  static requiredArgs = new Set(['this', 'passphrase']);
  static availableArgs = new Set([
    'this',
    'passphrase',
    'aad',
    'encryptionMethod',
    'safe',
  ]);

  static argOrder = [
    'passphrase',
    'aad',
    'encryptionMethod',
    'safe',
  ];

  declare args: DecryptExprArgs;

  constructor (args: DecryptExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DecryptRawExprArgs = Merge<[
  FuncExprArgs,
  {
    key?: ExpressionValue;
    iv?: Expression;
    aad?: Expression;
    encryptionMethod?: string | Expression;
    aead?: Expression;
    safe?: boolean;
  },
]>;

export class DecryptRawExpr extends FuncExpr {
  static key = ExpressionKey.DECRYPT_RAW;

  static requiredArgs = new Set([
    'this',
    'key',
    'iv',
  ]);

  static availableArgs = new Set([
    'this',
    'key',
    'iv',
    'aad',
    'encryptionMethod',
    'aead',
    'safe',
  ]);

  static argOrder = [
    'key',
    'iv',
    'aad',
    'encryptionMethod',
    'aead',
    'safe',
  ];

  declare args: DecryptRawExprArgs;

  constructor (args: DecryptRawExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DiToDateExprArgs = Merge<[
  FuncExprArgs,
]>;

export class DiToDateExpr extends FuncExpr {
  static key = ExpressionKey.DI_TO_DATE;

  static argOrder = ['this'];

  declare args: DiToDateExprArgs;

  constructor (args: DiToDateExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type EncodeExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    charset?: Expression;
  },
]>;

export class EncodeExpr extends FuncExpr {
  static key = ExpressionKey.ENCODE;

  static requiredArgs = new Set(['this', 'charset']);

  static availableArgs = new Set(['this', 'charset']);

  static argOrder = ['this', 'charset'];

  declare args: EncodeExprArgs;

  constructor (args: EncodeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type EncryptExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    passphrase?: Expression;
    aad?: Expression;
    encryptionMethod?: string;
  },
]>;

export class EncryptExpr extends FuncExpr {
  static key = ExpressionKey.ENCRYPT;

  static requiredArgs = new Set(['this', 'passphrase']);

  static availableArgs = new Set([
    'this',
    'passphrase',
    'aad',
    'encryptionMethod',
  ]);

  static argOrder = [
    'this',
    'passphrase',
    'aad',
    'encryptionMethod',
  ];

  declare args: EncryptExprArgs;

  constructor (args: EncryptExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type EncryptRawExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    key?: unknown;
    iv?: Expression;
    aad?: Expression;
    encryptionMethod?: string;
  },
]>;

export class EncryptRawExpr extends FuncExpr {
  static key = ExpressionKey.ENCRYPT_RAW;

  static requiredArgs = new Set([
    'this',
    'key',
    'iv',
  ]);

  static availableArgs = new Set([
    'this',
    'key',
    'iv',
    'aad',
    'encryptionMethod',
  ]);

  static argOrder = [
    'this',
    'key',
    'iv',
    'aad',
    'encryptionMethod',
  ];

  declare args: EncryptRawExprArgs;

  constructor (args: EncryptRawExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type EqualNullExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class EqualNullExpr extends FuncExpr {
  static key = ExpressionKey.EqUAL_NULL;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: EqualNullExprArgs;

  constructor (args: EqualNullExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ExpExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ExpExpr extends FuncExpr {
  static key = ExpressionKey.EXP;

  static argOrder = ['this'];

  declare args: ExpExprArgs;

  constructor (args: ExpExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type FactorialExprArgs = Merge<[
  FuncExprArgs,
]>;

export class FactorialExpr extends FuncExpr {
  static key = ExpressionKey.FACTORIAL;

  static argOrder = ['this'];

  declare args: FactorialExprArgs;

  constructor (args: FactorialExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ExplodeExprArgs = Merge<[
  FuncExprArgs,
  UdtfExprArgs,
  {
    expression?: number | ExpressionOrString;
    this?: number | ExpressionOrString;
    expressions?: Expression[];
  },
]>;

export class ExplodeExpr extends multiInherit(FuncExpr, UdtfExpr) {
  static key = ExpressionKey.EXPLODE;

  static isVarLenArgs = true;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this', 'expressions']);

  declare args: ExplodeExprArgs;

  constructor (args: ExplodeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type InlineExprArgs = Merge<[
  FuncExprArgs,
]>;

export class InlineExpr extends FuncExpr {
  static key = ExpressionKey.INLINE;

  static argOrder = ['this'];

  declare args: InlineExprArgs;

  constructor (args: InlineExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnnestExprArgs = Merge<[
  FuncExprArgs,
  UdtfExprArgs,
  {
    this?: ExpressionValue;
    expression?: ExpressionValue;
    expressions?: ExpressionValue[];
    alias?: Expression;
    offset?: boolean | Expression;
    explodeArray?: Expression;
  },
]>;

export class UnnestExpr extends multiInherit(FuncExpr, UdtfExpr) {
  static key = ExpressionKey.UNNEST;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set([
    'expressions',
    'alias',
    'offset',
    'explodeArray',
  ]);

  declare args: UnnestExprArgs;

  constructor (args: UnnestExprArgs = {}) {
    super(args);
  }

  get selects (): Expression[] {
    const columns = super.selects;
    const offset = this.args.offset;
    if (offset) {
      const offsetCol = offset === true ? toIdentifier('offset') : offset;
      return [...columns, offsetCol];
    }
    return columns;
  }

  static {
    this.register();
  }
}

export type FloorExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    decimals?: Expression;
    to?: Expression;
  },
]>;

export class FloorExpr extends FuncExpr {
  static key = ExpressionKey.FLOOR;

  static availableArgs = new Set([
    'this',
    'decimals',
    'to',
  ]);

  static argOrder = [
    'this',
    'decimals',
    'to',
  ];

  declare args: FloorExprArgs;

  constructor (args: FloorExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type FromBase32ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class FromBase32Expr extends FuncExpr {
  static key = ExpressionKey.FROM_BASE32;

  static argOrder = ['this'];

  declare args: FromBase32ExprArgs;

  constructor (args: FromBase32ExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type FromBase64ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class FromBase64Expr extends FuncExpr {
  static key = ExpressionKey.FROM_BASE64;

  static argOrder = ['this'];

  declare args: FromBase64ExprArgs;

  constructor (args: FromBase64ExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToBase32ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ToBase32Expr extends FuncExpr {
  static key = ExpressionKey.TO_BASE32;

  static argOrder = ['this'];

  declare args: ToBase32ExprArgs;

  constructor (args: ToBase32ExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToBase64ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ToBase64Expr extends FuncExpr {
  static key = ExpressionKey.TO_BASE64;

  static argOrder = ['this'];

  declare args: ToBase64ExprArgs;

  constructor (args: ToBase64ExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ToBinaryExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    format?: string | Expression;
    safe?: boolean;
  },
]>;

export class ToBinaryExpr extends FuncExpr {
  static key = ExpressionKey.TO_BINARY;

  static availableArgs = new Set([
    'this',
    'format',
    'safe',
  ]);

  static argOrder = [
    'this',
    'format',
    'safe',
  ];

  declare args: ToBinaryExprArgs;

  constructor (args: ToBinaryExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Base64DecodeBinaryExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    alphabet?: Expression;
  },
]>;

export class Base64DecodeBinaryExpr extends FuncExpr {
  static key = ExpressionKey.BASE64_DECODE_BINARY;

  static availableArgs = new Set(['this', 'alphabet']);

  static argOrder = ['this', 'alphabet'];

  declare args: Base64DecodeBinaryExprArgs;

  constructor (args: Base64DecodeBinaryExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Base64DecodeStringExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    alphabet?: Expression;
  },
]>;

export class Base64DecodeStringExpr extends FuncExpr {
  static key = ExpressionKey.BASE64_DECODE_STRING;

  static availableArgs = new Set(['this', 'alphabet']);

  static argOrder = ['this', 'alphabet'];

  declare args: Base64DecodeStringExprArgs;

  constructor (args: Base64DecodeStringExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Base64EncodeExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    maxLineLength?: number | Expression;
    alphabet?: Expression;
  },
]>;

export class Base64EncodeExpr extends FuncExpr {
  static key = ExpressionKey.BASE64_ENCODE;

  static availableArgs = new Set([
    'this',
    'maxLineLength',
    'alphabet',
  ]);

  static argOrder = [
    'this',
    'maxLineLength',
    'alphabet',
  ];

  declare args: Base64EncodeExprArgs;

  constructor (args: Base64EncodeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TryBase64DecodeBinaryExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    alphabet?: Expression;
  },
]>;

export class TryBase64DecodeBinaryExpr extends FuncExpr {
  static key = ExpressionKey.TRY_BASE64_DECODE_BINARY;

  static availableArgs = new Set(['this', 'alphabet']);

  static argOrder = ['this', 'alphabet'];

  declare args: TryBase64DecodeBinaryExprArgs;

  constructor (args: TryBase64DecodeBinaryExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TryBase64DecodeStringExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    alphabet?: Expression;
  },
]>;

export class TryBase64DecodeStringExpr extends FuncExpr {
  static key = ExpressionKey.TRY_BASE64_DECODE_STRING;

  static availableArgs = new Set(['this', 'alphabet']);

  static argOrder = ['this', 'alphabet'];

  declare args: TryBase64DecodeStringExprArgs;

  constructor (args: TryBase64DecodeStringExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TryHexDecodeBinaryExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TryHexDecodeBinaryExpr extends FuncExpr {
  static key = ExpressionKey.TRY_HEX_DECODE_BINARY;

  static argOrder = ['this'];

  declare args: TryHexDecodeBinaryExprArgs;

  constructor (args: TryHexDecodeBinaryExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TryHexDecodeStringExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TryHexDecodeStringExpr extends FuncExpr {
  static key = ExpressionKey.TRY_HEX_DECODE_STRING;

  static argOrder = ['this'];

  declare args: TryHexDecodeStringExprArgs;

  constructor (args: TryHexDecodeStringExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type FromIso8601TimestampExprArgs = Merge<[
  FuncExprArgs,
]>;

export class FromIso8601TimestampExpr extends FuncExpr {
  static key = ExpressionKey.FROM_ISO8601_TIMESTAMP;

  static argOrder = ['this'];

  declare args: FromIso8601TimestampExprArgs;

  constructor (args: FromIso8601TimestampExprArgs = {}) {
    super(args);
  }

  static _sqlNames = ['FROM_ISO8601_TIMESTAMP'];

  static {
    this.register();
  }
}

export type GapFillExprArgs = Merge<[
  FuncExprArgs,
  {
    tsColumn?: Expression;
    bucketWidth?: Expression;
    partitioningColumns?: Expression[];
    valueColumns?: Expression[];
    origin?: Expression;
    ignoreNulls?: Expression[];
  },
]>;

export class GapFillExpr extends FuncExpr {
  static key = ExpressionKey.GAP_FILL;

  static requiredArgs = new Set([
    'this',
    'tsColumn',
    'bucketWidth',
  ]);

  static availableArgs = new Set([
    'this',
    'tsColumn',
    'bucketWidth',
    'partitioningColumns',
    'valueColumns',
    'origin',
    'ignoreNulls',
  ]);

  static argOrder = [
    'tsColumn',
    'bucketWidth',
    'partitioningColumns',
    'valueColumns',
    'origin',
    'ignoreNulls',
  ];

  declare args: GapFillExprArgs;

  static {
    this.register();
  }

  constructor (args: GapFillExprArgs = {}) {
    super(args);
  }
}

export type GenerateDateArrayExprArgs = Merge<[
  FuncExprArgs,
  {
    start?: Expression;
    end?: Expression;
    step?: Expression;
  },
]>;

export class GenerateDateArrayExpr extends FuncExpr {
  static key = ExpressionKey.GENERATE_DATE_ARRAY;

  static requiredArgs = new Set(['start', 'end']);

  static availableArgs = new Set([
    'start',
    'end',
    'step',
  ]);

  static argOrder = [
    'start',
    'end',
    'step',
  ];

  declare args: GenerateDateArrayExprArgs;

  constructor (args: GenerateDateArrayExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type GenerateTimestampArrayExprArgs = Merge<[
  FuncExprArgs,
  {
    start?: Expression;
    end?: Expression;
    step?: Expression;
  },
]>;

export class GenerateTimestampArrayExpr extends FuncExpr {
  static key = ExpressionKey.GENERATE_TIMESTAMP_ARRAY;

  static requiredArgs = new Set([
    'start',
    'end',
    'step',
  ]);

  static availableArgs = new Set([
    'start',
    'end',
    'step',
  ]);

  static argOrder = [
    'start',
    'end',
    'step',
  ];

  declare args: GenerateTimestampArrayExprArgs;

  constructor (args: GenerateTimestampArrayExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type GetExtractExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class GetExtractExpr extends FuncExpr {
  static key = ExpressionKey.GET_EXTRACT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: GetExtractExprArgs;

  constructor (args: GetExtractExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type GetbitExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    zeroIsMsb?: boolean;
  },
]>;

export class GetbitExpr extends FuncExpr {
  static key = ExpressionKey.GETBIT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'zeroIsMsb',
  ]);

  static argOrder = [
    'this',
    'expression',
    'zeroIsMsb',
  ];

  declare args: GetbitExprArgs;

  constructor (args: GetbitExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type GreatestExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
    ignoreNulls?: boolean;
  },
]>;

export class GreatestExpr extends FuncExpr {
  static key = ExpressionKey.GREATEST;

  static isVarLenArgs = true;

  static requiredArgs = new Set(['this', 'ignoreNulls']);

  static availableArgs = new Set([
    'this',
    'expressions',
    'ignoreNulls',
  ]);

  static argOrder = [
    'this',
    'expressions',
    'ignoreNulls',
  ];

  declare args: GreatestExprArgs;

  constructor (args: GreatestExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type HexExprArgs = Merge<[
  FuncExprArgs,
]>;

export class HexExpr extends FuncExpr {
  static key = ExpressionKey.HEX;

  static argOrder = ['this'];

  declare args: HexExprArgs;

  constructor (args: HexExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type HexDecodeStringExprArgs = Merge<[
  FuncExprArgs,
]>;

export class HexDecodeStringExpr extends FuncExpr {
  static key = ExpressionKey.HEX_DECODE_STRING;

  static argOrder = ['this'];

  declare args: HexDecodeStringExprArgs;

  constructor (args: HexDecodeStringExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type HexEncodeExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    case?: Expression;
  },
]>;

export class HexEncodeExpr extends FuncExpr {
  static key = ExpressionKey.HEX_ENCODE;

  static availableArgs = new Set(['this', 'case']);

  static argOrder = ['this', 'case'];

  declare args: HexEncodeExprArgs;

  constructor (args: HexEncodeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type HourExprArgs = Merge<[
  FuncExprArgs,
]>;

export class HourExpr extends FuncExpr {
  static key = ExpressionKey.HOUR;

  static argOrder = ['this'];

  declare args: HourExprArgs;

  constructor (args: HourExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MinuteExprArgs = Merge<[
  FuncExprArgs,
]>;

export class MinuteExpr extends FuncExpr {
  static key = ExpressionKey.MINUTE;

  static argOrder = ['this'];

  declare args: MinuteExprArgs;

  constructor (args: MinuteExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SecondExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SecondExpr extends FuncExpr {
  static key = ExpressionKey.SECOND;

  static argOrder = ['this'];

  declare args: SecondExprArgs;

  constructor (args: SecondExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CompressExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    method?: string;
  },
]>;

export class CompressExpr extends FuncExpr {
  static key = ExpressionKey.COMPRESS;

  static availableArgs = new Set(['this', 'method']);

  static argOrder = ['this', 'method'];

  declare args: CompressExprArgs;

  constructor (args: CompressExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DecompressBinaryExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    method?: string;
  },
]>;

export class DecompressBinaryExpr extends FuncExpr {
  static key = ExpressionKey.DECOMPRESS_BINARY;

  static requiredArgs = new Set(['this', 'method']);

  static availableArgs = new Set(['this', 'method']);

  static argOrder = ['this', 'method'];

  declare args: DecompressBinaryExprArgs;

  constructor (args: DecompressBinaryExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type DecompressStringExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    method?: string;
  },
]>;

export class DecompressStringExpr extends FuncExpr {
  static key = ExpressionKey.DECOMPRESS_STRING;

  static requiredArgs = new Set(['this', 'method']);

  static availableArgs = new Set(['this', 'method']);

  static argOrder = ['this', 'method'];

  declare args: DecompressStringExprArgs;

  constructor (args: DecompressStringExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type IfExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: ExpressionValue;
    true?: ExpressionOrString;
    false?: ExpressionOrString;
  },
]>;

export class IfExpr extends FuncExpr {
  static key = ExpressionKey.IF;

  static _sqlNames = ['IF', 'IIF'];

  static requiredArgs = new Set(['this', 'true']);

  static availableArgs = new Set([
    'this',
    'true',
    'false',
  ]);

  static argOrder = [
    'this',
    'true',
    'false',
  ];

  declare args: IfExprArgs;

  constructor (args: IfExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type NullifExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class NullifExpr extends FuncExpr {
  static key = ExpressionKey.NULLIF;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: NullifExprArgs;

  constructor (args: NullifExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type InitcapExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class InitcapExpr extends FuncExpr {
  static key = ExpressionKey.INITCAP;

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: InitcapExprArgs;

  constructor (args: InitcapExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type IsAsciiExprArgs = Merge<[
  FuncExprArgs,
]>;

export class IsAsciiExpr extends FuncExpr {
  static key = ExpressionKey.IS_ASCII;

  static argOrder = ['this'];

  declare args: IsAsciiExprArgs;

  constructor (args: IsAsciiExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type IsNanExprArgs = Merge<[
  FuncExprArgs,
]>;

export class IsNanExpr extends FuncExpr {
  static key = ExpressionKey.IS_NAN;

  static argOrder = ['this'];

  declare args: IsNanExprArgs;

  constructor (args: IsNanExprArgs = {}) {
    super(args);
  }

  static _sqlNames = ['IS_NAN', 'ISNAN'];

  static {
    this.register();
  }
}

export type Int64ExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class Int64Expr extends FuncExpr {
  static key = ExpressionKey.INT64;

  static argOrder = ['this'];

  declare args: Int64ExprArgs;

  constructor (args: Int64ExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type IsInfExprArgs = Merge<[
  FuncExprArgs,
]>;

export class IsInfExpr extends FuncExpr {
  static key = ExpressionKey.IS_INF;

  static argOrder = ['this'];

  declare args: IsInfExprArgs;

  constructor (args: IsInfExprArgs = {}) {
    super(args);
  }

  static _sqlNames = ['IS_INF', 'ISINF'];

  static {
    this.register();
  }
}

export type IsNullValueExprArgs = Merge<[
  FuncExprArgs,
]>;

export class IsNullValueExpr extends FuncExpr {
  static key = ExpressionKey.IS_NULL_VALUE;

  static argOrder = ['this'];

  declare args: IsNullValueExprArgs;

  constructor (args: IsNullValueExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type IsArrayExprArgs = Merge<[
  FuncExprArgs,
]>;

export class IsArrayExpr extends FuncExpr {
  static key = ExpressionKey.IS_ARRAY;

  static argOrder = ['this'];

  declare args: IsArrayExprArgs;

  constructor (args: IsArrayExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type FormatExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class FormatExpr extends FuncExpr {
  static key = ExpressionKey.FORMAT;

  static isVarLenArgs = true;

  static availableArgs = new Set(['this', 'expressions']);

  static argOrder = ['this', 'expressions'];

  declare args: FormatExprArgs;

  constructor (args: FormatExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JsonKeysExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
    expressions?: Expression[];
  },
]>;

export class JsonKeysExpr extends FuncExpr {
  static key = ExpressionKey.JSON_KEYS;

  static isVarLenArgs = true;

  static availableArgs = new Set([
    'this',
    'expression',
    'expressions',
  ]);

  static argOrder = [
    'this',
    'expression',
    'expressions',
  ];

  declare args: JsonKeysExprArgs;

  constructor (args: JsonKeysExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JsonKeysAtDepthExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
    mode?: Expression;
  },
]>;

export class JsonKeysAtDepthExpr extends FuncExpr {
  static key = ExpressionKey.JSON_KEYS_AT_DEPTH;

  static availableArgs = new Set([
    'this',
    'expression',
    'mode',
  ]);

  static argOrder = [
    'this',
    'expression',
    'mode',
  ];

  declare args: JsonKeysAtDepthExprArgs;

  constructor (args: JsonKeysAtDepthExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JsonObjectExprArgs = Merge<[
  FuncExprArgs,
  {
    nullHandling?: Expression;
    uniqueKeys?: Expression[];
    returnType?: Expression;
    encoding?: Expression;
  },
]>;

export class JsonObjectExpr extends FuncExpr {
  static key = ExpressionKey.JSON_OBJECT;

  static availableArgs = new Set([
    'expressions',
    'nullHandling',
    'uniqueKeys',
    'returnType',
    'encoding',
  ]);

  static argOrder = [
    'nullHandling',
    'uniqueKeys',
    'returnType',
    'encoding',
  ];

  declare args: JsonObjectExprArgs;

  constructor (args: JsonObjectExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JsonArrayExprArgs = Merge<[
  FuncExprArgs,
  {
    nullHandling?: Expression;
    returnType?: Expression;
    strict?: Expression;
  },
]>;

export class JsonArrayExpr extends FuncExpr {
  static key = ExpressionKey.JSON_ARRAY;

  static availableArgs = new Set([
    'expressions',
    'nullHandling',
    'returnType',
    'strict',
  ]);

  static argOrder = [
    'nullHandling',
    'returnType',
    'strict',
  ];

  declare args: JsonArrayExprArgs;

  constructor (args: JsonArrayExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JsonExistsExprArgs = Merge<[
  FuncExprArgs,
  {
    path?: Expression;
    passing?: Expression;
    onCondition?: Expression;
    fromDcolonqmark?: Expression;
  },
]>;

export class JsonExistsExpr extends FuncExpr {
  static key = ExpressionKey.JSON_EXISTS;

  static requiredArgs = new Set(['this', 'path']);
  static availableArgs = new Set([
    'this',
    'path',
    'passing',
    'onCondition',
    'fromDcolonqmark',
  ]);

  static argOrder = [
    'path',
    'passing',
    'onCondition',
    'fromDcolonqmark',
  ];

  declare args: JsonExistsExprArgs;

  constructor (args: JsonExistsExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JsonSetExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class JsonSetExpr extends FuncExpr {
  static key = ExpressionKey.JSON_SET;

  static isVarLenArgs = true;
  static _sqlNames = ['JSON_SET'];

  static requiredArgs = new Set(['this', 'expressions']);

  static availableArgs = new Set(['this', 'expressions']);

  static argOrder = ['this', 'expressions'];

  declare args: JsonSetExprArgs;

  constructor (args: JsonSetExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JsonStripNullsExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
    includeArrays?: Expression;
    removeEmpty?: Expression;
  },
]>;

export class JsonStripNullsExpr extends FuncExpr {
  static key = ExpressionKey.JSON_STRIP_NULLS;

  static _sqlNames = ['JSON_STRIP_NULLS'];

  static availableArgs = new Set([
    'this',
    'expression',
    'includeArrays',
    'removeEmpty',
  ]);

  static argOrder = [
    'this',
    'expression',
    'includeArrays',
    'removeEmpty',
  ];

  declare args: JsonStripNullsExprArgs;

  constructor (args: JsonStripNullsExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JsonValueArrayExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
  },
]>;

export class JsonValueArrayExpr extends FuncExpr {
  static key = ExpressionKey.JSON_VALUE_ARRAY;

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: JsonValueArrayExprArgs;

  constructor (args: JsonValueArrayExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JsonRemoveExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class JsonRemoveExpr extends FuncExpr {
  static key = ExpressionKey.JSON_REMOVE;

  static isVarLenArgs = true;
  static _sqlNames = ['JSON_REMOVE'];

  static requiredArgs = new Set(['this', 'expressions']);

  static availableArgs = new Set(['this', 'expressions']);

  static argOrder = ['this', 'expressions'];

  declare args: JsonRemoveExprArgs;

  constructor (args: JsonRemoveExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JsonTableExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    schema?: Expression;
    path?: ExpressionOrString;
    errorHandling?: ExpressionOrString;
    emptyHandling?: ExpressionOrString;
  },
]>;

export class JsonTableExpr extends FuncExpr {
  static key = ExpressionKey.JSON_TABLE;

  static requiredArgs = new Set(['this', 'schema']);

  static availableArgs = new Set([
    'this',
    'schema',
    'path',
    'errorHandling',
    'emptyHandling',
  ]);

  static argOrder = [
    'this',
    'schema',
    'path',
    'errorHandling',
    'emptyHandling',
  ];

  declare args: JsonTableExprArgs;

  constructor (args: JsonTableExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JsonTypeExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
  },
]>;

export class JsonTypeExpr extends FuncExpr {
  static key = ExpressionKey.JSON_TYPE;

  static _sqlNames = ['JSON_TYPE'];

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: JsonTypeExprArgs;

  constructor (args: JsonTypeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ObjectInsertExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    key?: Expression;
    value?: Expression;
    updateFlag?: Expression;
  },
]>;

export class ObjectInsertExpr extends FuncExpr {
  static key = ExpressionKey.OBJECT_INSERT;

  static requiredArgs = new Set([
    'this',
    'key',
    'value',
  ]);

  static availableArgs = new Set([
    'this',
    'key',
    'value',
    'updateFlag',
  ]);

  static argOrder = [
    'this',
    'key',
    'value',
    'updateFlag',
  ];

  declare args: ObjectInsertExprArgs;

  constructor (args: ObjectInsertExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type OpenJsonExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    path?: Expression;
    expressions?: Expression[];
  },
]>;

export class OpenJsonExpr extends FuncExpr {
  static key = ExpressionKey.OPEN_JSON;

  static availableArgs = new Set([
    'this',
    'path',
    'expressions',
  ]);

  static argOrder = [
    'this',
    'path',
    'expressions',
  ];

  declare args: OpenJsonExprArgs;

  constructor (args: OpenJsonExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JsonbContainsExprArgs = Merge<[
  FuncExprArgs,
  BinaryExprArgs,
]>;

export class JsonbContainsExpr extends multiInherit(BinaryExpr, FuncExpr) {
  static key = ExpressionKey.JSONB_CONTAINS;

  static _sqlNames = ['JSONB_CONTAINS'];

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: JsonbContainsExprArgs;

  constructor (args: JsonbContainsExprArgs = {}) {
    super(args);
  }
}

export type JsonbContainsAnyTopKeysExprArgs = Merge<[
  FuncExprArgs,
  BinaryExprArgs,
]>;

export class JsonbContainsAnyTopKeysExpr extends multiInherit(BinaryExpr, FuncExpr) {
  static key = ExpressionKey.JSONB_CONTAINS_ANY_TOP_KEYS;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: JsonbContainsAnyTopKeysExprArgs;

  constructor (args: JsonbContainsAnyTopKeysExprArgs = {}) {
    super(args);
  }
}

export type JsonbContainsAllTopKeysExprArgs = Merge<[
  FuncExprArgs,
  BinaryExprArgs,
]>;

export class JsonbContainsAllTopKeysExpr extends multiInherit(BinaryExpr, FuncExpr) {
  static key = ExpressionKey.JSONB_CONTAINS_ALL_TOP_KEYS;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: JsonbContainsAllTopKeysExprArgs;

  constructor (args: JsonbContainsAllTopKeysExprArgs = {}) {
    super(args);
  }
}

export type JsonbExistsExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    path?: Expression;
  },
]>;

export class JsonbExistsExpr extends FuncExpr {
  static key = ExpressionKey.JSONB_EXISTS;

  static _sqlNames = ['JSONB_EXISTS'];

  static requiredArgs = new Set(['this', 'path']);

  static availableArgs = new Set(['this', 'path']);

  static argOrder = ['this', 'path'];

  declare args: JsonbExistsExprArgs;

  constructor (args: JsonbExistsExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JsonbDeleteAtPathExprArgs = Merge<[
  FuncExprArgs,
  BinaryExprArgs,
]>;

export class JsonbDeleteAtPathExpr extends multiInherit(BinaryExpr, FuncExpr) {
  static key = ExpressionKey.JSONB_DELETE_AT_PATH;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: JsonbDeleteAtPathExprArgs;

  constructor (args: JsonbDeleteAtPathExprArgs = {}) {
    super(args);
  }
}

export type JsonExtractExprArgs = Merge<[
  FuncExprArgs,
  BinaryExprArgs,
  {
    onlyJsonTypes?: boolean;
    expressions?: Expression[];
    variantExtract?: Expression;
    jsonQuery?: Expression;
    option?: Expression;
    quote?: Expression;
    onCondition?: Expression;
    requiresJson?: ExpressionOrBoolean;
    expression?: number | ExpressionOrString;
  },
]>;

export class JsonExtractExpr extends multiInherit(BinaryExpr, FuncExpr) {
  static key = ExpressionKey.JSON_EXTRACT;

  static isVarLenArgs = true;
  static _sqlNames = ['JSON_EXTRACT'];

  static requiredArgs = new Set(['this', 'expression']);
  static availableArgs = new Set([
    'this',
    'expression',
    'onlyJsonTypes',
    'expressions',
    'variantExtract',
    'jsonQuery',
    'option',
    'quote',
    'onCondition',
    'requiresJson',
  ]);

  declare args: JsonExtractExprArgs;

  constructor (args: JsonExtractExprArgs = {}) {
    super(args);
  }

  get outputName (): string {
    return !this.args.expressions ? (this.args.expression as Expression | undefined)?.outputName ?? '' : '';
  }
}

export type JsonExtractArrayExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
  },
]>;

export class JsonExtractArrayExpr extends FuncExpr {
  static key = ExpressionKey.JSON_EXTRACT_ARRAY;

  static _sqlNames = ['JSON_EXTRACT_ARRAY'];

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: JsonExtractArrayExprArgs;

  constructor (args: JsonExtractArrayExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JsonExtractScalarExprArgs = Merge<[
  BinaryExprArgs,
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    onlyJsonTypes?: Expression;
    expressions?: Expression[];
    jsonType?: string | Expression;
    scalarOnly?: boolean;
  },
]>;

export class JsonExtractScalarExpr extends multiInherit(BinaryExpr, FuncExpr) {
  static key = ExpressionKey.JSON_EXTRACT_SCALAR;

  static isVarLenArgs = true;
  static _sqlNames = ['JSON_EXTRACT_SCALAR'];

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'onlyJsonTypes',
    'expressions',
    'jsonType',
    'scalarOnly',
  ]);

  declare args: JsonExtractScalarExprArgs;

  constructor (args: JsonExtractScalarExprArgs = {}) {
    super(args);
  }

  get outputName (): string {
    return this.args.expression?.outputName ?? '';
  }
}

export type JsonbExtractExprArgs = Merge<[
  FuncExprArgs,
  BinaryExprArgs,
]>;

export class JsonbExtractExpr extends multiInherit(BinaryExpr, FuncExpr) {
  static key = ExpressionKey.JSONB_EXTRACT;

  static _sqlNames = ['JSONB_EXTRACT'];

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: JsonbExtractExprArgs;

  constructor (args: JsonbExtractExprArgs = {}) {
    super(args);
  }
}

export type JsonbExtractScalarExprArgs = Merge<[
  BinaryExprArgs,
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    jsonType?: Expression;
    expressions?: Expression[];
  },
]>;

export class JsonbExtractScalarExpr extends multiInherit(BinaryExpr, FuncExpr) {
  static key = ExpressionKey.JSONB_EXTRACT_SCALAR;

  static _sqlNames = ['JSONB_EXTRACT_SCALAR'];

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'jsonType',
  ]);

  declare args: JsonbExtractScalarExprArgs;

  constructor (args: JsonbExtractScalarExprArgs = {}) {
    super(args);
  }
}

export type JsonFormatExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    options?: Expression;
    isJson?: boolean;
    toJson?: boolean;
  },
]>;

export class JsonFormatExpr extends FuncExpr {
  static key = ExpressionKey.JSON_FORMAT;

  static _sqlNames = ['JSON_FORMAT'];

  static availableArgs = new Set([
    'this',
    'options',
    'isJson',
    'toJson',
  ]);

  static argOrder = [
    'this',
    'options',
    'isJson',
    'toJson',
  ];

  declare args: JsonFormatExprArgs;

  constructor (args: JsonFormatExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JsonArrayAppendExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class JsonArrayAppendExpr extends FuncExpr {
  static key = ExpressionKey.JSON_ARRAY_APPEND;

  static isVarLenArgs = true;
  static _sqlNames = ['JSON_ARRAY_APPEND'];

  static requiredArgs = new Set(['this', 'expressions']);

  static availableArgs = new Set(['this', 'expressions']);

  static argOrder = ['this', 'expressions'];

  declare args: JsonArrayAppendExprArgs;

  constructor (args: JsonArrayAppendExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JsonArrayContainsExprArgs = Merge<[
  BinaryExprArgs,
  {
    this?: Expression;
    jsonType?: string | Expression;
    expression?: Expression;
  },
]>;

export class JsonArrayContainsExpr extends multiInherit(BinaryExpr, PredicateExpr, FuncExpr) {
  static key = ExpressionKey.JSON_ARRAY_CONTAINS;

  static _sqlNames = ['JSON_ARRAY_CONTAINS'];

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'jsonType',
  ]);

  declare args: JsonArrayContainsExprArgs;

  constructor (args: JsonArrayContainsExprArgs = {}) {
    super(args);
  }
}

export type JsonArrayInsertExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class JsonArrayInsertExpr extends FuncExpr {
  static key = ExpressionKey.JSON_ARRAY_INSERT;

  static isVarLenArgs = true;
  static _sqlNames = ['JSON_ARRAY_INSERT'];

  static requiredArgs = new Set(['this', 'expressions']);

  static availableArgs = new Set(['this', 'expressions']);

  static argOrder = ['this', 'expressions'];

  declare args: JsonArrayInsertExprArgs;

  constructor (args: JsonArrayInsertExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ParseBignumericExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ParseBignumericExpr extends FuncExpr {
  static key = ExpressionKey.PARSE_BIGNUMERIC;

  static argOrder = ['this'];

  declare args: ParseBignumericExprArgs;

  constructor (args: ParseBignumericExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ParseNumericExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ParseNumericExpr extends FuncExpr {
  static key = ExpressionKey.PARSE_NUMERIC;

  static argOrder = ['this'];

  declare args: ParseNumericExprArgs;

  constructor (args: ParseNumericExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ParseJsonExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: ExpressionValue;
    expression?: ExpressionValue;
    safe?: boolean;
  },
]>;

export class ParseJsonExpr extends FuncExpr {
  static key = ExpressionKey.PARSE_JSON;

  static _sqlNames = ['PARSE_JSON', 'JSON_PARSE'];

  static availableArgs = new Set([
    'this',
    'expression',
    'safe',
  ]);

  static argOrder = [
    'this',
    'expression',
    'safe',
  ];

  declare args: ParseJsonExprArgs;

  constructor (args: ParseJsonExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ParseUrlExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    partToExtract?: Expression;
    key?: unknown;
    permissive?: Expression;
  },
]>;

export class ParseUrlExpr extends FuncExpr {
  static key = ExpressionKey.PARSE_URL;

  static availableArgs = new Set([
    'this',
    'partToExtract',
    'key',
    'permissive',
  ]);

  static argOrder = [
    'this',
    'partToExtract',
    'key',
    'permissive',
  ];

  declare args: ParseUrlExprArgs;

  constructor (args: ParseUrlExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ParseIpExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    type?: Expression;
    permissive?: Expression;
  },
]>;

export class ParseIpExpr extends FuncExpr {
  static key = ExpressionKey.PARSE_IP;

  static requiredArgs = new Set(['this', 'type']);

  static availableArgs = new Set([
    'this',
    'type',
    'permissive',
  ]);

  static argOrder = [
    'this',
    'type',
    'permissive',
  ];

  declare args: ParseIpExprArgs;

  constructor (args: ParseIpExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ParseTimeExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    format?: string;
  },
]>;

export class ParseTimeExpr extends FuncExpr {
  static key = ExpressionKey.PARSE_TIME;

  static requiredArgs = new Set(['this', 'format']);

  static availableArgs = new Set(['this', 'format']);

  static argOrder = ['this', 'format'];

  declare args: ParseTimeExprArgs;

  constructor (args: ParseTimeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ParseDatetimeExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    format?: string;
    zone?: Expression;
  },
]>;

export class ParseDatetimeExpr extends FuncExpr {
  static key = ExpressionKey.PARSE_DATETIME;

  static availableArgs = new Set([
    'this',
    'format',
    'zone',
  ]);

  static argOrder = [
    'this',
    'format',
    'zone',
  ];

  declare args: ParseDatetimeExprArgs;

  constructor (args: ParseDatetimeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LeastExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
    ignoreNulls?: boolean;
  },
]>;

export class LeastExpr extends FuncExpr {
  static key = ExpressionKey.LEAST;

  static isVarLenArgs = true;

  static requiredArgs = new Set(['this', 'ignoreNulls']);

  static availableArgs = new Set([
    'this',
    'expressions',
    'ignoreNulls',
  ]);

  static argOrder = [
    'this',
    'expressions',
    'ignoreNulls',
  ];

  declare args: LeastExprArgs;

  constructor (args: LeastExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LeftExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class LeftExpr extends FuncExpr {
  static key = ExpressionKey.LEFT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: LeftExprArgs;

  constructor (args: LeftExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RightExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class RightExpr extends FuncExpr {
  static key = ExpressionKey.RIGHT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: RightExprArgs;

  constructor (args: RightExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ReverseExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ReverseExpr extends FuncExpr {
  static key = ExpressionKey.REVERSE;

  static argOrder = ['this'];

  declare args: ReverseExprArgs;

  constructor (args: ReverseExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LengthExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    binary?: boolean;
    encoding?: Expression;
  },
]>;

export class LengthExpr extends FuncExpr {
  static key = ExpressionKey.LENGTH;

  static _sqlNames = [
    'LENGTH',
    'LEN',
    'CHAR_LENGTH',
    'CHARACTER_LENGTH',
  ];

  static availableArgs = new Set([
    'this',
    'binary',
    'encoding',
  ]);

  static argOrder = [
    'this',
    'binary',
    'encoding',
  ];

  declare args: LengthExprArgs;

  constructor (args: LengthExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RtrimmedLengthExprArgs = Merge<[
  FuncExprArgs,
]>;

export class RtrimmedLengthExpr extends FuncExpr {
  static key = ExpressionKey.RTRIMMED_LENGTH;

  static argOrder = ['this'];

  declare args: RtrimmedLengthExprArgs;

  constructor (args: RtrimmedLengthExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitLengthExprArgs = Merge<[
  FuncExprArgs,
]>;

export class BitLengthExpr extends FuncExpr {
  static key = ExpressionKey.BIT_LENGTH;

  static argOrder = ['this'];

  declare args: BitLengthExprArgs;

  constructor (args: BitLengthExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LevenshteinExprArgs = Merge<[
  FuncExprArgs,
  {
    insCost?: Expression;
    delCost?: Expression;
    subCost?: Expression;
    maxDist?: Expression;
  },
]>;

export class LevenshteinExpr extends FuncExpr {
  static key = ExpressionKey.LEVENSHTEIN;

  static availableArgs = new Set([
    'this',
    'expression',
    'insCost',
    'delCost',
    'subCost',
    'maxDist',
  ]);

  static argOrder = [
    'insCost',
    'delCost',
    'subCost',
    'maxDist',
  ];

  declare args: LevenshteinExprArgs;

  constructor (args: LevenshteinExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LnExprArgs = Merge<[
  FuncExprArgs,
]>;

export class LnExpr extends FuncExpr {
  static key = ExpressionKey.LN;

  static argOrder = ['this'];

  declare args: LnExprArgs;

  constructor (args: LnExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LogExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class LogExpr extends FuncExpr {
  static key = ExpressionKey.LOG;

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: LogExprArgs;

  constructor (args: LogExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LowerExprArgs = Merge<[
  FuncExprArgs,
]>;

export class LowerExpr extends FuncExpr {
  static key = ExpressionKey.LOWER;

  static argOrder = ['this'];

  declare args: LowerExprArgs;

  constructor (args: LowerExprArgs = {}) {
    super(args);
  }

  static _sqlNames = ['LOWER', 'LCASE'];

  static {
    this.register();
  }
}

export type MapExprArgs = Merge<[
  FuncExprArgs,
  {
    keys?: Expression[];
    values?: Expression[];
  },
]>;

export class MapExpr extends FuncExpr {
  static key = ExpressionKey.MAP;

  static availableArgs = new Set(['keys', 'values']);

  static argOrder = ['keys', 'values'];

  declare args: MapExprArgs;

  static {
    this.register();
  }

  constructor (args: MapExprArgs = {}) {
    super(args);
  }

  get keys (): ExpressionValue[] {
    const keysArg = this.args.keys;
    return (keysArg?.[0]?.args?.expressions || []) as ExpressionValue[];
  }

  get values (): ExpressionValue[] {
    const valuesArg = this.args.values;
    return (valuesArg?.[0]?.args?.expressions || []) as ExpressionValue[];
  }
}

export type ToMapExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ToMapExpr extends FuncExpr {
  static key = ExpressionKey.TO_MAP;

  static argOrder = ['this'];

  declare args: ToMapExprArgs;

  constructor (args: ToMapExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MapFromEntriesExprArgs = Merge<[
  FuncExprArgs,
]>;

export class MapFromEntriesExpr extends FuncExpr {
  static key = ExpressionKey.MAP_FROM_ENTRIES;

  static argOrder = ['this'];

  declare args: MapFromEntriesExprArgs;

  constructor (args: MapFromEntriesExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MapCatExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class MapCatExpr extends FuncExpr {
  static key = ExpressionKey.MAP_CAT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: MapCatExprArgs;

  constructor (args: MapCatExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MapContainsKeyExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    key?: Expression;
  },
]>;

export class MapContainsKeyExpr extends FuncExpr {
  static key = ExpressionKey.MAP_CONTAINS_KEY;

  static requiredArgs = new Set(['this', 'key']);

  static availableArgs = new Set(['this', 'key']);

  static argOrder = ['this', 'key'];

  declare args: MapContainsKeyExprArgs;

  constructor (args: MapContainsKeyExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MapDeleteExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class MapDeleteExpr extends FuncExpr {
  static key = ExpressionKey.MAP_DELETE;

  static isVarLenArgs = true;

  static requiredArgs = new Set(['this', 'expressions']);

  static availableArgs = new Set(['this', 'expressions']);

  static argOrder = ['this', 'expressions'];

  declare args: MapDeleteExprArgs;

  constructor (args: MapDeleteExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MapInsertExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    key?: Expression;
    value?: string;
    updateFlag?: Expression;
  },
]>;

export class MapInsertExpr extends FuncExpr {
  static key = ExpressionKey.MAP_INSERT;

  static requiredArgs = new Set(['this', 'value']);

  static availableArgs = new Set([
    'this',
    'key',
    'value',
    'updateFlag',
  ]);

  static argOrder = [
    'this',
    'key',
    'value',
    'updateFlag',
  ];

  declare args: MapInsertExprArgs;

  constructor (args: MapInsertExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MapKeysExprArgs = Merge<[
  FuncExprArgs,
]>;

export class MapKeysExpr extends FuncExpr {
  static key = ExpressionKey.MAP_KEYS;

  static argOrder = ['this'];

  declare args: MapKeysExprArgs;

  constructor (args: MapKeysExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MapPickExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class MapPickExpr extends FuncExpr {
  static key = ExpressionKey.MAP_PICK;

  static isVarLenArgs = true;

  static requiredArgs = new Set(['this', 'expressions']);

  static availableArgs = new Set(['this', 'expressions']);

  static argOrder = ['this', 'expressions'];

  declare args: MapPickExprArgs;

  constructor (args: MapPickExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MapSizeExprArgs = Merge<[
  FuncExprArgs,
]>;

export class MapSizeExpr extends FuncExpr {
  static key = ExpressionKey.MAP_SIZE;

  static argOrder = ['this'];

  declare args: MapSizeExprArgs;

  constructor (args: MapSizeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StarMapExprArgs = Merge<[
  FuncExprArgs,
]>;

export class StarMapExpr extends FuncExpr {
  static key = ExpressionKey.STAR_MAP;

  static argOrder = ['this'];

  declare args: StarMapExprArgs;

  constructor (args: StarMapExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type VarMapExprArgs = Merge<[
  FuncExprArgs,
  {
    keys?: Expression;
    values?: Expression;
  },
]>;

export class VarMapExpr extends FuncExpr {
  static key = ExpressionKey.VAR_MAP;

  static isVarLenArgs = true;
  static requiredArgs = new Set(['keys', 'values']);

  static availableArgs = new Set(['keys', 'values']);

  static argOrder = ['keys', 'values'];

  declare args: VarMapExprArgs;

  constructor (args: VarMapExprArgs = {}) {
    super(args);
  }

  get keys (): ExpressionValueList {
    const keysArg = this.args.keys;
    return keysArg?.args.expressions as ExpressionValueList ?? [];
  }

  get values (): ExpressionValueList {
    const valuesArg = this.args.values;
    return valuesArg?.args.expressions as ExpressionValueList ?? [];
  }

  static {
    this.register();
  }
}

export type MatchAgainstExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
    modifier?: Expression;
  },
]>;

export class MatchAgainstExpr extends FuncExpr {
  static key = ExpressionKey.MATCH_AGAINST;

  static requiredArgs = new Set(['this', 'expressions']);

  static availableArgs = new Set([
    'this',
    'expressions',
    'modifier',
  ]);

  static argOrder = [
    'this',
    'expressions',
    'modifier',
  ];

  declare args: MatchAgainstExprArgs;

  constructor (args: MatchAgainstExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Md5ExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class Md5Expr extends FuncExpr {
  static key = ExpressionKey.MD5;

  static _sqlNames = ['MD5'];

  static argOrder = ['this'];

  declare args: Md5ExprArgs;

  constructor (args: Md5ExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Md5DigestExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class Md5DigestExpr extends FuncExpr {
  static key = ExpressionKey.MD5_DIGEST;

  static isVarLenArgs = true;
  static _sqlNames = ['MD5_DIGEST'];

  static availableArgs = new Set(['this', 'expressions']);

  static argOrder = ['this', 'expressions'];

  declare args: Md5DigestExprArgs;

  constructor (args: Md5DigestExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Md5NumberLower64ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class Md5NumberLower64Expr extends FuncExpr {
  static key = ExpressionKey.MD5_NUMBER_LOWER64;

  static argOrder = ['this'];

  declare args: Md5NumberLower64ExprArgs;

  constructor (args: Md5NumberLower64ExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Md5NumberUpper64ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class Md5NumberUpper64Expr extends FuncExpr {
  static key = ExpressionKey.MD5_NUMBER_UPPER64;

  static argOrder = ['this'];

  declare args: Md5NumberUpper64ExprArgs;

  constructor (args: Md5NumberUpper64ExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MonthExprArgs = Merge<[
  FuncExprArgs,
]>;

export class MonthExpr extends FuncExpr {
  static key = ExpressionKey.MONTH;

  static argOrder = ['this'];

  declare args: MonthExprArgs;

  constructor (args: MonthExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MonthnameExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    abbreviated?: boolean | Expression;
  },
]>;

export class MonthnameExpr extends FuncExpr {
  static key = ExpressionKey.MONTHNAME;

  static availableArgs = new Set(['this', 'abbreviated']);

  static argOrder = ['this', 'abbreviated'];

  declare args: MonthnameExprArgs;

  constructor (args: MonthnameExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AddMonthsExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    preserveEndOfMonth?: boolean | Expression;
  },
]>;

export class AddMonthsExpr extends FuncExpr {
  static key = ExpressionKey.ADD_MONTHS;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'preserveEndOfMonth',
  ]);

  static argOrder = [
    'this',
    'expression',
    'preserveEndOfMonth',
  ];

  declare args: AddMonthsExprArgs;

  constructor (args: AddMonthsExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Nvl2ExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    true?: Expression;
    false?: Expression;
  },
]>;

export class Nvl2Expr extends FuncExpr {
  static key = ExpressionKey.NVL2;

  static requiredArgs = new Set(['this', 'true']);

  static availableArgs = new Set([
    'this',
    'true',
    'false',
  ]);

  static argOrder = [
    'this',
    'true',
    'false',
  ];

  declare args: Nvl2ExprArgs;

  constructor (args: Nvl2ExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type NormalizeExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    form?: Expression;
    isCasefold?: boolean | Expression;
  },
]>;

export class NormalizeExpr extends FuncExpr {
  static key = ExpressionKey.NORMALIZE;

  static availableArgs = new Set([
    'this',
    'form',
    'isCasefold',
  ]);

  static argOrder = [
    'this',
    'form',
    'isCasefold',
  ];

  declare args: NormalizeExprArgs;

  constructor (args: NormalizeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type NormalExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    stddev?: Expression;
    gen?: Expression;
  },
]>;

export class NormalExpr extends FuncExpr {
  static key = ExpressionKey.NORMAL;

  static requiredArgs = new Set([
    'this',
    'stddev',
    'gen',
  ]);

  static availableArgs = new Set([
    'this',
    'stddev',
    'gen',
  ]);

  static argOrder = [
    'this',
    'stddev',
    'gen',
  ];

  declare args: NormalExprArgs;

  constructor (args: NormalExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type NetFuncExprArgs = Merge<[
  FuncExprArgs,
]>;

export class NetFuncExpr extends FuncExpr {
  static key = ExpressionKey.NET_FUNC;

  static argOrder = ['this'];

  declare args: NetFuncExprArgs;

  constructor (args: NetFuncExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type HostExprArgs = Merge<[
  FuncExprArgs,
]>;

export class HostExpr extends FuncExpr {
  static key = ExpressionKey.HOST;

  static argOrder = ['this'];

  declare args: HostExprArgs;

  constructor (args: HostExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegDomainExprArgs = Merge<[
  FuncExprArgs,
]>;

export class RegDomainExpr extends FuncExpr {
  static key = ExpressionKey.REG_DOMAIN;

  static argOrder = ['this'];

  declare args: RegDomainExprArgs;

  constructor (args: RegDomainExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type OverlayExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    from?: Expression;
    for?: Expression;
  },
]>;

export class OverlayExpr extends FuncExpr {
  static key = ExpressionKey.OVERLAY;

  static requiredArgs = new Set([
    'this',
    'expression',
    'from',
  ]);

  static availableArgs = new Set([
    'this',
    'expression',
    'from',
    'for',
  ]);

  static argOrder = [
    'this',
    'expression',
    'fromPosition',
    'for',
  ];

  declare args: OverlayExprArgs;

  constructor (args: OverlayExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type PredictExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    paramsStruct?: Expression;
  },
]>;

export class PredictExpr extends FuncExpr {
  static key = ExpressionKey.PREDICT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'paramsStruct',
  ]);

  static argOrder = [
    'this',
    'expression',
    'paramsStruct',
  ];

  declare args: PredictExprArgs;

  constructor (args: PredictExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MlTranslateExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    paramsStruct?: Expression;
  },
]>;

export class MlTranslateExpr extends FuncExpr {
  static key = ExpressionKey.ML_TRANSLATE;

  static requiredArgs = new Set([
    'this',
    'expression',
    'paramsStruct',
  ]);

  static availableArgs = new Set([
    'this',
    'expression',
    'paramsStruct',
  ]);

  static argOrder = [
    'this',
    'expression',
    'paramsStruct',
  ];

  declare args: MlTranslateExprArgs;

  constructor (args: MlTranslateExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type FeaturesAtTimeExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    time?: Expression;
    numRows?: Expression[];
    ignoreFeatureNulls?: Expression[];
  },
]>;

export class FeaturesAtTimeExpr extends FuncExpr {
  static key = ExpressionKey.FEATURES_AT_TIME;

  static availableArgs = new Set([
    'this',
    'time',
    'numRows',
    'ignoreFeatureNulls',
  ]);

  static argOrder = [
    'this',
    'time',
    'numRows',
    'ignoreFeatureNulls',
  ];

  declare args: FeaturesAtTimeExprArgs;

  constructor (args: FeaturesAtTimeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type GenerateEmbeddingExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    paramsStruct?: Expression;
    isText?: string;
  },
]>;

export class GenerateEmbeddingExpr extends FuncExpr {
  static key = ExpressionKey.GENERATE_EMBEDDING;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'paramsStruct',
    'isText',
  ]);

  static argOrder = [
    'this',
    'expression',
    'paramsStruct',
    'isText',
  ];

  declare args: GenerateEmbeddingExprArgs;

  constructor (args: GenerateEmbeddingExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MlForecastExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
    paramsStruct?: Expression;
  },
]>;

export class MlForecastExpr extends FuncExpr {
  static key = ExpressionKey.ML_FORECAST;

  static availableArgs = new Set([
    'this',
    'expression',
    'paramsStruct',
  ]);

  static argOrder = [
    'this',
    'expression',
    'paramsStruct',
  ];

  declare args: MlForecastExprArgs;

  constructor (args: MlForecastExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type VectorSearchExprArgs = Merge<[
  FuncExprArgs,
  {
    columnToSearch?: Expression;
    queryTable?: Expression;
    queryColumnToSearch?: Expression;
    topK?: Expression;
    distanceType?: Expression;
    options?: Expression[];
  },
]>;

export class VectorSearchExpr extends FuncExpr {
  static key = ExpressionKey.VECTOR_SEARCH;

  static requiredArgs = new Set([
    'this',
    'columnToSearch',
    'queryTable',
  ]);

  static availableArgs = new Set([
    'this',
    'columnToSearch',
    'queryTable',
    'queryColumnToSearch',
    'topK',
    'distanceType',
    'options',
  ]);

  static argOrder = [
    'columnToSearch',
    'queryTable',
    'queryColumnToSearch',
    'topK',
    'distanceType',
    'options',
  ];

  declare args: VectorSearchExprArgs;

  constructor (args: VectorSearchExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type PiExprArgs = Merge<[
  FuncExprArgs,
]>;

export class PiExpr extends FuncExpr {
  static key = ExpressionKey.PI;

  static requiredArgs = new Set<string>();

  static availableArgs = new Set<string>();

  static argOrder = ['this'];

  declare args: PiExprArgs;

  constructor (args: PiExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type PowExprArgs = Merge<[
  FuncExprArgs,
  BinaryExprArgs,
]>;
export class PowExpr extends multiInherit(BinaryExpr, FuncExpr) {
  static key = ExpressionKey.POW;

  static _sqlNames = ['POWER', 'POW'];

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: PowExprArgs;

  constructor (args: PowExprArgs = {}) {
    super(args);
  }
}

export type ApproxPercentileEstimateExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    percentile?: Expression;
  },
]>;

export class ApproxPercentileEstimateExpr extends FuncExpr {
  static key = ExpressionKey.APPROX_PERCENTILE_ESTIMATE;

  static requiredArgs = new Set(['this', 'percentile']);

  static availableArgs = new Set(['this', 'percentile']);

  static argOrder = ['this', 'percentile'];

  declare args: ApproxPercentileEstimateExprArgs;

  constructor (args: ApproxPercentileEstimateExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type QuarterExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class QuarterExpr extends FuncExpr {
  static key = ExpressionKey.QUARTER;

  static argOrder = ['this'];

  declare args: QuarterExprArgs;

  constructor (args: QuarterExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RandExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    lower?: Expression;
    upper?: Expression;
  },
]>;

export class RandExpr extends FuncExpr {
  static key = ExpressionKey.RAND;

  static _sqlNames = ['RAND', 'RANDOM'];

  static availableArgs = new Set([
    'this',
    'lower',
    'upper',
  ]);

  static argOrder = [
    'this',
    'lower',
    'upper',
  ];

  declare args: RandExprArgs;

  constructor (args: RandExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RandnExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class RandnExpr extends FuncExpr {
  static key = ExpressionKey.RANDN;

  static argOrder = ['this'];

  declare args: RandnExprArgs;

  constructor (args: RandnExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RandstrExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    generator?: Expression;
  },
]>;

export class RandstrExpr extends FuncExpr {
  static key = ExpressionKey.RANDSTR;

  static availableArgs = new Set(['this', 'generator']);

  static argOrder = ['this', 'generator'];

  declare args: RandstrExprArgs;

  constructor (args: RandstrExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RangeNExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
    each?: Expression;
  },
]>;

export class RangeNExpr extends FuncExpr {
  static key = ExpressionKey.RANGE_N;

  static requiredArgs = new Set(['this', 'expressions']);

  static availableArgs = new Set([
    'this',
    'expressions',
    'each',
  ]);

  static argOrder = [
    'this',
    'expressions',
    'each',
  ];

  declare args: RangeNExprArgs;

  constructor (args: RangeNExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RangeBucketExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class RangeBucketExpr extends FuncExpr {
  static key = ExpressionKey.RANGE_BUCKET;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: RangeBucketExprArgs;

  constructor (args: RangeBucketExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ReadCsvExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class ReadCsvExpr extends FuncExpr {
  static key = ExpressionKey.READ_CSV;

  static isVarLenArgs = true;
  static _sqlNames = ['READ_CSV'];

  static availableArgs = new Set(['this', 'expressions']);

  static argOrder = ['this', 'expressions'];

  declare args: ReadCsvExprArgs;

  constructor (args: ReadCsvExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ReadParquetExprArgs = Merge<[
  FuncExprArgs,
  { expressions?: Expression[] },
]>;

export class ReadParquetExpr extends FuncExpr {
  static key = ExpressionKey.READ_PARQUET;

  static isVarLenArgs = true;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  static argOrder = ['expressions'];

  declare args: ReadParquetExprArgs;

  constructor (args: ReadParquetExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ReduceExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    initial?: Expression;
    merge?: Expression;
    finish?: Expression;
  },
]>;

export class ReduceExpr extends FuncExpr {
  static key = ExpressionKey.REDUCE;

  static requiredArgs = new Set([
    'this',
    'initial',
    'merge',
  ]);

  static availableArgs = new Set([
    'this',
    'initial',
    'merge',
    'finish',
  ]);

  static argOrder = [
    'this',
    'initial',
    'merge',
    'finish',
  ];

  declare args: ReduceExprArgs;

  constructor (args: ReduceExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegexpExtractExprArgs = Merge<[
  FuncExprArgs,
  {
    position?: Expression;
    occurrence?: Expression;
    parameters?: Expression;
    group?: Expression;
    nullIfPosOverflow?: boolean;
  },
]>;

export class RegexpExtractExpr extends FuncExpr {
  static key = ExpressionKey.REGEXP_EXTRACT;

  static requiredArgs = new Set(['this', 'expression']);
  static availableArgs = new Set([
    'this',
    'expression',
    'position',
    'occurrence',
    'parameters',
    'group',
    'nullIfPosOverflow',
  ]);

  static argOrder = [
    'position',
    'occurrence',
    'parameters',
    'group',
    'nullIfPosOverflow',
  ];

  declare args: RegexpExtractExprArgs;

  constructor (args: RegexpExtractExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegexpExtractAllExprArgs = Merge<[
  FuncExprArgs,
  {
    group?: Expression;
    parameters?: Expression;
    position?: Expression;
    occurrence?: Expression;
  },
]>;

export class RegexpExtractAllExpr extends FuncExpr {
  static key = ExpressionKey.REGEXP_EXTRACT_ALL;

  static requiredArgs = new Set(['this', 'expression']);
  static availableArgs = new Set([
    'this',
    'expression',
    'group',
    'parameters',
    'position',
    'occurrence',
  ]);

  static argOrder = [
    'group',
    'parameters',
    'position',
    'occurrence',
  ];

  declare args: RegexpExtractAllExprArgs;

  constructor (args: RegexpExtractAllExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegexpReplaceExprArgs = Merge<[
  FuncExprArgs,
  {
    replacement?: string | Expression;
    position?: Expression;
    occurrence?: Expression;
    modifiers?: Expression;
    singleReplace?: boolean | Expression;
  },
]>;

export class RegexpReplaceExpr extends FuncExpr {
  static key = ExpressionKey.REGEXP_REPLACE;

  static requiredArgs = new Set(['this', 'expression']);
  static availableArgs = new Set([
    'this',
    'expression',
    'replacement',
    'position',
    'occurrence',
    'modifiers',
    'singleReplace',
  ]);

  static argOrder = [
    'replacement',
    'position',
    'occurrence',
    'modifiers',
    'singleReplace',
  ];

  declare args: RegexpReplaceExprArgs;

  constructor (args: RegexpReplaceExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegexpLikeExprArgs = Merge<[
  BinaryExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    flag?: Expression;
  },
]>;

export class RegexpLikeExpr extends multiInherit(BinaryExpr, FuncExpr) {
  static key = ExpressionKey.REGEXP_LIKE;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'flag',
    'fullMatch',
  ]);

  declare args: RegexpLikeExprArgs;

  constructor (args: RegexpLikeExprArgs = {}) {
    super(args);
  }
}

export type RegexpILikeExprArgs = Merge<[
  BinaryExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    flag?: Expression;
  },
]>;

export class RegexpILikeExpr extends multiInherit(BinaryExpr, FuncExpr) {
  static key = ExpressionKey.REGEXP_ILIKE;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'flag',
  ]);

  declare args: RegexpILikeExprArgs;

  constructor (args: RegexpILikeExprArgs = {}) {
    super(args);
  }
}

export type RegexpFullMatchExprArgs = Merge<[
  BinaryExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    options?: Expression[];
  },
]>;

export class RegexpFullMatchExpr extends multiInherit(BinaryExpr, FuncExpr) {
  static key = ExpressionKey.REGEXP_FULL_MATCH;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'options',
  ]);

  declare args: RegexpFullMatchExprArgs;

  constructor (args: RegexpFullMatchExprArgs = {}) {
    super(args);
  }
}

export type RegexpInstrExprArgs = Merge<[
  FuncExprArgs,
  {
    position?: Expression;
    occurrence?: Expression;
    option?: Expression;
    parameters?: Expression[];
    group?: Expression;
  },
]>;

export class RegexpInstrExpr extends FuncExpr {
  static key = ExpressionKey.REGEXP_INSTR;

  static requiredArgs = new Set(['this', 'expression']);
  static availableArgs = new Set([
    'this',
    'expression',
    'position',
    'occurrence',
    'option',
    'parameters',
    'group',
  ]);

  static argOrder = [
    'position',
    'occurrence',
    'option',
    'parameters',
    'group',
  ];

  declare args: RegexpInstrExprArgs;

  constructor (args: RegexpInstrExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegexpSplitExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    limit?: number | Expression;
  },
]>;

export class RegexpSplitExpr extends FuncExpr {
  static key = ExpressionKey.REGEXP_SPLIT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'limit',
  ]);

  static argOrder = [
    'this',
    'expression',
    'limit',
  ];

  declare args: RegexpSplitExprArgs;

  constructor (args: RegexpSplitExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegexpCountExprArgs = Merge<[
  FuncExprArgs,
  {
    position?: Expression;
    parameters?: Expression[];
  },
]>;

export class RegexpCountExpr extends FuncExpr {
  static key = ExpressionKey.REGEXP_COUNT;

  static requiredArgs = new Set(['this', 'expression']);
  static availableArgs = new Set([
    'this',
    'expression',
    'position',
    'parameters',
  ]);

  static argOrder = ['position', 'parameters'];

  declare args: RegexpCountExprArgs;

  constructor (args: RegexpCountExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RepeatExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    times?: Expression[];
  },
]>;

export class RepeatExpr extends FuncExpr {
  static key = ExpressionKey.REPEAT;

  static requiredArgs = new Set(['this', 'times']);

  static availableArgs = new Set(['this', 'times']);

  static argOrder = ['this', 'times'];

  declare args: RepeatExprArgs;

  constructor (args: RepeatExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ReplaceExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    replacement?: LiteralExpr | string;
  },
]>;

export class ReplaceExpr extends FuncExpr {
  static key = ExpressionKey.REPLACE;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'replacement',
  ]);

  static argOrder = [
    'this',
    'expression',
    'replacement',
  ];

  declare args: ReplaceExprArgs;

  constructor (args: ReplaceExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RadiansExprArgs = Merge<[
  FuncExprArgs,
]>;

export class RadiansExpr extends FuncExpr {
  static key = ExpressionKey.RADIANS;

  static argOrder = ['this'];

  declare args: RadiansExprArgs;

  constructor (args: RadiansExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RoundExprArgs = Merge<[
  FuncExprArgs,
  {
    decimals?: Expression;
    truncate?: Expression;
    castsNonIntegerDecimals?: boolean;
  },
]>;

export class RoundExpr extends FuncExpr {
  static key = ExpressionKey.ROUND;

  static availableArgs = new Set([
    'this',
    'decimals',
    'truncate',
    'castsNonIntegerDecimals',
  ]);

  static argOrder = [
    'decimals',
    'truncate',
    'castsNonIntegerDecimals',
  ];

  declare args: RoundExprArgs;

  constructor (args: RoundExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TruncExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    decimals?: Expression;
  },
]>;

export class TruncExpr extends FuncExpr {
  static key = ExpressionKey.TRUNC;

  static _sqlNames = ['TRUNC', 'TRUNCATE'];

  static availableArgs = new Set(['this', 'decimals']);

  static argOrder = ['this', 'decimals'];

  declare args: TruncExprArgs;

  constructor (args: TruncExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RowNumberExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class RowNumberExpr extends FuncExpr {
  static key = ExpressionKey.ROW_NUMBER;

  static argOrder = ['this'];

  declare args: RowNumberExprArgs;

  constructor (args: RowNumberExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Seq1ExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class Seq1Expr extends FuncExpr {
  static key = ExpressionKey.SEQ1;

  static argOrder = ['this'];

  declare args: Seq1ExprArgs;

  constructor (args: Seq1ExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Seq2ExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class Seq2Expr extends FuncExpr {
  static key = ExpressionKey.SEQ2;

  static argOrder = ['this'];

  declare args: Seq2ExprArgs;

  constructor (args: Seq2ExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Seq4ExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class Seq4Expr extends FuncExpr {
  static key = ExpressionKey.SEQ4;

  static argOrder = ['this'];

  declare args: Seq4ExprArgs;

  constructor (args: Seq4ExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Seq8ExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class Seq8Expr extends FuncExpr {
  static key = ExpressionKey.SEQ8;

  static argOrder = ['this'];

  declare args: Seq8ExprArgs;

  constructor (args: Seq8ExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SafeAddExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class SafeAddExpr extends FuncExpr {
  static key = ExpressionKey.SAFE_ADD;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: SafeAddExprArgs;

  constructor (args: SafeAddExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SafeDivideExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class SafeDivideExpr extends FuncExpr {
  static key = ExpressionKey.SAFE_DIVIDE;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: SafeDivideExprArgs;

  constructor (args: SafeDivideExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SafeMultiplyExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class SafeMultiplyExpr extends FuncExpr {
  static key = ExpressionKey.SAFE_MULTIPLY;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: SafeMultiplyExprArgs;

  constructor (args: SafeMultiplyExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SafeNegateExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SafeNegateExpr extends FuncExpr {
  static key = ExpressionKey.SAFE_NEGATE;

  static argOrder = ['this'];

  declare args: SafeNegateExprArgs;

  constructor (args: SafeNegateExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SafeSubtractExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class SafeSubtractExpr extends FuncExpr {
  static key = ExpressionKey.SAFE_SUBTRACT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: SafeSubtractExprArgs;

  constructor (args: SafeSubtractExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SafeConvertBytesToStringExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SafeConvertBytesToStringExpr extends FuncExpr {
  static key = ExpressionKey.SAFE_CONVERT_BYTES_TO_STRING;

  static argOrder = ['this'];

  declare args: SafeConvertBytesToStringExprArgs;

  constructor (args: SafeConvertBytesToStringExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ShaExprArgs = Merge<[
  FuncExprArgs,
]>;

export class ShaExpr extends FuncExpr {
  static key = ExpressionKey.SHA;

  static argOrder = ['this'];

  declare args: ShaExprArgs;

  constructor (args: ShaExprArgs = {}) {
    super(args);
  }

  static _sqlNames = ['SHA', 'Sha1'];

  static {
    this.register();
  }
}

export type Sha2ExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    length?: number | Expression;
  },
]>;

export class Sha2Expr extends FuncExpr {
  static key = ExpressionKey.Sha2;

  static _sqlNames = ['Sha2'];

  static availableArgs = new Set(['this', 'length']);

  static argOrder = ['this', 'length'];

  declare args: Sha2ExprArgs;

  static {
    this.register();
  }

  constructor (args: Sha2ExprArgs = {}) {
    super(args);
  }
}

export type Sha1DigestExprArgs = Merge<[
  FuncExprArgs,
]>;

export class Sha1DigestExpr extends FuncExpr {
  static key = ExpressionKey.Sha1_DIGEST;

  static argOrder = ['this'];

  declare args: Sha1DigestExprArgs;

  constructor (args: Sha1DigestExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type Sha2DigestExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    length?: number | Expression;
  },
]>;

export class Sha2DigestExpr extends FuncExpr {
  static key = ExpressionKey.Sha2_DIGEST;

  static availableArgs = new Set(['this', 'length']);

  static argOrder = ['this', 'length'];

  declare args: Sha2DigestExprArgs;

  constructor (args: Sha2DigestExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SignExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SignExpr extends FuncExpr {
  static key = ExpressionKey.SIGN;

  static argOrder = ['this'];

  declare args: SignExprArgs;

  constructor (args: SignExprArgs = {}) {
    super(args);
  }

  static _sqlNames = ['SIGN', 'SIGNUM'];

  static {
    this.register();
  }
}

export type SortArrayExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    asc?: Expression;
    nullsFirst?: Expression;
  },
]>;

export class SortArrayExpr extends FuncExpr {
  static key = ExpressionKey.SORT_ARRAY;

  static availableArgs = new Set([
    'this',
    'asc',
    'nullsFirst',
  ]);

  static argOrder = [
    'this',
    'asc',
    'nullsFirst',
  ];

  declare args: SortArrayExprArgs;

  static {
    this.register();
  }

  constructor (args: SortArrayExprArgs = {}) {
    super(args);
  }
}

export type SoundexExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SoundexExpr extends FuncExpr {
  static key = ExpressionKey.SOUNDEX;

  static argOrder = ['this'];

  declare args: SoundexExprArgs;

  constructor (args: SoundexExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SoundexP123ExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SoundexP123Expr extends FuncExpr {
  static key = ExpressionKey.SOUNDEX_P123;

  static argOrder = ['this'];

  declare args: SoundexP123ExprArgs;

  constructor (args: SoundexP123ExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SplitExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    limit?: number | Expression;
  },
]>;

export class SplitExpr extends FuncExpr {
  static key = ExpressionKey.SPLIT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'limit',
  ]);

  static argOrder = [
    'this',
    'expression',
    'limit',
  ];

  declare args: SplitExprArgs;

  constructor (args: SplitExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SplitPartExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    delimiter?: number | Expression;
    partIndex?: Expression;
  },
]>;

export class SplitPartExpr extends FuncExpr {
  static key = ExpressionKey.SPLIT_PART;

  static availableArgs = new Set([
    'this',
    'delimiter',
    'partIndex',
  ]);

  static argOrder = [
    'this',
    'delimiter',
    'partIndex',
  ];

  declare args: SplitPartExprArgs;

  constructor (args: SplitPartExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SubstringExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    start?: Expression;
    length?: number | Expression;
  },
]>;

export class SubstringExpr extends FuncExpr {
  static key = ExpressionKey.SUBSTRING;
  static _sqlNames = ['SUBSTRING', 'SUBSTR'];

  static availableArgs = new Set([
    'this',
    'start',
    'length',
  ]);

  static argOrder = [
    'this',
    'start',
    'length',
  ];

  declare args: SubstringExprArgs;

  constructor (args: SubstringExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SubstringIndexExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    delimiter?: number | Expression;
    count?: Expression;
  },
]>;

export class SubstringIndexExpr extends FuncExpr {
  static key = ExpressionKey.SUBSTRING_INDEX;

  static requiredArgs = new Set([
    'this',
    'delimiter',
    'count',
  ]);

  static availableArgs = new Set([
    'this',
    'delimiter',
    'count',
  ]);

  static argOrder = [
    'this',
    'delimiter',
    'count',
  ];

  declare args: SubstringIndexExprArgs;

  constructor (args: SubstringIndexExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StandardHashExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
  },
]>;

export class StandardHashExpr extends FuncExpr {
  static key = ExpressionKey.STANDARD_HASH;

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: StandardHashExprArgs;

  constructor (args: StandardHashExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StartsWithExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class StartsWithExpr extends FuncExpr {
  static key = ExpressionKey.STARTS_WITH;
  static _sqlNames = ['STARTS_WITH', 'STARTSWITH'];

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: StartsWithExprArgs;

  constructor (args: StartsWithExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type EndsWithExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class EndsWithExpr extends FuncExpr {
  static key = ExpressionKey.ENDS_WITH;
  static _sqlNames = ['ENDS_WITH', 'ENDSWITH'];

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: EndsWithExprArgs;

  constructor (args: EndsWithExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StrPositionExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    substr?: Expression;
    position?: Expression;
    occurrence?: Expression;
  },
]>;

export class StrPositionExpr extends FuncExpr {
  static key = ExpressionKey.STR_POSITION;

  static requiredArgs = new Set(['this', 'substr']);

  static availableArgs = new Set([
    'this',
    'substr',
    'position',
    'occurrence',
  ]);

  static argOrder = [
    'this',
    'substr',
    'position',
    'occurrence',
  ];

  declare args: StrPositionExprArgs;

  constructor (args: StrPositionExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SearchExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    jsonScope?: Expression;
    analyzer?: Expression;
    analyzerOptions?: Expression[];
    searchMode?: Expression;
  },
]>;

export class SearchExpr extends FuncExpr {
  static key = ExpressionKey.SEARCH;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'jsonScope',
    'analyzer',
    'analyzerOptions',
    'searchMode',
  ]);

  static argOrder = [
    'this',
    'expression',
    'jsonScope',
    'analyzer',
    'analyzerOptions',
    'searchMode',
  ];

  declare args: SearchExprArgs;

  constructor (args: SearchExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SearchIpExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class SearchIpExpr extends FuncExpr {
  static key = ExpressionKey.SEARCH_IP;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: SearchIpExprArgs;

  constructor (args: SearchIpExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StrToDateExprArgs = Merge<[
  FuncExprArgs,
  {
    format?: string | Expression;
    safe?: boolean;
    this?: Expression;
  },
]>;

export class StrToDateExpr extends FuncExpr {
  static key = ExpressionKey.STR_TO_DATE;

  static availableArgs = new Set([
    'this',
    'format',
    'safe',
  ]);

  static argOrder = [
    'format',
    'safe',
    'this',
  ];

  declare args: StrToDateExprArgs;

  constructor (args: StrToDateExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StrToTimeExprArgs = Merge<[
  FuncExprArgs,
  {
    format?: string | Expression;
    zone?: Expression;
    safe?: boolean;
    targetType?: Expression;
    this?: Expression;
  },
]>;

export class StrToTimeExpr extends FuncExpr {
  static key = ExpressionKey.STR_TO_TIME;

  static requiredArgs = new Set(['this', 'format']);

  static availableArgs = new Set([
    'this',
    'format',
    'zone',
    'safe',
    'targetType',
  ]);

  static argOrder = [
    'format',
    'zone',
    'safe',
    'targetType',
    'this',
  ];

  declare args: StrToTimeExprArgs;

  constructor (args: StrToTimeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StrToUnixExprArgs = Merge<[
  FuncExprArgs,
  {
    format?: string;
    this?: Expression;
  },
]>;

export class StrToUnixExpr extends FuncExpr {
  static key = ExpressionKey.STR_TO_UNIX;

  static availableArgs = new Set(['this', 'format']);

  static argOrder = ['format', 'this'];

  declare args: StrToUnixExprArgs;

  constructor (args: StrToUnixExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StrToMapExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    pairDelim?: string | Expression;
    keyValueDelim?: string | Expression;
    duplicateResolutionCallback?: Expression;
  },
]>;

export class StrToMapExpr extends FuncExpr {
  static key = ExpressionKey.STR_TO_MAP;

  static availableArgs = new Set([
    'this',
    'pairDelim',
    'keyValueDelim',
    'duplicateResolutionCallback',
  ]);

  static argOrder = [
    'this',
    'pairDelim',
    'keyValueDelim',
    'duplicateResolutionCallback',
  ];

  declare args: StrToMapExprArgs;

  constructor (args: StrToMapExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type NumberToStrExprArgs = Merge<[
  FuncExprArgs,
  {
    format?: string;
    culture?: Expression;
    this?: Expression;
  },
]>;

export class NumberToStrExpr extends FuncExpr {
  static key = ExpressionKey.NUMBER_TO_STR;

  static requiredArgs = new Set(['this', 'format']);

  static availableArgs = new Set([
    'this',
    'format',
    'culture',
  ]);

  static argOrder = [
    'format',
    'culture',
    'this',
  ];

  declare args: NumberToStrExprArgs;

  constructor (args: NumberToStrExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type FromBaseExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class FromBaseExpr extends FuncExpr {
  static key = ExpressionKey.FROM_BASE;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: FromBaseExprArgs;

  constructor (args: FromBaseExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SpaceExprArgs = Merge<[
  FuncExprArgs,
  { this?: ExpressionOrString },
]>;

export class SpaceExpr extends FuncExpr {
  static key = ExpressionKey.SPACE;

  static argOrder = ['this'];

  declare args: SpaceExprArgs;

  constructor (args: SpaceExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StructExprArgs = Merge<[
  FuncExprArgs,
  { expressions?: Expression[] },
]>;

export class StructExpr extends FuncExpr {
  static key = ExpressionKey.STRUCT;

  static isVarLenArgs = true;
  static availableArgs = new Set(['expressions']);

  static argOrder = ['expressions'];

  declare args: StructExprArgs;

  constructor (args: StructExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StructExtractExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class StructExtractExpr extends FuncExpr {
  static key = ExpressionKey.STRUCT_EXTRACT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: StructExtractExprArgs;

  constructor (args: StructExtractExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StuffExprArgs = Merge<[
  FuncExprArgs,
  {
    start?: Expression;
    length?: number | Expression;
    this?: Expression;
    expression?: Expression;
  },
]>;

export class StuffExpr extends FuncExpr {
  static key = ExpressionKey.STUFF;
  static _sqlNames = ['STUFF', 'INSERT'];

  static requiredArgs = new Set([
    'this',
    'start',
    'length',
    'expression',
  ]);

  static availableArgs = new Set([
    'this',
    'start',
    'length',
    'expression',
  ]);

  static argOrder = [
    'start',
    'length',
    'this',
    'expression',
  ];

  declare args: StuffExprArgs;

  constructor (args: StuffExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SqrtExprArgs = Merge<[
  FuncExprArgs,
]>;

export class SqrtExpr extends FuncExpr {
  static key = ExpressionKey.SQRT;

  static argOrder = ['this'];

  declare args: SqrtExprArgs;

  constructor (args: SqrtExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeExprArgs = Merge<[
  FuncExprArgs,
  {
    zone?: Expression;
    this?: Expression;
  },
]>;

export class TimeExpr extends FuncExpr {
  static key = ExpressionKey.TIME;

  static availableArgs = new Set(['this', 'zone']);

  static argOrder = ['zone', 'this'];

  declare args: TimeExprArgs;

  constructor (args: TimeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeToStrExprArgs = Merge<[
  FuncExprArgs,
  {
    format?: string | Expression;
    culture?: Expression;
    zone?: Expression;
    this?: Expression;
  },
]>;

export class TimeToStrExpr extends FuncExpr {
  static key = ExpressionKey.TIME_TO_STR;

  static requiredArgs = new Set(['this', 'format']);

  static availableArgs = new Set([
    'this',
    'format',
    'culture',
    'zone',
  ]);

  static argOrder = [
    'format',
    'culture',
    'zone',
    'this',
  ];

  declare args: TimeToStrExprArgs;

  constructor (args: TimeToStrExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeToTimeStrExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TimeToTimeStrExpr extends FuncExpr {
  static key = ExpressionKey.TIME_TO_TIME_STR;

  static argOrder = ['this'];

  declare args: TimeToTimeStrExprArgs;

  constructor (args: TimeToTimeStrExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeToUnixExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TimeToUnixExpr extends FuncExpr {
  static key = ExpressionKey.TIME_TO_UNIX;

  static argOrder = ['this'];

  declare args: TimeToUnixExprArgs;

  constructor (args: TimeToUnixExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeStrToDateExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TimeStrToDateExpr extends FuncExpr {
  static key = ExpressionKey.TIME_STR_TO_DATE;

  static argOrder = ['this'];

  declare args: TimeStrToDateExprArgs;

  constructor (args: TimeStrToDateExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeStrToTimeExprArgs = Merge<[
  FuncExprArgs,
  {
    zone?: Expression;
    this?: Expression;
  },
]>;

export class TimeStrToTimeExpr extends FuncExpr {
  static key = ExpressionKey.TIME_STR_TO_TIME;

  static availableArgs = new Set(['this', 'zone']);

  static argOrder = ['zone', 'this'];

  declare args: TimeStrToTimeExprArgs;

  constructor (args: TimeStrToTimeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimeStrToUnixExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TimeStrToUnixExpr extends FuncExpr {
  static key = ExpressionKey.TIME_STR_TO_UNIX;

  static argOrder = ['this'];

  declare args: TimeStrToUnixExprArgs;

  constructor (args: TimeStrToUnixExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TrimExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
    position?: TrimPosition;
    collation?: Expression;
  },
]>;

export class TrimExpr extends FuncExpr {
  static key = ExpressionKey.TRIM;

  static availableArgs = new Set([
    'this',
    'expression',
    'position',
    'collation',
  ]);

  static argOrder = [
    'this',
    'expression',
    'position',
    'collation',
  ];

  declare args: TrimExprArgs;

  constructor (args: TrimExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TsOrDsAddExprArgs = Merge<[
  FuncExprArgs,
  {
    unit?: Expression;
    returnType?: Expression;
    this?: Expression;
    expression?: Expression;
  },
]>;

export class TsOrDsAddExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  static key = ExpressionKey.TS_OR_DS_ADD;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'unit',
    'returnType',
  ]);

  declare args: TsOrDsAddExprArgs;

  constructor (args: TsOrDsAddExprArgs = {}) {
    super(args);
  }

  get returnType (): Expression | undefined {
    const returnTypeArg = this.args.returnType;
    if (returnTypeArg instanceof DataTypeExpr) {
      return returnTypeArg;
    }
    return DataTypeExpr.build(DataTypeExprKind.DATE);
  }

  static {
    this.register();
  }
}

export type TsOrDsDiffExprArgs = Merge<[
  FuncExprArgs,
  {
    unit?: Expression;
    this?: Expression;
    expression?: Expression;
  },
]>;

export class TsOrDsDiffExpr extends multiInherit(FuncExpr, TimeUnitExpr) {
  static key = ExpressionKey.TS_OR_DS_DIFF;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'unit',
  ]);

  declare args: TsOrDsDiffExprArgs;

  constructor (args: TsOrDsDiffExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TsOrDsToDateStrExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TsOrDsToDateStrExpr extends FuncExpr {
  static key = ExpressionKey.TS_OR_DS_TO_DATE_STR;

  static argOrder = ['this'];

  declare args: TsOrDsToDateStrExprArgs;

  constructor (args: TsOrDsToDateStrExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TsOrDsToDateExprArgs = Merge<[
  FuncExprArgs,
  {
    format?: string;
    safe?: boolean;
    this?: Expression;
  },
]>;

export class TsOrDsToDateExpr extends FuncExpr {
  static key = ExpressionKey.TS_OR_DS_TO_DATE;

  static availableArgs = new Set([
    'this',
    'format',
    'safe',
  ]);

  static argOrder = [
    'format',
    'safe',
    'this',
  ];

  declare args: TsOrDsToDateExprArgs;

  constructor (args: TsOrDsToDateExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TsOrDsToDatetimeExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class TsOrDsToDatetimeExpr extends FuncExpr {
  static key = ExpressionKey.TS_OR_DS_TO_DATETIME;

  static argOrder = ['this'];

  declare args: TsOrDsToDatetimeExprArgs;

  constructor (args: TsOrDsToDatetimeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TsOrDsToTimeExprArgs = Merge<[
  FuncExprArgs,
  {
    format?: string;
    safe?: boolean;
    this?: Expression;
  },
]>;

export class TsOrDsToTimeExpr extends FuncExpr {
  static key = ExpressionKey.TS_OR_DS_TO_TIME;

  static availableArgs = new Set([
    'this',
    'format',
    'safe',
  ]);

  static argOrder = [
    'format',
    'safe',
    'this',
  ];

  declare args: TsOrDsToTimeExprArgs;

  constructor (args: TsOrDsToTimeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TsOrDsToTimestampExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class TsOrDsToTimestampExpr extends FuncExpr {
  static key = ExpressionKey.TS_OR_DS_TO_TIMESTAMP;

  static argOrder = ['this'];

  declare args: TsOrDsToTimestampExprArgs;

  constructor (args: TsOrDsToTimestampExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TsOrDiToDiExprArgs = Merge<[
  FuncExprArgs,
]>;

export class TsOrDiToDiExpr extends FuncExpr {
  static key = ExpressionKey.TS_OR_DI_TO_DI;

  static argOrder = ['this'];

  declare args: TsOrDiToDiExprArgs;

  constructor (args: TsOrDiToDiExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnhexExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
  },
]>;

export class UnhexExpr extends FuncExpr {
  static key = ExpressionKey.UNHEX;

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: UnhexExprArgs;

  constructor (args: UnhexExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnicodeExprArgs = Merge<[
  FuncExprArgs,
]>;

export class UnicodeExpr extends FuncExpr {
  static key = ExpressionKey.UNICODE;

  static argOrder = ['this'];

  declare args: UnicodeExprArgs;

  constructor (args: UnicodeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UniformExprArgs = Merge<[
  FuncExprArgs,
  {
    gen?: Expression;
    seed?: Expression;
    this?: Expression;
    expression?: Expression;
  },
]>;

export class UniformExpr extends FuncExpr {
  static key = ExpressionKey.UNIFORM;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'gen',
    'seed',
  ]);

  static argOrder = [
    'gen',
    'seed',
    'this',
    'expression',
  ];

  declare args: UniformExprArgs;

  constructor (args: UniformExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnixDateExprArgs = Merge<[
  FuncExprArgs,
  { this?: ExpressionOrString },
]>;

export class UnixDateExpr extends FuncExpr {
  static key = ExpressionKey.UNIX_DATE;

  static argOrder = ['this'];

  declare args: UnixDateExprArgs;

  constructor (args: UnixDateExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnixToStrExprArgs = Merge<[
  FuncExprArgs,
  {
    format?: string;
    this?: Expression;
  },
]>;

export class UnixToStrExpr extends FuncExpr {
  static key = ExpressionKey.UNIX_TO_STR;

  static availableArgs = new Set(['this', 'format']);

  static argOrder = ['format', 'this'];

  declare args: UnixToStrExprArgs;

  constructor (args: UnixToStrExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnixToTimeExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    scale?: Expression;
    zone?: Expression;
    hours?: Expression;
    minutes?: Expression;
    format?: string;
    targetType?: Expression;
  },
]>;

export class UnixToTimeExpr extends FuncExpr {
  static key = ExpressionKey.UNIX_TO_TIME;

  static availableArgs = new Set([
    'this',
    'scale',
    'zone',
    'hours',
    'minutes',
    'format',
    'targetType',
  ]);

  static argOrder = [
    'this',
    'scale',
    'zone',
    'hours',
    'minutes',
    'format',
    'targetType',
  ];

  declare args: UnixToTimeExprArgs;

  constructor (args: UnixToTimeExprArgs = {}) {
    super(args);
  }

  static SECONDS = LiteralExpr.number(0);
  static DECIS = LiteralExpr.number(1);
  static CENTIS = LiteralExpr.number(2);
  static MILLIS = LiteralExpr.number(3);
  static DECIMILLIS = LiteralExpr.number(4);
  static CENTIMILLIS = LiteralExpr.number(5);
  static MICROS = LiteralExpr.number(6);
  static DECIMICROS = LiteralExpr.number(7);
  static CENTIMICROS = LiteralExpr.number(8);
  static NANOS = LiteralExpr.number(9);

  static {
    this.register();
  }
}

export type UnixToTimeStrExprArgs = Merge<[
  FuncExprArgs,
]>;

export class UnixToTimeStrExpr extends FuncExpr {
  static key = ExpressionKey.UNIX_TO_TIME_STR;

  static argOrder = ['this'];

  declare args: UnixToTimeStrExprArgs;

  constructor (args: UnixToTimeStrExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnixSecondsExprArgs = Merge<[
  FuncExprArgs,
  { this?: Expression },
]>;

export class UnixSecondsExpr extends FuncExpr {
  static key = ExpressionKey.UNIX_SECONDS;

  static argOrder = ['this'];

  declare args: UnixSecondsExprArgs;

  constructor (args: UnixSecondsExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnixMicrosExprArgs = Merge<[
  FuncExprArgs,
]>;

export class UnixMicrosExpr extends FuncExpr {
  static key = ExpressionKey.UNIX_MICROS;

  static argOrder = ['this'];

  declare args: UnixMicrosExprArgs;

  constructor (args: UnixMicrosExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UnixMillisExprArgs = Merge<[
  FuncExprArgs,
]>;

export class UnixMillisExpr extends FuncExpr {
  static key = ExpressionKey.UNIX_MILLIS;

  static argOrder = ['this'];

  declare args: UnixMillisExprArgs;

  constructor (args: UnixMillisExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UuidExprArgs = Merge<[
  FuncExprArgs,
  {
    name?: Expression;
    isString?: boolean;
    this?: Expression;
  },
]>;

export class UuidExpr extends FuncExpr {
  static key = ExpressionKey.UUID;

  static _sqlNames = [
    'UUID',
    'GEN_RANDOM_UUID',
    'GENERATE_UUID',
    'UUID_STRING',
  ];

  static availableArgs = new Set([
    'this',
    'name',
    'isString',
  ]);

  static argOrder = [
    'name',
    'isString',
    'this',
  ];

  declare args: UuidExprArgs;

  constructor (args: UuidExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export const TIMESTAMP_PARTS = {
  year: false,
  month: false,
  day: false,
  hour: false,
  min: false,
  sec: false,
  nano: false,
} as const;

export type TimestampFromPartsExprArgs = Merge<[
  FuncExprArgs,
  {
    year?: Expression;
    month?: Expression;
    day?: Expression;
    hour?: Expression;
    min?: Expression;
    sec?: Expression;
    nano?: Expression;
    zone?: Expression;
    milli?: Expression;
    this?: Expression;
    expression?: ExpressionOrString | undefined;
  },
]>;

export class TimestampFromPartsExpr extends FuncExpr {
  static key = ExpressionKey.TIMESTAMP_FROM_PARTS;
  static _sqlNames = ['TIMESTAMP_FROM_PARTS', 'TIMESTAMPFROMPARTS'];

  static availableArgs = new Set([
    'year',
    'month',
    'day',
    'hour',
    'min',
    'sec',
    'nano',
    'zone',
    'milli',
    'this',
    'expression',
  ]);

  static argOrder = [
    'zone',
    'milli',
    'this',
    'expression',
  ];

  declare args: TimestampFromPartsExprArgs;

  constructor (args: TimestampFromPartsExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TimestampLtzFromPartsExprArgs = Merge<[
  FuncExprArgs,
  {
    year?: Expression;
    month?: Expression;
    day?: Expression;
    hour?: Expression;
    min?: Expression;
    sec?: Expression;
    nano?: Expression;
  },
]>;

export class TimestampLtzFromPartsExpr extends FuncExpr {
  static key = ExpressionKey.TIMESTAMP_LTZ_FROM_PARTS;

  static _sqlNames = ['TIMESTAMP_LTZ_FROM_PARTS', 'TIMESTAMPLTZFROMPARTS'];

  static availableArgs = new Set([
    'year',
    'month',
    'day',
    'hour',
    'min',
    'sec',
    'nano',
  ]);

  declare args: TimestampLtzFromPartsExprArgs;

  static {
    this.register();
  }

  constructor (args: TimestampLtzFromPartsExprArgs = {}) {
    super(args);
  }
}

export type TimestampTzFromPartsExprArgs = Merge<[
  FuncExprArgs,
  {
    year?: Expression;
    month?: Expression;
    day?: Expression;
    hour?: Expression;
    min?: Expression;
    sec?: Expression;
    nano?: Expression;
    zone?: Expression;
  },
]>;

export class TimestampTzFromPartsExpr extends FuncExpr {
  static key = ExpressionKey.TIMESTAMP_TZ_FROM_PARTS;
  static _sqlNames = ['TIMESTAMP_TZ_FROM_PARTS', 'TIMESTAMPTZFROMPARTS'];

  static availableArgs = new Set([
    'year',
    'month',
    'day',
    'hour',
    'min',
    'sec',
    'nano',
    'zone',
  ]);

  static argOrder = ['zone'];

  declare args: TimestampTzFromPartsExprArgs;

  constructor (args: TimestampTzFromPartsExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type UpperExprArgs = Merge<[
  FuncExprArgs,
]>;

export class UpperExpr extends FuncExpr {
  static key = ExpressionKey.UPPER;

  static argOrder = ['this'];

  declare args: UpperExprArgs;

  constructor (args: UpperExprArgs = {}) {
    super(args);
  }

  static _sqlNames = ['UPPER', 'UCASE'];

  static {
    this.register();
  }
}

export type CorrExprArgs = Merge<[
  BinaryExprArgs,
  AggFuncExprArgs,
  {
    nullOnZeroVariance?: boolean | Expression;
    this?: Expression;
    expression?: Expression;
    expressions?: Expression[];
  },
]>;

export class CorrExpr extends multiInherit(BinaryExpr, AggFuncExpr) {
  static key = ExpressionKey.CORR;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'nullOnZeroVariance',
  ]);

  declare args: CorrExprArgs;

  constructor (args: CorrExprArgs = {}) {
    super(args);
  }
}

export type WidthBucketExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    minValue?: string;
    maxValue?: string;
    numBuckets?: Expression[];
    threshold?: Expression;
  },
]>;

export class WidthBucketExpr extends FuncExpr {
  static key = ExpressionKey.WIDTH_BUCKET;

  static availableArgs = new Set([
    'this',
    'minValue',
    'maxValue',
    'numBuckets',
    'threshold',
  ]);

  static argOrder = [
    'this',
    'minValue',
    'maxValue',
    'numBuckets',
    'threshold',
  ];

  declare args: WidthBucketExprArgs;

  constructor (args: WidthBucketExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type WeekExprArgs = Merge<[
  FuncExprArgs,
  {
    mode?: Expression;
    this?: Expression;
  },
]>;

export class WeekExpr extends FuncExpr {
  static key = ExpressionKey.WEEK;

  static availableArgs = new Set(['this', 'mode']);

  static argOrder = ['mode', 'this'];

  declare args: WeekExprArgs;

  constructor (args: WeekExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type NextDayExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
  },
]>;

export class NextDayExpr extends FuncExpr {
  static key = ExpressionKey.NEXT_DAY;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  static argOrder = ['this', 'expression'];

  declare args: NextDayExprArgs;

  constructor (args: NextDayExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type XmlElementExprArgs = Merge<[
  FuncExprArgs,
  {
    evalname?: string;
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class XmlElementExpr extends FuncExpr {
  static key = ExpressionKey.XML_ELEMENT;

  static _sqlNames = ['XmlELEMENT'];

  static availableArgs = new Set([
    'this',
    'expressions',
    'evalname',
  ]);

  static argOrder = [
    'evalname',
    'this',
    'expressions',
  ];

  declare args: XmlElementExprArgs;

  constructor (args: XmlElementExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type XmlGetExprArgs = Merge<[
  FuncExprArgs,
  {
    instance?: Expression;
    this?: Expression;
    expression?: Expression;
  },
]>;

export class XmlGetExpr extends FuncExpr {
  static key = ExpressionKey.XML_GET;
  static _sqlNames = ['XmlGET'];

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'instance',
  ]);

  static argOrder = [
    'instance',
    'this',
    'expression',
  ];

  declare args: XmlGetExprArgs;

  constructor (args: XmlGetExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type XmlTableExprArgs = Merge<[
  FuncExprArgs,
  {
    this?: Expression;
    namespaces?: Expression[];
    passing?: Expression;
    columns?: Expression[];
    byRef?: Expression;
  },
]>;

export class XmlTableExpr extends FuncExpr {
  static key = ExpressionKey.XML_TABLE;

  static availableArgs = new Set([
    'this',
    'namespaces',
    'passing',
    'columns',
    'byRef',
  ]);

  static argOrder = [
    'this',
    'namespaces',
    'passing',
    'columns',
    'byRef',
  ];

  declare args: XmlTableExprArgs;

  constructor (args: XmlTableExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type YearExprArgs = Merge<[
  FuncExprArgs,
]>;

export class YearExpr extends FuncExpr {
  static key = ExpressionKey.YEAR;

  static argOrder = ['this'];

  declare args: YearExprArgs;

  constructor (args: YearExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ZipfExprArgs = Merge<[
  FuncExprArgs,
  {
    elementcount?: Expression;
    gen?: Expression;
    this?: Expression;
  },
]>;

export class ZipfExpr extends FuncExpr {
  static key = ExpressionKey.ZIPF;

  static requiredArgs = new Set([
    'this',
    'elementcount',
    'gen',
  ]);

  static availableArgs = new Set([
    'this',
    'elementcount',
    'gen',
  ]);

  static argOrder = [
    'elementcount',
    'gen',
    'this',
  ];

  declare args: ZipfExprArgs;

  constructor (args: ZipfExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type NextValueForExprArgs = Merge<[
  FuncExprArgs,
  {
    order?: Expression;
    this?: Expression;
  },
]>;

export class NextValueForExpr extends FuncExpr {
  static key = ExpressionKey.NEXT_VALUE_FOR;

  static availableArgs = new Set(['this', 'order']);

  static argOrder = ['order', 'this'];

  declare args: NextValueForExprArgs;

  constructor (args: NextValueForExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AllExprArgs = Merge<[
  SubqueryPredicateExprArgs,
  {
    this?: Expression;
  },
]>;

export class AllExpr extends SubqueryPredicateExpr {
  static key = ExpressionKey.ALL;

  declare args: AllExprArgs;

  constructor (args: AllExprArgs = {}) {
    super(args);
  }
}

export type AnyExprArgs = Merge<[
  SubqueryPredicateExprArgs,
  {
    this?: Expression;
  },
]>;

export class AnyExpr extends SubqueryPredicateExpr {
  static key = ExpressionKey.ANY;

  declare args: AnyExprArgs;

  constructor (args: AnyExprArgs = {}) {
    super(args);
  }
}

export type BitwiseAndAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class BitwiseAndAggExpr extends AggFuncExpr {
  static key = ExpressionKey.BITWISE_AND_AGG;

  declare args: BitwiseAndAggExprArgs;

  constructor (args: BitwiseAndAggExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitwiseOrAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class BitwiseOrAggExpr extends AggFuncExpr {
  static key = ExpressionKey.BITWISE_OR_AGG;

  declare args: BitwiseOrAggExprArgs;

  constructor (args: BitwiseOrAggExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitwiseXorAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class BitwiseXorAggExpr extends AggFuncExpr {
  static key = ExpressionKey.BITWISE_XOR_AGG;

  declare args: BitwiseXorAggExprArgs;

  constructor (args: BitwiseXorAggExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BoolxorAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class BoolxorAggExpr extends AggFuncExpr {
  static key = ExpressionKey.BOOLXOR_AGG;

  declare args: BoolxorAggExprArgs;

  constructor (args: BoolxorAggExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitmapConstructAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class BitmapConstructAggExpr extends AggFuncExpr {
  static key = ExpressionKey.BITMAP_CONSTRUCT_AGG;

  declare args: BitmapConstructAggExprArgs;

  constructor (args: BitmapConstructAggExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type BitmapOrAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class BitmapOrAggExpr extends AggFuncExpr {
  static key = ExpressionKey.BITMAP_OR_AGG;

  declare args: BitmapOrAggExprArgs;

  constructor (args: BitmapOrAggExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ParameterizedAggExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
    params?: Expression[];
  },
]>;

export class ParameterizedAggExpr extends AggFuncExpr {
  static key = ExpressionKey.PARAMETERIZED_AGG;

  static requiredArgs = new Set([
    'this',
    'expressions',
    'params',
  ]);

  static availableArgs = new Set([
    'this',
    'expressions',
    'params',
  ]);

  declare args: ParameterizedAggExprArgs;

  constructor (args: ParameterizedAggExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArgMaxExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    count?: Expression;
  },
]>;

export class ArgMaxExpr extends AggFuncExpr {
  static key = ExpressionKey.ARG_MAX;

  static _sqlNames = [
    'ARG_MAX',
    'ARGMAX',
    'MAX_BY',
  ];

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'count',
  ]);

  declare args: ArgMaxExprArgs;

  constructor (args: ArgMaxExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArgMinExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    count?: Expression;
  },
]>;

export class ArgMinExpr extends AggFuncExpr {
  static key = ExpressionKey.ARG_MIN;

  static _sqlNames = [
    'ARG_MIN',
    'ARGMIN',
    'MIN_BY',
  ];

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set([
    'this',
    'expression',
    'count',
  ]);

  declare args: ArgMinExprArgs;

  constructor (args: ArgMinExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ApproxTopKExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString | undefined;
    counters?: Expression;
  },
]>;

export class ApproxTopKExpr extends AggFuncExpr {
  static key = ExpressionKey.APPROX_TOP_K;

  static availableArgs = new Set([
    'this',
    'expression',
    'counters',
  ]);

  declare args: ApproxTopKExprArgs;

  constructor (args: ApproxTopKExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/approx_top_k_accumulate
 * https://spark.apache.org/docs/preview/api/sql/index.html#approx_top_k_accumulate
 */
export type ApproxTopKAccumulateExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
  },
]>;

export class ApproxTopKAccumulateExpr extends AggFuncExpr {
  static key = ExpressionKey.APPROX_TOP_K_ACCUMULATE;

  static availableArgs = new Set(['this', 'expression']);

  declare args: ApproxTopKAccumulateExprArgs;

  constructor (args: ApproxTopKAccumulateExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/approx_top_k_combine
 */
export type ApproxTopKCombineExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
  },
]>;

export class ApproxTopKCombineExpr extends AggFuncExpr {
  static key = ExpressionKey.APPROX_TOP_K_COMBINE;

  static availableArgs = new Set(['this', 'expression']);

  declare args: ApproxTopKCombineExprArgs;

  constructor (args: ApproxTopKCombineExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ApproxTopSumExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    count?: Expression;
  },
]>;

export class ApproxTopSumExpr extends AggFuncExpr {
  static key = ExpressionKey.APPROX_TOP_SUM;

  static requiredArgs = new Set([
    'this',
    'expression',
    'count',
  ]);

  static availableArgs = new Set([
    'this',
    'expression',
    'count',
  ]);

  declare args: ApproxTopSumExprArgs;

  constructor (args: ApproxTopSumExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ApproxQuantilesExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
  },
]>;

export class ApproxQuantilesExpr extends AggFuncExpr {
  static key = ExpressionKey.APPROX_QUANTILES;

  static availableArgs = new Set(['this', 'expression']);

  declare args: ApproxQuantilesExprArgs;

  constructor (args: ApproxQuantilesExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/approx_percentile_combine
 */
export type ApproxPercentileCombineExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class ApproxPercentileCombineExpr extends AggFuncExpr {
  static key = ExpressionKey.APPROX_PERCENTILE_COMBINE;

  declare args: ApproxPercentileCombineExprArgs;

  constructor (args: ApproxPercentileCombineExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/minhash
 */
export type MinhashExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class MinhashExpr extends AggFuncExpr {
  static key = ExpressionKey.MINHASH;

  static isVarLenArgs = true;

  static requiredArgs = new Set(['this', 'expressions']);

  static availableArgs = new Set(['this', 'expressions']);

  declare args: MinhashExprArgs;

  constructor (args: MinhashExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/minhash_combine
 */
export type MinhashCombineExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class MinhashCombineExpr extends AggFuncExpr {
  static key = ExpressionKey.MINHASH_COMBINE;

  declare args: MinhashCombineExprArgs;

  constructor (args: MinhashCombineExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/approximate_similarity
 */
export type ApproximateSimilarityExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class ApproximateSimilarityExpr extends AggFuncExpr {
  static key = ExpressionKey.APPROXIMATE_SIMILARITY;

  static _sqlNames = ['APPROXIMATE_SIMILARITY', 'APPROXIMATE_JACCARD_INDEX'];

  declare args: ApproximateSimilarityExprArgs;

  constructor (args: ApproximateSimilarityExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type GroupingExprArgs = Merge<[
  AggFuncExprArgs,
  { expressions?: Expression[] },
]>;

export class GroupingExpr extends AggFuncExpr {
  static key = ExpressionKey.GROUPING;

  static isVarLenArgs = true;

  static requiredArgs = new Set(['expressions']);

  static availableArgs = new Set(['expressions']);

  declare args: GroupingExprArgs;

  constructor (args: GroupingExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type GroupingIdExprArgs = Merge<[
  AggFuncExprArgs,
  { expressions?: Expression[] },
]>;

export class GroupingIdExpr extends AggFuncExpr {
  static key = ExpressionKey.GROUPING_ID;

  static isVarLenArgs = true;

  static availableArgs = new Set(['expressions']);

  declare args: GroupingIdExprArgs;

  constructor (args: GroupingIdExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AnonymousAggFuncExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: string | Expression;
    expressions?: Expression[];
  },
]>;

export class AnonymousAggFuncExpr extends AggFuncExpr {
  static key = ExpressionKey.ANONYMOUS_AGG_FUNC;

  static isVarLenArgs = true;

  static availableArgs = new Set(['this', 'expressions']);

  declare args: AnonymousAggFuncExprArgs;

  constructor (args: AnonymousAggFuncExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/hash_agg
 */
export type HashAggExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class HashAggExpr extends AggFuncExpr {
  static key = ExpressionKey.HASH_AGG;

  static isVarLenArgs = true;

  static availableArgs = new Set(['this', 'expressions']);

  declare args: HashAggExprArgs;

  constructor (args: HashAggExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * https://docs.snowflake.com/en/sql-reference/functions/hll
 * https://docs.aws.amazon.com/redshift/latest/dg/r_HLL_function.html
 */
export type HllExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class HllExpr extends AggFuncExpr {
  static key = ExpressionKey.HLL;

  static isVarLenArgs = true;

  static availableArgs = new Set(['this', 'expressions']);

  declare args: HllExprArgs;

  constructor (args: HllExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ApproxDistinctExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    accuracy?: Expression;
  },
]>;

export class ApproxDistinctExpr extends AggFuncExpr {
  static key = ExpressionKey.APPROX_DISTINCT;

  static _sqlNames = ['APPROX_DISTINCT', 'APPROX_COUNT_DISTINCT'];

  static availableArgs = new Set(['this', 'accuracy']);

  declare args: ApproxDistinctExprArgs;

  constructor (args: ApproxDistinctExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * Postgres' GENERATE_SERIES function returns a row set, i.e. it implicitly explodes when it's
 * used in a projection, so this expression is a helper that facilitates transpilation to other
 * dialects. For example, we'd generate UNNEST(GENERATE_SERIES(...)) in DuckDB
 */
export type ExplodingGenerateSeriesExprArgs = Merge<[
  GenerateSeriesExprArgs,
]>;

export class ExplodingGenerateSeriesExpr extends GenerateSeriesExpr {
  static key = ExpressionKey.EXPLODING_GENERATE_SERIES;

  static {
    this.register();
  }
}

export type ArrayAggExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: ExpressionValue;
    nullsExcluded?: boolean;
  },
]>;

export class ArrayAggExpr extends AggFuncExpr {
  static key = ExpressionKey.ARRAY_AGG;

  static availableArgs = new Set(['this', 'nullsExcluded']);

  declare args: ArrayAggExprArgs;

  constructor (args: ArrayAggExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayUniqueAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class ArrayUniqueAggExpr extends AggFuncExpr {
  static key = ExpressionKey.ARRAY_UNIQUE_AGG;

  declare args: ArrayUniqueAggExprArgs;

  constructor (args: ArrayUniqueAggExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AiAggExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class AiAggExpr extends AggFuncExpr {
  static key = ExpressionKey.AI_AGG;

  static _sqlNames = ['AI_AGG'];

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: AiAggExprArgs;

  constructor (args: AiAggExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AiSummarizeAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class AiSummarizeAggExpr extends AggFuncExpr {
  static key = ExpressionKey.AI_SUMMARIZE_AGG;

  static _sqlNames = ['AI_SUMMARIZE_AGG'];

  declare args: AiSummarizeAggExprArgs;

  constructor (args: AiSummarizeAggExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayConcatAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class ArrayConcatAggExpr extends AggFuncExpr {
  static key = ExpressionKey.ARRAY_CONCAT_AGG;

  declare args: ArrayConcatAggExprArgs;

  constructor (args: ArrayConcatAggExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ArrayUnionAggExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class ArrayUnionAggExpr extends AggFuncExpr {
  static key = ExpressionKey.ARRAY_UNION_AGG;

  declare args: ArrayUnionAggExprArgs;

  constructor (args: ArrayUnionAggExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AvgExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class AvgExpr extends AggFuncExpr {
  static key = ExpressionKey.AVG;

  declare args: AvgExprArgs;

  constructor (args: AvgExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type AnyValueExprArgs = Merge<[
  AggFuncExprArgs,
  { max?: ExpressionValue },
]>;

export class AnyValueExpr extends AggFuncExpr {
  static key = ExpressionKey.ANY_VALUE;

  declare args: AnyValueExprArgs;

  constructor (args: AnyValueExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LagExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    offset?: Expression;
    default?: Expression;
  },
]>;

export class LagExpr extends AggFuncExpr {
  static key = ExpressionKey.LAG;

  static availableArgs = new Set([
    'this',
    'offset',
    'default',
  ]);

  declare args: LagExprArgs;

  constructor (args: LagExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LeadExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    offset?: Expression;
    default?: Expression;
  },
]>;

export class LeadExpr extends AggFuncExpr {
  static key = ExpressionKey.LEAD;

  static availableArgs = new Set([
    'this',
    'offset',
    'default',
  ]);

  declare args: LeadExprArgs;

  constructor (args: LeadExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type FirstExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
  },
]>;

export class FirstExpr extends AggFuncExpr {
  static key = ExpressionKey.FIRST;

  static availableArgs = new Set(['this', 'expression']);

  declare args: FirstExprArgs;

  constructor (args: FirstExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LastExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: ExpressionOrString;
  },
]>;

export class LastExpr extends AggFuncExpr {
  static key = ExpressionKey.LAST;

  static availableArgs = new Set(['this', 'expression']);

  declare args: LastExprArgs;

  constructor (args: LastExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type FirstValueExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class FirstValueExpr extends AggFuncExpr {
  static key = ExpressionKey.FIRST_VALUE;

  declare args: FirstValueExprArgs;

  constructor (args: FirstValueExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LastValueExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class LastValueExpr extends AggFuncExpr {
  static key = ExpressionKey.LAST_VALUE;

  declare args: LastValueExprArgs;

  constructor (args: LastValueExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type NthValueExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    offset?: Expression;
    fromFirst?: Expression;
  },
]>;

export class NthValueExpr extends AggFuncExpr {
  static key = ExpressionKey.NTH_VALUE;

  static requiredArgs = new Set(['this', 'offset']);

  static availableArgs = new Set([
    'this',
    'offset',
    'fromFirst',
  ]);

  declare args: NthValueExprArgs;

  constructor (args: NthValueExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ObjectAggExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class ObjectAggExpr extends AggFuncExpr {
  static key = ExpressionKey.OBJECT_AGG;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: ObjectAggExprArgs;

  constructor (args: ObjectAggExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type TryCastExprArgs = Merge<[
  CastExprArgs,
  {
    this?: Expression;
    requiresString?: boolean | Expression;
  },
]>;

export class TryCastExpr extends CastExpr {
  static key = ExpressionKey.TRY_CAST;

  static availableArgs = new Set([
    'this',
    'to',
    'format',
    'safe',
    'action',
    'default',
    'requiresString',
  ]);

  declare args: TryCastExprArgs;

  constructor (args: TryCastExprArgs = {}) {
    super(args);
  }
}

export type JsonCastExprArgs = Merge<[
  CastExprArgs,
]>;

export class JsonCastExpr extends CastExpr {
  static key = ExpressionKey.JSON_CAST;

  declare args: JsonCastExprArgs;

  constructor (args: JsonCastExprArgs = {}) {
    super(args);
  }
}

export type ConcatWsExprArgs = Merge<[
  ConcatExprArgs,
]>;

export class ConcatWsExpr extends ConcatExpr {
  static key = ExpressionKey.CONCAT_WS;

  static _sqlNames = ['CONCAT_WS'];

  declare args: ConcatWsExprArgs;

  constructor (args: ConcatWsExprArgs = {}) {
    super(args);
  }
}

export type CountExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
    bigInt?: ExpressionOrBoolean;
  },
]>;

export class CountExpr extends AggFuncExpr {
  static key = ExpressionKey.COUNT;

  static isVarLenArgs = true;

  static availableArgs = new Set([
    'this',
    'expressions',
    'bigInt',
  ]);

  declare args: CountExprArgs;

  constructor (args: CountExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CountIfExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class CountIfExpr extends AggFuncExpr {
  static key = ExpressionKey.COUNT_IF;

  declare args: CountIfExprArgs;

  constructor (args: CountIfExprArgs = {}) {
    super(args);
  }

  static _sqlNames = ['COUNT_IF', 'COUNTIF'];

  static {
    this.register();
  }
}

export type DenseRankExprArgs = Merge<[
  AggFuncExprArgs,
  { expressions?: Expression[] },
]>;

export class DenseRankExpr extends AggFuncExpr {
  static key = ExpressionKey.DENSE_RANK;

  static isVarLenArgs = true;

  static availableArgs = new Set(['expressions']);

  declare args: DenseRankExprArgs;

  constructor (args: DenseRankExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ExplodeOuterExprArgs = Merge<[
  ExplodeExprArgs,
]>;
export class ExplodeOuterExpr extends ExplodeExpr {
  static key = ExpressionKey.EXPLODE_OUTER;

  declare args: ExplodeOuterExprArgs;

  constructor (args: ExplodeOuterExprArgs = {}) {
    super(args);
  }
}

export type PosexplodeExprArgs = Merge<[
  ExplodeExprArgs,
]>;
export class PosexplodeExpr extends ExplodeExpr {
  static key = ExpressionKey.POSEXPLODE;

  declare args: PosexplodeExprArgs;

  constructor (args: PosexplodeExprArgs = {}) {
    super(args);
  }
}

export type GroupConcatExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    separator?: string;
    onOverflow?: Expression;
  },
]>;

export class GroupConcatExpr extends AggFuncExpr {
  static key = ExpressionKey.GROUP_CONCAT;

  static availableArgs = new Set([
    'this',
    'separator',
    'onOverflow',
  ]);

  declare args: GroupConcatExprArgs;

  constructor (args: GroupConcatExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LowerHexExprArgs = Merge<[
  HexExprArgs,
]>;
export class LowerHexExpr extends HexExpr {
  static key = ExpressionKey.LOWER_HEX;

  declare args: LowerHexExprArgs;

  constructor (args: LowerHexExprArgs = {}) {
    super(args);
  }
}

export type AndExprArgs = Merge<[
  ConnectorExprArgs,
]>;
export class AndExpr extends multiInherit(ConnectorExpr, FuncExpr) {
  static key = ExpressionKey.AND;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: AndExprArgs;

  constructor (args: AndExprArgs = {}) {
    super(args);
  }
}

export type OrExprArgs = Merge<[
  ConnectorExprArgs,
  { this?: string | Expression },
]>;
export class OrExpr extends multiInherit(ConnectorExpr, FuncExpr) {
  static key = ExpressionKey.OR;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: OrExprArgs;

  constructor (args: OrExprArgs = {}) {
    super(args);
  }
}

export type XorExprArgs = Merge<[
  ConnectorExprArgs,
  FuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
    expressions?: Expression[];
    roundInput?: boolean | Expression;
  },
]>;

export class XorExpr extends multiInherit(ConnectorExpr, FuncExpr) {
  static key = ExpressionKey.XOR;

  static isVarLenArgs = true;

  // Note: multiInherit classes don't have direct super access, so we create new Sets
  static availableArgs = new Set([
    'this',
    'expression',
    'expressions',
    'roundInput',
  ]);

  declare args: XorExprArgs;

  constructor (args: XorExprArgs = {}) {
    super(args);
  }
}

export type JsonObjectAggExprArgs = Merge<[
  AggFuncExprArgs,
  {
    nullHandling?: Expression;
    uniqueKeys?: Expression[];
    returnType?: Expression;
    encoding?: Expression;
  },
]>;

export class JsonObjectAggExpr extends AggFuncExpr {
  static key = ExpressionKey.JSON_OBJECT_AGG;

  static availableArgs = new Set([
    'expressions',
    'nullHandling',
    'uniqueKeys',
    'returnType',
    'encoding',
  ]);

  declare args: JsonObjectAggExprArgs;

  constructor (args: JsonObjectAggExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JsonbObjectAggExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class JsonbObjectAggExpr extends AggFuncExpr {
  static key = ExpressionKey.JSONB_OBJECT_AGG;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: JsonbObjectAggExprArgs;

  constructor (args: JsonbObjectAggExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type JsonArrayAggExprArgs = Merge<[
  AggFuncExprArgs,
  {
    order?: Expression;
    nullHandling?: Expression;
    returnType?: Expression;
    strict?: Expression;
  },
]>;

export class JsonArrayAggExpr extends AggFuncExpr {
  static key = ExpressionKey.JSON_ARRAY_AGG;

  static availableArgs = new Set([
    'this',
    'order',
    'nullHandling',
    'returnType',
    'strict',
  ]);

  declare args: JsonArrayAggExprArgs;

  constructor (args: JsonArrayAggExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type LogicalOrExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class LogicalOrExpr extends AggFuncExpr {
  static key = ExpressionKey.LOGICAL_OR;

  declare args: LogicalOrExprArgs;

  constructor (args: LogicalOrExprArgs = {}) {
    super(args);
  }

  static _sqlNames = [
    'LOGICAL_OR',
    'BOOL_OR',
    'BOOLOR_AGG',
  ];

  static {
    this.register();
  }
}

export type LogicalAndExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class LogicalAndExpr extends AggFuncExpr {
  static key = ExpressionKey.LOGICAL_AND;

  declare args: LogicalAndExprArgs;

  constructor (args: LogicalAndExprArgs = {}) {
    super(args);
  }

  static _sqlNames = [
    'LOGICAL_AND',
    'BOOL_AND',
    'BOOLAND_AGG',
  ];

  static {
    this.register();
  }
}

export type MaxExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class MaxExpr extends AggFuncExpr {
  static key = ExpressionKey.MAX;

  static isVarLenArgs = true;

  static availableArgs = new Set(['this', 'expressions']);

  declare args: MaxExprArgs;

  constructor (args: MaxExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MedianExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class MedianExpr extends AggFuncExpr {
  static key = ExpressionKey.MEDIAN;

  declare args: MedianExprArgs;

  constructor (args: MedianExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ModeExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    deterministic?: Expression;
  },
]>;

export class ModeExpr extends AggFuncExpr {
  static key = ExpressionKey.MODE;

  static availableArgs = new Set(['this', 'deterministic']);

  declare args: ModeExprArgs;

  constructor (args: ModeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type MinExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
  },
]>;

export class MinExpr extends AggFuncExpr {
  static key = ExpressionKey.MIN;

  static isVarLenArgs = true;

  static availableArgs = new Set(['this', 'expressions']);

  declare args: MinExprArgs;

  constructor (args: MinExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type NtileExprArgs = Merge<[
  AggFuncExprArgs,
  { this?: Expression },
]>;

export class NtileExpr extends AggFuncExpr {
  static key = ExpressionKey.NTILE;

  declare args: NtileExprArgs;

  constructor (args: NtileExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type PercentileContExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression | undefined;
  },
]>;

export class PercentileContExpr extends AggFuncExpr {
  static key = ExpressionKey.PERCENTILE_CONT;

  static availableArgs = new Set(['this', 'expression']);

  declare args: PercentileContExprArgs;

  constructor (args: PercentileContExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type PercentileDiscExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class PercentileDiscExpr extends AggFuncExpr {
  static key = ExpressionKey.PERCENTILE_DISC;

  static availableArgs = new Set(['this', 'expression']);

  declare args: PercentileDiscExprArgs;

  constructor (args: PercentileDiscExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type PercentRankExprArgs = Merge<[
  AggFuncExprArgs,
  { expressions?: Expression[] },
]>;

export class PercentRankExpr extends AggFuncExpr {
  static key = ExpressionKey.PERCENT_RANK;

  static isVarLenArgs = true;

  static availableArgs = new Set(['expressions']);

  declare args: PercentRankExprArgs;

  constructor (args: PercentRankExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type QuantileExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    quantile?: ExpressionValue;
  },
]>;

export class QuantileExpr extends AggFuncExpr {
  static key = ExpressionKey.QUANTILE;

  static requiredArgs = new Set(['this', 'quantile']);

  static availableArgs = new Set(['this', 'quantile']);

  declare args: QuantileExprArgs;

  constructor (args: QuantileExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type ApproxPercentileAccumulateExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class ApproxPercentileAccumulateExpr extends AggFuncExpr {
  static key = ExpressionKey.APPROX_PERCENTILE_ACCUMULATE;

  declare args: ApproxPercentileAccumulateExprArgs;

  constructor (args: ApproxPercentileAccumulateExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RankExprArgs = Merge<[
  AggFuncExprArgs,
  { expressions?: Expression[] },
]>;

export class RankExpr extends AggFuncExpr {
  static key = ExpressionKey.RANK;

  static isVarLenArgs = true;

  static availableArgs = new Set(['expressions']);

  declare args: RankExprArgs;

  constructor (args: RankExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrValxExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class RegrValxExpr extends AggFuncExpr {
  static key = ExpressionKey.REGR_VALX;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: RegrValxExprArgs;

  constructor (args: RegrValxExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrValyExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class RegrValyExpr extends AggFuncExpr {
  static key = ExpressionKey.REGR_VALY;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: RegrValyExprArgs;

  constructor (args: RegrValyExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrAvgyExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class RegrAvgyExpr extends AggFuncExpr {
  static key = ExpressionKey.REGR_AVGY;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: RegrAvgyExprArgs;

  constructor (args: RegrAvgyExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrAvgxExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class RegrAvgxExpr extends AggFuncExpr {
  static key = ExpressionKey.REGR_AVGX;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: RegrAvgxExprArgs;

  constructor (args: RegrAvgxExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrCountExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class RegrCountExpr extends AggFuncExpr {
  static key = ExpressionKey.REGR_COUNT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: RegrCountExprArgs;

  constructor (args: RegrCountExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrInterceptExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class RegrInterceptExpr extends AggFuncExpr {
  static key = ExpressionKey.REGR_INTERCEPT;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: RegrInterceptExprArgs;

  constructor (args: RegrInterceptExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrR2ExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class RegrR2Expr extends AggFuncExpr {
  static key = ExpressionKey.REGR_R2;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: RegrR2ExprArgs;

  constructor (args: RegrR2ExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrSxxExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class RegrSxxExpr extends AggFuncExpr {
  static key = ExpressionKey.REGR_SXX;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: RegrSxxExprArgs;

  constructor (args: RegrSxxExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrSxyExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class RegrSxyExpr extends AggFuncExpr {
  static key = ExpressionKey.REGR_SXY;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: RegrSxyExprArgs;

  constructor (args: RegrSxyExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrSyyExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class RegrSyyExpr extends AggFuncExpr {
  static key = ExpressionKey.REGR_SYY;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: RegrSyyExprArgs;

  constructor (args: RegrSyyExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type RegrSlopeExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class RegrSlopeExpr extends AggFuncExpr {
  static key = ExpressionKey.REGR_SLOPE;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: RegrSlopeExprArgs;

  constructor (args: RegrSlopeExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SumExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class SumExpr extends AggFuncExpr {
  static key = ExpressionKey.SUM;

  declare args: SumExprArgs;

  constructor (args: SumExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StddevExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class StddevExpr extends AggFuncExpr {
  static key = ExpressionKey.STDDEV;

  declare args: StddevExprArgs;

  constructor (args: StddevExprArgs = {}) {
    super(args);
  }

  static _sqlNames = ['STDDEV', 'STDEV'];

  static {
    this.register();
  }
}

export type StddevPopExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class StddevPopExpr extends AggFuncExpr {
  static key = ExpressionKey.STDDEV_POP;

  declare args: StddevPopExprArgs;

  constructor (args: StddevPopExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type StddevSampExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class StddevSampExpr extends AggFuncExpr {
  static key = ExpressionKey.STDDEV_SAMP;

  declare args: StddevSampExprArgs;

  constructor (args: StddevSampExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CumeDistExprArgs = Merge<[
  AggFuncExprArgs,
  { expressions?: Expression[] },
]>;

export class CumeDistExpr extends AggFuncExpr {
  static key = ExpressionKey.CUME_DIST;

  static isVarLenArgs = true;
  static availableArgs = new Set(['expressions']);

  declare args: CumeDistExprArgs;

  constructor (args: CumeDistExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type VarianceExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class VarianceExpr extends AggFuncExpr {
  static key = ExpressionKey.VARIANCE;

  declare args: VarianceExprArgs;

  constructor (args: VarianceExprArgs = {}) {
    super(args);
  }

  static _sqlNames = [
    'VARIANCE',
    'VARIANCE_SAMP',
    'VAR_SAMP',
  ];

  static {
    this.register();
  }
}

export type VariancePopExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class VariancePopExpr extends AggFuncExpr {
  static key = ExpressionKey.VARIANCE_POP;

  declare args: VariancePopExprArgs;

  constructor (args: VariancePopExprArgs = {}) {
    super(args);
  }

  static _sqlNames = ['VARIANCE_POP', 'VAR_POP'];

  static {
    this.register();
  }
}

export type KurtosisExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class KurtosisExpr extends AggFuncExpr {
  static key = ExpressionKey.KURTOSIS;

  declare args: KurtosisExprArgs;

  constructor (args: KurtosisExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type SkewnessExprArgs = Merge<[
  AggFuncExprArgs,
]>;

export class SkewnessExpr extends AggFuncExpr {
  static key = ExpressionKey.SKEWNESS;

  declare args: SkewnessExprArgs;

  constructor (args: SkewnessExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CovarSampExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class CovarSampExpr extends AggFuncExpr {
  static key = ExpressionKey.COVAR_SAMP;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: CovarSampExprArgs;

  constructor (args: CovarSampExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CovarPopExprArgs = Merge<[
  AggFuncExprArgs,
  {
    this?: Expression;
    expression?: Expression;
  },
]>;

export class CovarPopExpr extends AggFuncExpr {
  static key = ExpressionKey.COVAR_POP;

  static requiredArgs = new Set(['this', 'expression']);

  static availableArgs = new Set(['this', 'expression']);

  declare args: CovarPopExprArgs;

  constructor (args: CovarPopExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

/**
 * https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators
 */
export type CombinedAggFuncExprArgs = Merge<[
  AnonymousAggFuncExprArgs,
  {
    this?: string | Expression;
    expressions?: Expression[];
  },
]>;

export class CombinedAggFuncExpr extends AnonymousAggFuncExpr {
  static key = ExpressionKey.COMBINED_AGG_FUNC;

  declare args: CombinedAggFuncExprArgs;

  constructor (args: CombinedAggFuncExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type CombinedParameterizedAggExprArgs = Merge<[
  ParameterizedAggExprArgs,
  {
    this?: Expression;
    expressions?: Expression[];
    params?: Expression[];
  },
]>;

export class CombinedParameterizedAggExpr extends ParameterizedAggExpr {
  static key = ExpressionKey.COMBINED_PARAMETERIZED_AGG;

  declare args: CombinedParameterizedAggExprArgs;

  constructor (args: CombinedParameterizedAggExprArgs = {}) {
    super(args);
  }

  static {
    this.register();
  }
}

export type PosexplodeOuterExprArgs = Merge<[
  ExplodeExprArgs,
]>;
export class PosexplodeOuterExpr extends multiInherit(PosexplodeExpr, ExplodeOuterExpr) {
  static key = ExpressionKey.POSEXPLODE_OUTER;

  static requiredArgs = new Set(['this']);

  static availableArgs = new Set(['this', 'expressions']);

  declare args: PosexplodeOuterExprArgs;

  constructor (args: PosexplodeOuterExprArgs = {}) {
    super(args);
  }
}

export type ApproxQuantileExprArgs = Merge<[
  QuantileExprArgs,
  {
    quantile?: ExpressionValue;
    accuracy?: Expression;
    weight?: Expression;
    errorTolerance?: Expression;
  },
]>;

export class ApproxQuantileExpr extends QuantileExpr {
  static key = ExpressionKey.APPROX_QUANTILE;

  static availableArgs = new Set([
    'this',
    'quantile',
    'accuracy',
    'weight',
    'errorTolerance',
  ]);

  declare args: ApproxQuantileExprArgs;

  constructor (args: ApproxQuantileExprArgs = {}) {
    super(args);
  }
}

/**
 * Create a column expression (optionally qualified with table name).
 *
 * @param name - Column name
 * @param table - Optional table name
 * @returns Column expression
 *
 * @example
 * // Simple column: name
 * const col = column('name');
 *
 * @example
 * // Qualified column: users.id
 * const col = column('id', 'users');
 */
/**
 * Build a Column.
 *
 * Example:
 *     column('col', 'table').sql()
 *     // 'table.col'
 *
 *     column('col', 'table', { fields: ['field1', 'field2'] }).sql()
 *     // 'table.col.field1.field2'
 *
 * @param col - Column name (can be string, Identifier, or Star)
 * @param table - Table name
 * @param options - Options object
 * @param options.db - Database name
 * @param options.catalog - Catalog name
 * @param options.fields - Additional fields using dots
 * @param options.quoted - Whether to force quotes on the column's identifiers
 * @param options.copy - Whether to copy identifiers if passed in
 * @returns The new Column or Dot instance
 */
export function column (
  columnRef: {
    col?: string | IdentifierExpr | StarExpr;
    table?: string | IdentifierExpr;
    db?: string | IdentifierExpr;
    catalog?: string | IdentifierExpr;
  },
  options: {
    fields?: (string | IdentifierExpr)[];
    quoted?: boolean;
    copy?: boolean;
  } = {},
): ColumnExpr | DotExpr {
  const {
    col, table, db, catalog,
  } = columnRef;

  const {
    fields, quoted, copy = true,
  } = options;

  let colIdent: IdentifierExpr | StarExpr | undefined;
  if (col instanceof StarExpr) {
    colIdent = col;
  } else {
    colIdent = toIdentifier(col, {
      quoted,
      copy,
    });
  }

  const columnExpr: ColumnExpr = new ColumnExpr({
    this: colIdent,
    table: table !== undefined
      ? toIdentifier(table, {
        quoted,
        copy,
      })
      : undefined,
    db: db !== undefined
      ? toIdentifier(db, {
        quoted,
        copy,
      })
      : undefined,
    catalog: catalog !== undefined
      ? toIdentifier(catalog, {
        quoted,
        copy,
      })
      : undefined,
  });

  if (fields && 0 < fields.length) {
    const fieldIdents = fields.map((field) => toIdentifier(field, {
      quoted,
      copy,
    })).filter((f): f is IdentifierExpr => f !== undefined);
    return DotExpr.build([columnExpr, ...fieldIdents]);
  }

  return columnExpr;
}

/**
 * Create a table expression (optionally qualified with database and catalog).
 *
 * @param name - Table name
 * @param db - Optional database name
 * @param catalog - Optional catalog name
 * @returns Table expression
 *
 * @example
 * // Simple table: users
 * const tbl = table('users');
 *
 * @example
 * // Fully qualified: catalog.database.table
 * const tbl = table('users', 'mydb', 'mycatalog');
 */
/**
 * Build a Table.
 *
 * Example:
 *     table_('users', { quoted: true }).sql()
 *     // '"users"'
 *
 *     table_('users', { db: 'mydb', catalog: 'mycatalog' }).sql()
 *     // 'mycatalog.mydb.users'
 *
 * @param tableName - Table name
 * @param options - Options object
 * @param options.db - Database name
 * @param options.catalog - Catalog name
 * @param options.quoted - Whether to force quotes on the table's identifiers
 * @param options.alias - Table's alias
 * @returns The new Table instance
 */
export function table_ (
  tableName: string | IdentifierExpr,
  options: {
    db?: string | IdentifierExpr;
    catalog?: string | IdentifierExpr;
    quoted?: boolean;
    alias?: string | IdentifierExpr;
  } = {},
): TableExpr {
  const {
    db, catalog, quoted, alias: aliasName,
  } = options;

  return new TableExpr({
    this: tableName ? toIdentifier(tableName, { quoted }) : undefined,
    db: db ? toIdentifier(db, { quoted }) : undefined,
    catalog: catalog ? toIdentifier(catalog, { quoted }) : undefined,
    alias: aliasName ? new TableAliasExpr({ this: toIdentifier(aliasName) }) : undefined,
  });
}

export function table (name: string, db?: string, catalog?: string): TableExpr {
  const args: TableExprArgs = { this: new IdentifierExpr({ this: name }) };
  if (db) {
    args.db = new IdentifierExpr({ this: db });
  }
  if (catalog) {
    args.catalog = new IdentifierExpr({ this: catalog });
  }
  return new TableExpr(args);
}

/**
 * Create an AND expression from multiple conditions.
 * Automatically chains multiple conditions with AND.
 *
 * @param conditions - Conditions to AND together (nulls are filtered out)
 * @param options - Options object
 * @param options.dialect - The dialect to use for parsing
 * @param options.copy - Whether to copy expressions (handled by caller)
 * @param options.wrap - Whether to wrap in Parens
 * @returns AND expression or single condition if only one provided
 *
 * @example
 * // WHERE a = 1 AND b = 2 AND c = 3
 * const condition = and([cond1, cond2, cond3]);
 *
 * @example
 * // With options
 * const condition = and([cond1, cond2], { wrap: true });
 */
/**
 * Combine multiple conditions with an AND logical operator.
 *
 * Example:
 *     and(["x=1", and(["y=1", "z=1"])]).sql()
 *     // 'x = 1 AND (y = 1 AND z = 1)'
 *
 * @param expressions - The SQL code strings to parse. If an Expression instance is passed, this is used as-is.
 * @param options - Options object
 * @param options.dialect - The dialect used to parse the input expression
 * @param options.copy - Whether to copy expressions (only applies to Expressions)
 * @param options.wrap - Whether to wrap the operands in Parens
 * @returns The new condition
 */
export function and (
  expressions?: ExpressionValue | (ExpressionValue | undefined)[],
  options: {
    dialect?: DialectType;
    copy?: boolean;
    wrap?: boolean;
    [key: string]: unknown;
  } = {},
): ConditionExpr {
  const {
    dialect, copy = true, wrap = true, ...opts
  } = options;
  return combine(expressions, AndExpr, {
    dialect,
    copy,
    wrap,
    ...opts,
  }) as ConditionExpr;
}

/**
 * Combine multiple conditions with an OR logical operator.
 *
 * Example:
 *     or(["x=1", or(["y=1", "z=1"])]).sql()
 *     // 'x = 1 OR (y = 1 OR z = 1)'
 *
 * @param expressions - The SQL code strings to parse. If an Expression instance is passed, this is used as-is.
 * @param options - Options object
 * @param options.dialect - The dialect used to parse the input expression
 * @param options.copy - Whether to copy expressions (only applies to Expressions)
 * @param options.wrap - Whether to wrap the operands in Parens
 * @returns The new condition
 */
export function or (
  expressions?: string | Expression | (string | Expression | undefined)[],
  options: {
    dialect?: DialectType;
    copy?: boolean;
    wrap?: boolean;
    [key: string]: unknown;
  } = {},
): ConditionExpr {
  const {
    dialect, copy = true, wrap = true, ...opts
  } = options;
  return combine(expressions, OrExpr, {
    dialect,
    copy,
    wrap,
    ...opts,
  }) as ConditionExpr;
}

/**
 * Combine multiple conditions with an XOR logical operator.
 *
 * Example:
 *     xor(["x=1", xor(["y=1", "z=1"])]).sql()
 *     // 'x = 1 XOR (y = 1 XOR z = 1)'
 *
 * @param expressions - The SQL code strings to parse. If an Expression instance is passed, this is used as-is.
 * @param options - Options object
 * @param options.dialect - The dialect used to parse the input expression
 * @param options.copy - Whether to copy expressions (only applies to Expressions)
 * @param options.wrap - Whether to wrap the operands in Parens
 * @returns The new condition
 */
export function xor (
  expressions?: string | Expression | (string | Expression | undefined)[],
  options: {
    dialect?: DialectType;
    copy?: boolean;
    wrap?: boolean;
    [key: string]: unknown;
  } = {},
): ConditionExpr {
  const {
    dialect, copy = true, wrap = true, ...opts
  } = options;
  return combine(expressions, XorExpr, {
    dialect,
    copy,
    wrap,
    ...opts,
  }) as ConditionExpr;
}

/**
 * Wrap a condition with a NOT operator.
 *
 * Example:
 *     not_("this_suit='black'").sql()
 *     // "NOT this_suit = 'black'"
 *
 * @param expression - The SQL code string to parse. If an Expression instance is passed, this is used as-is.
 * @param options - Options object
 * @param options.dialect - The dialect used to parse the input expression
 * @param options.copy - Whether to copy the expression
 * @returns The new condition
 */
export function not (
  expression?: ExpressionOrString,
  options: {
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
  } = {},
): NotExpr {
  const thisExpr = condition(expression, options);
  return new NotExpr({ this: wrap(thisExpr, ConnectorExpr) || thisExpr });
}

/**
 * Wrap an expression in parentheses.
 *
 * Example:
 *     paren("5 + 3").sql()
 *     // '(5 + 3)'
 *
 * @param expression - The SQL code string to parse.
 *                     If an Expression instance is passed, this is used as-is.
 * @param options - Options object
 * @param options.copy - Whether to copy the expression or not
 * @returns The wrapped expression
 */
export function paren (
  expression: string | Expression,
  options: {
    copy?: boolean;
    [key: string]: unknown;
  } = {},
): ParenExpr {
  const {
    copy = true, ...opts
  } = options;
  return new ParenExpr({
    this: maybeParse(expression, {
      copy,
      ...opts,
    }),
  });
}

export const SAFE_IDENTIFIER_RE = /^[_a-zA-Z][\w]*$/;

/**
 * Builds an identifier.
 *
 * Example:
 *     toIdentifier("my_column").sql()
 *     // 'my_column'
 *     toIdentifier("column name", { quoted: true }).sql()
 *     // '"column name"'
 *
 * @param name - The name to turn into an identifier
 * @param options - Options object
 * @param options.quoted - Whether to force quote the identifier
 * @param options.copy - Whether to copy name if it's an Identifier
 * @returns The identifier ast node or undefined if name is undefined
 */
export function toIdentifier (
  name: undefined,
  options?: {
    quoted?: boolean;
    copy?: boolean;
  },
): undefined;
export function toIdentifier (
  name: ExpressionValue<IdentifierExpr>,
  options?: {
    quoted?: boolean;
    copy?: boolean;
  },
): IdentifierExpr;
export function toIdentifier (
  name: undefined | ExpressionValue<IdentifierExpr>,
  options?: {
    quoted?: boolean;
    copy?: boolean;
  },
): IdentifierExpr | undefined;
export function toIdentifier (
  name: undefined | ExpressionValue<IdentifierExpr>,
  options: {
    quoted?: boolean;
    copy?: boolean;
  } = {},
): IdentifierExpr | undefined {
  const {
    quoted, copy = true,
  } = options;

  if (name === undefined) return undefined;

  if (name instanceof IdentifierExpr) {
    return maybeCopy(name, copy) as IdentifierExpr;
  }

  return new IdentifierExpr({
    this: name,
    quoted: quoted !== undefined ? quoted : !SAFE_IDENTIFIER_RE.test(name.toString()),
  });

  // throw new Error(`Name needs to be a string or an Identifier, got: ${name?.constructor?.name}`);
}

/**
 * Parses a given string into an identifier.
 *
 * Example:
 *     parseIdentifier("my_table").sql()
 *     // 'my_table'
 *
 * @param name - The name to parse into an identifier
 * @param options - Options object
 * @param options.dialect - The dialect to parse against
 * @returns The identifier ast node
 */
export function parseIdentifier (
  name: string | IdentifierExpr,
  options: {
    dialect?: DialectType;
    [key: string]: unknown;
  } = {},
): IdentifierExpr {
  const {
    dialect, ...opts
  } = options;
  try {
    return maybeParse(name, {
      dialect,
      into: IdentifierExpr,
      ...opts,
    }) as IdentifierExpr;
  } catch {
    return toIdentifier(name) as IdentifierExpr;
  }
}

/**
 * Matches interval strings like "1 day" or "5.5 months"
 * Captures: (number, unit)
 */
export const INTERVAL_STRING_RE = /\s*(-?[0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z]+)\s*/;

/**
 * Matches day-time interval strings that contain:
 * - A number of days (possibly negative or with decimals)
 * - At least one space
 * - Portions of a time-like signature, potentially negative
 *   - Standard format                   [-]h+:m+:s+[.f+]
 *   - Just minutes/seconds/frac seconds [-]m+:s+.f+
 *   - Just hours, minutes, maybe colon  [-]h+:m+[:]
 *   - Just hours, maybe colon           [-]h+[:]
 *   - Just colon                        :
 */
export const INTERVAL_DAY_TIME_RE = /\s*-?\s*\d+(?:\.\d+)?\s+(?:-?(?:\d+:)?\d+:\d+(?:\.\d+)?|-?(?:\d+:){1,2}|:)\s*/;

/**
 * Builds an interval expression from a string like '1 day' or '5 months'.
 *
 * Example:
 *     toInterval("1 day").sql()
 *     // 'INTERVAL 1 DAY'
 *
 * @param interval - The interval string or Literal expression
 * @returns The interval expression
 */
export function toInterval (
  interval?: ExpressionOrString<LiteralExpr>,
): IntervalExpr {
  let intervalStr: string | undefined;

  if (interval instanceof LiteralExpr) {
    if (!interval.args.isString) {
      throw new Error('Invalid interval string.');
    }
    intervalStr = interval.args.this;
  } else {
    intervalStr = interval;
  }

  const parsed = maybeParse(`INTERVAL ${intervalStr}`);
  if (!(parsed instanceof IntervalExpr)) {
    throw new Error('Failed to parse interval expression');
  }
  return parsed;
}

/**
 * Create an Alias expression.
 *
 * Example:
 *     alias_('foo', 'bar').sql()
 *     // 'foo AS bar'
 *
 *     alias_('(select 1, 2)', 'bar', { table: ['a', 'b'] }).sql()
 *     // '(SELECT 1, 2) AS bar(a, b)'
 *
 * @param expression - The SQL code string to parse.
 *                     If an Expression instance is passed, this is used as-is.
 * @param aliasName - The alias name to use. If the name has special characters it is quoted.
 * @param options - Options object
 * @param options.table - Whether to create a table alias, can also be a list of columns
 * @param options.quoted - Whether to quote the alias
 * @param options.dialect - The dialect used to parse the input expression
 * @param options.copy - Whether to copy the expression
 * @returns The aliased expression
 */
export function alias<E extends Expression> (
  expression?: ExpressionValue<E>,
  aliasName?: ExpressionValue<IdentifierExpr>,
  options: {
    table?: boolean | ExpressionOrStringList<IdentifierExpr>;
    quoted?: boolean;
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
  } = {},
): E | AliasExpr {
  const {
    table: tableOpt, quoted, dialect, copy = true, ...opts
  } = options;

  const exp = maybeParse(expression, {
    dialect,
    copy,
    ...opts,
  });
  const aliasIdent = aliasName !== undefined ? toIdentifier(aliasName instanceof IdentifierExpr ? aliasName : aliasName.toString(), { quoted }) : undefined;

  if (tableOpt) {
    const tableAlias = new TableAliasExpr({ this: aliasIdent });
    exp.setArgKey('alias', tableAlias);

    if (Array.isArray(tableOpt)) {
      for (const column of tableOpt) {
        const columnIdent = toIdentifier(column, { quoted });
        if (columnIdent) {
          tableAlias.append('columns', columnIdent);
        }
      }
    }

    return exp;
  }

  // We don't set the "alias" arg for Window expressions, because that would add an IDENTIFIER node in
  // the AST, representing a "named_window" construct (eg. bigquery). What we want is an ALIAS node
  // for the complete Window expression.
  if (exp._constructor.availableArgs.has('alias') && !(exp instanceof WindowExpr)) {
    if (aliasIdent) {
      exp.setArgKey('alias', aliasIdent);
    }
    return exp;
  }

  return new AliasExpr({
    this: exp,
    alias: aliasIdent,
  });
}

/**
 * Build a subquery expression that's selected from.
 *
 * Example:
 *     subquery('select x from tbl', 'bar').select(['x']).sql()
 *     // 'SELECT x FROM (SELECT x FROM tbl) AS bar'
 *
 * @param expression - The SQL code string to parse.
 *                     If an Expression instance is passed, this is used as-is.
 * @param aliasName - The alias name to use
 * @param options - Options object
 * @param options.dialect - The dialect used to parse the input expression
 * @returns A new Select instance with the subquery expression included
 */
export function subquery (
  expression: string | Expression,
  aliasName?: string | IdentifierExpr,
  options: {
    dialect?: DialectType;
    [key: string]: unknown;
  } = {},
): SelectExpr {
  const {
    dialect, ...opts
  } = options;
  const parsed = maybeParse(expression, {
    dialect,
    ...opts,
  });
  if (!(parsed instanceof QueryExpr)) {
    throw new Error('The input sql is not a QueryExpr');
  }
  const subqueryExpr = parsed.subquery(aliasName, opts);
  return new SelectExpr({}).from(subqueryExpr, {
    dialect,
    ...opts,
  });
}

/**
 * Create a SELECT expression.
 *
 * @param columns - Columns to select (strings or expressions)
 * @returns SELECT expression
 *
 * @example
 * // SELECT col1, col2
 * const sel = select('col1', 'col2');
 *
 * @example
 * // SELECT users.id, users.name
 * const sel = select(
 *   column('id', 'users'),
 *   column('name', 'users')
 * );
 */
export function select (
  expressions?: ExpressionValue | (ExpressionValue | undefined)[],
  options: {
    dialect?: DialectType;
    [key: string]: unknown;
  } = {},
): SelectExpr {
  return new SelectExpr({}).select(expressions, options);
}

/**
 * Initializes a syntax tree from a FROM expression.
 *
 * Example:
 *     from("tbl").select("col1", "col2").sql()
 *     // 'SELECT col1, col2 FROM tbl'
 *
 * @param expression - The SQL code string to parse as the FROM expression of a
 *                     SELECT statement. If an Expression instance is passed, this is used as-is.
 * @param options - Options object
 * @param options.dialect - The dialect used to parse the input expression
 * @returns The syntax tree for the SELECT statement
 */
export function from (
  expression: ExpressionOrString,
  options: {
    dialect?: DialectType;
    [key: string]: unknown;
  } = {},
): SelectExpr {
  return new SelectExpr({}).from(expression, options);
}

/**
 * Create a CASE expression
 * @param conditions - Condition-value pairs
 * @param defaultValue - Default value
 * @returns CASE expression
 */
/**
 * Initialize a CASE statement.
 *
 * Example:
 *     case_().when("a = 1", "foo").else_("bar")
 *
 * @param expression - Optionally, the input expression (not all dialects support this)
 * @param options - Extra options for parsing expression
 * @returns A Case expression
 */
export function case_ (
  expression?: ExpressionOrString,
  options: {
    [key: string]: unknown;
  } = {},
): CaseExpr {
  let thisExpr: Expression | undefined;
  if (expression !== undefined) {
    thisExpr = maybeParse(expression, options);
  }
  return new CaseExpr({
    this: thisExpr,
    ifs: [],
  });
}

/**
 * Create a CAST expression.
 *
 * @param expr - Expression to cast
 * @param toType - Target data type (string or DataTypeExpr)
 * @returns CAST expression
 *
 * @example
 * // CAST(col AS VARCHAR)
 * const casted = cast(column('col'), 'VARCHAR');
 *
 * @example
 * // CAST(value AS INTEGER)
 * const casted = cast(literal('123'), DataTypeExpr.build('INTEGER'));
 */
/**
 * Cast an expression to a data type.
 *
 * Example:
 *     cast('x + 1', 'int').sql()
 *     // 'CAST(x + 1 AS INT)'
 *
 * @param expression - The expression to cast
 * @param to - The datatype to cast to
 * @param options - Options object
 * @param options.copy - Whether to copy the supplied expressions
 * @param options.dialect - The target dialect. This is used to prevent a re-cast in the following scenario:
 *                          - The expression to be cast is already a Cast expression
 *                          - The existing cast is to a type that is logically equivalent to new type
 *
 *                          For example, if expression='CAST(x as DATETIME)' and to=Type.TIMESTAMP,
 *                          but in the target dialect DATETIME is mapped to TIMESTAMP, then we will NOT return
 *                          CAST(x (as DATETIME) as TIMESTAMP) and instead just return the original expression
 *                          CAST(x as DATETIME).
 *
 *                          This is to prevent it being output as a double cast CAST(x (as TIMESTAMP) as TIMESTAMP)
 *                          once the DATETIME -> TIMESTAMP mapping is applied in the target dialect generator.
 * @returns The new Cast instance
 */
export function cast (
  expression?: ExpressionOrString,
  to?: DataTypeExprKind | ExpressionOrString<ColumnDefExpr | IdentifierExpr | DotExpr | DataTypeExpr>,
  options: {
    copy?: boolean;
    dialect?: DialectType;
    [key: string]: unknown;
  } = {},
): CastExpr {
  const {
    copy = true, dialect, ...opts
  } = options;

  const expr = maybeParse(expression, {
    copy,
    dialect,
    ...opts,
  });
  const dataType = DataTypeExpr.build(to, {
    copy,
    dialect,
    ...opts,
  });

  // Don't re-cast if the expression is already a cast to the correct type
  if (expr instanceof CastExpr) {
    const targetDialect = Dialect.getOrRaise(dialect);
    const typeMapping = targetDialect._constructor.generatorClass.TYPE_MAPPING;
    const existingCastType = (expr.args.to as Expression | undefined)?.args.this;
    const newCastType = dataType?.args.this;
    const typesAreEquivalent = existingCastType != null
      && (typeMapping.get(existingCastType.toString()) || existingCastType) === (typeMapping.get(newCastType?.toString() ?? '') || newCastType);

    if ((dataType !== undefined && expr.isType([dataType])) || typesAreEquivalent) {
      return expr;
    }
  }

  const castExpr = new CastExpr({
    this: expr,
    to: dataType,
  });
  castExpr.type = dataType;

  return castExpr;
}

/**
 * Build VALUES statement.
 *
 * Example:
 *     values([[1, '2']]).sql()
 *     // "VALUES (1, '2')"
 *
 * @param valuesList - Values statements that will be converted to SQL (array of tuples/arrays)
 * @param options - Options object
 * @param options.alias - Optional alias
 * @param options.columns - Optional list of ordered column names. If provided then an alias is also required.
 * @returns The Values expression object
 */
export function values (
  valuesList: (ExpressionValue | undefined)[][],
  options: {
    alias?: string;
    columns?: (string | IdentifierExpr)[];
  } = {},
): ValuesExpr {
  const {
    alias: aliasName, columns,
  } = options;

  if (columns && !aliasName) {
    throw new Error('Alias is required when providing columns');
  }

  const expressions = valuesList.map((tup) => convert(tup));

  let alias: TableAliasExpr | undefined;
  if (columns) {
    alias = new TableAliasExpr({
      this: aliasName !== undefined ? toIdentifier(aliasName) : undefined,
      columns: columns.map((col) => toIdentifier(col)).filter((c): c is IdentifierExpr => c !== undefined),
    });
  } else if (aliasName) {
    alias = new TableAliasExpr({ this: toIdentifier(aliasName) });
  }

  return new ValuesExpr({
    expressions,
    alias,
  });
}

/**
 * Create a function expression
 * @param name - Function name
 * @param args - Function arguments
 * @returns Function expression
 */
export function func (name: string, ...args: ExpressionValue[]): FuncExpr {
  return new FuncExpr({
    this: name,
    expressions: args,
  });
}

/**
 * Create a UNION expression.
 *
 * @param left - Left query
 * @param right - Right query
 * @param distinct - Whether to use UNION DISTINCT (default: false for UNION ALL)
 * @returns UNION expression
 *
 * @example
 * // SELECT ... UNION ALL SELECT ...
 * const un = union(query1, query2);
 *
 * @example
 * // SELECT ... UNION DISTINCT SELECT ...
 * const un = union(query1, query2, true);
 */
/**
 * Helper function to build set operations (UNION, INTERSECT, EXCEPT) by chaining expressions.
 * @param expressions - The expressions to combine
 * @param setOperation - The set operation class constructor
 * @param options - Options including distinct, dialect, copy, etc.
 * @returns The chained set operation expression
 */
function applySetOperation<S extends Expression> (
  expressions: undefined | ExpressionValue[],
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  setOperation: new (args: any) => S,
  options: {
    distinct?: boolean;
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
  } = {},
): S | undefined {
  const {
    distinct = true, dialect, copy = true, ...opts
  } = options;

  const parsedExpressions = expressions?.map((e) =>
    maybeParse(e, {
      dialect,
      copy,
      ...opts,
    }));

  return parsedExpressions?.reduce((left, right) =>
    new setOperation({
      this: left,
      expression: right,
      distinct,
      ...opts,
    })) as S | undefined;
}

/**
 * Initializes a syntax tree for the `UNION` operation.
 *
 * Example:
 *     union(["SELECT * FROM foo", "SELECT * FROM bla"]).sql();
 *     // 'SELECT * FROM foo UNION SELECT * FROM bla'
 *
 * @param expressions - The SQL code strings, corresponding to the `UNION`'s operands.
 *                      If `Expression` instances are passed, they will be used as-is.
 * @param options - Options object
 * @param options.distinct - Set the DISTINCT flag if and only if this is true. Default is `true`.
 * @param options.dialect - The dialect used to parse the input expression
 * @param options.copy - Whether to copy the expression. Default is `true`.
 * @returns The new Union instance
 */
export function union (
  expressions?: ExpressionValue | (ExpressionValue | undefined)[],
  options: {
    distinct?: boolean;
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
  } = {},
): UnionExpr | undefined {
  const expressionList = Array.from(ensureIterable(expressions)).filter((e): e is string | Expression => e !== undefined);
  if (expressionList.length < 2) {
    throw new Error('At least two expressions are required by `union`.');
  }
  return applySetOperation(expressionList, UnionExpr, options);
}

/**
 * Initializes a syntax tree for the `INTERSECT` operation.
 *
 * Example:
 *     intersectExpr(["SELECT * FROM foo", "SELECT * FROM bla"]).sql();
 *     // 'SELECT * FROM foo INTERSECT SELECT * FROM bla'
 *
 * @param expressions - The SQL code strings, corresponding to the `INTERSECT`'s operands.
 *                      If `Expression` instances are passed, they will be used as-is.
 * @param options - Options object
 * @param options.distinct - Set the DISTINCT flag if and only if this is true. Default is `true`.
 * @param options.dialect - The dialect used to parse the input expression
 * @param options.copy - Whether to copy the expression. Default is `true`.
 * @returns The new Intersect instance
 */
export function intersect (
  expressions?: ExpressionOrString | (ExpressionOrString | undefined)[],
  options: {
    distinct?: boolean;
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
  } = {},
): IntersectExpr | undefined {
  const expressionList = Array.from(ensureIterable(expressions)).filter((e): e is string | Expression => e !== undefined);
  if (expressionList.length < 2) {
    throw new Error('At least two expressions are required by `intersect`.');
  }
  return applySetOperation(expressionList, IntersectExpr, options);
}

/**
 * Initializes a syntax tree for the `EXCEPT` operation.
 *
 * Example:
 *     exceptExpr(["SELECT * FROM foo", "SELECT * FROM bla"]).sql();
 *     // 'SELECT * FROM foo EXCEPT SELECT * FROM bla'
 *
 * @param expressions - The SQL code strings, corresponding to the `EXCEPT`'s operands.
 *                      If `Expression` instances are passed, they will be used as-is.
 * @param options - Options object
 * @param options.distinct - Set the DISTINCT flag if and only if this is true. Default is `true`.
 * @param options.dialect - The dialect used to parse the input expression
 * @param options.copy - Whether to copy the expression. Default is `true`.
 * @returns The new Except instance
 */
export function except (
  expressions?: ExpressionValue | (ExpressionValue | undefined)[],
  options: {
    distinct?: boolean;
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
  } = {},
): ExceptExpr | undefined {
  const expressionList = Array.from(ensureIterable(expressions)).filter((e): e is string | Expression => e !== undefined);
  if (expressionList.length < 2) {
    throw new Error('At least two expressions are required by `except`.');
  }
  return applySetOperation(expressionList, ExceptExpr, options);
}

/**
 * Builds an INSERT statement.
 *
 * Example:
 *     insert("VALUES (1, 2, 3)", "tbl").sql()
 *     // 'INSERT INTO tbl VALUES (1, 2, 3)'
 *
 * @param expression - The SQL string or expression of the INSERT statement
 * @param into - The table to insert data to
 * @param options - Options object
 * @param options.columns - Optionally the table's column names
 * @param options.overwrite - Whether to INSERT OVERWRITE or not
 * @param options.returning - SQL conditional parsed into a RETURNING statement
 * @param options.dialect - The dialect used to parse the input expressions
 * @param options.copy - Whether to copy the expression
 * @returns The syntax tree for the INSERT statement
 */
export function insert (
  expression: string | SelectExpr,
  into: string | TableExpr,
  options: {
    columns?: (string | IdentifierExpr)[];
    overwrite?: boolean;
    returning?: string | Expression;
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
  } = {},
): InsertExpr {
  const {
    columns, overwrite, returning, dialect, copy = true, ...opts
  } = options;

  const expr = maybeParse(expression, {
    dialect,
    copy,
    ...opts,
  });

  let thisExpr: TableExpr | SchemaExpr = maybeParse(into, {
    into: TableExpr,
    dialect,
    copy,
    ...opts,
  });

  if (columns) {
    thisExpr = new SchemaExpr({
      this: thisExpr,
      expressions: columns.map((c) =>
        typeof c === 'string' ? toIdentifier(c) : c),
    });
  }

  let insertExpr = new InsertExpr({
    this: thisExpr,
    expression: expr,
    overwrite,
  });

  if (returning) {
    insertExpr = insertExpr.returning(returning, {
      dialect,
      copy: false,
      ...opts,
    }) as InsertExpr;
  }

  return insertExpr;
}

/**
 * Builds a DELETE statement.
 *
 * Example:
 *     delete_("my_table", { where: "id > 1" }).sql()
 *     // 'DELETE FROM my_table WHERE id > 1'
 *
 * @param table - The table to delete from
 * @param options - Options object
 * @param options.where - SQL conditional parsed into a WHERE statement
 * @param options.returning - SQL conditional parsed into a RETURNING statement
 * @param options.dialect - The dialect used to parse the input expressions
 * @returns The syntax tree for the DELETE statement
 */
export function delete_ (
  table: string | Expression,
  options: {
    where?: string | Expression;
    returning?: string | Expression;
    dialect?: DialectType;
    [key: string]: unknown;
  } = {},
): DeleteExpr {
  const {
    where, returning, dialect, ...opts
  } = options;

  let deleteExpr = new DeleteExpr({}).delete(table, {
    dialect,
    copy: false,
    ...opts,
  }) as DeleteExpr;

  if (where) {
    deleteExpr = deleteExpr.where(where, {
      dialect,
      copy: false,
      ...opts,
    }) as DeleteExpr;
  }

  if (returning) {
    deleteExpr = deleteExpr.returning(returning, {
      dialect,
      copy: false,
      ...opts,
    }) as DeleteExpr;
  }

  return deleteExpr;
}

/**
 * Creates an UPDATE statement.
 *
 * Example:
 *     update("my_table", { properties: { x: 1, y: "2" }, where: "id > 1" }).sql()
 *     // "UPDATE my_table SET x = 1, y = '2' WHERE id > 1"
 *
 * @param table - The table to update
 * @param options - Options object
 * @param options.properties - Dictionary of properties to SET
 * @param options.where - SQL conditional parsed into a WHERE statement
 * @param options.from - SQL statement parsed into a FROM statement
 * @param options.with - Dictionary of CTE aliases / select statements
 * @param options.dialect - The dialect used to parse the input expressions
 * @returns The syntax tree for the UPDATE statement
 */
export function update<T> (
  table: string | TableExpr,
  properties?: Record<string, unknown>,
  options?: {
    where?: string | Expression;
    from?: string | Expression;
    with?: Record<string, string | Expression>;
    dialect?: DialectType;
  } & { [K in keyof T]: K extends 'dialect' | 'with' ? unknown : ExpressionValue | ExpressionValueList },
): UpdateExpr;
export function update (
  table: string | TableExpr,
  properties?: Record<string, unknown>,
  options: {
    where?: string | Expression;
    from?: string | Expression;
    with?: Record<string, string | Expression>;
    dialect?: DialectType;
  } & { [key: string]: ExpressionValue | ExpressionValueList } = {},
): UpdateExpr {
  const {
    where, from: fromExpr, with: withCtes, dialect, ...opts
  } = options;

  const updateExpr = new UpdateExpr({
    this: maybeParse(table, {
      into: TableExpr,
      dialect,
    }),
  });

  if (properties) {
    updateExpr.setArgKey('expressions', Object.entries(properties).map(([k, v]) =>
      new EqExpr({
        this: maybeParse(k, {
          dialect,
          ...opts,
        }),
        expression: convert(v),
      })));
  }

  if (fromExpr) {
    updateExpr.setArgKey('from', maybeParse(fromExpr, {
      into: FromExpr,
      dialect,
      prefix: 'FROM',
      ...opts,
    }));
  }

  if (where) {
    let whereExpr = where;
    if (where instanceof ConditionExpr) {
      whereExpr = new WhereExpr({ this: where });
    }
    updateExpr.setArgKey('where', maybeParse(whereExpr, {
      into: WhereExpr,
      dialect,
      prefix: 'WHERE',
      ...opts,
    }));
  }

  if (withCtes) {
    const cteList = Object.entries(withCtes).map(([aliasName, qry]) =>
      alias(new CteExpr({
        this: maybeParse(qry, {
          dialect,
          ...opts,
        }),
      }), aliasName, { table: true })) as CteExpr[];

    updateExpr.setArgKey('with', new WithExpr({ expressions: cteList }));
  }

  return updateExpr;
}

/**
 * Builds a MERGE statement.
 *
 * Example:
 *     merge(["WHEN MATCHED THEN UPDATE..."], {
 *       into: "my_table",
 *       using: "source_table",
 *       on: "my_table.id = source_table.id"
 *     }).sql()
 *
 * @param _whenExprs - The WHEN clauses specifying actions for matched and unmatched rows
 * @param options - Options object
 * @param options.into - The target table to merge data into
 * @param options.using - The source table to merge data from
 * @param options.on - The join condition for the merge
 * @param options.returning - The columns to return from the merge
 * @param options.dialect - The dialect used to parse the input expressions
 * @param options.copy - Whether to copy the expression
 * @returns The syntax tree for the MERGE statement
 */
export function merge (
  _whenExprs?: ExpressionOrString | ExpressionOrString[],
  options: {
    into?: ExpressionOrString<TableExpr | AliasExpr>;
    using?: ExpressionOrString;
    on?: ExpressionOrString;
    returning?: ExpressionOrString;
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
  } = {},
): MergeExpr {
  const {
    into, using: usingExpr, on, returning, dialect, copy = true, ...restOptions
  } = options;

  const whenExprs = _whenExprs !== undefined ? Array.from(ensureIterable<ExpressionOrString>(_whenExprs)) : [];

  const expressions: WhenExpr[] = [];
  for (const whenExpr of whenExprs) {
    const expr = maybeParse(whenExpr, {
      dialect,
      copy,
      into: WhensExpr,
      ...restOptions,
    });
    if (expr instanceof WhenExpr) {
      expressions.push(expr);
    }
    expressions.push(...expr.args.expressions as WhenExpr[]);
  }

  let mergeExpr = new MergeExpr({
    this: maybeParse(into, {
      dialect,
      copy,
      ...restOptions,
    }),
    using: maybeParse(usingExpr, {
      dialect,
      copy,
      ...restOptions,
    }),
    on: maybeParse(on, {
      dialect,
      copy,
      ...restOptions,
    }),
    whens: new WhensExpr({ expressions }),
  });

  if (returning) {
    mergeExpr = mergeExpr.returning(returning, {
      dialect,
      copy: false,
      ...restOptions,
    });
  }

  const usingClause = mergeExpr.args.using;
  if (usingClause instanceof AliasExpr) {
    const usingAlias = usingClause.args.alias;
    const aliasName = typeof usingAlias === 'string' ? usingAlias : isInstanceOf(usingAlias, IdentifierExpr) ? usingAlias : undefined;
    usingClause.replace(alias(usingClause.args.this, aliasName, { table: true }));
  }

  return mergeExpr;
}

/**
 * Initialize a logical condition expression.
 *
 * Example:
 *     condition("x=1").sql()
 *     // 'x = 1'
 *
 * This is helpful for composing larger logical syntax trees:
 *     const where = condition("x=1")
 *     where = where.and("y=1")
 *     Select().from("tbl").select("*").where(where).sql()
 *     // 'SELECT * FROM tbl WHERE x = 1 AND y = 1'
 *
 * @param expression - The SQL code string to parse. If an Expression instance is passed, this is used as-is.
 * @param options - Options object
 * @param options.dialect - The dialect used to parse the input expression
 * @param options.copy - Whether to copy the expression
 * @returns The new Condition instance
 */
export function condition (
  expression: string | Expression | undefined,
  options: {
    dialect?: DialectType;
    copy?: boolean;
    [key: string]: unknown;
  } = {},
): ConditionExpr {
  return maybeParse(expression, {
    into: ConditionExpr,
    ...options,
  }) as ConditionExpr;
}

/**
 * Parse SQL text into an expression if needed
 * @param sqlOrExpression - SQL text or expression
 * @param options - Parsing options
 * @returns Expression
 */
export function maybeParse<RetT extends Expression> (
  sqlOrExpression?: ExpressionValue<RetT>,
  options?: ParseOptions<RetT> & {
    prefix?: string;
    copy?: boolean;
  },
): RetT {
  // If it's already an Expression
  if (sqlOrExpression instanceof Expression) {
    if (options?.copy) {
      return sqlOrExpression.copy();
    }
    return sqlOrExpression;
  }

  // SQL cannot be None/null
  if (sqlOrExpression === undefined) {
    throw new ParseError('SQL cannot be null or undefined');
  }

  // Convert to string and optionally add prefix
  let sql = String(sqlOrExpression);
  if (options?.prefix) {
    sql = `${options.prefix} ${sql}`;
  }

  // Extract prefix and copy from options, pass the rest to parseOne
  const {
    dialect, ...parseOptions
  } = options || {};

  // Parse the SQL string
  return parseOne<RetT>(sql, {
    ...parseOptions,
    read: dialect || parseOptions.read,
  });
}

/**
 * Convert SQL text to a column expression
 * @param sql - SQL text
 * @param dialect - SQL dialect
 * @returns Column expression
 */
/**
 * Create a table expression from a `[catalog].[schema].[table]` sql path.
 * Catalog and schema are optional. If a table is passed in then that table is returned.
 *
 * Example:
 *     to_table("catalog.schema.table").sql()
 *     // 'catalog.schema.table'
 *
 * @param sqlPath - A `[catalog].[schema].[table]` string or TableExpr instance
 * @param options - Options object
 * @param options.dialect - The source dialect according to which the table name will be parsed
 * @param options.copy - Whether to copy a table if it is passed in
 * @returns A table expression
 */
export function toTable<T> (
  sqlPath: string | TableExpr,
  options?: {
    dialect?: DialectType;
    copy?: boolean;
  } & { [K in keyof T]: K extends 'dialect' ? unknown : ExpressionValue | ExpressionValueList },
): TableExpr;
export function toTable (
  sqlPath: string | TableExpr,
  options: {
    dialect?: DialectType;
    copy?: boolean;
  } & { [key: string]: ExpressionValue | ExpressionValueList } = {},
): TableExpr {
  const {
    dialect, copy = true, ...opts
  } = options;

  if (sqlPath instanceof TableExpr) {
    return maybeCopy(sqlPath, copy) as TableExpr;
  }

  try {
    const parsed = maybeParse(sqlPath, {
      into: TableExpr,
      dialect,
      ...opts,
    });
    for (const [k, v] of Object.entries(opts)) {
      parsed.setArgKey(k, v);
    }
    return parsed as TableExpr;
  } catch {
    const [
      catalog,
      db,
      name,
    ] = splitNumWords(sqlPath, '.', 3);

    if (!name) {
      throw new Error(`Invalid table path: ${sqlPath}`);
    }

    const tableExpr = table(name, db, catalog);
    for (const [k, v] of Object.entries(opts)) {
      tableExpr.setArgKey(k, v);
    }
    return tableExpr;
  }
}

/**
 * Create a column from a `[table].[column]` sql path. Table is optional.
 * If a column is passed in then that column is returned.
 *
 * Example:
 *     to_column("table.column").sql()
 *     // 'table.column'
 *
 * @param sqlPath - A `[table].[column]` string or ColumnExpr instance
 * @param options - Options object
 * @param options.quoted - Whether or not to force quote identifiers
 * @param options.dialect - The source dialect according to which the column name will be parsed
 * @param options.copy - Whether to copy a column if it is passed in
 * @returns A column expression
 */
export function toColumn<T> (
  sqlPath: string | ColumnExpr,
  options?: {
    quoted?: boolean;
    dialect?: DialectType;
    copy?: boolean;
  } & { [K in keyof T]: K extends 'dialect' ? unknown : ExpressionValue | ExpressionValueList },
): ColumnExpr;
export function toColumn (
  sqlPath: string | ColumnExpr,
  options: {
    quoted?: boolean;
    dialect?: DialectType;
    copy?: boolean;
  } & { [key: string]: ExpressionValue | ExpressionValueList } = {},
): ColumnExpr {
  const {
    quoted, dialect, copy = true, ...opts
  } = options;

  if (sqlPath instanceof ColumnExpr) {
    return maybeCopy(sqlPath, copy) as ColumnExpr;
  }

  try {
    const col = maybeParse(sqlPath, {
      into: ColumnExpr,
      dialect,
      ...opts,
    }) as ColumnExpr;
    for (const [k, v] of Object.entries(opts)) {
      col.setArgKey(k, v);
    }

    if (quoted) {
      for (const identifier of col.findAll(IdentifierExpr)) {
        identifier.setArgKey('quoted', true);
      }
    }

    return col;
  } catch {
    const parts = sqlPath.split('.').reverse();
    const [name, tableName] = parts;
    const args: ColumnExprArgs = {
      this: toIdentifier(name, { quoted }) as IdentifierExpr,
    };
    if (tableName) {
      args.table = toIdentifier(tableName, { quoted }) as IdentifierExpr;
    }
    const col = new ColumnExpr(args);
    for (const [k, v] of Object.entries(opts)) {
      col.setArgKey(k, v);
    }
    return col;
  }
}

/**
 * Find all table references in an expression tree.
 * Walks the entire tree and collects all TableExpr instances.
 *
 * @param expr - Expression to search
 * @returns Array of table expressions found in the tree
 *
 * @example
 * // Find all tables in a query
 * const tables = findTables(select('*'));
 * // Returns [TableExpr('users'), TableExpr('orders'), ...]
 */
/**
 * Find all tables referenced in a query.
 *
 * @param expression - The query to find the tables in
 * @returns A set of all the tables
 */
export function findTables (expression: Expression): Set<TableExpr> {
  const tables = new Set<TableExpr>();
  for (const scope of traverseScope(expression)) {
    for (const table of scope.tables) {
      if (table.name && !scope.cteSources.has(table.name)) {
        tables.add(table);
      }
    }
  }
  return tables;
}

/**
 * Maybe copy an expression if copy is true
 * @param instance - Expression instance to potentially copy
 * @param copy - Whether to copy the instance
 * @returns The instance or a copy of it
 */
export function maybeCopy<E extends Expression | undefined> (instance: E, copy = true): E {
  if (copy && instance) {
    return instance.copy() as E;
  }
  return instance;
}

/**
 * Generate a textual representation of an Expression tree
 * @param node - The node to convert to string
 * @param verbose - Include additional metadata like _id, _comments
 * @param level - Current indentation level
 * @param reprStr - Whether to use repr for strings
 * @returns String representation of the expression tree
 */
function _toS (node: unknown, verbose = false, level = 0, reprStr = false): string {
  let indent = '\n' + ('  '.repeat(level + 1));
  let delim = `,${indent}`;

  if (node instanceof Expression) {
    const args: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(node.args)) {
      if ((v !== undefined && (!Array.isArray(v) || 0 < v.length)) || verbose) {
        args[k] = v;
      }
    }

    if ((node.type || verbose) && !(node instanceof DataTypeExpr)) {
      args._type = node.type;
    }
    if (node.comments || verbose) {
      args._comments = node.comments;
    }

    if (verbose) {
      args._id = node.hash; // Use _hash as a proxy for id
    }

    // Inline leaves for a more compact representation
    if (node.isLeaf) {
      indent = '';
      delim = ', ';
    }

    const isReprStr = node.isString || (node instanceof IdentifierExpr && node.args.quoted);
    const items = Object.entries(args)
      .map(([k, v]) => `${k}=${_toS(v, verbose, level + 1, isReprStr)}`)
      .join(delim);

    return `${node.constructor.name}(${indent}${items})`;
  }

  if (Array.isArray(node)) {
    const items = node.map((i) => _toS(i, verbose, level + 1)).join(delim);
    return `[${indent}${items}]`;
  }

  // We use the representation of the string to avoid stripping out important whitespace
  if (reprStr && typeof node === 'string') {
    node = JSON.stringify(node);
  }

  // Indent multiline strings to match the current level
  const str = String(node).replace(/^\n+|\n+$/g, '');
  return str.split('\n').join(indent);
}

/**
 * Check if an expression is the wrong type
 * @param expression - The expression to check
 * @param into - The expected expression class
 * @returns True if the expression is wrong type
 */
function isWrongExpression (expression: unknown, into: typeof Expression): expression is Expression {
  return expression instanceof Expression && !(expression instanceof into);
}

/**
 * Apply a builder function that sets a single argument on an instance
 * @param options - Options object
 * @returns The modified instance
 */
function applyBuilder<RetT extends Expression, ArgT extends Expression> (expression: undefined | ArgT | string | number, options: {
  instance?: RetT;
  arg?: string;
  copy?: boolean;
  prefix?: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  into?: new (args: any) => ArgT;
  dialect?: DialectType;
  intoArg?: string;
  [key: string]: unknown;
}): RetT {
  const {
    instance,
    arg,
    copy = true,
    prefix,
    into,
    dialect,
    intoArg = 'this',
    ...opts
  } = options;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  if (into && isWrongExpression(expression, into as any)) {
    expression = new into({ [intoArg]: expression });
  }

  const inst = maybeCopy(instance, copy)!;
  expression = maybeParse(expression, {
    prefix,
    into,
    dialect,
    ...opts,
  });

  if (arg !== undefined) inst.setArgKey(arg, expression);
  return inst;
}

/**
 * Apply a builder function that sets a list of child expressions
 * @param options - Options object
 * @returns The modified instance
 */
function applyChildListBuilder<ArgT extends Expression, IntoT extends Expression, RetT extends Expression> (
  expressions?: ExpressionValue<ArgT> | (ExpressionValue<ArgT> | undefined)[],
  options: {
    instance?: RetT;
    arg?: string;
    append?: boolean;
    copy?: boolean;
    prefix?: string;
    into?: new (args: { expressions?: ArgT[] }) => IntoT;
    dialect?: DialectType;
    properties?: Record<string, ExpressionValue | ExpressionValueList>;
    [key: string]: unknown;
  } = {},
): RetT {
  const {
    instance,
    arg,
    append = true,
    copy = true,
    prefix,
    into,
    dialect,
    properties: initialProperties,
    ...opts
  } = options;

  const inst = maybeCopy(instance, copy)!;
  const parsed: Expression[] = [];
  const properties: Record<string, unknown> = initialProperties || {};

  const expressionList = ensureIterable(expressions);
  for (const expression of expressionList) {
    if (expression === undefined) {
      continue;
    }

    let expr: ExpressionValue<IntoT | ArgT> = expression;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    if (into && isWrongExpression(expr, into as any)) {
      expr = new into({ expressions: [expr] });
    }

    const parsedExpr = maybeParse(expr, {
      into,
      dialect,
      prefix,
      ...opts,
    });

    for (const [k, v] of Object.entries(parsedExpr.args)) {
      if (k === 'expressions') {
        parsed.push(...(v as Expression[]));
      } else {
        properties[k] = v;
      }
    }
  }

  const existing = inst.getArgKey(arg) as Expression | undefined;
  let allExpressions = parsed;
  if (append && existing && existing.args.expressions) {
    allExpressions = [...(existing.args.expressions as Expression[]), ...parsed];
  }

  const child = into
    ? new into({ expressions: allExpressions as ArgT[] })
    : new Expression({ expressions: allExpressions });
  for (const [k, v] of Object.entries(properties)) {
    child.setArgKey(k, v as ExpressionValue | ExpressionValueList);
  }
  if (arg !== undefined) inst.setArgKey(arg, child);

  return inst;
}

/**
 * Apply a builder function that sets a flat list of expressions
 * @param expressions - Array of expressions to add
 * @param options - Options object
 * @returns The modified instance
 */
function applyListBuilder<ArgT extends Expression, RetT extends Expression> (
  expressions: ExpressionValue<ArgT> | undefined | (ExpressionValue<ArgT> | undefined)[],
  options: {
    instance?: RetT;
    arg?: string;
    append?: boolean;
    copy?: boolean;
    prefix?: string;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    into?: new (args: any) => ArgT;
    dialect?: DialectType;
    [key: string]: unknown;
  },
): RetT {
  const {
    instance,
    arg,
    append = true,
    copy = true,
    prefix,
    into,
    dialect,
    ...opts
  } = options;

  const inst = maybeCopy(instance, copy);

  const expressionList = Array.from(ensureIterable(expressions));
  const parsedExpressions = expressionList
    .filter((expr) => expr !== undefined)
    .map((expr) =>
      maybeParse(expr, {
        into,
        prefix,
        dialect,
        ...opts,
      }));

  const existing = inst?.getArgKey(arg) as Expression[] | undefined;
  if (arg !== undefined) {
    if (append && existing) {
      inst?.setArgKey(arg, [...existing, ...parsedExpressions]);
    } else {
      inst?.setArgKey(arg, parsedExpressions);
    }
  }

  return inst!;
}

/**
 * Apply a conjunction builder (combines expressions with AND)
 * @param expressions - Array of expressions to combine with AND
 * @param options - Options object
 * @returns The modified instance
 */
function applyConjunctionBuilder<E extends Expression> (
  expressions?: ExpressionValue | (ExpressionValue | undefined)[],
  options: {
    instance?: E;
    arg?: string;
    into?: typeof Expression;
    append?: boolean;
    copy?: boolean;
    dialect?: DialectType;
    [key: string]: unknown;
  } = {},
): E | undefined {
  const {
    instance,
    arg,
    into,
    append = true,
    copy = true,
    dialect,
    ...opts
  } = options;

  // Filter out undefined and empty strings
  const expressionList = Array.from(ensureIterable(expressions));
  const filteredExpressions = expressionList.filter(
    (expr) => expr !== undefined && expr !== '',
  );

  if (filteredExpressions.length === 0) {
    return instance;
  }

  const inst = maybeCopy(instance, copy)!;

  const existing = inst.getArgKey(arg);
  let allExpressions = [...filteredExpressions];

  if (append && existing instanceof Expression) {
    const existingExpr = into && 'this' in existing.args ? existing.args.this as Expression : existing;
    allExpressions = [existingExpr, ...filteredExpressions];
  }

  // Create AND conjunction of all expressions
  let combined: Expression | undefined;
  if (0 < allExpressions.length) {
    combined = allExpressions
      .map((expr) => maybeParse(expr as ExpressionValue, {
        dialect,
        copy,
        ...opts,
      }))
      .reduce((left, right) =>
        new AndExpr({
          this: left,
          expression: right,
        }));
  }

  const node = into && combined ? new into({ this: combined }) : combined;

  if (node && arg !== undefined) {
    inst.setArgKey(arg, node);
  }

  return inst;
}

/**
 * Apply a CTE builder
 * @param options - Options object
 * @returns The modified instance
 */
function applyCteBuilder<E extends Expression> (options: {
  instance?: E;
  alias?: string | IdentifierExpr | TableAliasExpr;
  as?: string | QueryExpr;
  recursive?: boolean;
  materialized?: boolean;
  append?: boolean;
  dialect?: DialectType;
  copy?: boolean;
  scalar?: boolean;
  [key: string]: unknown;
}): E {
  const {
    instance,
    alias,
    as: asExpr,
    recursive,
    materialized,
    append = true,
    dialect,
    copy = true,
    scalar,
    ...opts
  } = options;

  const aliasExpression = maybeParse(alias, {
    dialect,
    into: TableAliasExpr,
    ...opts,
  });

  let asExpression = maybeParse(asExpr, {
    dialect,
    copy,
    ...opts,
  });

  // Scalar CTE must be wrapped in a subquery
  if (scalar && !(asExpression instanceof SubqueryExpr)) {
    asExpression = new SubqueryExpr({ this: asExpression });
  }

  const cte = new CteExpr({
    this: asExpression,
    alias: aliasExpression,
    materialized,
    scalar,
  });

  return applyChildListBuilder([cte], {
    instance,
    arg: 'with',
    append,
    copy,
    into: WithExpr,
    properties: recursive ? { recursive } : {},
  });
}

/**
 * Combine expressions with a connector operator
 * @param expressions - Expressions to combine
 * @param operator - The connector operator class (AndExpr, OrExpr, etc.)
 * @param options - Options object
 * @returns Combined expression
 */
export function combine<T extends ConnectorExpr> (
  expressions: undefined | ExpressionValue | (ExpressionValue | undefined)[],
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  operator: new (args: any) => T,
  options: {
    dialect?: DialectType;
    copy?: boolean;
    wrap?: boolean;
    [key: string]: unknown;
  } = {},
): Expression {
  const {
    dialect, copy = true, wrap: shouldWrap = true, ...opts
  } = options;

  const expressionList = Array.from(ensureIterable(expressions));
  const conditions = expressionList
    .filter((expr) => expr !== undefined && expr !== '')
    .map((expr) => maybeParse(expr as ExpressionValue, {
      dialect,
      copy,
      ...opts,
    }));

  const [first, ...rest] = conditions;

  let result = first;
  if (0 < rest.length && shouldWrap) {
    result = wrap(result, ConnectorExpr) || result;
  }

  for (const expr of rest) {
    result = new operator({
      this: result,
      expression: shouldWrap ? wrap(expr, ConnectorExpr) || expr : expr,
    });
  }

  return result;
}

/**
 * Wrap an expression in parentheses if it's of a certain type
 * @param expression - Expression to potentially wrap
 * @param kind - The expression class to check against
 * @returns The expression wrapped in parentheses if it matches the kind, otherwise the original
 * expression
 */
export function wrap (expression: Expression, kind: typeof Expression): Expression | ParenExpr;
export function wrap (expression: undefined, kind: typeof Expression): undefined;
export function wrap (expression: Expression | undefined, kind: typeof Expression): Expression | ParenExpr | undefined;
export function wrap (expression: Expression | undefined, kind: typeof Expression): Expression | ParenExpr | undefined {
  if (expression instanceof kind) {
    return new ParenExpr({ this: expression });
  }
  return expression;
}

/**
 * Creates a literal expression from a value
 * @param value - The literal value (string, number, boolean, etc.)
 * @returns A Literal expression (or NegExpr for negative numbers)
 */
export function literal (value: unknown): LiteralExpr | NegExpr {
  if (typeof value === 'string') {
    return LiteralExpr.string(value);
  }
  if (typeof value === 'number') {
    return LiteralExpr.number(value);
  }
  return LiteralExpr.string(String(value));
}

/**
 * Returns a Null expression
 * @returns A Null expression
 */
export function null_ (): NullExpr {
  return new NullExpr({});
}

/**
 * Returns a true Boolean expression
 * @returns A true Boolean expression
 */
export function true_ (): BooleanExpr {
  return new BooleanExpr({ this: true });
}

/**
 * Returns a false Boolean expression
 * @returns A false Boolean expression
 */
export function false_ (): BooleanExpr {
  return new BooleanExpr({ this: false });
}

/**
 * Convert a JavaScript value into an expression object.
 * Raises an error if a conversion is not possible.
 *
 * @param value - A JavaScript value
 * @param copy - Whether to copy `value` (only applies to Expressions and collections)
 * @returns The equivalent expression object
 */
export function convert (value: unknown, options: { copy?: boolean } = {}): Expression {
  const { copy = false } = options;
  // Handle Expression instances
  if (value instanceof Expression) {
    const result = maybeCopy(value, copy);
    if (result) {
      return result;
    }
  }

  // Handle strings
  if (typeof value === 'string') {
    return LiteralExpr.string(value);
  }

  // Handle booleans
  if (typeof value === 'boolean') {
    return new BooleanExpr({ this: value });
  }

  // Handle null, undefined, or NaN
  if (value === undefined || (typeof value === 'number' && isNaN(value))) {
    return null_();
  }

  // Handle numbers
  if (typeof value === 'number') {
    return LiteralExpr.number(value);
  }

  // Handle Luxon DateTime objects (datetime.datetime in Python)
  // Luxon provides proper timezone support similar to Python's datetime with tzinfo
  if (DateTime.isDateTime(value)) {
    // Format datetime similar to Python's isoformat(sep=" ")
    // Python: "2024-01-15 10:30:45" (no milliseconds)
    const datetimeStr = value.toFormat('yyyy-MM-dd HH:mm:ss');
    const datetimeLiteral = LiteralExpr.string(datetimeStr);

    // Extract timezone similar to Python's str(value.tzinfo)
    // This returns IANA timezone names like "America/Los_Angeles"
    let tz: LiteralExpr | undefined;
    if (value.zoneName && value.zoneName !== 'UTC') {
      tz = LiteralExpr.string(value.zoneName);
    }

    return new TimeStrToTimeExpr({
      this: datetimeLiteral,
      zone: tz,
    });
  }

  // Handle native JavaScript Date objects (fallback)
  if (value instanceof Date) {
    // Convert to Luxon DateTime for consistent handling
    const dt = DateTime.fromJSDate(value);
    const datetimeStr = dt.toFormat('yyyy-MM-dd HH:mm:ss');
    const datetimeLiteral = LiteralExpr.string(datetimeStr);

    return new TimeStrToTimeExpr({
      this: datetimeLiteral,
      zone: undefined, // Native Date doesn't have timezone info
    });
  }

  // Handle arrays
  if (Array.isArray(value)) {
    return new ArrayExpr({ expressions: value.map((v) => convert(v, { copy })) });
  }

  // Handle objects as structs
  if (value !== null && typeof value === 'object') {
    const entries = Object.entries(value);
    return new StructExpr({
      expressions: entries.map(([k, v]) =>
        new PropertyEqExpr({
          this: toIdentifier(k),
          expression: convert(v, { copy }),
        })),
    });
  }

  throw new Error(`Cannot convert ${value}`);
}

/**
 * Build a SQL variable.
 *
 * Example:
 *     var_('x').sql()
 *     // 'x'
 *
 * @param name - The name of the var or an expression whose name will become the var
 * @returns The new variable node
 */
export function var_ (name: string | Expression | undefined): VarExpr {
  if (!name) {
    throw new Error('Cannot convert empty name into var.');
  }

  if (name instanceof Expression) {
    name = name.name;
  }

  return new VarExpr({ this: name });
}

/**
 * Returns an array.
 *
 * Example:
 *     array([1, 'x']).sql()
 *     // 'ARRAY(1, x)'
 *
 * @param expressions - The expressions to add to the array
 * @param options - Options object
 * @param options.copy - Whether to copy the argument expressions
 * @param options.dialect - The source dialect
 * @returns An array expression
 */
export function array (
  expressions?: ExpressionValue | (ExpressionValue | undefined)[],
  options: {
    copy?: boolean;
    dialect?: DialectType;
    [key: string]: unknown;
  } = {},
): ArrayExpr {
  const {
    copy = true, dialect, ...opts
  } = options;

  const expressionList = Array.from(ensureIterable(expressions));
  return new ArrayExpr({
    expressions: expressionList.map((expr) => maybeParse(expr as ExpressionValue, {
      copy,
      dialect,
      ...opts,
    })),
  });
}

/**
 * Returns a tuple.
 *
 * Example:
 *     tuple_([1, 'x']).sql()
 *     // '(1, x)'
 *
 * @param expressions - The expressions to add to the tuple
 * @param options - Options object
 * @param options.copy - Whether to copy the argument expressions
 * @param options.dialect - The source dialect
 * @returns A tuple expression
 */
export function tuple (
  expressions?: ExpressionValue | (ExpressionValue | undefined)[],
  options: {
    copy?: boolean;
    dialect?: DialectType;
    [key: string]: unknown;
  } = {},
): TupleExpr {
  const {
    copy = true, dialect, ...opts
  } = options;

  const expressionList = Array.from(ensureIterable(expressions));
  return new TupleExpr({
    expressions: expressionList.map((expr) => maybeParse(expr as ExpressionValue, {
      copy,
      dialect,
      ...opts,
    })),
  });
}

/**
 * Build ALTER TABLE... RENAME... expression
 *
 * Example:
 *     renameTable('old_table', 'new_table').sql()
 *     // 'ALTER TABLE old_table RENAME TO new_table'
 *
 * @param oldName - The old name of the table
 * @param newName - The new name of the table
 * @param options - Options object
 * @param options.dialect - The dialect to parse the table
 * @returns Alter table expression
 */
export function renameTable (
  oldName: ExpressionOrString<TableExpr>,
  newName: ExpressionOrString<TableExpr>,
  options: {
    dialect?: DialectType;
  } = {},
): AlterExpr {
  const { dialect } = options;

  const oldTable = toTable(oldName, { dialect });
  const newTable = toTable(newName, { dialect });

  return new AlterExpr({
    this: oldTable,
    kind: AlterExprKind.TABLE,
    actions: [new AlterRenameExpr({ this: newTable })],
  });
}

/**
 * Build ALTER TABLE... RENAME COLUMN... expression
 *
 * Example:
 *     renameColumn('my_table', 'old_col', 'new_col').sql()
 *     // 'ALTER TABLE my_table RENAME COLUMN old_col TO new_col'
 *
 * @param tableName - Name of the table
 * @param oldColumnName - The old name of the column
 * @param newColumnName - The new name of the column
 * @param options - Options object
 * @param options.exists - Whether to add the IF EXISTS clause
 * @param options.dialect - The dialect to parse the table/column
 * @returns Alter table expression
 */
export function renameColumn (
  tableName: ExpressionOrString<TableExpr>,
  oldColumnName: ExpressionOrString<ColumnExpr>,
  newColumnName: ExpressionOrString<ColumnExpr>,
  options: {
    exists?: boolean;
    dialect?: DialectType;
  } = {},
): AlterExpr {
  const {
    exists, dialect,
  } = options;

  const tableExpr = toTable(tableName, { dialect });
  const oldColumn = toColumn(oldColumnName, { dialect });
  const newColumn = toColumn(newColumnName, { dialect });

  return new AlterExpr({
    this: tableExpr,
    kind: AlterExprKind.TABLE,
    actions: [
      new RenameColumnExpr({
        this: oldColumn,
        to: newColumn,
        exists,
      }),
    ],
  });
}

/**
 * Return all table names referenced through columns in an expression.
 *
 * Example:
 *     columnTableNames(parse('a.b AND c.d AND c.e'))
 *     // Set(['a', 'c'])
 *
 * @param expression - Expression to find table names
 * @param exclude - A table name to exclude
 * @returns A set of unique table names
 */
export function columnTableNames (
  expression: Expression,
  options: { exclude?: string } = {},
): Set<string> {
  const { exclude = '' } = options;
  const tableNames = new Set<string>();

  for (const col of expression.findAll(ColumnExpr)) {
    const tableName = col.table;
    if (tableName && tableName !== exclude) {
      tableNames.add(tableName);
    }
  }

  return tableNames;
}

/**
 * Get the full name of a table as a string.
 *
 * Example:
 *     tableName(parse('select * from a.b.c').find(TableExpr))
 *     // 'a.b.c'
 *
 * @param tableExpr - Table expression node or string
 * @param options - Options object
 * @param options.dialect - The dialect to generate the table name for
 * @param options.identify - Whether to always quote identifiers
 * @returns The table name
 */
export function tableName (
  tableExpr: TableExpr | string,
  options: {
    dialect?: DialectType;
    identify?: boolean;
  } = {},
): string {
  const {
    dialect, identify = false,
  } = options;

  const table = maybeParse(tableExpr, {
    into: TableExpr,
    dialect,
  });

  if (!table) {
    throw new Error(`Cannot parse ${tableExpr}`);
  }

  return table.parts
    .map((part) => {
      if (identify || !SAFE_IDENTIFIER_RE.test(part.name)) {
        return part.sql({
          dialect,
          identify: true,
          copy: false,
          comments: false,
        });
      }
      return part.name;
    })
    .join('.');
}

/**
 * Replace children of an expression with the result of a function.
 *
 * @param expression - The expression whose children to replace
 * @param fun - Function to apply to each child node
 * @param args - Additional arguments to pass to the function
 * @param kwargs - Additional keyword arguments to pass to the function
 */
export function replaceChildren (
  expression: Expression,
  fun: (child: Expression, ...args: unknown[]) => Expression | Expression[],
  ...args: unknown[]
): void {
  for (const [key, value] of Object.entries(expression.args)) {
    const isListArg = Array.isArray(value);
    const childNodes = isListArg ? value : [value];
    const newChildNodes: Expression[] = [];

    for (const childNode of childNodes) {
      if (childNode instanceof Expression) {
        const result = fun(childNode, ...args);
        const resultArray = Array.isArray(result) ? result : [result];
        newChildNodes.push(...resultArray);
      } else {
        newChildNodes.push(childNode);
      }
    }

    if (isListArg) {
      expression.setArgKey(key, newChildNodes);
    } else {
      expression.setArgKey(key, newChildNodes[0]);
    }
  }
}

/**
 * Replace an entire tree with the result of function calls on each node.
 *
 * This will be traversed in reverse DFS, so leaves first.
 * If new nodes are created as a result of function calls, they will also be traversed.
 *
 * @param expression - The root expression
 * @param fun - Function to apply to each node
 * @param prune - Optional function to determine if a subtree should be pruned
 * @returns The transformed expression
 */
export function replaceTree (
  expression: Expression,
  fun: (node: Expression) => Expression,
  prune?: (node: Expression) => boolean,
): Expression {
  const stack = Array.from(expression.dfs({ prune }));
  let newNode = expression;

  while (0 < stack.length) {
    const node = stack.pop()!;
    newNode = fun(node);

    if (newNode !== node) {
      node.replace(newNode);

      if (newNode instanceof Expression) {
        stack.push(newNode);
      }
    }
  }

  return newNode;
}

/**
 * Returns a case normalized table name without quotes.
 *
 * Example:
 *     normalizeTableName('`A-B`.c', { dialect: 'bigquery' })
 *     // 'A-B.c'
 *
 * @param tableExpr - The table to normalize
 * @param options - Options object
 * @param options.dialect - The dialect to use for normalization rules
 * @param options.copy - Whether to copy the expression
 * @returns Normalized table name
 */
export function normalizeTableName (
  tableExpr: ExpressionOrString<TableExpr>,
  options: {
    dialect?: DialectType;
    copy?: boolean;
  } = {},
): string {
  const {
    dialect, copy = true,
  } = options;

  return normalizeIdentifiers(toTable(tableExpr, {
    dialect,
    copy,
  }), { dialect }).parts.map((p) => p.name).join('.');
}

/**
 * Replace all tables in expression according to the mapping.
 *
 * Example:
 *     replaceTables(parse('select * from a.b'), { 'a.b': 'c' }).sql()
 *     // 'SELECT * FROM c'
 *
 * @param expression - Expression node to be transformed and replaced
 * @param mapping - Mapping of table names
 * @param options - Options object
 * @param options.dialect - The dialect of the mapping table
 * @param options.copy - Whether to copy the expression
 * @returns The mapped expression
 */
export function replaceTables<T extends Expression> (
  expression: T,
  mapping: Record<string, string>,
  options: {
    dialect?: DialectType;
    copy?: boolean;
  } = {},
): T {
  const {
    dialect, copy = true,
  } = options;

  const normalizedMapping: Record<string, string> = {};
  for (const [key, value] of Object.entries(mapping)) {
    normalizedMapping[normalizeTableName(key, { dialect })] = value;
  }

  function replaceTablesTransform (node: Expression): Expression {
    if (node instanceof TableExpr && node.meta['replace'] !== false) {
      const original = normalizeTableName(node, { dialect });
      const newName = normalizedMapping[original];

      if (newName) {
        const newTable = toTable(newName, { dialect });
        // Copy over other args except table parts
        for (const [key, value] of Object.entries(node.args)) {
          if (![
            'this',
            'db',
            'catalog',
          ].includes(key)) {
            newTable.setArgKey(key, value as ExpressionValue | ExpressionValueList);
          }
        }
        newTable.addComments([original]);
        return newTable;
      }
    }
    return node;
  }

  return expression.transform(replaceTablesTransform, { copy }) as T;
}

/**
 * Replace placeholders in an expression.
 *
 * Example:
 *     replacePlaceholders(
 *       parse('select * from :tbl where ? = ?'),
 *       toIdentifier('str_col'), 'b', { tbl: toIdentifier('foo') }
 *     ).sql()
 *     // "SELECT * FROM foo WHERE str_col = 'b'"
 *
 * @param expression - Expression node to be transformed and replaced
 * @param args - Positional values that will substitute unnamed placeholders in order
 * @returns The mapped expression
 */
export function replacePlaceholders (
  expression: Expression,
  ...args: unknown[]
): Expression {
  // Separate positional args from the last arg if it's an object (kwargs)
  let positionalArgs: unknown[];
  let kwargs: Record<string, unknown> = {};

  if (0 < args.length && typeof args[args.length - 1] === 'object' && args[args.length - 1] != undefined && !Array.isArray(args[args.length - 1]) && !(args[args.length - 1] instanceof Expression)) {
    kwargs = args[args.length - 1] as Record<string, unknown>;
    positionalArgs = args.slice(0, -1);
  } else {
    positionalArgs = args;
  }

  let argIndex = 0;

  function replacePlaceholder (node: Expression): Expression {
    if (node instanceof PlaceholderExpr) {
      if (typeof node.args.this === 'string') {
        const newName = kwargs[node.args.this];
        if (newName !== undefined) {
          return convert(newName);
        }
      } else {
        if (argIndex < positionalArgs.length) {
          return convert(positionalArgs[argIndex++]);
        }
      }
    }
    return node;
  }

  return expression.transform(replacePlaceholder);
}

/**
 * Transforms an expression by expanding all referenced sources into subqueries.
 *
 * Example:
 *     expand(parse('select * from x AS z'), { x: parse('select * from y') }).sql()
 *     // 'SELECT * FROM (SELECT * FROM y) AS z'
 *
 * @param expression - The expression to expand
 * @param sources - A dict of name to query or a callable that provides a query on demand
 * @param options - Options object
 * @param options.dialect - The dialect of the sources dict or the callable
 * @param options.copy - Whether to copy the expression during transformation
 * @returns The transformed expression
 */
export function expand (
  expression: Expression,
  sources: Record<string, QueryExpr | (() => QueryExpr)>,
  options: {
    dialect?: DialectType;
    copy?: boolean;
  } = {},
): Expression {
  const {
    dialect, copy = true,
  } = options;

  const normalizedSources: Record<string, QueryExpr | (() => QueryExpr)> = {};
  for (const [key, value] of Object.entries(sources)) {
    normalizedSources[normalizeTableName(key, { dialect })] = value;
  }

  function expandTransform (node: Expression): Expression {
    if (node instanceof TableExpr) {
      const name = normalizeTableName(node, { dialect });
      const source = normalizedSources[name];

      if (source) {
        const parsedSource = typeof source === 'function' ? source() : source;
        const aliasName = node.args.alias || name;
        const subqueryExpr = parsedSource.subquery(aliasName);
        subqueryExpr.comments = [`source: ${name}`];

        return subqueryExpr.transform(expandTransform, { copy: false });
      }
    }
    return node;
  }

  return expression.transform(expandTransform, { copy });
}

/** Query expression types that don't need to be wrapped in parentheses */
export const UNWRAPPED_QUERIES = [SelectExpr, SetOperationExpr] as const;

/** Percentile function classes */
export const PERCENTILES = [PercentileContExpr, PercentileDiscExpr] as const;

/** Non-null constant expression types */
export const NONNULL_CONSTANTS = [LiteralExpr, BooleanExpr] as const;

/** All constant expression types (including NULL) */
export const CONSTANTS = [
  LiteralExpr,
  BooleanExpr,
  NullExpr,
] as const;

export {
  FUNCTION_BY_NAME, ALL_FUNCTIONS,
} from '../parser/function_registry';

/**
 * Set of JSON path part expression classes
 */
export const JSON_PATH_PARTS = new Set<typeof Expression>([
  JsonPathFilterExpr,
  JsonPathKeyExpr,
  JsonPathRecursiveExpr,
  JsonPathRootExpr,
  JsonPathScriptExpr,
  JsonPathSliceExpr,
  JsonPathSubscriptExpr,
  JsonPathUnionExpr,
  JsonPathWildcardExpr,
]);

/**
 * Checks whether the given value matches any of the provided DataTypeExprKind values.
 * - If `value` is an Expression, delegates to its `.isType()` method.
 * - If `value` is a string or DataTypeExprKind, normalizes it via `enumFromString` and compares.
 */
export function isType (
  value: unknown,
  dtypes: DataTypeExprKind | Iterable<DataTypeExprKind> | string | Iterable<string>,
): boolean {
  if (!isInstanceOf(value, 'string', Expression)) {
    return false;
  }

  if (value === undefined) {
    return false;
  }

  if (value instanceof Expression) {
    return value.isType(dtypes);
  }

  const dtypesIterable = typeof dtypes !== 'string' && isIterable(dtypes)
    ? dtypes
    : [dtypes as DataTypeExprKind | string];

  for (const dtype of dtypesIterable) {
    if (dtype.toLowerCase() === value.toLowerCase()) {
      return true;
    }
  }

  const normalized = enumFromString(DataTypeExprKind, value.toString());
  if (normalized === undefined) {
    return false;
  }

  for (const dtype of dtypesIterable) {
    if (dtype === normalized) return true;
  }
  return false;
}
