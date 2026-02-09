// https://github.com/tobymao/sqlglot/blob/main/sqlglot/generator.py

import { Expression } from './expressions';
import type { ParseOptions } from './parser';
import { Dialect, type DialectType, setGeneratorClass } from './dialects/dialect';
import { ErrorLevel, UnsupportedError, concatMessages } from './errors';

export interface GeneratorOptions extends ParseOptions {
  pretty?: boolean;
  identify?: string | boolean;
  normalize?: boolean;
  pad?: number;
  indent?: number;
  normalizeFunctions?: string | boolean;
  unsupportedLevel?: ErrorLevel;
  maxUnsupported?: number;
  leadingComma?: boolean;
  maxTextWidth?: number;
  comments?: boolean;
  dialect?: DialectType;
  [key: string]: unknown;
}

export interface TranspileOptions extends ParseOptions {
  pretty?: boolean;
  [key: string]: unknown;
}

type TransformFn = (generator: Generator, expression: Expression) => string;

/**
 * Generator converts a given syntax tree to the corresponding SQL string.
 *
 * Args:
 *   pretty: Whether to format the produced SQL string. Default: False.
 *   identify: Determines when an identifier should be quoted. Possible values are:
 *     False (default): Never quote, except in cases where it's mandatory by the dialect.
 *     True: Always quote except for specials cases.
 *     'safe': Only quote identifiers that are case insensitive.
 *   normalize: Whether to normalize identifiers to lowercase. Default: False.
 *   pad: The pad size in a formatted string. Default: 2.
 *   indent: The indentation size in a formatted string. Default: 2.
 *   normalizeFunctions: How to normalize function names. Possible values are:
 *     "upper" or True (default): Convert names to uppercase.
 *     "lower": Convert names to lowercase.
 *     False: Disables function name normalization.
 *   unsupportedLevel: Determines the generator's behavior when it encounters unsupported expressions.
 *     Default ErrorLevel.WARN.
 *   maxUnsupported: Maximum number of unsupported messages to include in a raised UnsupportedError.
 *     This is only relevant if unsupported_level is ErrorLevel.RAISE. Default: 3
 *   leadingComma: Whether the comma is leading or trailing in select expressions.
 *     This is only relevant when generating in pretty mode. Default: False
 *   maxTextWidth: The max number of characters in a segment before creating new lines in pretty mode.
 *     Default: 80
 *   comments: Whether to preserve comments in the output SQL code. Default: True
 */
export class Generator {
  // Static feature flags
  static NULL_ORDERING_SUPPORTED: boolean | null = true;
  static IGNORE_NULLS_IN_FUNC = false;
  static LOCKING_READS_SUPPORTED = false;
  static EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = true;
  static WRAP_DERIVED_VALUES = true;
  static CREATE_FUNCTION_RETURN_AS = true;
  static MATCHED_BY_SOURCE = true;
  static SINGLE_STRING_INTERVAL = false;
  static INTERVAL_ALLOWS_PLURAL_FORM = true;
  static LIMIT_FETCH = 'ALL';
  static LIMIT_ONLY_LITERALS = false;
  static RENAME_TABLE_WITH_DB = true;
  static GROUPINGS_SEP = ',';
  static INDEX_ON = 'ON';
  static INOUT_SEPARATOR = ' ';
  static JOIN_HINTS = true;
  static DIRECTED_JOINS = false;
  static TABLE_HINTS = true;
  static QUERY_HINTS = true;
  static QUERY_HINT_SEP = ', ';
  static IS_BOOL_ALLOWED = true;
  static DUPLICATE_KEY_UPDATE_WITH_SET = true;
  static LIMIT_IS_TOP = false;
  static RETURNING_END = true;
  static EXTRACT_ALLOWS_QUOTES = true;
  static TZ_TO_WITH_TIME_ZONE = false;
  static NVL2_SUPPORTED = true;
  static SELECT_KINDS: string[] = ['STRUCT', 'VALUE'];
  static VALUES_AS_TABLE = true;
  static ALTER_TABLE_INCLUDE_COLUMN_KEYWORD = true;
  static UNNEST_WITH_ORDINALITY = true;
  static AGGREGATE_FILTER_SUPPORTED = true;
  static SEMI_ANTI_JOIN_WITH_SIDE = true;
  static COMPUTED_COLUMN_WITH_TYPE = true;
  static SUPPORTS_TABLE_COPY = true;
  static TABLESAMPLE_REQUIRES_PARENS = true;
  static TABLESAMPLE_SIZE_IS_ROWS = true;
  static TABLESAMPLE_KEYWORDS = 'TABLESAMPLE';
  static TABLESAMPLE_WITH_METHOD = true;
  static TABLESAMPLE_SEED_KEYWORD = 'SEED';
  static COLLATE_IS_FUNC = false;
  static DATA_TYPE_SPECIFIERS_ALLOWED = false;
  static ENSURE_BOOLS = false;
  static CTE_RECURSIVE_KEYWORD_REQUIRED = true;
  static SUPPORTS_SINGLE_ARG_CONCAT = true;
  static LAST_DAY_SUPPORTS_DATE_PART = true;
  static SUPPORTS_TABLE_ALIAS_COLUMNS = true;
  static UNPIVOT_ALIASES_ARE_IDENTIFIERS = true;
  static JSON_KEY_VALUE_PAIR_SEP = ':';
  static INSERT_OVERWRITE = ' OVERWRITE TABLE';
  static SUPPORTS_SELECT_INTO = false;
  static SUPPORTS_UNLOGGED_TABLES = false;
  static SUPPORTS_CREATE_TABLE_LIKE = true;
  static LIKE_PROPERTY_INSIDE_SCHEMA = false;
  static MULTI_ARG_DISTINCT = true;
  static JSON_TYPE_REQUIRED_FOR_EXTRACTION = false;
  static JSON_PATH_BRACKETED_KEY_SUPPORTED = true;
  static JSON_PATH_SINGLE_QUOTE_ESCAPE = false;
  static CAN_IMPLEMENT_ARRAY_ANY = false;
  static SUPPORTS_TO_NUMBER = true;
  static SUPPORTS_WINDOW_EXCLUDE = false;
  static SET_OP_MODIFIERS = true;
  static COPY_PARAMS_ARE_WRAPPED = true;
  static COPY_PARAMS_EQ_REQUIRED = false;
  static COPY_HAS_INTO_KEYWORD = true;
  static TRY_SUPPORTED = true;
  static SUPPORTS_UESCAPE = true;
  static STAR_EXCEPT = 'EXCEPT';
  static HEX_FUNC = 'HEX';
  static WITH_PROPERTIES_PREFIX = 'WITH';
  static QUOTE_JSON_PATH = true;
  static PAD_FILL_PATTERN_IS_REQUIRED = false;
  static SUPPORTS_EXPLODING_PROJECTIONS = true;
  static ARRAY_CONCAT_IS_VAR_LEN = true;
  static SUPPORTS_CONVERT_TIMEZONE = false;
  static SUPPORTS_MEDIAN = true;
  static SUPPORTS_UNIX_SECONDS = false;
  static ALTER_SET_WRAPPED = false;
  static NORMALIZE_EXTRACT_DATE_PARTS = false;
  static PARSE_JSON_NAME: string | null = 'PARSE_JSON';
  static ARRAY_SIZE_NAME = 'ARRAY_LENGTH';
  static ALTER_SET_TYPE = 'SET DATA TYPE';
  static ARRAY_SIZE_DIM_REQUIRED: boolean | null = null;
  static SUPPORTS_DECODE_CASE = true;
  static SUPPORTS_BETWEEN_FLAGS = false;
  static SUPPORTS_LIKE_QUANTIFIERS = true;
  static MATCH_AGAINST_TABLE_PREFIX: string | null = null;
  static SET_ASSIGNMENT_REQUIRES_VARIABLE_KEYWORD = false;
  static UPDATE_STATEMENT_SUPPORTS_FROM = true;

  static SENTINEL_LINE_BREAK = '__SQLGLOT__LB__';

  // TRANSFORMS caching
  protected static transformsCache = new WeakMap<typeof Generator, Map<typeof Expression, TransformFn>>();

  // Instance properties
  protected pretty: boolean;
  protected identify: string | boolean;
  protected normalize: boolean;
  protected pad: number;
  protected _indent: number;
  protected normalizeFunctions: string | boolean;
  protected unsupportedLevel: ErrorLevel;
  protected maxUnsupported: number;
  protected leadingComma: boolean;
  protected maxTextWidth: number;
  protected comments: boolean;
  protected dialect: Dialect;
  protected unsupportedMessages: string[];

  constructor(options?: GeneratorOptions) {
    const opts = options ?? {};

    this.pretty = opts.pretty ?? false;
    this.identify = opts.identify ?? false;
    this.normalize = opts.normalize ?? false;
    this.pad = opts.pad ?? 2;
    this._indent = opts.indent ?? 2;
    this.unsupportedLevel = opts.unsupportedLevel ?? ErrorLevel.WARN;
    this.maxUnsupported = opts.maxUnsupported ?? 3;
    this.leadingComma = opts.leadingComma ?? false;
    this.maxTextWidth = opts.maxTextWidth ?? 80;
    this.comments = opts.comments ?? true;

    // Get dialect and prioritize option over dialect default
    this.dialect = Dialect.getOrRaise(opts.dialect);
    const dialectClass = this.dialect.constructor as typeof Dialect;
    this.normalizeFunctions = opts.normalizeFunctions ?? dialectClass.NORMALIZE_FUNCTIONS ?? 'upper';

    this.unsupportedMessages = [];
  }

  /**
   * Get or create the TRANSFORMS map for this Generator class.
   * Uses WeakMap caching to avoid recreating the map on every instantiation.
   */
  protected static getTransforms(): Map<typeof Expression, TransformFn> {
    let cache = this.transformsCache.get(this);
    if (cache === undefined) {
      cache = new Map();
      // TODO: Populate TRANSFORMS in Phase 3
      this.transformsCache.set(this, cache);
    }
    return cache;
  }

  /**
   * Main generate method - converts an expression tree to SQL string.
   */
  generate(expression: Expression, options?: { copy?: boolean }): string {
    const copy = options?.copy ?? true;
    const expr = copy ? expression.copy() : expression;

    // Preprocess expression
    const preprocessed = this.preprocess(expr);

    // Reset unsupported messages
    this.unsupportedMessages = [];

    // Generate SQL
    let sql = this.sql(preprocessed).trim();

    // Replace sentinel line breaks in pretty mode
    if (this.pretty) {
      sql = sql.replace(new RegExp((this.constructor as typeof Generator).SENTINEL_LINE_BREAK, 'g'), '\n');
    }

    // Handle unsupported messages
    if (this.unsupportedLevel === ErrorLevel.IGNORE) {
      return sql;
    }

    if (this.unsupportedLevel === ErrorLevel.WARN) {
      for (const msg of this.unsupportedMessages) {
        console.warn(msg);
      }
    } else if (this.unsupportedLevel === ErrorLevel.RAISE && this.unsupportedMessages.length > 0) {
      throw new UnsupportedError(concatMessages(this.unsupportedMessages, this.maxUnsupported));
    }

    return sql;
  }

  /**
   * Apply generic preprocessing transformations to an expression.
   */
  protected preprocess(expression: Expression): Expression {
    // TODO: Add preprocessing logic like moving CTEs to top level
    return expression;
  }

  /**
   * Core SQL generation method with auto-discovery.
   *
   * @param expression - Expression to generate SQL for (or string/undefined)
   * @param key - Optional key to extract from expression.args
   * @param comment - Whether to include comments (default: true)
   */
  sql(expression?: Expression | string, key?: string, comment = true): string {
    // Handle undefined/null early
    if (expression === undefined || expression === null) {
      return '';
    }

    // Handle string literals
    if (typeof expression === 'string') {
      return expression;
    }

    // Handle key extraction
    if (key !== undefined) {
      const value = expression.args[key];
      if (value !== undefined && value !== null) {
        if (typeof value === 'string' || value instanceof Expression) {
          return this.sql(value);
        }
      }
      return '';
    }

    // Check TRANSFORMS
    const ctor = expression.constructor as typeof Expression;
    const transforms = (this.constructor as typeof Generator).getTransforms();
    const transform = transforms.get(ctor);

    if (transform !== undefined) {
      const sql = transform(this, expression);
      return comment ? this.maybeComment(sql, expression) : sql;
    }

    // Auto-discover method: {expressionKey}_sql()
    const methodName = `${expression.key}_sql`;
    const method = (this as any)[methodName];

    if (typeof method === 'function') {
      const sql = method.call(this, expression);
      return comment ? this.maybeComment(sql, expression) : sql;
    }

    // TODO: Add fallbacks for Func and Property expressions

    // Unsupported expression
    this.unsupported(`Unsupported expression type: ${expression.constructor.name}`);
    return '';
  }

  /**
   * Record an unsupported expression/feature.
   */
  unsupported(message: string): void {
    if (this.unsupportedLevel === ErrorLevel.IMMEDIATE) {
      throw new UnsupportedError(message);
    }
    this.unsupportedMessages.push(message);
  }

  /**
   * Add comment to SQL if present.
   */
  protected maybeComment(sql: string, _expression: Expression): string {
    // TODO: Implement comment handling
    return sql;
  }

  /**
   * Generate a separator (space or newline based on pretty mode).
   */
  sep(separator = ' '): string {
    if (this.pretty) {
      return `\n${separator}`;
    }
    return separator;
  }

  /**
   * Generate a segment with separator.
   */
  seg(sql: string, separator = ' '): string {
    if (!sql) {
      return '';
    }
    return `${this.sep(separator)}${sql}`;
  }

  /**
   * Indent SQL lines.
   */
  indent(
    sql: string,
    options?: { level?: number; pad?: number; skipFirst?: boolean; skipLast?: boolean }
  ): string {
    if (!this.pretty || !sql) {
      return sql;
    }

    const level = options?.level ?? 0;
    const pad = options?.pad ?? this.pad;
    const skipFirst = options?.skipFirst ?? false;
    const skipLast = options?.skipLast ?? false;

    const lines = sql.split('\n');
    const indentStr = ' '.repeat(level * this._indent + pad);

    return lines
      .map((line, i) => {
        if ((skipFirst && i === 0) || (skipLast && i === lines.length - 1)) {
          return line;
        }
        return `${indentStr}${line}`;
      })
      .join('\n');
  }

  /**
   * Wrap an expression in parentheses.
   */
  wrap(expression: Expression | string): string {
    const thisSql = typeof expression === 'string' ? expression : this.sql(expression, 'this');
    if (!thisSql) {
      return '()';
    }

    const indented = this.indent(thisSql, { level: 1, pad: 0 });
    return `(${this.sep('')}${indented}${this.seg(')', '')}`;
  }

  /**
   * Normalize a function name based on settings.
   */
  normalizeFunc(name: string): string {
    if (this.normalizeFunctions === 'upper' || this.normalizeFunctions === true) {
      return name.toUpperCase();
    }
    if (this.normalizeFunctions === 'lower') {
      return name.toLowerCase();
    }
    return name;
  }

  /**
   * Generate a function call.
   */
  func(name: string, ...args: Array<Expression | string | undefined>): string {
    const options = {
      prefix: '(',
      suffix: ')',
      normalize: true,
    };
    const normalizedName = options.normalize ? this.normalizeFunc(name) : name;
    return `${normalizedName}${options.prefix}${this.formatArgs(...args)}${options.suffix}`;
  }

  /**
   * Format function arguments.
   */
  formatArgs(...args: Array<Expression | string | undefined | boolean>): string {
    const sep = ', ';
    const argSqls = args
      .filter((arg) => arg !== undefined && arg !== null && typeof arg !== 'boolean')
      .map((arg) => this.sql(arg as Expression | string));

    if (this.pretty && this.tooWide(argSqls)) {
      const joined = `\n${argSqls.join(`${sep.trim()}\n`)}\n`;
      return this.indent(joined, { skipFirst: true, skipLast: true });
    }

    return argSqls.join(sep);
  }

  /**
   * Check if arguments are too wide for a single line.
   */
  tooWide(args: string[]): boolean {
    return args.reduce((sum, arg) => sum + arg.length, 0) > this.maxTextWidth;
  }

  /**
   * Generate SQL for a list of expressions.
   */
  expressions(
    expression?: Expression,
    options?: {
      key?: string;
      sqls?: Array<string | Expression>;
      flat?: boolean;
      indent?: boolean;
      skipFirst?: boolean;
      skipLast?: boolean;
      sep?: string;
      prefix?: string;
      dynamic?: boolean;
      newLine?: boolean;
    }
  ): string {
    const key = options?.key ?? 'expressions';
    const flat = options?.flat ?? false;
    const indentOpt = options?.indent ?? true;
    const skipFirst = options?.skipFirst ?? false;
    const skipLast = options?.skipLast ?? false;
    const sep = options?.sep ?? ', ';
    const prefix = options?.prefix ?? '';
    const dynamic = options?.dynamic ?? false;
    const newLine = options?.newLine ?? false;

    const exprs = expression ? (expression.args[key] as Expression[] | undefined) : options?.sqls;

    if (!exprs || exprs.length === 0) {
      return '';
    }

    // Flat mode - simple join
    if (flat) {
      return exprs
        .map((e) => this.sql(e))
        .filter((sql) => sql)
        .join(sep);
    }

    // Pretty mode with formatting
    const numExprs = exprs.length;
    const resultSqls: string[] = [];

    for (let i = 0; i < exprs.length; i++) {
      const e = exprs[i];
      const sql = typeof e === 'string' ? e : this.sql(e, undefined, false);

      if (!sql) {
        continue;
      }

      const comments = typeof e === 'string' ? '' : this.maybeComment('', e);

      if (this.pretty) {
        if (this.leadingComma) {
          resultSqls.push(`${i > 0 ? sep : ''}${prefix}${sql}${comments}`);
        } else {
          const sepStr = comments ? sep.trimEnd() : sep;
          const trailingSep = i + 1 < numExprs ? sepStr : '';
          resultSqls.push(`${prefix}${sql}${trailingSep}${comments}`);
        }
      } else {
        const trailingSep = i + 1 < numExprs ? sep : '';
        resultSqls.push(`${prefix}${sql}${comments}${trailingSep}`);
      }
    }

    let resultSql: string;
    if (this.pretty && (!dynamic || this.tooWide(resultSqls))) {
      if (newLine) {
        resultSqls.unshift('');
        resultSqls.push('');
      }
      resultSql = resultSqls.map((s) => s.trimEnd()).join('\n');
    } else {
      resultSql = resultSqls.join('');
    }

    return indentOpt ? this.indent(resultSql, { skipFirst, skipLast }) : resultSql;
  }

  /**
   * Generate SQL for a binary operation.
   */
  binary(expression: Expression, op: string): string {
    const sqls: string[] = [];
    const stack: Array<string | Expression> = [expression];
    const binaryType = expression.constructor;

    while (stack.length > 0) {
      const node = stack.pop()!;

      if (node.constructor === binaryType) {
        const expr = node as Expression;
        const left = expr.args.left as Expression;
        const right = expr.args.expression as Expression;

        // TODO: Handle operator() function
        stack.push(right);
        stack.push(` ${op} `);
        stack.push(left);
      } else {
        sqls.push(this.sql(node as Expression | string));
      }
    }

    return sqls.join('');
  }

  // ============================================================================
  // Core Expression Methods
  // ============================================================================

  /**
   * Generate SQL for a literal value.
   */
  literal_sql(expression: Expression): string {
    const text = (expression as any).$this ?? '';
    // TODO: Handle string escaping properly
    return String(text);
  }

  /**
   * Generate SQL for an identifier.
   */
  identifier_sql(expression: Expression): string {
    let text = (expression as any).name ?? (expression as any).$this ?? '';
    const quoted = (expression.args as any).quoted;

    // Normalize to lowercase if needed
    const lower = text.toLowerCase();
    text = this.normalize && !quoted ? lower : text;

    // TODO: Handle identifier escaping and quoting
    // TODO: Check reserved keywords
    // TODO: Check if quoting is needed

    return text;
  }

  /**
   * Generate SQL for a column reference.
   */
  column_sql(expression: Expression): string {
    const parts: string[] = [];

    const catalog = expression.args.catalog;
    if (catalog && (typeof catalog === 'string' || catalog instanceof Expression)) {
      parts.push(this.sql(catalog));
    }

    const db = expression.args.db;
    if (db && (typeof db === 'string' || db instanceof Expression)) {
      parts.push(this.sql(db));
    }

    const table = expression.args.table;
    if (table && (typeof table === 'string' || table instanceof Expression)) {
      parts.push(this.sql(table));
    }

    const thisArg = expression.args.this;
    if (thisArg && (typeof thisArg === 'string' || thisArg instanceof Expression)) {
      parts.push(this.sql(thisArg));
    }

    return parts.filter((p) => p).join('.');
  }

  /**
   * Generate SQL for a table reference.
   */
  table_sql(expression: Expression): string {
    const parts: string[] = [];

    const catalog = expression.args.catalog;
    if (catalog && (typeof catalog === 'string' || catalog instanceof Expression)) {
      parts.push(this.sql(catalog));
    }

    const db = expression.args.db;
    if (db && (typeof db === 'string' || db instanceof Expression)) {
      parts.push(this.sql(db));
    }

    const thisArg = expression.args.this;
    if (thisArg && (typeof thisArg === 'string' || thisArg instanceof Expression)) {
      parts.push(this.sql(thisArg));
    }

    const tableName = parts.filter((p) => p).join('.');

    const alias = expression.args.alias;
    if (alias && (typeof alias === 'string' || alias instanceof Expression)) {
      return `${tableName} AS ${this.sql(alias)}`;
    }

    return tableName;
  }

  /**
   * Generate SQL for NULL.
   */
  null_sql(_expression: Expression): string {
    return 'NULL';
  }

  /**
   * Generate SQL for a boolean.
   */
  boolean_sql(expression: Expression): string {
    return (expression as any).$this ? 'TRUE' : 'FALSE';
  }

  /**
   * Generate SQL for a star (SELECT *).
   */
  star_sql(_expression: Expression): string {
    return '*';
  }

  /**
   * Generate SQL for a placeholder.
   */
  placeholder_sql(expression: Expression): string {
    return this.sql(expression, 'this') || '?';
  }

  /**
   * Fallback for unsupported function expressions.
   */
  function_fallback_sql(expression: Expression): string {
    const name = (expression as any).sql_name?.() ?? expression.key;
    const args: Expression[] = [];

    // Collect all arguments from arg_types
    // TODO: Implement proper argument collection based on expression.arg_types

    return this.func(name, ...args);
  }

  // ============================================================================
  // SQL Clause Methods
  // ============================================================================

  /**
   * Generate SQL for FROM clause.
   */
  from_sql(expression: Expression): string {
    return `${this.seg('FROM')} ${this.sql(expression, 'this')}`;
  }

  /**
   * Generate SQL for WHERE clause.
   */
  where_sql(expression: Expression): string {
    const condition = this.indent(this.sql(expression, 'this'));
    return `${this.seg('WHERE')}${this.sep()}${condition}`;
  }

  /**
   * Generate SQL for GROUP BY clause.
   */
  group_sql(expression: Expression): string {
    const groupByAll = expression.args.all;
    let modifier = '';
    if (groupByAll === true) {
      modifier = ' ALL';
    } else if (groupByAll === false) {
      modifier = ' DISTINCT';
    }

    return this.opExpressions(`GROUP BY${modifier}`, expression);
  }

  /**
   * Generate SQL for HAVING clause.
   */
  having_sql(expression: Expression): string {
    const condition = this.indent(this.sql(expression, 'this'));
    return `${this.seg('HAVING')}${this.sep()}${condition}`;
  }

  /**
   * Generate SQL for ORDER BY clause.
   */
  order_sql(expression: Expression): string {
    const thisArg = this.sql(expression, 'this');
    const prefix = thisArg ? `${thisArg} ` : '';
    return this.opExpressions(`${prefix}ORDER BY`, expression);
  }

  /**
   * Helper for generating operator expressions like "GROUP BY x, y, z".
   */
  protected opExpressions(op: string, expression: Expression, options?: { flat?: boolean }): string {
    const flat = options?.flat ?? false;
    const expressionsSql = this.expressions(expression, { flat });
    if (flat) {
      return `${op} ${expressionsSql}`;
    }
    return `${this.seg(op)}${expressionsSql ? this.sep() : ''}${expressionsSql}`;
  }

  /**
   * Generate SQL for a simple SELECT statement.
   */
  select_sql(expression: Expression): string {
    // Get the projection list
    const projections = this.expressions(expression);
    const expressionsSql = projections ? `${this.sep()}${projections}` : '';

    // Build the SELECT clause
    let sql = `SELECT${expressionsSql}`;

    // Add FROM clause
    const fromClause = this.sql(expression, 'from_', false);
    if (fromClause) {
      sql += fromClause;
    }

    // Add WHERE clause
    const whereClause = this.sql(expression, 'where', false);
    if (whereClause) {
      sql += whereClause;
    }

    // Add GROUP BY clause
    const groupClause = this.sql(expression, 'group', false);
    if (groupClause) {
      sql += groupClause;
    }

    // Add HAVING clause
    const havingClause = this.sql(expression, 'having', false);
    if (havingClause) {
      sql += havingClause;
    }

    // Add ORDER BY clause
    const orderClause = this.sql(expression, 'order', false);
    if (orderClause) {
      sql += orderClause;
    }

    // Add LIMIT clause
    const limitClause = this.sql(expression, 'limit', false);
    if (limitClause) {
      sql += limitClause;
    }

    return sql;
  }

  /**
   * Generate SQL for LIMIT clause.
   */
  limit_sql(expression: Expression): string {
    const limit = this.sql(expression, 'this');
    return `${this.seg('LIMIT')} ${limit}`;
  }

  // ============================================================================
  // Operator Methods
  // ============================================================================

  /**
   * Generate SQL for addition (+).
   */
  add_sql(expression: Expression): string {
    return this.binary(expression, '+');
  }

  /**
   * Generate SQL for subtraction (-).
   */
  sub_sql(expression: Expression): string {
    return this.binary(expression, '-');
  }

  /**
   * Generate SQL for multiplication (*).
   */
  mul_sql(expression: Expression): string {
    return this.binary(expression, '*');
  }

  /**
   * Generate SQL for division (/).
   */
  div_sql(expression: Expression): string {
    return this.binary(expression, '/');
  }

  /**
   * Generate SQL for modulo (%).
   */
  mod_sql(expression: Expression): string {
    return this.binary(expression, '%');
  }

  /**
   * Generate SQL for equality (=).
   */
  eq_sql(expression: Expression): string {
    return this.binary(expression, '=');
  }

  /**
   * Generate SQL for inequality (<>).
   */
  neq_sql(expression: Expression): string {
    return this.binary(expression, '<>');
  }

  /**
   * Generate SQL for greater than (>).
   */
  gt_sql(expression: Expression): string {
    return this.binary(expression, '>');
  }

  /**
   * Generate SQL for greater than or equal (>=).
   */
  gte_sql(expression: Expression): string {
    return this.binary(expression, '>=');
  }

  /**
   * Generate SQL for less than (<).
   */
  lt_sql(expression: Expression): string {
    return this.binary(expression, '<');
  }

  /**
   * Generate SQL for less than or equal (<=).
   */
  lte_sql(expression: Expression): string {
    return this.binary(expression, '<=');
  }

  /**
   * Generate SQL for AND.
   */
  and_sql(expression: Expression): string {
    return this.connectorSql(expression, 'AND');
  }

  /**
   * Generate SQL for OR.
   */
  or_sql(expression: Expression): string {
    return this.connectorSql(expression, 'OR');
  }

  /**
   * Generate SQL for NOT.
   */
  not_sql(expression: Expression): string {
    return `NOT ${this.sql(expression, 'this')}`;
  }

  /**
   * Helper for generating connector SQL (AND/OR).
   */
  protected connectorSql(expression: Expression, op: string): string {
    // Check if we have a list of expressions
    const exprs = expression.args.expressions as Expression[] | undefined;
    if (exprs && exprs.length > 0) {
      return this.expressions(expression, { sep: ` ${op} ` });
    }

    // Otherwise use binary format
    return this.binary(expression, op);
  }

  /**
   * Generate SQL for IS NULL.
   */
  isnull_sql(expression: Expression): string {
    return `${this.sql(expression, 'this')} IS NULL`;
  }

  /**
   * Generate SQL for IS NOT NULL.
   */
  isnotnull_sql(expression: Expression): string {
    return `${this.sql(expression, 'this')} IS NOT NULL`;
  }

  /**
   * Generate SQL for IN.
   */
  in_sql(expression: Expression): string {
    const query = this.sql(expression, 'this');
    const expressions = this.sql(expression, 'expressions');
    return `${query} IN ${this.wrap(expressions)}`;
  }

  /**
   * Generate SQL for LIKE.
   */
  like_sql(expression: Expression): string {
    return this.binary(expression, 'LIKE');
  }

  /**
   * Generate SQL for BETWEEN.
   */
  between_sql(expression: Expression): string {
    const thisSql = this.sql(expression, 'this');
    const low = this.sql(expression, 'low');
    const high = this.sql(expression, 'high');
    return `${thisSql} BETWEEN ${low} AND ${high}`;
  }

  // ============================================================================
  // Additional Expression Methods
  // ============================================================================

  /**
   * Generate SQL for parenthesized expressions.
   */
  paren_sql(expression: Expression): string {
    const sql = this.seg(this.indent(this.sql(expression, 'this')), '');
    return `(${sql}${this.seg(')', '')}`;
  }

  /**
   * Generate SQL for aliased expressions.
   */
  alias_sql(expression: Expression): string {
    const alias = this.sql(expression, 'alias');
    const aliasSql = alias ? ` AS ${alias}` : '';
    return `${this.sql(expression, 'this')}${aliasSql}`;
  }

  /**
   * Generate SQL for CAST expressions.
   */
  cast_sql(expression: Expression): string {
    const format = this.sql(expression, 'format');
    const formatSql = format ? ` FORMAT ${format}` : '';
    const to = this.sql(expression, 'to');
    const toSql = to ? ` ${to}` : '';
    return `CAST(${this.sql(expression, 'this')} AS${toSql}${formatSql})`;
  }

  /**
   * Generate SQL for CASE expressions.
   */
  case_sql(expression: Expression): string {
    const thisSql = this.sql(expression, 'this');
    const statements: string[] = [];

    statements.push(thisSql ? `CASE ${thisSql}` : 'CASE');

    // Process WHEN...THEN clauses
    const ifs = expression.args.ifs as Expression[] | undefined;
    if (ifs) {
      for (const ifExpr of ifs) {
        statements.push(`WHEN ${this.sql(ifExpr, 'this')}`);
        statements.push(`THEN ${this.sql(ifExpr, 'true')}`);
      }
    }

    // Process ELSE clause
    const defaultExpr = this.sql(expression, 'default');
    if (defaultExpr) {
      statements.push(`ELSE ${defaultExpr}`);
    }

    statements.push('END');

    // Check if too wide for single line
    if (this.pretty && this.tooWide(statements)) {
      return this.indent(statements.join('\n'), { skipFirst: true, skipLast: true });
    }

    return statements.join(' ');
  }

  /**
   * Generate SQL for JOIN clauses.
   */
  join_sql(expression: Expression): string {
    const kind = expression.args.kind as string | undefined;
    const side = expression.args.side as string | undefined;
    const method = expression.args.method as string | undefined;

    // Build the JOIN operator
    const ops: string[] = [];
    if (method) ops.push(method);
    if (side) ops.push(side);
    if (kind) ops.push(kind);

    let opSql = ops.filter((op) => op).join(' ');
    if (opSql && opSql !== 'STRAIGHT_JOIN') {
      opSql = `${opSql} JOIN`;
    } else if (!opSql) {
      opSql = 'JOIN';
    }

    // Get the table being joined
    const thisSql = this.sql(expression, 'this');

    // Get the ON condition
    let onSql = this.sql(expression, 'on');
    if (onSql) {
      onSql = this.indent(onSql, { skipFirst: true });
      const space = this.pretty ? this.seg(' '.repeat(this.pad)) : ' ';
      onSql = `${space}ON ${onSql}`;
    }

    return `${this.seg(opSql)} ${thisSql}${onSql}`;
  }

  /**
   * Generate SQL for negative numbers.
   */
  neg_sql(expression: Expression): string {
    const thisSql = this.sql(expression, 'this');
    // Avoid converting "- -5" to "--5" which is a comment
    const sep = thisSql[0] === '-' ? ' ' : '';
    return `-${sep}${thisSql}`;
  }

  /**
   * Generate SQL for data types.
   */
  datatype_sql(expression: Expression): string {
    const type = expression.args.this;
    if (!type) {
      return '';
    }

    // Handle nested types (e.g., ARRAY<INT>)
    const nested = expression.args.nested as Expression | undefined;
    if (nested) {
      return `${type}<${this.sql(nested)}>`;
    }

    // Handle parameterized types (e.g., VARCHAR(255))
    const expressions = expression.args.expressions as Expression[] | undefined;
    if (expressions && expressions.length > 0) {
      const params = expressions.map((e) => this.sql(e)).join(', ');
      return `${type}(${params})`;
    }

    return String(type);
  }
}

/**
 * Standalone generate function for convenience.
 */
export function generate(expression: Expression, opts?: GeneratorOptions): string {
  const generator = new Generator(opts);
  return generator.generate(expression);
}

// Set the generator class on Dialect to avoid circular dependency
setGeneratorClass(Generator);
