// https://github.com/tobymao/sqlglot/blob/main/sqlglot/parser.py

import type { Expression } from './expressions';
import * as exp from './expressions';
import {
  Dialect, type DialectType,
} from './dialects/dialect';
import {
  ErrorLevel, ParseError, TokenError, concatMessages,
} from './errors';
import type { Token } from './tokens';
import {
  Tokenizer, TokenType,
} from './tokens';

export interface ParseOptions {
  dialect?: DialectType;
  errorLevel?: ErrorLevel;
  maxErrors?: number;
  [key: string]: unknown;
}

/**
 * Parser converts a list of tokens into an Abstract Syntax Tree (AST).
 *
 * The parser follows a recursive descent approach with separate methods
 * for each grammar rule. Methods are named _parse_* for internal parsing
 * logic.
 */
export class Parser {
  // Static configuration - operator precedence mappings
  static UNARY_PARSERS: Partial<Record<TokenType, (parser: Parser) => Expression>> = {};
  static PRIMARY_PARSERS: Partial<Record<TokenType, (parser: Parser, token: Token) => Expression>> = {};
  static PLACEHOLDER_PARSERS: Partial<Record<TokenType, (parser: Parser) => Expression>> = {};

  static RANGE_PARSERS: Partial<Record<TokenType, (parser: Parser, expr: Expression) => Expression>> = {};
  static STATEMENT_PARSERS: Partial<Record<TokenType, (parser: Parser) => Expression>> = {};

  // Operator precedence levels
  static EQUALITY: Partial<Record<TokenType, typeof Expression>> = {
    [TokenType.EQ]: exp.EQExpr,
    [TokenType.NEQ]: exp.NEQExpr,
  };

  static COMPARISON: Partial<Record<TokenType, typeof Expression>> = {
    [TokenType.GT]: exp.GTExpr,
    [TokenType.GTE]: exp.GTEExpr,
    [TokenType.LT]: exp.LTExpr,
    [TokenType.LTE]: exp.LTEExpr,
  };

  static BITWISE: Partial<Record<TokenType, typeof Expression>> = {
    [TokenType.AMP]: exp.BitwiseAndExpr,
    [TokenType.CARET]: exp.BitwiseXorExpr,
    [TokenType.PIPE]: exp.BitwiseOrExpr,
  };

  static TERM: Partial<Record<TokenType, typeof Expression>> = {
    [TokenType.DASH]: exp.SubExpr,
    [TokenType.PLUS]: exp.AddExpr,
    [TokenType.MOD]: exp.ModExpr,
  };

  static FACTOR: Partial<Record<TokenType, typeof Expression>> = {
    [TokenType.DIV]: exp.IntDivExpr,
    [TokenType.SLASH]: exp.DivExpr,
    [TokenType.STAR]: exp.MulExpr,
  };

  // Instance variables
  protected sql: string;
  protected dialect: Dialect;
  protected errorLevel: ErrorLevel;
  protected maxErrors: number;
  protected errors: ParseError[];
  protected _tokens: Token[];
  protected _index: number;
  protected _curr?: Token;
  protected _next?: Token;
  protected _prev?: Token;

  constructor (options?: ParseOptions) {
    const opts = options ?? {};
    this.sql = '';
    this.dialect = Dialect.getOrRaise(opts.dialect);
    this.errorLevel = opts.errorLevel ?? ErrorLevel.IMMEDIATE;
    this.maxErrors = opts.maxErrors ?? 3;
    this.errors = [];
    this._tokens = [];
    this._index = 0;
    this._curr = null;
    this._next = null;
    this._prev = null;
  }

  /**
   * Parse SQL string into an array of expressions.
   */
  parse (sql: string | Token[], opts?: ParseOptions): Expression[] {
    if (typeof sql === 'string') {
      this.sql = sql;
      this.errors = [];
      this.dialect = opts?.dialect
        ? Dialect.getOrRaise(opts.dialect)
        : this.dialect;

      // Tokenize the SQL
      const tokenizer = new Tokenizer();
      try {
        this._tokens = tokenizer.tokenize(sql);
      } catch (e) {
        if (e instanceof TokenError) {
          this.errors.push(new ParseError(e.message));
          return [];
        }
        throw e;
      }
    } else {
      this._tokens = sql;
    }

    this._index = 0;
    this._reset();

    const expressions: Expression[] = [];

    while (this._index < this._tokens.length) {
      if (this._curr?.tokenType === TokenType.SEMICOLON) {
        this._advance();
        continue;
      }

      const expr = this._parseStatement();
      if (expr) {
        expressions.push(expr);
      }

      // Safety check to prevent infinite loops
      if (this._tokens.length <= this._index) {
        break;
      }
    }

    if (this.errorLevel === ErrorLevel.RAISE && 0 < this.errors.length) {
      throw new ParseError(concatMessages(this.errors.map((e) => e.message), this.maxErrors));
    }

    return expressions;
  }

  /**
   * Parse SQL into a specific expression type.
   */
  parseInto (_into: typeof Expression, sql: string, opts?: ParseOptions): Expression | undefined {
    const expressions = this.parse(sql, opts);
    return expressions[0];
  }

  /**
   * Reset the parser state and advance to first token.
   */
  protected _reset (): void {
    this._index = 0;
    this._curr = null;
    this._next = null;
    this._prev = null;
    this._advance();
  }

  /**
   * Advance to the next token.
   */
  protected _advance (times = 1): boolean {
    for (let i = 0; i < times; i++) {
      this._prev = this._curr;
      this._curr = this._next || (this._index < this._tokens.length
        ? this._tokens[this._index]
        : undefined);
      this._index += 1;
      this._next = this._index < this._tokens.length
        ? this._tokens[this._index]
        : undefined;
    }
    return this._curr !== undefined;
  }

  /**
   * Retreat to the previous token.
   */
  protected _retreat (index: number): void {
    if (0 <= index && index < this._tokens.length) {
      this._index = index;
      this._curr = this._tokens[index];
      this._next = index + 1 < this._tokens.length
        ? this._tokens[index + 1]
        : undefined;
      this._prev = 0 < index
        ? this._tokens[index - 1]
        : undefined;
    }
  }

  /**
   * Check if current token matches the given type.
   */
  protected _match (tokenType: TokenType, advance = true): boolean {
    if (this._curr?.tokenType === tokenType) {
      if (advance) {
        this._advance();
      }
      return true;
    }
    return false;
  }

  /**
   * Check if current token matches any of the given types.
   */
  protected _matchSet (tokenTypes: TokenType[] | Set<TokenType>, advance = true): boolean {
    const types = Array.isArray(tokenTypes)
      ? tokenTypes
      : Array.from(tokenTypes);
    for (const tokenType of types) {
      if (this._match(tokenType, advance)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if current token text matches (case-insensitive).
   */
  protected _matchTextSeq (...texts: string[]): boolean {
    const index = this._index - 1;
    for (let i = 0; i < texts.length; i++) {
      const token = this._tokens[index + i];
      if (!token || token.text.toUpperCase() !== texts[i].toUpperCase()) {
        return false;
      }
    }
    this._advance(texts.length);
    return true;
  }

  /**
   * Create an expression node.
   */
  expression<T extends Expression>(
    expClass: new (args: any) => T,
    options?: { [key: string]: any },
  ): T {
    return new expClass(options ?? {});
  }

  /**
   * Raise a parse error.
   */
  protected raiseError (message: string, _token?: Token): void {
    const err = new ParseError(message);
    this.errors.push(err);

    if (this.errorLevel === ErrorLevel.IMMEDIATE) {
      throw err;
    }
  }

  // ============================================================================
  // Statement Parsers
  // ============================================================================

  /**
   * Parse a SQL statement.
   */
  protected _parseStatement (): Expression | undefined {
    if (!this._curr) {
      return null;
    }

    const parser = Parser.STATEMENT_PARSERS[this._curr.tokenType];
    if (parser) {
      return parser(this);
    }

    // Try to parse as SELECT
    if (this._curr.tokenType === TokenType.SELECT || this._curr.tokenType === TokenType.WITH) {
      return this._parseSelect();
    }

    // Try to parse as INSERT
    if (this._curr.tokenType === TokenType.INSERT) {
      return this._parseInsert();
    }

    // Try to parse as UPDATE
    if (this._curr.tokenType === TokenType.UPDATE) {
      return this._parseUpdate();
    }

    // Try to parse as DELETE
    if (this._curr.tokenType === TokenType.DELETE) {
      return this._parseDelete();
    }

    // Try to parse as a general expression
    const expr = this._parseExpression();
    if (expr) {
      return expr;
    }

    this.raiseError(`Unexpected token: ${this._curr.text}`);
    this._advance();
    return null;
  }

  /**
   * Parse a SELECT statement.
   */
  protected _parseSelect (): Expression | undefined {
    // Check for WITH clause (CTE)
    const withClause = this._parseWith();

    if (!this._match(TokenType.SELECT)) {
      return null;
    }

    // Parse DISTINCT
    const distinct = this._match(TokenType.DISTINCT);

    const expressions: Expression[] = [];

    // Parse projection list
    do {
      if (this._match(TokenType.STAR)) {
        expressions.push(this.expression(exp.StarExpr, {}));
      } else {
        const expr = this._parseExpression();
        if (expr) {
          // Check for alias
          if (this._match(TokenType.ALIAS) || this._curr?.tokenType === TokenType.VAR) {
            const alias = this._parseIdVar();
            if (alias) {
              const aliased = this.expression(exp.AliasExpr, {
                this: expr,
                alias,
              });
              expressions.push(aliased);
              continue;
            }
          }
          expressions.push(expr);
        }
      }
    } while (this._match(TokenType.COMMA));

    const args: any = { expressions };

    if (distinct) {
      args.distinct = true;
    }

    if (withClause) {
      args.with = withClause;
    }

    // Parse FROM
    const from = this._parseFrom();
    if (from) {
      args.from = from;
    }

    // Parse WHERE
    const where = this._parseWhere();
    if (where) {
      args.where = where;
    }

    // Parse GROUP BY
    const group = this._parseGroup();
    if (group) {
      args.group = group;
    }

    // Parse HAVING
    const having = this._parseHaving();
    if (having) {
      args.having = having;
    }

    // Parse ORDER BY
    const order = this._parseOrder();
    if (order) {
      args.order = order;
    }

    // Parse LIMIT
    const limit = this._parseLimit();
    if (limit) {
      args.limit = limit;
    }

    return this.expression(exp.SelectExpr, args);
  }

  // ============================================================================
  // Expression Parsers
  // ============================================================================

  /**
   * Parse a general expression.
   */
  protected _parseExpression (): Expression | undefined {
    return this._parseDisjunction();
  }

  /**
   * Parse OR expressions.
   */
  protected _parseDisjunction (): Expression | undefined {
    return this._parseConjunction();
  }

  /**
   * Parse AND expressions.
   */
  protected _parseConjunction (): Expression | undefined {
    return this._parseEquality();
  }

  /**
   * Parse equality expressions (=, !=, <>).
   */
  protected _parseEquality (): Expression | undefined {
    return this._parseBinary(this._parseComparison.bind(this), Parser.EQUALITY);
  }

  /**
   * Parse comparison expressions (<, <=, >, >=).
   */
  protected _parseComparison (): Expression | undefined {
    const expr = this._parseBinary(this._parseBitwise.bind(this), Parser.COMPARISON);
    if (!expr) {
      return null;
    }

    // Handle IN operator
    if (this._match(TokenType.IN)) {
      if (this._match(TokenType.L_PAREN)) {
        const values: Expression[] = [];
        do {
          const val = this._parseExpression();
          if (val) {
            values.push(val);
          }
        } while (this._match(TokenType.COMMA));
        this._match(TokenType.R_PAREN);

        return this.expression(exp.InExpr, {
          this: expr,
          expressions: values,
        });
      }
    }

    // Handle LIKE operator
    if (this._match(TokenType.LIKE)) {
      const pattern = this._parseBitwise();
      if (pattern) {
        return this.expression(exp.LikeExpr, {
          this: expr,
          expression: pattern,
        });
      }
    }

    // Handle BETWEEN operator
    if (this._match(TokenType.BETWEEN)) {
      const low = this._parseBitwise();
      if (low && this._match(TokenType.AND)) {
        const high = this._parseBitwise();
        if (high) {
          return this.expression(exp.BetweenExpr, {
            this: expr,
            low,
            high,
          });
        }
      }
    }

    // Handle IS NULL / IS NOT NULL
    if (this._match(TokenType.IS)) {
      const not = this._match(TokenType.NOT);
      if (this._match(TokenType.NULL)) {
        if (not) {
          return this.expression(exp.IsExpr, {
            this: expr,
            expression: this.expression(exp.NotExpr, { this: this.expression(exp.NullExpr, {}) }),
          });
        }
        return this.expression(exp.IsExpr, {
          this: expr,
          expression: this.expression(exp.NullExpr, {}),
        });
      }
    }

    return expr;
  }

  /**
   * Parse bitwise expressions (&, |, ^).
   */
  protected _parseBitwise (): Expression | undefined {
    return this._parseBinary(this._parseTerm.bind(this), Parser.BITWISE);
  }

  /**
   * Parse term expressions (+, -).
   */
  protected _parseTerm (): Expression | undefined {
    return this._parseBinary(this._parseFactor.bind(this), Parser.TERM);
  }

  /**
   * Parse factor expressions (*, /, %).
   */
  protected _parseFactor (): Expression | undefined {
    return this._parseBinary(this._parseUnary.bind(this), Parser.FACTOR);
  }

  /**
   * Parse unary expressions (-, NOT).
   */
  protected _parseUnary (): Expression | undefined {
    if (this._curr?.tokenType && Parser.UNARY_PARSERS[this._curr.tokenType]) {
      const parser = Parser.UNARY_PARSERS[this._curr.tokenType];
      if (parser) {
        this._advance();
        return parser(this);
      }
    }
    return this._parsePrimary();
  }

  /**
   * Parse primary expressions (literals, identifiers, etc.).
   */
  protected _parsePrimary (): Expression | undefined {
    if (!this._curr) {
      return null;
    }

    const token = this._curr;
    const parser = Parser.PRIMARY_PARSERS[token.tokenType];

    if (parser) {
      this._advance();
      return parser(this, token);
    }

    // Try to parse parenthesized expression or subquery
    const paren = this._parseParen();
    if (paren) {
      return paren;
    }

    // Try to parse function call
    const func = this._parseFunction();
    if (func) {
      return func;
    }

    // Try to parse as identifier or column
    if (token.tokenType === TokenType.VAR || token.tokenType === TokenType.IDENTIFIER) {
      return this._parseColumn();
    }

    return null;
  }

  /**
   * Parse parenthesized expression or subquery.
   */
  protected _parseParen (): Expression | undefined {
    if (!this._match(TokenType.L_PAREN)) {
      return null;
    }

    // Try to parse as SELECT (subquery)
    const select = this._parseSelect();
    if (select) {
      this._match(TokenType.R_PAREN);
      return this.expression(exp.SubqueryExpr, { this: select });
    }

    // Parse as expression
    const expr = this._parseExpression();
    if (!expr) {
      this._match(TokenType.R_PAREN);
      return null;
    }

    this._match(TokenType.R_PAREN);
    return this.expression(exp.ParenExpr, { this: expr });
  }

  /**
   * Parse a function call.
   */
  protected _parseFunction (): Expression | undefined {
    if (!this._curr || !this._next) {
      return null;
    }

    // Check if it looks like a function call (identifier followed by left paren)
    if (this._curr.tokenType !== TokenType.VAR && this._curr.tokenType !== TokenType.IDENTIFIER) {
      return null;
    }

    if (this._next.tokenType !== TokenType.L_PAREN) {
      return null;
    }

    const funcName = this._curr.text;
    this._advance(2); // Advance past function name and '('

    // Parse function arguments
    const args: Expression[] = [];
    if (!this._match(TokenType.R_PAREN, false)) {
      do {
        const arg = this._parseExpression();
        if (arg) {
          args.push(arg);
        }
      } while (this._match(TokenType.COMMA));
      this._match(TokenType.R_PAREN);
    } else {
      this._advance(); // Match closing paren
    }

    // Create appropriate function expression based on name
    const upperName = funcName.toUpperCase();

    // Check for CAST function
    if (upperName === 'CAST' && 0 < args.length) {
      // CAST(expr AS type) - args[0] should be expr, look for AS
      // For now, treat as anonymous function and handle later
      return this.expression(exp.CastExpr, {
        this: args[0],
        to: args[1] || null,
      });
    }

    // Check for common aggregate functions
    if (upperName === 'COUNT') {
      return this.expression(exp.CountExpr, { expressions: args });
    } else if (upperName === 'SUM') {
      return this.expression(exp.SumExpr, { this: args[0] });
    } else if (upperName === 'AVG') {
      return this.expression(exp.AvgExpr, { this: args[0] });
    } else if (upperName === 'MIN') {
      return this.expression(exp.MinExpr, { this: args[0] });
    } else if (upperName === 'MAX') {
      return this.expression(exp.MaxExpr, { this: args[0] });
    }

    // Default to anonymous function
    return this.expression(exp.AnonymousExpr, {
      this: funcName,
      expressions: args,
    });
  }

  /**
   * Parse CASE expression.
   */
  protected _parseCase (): Expression | undefined {
    if (!this._match(TokenType.CASE)) {
      return null;
    }

    // Optional: CASE <expr> WHEN ... (simple case)
    let caseExpr: Expression | undefined;
    if (!this._match(TokenType.WHEN, false)) {
      caseExpr = this._parseExpression();
    }

    const ifs: Expression[] = [];

    // Parse WHEN clauses
    while (this._match(TokenType.WHEN)) {
      const condition = this._parseExpression();
      if (!condition) {
        this.raiseError('Expected condition after WHEN');
        break;
      }

      if (!this._match(TokenType.THEN)) {
        this.raiseError('Expected THEN after WHEN condition');
        break;
      }

      const result = this._parseExpression();
      if (!result) {
        this.raiseError('Expected result after THEN');
        break;
      }

      ifs.push(this.expression(exp.IfExpr, {
        this: condition,
        true: result,
      }));
    }

    // Optional: ELSE clause
    let defaultExpr: Expression | undefined;
    if (this._match(TokenType.ELSE)) {
      defaultExpr = this._parseExpression();
    }

    if (!this._match(TokenType.END)) {
      this.raiseError('Expected END after CASE');
    }

    return this.expression(exp.CaseExpr, {
      this: caseExpr,
      ifs,
      default: defaultExpr,
    });
  }

  /**
   * Parse a binary expression with given precedence.
   */
  protected _parseBinary (
    parseNext: () => Expression | undefined,
    operators: Partial<Record<TokenType, typeof Expression>>,
  ): Expression | undefined {
    let left = parseNext();
    if (!left) {
      return null;
    }

    while (this._curr && operators[this._curr.tokenType]) {
      const ExprClass = operators[this._curr.tokenType]!;
      this._advance();
      const right = parseNext();
      if (!right) {
        this.raiseError('Expected expression after operator');
        return left;
      }
      left = this.expression(ExprClass, {
        left,
        expression: right,
      });
    }

    return left;
  }

  /**
   * Parse a column reference.
   */
  protected _parseColumn (): Expression | undefined {
    const parts: Expression[] = [];

    while (true) {
      if (!this._curr) break;

      if (this._curr.tokenType === TokenType.VAR || this._curr.tokenType === TokenType.IDENTIFIER) {
        const identifier = this.expression(exp.IdentifierExpr, { this: this._curr.text });
        parts.push(identifier);
        this._advance();

        if (this._match(TokenType.DOT)) {
          continue;
        }
      }
      break;
    }

    if (parts.length === 0) {
      return null;
    }

    if (parts.length === 1) {
      return this.expression(exp.ColumnExpr, { this: parts[0] });
    }

    // Handle qualified names: catalog.db.table.column
    const args: any = { this: parts[parts.length - 1] };
    if (2 <= parts.length) args.table = parts[parts.length - 2];
    if (3 <= parts.length) args.db = parts[parts.length - 3];
    if (4 <= parts.length) args.catalog = parts[parts.length - 4];

    return this.expression(exp.ColumnExpr, args);
  }

  // ============================================================================
  // SQL Clause Parsers
  // ============================================================================

  /**
   * Parse FROM clause.
   */
  protected _parseFrom (): Expression | undefined {
    if (!this._match(TokenType.FROM)) {
      return null;
    }

    const table = this._parseTable();
    if (!table) {
      return null;
    }

    return this.expression(exp.FromExpr, { this: table });
  }

  /**
   * Parse WHERE clause.
   */
  protected _parseWhere (): Expression | undefined {
    if (!this._match(TokenType.WHERE)) {
      return null;
    }

    const condition = this._parseDisjunction();
    if (!condition) {
      return null;
    }

    return this.expression(exp.WhereExpr, { this: condition });
  }

  /**
   * Parse GROUP BY clause.
   */
  protected _parseGroup (): Expression | undefined {
    if (!this._match(TokenType.GROUP_BY)) {
      return null;
    }

    const expressions: Expression[] = [];
    do {
      const expr = this._parseExpression();
      if (expr) {
        expressions.push(expr);
      }
    } while (this._match(TokenType.COMMA));

    if (expressions.length === 0) {
      return null;
    }

    return this.expression(exp.GroupExpr, { expressions });
  }

  /**
   * Parse HAVING clause.
   */
  protected _parseHaving (): Expression | undefined {
    if (!this._match(TokenType.HAVING)) {
      return null;
    }

    const condition = this._parseDisjunction();
    if (!condition) {
      return null;
    }

    return this.expression(exp.HavingExpr, { this: condition });
  }

  /**
   * Parse ORDER BY clause.
   */
  protected _parseOrder (): Expression | undefined {
    if (!this._match(TokenType.ORDER_BY)) {
      return null;
    }

    const expressions: Expression[] = [];
    do {
      const ordered = this._parseOrdered();
      if (ordered) {
        expressions.push(ordered);
      }
    } while (this._match(TokenType.COMMA));

    if (expressions.length === 0) {
      return null;
    }

    return this.expression(exp.OrderExpr, { expressions });
  }

  /**
   * Parse an ordered expression (for ORDER BY).
   */
  protected _parseOrdered (): Expression | undefined {
    const expr = this._parseExpression();
    if (!expr) {
      return null;
    }

    this._match(TokenType.ASC);
    const desc = this._match(TokenType.DESC);

    const args: any = { this: expr };
    if (desc) {
      args.desc = true;
    }

    // Parse NULLS FIRST/LAST
    if (this._matchTextSeq('NULLS', 'FIRST')) {
      args.nullsFirst = true;
    } else if (this._matchTextSeq('NULLS', 'LAST')) {
      args.nullsFirst = false;
    }

    return this.expression(exp.OrderedExpr, args);
  }

  /**
   * Parse LIMIT clause.
   */
  protected _parseLimit (): Expression | undefined {
    if (!this._match(TokenType.LIMIT)) {
      return null;
    }

    const expr = this._parseExpression();
    if (!expr) {
      return null;
    }

    const args: any = { expression: expr };

    // Parse OFFSET
    if (this._match(TokenType.COMMA)) {
      args.offset = expr;
      args.expression = this._parseExpression();
    } else if (this._match(TokenType.OFFSET)) {
      args.offset = this._parseExpression();
    }

    return this.expression(exp.LimitExpr, args);
  }

  /**
   * Parse a table reference.
   */
  protected _parseTable (): Expression | undefined {
    const parts: Expression[] = [];

    // Parse table name parts (catalog.db.table)
    while (true) {
      if (!this._curr) break;

      if (this._curr.tokenType === TokenType.VAR || this._curr.tokenType === TokenType.IDENTIFIER) {
        const identifier = this.expression(exp.IdentifierExpr, { this: this._curr.text });
        parts.push(identifier);
        this._advance();

        if (this._match(TokenType.DOT)) {
          continue;
        }
      }
      break;
    }

    if (parts.length === 0) {
      return null;
    }

    // Build table expression
    const args: any = { this: parts[parts.length - 1] };
    if (2 <= parts.length) args.db = parts[parts.length - 2];
    if (3 <= parts.length) args.catalog = parts[parts.length - 3];

    const table = this.expression(exp.TableExpr, args);

    // Parse table alias
    if (this._match(TokenType.ALIAS) || this._curr?.tokenType === TokenType.VAR) {
      const alias = this._parseIdVar();
      if (alias) {
        return this.expression(exp.TableExpr, {
          ...args,
          alias: this.expression(exp.TableAliasExpr, { this: alias }),
        });
      }
    }

    // Parse JOINs
    const joins: Expression[] = [];
    let join: Expression | undefined;
    while ((join = this._parseJoin())) {
      joins.push(join);
    }

    if (0 < joins.length) {
      table.set('joins', joins);
    }

    return table;
  }

  /**
   * Parse a JOIN clause.
   */
  protected _parseJoin (): Expression | undefined {
    const index = this._index;

    // Parse join type
    let side: string | undefined;
    let kind: string | undefined;

    // Parse side (LEFT, RIGHT, FULL)
    if (this._match(TokenType.LEFT)) {
      side = 'LEFT';
    } else if (this._match(TokenType.RIGHT)) {
      side = 'RIGHT';
    } else if (this._match(TokenType.FULL)) {
      side = 'FULL';
    }

    // Parse kind (OUTER, INNER, CROSS)
    if (this._match(TokenType.OUTER)) {
      kind = 'OUTER';
    } else if (this._match(TokenType.INNER)) {
      kind = 'INNER';
    } else if (this._match(TokenType.CROSS)) {
      kind = 'CROSS';
    }

    // Must have JOIN keyword
    if (!this._match(TokenType.JOIN)) {
      this._retreat(index);
      return null;
    }

    // Parse joined table
    const table = this._parseTableParts();
    if (!table) {
      this.raiseError('Expected table after JOIN');
      return null;
    }

    const args: any = { this: table };

    if (side) args.side = side;
    if (kind) args.kind = kind;

    // Parse ON clause
    if (this._match(TokenType.ON)) {
      args.on = this._parseDisjunction();
    }
    // Parse USING clause
    else if (this._match(TokenType.USING)) {
      if (this._match(TokenType.L_PAREN)) {
        const columns: Expression[] = [];
        do {
          const col = this._parseColumn();
          if (col) {
            columns.push(col);
          }
        } while (this._match(TokenType.COMMA));
        this._match(TokenType.R_PAREN);
        args.using = columns;
      }
    }

    return this.expression(exp.JoinExpr, args);
  }

  /**
   * Parse table name parts (for qualified names).
   */
  protected _parseTableParts (): Expression | undefined {
    const parts: Expression[] = [];

    while (true) {
      if (!this._curr) break;

      if (this._curr.tokenType === TokenType.VAR || this._curr.tokenType === TokenType.IDENTIFIER) {
        const identifier = this.expression(exp.IdentifierExpr, { this: this._curr.text });
        parts.push(identifier);
        this._advance();

        if (this._match(TokenType.DOT)) {
          continue;
        }
      }
      break;
    }

    if (parts.length === 0) {
      return null;
    }

    const args: any = { this: parts[parts.length - 1] };
    if (2 <= parts.length) args.db = parts[parts.length - 2];
    if (3 <= parts.length) args.catalog = parts[parts.length - 3];

    return this.expression(exp.TableExpr, args);
  }

  /**
   * Parse an identifier variable.
   */
  protected _parseIdVar (): Expression | undefined {
    if (!this._curr) {
      return null;
    }

    if (this._curr.tokenType === TokenType.VAR || this._curr.tokenType === TokenType.IDENTIFIER) {
      const text = this._curr.text;
      this._advance();
      return this.expression(exp.IdentifierExpr, { this: text });
    }

    return null;
  }

  // ============================================================================
  // DML Statement Parsers (INSERT, UPDATE, DELETE)
  // ============================================================================

  /**
   * Parse INSERT statement.
   */
  protected _parseInsert (): Expression | undefined {
    if (!this._match(TokenType.INSERT)) {
      return null;
    }

    // Optional: INTO keyword
    this._match(TokenType.INTO);

    // Parse target table
    const table = this._parseTable();
    if (!table) {
      this.raiseError('Expected table name after INSERT INTO');
      return null;
    }

    const args: any = { this: table };

    // Optional: column list
    if (this._match(TokenType.L_PAREN)) {
      const columns: Expression[] = [];
      do {
        const col = this._parseColumn();
        if (col) {
          columns.push(col);
        }
      } while (this._match(TokenType.COMMA));
      this._match(TokenType.R_PAREN);

      if (0 < columns.length) {
        args.columns = columns;
      }
    }

    // Parse VALUES clause or SELECT
    if (this._match(TokenType.VALUES)) {
      const values: Expression[] = [];

      do {
        if (this._match(TokenType.L_PAREN)) {
          const row: Expression[] = [];
          do {
            const val = this._parseExpression();
            if (val) {
              row.push(val);
            }
          } while (this._match(TokenType.COMMA));
          this._match(TokenType.R_PAREN);

          values.push(this.expression(exp.TupleExpr, { expressions: row }));
        }
      } while (this._match(TokenType.COMMA));

      if (0 < values.length) {
        args.expression = this.expression(exp.ValuesExpr, { expressions: values });
      }
    } else {
      // Try to parse SELECT
      const select = this._parseSelect();
      if (select) {
        args.expression = select;
      }
    }

    return this.expression(exp.InsertExpr, args);
  }

  /**
   * Parse UPDATE statement.
   */
  protected _parseUpdate (): Expression | undefined {
    if (!this._match(TokenType.UPDATE)) {
      return null;
    }

    // Parse target table
    const table = this._parseTable();
    if (!table) {
      this.raiseError('Expected table name after UPDATE');
      return null;
    }

    const args: any = { this: table };

    // Parse SET clause
    if (this._match(TokenType.SET)) {
      const expressions: Expression[] = [];
      do {
        const col = this._parseColumn();
        if (col && this._match(TokenType.EQ)) {
          const val = this._parseExpression();
          if (val) {
            expressions.push(this.expression(exp.EQExpr, {
              this: col,
              expression: val,
            }));
          }
        }
      } while (this._match(TokenType.COMMA));

      if (0 < expressions.length) {
        args.expressions = expressions;
      }
    }

    // Optional: WHERE clause
    const where = this._parseWhere();
    if (where) {
      args.where = where;
    }

    // Optional: ORDER BY
    const order = this._parseOrder();
    if (order) {
      args.order = order;
    }

    // Optional: LIMIT
    const limit = this._parseLimit();
    if (limit) {
      args.limit = limit;
    }

    return this.expression(exp.UpdateExpr, args);
  }

  /**
   * Parse DELETE statement.
   */
  protected _parseDelete (): Expression | undefined {
    if (!this._match(TokenType.DELETE)) {
      return null;
    }

    const args: any = {};

    // Optional: FROM keyword
    if (!this._match(TokenType.FROM)) {
      this.raiseError('Expected FROM after DELETE');
      return null;
    }

    // Parse target table
    const table = this._parseTable();
    if (!table) {
      this.raiseError('Expected table name after DELETE FROM');
      return null;
    }

    args.this = table;

    // Optional: WHERE clause
    const where = this._parseWhere();
    if (where) {
      args.where = where;
    }

    // Optional: ORDER BY
    const order = this._parseOrder();
    if (order) {
      args.order = order;
    }

    // Optional: LIMIT
    const limit = this._parseLimit();
    if (limit) {
      args.limit = limit;
    }

    return this.expression(exp.DeleteExpr, args);
  }

  // ============================================================================
  // CTE and Advanced Features
  // ============================================================================

  /**
   * Parse WITH clause (Common Table Expressions).
   */
  protected _parseWith (): Expression | undefined {
    if (!this._match(TokenType.WITH)) {
      return null;
    }

    const ctes: Expression[] = [];

    do {
      // Parse CTE name
      const name = this._parseIdVar();
      if (!name) {
        this.raiseError('Expected CTE name after WITH');
        break;
      }

      // Optional: column list
      let columns: Expression[] | undefined;
      if (this._match(TokenType.L_PAREN)) {
        columns = [];
        do {
          const col = this._parseIdVar();
          if (col) {
            columns.push(col);
          }
        } while (this._match(TokenType.COMMA));
        this._match(TokenType.R_PAREN);
      }

      // AS keyword
      if (!this._match(TokenType.ALIAS)) {
        this.raiseError('Expected AS after CTE name');
        break;
      }

      // Parse CTE query (must be in parentheses)
      if (!this._match(TokenType.L_PAREN)) {
        this.raiseError('Expected ( after AS');
        break;
      }

      const query = this._parseSelect();
      if (!query) {
        this.raiseError('Expected SELECT in CTE');
        break;
      }

      this._match(TokenType.R_PAREN);

      const cte = this.expression(exp.CTEExpr, {
        this: query,
        alias: this.expression(exp.TableAliasExpr, {
          this: name,
          columns,
        }),
      });

      ctes.push(cte);
    } while (this._match(TokenType.COMMA));

    if (ctes.length === 0) {
      return null;
    }

    return this.expression(exp.WithExpr, { expressions: ctes });
  }
}

// Initialize static parsers
Parser.PRIMARY_PARSERS = {
  [TokenType.STRING]: (parser, token) => parser.expression(exp.LiteralExpr, {
    this: token.text,
    isString: true,
  }),
  [TokenType.NUMBER]: (parser, token) => parser.expression(exp.LiteralExpr, {
    this: token.text,
    isString: false,
  }),
  [TokenType.NULL]: (parser) => parser.expression(exp.NullExpr, {}),
  [TokenType.TRUE]: (parser) => parser.expression(exp.BooleanExpr, { this: true }),
  [TokenType.FALSE]: (parser) => parser.expression(exp.BooleanExpr, { this: false }),
  [TokenType.STAR]: (parser) => parser.expression(exp.StarExpr, {}),
  [TokenType.CASE]: (parser) => parser['_parseCase'](),
};

Parser.UNARY_PARSERS = {
  [TokenType.DASH]: (parser) => parser.expression(exp.NegExpr, { this: parser['_parseUnary']() }),
  [TokenType.NOT]: (parser) => parser.expression(exp.NotExpr, { this: parser['_parseEquality']() }),
};

/**
 * Standalone parse function for convenience.
 */
export function parse (
  sql: string | Token[],
  dialect?: DialectType,
  opts?: ParseOptions,
): Expression[] {
  const parser = new Parser({
    dialect,
    ...opts,
  });
  return parser.parse(sql);
}

/**
 * Parse a single expression.
 */
export function parseOne (
  sql: string,
  dialect?: DialectType,
  opts?: ParseOptions,
): Expression | undefined {
  const expressions = parse(sql, dialect, opts);
  return expressions[0];
}
