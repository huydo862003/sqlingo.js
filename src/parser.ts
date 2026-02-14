// https://github.com/tobymao/sqlglot/blob/main/sqlglot/parser.py

import type { Expression } from './expressions';
import {
  array,
  ArrayAggExpr,
  ArrayAppendExpr,
  ArrayConcatExpr,
  ArrayExpr,
  ArrayPrependExpr,
  ArrayRemoveExpr,
  BinaryExpr,
  CastExpr,
  CoalesceExpr,
  ConcatExpr,
  ConcatWsExpr,
  ConvertTimezoneExpr,
  CountExpr,
  DataTypeExpr,
  DataTypeType,
  EscapeExpr,
  GenerateDateArrayExpr,
  GlobExpr,
  GreatestExpr,
  HexExpr,
  IntervalExpr,
  JSONExtractExpr,
  JSONExtractScalarExpr,
  JSONKeysExpr,
  LeastExpr,
  LikeExpr,
  LiteralExpr,
  LnExpr,
  LogExpr,
  LowerExpr,
  LowerHexExpr,
  ModExpr,
  PadExpr,
  ParenExpr,
  ScopeResolutionExpr,
  StarMapExpr,
  StrPositionExpr,
  SubstringExpr,
  TrimExpr,
  TrimPosition,
  UnnestExpr,
  UpperExpr,
  UuidExpr,
  varExpr,
  VarMapExpr,
} from './expressions';
import { ensureList, seqGet } from './helper';
import { Dialect, type DialectType } from './dialects/dialect';
import { ErrorLevel, ParseError } from './errors';
import type { Token } from './tokens';
import { TokenType } from './tokens';

export type OptionsType = Record<string, (string[] | string)[]>;

// Used to detect alphabetical characters and +/- in timestamp literals
export const TIME_ZONE_RE: RegExp = /:.*?[a-zA-Z+\-]/;

export function buildVarMap (args: Expression[]): StarMapExpr | VarMapExpr {
  if (args.length < 1) {
    throw new Error('buildVarMap only accepts an expression list with at least one expression');
  }

  if (args.length === 1 && args[0].isStar) {
    return new StarMapExpr({ this: args[0] });
  }

  const keys: Expression[] = [];
  const values: Expression[] = [];
  for (let i = 0; i < args.length; i += 2) {
    keys.push(args[i]);
    values.push(args[i + 1]);
  }

  return new VarMapExpr({
    keys: array(keys, { copy: false }),
    values: array(values, { copy: false }),
  });
}

export function buildLike (args: Expression[]): EscapeExpr | LikeExpr {
  if (args.length < 2) {
    throw new Error('buildLike only accept expression lists with at least 2 expressions');
  }
  const like = new LikeExpr({
    this: seqGet(args, 1)!,
    expression: seqGet(args, 0)!,
  });
  return 2 < args.length
    ? new EscapeExpr({
      this: like,
      expression: seqGet(args, 2)!,
    })
    : like;
}

export function binaryRangeParser (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  exprType: new (args: any) => Expression,
  options: { reverseArgs?: boolean } = {},
): (parser: Parser, thisExpr: Expression | undefined) => Expression | undefined {
  const { reverseArgs = false } = options;

  return function parseBinaryRange (
    parser: Parser,
    thisExpr: Expression | undefined,
  ): Expression | undefined {
    let expression = parser._parseBitwise();
    let thisArg = thisExpr;

    if (reverseArgs) {
      [thisArg, expression] = [expression, thisArg];
    }

    return parser._parseEscape(
      parser.expression(exprType, {
        this: thisArg,
        expression,
      }),
    );
  };
}

export function buildLogarithm (args: Expression[], dialect: Dialect): LogExpr | LnExpr {
  if (args.length < 1) {
    throw new Error('buildAlgorithm only accepts an expression list with at least one expression');
  }
  // Default argument order is base, expression
  let thisArg = seqGet(args, 0);
  let expression = seqGet(args, 1);

  if (thisArg && expression) {
    if (!dialect.LOG_BASE_FIRST) {
      [thisArg, expression] = [expression, thisArg];
    }
    return new LogExpr({
      this: thisArg,
      expression,
    });
  }

  // Check if dialect's parser class has LOG_DEFAULTS_TO_LN property
  const parserClass = dialect.parserClass;
  const logDefaultsToLn = parserClass?.LOG_DEFAULTS_TO_LN ?? false;

  return logDefaultsToLn
    ? new LnExpr({ this: thisArg })
    : new LogExpr({ this: thisArg! });
}

export function buildHex (args: Expression[], dialect: Dialect): HexExpr | LowerHexExpr {
  if (args.length < 1) {
    throw new Error('buildHex only accepts an expression list with at least one expression');
  }
  const arg = seqGet(args, 0)!;
  return dialect.HEX_LOWERCASE
    ? new LowerHexExpr({ this: arg })
    : new HexExpr({ this: arg });
}

export function buildLower (args: Expression[]): LowerExpr | LowerHexExpr {
  if (args.length < 1) {
    throw new Error('buildLower only accepts an expression list with at least one expression');
  }
  // LOWER(HEX(..)) can be simplified to LowerHex to simplify its transpilation
  const arg = seqGet(args, 0)!;
  return arg instanceof HexExpr
    ? new LowerHexExpr({ this: arg.this })
    : new LowerExpr({ this: arg });
}

export function buildUpper (args: Expression[]): UpperExpr | HexExpr {
  if (args.length < 1) {
    throw new Error('buildUpper only accepts an expression list with at least one expression');
  }
  // UPPER(HEX(..)) can be simplified to Hex to simplify its transpilation
  const arg = seqGet(args, 0)!;
  return arg instanceof LowerHexExpr
    ? new HexExpr({ this: arg.this })
    : new UpperExpr({ this: arg });
}

export function buildExtractJsonWithPath<E extends Expression> (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  exprType: new (args: any) => E,
): (args: Expression[], dialect: Dialect) => E {
  return function builder (args: Expression[], dialect: Dialect): E {
    if (args.length < 2) {
      throw new Error('buildExtractJsonWithPath only accepts an expression list with at least two expressions');
    }
    const expression = new exprType({
      this: seqGet(args, 0)!,
      expression: dialect.toJsonPath(seqGet(args, 1)),
    });

    if (2 < args.length && expression instanceof JSONExtractExpr) {
      expression.setArgKey('expressions', args.slice(2));
    }

    if (expression instanceof JSONExtractScalarExpr) {
      expression.setArgKey('scalarOnly', dialect.JSON_EXTRACT_SCALAR_SCALAR_ONLY);
    }

    return expression;
  };
}

export function buildMod (args: Expression[]): ModExpr {
  if (args.length < 2) {
    throw new Error('buildMod only accepts an expression list with at least two expressions');
  }
  let thisArg = seqGet(args, 0)!;
  let expression = seqGet(args, 1)!;

  // Wrap the operands if they are binary nodes, e.g. MOD(a + 1, 7) -> (a + 1) % 7
  thisArg = thisArg instanceof BinaryExpr ? new ParenExpr({ this: thisArg }) : thisArg;
  expression = expression instanceof BinaryExpr ? new ParenExpr({ this: expression }) : expression;

  return new ModExpr({
    this: thisArg,
    expression,
  });
}

export function buildPad (args: Expression[], options: { isLeft?: boolean } = {}): PadExpr {
  if (args.length < 2) {
    throw new Error('buildPad only accepts an expression list with at least two expressions');
  }
  const { isLeft = true } = options;

  return new PadExpr({
    this: seqGet(args, 0)!,
    expression: seqGet(args, 1)!,
    fillPattern: seqGet(args, 2),
    isLeft,
  });
}

export function buildArrayConstructor<E extends Expression> (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  exprClass: new (args: any) => E,
  args: Expression[],
  bracketKind: TokenType,
  dialect: Dialect,
): E {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const arrayExpr = new exprClass({ expressions: args } as any);

  if (arrayExpr instanceof ArrayExpr && dialect.HAS_DISTINCT_ARRAY_CONSTRUCTORS) {
    arrayExpr.setArgKey('bracketNotation', bracketKind === TokenType.L_BRACKET);
  }

  return arrayExpr;
}

export function buildConvertTimezone (
  args: Expression[],
  options: { defaultSourceTz?: string } = {},
): ConvertTimezoneExpr {
  if (args.length < 2) {
    throw new Error('buildConvertTimezone only accepts an expression list with at least two expressions');
  }
  const { defaultSourceTz } = options;

  if (args.length === 2) {
    const sourceTz = defaultSourceTz ? LiteralExpr.string(defaultSourceTz) : undefined;
    return new ConvertTimezoneExpr({
      sourceTz,
      targetTz: seqGet(args, 0)!,
      timestamp: seqGet(args, 1)!,
    });
  }

  return ConvertTimezoneExpr.fromArgList(args);
}

export function buildTrim (
  args: Expression[],
  options: { isLeft?: boolean;
    reverseArgs?: boolean; } = {},
): TrimExpr {
  if (args.length < 1) {
    throw new Error('buildTrim only accepts an expression list with at least one expression');
  }
  const {
    isLeft = true, reverseArgs = false,
  } = options;

  let thisArg = seqGet(args, 0)!;
  let expression = seqGet(args, 1);

  if (expression && reverseArgs) {
    [thisArg, expression] = [expression, thisArg];
  }

  return new TrimExpr({
    this: thisArg,
    expression,
    position: isLeft ? TrimPosition.LEADING : TrimPosition.TRAILING,
  });
}

export function buildCoalesce (
  args: Expression[],
  options: {
    isNvl?: boolean;
    isNull?: boolean;
  } = {},
): CoalesceExpr {
  if (args.length < 1) {
    throw new Error('buildCoalesce only accepts an expression list with at least one expression');
  }
  const {
    isNvl, isNull,
  } = options;

  return new CoalesceExpr({
    this: seqGet(args, 0)!,
    expressions: args.slice(1),
    isNvl,
    isNull,
  });
}

export function buildLocateStrposition (args: Expression[]): StrPositionExpr {
  if (args.length < 2) {
    throw new Error('buildLocateStrposition only accepts an expression list with at least two expressions');
  }

  return new StrPositionExpr({
    this: seqGet(args, 1)!,
    substr: seqGet(args, 0)!,
    position: seqGet(args, 2),
  });
}

export function buildArrayAppend (args: Expression[], dialect: Dialect): ArrayAppendExpr {
  if (args.length < 2) {
    throw new Error('buildArrayAppend only accepts an expression list with at least two expressions');
  }

  /**
   * Builds ArrayAppend with NULL propagation semantics based on the dialect configuration.
   *
   * Some dialects (Databricks, Spark, Snowflake) return NULL when the input array is NULL.
   * Others (DuckDB, PostgreSQL) create a new single-element array instead.
   */
  return new ArrayAppendExpr({
    this: seqGet(args, 0)!,
    expression: seqGet(args, 1)!,
    nullPropagation: dialect.ARRAY_FUNCS_PROPAGATES_NULLS,
  });
}

export function buildArrayPrepend (args: Expression[], dialect: Dialect): ArrayPrependExpr {
  if (args.length < 2) {
    throw new Error('buildArrayPrepend only accepts an expression list with at least two expressions');
  }

  /**
   * Builds ArrayPrepend with NULL propagation semantics based on the dialect configuration.
   *
   * Some dialects (Databricks, Spark, Snowflake) return NULL when the input array is NULL.
   * Others (DuckDB, PostgreSQL) create a new single-element array instead.
   */
  return new ArrayPrependExpr({
    this: seqGet(args, 0)!,
    expression: seqGet(args, 1)!,
    nullPropagation: dialect.ARRAY_FUNCS_PROPAGATES_NULLS,
  });
}

export function buildArrayConcat (args: Expression[], dialect: Dialect): ArrayConcatExpr {
  if (args.length < 1) {
    throw new Error('buildArrayConcat only accepts an expression list with at least one expression');
  }

  /**
   * Builds ArrayConcat with NULL propagation semantics based on the dialect configuration.
   *
   * Some dialects (Redshift, Snowflake) return NULL when any input array is NULL.
   * Others (DuckDB, PostgreSQL) skip NULL arrays and continue concatenation.
   */
  return new ArrayConcatExpr({
    this: seqGet(args, 0)!,
    expressions: args.slice(1),
    nullPropagation: dialect.ARRAY_FUNCS_PROPAGATES_NULLS,
  });
}

export function buildArrayRemove (args: Expression[], dialect: Dialect): ArrayRemoveExpr {
  if (args.length < 2) {
    throw new Error('buildArrayRemove only accepts an expression list with at least two expressions');
  }

  /**
   * Builds ArrayRemove with NULL propagation semantics based on the dialect configuration.
   *
   * Some dialects (Snowflake) return NULL when the removal value is NULL.
   * Others (DuckDB) may return empty array due to NULL comparison semantics.
   */
  return new ArrayRemoveExpr({
    this: seqGet(args, 0)!,
    expression: seqGet(args, 1)!,
    nullPropagation: dialect.ARRAY_FUNCS_PROPAGATES_NULLS,
  });
}

export interface ParseOptions<IntoT extends Expression = Expression> {
  read?: DialectType;
  dialect?: DialectType;
  errorLevel?: ErrorLevel;
  errorMessageContext?: number;
  maxErrors?: number;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  into?: string | (new (args: any) => IntoT);
  [key: string]: unknown;
}

/**
 * Parser consumes a list of tokens produced by the Tokenizer and produces a parsed syntax tree.
 *
 * Args:
 *   errorLevel: The desired error level. Default: ErrorLevel.IMMEDIATE
 *   errorMessageContext: The amount of context to capture from a query string when displaying
 *     the error message (in number of characters). Default: 100
 *   maxErrors: Maximum number of error messages to include in a raised ParseError.
 *     This is only relevant if error_level is ErrorLevel.RAISE. Default: 3
 */
export class Parser {
  // Cached tries for SHOW and SET parsers (metaclass pattern)
  private static _showTrie?: unknown; // TODO: Replace with proper Trie type
  private static _setTrie?: unknown;

  static get SHOW_TRIE (): unknown {
    if (!this._showTrie) {
      // TODO: Implement newTrie function
      // this._showTrie = newTrie(Object.keys(this.SHOW_PARSERS).map(key => key.split(' ')));
      throw new Error('SHOW_TRIE not implemented');
    }
    return this._showTrie;
  }

  static get SET_TRIE (): unknown {
    if (!this._setTrie) {
      // TODO: Implement newTrie function
      // this._setTrie = newTrie(Object.keys(this.SET_PARSERS).map(key => key.split(' ')));
      throw new Error('SET_TRIE not implemented');
    }
    return this._setTrie;
  }

  // Static parser dictionaries (to be defined)
  static SHOW_PARSERS: Record<string, unknown> = {};
  static SET_PARSERS: Record<string, unknown> = {};

  // Function name to builder mapping
  static FUNCTIONS: Record<string, (args: Expression[], dialect: Dialect) => Expression> = {
    // TODO: Spread all fromArgList functions from FUNCTION_BY_NAME

    // Coalesce variants
    COALESCE: (args, dialect) => buildCoalesce(args),
    IFNULL: (args, dialect) => buildCoalesce(args),
    NVL: (args, dialect) => buildCoalesce(args),

    // Array functions
    ARRAY: (args, dialect) => new ArrayExpr({ expressions: args }),

    ARRAYAGG: (args, dialect) => new ArrayAggExpr({
      this: seqGet(args, 0),
      nullsExcluded: dialect.ARRAY_AGG_INCLUDES_NULLS === undefined ? undefined : undefined,
    }),

    ARRAY_AGG: (args, dialect) => new ArrayAggExpr({
      this: seqGet(args, 0),
      nullsExcluded: dialect.ARRAY_AGG_INCLUDES_NULLS === undefined ? undefined : undefined,
    }),

    ARRAY_APPEND: buildArrayAppend,
    ARRAY_CAT: buildArrayConcat,
    ARRAY_CONCAT: buildArrayConcat,
    ARRAY_PREPEND: buildArrayPrepend,
    ARRAY_REMOVE: buildArrayRemove,

    // Aggregate functions
    COUNT: (args, dialect) => new CountExpr({
      this: seqGet(args, 0),
      expressions: args.slice(1),
      bigInt: true,
    }),

    // String functions
    CONCAT: (args, dialect) => new ConcatExpr({
      expressions: args,
      safe: !dialect.STRICT_STRING_CONCAT,
      coalesce: dialect.CONCAT_COALESCE,
    }),

    CONCAT_WS: (args, dialect) => new ConcatWsExpr({
      expressions: args,
      safe: !dialect.STRICT_STRING_CONCAT,
      coalesce: dialect.CONCAT_COALESCE,
    }),

    // Conversion functions
    CONVERT_TIMEZONE: (args, dialect) => buildConvertTimezone(args),

    DATE_TO_DATE_STR: (args, dialect) => new CastExpr({
      this: seqGet(args, 0),
      to: new DataTypeExpr({ this: DataTypeType.TEXT }),
    }),

    TIME_TO_TIME_STR: (args, dialect) => new CastExpr({
      this: seqGet(args, 0),
      to: new DataTypeExpr({ this: DataTypeType.TEXT }),
    }),

    // Generator functions
    GENERATE_DATE_ARRAY: (args, dialect) => new GenerateDateArrayExpr({
      start: seqGet(args, 0),
      end: seqGet(args, 1),
      step: seqGet(args, 2) || new IntervalExpr({
        this: LiteralExpr.string('1'),
        unit: varExpr('DAY'),
      }),
    }),

    GENERATE_UUID: (args, dialect) => new UuidExpr({
      isString: dialect.UUID_IS_STRING_TYPE || undefined,
    }),

    // Pattern matching
    GLOB: (args, dialect) => new GlobExpr({
      this: seqGet(args, 1),
      expression: seqGet(args, 0),
    }),

    LIKE: (args, dialect) => buildLike(args),

    // Comparison functions
    GREATEST: (args, dialect) => new GreatestExpr({
      this: seqGet(args, 0),
      expressions: args.slice(1),
      ignoreNulls: dialect.LEAST_GREATEST_IGNORES_NULLS,
    }),

    LEAST: (args, dialect) => new LeastExpr({
      this: seqGet(args, 0),
      expressions: args.slice(1),
      ignoreNulls: dialect.LEAST_GREATEST_IGNORES_NULLS,
    }),

    // Encoding functions
    HEX: (args, dialect) => buildHex(args, dialect),
    TO_HEX: (args, dialect) => buildHex(args, dialect),

    // JSON functions
    JSON_EXTRACT: buildExtractJsonWithPath(JSONExtractExpr),
    JSON_EXTRACT_SCALAR: buildExtractJsonWithPath(JSONExtractScalarExpr),
    JSON_EXTRACT_PATH_TEXT: buildExtractJsonWithPath(JSONExtractScalarExpr),

    JSON_KEYS: (args, dialect) => new JSONKeysExpr({
      this: seqGet(args, 0),
      expression: dialect.toJsonPath(seqGet(args, 1)),
    }),

    // Math functions
    LOG: (args, dialect) => buildLogarithm(args, dialect),
    LOG2: (args, dialect) => new LogExpr({
      this: LiteralExpr.number(2),
      expression: seqGet(args, 0),
    }),
    LOG10: (args, dialect) => new LogExpr({
      this: LiteralExpr.number(10),
      expression: seqGet(args, 0),
    }),
    MOD: (args, dialect) => buildMod(args),

    // String manipulation
    LOWER: (args, dialect) => buildLower(args),
    UPPER: (args, dialect) => buildUpper(args),

    LPAD: (args, dialect) => buildPad(args),
    LEFTPAD: (args, dialect) => buildPad(args),
    RPAD: (args, dialect) => buildPad(args, { isLeft: false }),
    RIGHTPAD: (args, dialect) => buildPad(args, { isLeft: false }),

    LTRIM: (args, dialect) => buildTrim(args),
    RTRIM: (args, dialect) => buildTrim(args, { isLeft: false }),

    // String search
    STRPOS: (args, dialect) => StrPositionExpr.fromArgList(args),
    INSTR: (args, dialect) => StrPositionExpr.fromArgList(args),
    CHARINDEX: (args, dialect) => buildLocateStrposition(args),
    LOCATE: (args, dialect) => buildLocateStrposition(args),

    // Scope resolution
    SCOPE_RESOLUTION: (args, dialect) => args.length !== 2
      ? new ScopeResolutionExpr({ expression: seqGet(args, 0) })
      : new ScopeResolutionExpr({
        this: seqGet(args, 0),
        expression: seqGet(args, 1),
      }),

    // String operations
    TS_OR_DS_TO_DATE_STR: (args, dialect) => new SubstringExpr({
      this: new CastExpr({
        this: seqGet(args, 0),
        to: new DataTypeExpr({ this: DataTypeType.TEXT }),
      }),
      start: LiteralExpr.number(1),
      length: LiteralExpr.number(10),
    }),

    // Array operations
    UNNEST: (args, dialect) => new UnnestExpr({
      expressions: ensureList(seqGet(args, 0)),
    }),

    // UUID
    UUID: (args, dialect) => new UuidExpr({
      isString: dialect.UUID_IS_STRING_TYPE || undefined,
    }),

    // Map operations
    VAR_MAP: (args, dialect) => buildVarMap(args),
  };

  // Instance properties
  protected sql: string;
  protected dialect: Dialect;
  protected errorLevel: ErrorLevel;
  protected errorMessageContext: number;
  protected maxErrors: number;
  protected errors: ParseError[];
  protected tokens: Token[];
  protected index: number;

  constructor (options?: ParseOptions) {
    const opts = options ?? {};
    this.sql = '';
    this.dialect = Dialect.getOrRaise(opts.dialect);
    this.errorLevel = opts.errorLevel ?? ErrorLevel.IMMEDIATE;
    this.errorMessageContext = opts.errorMessageContext ?? 100;
    this.maxErrors = opts.maxErrors ?? 3;
    this.errors = [];
    this.tokens = [];
    this.index = 0;
  }

  // Main parse method with generic type parameter
  parse<IntoT extends Expression> (sql: string | Token[], opts?: ParseOptions<IntoT>): Expression[] {
    // TODO: Implement parsing logic
    throw new Error('Not implemented');
  }

  // Helper methods
  protected _curr (): Token | undefined {
    return this.tokens[this.index];
  }

  protected _prev (): Token | undefined {
    return this.tokens[this.index - 1];
  }

  protected _next (): Token | undefined {
    return this.tokens[this.index + 1];
  }

  protected _advance (): void {
    this.index++;
  }

  protected _retreat (): void {
    this.index--;
  }

  protected _match (tokenType: TokenType): boolean {
    if (this._curr()?.tokenType === tokenType) {
      this._advance();
      return true;
    }
    return false;
  }
}
