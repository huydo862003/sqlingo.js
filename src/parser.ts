// https://github.com/tobymao/sqlglot/blob/main/sqlglot/parser.py

import type { Expression } from './expressions';
import {
  AddExpr,
  AllExpr,
  AndExpr,
  AnyExpr,
  array,
  ArrayAggExpr,
  ArrayAppendExpr,
  ArrayConcatExpr,
  ArrayExpr,
  ArrayPrependExpr,
  ArrayRemoveExpr,
  BinaryExpr,
  BitwiseAndExpr,
  BitwiseOrExpr,
  BitwiseXorExpr,
  CastExpr,
  CoalesceExpr,
  CollateExpr,
  ConcatExpr,
  ConcatWsExpr,
  ConvertTimezoneExpr,
  CountExpr,
  CurrentDateExpr,
  CurrentDatetimeExpr,
  CurrentRoleExpr,
  CurrentTimeExpr,
  CurrentTimestampExpr,
  CurrentUserExpr,
  DataTypeExpr,
  DataTypeType,
  DistanceExpr,
  DivExpr,
  EQExpr,
  EscapeExpr,
  ExistsExpr,
  FUNCTION_BY_NAME,
  GenerateDateArrayExpr,
  GlobExpr,
  GreatestExpr,
  GTEExpr,
  GTExpr,
  HexExpr,
  IntDivExpr,
  IntervalExpr,
  JSONExtractExpr,
  JSONExtractScalarExpr,
  JSONKeysExpr,
  LeastExpr,
  LikeExpr,
  ListExpr,
  LiteralExpr,
  LnExpr,
  LocaltimeExpr,
  LocaltimestampExpr,
  LogExpr,
  LowerExpr,
  LowerHexExpr,
  LTEExpr,
  LTExpr,
  ModExpr,
  MulExpr,
  NEQExpr,
  NullSafeEQExpr,
  OrExpr,
  PadExpr,
  ParenExpr,
  PropertyEQExpr,
  ScopeResolutionExpr,
  StarMapExpr,
  StrPositionExpr,
  SubExpr,
  SubstringExpr,
  TrimExpr,
  TrimPosition,
  UnnestExpr,
  UpperExpr,
  UuidExpr,
  varExpr,
  VarMapExpr,
} from './expressions';
import {
  ensureList, seqGet,
} from './helper';
import {
  Dialect, type DialectType,
} from './dialects/dialect';
import type { ParseError } from './errors';
import { ErrorLevel } from './errors';
import type { Token } from './tokens';
import {
  Tokenizer, TokenType,
} from './tokens';
import {
  newTrie, type TrieNode,
} from './trie';

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

  private static _showTrie?: TrieNode;
  static get SHOW_TRIE (): TrieNode {
    if (!this._showTrie) {
      this._showTrie = newTrie(
        Object.keys(this.SHOW_PARSERS).map((key) => key.split(' ')),
      );
    }
    return this._showTrie;
  }

  private static _setTrie?: TrieNode;
  static get SET_TRIE (): TrieNode {
    if (!this._setTrie) {
      this._setTrie = newTrie(
        Object.keys(this.SET_PARSERS).map((key) => key.split(' ')),
      );
    }
    return this._setTrie;
  }

  // Function name to builder mapping
  static FUNCTIONS: Record<string, (args: Expression[], dialect: Dialect) => Expression> = {
    // Spread all fromArgList functions from FUNCTION_BY_NAME
    ...Object.fromEntries(
      Array.from(FUNCTION_BY_NAME.entries()).map(([name, func]) => [name, (args: Expression[], _dialect: Dialect) => func.fromArgList(args)]),
    ),

    // Coalesce variants
    ...Object.fromEntries(
      [
        'COALESCE',
        'IFNULL',
        'NVL',
      ].map((name) => [name, (args: Expression[], _dialect: Dialect) => buildCoalesce(args)]),
    ),

    // Array functions
    ARRAY: (args, _dialect) => new ArrayExpr({ expressions: args }),

    ARRAYAGG: (args, dialect) => new ArrayAggExpr({
      this: seqGet(args, 0)!,
      nullsExcluded: dialect.ARRAY_AGG_INCLUDES_NULLS === undefined ? true : undefined,
    }),

    ARRAY_AGG: (args, dialect) => new ArrayAggExpr({
      this: seqGet(args, 0)!,
      nullsExcluded: dialect.ARRAY_AGG_INCLUDES_NULLS === undefined ? true : undefined,
    }),

    ARRAY_APPEND: buildArrayAppend,
    ARRAY_CAT: buildArrayConcat,
    ARRAY_CONCAT: buildArrayConcat,
    ARRAY_PREPEND: buildArrayPrepend,
    ARRAY_REMOVE: buildArrayRemove,

    // Aggregate functions
    COUNT: (args, _dialect) => new CountExpr({
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
    CONVERT_TIMEZONE: (args, _dialect) => buildConvertTimezone(args),

    DATE_TO_DATE_STR: (args, _dialect) => new CastExpr({
      this: seqGet(args, 0)!,
      to: new DataTypeExpr({ this: DataTypeType.TEXT }),
    }),

    TIME_TO_TIME_STR: (args, _dialect) => new CastExpr({
      this: seqGet(args, 0)!,
      to: new DataTypeExpr({ this: DataTypeType.TEXT }),
    }),

    // Generator functions
    GENERATE_DATE_ARRAY: (args, _dialect) => new GenerateDateArrayExpr({
      start: seqGet(args, 0)!,
      end: seqGet(args, 1)!,
      step: seqGet(args, 2) || new IntervalExpr({
        this: LiteralExpr.string('1'),
        unit: varExpr('DAY'),
      }),
    }),

    GENERATE_UUID: (args, dialect) => new UuidExpr({
      isString: dialect.UUID_IS_STRING_TYPE || undefined,
    }),

    // Pattern matching
    GLOB: (args, _dialect) => new GlobExpr({
      this: seqGet(args, 1)!,
      expression: seqGet(args, 0)!,
    }),

    LIKE: (args, _dialect) => buildLike(args),

    // Comparison functions
    GREATEST: (args, dialect) => new GreatestExpr({
      this: seqGet(args, 0)!,
      expressions: args.slice(1),
      ignoreNulls: dialect.LEAST_GREATEST_IGNORES_NULLS,
    }),

    LEAST: (args, dialect) => new LeastExpr({
      this: seqGet(args, 0)!,
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
      this: seqGet(args, 0)!,
      expression: dialect.toJsonPath(seqGet(args, 1)),
    }),

    // Math functions
    LOG: (args, dialect) => buildLogarithm(args, dialect),
    LOG2: (args, _dialect) => new LogExpr({
      this: LiteralExpr.number(2),
      expression: seqGet(args, 0),
    }),
    LOG10: (args, _dialect) => new LogExpr({
      this: LiteralExpr.number(10),
      expression: seqGet(args, 0),
    }),
    MOD: (args, _dialect) => buildMod(args),

    // String manipulation
    LOWER: (args, _dialect) => buildLower(args),
    UPPER: (args, _dialect) => buildUpper(args),

    LPAD: (args, _dialect) => buildPad(args),
    LEFTPAD: (args, _dialect) => buildPad(args),
    RPAD: (args, _dialect) => buildPad(args, { isLeft: false }),
    RIGHTPAD: (args, _dialect) => buildPad(args, { isLeft: false }),

    LTRIM: (args, _dialect) => buildTrim(args),
    RTRIM: (args, _dialect) => buildTrim(args, { isLeft: false }),

    // String search
    STRPOS: (args, _dialect) => StrPositionExpr.fromArgList(args),
    INSTR: (args, _dialect) => StrPositionExpr.fromArgList(args),
    CHARINDEX: (args, _dialect) => buildLocateStrposition(args),
    LOCATE: (args, _dialect) => buildLocateStrposition(args),

    // Scope resolution
    SCOPE_RESOLUTION: (args, _dialect) => args.length !== 2
      ? new ScopeResolutionExpr({ expression: seqGet(args, 0)! })
      : new ScopeResolutionExpr({
        this: seqGet(args, 0),
        expression: seqGet(args, 1)!,
      }),

    // String operations
    TS_OR_DS_TO_DATE_STR: (args, _dialect) => new SubstringExpr({
      this: new CastExpr({
        this: seqGet(args, 0)!,
        to: new DataTypeExpr({ this: DataTypeType.TEXT }),
      }),
      start: LiteralExpr.number(1),
      length: LiteralExpr.number(10),
    }),

    // Array operations
    UNNEST: (args, _dialect) => new UnnestExpr({
      expressions: ensureList(seqGet(args, 0)),
    }),

    // UUID
    UUID: (args, dialect) => new UuidExpr({
      isString: dialect.UUID_IS_STRING_TYPE || undefined,
    }),

    // Map operations
    VAR_MAP: (args, _dialect) => buildVarMap(args),
  };

  // Function expressions that don't require parentheses
  static NO_PAREN_FUNCTIONS = {
    [TokenType.CURRENT_DATE]: CurrentDateExpr,
    [TokenType.CURRENT_DATETIME]: CurrentDateExpr,
    [TokenType.CURRENT_TIME]: CurrentTimeExpr,
    [TokenType.CURRENT_TIMESTAMP]: CurrentTimestampExpr,
    [TokenType.CURRENT_USER]: CurrentUserExpr,
    [TokenType.LOCALTIME]: LocaltimeExpr,
    [TokenType.LOCALTIMESTAMP]: LocaltimestampExpr,
    [TokenType.CURRENT_ROLE]: CurrentRoleExpr,
  };

  static STRUCT_TYPE_TOKENS = new Set([
    TokenType.FILE,
    TokenType.NESTED,
    TokenType.OBJECT,
    TokenType.STRUCT,
    TokenType.UNION,
  ]);

  static NESTED_TYPE_TOKENS = new Set([
    TokenType.ARRAY,
    TokenType.LIST,
    TokenType.LOWCARDINALITY,
    TokenType.MAP,
    TokenType.NULLABLE,
    TokenType.RANGE,
    ...Parser.STRUCT_TYPE_TOKENS,
  ]);

  static ENUM_TYPE_TOKENS = new Set([
    TokenType.DYNAMIC,
    TokenType.ENUM,
    TokenType.ENUM8,
    TokenType.ENUM16,
  ]);

  static AGGREGATE_TYPE_TOKENS = new Set([TokenType.AGGREGATEFUNCTION, TokenType.SIMPLEAGGREGATEFUNCTION]);

  static TYPE_TOKENS = new Set([
    TokenType.BIT,
    TokenType.BOOLEAN,
    TokenType.TINYINT,
    TokenType.UTINYINT,
    TokenType.SMALLINT,
    TokenType.USMALLINT,
    TokenType.INT,
    TokenType.UINT,
    TokenType.BIGINT,
    TokenType.UBIGINT,
    TokenType.BIGNUM,
    TokenType.INT128,
    TokenType.UINT128,
    TokenType.INT256,
    TokenType.UINT256,
    TokenType.MEDIUMINT,
    TokenType.UMEDIUMINT,
    TokenType.FIXEDSTRING,
    TokenType.FLOAT,
    TokenType.DOUBLE,
    TokenType.UDOUBLE,
    TokenType.CHAR,
    TokenType.NCHAR,
    TokenType.VARCHAR,
    TokenType.NVARCHAR,
    TokenType.BPCHAR,
    TokenType.TEXT,
    TokenType.MEDIUMTEXT,
    TokenType.LONGTEXT,
    TokenType.BLOB,
    TokenType.MEDIUMBLOB,
    TokenType.LONGBLOB,
    TokenType.BINARY,
    TokenType.VARBINARY,
    TokenType.JSON,
    TokenType.JSONB,
    TokenType.INTERVAL,
    TokenType.TINYBLOB,
    TokenType.TINYTEXT,
    TokenType.TIME,
    TokenType.TIMETZ,
    TokenType.TIME_NS,
    TokenType.TIMESTAMP,
    TokenType.TIMESTAMP_S,
    TokenType.TIMESTAMP_MS,
    TokenType.TIMESTAMP_NS,
    TokenType.TIMESTAMPTZ,
    TokenType.TIMESTAMPLTZ,
    TokenType.TIMESTAMPNTZ,
    TokenType.DATETIME,
    TokenType.DATETIME2,
    TokenType.DATETIME64,
    TokenType.SMALLDATETIME,
    TokenType.DATE,
    TokenType.DATE32,
    TokenType.INT4RANGE,
    TokenType.INT4MULTIRANGE,
    TokenType.INT8RANGE,
    TokenType.INT8MULTIRANGE,
    TokenType.NUMRANGE,
    TokenType.NUMMULTIRANGE,
    TokenType.TSRANGE,
    TokenType.TSMULTIRANGE,
    TokenType.TSTZRANGE,
    TokenType.TSTZMULTIRANGE,
    TokenType.DATERANGE,
    TokenType.DATEMULTIRANGE,
    TokenType.DECIMAL,
    TokenType.DECIMAL32,
    TokenType.DECIMAL64,
    TokenType.DECIMAL128,
    TokenType.DECIMAL256,
    TokenType.DECFLOAT,
    TokenType.UDECIMAL,
    TokenType.BIGDECIMAL,
    TokenType.UUID,
    TokenType.GEOGRAPHY,
    TokenType.GEOGRAPHYPOINT,
    TokenType.GEOMETRY,
    TokenType.POINT,
    TokenType.RING,
    TokenType.LINESTRING,
    TokenType.MULTILINESTRING,
    TokenType.POLYGON,
    TokenType.MULTIPOLYGON,
    TokenType.HLLSKETCH,
    TokenType.HSTORE,
    TokenType.PSEUDO_TYPE,
    TokenType.SUPER,
    TokenType.SERIAL,
    TokenType.SMALLSERIAL,
    TokenType.BIGSERIAL,
    TokenType.XML,
    TokenType.YEAR,
    TokenType.USERDEFINED,
    TokenType.MONEY,
    TokenType.SMALLMONEY,
    TokenType.ROWVERSION,
    TokenType.IMAGE,
    TokenType.VARIANT,
    TokenType.VECTOR,
    TokenType.VOID,
    TokenType.OBJECT,
    TokenType.OBJECT_IDENTIFIER,
    TokenType.INET,
    TokenType.IPADDRESS,
    TokenType.IPPREFIX,
    TokenType.IPV4,
    TokenType.IPV6,
    TokenType.UNKNOWN,
    TokenType.NOTHING,
    TokenType.NULL,
    TokenType.NAME,
    TokenType.TDIGEST,
    TokenType.DYNAMIC,
    ...Parser.ENUM_TYPE_TOKENS,
    ...Parser.NESTED_TYPE_TOKENS,
    ...Parser.AGGREGATE_TYPE_TOKENS,
  ]);

  static SIGNED_TO_UNSIGNED_TYPE_TOKEN = {
    [TokenType.BIGINT]: TokenType.UBIGINT,
    [TokenType.INT]: TokenType.UINT,
    [TokenType.MEDIUMINT]: TokenType.UMEDIUMINT,
    [TokenType.SMALLINT]: TokenType.USMALLINT,
    [TokenType.TINYINT]: TokenType.UTINYINT,
    [TokenType.DECIMAL]: TokenType.UDECIMAL,
    [TokenType.DOUBLE]: TokenType.UDOUBLE,
  };

  static SUBQUERY_PREDICATES = {
    [TokenType.ANY]: AnyExpr,
    [TokenType.ALL]: AllExpr,
    [TokenType.EXISTS]: ExistsExpr,
    [TokenType.SOME]: AnyExpr,
  };

  static RESERVED_TOKENS = new Set(
    [...Object.values(Tokenizer.SINGLE_TOKENS), TokenType.SELECT].filter((t) => t !== TokenType.IDENTIFIER),
  );

  static DB_CREATABLES = new Set([
    TokenType.DATABASE,
    TokenType.DICTIONARY,
    TokenType.FILE_FORMAT,
    TokenType.MODEL,
    TokenType.NAMESPACE,
    TokenType.SCHEMA,
    TokenType.SEMANTIC_VIEW,
    TokenType.SEQUENCE,
    TokenType.SINK,
    TokenType.SOURCE,
    TokenType.STAGE,
    TokenType.STORAGE_INTEGRATION,
    TokenType.STREAMLIT,
    TokenType.TABLE,
    TokenType.TAG,
    TokenType.VIEW,
    TokenType.WAREHOUSE,
  ]);

  static CREATABLES = new Set([
    TokenType.COLUMN,
    TokenType.CONSTRAINT,
    TokenType.FOREIGN_KEY,
    TokenType.FUNCTION,
    TokenType.INDEX,
    TokenType.PROCEDURE,
    ...Parser.DB_CREATABLES,
  ]);

  static ALTERABLES = new Set([
    TokenType.INDEX,
    TokenType.TABLE,
    TokenType.VIEW,
    TokenType.SESSION,
  ]);

  static ID_VAR_TOKENS = (() => {
    const tokens = new Set([
      TokenType.ALL,
      TokenType.ANALYZE,
      TokenType.ATTACH,
      TokenType.VAR,
      TokenType.ANTI,
      TokenType.APPLY,
      TokenType.ASC,
      TokenType.ASOF,
      TokenType.AUTO_INCREMENT,
      TokenType.BEGIN,
      TokenType.BPCHAR,
      TokenType.CACHE,
      TokenType.CASE,
      TokenType.COLLATE,
      TokenType.COMMAND,
      TokenType.COMMENT,
      TokenType.COMMIT,
      TokenType.CONSTRAINT,
      TokenType.COPY,
      TokenType.CUBE,
      TokenType.CURRENT_SCHEMA,
      TokenType.DEFAULT,
      TokenType.DELETE,
      TokenType.DESC,
      TokenType.DESCRIBE,
      TokenType.DETACH,
      TokenType.DICTIONARY,
      TokenType.DIV,
      TokenType.END,
      TokenType.EXECUTE,
      TokenType.EXPORT,
      TokenType.ESCAPE,
      TokenType.FALSE,
      TokenType.FIRST,
      TokenType.FILTER,
      TokenType.FINAL,
      TokenType.FORMAT,
      TokenType.FULL,
      TokenType.GET,
      TokenType.IDENTIFIER,
      TokenType.INOUT,
      TokenType.IS,
      TokenType.ISNULL,
      TokenType.INTERVAL,
      TokenType.KEEP,
      TokenType.KILL,
      TokenType.LEFT,
      TokenType.LIMIT,
      TokenType.LOAD,
      TokenType.LOCK,
      TokenType.MATCH,
      TokenType.MERGE,
      TokenType.NATURAL,
      TokenType.NEXT,
      TokenType.OFFSET,
      TokenType.OPERATOR,
      TokenType.ORDINALITY,
      TokenType.OVER,
      TokenType.OVERLAPS,
      TokenType.OVERWRITE,
      TokenType.PARTITION,
      TokenType.PERCENT,
      TokenType.PIVOT,
      TokenType.PRAGMA,
      TokenType.PUT,
      TokenType.RANGE,
      TokenType.RECURSIVE,
      TokenType.REFERENCES,
      TokenType.REFRESH,
      TokenType.RENAME,
      TokenType.REPLACE,
      TokenType.RIGHT,
      TokenType.ROLLUP,
      TokenType.ROW,
      TokenType.ROWS,
      TokenType.SEMI,
      TokenType.SET,
      TokenType.SETTINGS,
      TokenType.SHOW,
      TokenType.TEMPORARY,
      TokenType.TOP,
      TokenType.TRUE,
      TokenType.TRUNCATE,
      TokenType.UNIQUE,
      TokenType.UNNEST,
      TokenType.UNPIVOT,
      TokenType.UPDATE,
      TokenType.USE,
      TokenType.VOLATILE,
      TokenType.WINDOW,
      ...Parser.ALTERABLES,
      ...Parser.CREATABLES,
      ...Object.keys(Parser.SUBQUERY_PREDICATES) as TokenType[],
      ...Parser.TYPE_TOKENS,
      ...Object.keys(Parser.NO_PAREN_FUNCTIONS) as TokenType[],
    ]);
    tokens.delete(TokenType.UNION);
    return tokens;
  })();

  static TABLE_ALIAS_TOKENS = new Set(
    [...Parser.ID_VAR_TOKENS].filter((t) => ![
      TokenType.ANTI,
      TokenType.ASOF,
      TokenType.FULL,
      TokenType.LEFT,
      TokenType.LOCK,
      TokenType.NATURAL,
      TokenType.RIGHT,
      TokenType.SEMI,
      TokenType.WINDOW,
    ].includes(t)),
  );

  static ALIAS_TOKENS = Parser.ID_VAR_TOKENS;

  static COLON_PLACEHOLDER_TOKENS = Parser.ID_VAR_TOKENS;

  static ARRAY_CONSTRUCTORS = {
    ARRAY: ArrayExpr,
    LIST: ListExpr,
  };

  static COMMENT_TABLE_ALIAS_TOKENS = new Set(
    [...Parser.TABLE_ALIAS_TOKENS].filter(t => t !== TokenType.IS),
  );

  static UPDATE_ALIAS_TOKENS = new Set(
    [...Parser.TABLE_ALIAS_TOKENS].filter(t => t !== TokenType.SET),
  );

  static TRIM_TYPES = new Set(['LEADING', 'TRAILING', 'BOTH']);

  static FUNC_TOKENS = new Set([
    TokenType.COLLATE,
    TokenType.COMMAND,
    TokenType.CURRENT_DATE,
    TokenType.CURRENT_DATETIME,
    TokenType.CURRENT_SCHEMA,
    TokenType.CURRENT_TIMESTAMP,
    TokenType.CURRENT_TIME,
    TokenType.CURRENT_USER,
    TokenType.CURRENT_CATALOG,
    TokenType.FILTER,
    TokenType.FIRST,
    TokenType.FORMAT,
    TokenType.GET,
    TokenType.GLOB,
    TokenType.IDENTIFIER,
    TokenType.INDEX,
    TokenType.ISNULL,
    TokenType.ILIKE,
    TokenType.INSERT,
    TokenType.LIKE,
    TokenType.LOCALTIME,
    TokenType.LOCALTIMESTAMP,
    TokenType.MERGE,
    TokenType.NEXT,
    TokenType.OFFSET,
    TokenType.PRIMARY_KEY,
    TokenType.RANGE,
    TokenType.REPLACE,
    TokenType.RLIKE,
    TokenType.ROW,
    TokenType.SESSION_USER,
    TokenType.UNNEST,
    TokenType.VAR,
    TokenType.LEFT,
    TokenType.RIGHT,
    TokenType.SEQUENCE,
    TokenType.DATE,
    TokenType.DATETIME,
    TokenType.TABLE,
    TokenType.TIMESTAMP,
    TokenType.TIMESTAMPTZ,
    TokenType.TRUNCATE,
    TokenType.UTC_DATE,
    TokenType.UTC_TIME,
    TokenType.UTC_TIMESTAMP,
    TokenType.WINDOW,
    TokenType.XOR,
    ...Parser.TYPE_TOKENS,
    ...Object.keys(Parser.SUBQUERY_PREDICATES).map(Number),
  ]);

  static CONJUNCTION = {
    [TokenType.AND]: AndExpr,
  };

  static ASSIGNMENT = {
    [TokenType.COLON_EQ]: PropertyEQExpr,
  };

  static DISJUNCTION = {
    [TokenType.OR]: OrExpr,
  };

  static EQUALITY = {
    [TokenType.EQ]: EQExpr,
    [TokenType.NEQ]: NEQExpr,
    [TokenType.NULLSAFE_EQ]: NullSafeEQExpr,
  };

  static COMPARISON = {
    [TokenType.GT]: GTExpr,
    [TokenType.GTE]: GTEExpr,
    [TokenType.LT]: LTExpr,
    [TokenType.LTE]: LTEExpr,
  };

  static BITWISE = {
    [TokenType.AMP]: BitwiseAndExpr,
    [TokenType.CARET]: BitwiseXorExpr,
    [TokenType.PIPE]: BitwiseOrExpr,
  };

  static TERM = {
    [TokenType.DASH]: SubExpr,
    [TokenType.PLUS]: AddExpr,
    [TokenType.MOD]: ModExpr,
    [TokenType.COLLATE]: CollateExpr,
  };

  static FACTOR = {
    [TokenType.DIV]: IntDivExpr,
    [TokenType.LR_ARROW]: DistanceExpr,
    [TokenType.SLASH]: DivExpr,
    [TokenType.STAR]: MulExpr,
  };

  static EXPONENT = {};

  static TIMES = new Set([
    TokenType.TIME,
    TokenType.TIMETZ,
  ]);

  static TIMESTAMPS = new Set([
    TokenType.TIMESTAMP,
    TokenType.TIMESTAMPNTZ,
    TokenType.TIMESTAMPTZ,
    TokenType.TIMESTAMPLTZ,
    ...Parser.TIMES,
  ]);

  static SET_OPERATIONS = new Set([
    TokenType.UNION,
    TokenType.INTERSECT,
    TokenType.EXCEPT,
  ]);

  static JOIN_METHODS = new Set([
    TokenType.ASOF,
    TokenType.NATURAL,
    TokenType.POSITIONAL,
  ]);

  static JOIN_SIDES = new Set([
    TokenType.LEFT,
    TokenType.RIGHT,
    TokenType.FULL,
  ]);

  static JOIN_KINDS = new Set([
    TokenType.ANTI,
    TokenType.CROSS,
    TokenType.INNER,
    TokenType.OUTER,
    TokenType.SEMI,
    TokenType.STRAIGHT_JOIN,
  ]);

  static JOIN_HINTS: Set<string> = new Set();

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

  private get _constructor (): typeof Parser {
    return this.constructor as typeof Parser;
  }
}
