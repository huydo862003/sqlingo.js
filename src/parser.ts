// https://github.com/tobymao/sqlglot/blob/main/sqlglot/parser.py

import type { Expression } from './expressions';
import {
  array,
  ArrayAppendExpr,
  ArrayConcatExpr,
  ArrayExpr,
  ArrayPrependExpr,
  ArrayRemoveExpr,
  BinaryExpr,
  CoalesceExpr,
  ConvertTimezoneExpr,
  EscapeExpr,
  HexExpr,
  JSONExtractExpr,
  JSONExtractScalarExpr,
  LikeExpr,
  LiteralExpr,
  LnExpr,
  LogExpr,
  LowerExpr,
  LowerHexExpr,
  ModExpr,
  PadExpr,
  ParenExpr,
  StarMapExpr,
  StrPositionExpr,
  TrimExpr,
  TrimPosition,
  UpperExpr,
  VarMapExpr,
} from './expressions';
import { seqGet } from './helper';
import type { Dialect } from './dialects/dialect';
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
