// https://github.com/tobymao/sqlglot/blob/main/sqlglot/parser.py

import type { Expression } from './expressions';
import {
  AdjacentExpr,
  AddExpr,
  AliasExpr,
  AlgorithmPropertyExpr,
  AllExpr,
  AllowedValuesPropertyExpr,
  AndExpr,
  AnyExpr,
  AnonymousExpr,
  AutoIncrementPropertyExpr,
  array,
  ArgMaxExpr,
  ArgMinExpr,
  ArrayAggExpr,
  ArrayAppendExpr,
  ArrayConcatExpr,
  ArrayContainsAllExpr,
  ArrayExpr,
  ArrayPrependExpr,
  ArrayRemoveExpr,
  AtIndexExpr,
  AtTimeZoneExpr,
  BackupPropertyExpr,
  BinaryExpr,
  BetweenExpr,
  BitStringExpr,
  BitwiseAndExpr,
  BitwiseNotExpr,
  BitwiseLeftShiftExpr,
  BitwiseRightShiftExpr,
  BitwiseOrExpr,
  BitwiseXorExpr,
  BlockCompressionPropertyExpr,
  BracketExpr,
  BooleanExpr,
  ByteStringExpr,
  CastExpr,
  CastToStrTypeExpr,
  CaseExpr,
  CbrtExpr,
  CeilExpr,
  ChecksumPropertyExpr,
  CharacterSetExpr,
  CharacterSetPropertyExpr,
  ChrExpr,
  ChangesExpr,
  CloneExpr,
  ClusterExpr,
  ClusteredByPropertyExpr,
  CoalesceExpr,
  CollateExpr,
  CommentExpr,
  CommandExpr,
  CreateExpr,
  CollatePropertyExpr,
  ColumnExpr,
  ColumnConstraintExpr,
  ConcatExpr,
  ConcatWsExpr,
  ConnectByRootExpr,
  ConditionalInsertExpr,
  ConstraintExpr,
  ConvertTimezoneExpr,
  ConnectExpr,
  CountExpr,
  CopyGrantsPropertyExpr,
  CTEExpr,
  CubeExpr,
  CurrentDateExpr,
  CurrentDatetimeExpr,
  CurrentRoleExpr,
  CurrentTimeExpr,
  CurrentTimestampExpr,
  CurrentUserExpr,
  DataTypeExpr,
  DataTypeExprKind,
  DataTypeParamExpr,
  DataDeletionPropertyExpr,
  DataBlocksizePropertyExpr,
  DeleteExpr,
  DefinerPropertyExpr,
  DescribeExpr,
  DirectoryExpr,
  DistanceExpr,
  DistributeExpr,
  DistributedByPropertyExpr,
  DistKeyPropertyExpr,
  DistStylePropertyExpr,
  DistinctExpr,
  DivExpr,
  DotExpr,
  DropExpr,
  DPipeExpr,
  DuplicateKeyPropertyExpr,
  DynamicPropertyExpr,
  EmptyPropertyExpr,
  EnginePropertyExpr,
  EnviromentPropertyExpr,
  EQExpr,
  EscapeExpr,
  ExecuteAsPropertyExpr,
  ExceptExpr,
  ExistsExpr,
  ExpressionKey,
  ExtendsLeftExpr,
  ExtendsRightExpr,
  ExtractExpr,
  ExternalPropertyExpr,
  FallbackPropertyExpr,
  FetchExpr,
  FileFormatPropertyExpr,
  FloorExpr,
  ForeignKeyExpr,
  FreespacePropertyExpr,
  FromExpr,
  FUNCTION_BY_NAME,
  GlobalPropertyExpr,
  GenerateDateArrayExpr,
  GapFillExpr,
  GlobExpr,
  GreatestExpr,
  GTEExpr,
  GTExpr,
  GroupExpr,
  GroupingSetsExpr,
  HeapPropertyExpr,
  HavingExpr,
  HintExpr,
  HexExpr,
  HexStringExpr,
  HistoricalDataExpr,
  IcebergPropertyExpr,
  IfExpr,
  IdentifierExpr,
  ILikeExpr,
  IndexExpr,
  IndexParametersExpr,
  IndexTableHintExpr,
  InExpr,
  InheritsPropertyExpr,
  InputModelPropertyExpr,
  InputOutputFormatExpr,
  InsertExpr,
  IntoExpr,
  IntDivExpr,
  IntervalExpr,
  IntervalSpanExpr,
  IntersectExpr,
  IsolatedLoadingPropertyExpr,
  IsExpr,
  JSONBContainsAllTopKeysExpr,
  JSONBContainsAnyTopKeysExpr,
  JSONBContainsExpr,
  JSONBDeleteAtPathExpr,
  JSONBExtractExpr,
  JSONBExtractScalarExpr,
  JSONCastExpr,
  JSONExtractExpr,
  JSONExtractScalarExpr,
  JSONKeysExpr,
  JSONExpr,
  JournalPropertyExpr,
  JoinExpr,
  KwargExpr,
  KillExpr,
  LambdaExpr,
  LateralExpr,
  LanguagePropertyExpr,
  LeastExpr,
  LikeExpr,
  LikePropertyExpr,
  LimitExpr,
  LimitOptionsExpr,
  ListExpr,
  LiteralExpr,
  LocationPropertyExpr,
  LockingPropertyExpr,
  LockExpr,
  LnExpr,
  LocaltimeExpr,
  LocaltimestampExpr,
  LogExpr,
  LogPropertyExpr,
  LowerExpr,
  LowerHexExpr,
  LTEExpr,
  LTExpr,
  MaterializedPropertyExpr,
  MatchRecognizeExpr,
  MatchRecognizeMeasureExpr,
  MergeBlockRatioPropertyExpr,
  ModExpr,
  MulExpr,
  MultitableInsertsExpr,
  NationalExpr,
  NEQExpr,
  NegExpr,
  NextValueForExpr,
  NoPrimaryIndexPropertyExpr,
  NotExpr,
  NullExpr,
  NullSafeEQExpr,
  NullSafeNEQExpr,
  OrExpr,
  OrderExpr,
  OrderedExpr,
  OffsetExpr,
  OpclassExpr,
  ObjectIdentifierExpr,
  OutputModelPropertyExpr,
  OverlapsExpr,
  PadExpr,
  ParenExpr,
  ParseJSONExpr,
  PartitionBoundSpecExpr,
  PartitionByTruncateExpr,
  PartitionedByBucketExpr,
  PartitionedByPropertyExpr,
  PartitionedOfPropertyExpr,
  PivotExpr,
  PivotAliasExpr,
  PivotAnyExpr,
  PlaceholderExpr,
  PseudoTypeExpr,
  PragmaExpr,
  PropertyEQExpr,
  PropertyExpr,
  PriorExpr,
  PrimaryKeyExpr,
  PreWhereExpr,
  QueryExpr,
  QualifyExpr,
  RawStringExpr,
  RegexpILikeExpr,
  RegexpLikeExpr,
  RecursiveWithSearchExpr,
  ReferenceExpr,
  ReturnExpr,
  RemoteWithConnectionModelPropertyExpr,
  ReturnsPropertyExpr,
  ReturningExpr,
  RowFormatPropertyExpr,
  RowFormatDelimitedPropertyExpr,
  RowFormatSerdePropertyExpr,
  RollupExpr,
  SamplePropertyExpr,
  SchemaCommentPropertyExpr,
  SchemaExpr,
  ScopeResolutionExpr,
  SecurePropertyExpr,
  SecurityPropertyExpr,
  SelectExpr,
  SubqueryExpr,
  StreamExpr,
  SummarizeExpr,
  SemicolonExpr,
  SequencePropertiesExpr,
  SerdePropertiesExpr,
  SetPropertyExpr,
  SetOperationExpr,
  SettingsPropertyExpr,
  SharingPropertyExpr,
  SimilarToExpr,
  SliceExpr,
  SortExpr,
  SortKeyPropertyExpr,
  SqrtExpr,
  SqlSecurityPropertyExpr,
  SqlReadWritePropertyExpr,
  StabilityPropertyExpr,
  StarExpr,
  StarMapExpr,
  StorageHandlerPropertyExpr,
  StreamingTablePropertyExpr,
  StrToDateExpr,
  StrToTimeExpr,
  StrictPropertyExpr,
  SwapTableExpr,
  TableExpr,
  TableAliasExpr,
  TableFromRowsExpr,
  TableSampleExpr,
  StrPositionExpr,
  SubExpr,
  SubstringExpr,
  StructExpr,
  TemporaryPropertyExpr,
  ToTablePropertyExpr,
  TransformModelPropertyExpr,
  TransientPropertyExpr,
  TrimExpr,
  TrimPosition,
  TupleExpr,
  UnicodeStringExpr,
  UnloggedPropertyExpr,
  UnnestExpr,
  UnpivotExpr,
  UnpivotColumnsExpr,
  UnionExpr,
  UpperExpr,
  UuidExpr,
  varExpr,
  VarMapExpr,
  VarExpr,
  ValuesExpr,
  VersionExpr,
  VolatilePropertyExpr,
  ViewAttributePropertyExpr,
  WithJournalTablePropertyExpr,
  WithProcedureOptionsExpr,
  WithSchemaBindingPropertyExpr,
  WithSystemVersioningPropertyExpr,
  WithDataPropertyExpr,
  WithExpr,
  WithFillExpr,
  WithTableHintExpr,
  WhereExpr,
  AutoIncrementColumnConstraintExpr,
  CaseSpecificColumnConstraintExpr,
  CharacterSetColumnConstraintExpr,
  CheckColumnConstraintExpr,
  ClusteredColumnConstraintExpr,
  CollateColumnConstraintExpr,
  CommentColumnConstraintExpr,
  CompressColumnConstraintExpr,
  DateFormatColumnConstraintExpr,
  DefaultColumnConstraintExpr,
  EncodeColumnConstraintExpr,
  EphemeralColumnConstraintExpr,
  ExcludeColumnConstraintExpr,
  ForeignKeyColumnConstraintExpr,
  GeneratedAsIdentityColumnConstraintExpr,
  InlineColumnConstraintExpr,
  MergeTreeTTLExpr,
  MergeTreeTTLActionExpr,
  NonClusteredColumnConstraintExpr,
  NotNullColumnConstraintExpr,
  OnPropertyExpr,
  OnConflictExpr,
  OnCommitPropertyExpr,
  OnUpdateColumnConstraintExpr,
  PathColumnConstraintExpr,
  PeriodForSystemTimeConstraintExpr,
  PrimaryKeyColumnConstraintExpr,
  PropertiesExpr,
  ReferenceColumnConstraintExpr,
  TitleColumnConstraintExpr,
  UniqueColumnConstraintExpr,
  UppercaseColumnConstraintExpr,
} from './expressions';
import {
  ensureList, seqGet,
} from './helper';
import {
  Dialect, type DialectType,
} from './dialects/dialect';
import {
  concatMessages,
  ErrorLevel,
  highlightSql,
  mergeErrors,
  ParseError,
} from './errors';
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
      to: new DataTypeExpr({ this: DataTypeExprKind.TEXT }),
    }),

    TIME_TO_TIME_STR: (args, _dialect) => new CastExpr({
      this: seqGet(args, 0)!,
      to: new DataTypeExpr({ this: DataTypeExprKind.TEXT }),
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
        to: new DataTypeExpr({ this: DataTypeExprKind.TEXT }),
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
    [...Parser.TABLE_ALIAS_TOKENS].filter((t) => t !== TokenType.IS),
  );

  static UPDATE_ALIAS_TOKENS = new Set(
    [...Parser.TABLE_ALIAS_TOKENS].filter((t) => t !== TokenType.SET),
  );

  static TRIM_TYPES = new Set([
    'LEADING',
    'TRAILING',
    'BOTH',
  ]);

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

  static TIMES = new Set([TokenType.TIME, TokenType.TIMETZ]);

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

  static LAMBDAS = {
    [TokenType.ARROW]: (self: Parser, expressions: Expression[]) => self.expression(
      LambdaExpr,
      {
        this: self._replaceLambda(
          self._parseDisjunction(),
          expressions,
        ),
        expressions: expressions,
      },
    ),
    [TokenType.FARROW]: (self: Parser, expressions: Expression[]) => self.expression(
      KwargExpr,
      {
        this: varExpr(expressions[0].name),
        expression: self._parseDisjunction(),
      },
    ),
  };

  static COLUMN_OPERATORS = {
    [TokenType.DOT]: null,
    [TokenType.DOTCOLON]: (self: Parser, this_: Expression, to: Expression) => self.expression(
      JSONCastExpr,
      {
        this: this_,
        to: to,
      },
    ),
    [TokenType.DCOLON]: (self: Parser, this_: Expression, to: Expression) => self.buildCast({
      strict: self.STRICT_CAST,
      this: this_,
      to: to,
    }),
    [TokenType.ARROW]: (self: Parser, this_: Expression, path: Expression) => self.expression(
      JSONExtractExpr,
      {
        this: this_,
        expression: self.dialect.toJsonPath(path),
        onlyJsonTypes: self.JSON_ARROWS_REQUIRE_JSON_TYPE,
      },
    ),
    [TokenType.DARROW]: (self: Parser, this_: Expression, path: Expression) => self.expression(
      JSONExtractScalarExpr,
      {
        this: this_,
        expression: self.dialect.toJsonPath(path),
        onlyJsonTypes: self.JSON_ARROWS_REQUIRE_JSON_TYPE,
        scalarOnly: self.dialect.JSON_EXTRACT_SCALAR_SCALAR_ONLY,
      },
    ),
    [TokenType.HASH_ARROW]: (self: Parser, this_: Expression, path: Expression) => self.expression(
      JSONBExtractExpr,
      {
        this: this_,
        expression: path,
      },
    ),
    [TokenType.DHASH_ARROW]: (self: Parser, this_: Expression, path: Expression) => self.expression(
      JSONBExtractScalarExpr,
      {
        this: this_,
        expression: path,
      },
    ),
    [TokenType.PLACEHOLDER]: (self: Parser, this_: Expression, key: Expression) => self.expression(
      JSONBContainsExpr,
      {
        this: this_,
        expression: key,
      },
    ),
  };

  static CAST_COLUMN_OPERATORS = new Set([TokenType.DOTCOLON, TokenType.DCOLON]);

  static EXPRESSION_PARSERS = {
    [ExpressionKey.CLUSTER]: (self: Parser) => self._parseSort(ClusterExpr, TokenType.CLUSTER_BY),
    [ExpressionKey.COLUMN]: (self: Parser) => self._parseColumn(),
    [ExpressionKey.COLUMN_DEF]: (self: Parser) => self._parseColumnDef(self._parseColumn()),
    [ExpressionKey.CONDITION]: (self: Parser) => self._parseDisjunction(),
    [ExpressionKey.DATA_TYPE]: (self: Parser) => self._parseTypes({
      allowIdentifiers: false,
      schema: true,
    }),
    [ExpressionKey.EXPRESSION]: (self: Parser) => self._parseExpression(),
    [ExpressionKey.FROM]: (self: Parser) => self._parseFrom({ joins: true }),
    [ExpressionKey.GRANT_PRINCIPAL]: (self: Parser) => self._parseGrantPrincipal(),
    [ExpressionKey.GRANT_PRIVILEGE]: (self: Parser) => self._parseGrantPrivilege(),
    [ExpressionKey.GROUP]: (self: Parser) => self._parseGroup(),
    [ExpressionKey.HAVING]: (self: Parser) => self._parseHaving(),
    [ExpressionKey.HINT]: (self: Parser) => self._parseHintBody(),
    [ExpressionKey.IDENTIFIER]: (self: Parser) => self._parseIdVar(),
    [ExpressionKey.JOIN]: (self: Parser) => self._parseJoin(),
    [ExpressionKey.LAMBDA]: (self: Parser) => self._parseLambda(),
    [ExpressionKey.LATERAL]: (self: Parser) => self._parseLateral(),
    [ExpressionKey.LIMIT]: (self: Parser) => self._parseLimit(),
    [ExpressionKey.OFFSET]: (self: Parser) => self._parseOffset(),
    [ExpressionKey.ORDER]: (self: Parser) => self._parseOrder(),
    [ExpressionKey.ORDERED]: (self: Parser) => self._parseOrdered(),
    [ExpressionKey.PROPERTIES]: (self: Parser) => self._parseProperties(),
    [ExpressionKey.PARTITIONED_BY_PROPERTY]: (self: Parser) => self._parsePartitionedBy(),
    [ExpressionKey.QUALIFY]: (self: Parser) => self._parseQualify(),
    [ExpressionKey.RETURNING]: (self: Parser) => self._parseReturning(),
    [ExpressionKey.SELECT]: (self: Parser) => self._parseSelect(),
    [ExpressionKey.SORT]: (self: Parser) => self._parseSort(SortExpr, TokenType.SORT_BY),
    [ExpressionKey.TABLE]: (self: Parser) => self._parseTableParts(),
    [ExpressionKey.TABLE_ALIAS]: (self: Parser) => self._parseTableAlias(),
    [ExpressionKey.TUPLE]: (self: Parser) => self._parseValue({ values: false }),
    [ExpressionKey.WHENS]: (self: Parser) => self._parseWhenMatched(),
    [ExpressionKey.WHERE]: (self: Parser) => self._parseWhere(),
    [ExpressionKey.WINDOW]: (self: Parser) => self._parseNamedWindow(),
    [ExpressionKey.WITH]: (self: Parser) => self._parseWith(),
    JOIN_TYPE: (self: Parser) => self._parseJoinParts(),
  };

  static STATEMENT_PARSERS = {
    [TokenType.ALTER]: (self: Parser) => self._parseAlter(),
    [TokenType.ANALYZE]: (self: Parser) => self._parseAnalyze(),
    [TokenType.BEGIN]: (self: Parser) => self._parseTransaction(),
    [TokenType.CACHE]: (self: Parser) => self._parseCache(),
    [TokenType.COMMENT]: (self: Parser) => self._parseComment(),
    [TokenType.COMMIT]: (self: Parser) => self._parseCommitOrRollback(),
    [TokenType.COPY]: (self: Parser) => self._parseCopy(),
    [TokenType.CREATE]: (self: Parser) => self._parseCreate(),
    [TokenType.DELETE]: (self: Parser) => self._parseDelete(),
    [TokenType.DESC]: (self: Parser) => self._parseDescribe(),
    [TokenType.DESCRIBE]: (self: Parser) => self._parseDescribe(),
    [TokenType.DROP]: (self: Parser) => self._parseDrop(),
    [TokenType.GRANT]: (self: Parser) => self._parseGrant(),
    [TokenType.REVOKE]: (self: Parser) => self._parseRevoke(),
    [TokenType.INSERT]: (self: Parser) => self._parseInsert(),
    [TokenType.KILL]: (self: Parser) => self._parseKill(),
    [TokenType.LOAD]: (self: Parser) => self._parseLoad(),
    [TokenType.MERGE]: (self: Parser) => self._parseMerge(),
    [TokenType.PIVOT]: (self: Parser) => self._parseSimplifiedPivot(),
    [TokenType.PRAGMA]: (self: Parser) => self.expression(PragmaExpr, { this: self._parseExpression() }),
    [TokenType.REFRESH]: (self: Parser) => self._parseRefresh(),
    [TokenType.ROLLBACK]: (self: Parser) => self._parseCommitOrRollback(),
    [TokenType.SET]: (self: Parser) => self._parseSet(),
    [TokenType.TRUNCATE]: (self: Parser) => self._parseTruncateTable(),
    [TokenType.UNCACHE]: (self: Parser) => self._parseUncache(),
    [TokenType.UNPIVOT]: (self: Parser) => self._parseSimplifiedPivot({ isUnpivot: true }),
    [TokenType.UPDATE]: (self: Parser) => self._parseUpdate(),
    [TokenType.USE]: (self: Parser) => self._parseUse(),
    [TokenType.SEMICOLON]: (self: Parser) => new SemicolonExpr({}),
  };

  static UNARY_PARSERS = {
    [TokenType.PLUS]: (self: Parser) => self._parseUnary(),
    [TokenType.NOT]: (self: Parser) => self.expression(NotExpr, { this: self._parseEquality() }),
    [TokenType.TILDE]: (self: Parser) => self.expression(BitwiseNotExpr, { this: self._parseUnary() }),
    [TokenType.DASH]: (self: Parser) => self.expression(NegExpr, { this: self._parseUnary() }),
    [TokenType.PIPE_SLASH]: (self: Parser) => self.expression(SqrtExpr, { this: self._parseUnary() }),
    [TokenType.DPIPE_SLASH]: (self: Parser) => self.expression(CbrtExpr, { this: self._parseUnary() }),
  };

  static STRING_PARSERS = {
    [TokenType.HEREDOC_STRING]: (self: Parser, token: Token) => self.expression(RawStringExpr, { token }),
    [TokenType.NATIONAL_STRING]: (self: Parser, token: Token) => self.expression(NationalExpr, { token }),
    [TokenType.RAW_STRING]: (self: Parser, token: Token) => self.expression(RawStringExpr, { token }),
    [TokenType.STRING]: (self: Parser, token: Token) => self.expression(LiteralExpr, {
      token,
      isString: true,
    }),
    [TokenType.UNICODE_STRING]: (self: Parser, token: Token) => self.expression(
      UnicodeStringExpr,
      {
        token,
        escape: self._matchTextSeq('UESCAPE') && self._parseString(),
      },
    ),
  };

  static NUMERIC_PARSERS = {
    [TokenType.BIT_STRING]: (self: Parser, token: Token) => self.expression(BitStringExpr, { token }),
    [TokenType.BYTE_STRING]: (self: Parser, token: Token) => self.expression(
      ByteStringExpr,
      {
        token,
        isBytes: self.dialect.BYTE_STRING_IS_BYTES_TYPE || undefined,
      },
    ),
    [TokenType.HEX_STRING]: (self: Parser, token: Token) => self.expression(
      HexStringExpr,
      {
        token,
        isInteger: self.dialect.HEX_STRING_IS_INTEGER_TYPE || undefined,
      },
    ),
    [TokenType.NUMBER]: (self: Parser, token: Token) => self.expression(LiteralExpr, {
      token,
      isString: false,
    }),
  };

  static PRIMARY_PARSERS = {
    ...Parser.STRING_PARSERS,
    ...Parser.NUMERIC_PARSERS,
    [TokenType.INTRODUCER]: (self: Parser, token: Token) => self._parseIntroducer(token),
    [TokenType.NULL]: (self: Parser, _: Token) => self.expression(NullExpr, {}),
    [TokenType.TRUE]: (self: Parser, _: Token) => self.expression(BooleanExpr, { this: true }),
    [TokenType.FALSE]: (self: Parser, _: Token) => self.expression(BooleanExpr, { this: false }),
    [TokenType.SESSION_PARAMETER]: (self: Parser, _: Token) => self._parseSessionParameter(),
    [TokenType.STAR]: (self: Parser, _: Token) => self._parseStarOps(),
  };

  static PLACEHOLDER_PARSERS = {
    [TokenType.PLACEHOLDER]: (self: Parser) => self.expression(PlaceholderExpr, {}),
    [TokenType.PARAMETER]: (self: Parser) => self._parseParameter(),
    [TokenType.COLON]: (self: Parser) => (
      self._matchSet(self.COLON_PLACEHOLDER_TOKENS)
        ? self.expression(PlaceholderExpr, { this: self._prev.text })
        : null
    ),
  };

  static RANGE_PARSERS = {
    [TokenType.AT_GT]: binaryRangeParser(ArrayContainsAllExpr),
    [TokenType.BETWEEN]: (self: Parser, this_: Expression) => self._parseBetween(this_),
    [TokenType.GLOB]: binaryRangeParser(GlobExpr),
    [TokenType.ILIKE]: binaryRangeParser(ILikeExpr),
    [TokenType.IN]: (self: Parser, this_: Expression) => self._parseIn(this_),
    [TokenType.IRLIKE]: binaryRangeParser(RegexpILikeExpr),
    [TokenType.IS]: (self: Parser, this_: Expression) => self._parseIs(this_),
    [TokenType.LIKE]: binaryRangeParser(LikeExpr),
    [TokenType.LT_AT]: binaryRangeParser(ArrayContainsAllExpr, { reverseArgs: true }),
    [TokenType.OVERLAPS]: binaryRangeParser(OverlapsExpr),
    [TokenType.RLIKE]: binaryRangeParser(RegexpLikeExpr),
    [TokenType.SIMILAR_TO]: binaryRangeParser(SimilarToExpr),
    [TokenType.FOR]: (self: Parser, this_: Expression) => self._parseComprehension(this_),
    [TokenType.QMARK_AMP]: binaryRangeParser(JSONBContainsAllTopKeysExpr),
    [TokenType.QMARK_PIPE]: binaryRangeParser(JSONBContainsAnyTopKeysExpr),
    [TokenType.HASH_DASH]: binaryRangeParser(JSONBDeleteAtPathExpr),
    [TokenType.ADJACENT]: binaryRangeParser(AdjacentExpr),
    [TokenType.OPERATOR]: (self: Parser, this_: Expression) => self._parseOperator(this_),
    [TokenType.AMP_LT]: binaryRangeParser(ExtendsLeftExpr),
    [TokenType.AMP_GT]: binaryRangeParser(ExtendsRightExpr),
  };

  static PIPE_SYNTAX_TRANSFORM_PARSERS = {
    'AGGREGATE': (self: Parser, query: Expression) => self._parsePipeSyntaxAggregate(query),
    'AS': (self: Parser, query: Expression) => self._buildPipeCte(
      query,
      [new StarExpr({})],
      self._parseTableAlias(),
    ),
    'EXTEND': (self: Parser, query: Expression) => self._parsePipeSyntaxExtend(query),
    'LIMIT': (self: Parser, query: Expression) => self._parsePipeSyntaxLimit(query),
    'ORDER BY': (self: Parser, query: Expression) => query.orderBy(
      self._parseOrder(),
      {
        append: false,
        copy: false,
      },
    ),
    'PIVOT': (self: Parser, query: Expression) => self._parsePipeSyntaxPivot(query),
    'SELECT': (self: Parser, query: Expression) => self._parsePipeSyntaxSelect(query),
    'TABLESAMPLE': (self: Parser, query: Expression) => self._parsePipeSyntaxTablesample(query),
    'UNPIVOT': (self: Parser, query: Expression) => self._parsePipeSyntaxPivot(query),
    'WHERE': (self: Parser, query: Expression) => query.where(self._parseWhere(), { copy: false }),
  };

  static PROPERTY_PARSERS = {
    'ALLOWED_VALUES': (self: Parser) => self.expression(
      AllowedValuesPropertyExpr,
      { expressions: self._parseCsv(self._parsePrimary) },
    ),
    'ALGORITHM': (self: Parser) => self._parsePropertyAssignment(AlgorithmPropertyExpr),
    'AUTO': (self: Parser) => self._parseAutoProperty(),
    'AUTO_INCREMENT': (self: Parser) => self._parsePropertyAssignment(AutoIncrementPropertyExpr),
    'BACKUP': (self: Parser) => self.expression(
      BackupPropertyExpr,
      { this: self._parseVar({ anyToken: true }) },
    ),
    'BLOCKCOMPRESSION': (self: Parser) => self._parseBlockcompression(),
    'CHARSET': (self: Parser) => self._parseCharacterSet(),
    'CHARACTER SET': (self: Parser) => self._parseCharacterSet(),
    'CHECKSUM': (self: Parser) => self._parseChecksum(),
    'CLUSTER BY': (self: Parser) => self._parseCluster(),
    'CLUSTERED': (self: Parser) => self._parseClusteredBy(),
    'COLLATE': (self: Parser) => self._parsePropertyAssignment(CollatePropertyExpr),
    'COMMENT': (self: Parser) => self._parsePropertyAssignment(SchemaCommentPropertyExpr),
    'CONTAINS': (self: Parser) => self._parseContainsProperty(),
    'COPY': (self: Parser) => self._parseCopyProperty(),
    'DATABLOCKSIZE': (self: Parser) => self._parseDatablocksize(),
    'DATA_DELETION': (self: Parser) => self._parseDataDeletionProperty(),
    'DEFINER': (self: Parser) => self._parseDefiner(),
    'DETERMINISTIC': (self: Parser) => self.expression(
      StabilityPropertyExpr,
      { this: LiteralExpr.string('IMMUTABLE') },
    ),
    'DISTRIBUTED': (self: Parser) => self._parseDistributedProperty(),
    'DUPLICATE': (self: Parser) => self._parseCompositeKeyProperty(DuplicateKeyPropertyExpr),
    'DYNAMIC': (self: Parser) => self.expression(DynamicPropertyExpr, {}),
    'DISTKEY': (self: Parser) => self._parseDistkey(),
    'DISTSTYLE': (self: Parser) => self._parsePropertyAssignment(DistStylePropertyExpr),
    'EMPTY': (self: Parser) => self.expression(EmptyPropertyExpr, {}),
    'ENGINE': (self: Parser) => self._parsePropertyAssignment(EnginePropertyExpr),
    'ENVIRONMENT': (self: Parser) => self.expression(
      EnviromentPropertyExpr,
      { expressions: self._parseWrappedCsv(self._parseAssignment) },
    ),
    'EXECUTE': (self: Parser) => self._parsePropertyAssignment(ExecuteAsPropertyExpr),
    'EXTERNAL': (self: Parser) => self.expression(ExternalPropertyExpr, {}),
    'FALLBACK': (self: Parser) => self._parseFallback(),
    'FORMAT': (self: Parser) => self._parsePropertyAssignment(FileFormatPropertyExpr),
    'FREESPACE': (self: Parser) => self._parseFreespace(),
    'GLOBAL': (self: Parser) => self.expression(GlobalPropertyExpr, {}),
    'HEAP': (self: Parser) => self.expression(HeapPropertyExpr, {}),
    'ICEBERG': (self: Parser) => self.expression(IcebergPropertyExpr, {}),
    'IMMUTABLE': (self: Parser) => self.expression(
      StabilityPropertyExpr,
      { this: LiteralExpr.string('IMMUTABLE') },
    ),
    'INHERITS': (self: Parser) => self.expression(
      InheritsPropertyExpr,
      { expressions: self._parseWrappedCsv(self._parseTable) },
    ),
    'INPUT': (self: Parser) => self.expression(InputModelPropertyExpr, { this: self._parseSchema() }),
    'JOURNAL': (self: Parser) => self._parseJournal(),
    'LANGUAGE': (self: Parser) => self._parsePropertyAssignment(LanguagePropertyExpr),
    'LAYOUT': (self: Parser) => self._parseDictProperty({ this: 'LAYOUT' }),
    'LIFETIME': (self: Parser) => self._parseDictRange({ this: 'LIFETIME' }),
    'LIKE': (self: Parser) => self._parseCreateLike(),
    'LOCATION': (self: Parser) => self._parsePropertyAssignment(LocationPropertyExpr),
    'LOCK': (self: Parser) => self._parseLocking(),
    'LOCKING': (self: Parser) => self._parseLocking(),
    'LOG': (self: Parser) => self._parseLog(),
    'MATERIALIZED': (self: Parser) => self.expression(MaterializedPropertyExpr, {}),
    'MERGEBLOCKRATIO': (self: Parser) => self._parseMergeblockratio(),
    'MODIFIES': (self: Parser) => self._parseModifiesProperty(),
    'MULTISET': (self: Parser) => self.expression(SetPropertyExpr, { multi: true }),
    'NO': (self: Parser) => self._parseNoProperty(),
    'ON': (self: Parser) => self._parseOnProperty(),
    'ORDER BY': (self: Parser) => self._parseOrder({ skipOrderToken: true }),
    'OUTPUT': (self: Parser) => self.expression(OutputModelPropertyExpr, { this: self._parseSchema() }),
    'PARTITION': (self: Parser) => self._parsePartitionedOf(),
    'PARTITION BY': (self: Parser) => self._parsePartitionedBy(),
    'PARTITIONED BY': (self: Parser) => self._parsePartitionedBy(),
    'PARTITIONED_BY': (self: Parser) => self._parsePartitionedBy(),
    'PRIMARY KEY': (self: Parser) => self._parsePrimaryKey({ inProps: true }),
    'RANGE': (self: Parser) => self._parseDictRange({ this: 'RANGE' }),
    'READS': (self: Parser) => self._parseReadsProperty(),
    'REMOTE': (self: Parser) => self._parseRemoteWithConnection(),
    'RETURNS': (self: Parser) => self._parseReturns(),
    'STRICT': (self: Parser) => self.expression(StrictPropertyExpr, {}),
    'STREAMING': (self: Parser) => self.expression(StreamingTablePropertyExpr, {}),
    'ROW': (self: Parser) => self._parseRow(),
    'ROW_FORMAT': (self: Parser) => self._parsePropertyAssignment(RowFormatPropertyExpr),
    'SAMPLE': (self: Parser) => self.expression(
      SamplePropertyExpr,
      { this: self._matchTextSeq('BY') && self._parseBitwise() },
    ),
    'SECURE': (self: Parser) => self.expression(SecurePropertyExpr, {}),
    'SECURITY': (self: Parser) => self._parseSecurity(),
    'SET': (self: Parser) => self.expression(SetPropertyExpr, { multi: false }),
    'SETTINGS': (self: Parser) => self._parseSettingsProperty(),
    'SHARING': (self: Parser) => self._parsePropertyAssignment(SharingPropertyExpr),
    'SORTKEY': (self: Parser) => self._parseSortkey(),
    'SOURCE': (self: Parser) => self._parseDictProperty({ this: 'SOURCE' }),
    'STABLE': (self: Parser) => self.expression(
      StabilityPropertyExpr,
      { this: LiteralExpr.string('STABLE') },
    ),
    'STORED': (self: Parser) => self._parseStored(),
    'SYSTEM_VERSIONING': (self: Parser) => self._parseSystemVersioningProperty(),
    'TBLPROPERTIES': (self: Parser) => self._parseWrappedProperties(),
    'TEMP': (self: Parser) => self.expression(TemporaryPropertyExpr, {}),
    'TEMPORARY': (self: Parser) => self.expression(TemporaryPropertyExpr, {}),
    'TO': (self: Parser) => self._parseToTable(),
    'TRANSIENT': (self: Parser) => self.expression(TransientPropertyExpr, {}),
    'TRANSFORM': (self: Parser) => self.expression(
      TransformModelPropertyExpr,
      { expressions: self._parseWrappedCsv(self._parseExpression) },
    ),
    'TTL': (self: Parser) => self._parseTtl(),
    'USING': (self: Parser) => self._parsePropertyAssignment(FileFormatPropertyExpr),
    'UNLOGGED': (self: Parser) => self.expression(UnloggedPropertyExpr, {}),
    'VOLATILE': (self: Parser) => self._parseVolatileProperty(),
    'WITH': (self: Parser) => self._parseWithProperty(),
  };

  static CONSTRAINT_PARSERS = {
    'AUTOINCREMENT': (self: Parser) => self._parseAutoIncrement(),
    'AUTO_INCREMENT': (self: Parser) => self._parseAutoIncrement(),
    'CASESPECIFIC': (self: Parser) => self.expression(CaseSpecificColumnConstraintExpr, { not: false }),
    'CHARACTER SET': (self: Parser) => self.expression(
      CharacterSetColumnConstraintExpr,
      { this: self._parseVarOrString() },
    ),
    'CHECK': (self: Parser) => self._parseCheck(),
    'CLUSTERED': (self: Parser) => self.expression(ClusteredColumnConstraintExpr, {}),
    'COLLATE': (self: Parser) => self.expression(
      CollateColumnConstraintExpr,
      { this: self._parseVar() },
    ),
    'COMMENT': (self: Parser) => self.expression(
      CommentColumnConstraintExpr,
      { this: self._parseString() },
    ),
    'COMPRESS': (self: Parser) => self._parseCompress(),
    'ENCODE': (self: Parser) => self.expression(EncodeColumnConstraintExpr, { this: self._parseVar() }),
    'EPHEMERAL': (self: Parser) => self.expression(EphemeralColumnConstraintExpr, { this: self._parseBitwise() }),
    'EXCLUDE': (self: Parser) => self._parseExclude(),
    'FOREIGN KEY': (self: Parser) => self._parseForeignKey(),
    'FORMAT': (self: Parser) => self.expression(
      DateFormatColumnConstraintExpr,
      { this: self._parseVarOrString() },
    ),
    'GENERATED': (self: Parser) => self._parseGenerated(),
    'IDENTITY': (self: Parser) => self._parseGeneratedAsIdentity(),
    'INLINE': (self: Parser) => self._parseInline(),
    'LIKE': (self: Parser) => self._parseCreateLike(),
    'NONCLUSTERED': (self: Parser) => self.expression(NonClusteredColumnConstraintExpr, {}),
    'NOT': (self: Parser) => self._parseNotConstraint(),
    'NULL': (self: Parser) => self.expression(NotNullColumnConstraintExpr, { allowNull: true }),
    'ON': (self: Parser) => self.expression(OnPropertyExpr, { this: self._advanceAny() && self._prev.text }),
    'PATH': (self: Parser) => self.expression(PathColumnConstraintExpr, { this: self._parseString() }),
    'PERIOD': (self: Parser) => self._parsePeriodForSystemTime(),
    'PRIMARY KEY': (self: Parser) => self._parsePrimaryKey(),
    'REFERENCES': (self: Parser) => self._parseReferences(),
    'TITLE': (self: Parser) => self.expression(
      TitleColumnConstraintExpr,
      { this: self._parseVarOrString() },
    ),
    'TTL': (self: Parser) => self.expression(MergeTreeTTLExpr, { expressions: self._parseCsv(self._parseTtl) }),
    'UNIQUE': (self: Parser) => self._parseUnique(),
    'UPPERCASE': (self: Parser) => self.expression(UppercaseColumnConstraintExpr, {}),
    'WITH': (self: Parser) => self.expression(PropertiesExpr, { expressions: self._parseWrappedCsv(self._parseProperty) }),
  };

  static ALTER_PARSERS = {
    'ADD': (self: Parser) => self._parseAlterTableAdd(),
    'AS': (self: Parser) => self._parseSelect(),
    'ALTER': (self: Parser) => self._parseAlterTableAlter(),
    'CLUSTER BY': (self: Parser) => self._parseCluster({ wrapped: true }),
    'DELETE': (self: Parser) => self.expression(DeleteExpr, { where: self._parseWhere() }),
    'DROP': (self: Parser) => self._parseAlterTableDrop(),
    'RENAME': (self: Parser) => self._parseAlterTableRename(),
    'SET': (self: Parser) => self._parseAlterTableSet(),
    'SWAP': (self: Parser) => self.expression(
      SwapTableExpr,
      { this: self._match(TokenType.WITH) && self._parseTable({ schema: true }) },
    ),
  };

  static ALTER_ALTER_PARSERS = {
    DISTKEY: (self: Parser) => self._parseAlterDiststyle(),
    DISTSTYLE: (self: Parser) => self._parseAlterDiststyle(),
    SORTKEY: (self: Parser) => self._parseAlterSortkey(),
    COMPOUND: (self: Parser) => self._parseAlterSortkey({ compound: true }),
  };

  static SCHEMA_UNNAMED_CONSTRAINTS = new Set([
    'CHECK',
    'EXCLUDE',
    'FOREIGN KEY',
    'LIKE',
    'PERIOD',
    'PRIMARY KEY',
    'UNIQUE',
    'BUCKET',
    'TRUNCATE',
  ]);

  static NO_PAREN_FUNCTION_PARSERS = {
    ANY: (self: Parser) => self.expression(AnyExpr, { this: self._parseBitwise() }),
    CASE: (self: Parser) => self._parseCase(),
    CONNECT_BY_ROOT: (self: Parser) => self.expression(
      ConnectByRootExpr,
      { this: self._parseColumn() },
    ),
    IF: (self: Parser) => self._parseIf(),
  };

  static INVALID_FUNC_NAME_TOKENS = new Set([TokenType.IDENTIFIER, TokenType.STRING]);

  static FUNCTIONS_WITH_ALIASED_ARGS = new Set(['STRUCT']);

  static KEY_VALUE_DEFINITIONS = new Set([
    AliasExpr,
    EQExpr,
    PropertyEQExpr,
    SliceExpr,
  ]);

  static FUNCTION_PARSERS = {
    ...Object.fromEntries(
      ArgMaxExpr.sqlNames().map((name) => [name, (self: Parser) => self._parseMaxMinBy(ArgMaxExpr)]),
    ),
    ...Object.fromEntries(
      ArgMinExpr.sqlNames().map((name) => [name, (self: Parser) => self._parseMaxMinBy(ArgMinExpr)]),
    ),
    CAST: (self: Parser) => self._parseCast(self.STRICT_CAST),
    CEIL: (self: Parser) => self._parseCeilFloor(CeilExpr),
    CONVERT: (self: Parser) => self._parseConvert(self.STRICT_CAST),
    CHAR: (self: Parser) => self._parseChar(),
    CHR: (self: Parser) => self._parseChar(),
    DECODE: (self: Parser) => self._parseDecode(),
    EXTRACT: (self: Parser) => self._parseExtract(),
    FLOOR: (self: Parser) => self._parseCeilFloor(FloorExpr),
    GAP_FILL: (self: Parser) => self._parseGapFill(),
    INITCAP: (self: Parser) => self._parseInitcap(),
    JSON_OBJECT: (self: Parser) => self._parseJsonObject(),
    JSON_OBJECTAGG: (self: Parser) => self._parseJsonObject({ agg: true }),
    JSON_TABLE: (self: Parser) => self._parseJsonTable(),
    MATCH: (self: Parser) => self._parseMatchAgainst(),
    NORMALIZE: (self: Parser) => self._parseNormalize(),
    OPENJSON: (self: Parser) => self._parseOpenJson(),
    OVERLAY: (self: Parser) => self._parseOverlay(),
    POSITION: (self: Parser) => self._parsePosition(),
    SAFE_CAST: (self: Parser) => self._parseCast(false, { safe: true }),
    STRING_AGG: (self: Parser) => self._parseStringAgg(),
    SUBSTRING: (self: Parser) => self._parseSubstring(),
    TRIM: (self: Parser) => self._parseTrim(),
    TRY_CAST: (self: Parser) => self._parseCast(false, { safe: true }),
    TRY_CONVERT: (self: Parser) => self._parseConvert(false, { safe: true }),
    XMLELEMENT: (self: Parser) => self._parseXmlElement(),
    XMLTABLE: (self: Parser) => self._parseXmlTable(),
  };

  static QUERY_MODIFIER_PARSERS = {
    [TokenType.MATCH_RECOGNIZE]: (self: Parser): [string, Expression] => ['match', self._parseMatchRecognize()],
    [TokenType.PREWHERE]: (self: Parser): [string, Expression] => ['prewhere', self._parsePrewhere()],
    [TokenType.WHERE]: (self: Parser): [string, Expression] => ['where', self._parseWhere()],
    [TokenType.GROUP_BY]: (self: Parser): [string, Expression] => ['group', self._parseGroup()],
    [TokenType.HAVING]: (self: Parser): [string, Expression] => ['having', self._parseHaving()],
    [TokenType.QUALIFY]: (self: Parser): [string, Expression] => ['qualify', self._parseQualify()],
    [TokenType.WINDOW]: (self: Parser): [string, Expression] => ['windows', self._parseWindowClause()],
    [TokenType.ORDER_BY]: (self: Parser): [string, Expression] => ['order', self._parseOrder()],
    [TokenType.LIMIT]: (self: Parser): [string, Expression] => ['limit', self._parseLimit()],
    [TokenType.FETCH]: (self: Parser): [string, Expression] => ['limit', self._parseLimit()],
    [TokenType.OFFSET]: (self: Parser): [string, Expression] => ['offset', self._parseOffset()],
    [TokenType.FOR]: (self: Parser): [string, Expression] => ['locks', self._parseLocks()],
    [TokenType.LOCK]: (self: Parser): [string, Expression] => ['locks', self._parseLocks()],
    [TokenType.TABLE_SAMPLE]: (self: Parser): [string, Expression] => ['sample', self._parseTableSample({ asModifier: true })],
    [TokenType.USING]: (self: Parser): [string, Expression] => ['sample', self._parseTableSample({ asModifier: true })],
    [TokenType.CLUSTER_BY]: (self: Parser): [string, Expression] => ['cluster', self._parseSort(ClusterExpr, TokenType.CLUSTER_BY)],
    [TokenType.DISTRIBUTE_BY]: (self: Parser): [string, Expression] => ['distribute', self._parseSort(DistributeExpr, TokenType.DISTRIBUTE_BY)],
    [TokenType.SORT_BY]: (self: Parser): [string, Expression] => ['sort', self._parseSort(SortExpr, TokenType.SORT_BY)],
    [TokenType.CONNECT_BY]: (self: Parser): [string, Expression] => ['connect', self._parseConnect({ skipStartToken: true })],
    [TokenType.START_WITH]: (self: Parser): [string, Expression] => ['connect', self._parseConnect()],
  };

  static QUERY_MODIFIER_TOKENS = new Set(
    Object.keys(Parser.QUERY_MODIFIER_PARSERS) as TokenType[],
  );

  static SET_PARSERS = {
    GLOBAL: (self: Parser) => self._parseSetItemAssignment('GLOBAL'),
    LOCAL: (self: Parser) => self._parseSetItemAssignment('LOCAL'),
    SESSION: (self: Parser) => self._parseSetItemAssignment('SESSION'),
    TRANSACTION: (self: Parser) => self._parseSetTransaction(),
  };

  static SHOW_PARSERS: Record<string, (self: Parser) => Expression> = {};

  static TYPE_LITERAL_PARSERS = {
    [DataTypeExprKind.JSON]: (self: Parser, thisArg: Expression, _: unknown) => self.expression(ParseJSONExpr, { this: thisArg }),
  };

  static TYPE_CONVERTERS: Record<DataTypeExprKind, (dataType: DataTypeExpr) => DataTypeExpr> = {};

  static DDL_SELECT_TOKENS = new Set([
    TokenType.SELECT,
    TokenType.WITH,
    TokenType.L_PAREN,
  ]);

  static PRE_VOLATILE_TOKENS = new Set([
    TokenType.CREATE,
    TokenType.REPLACE,
    TokenType.UNIQUE,
  ]);

  static TRANSACTION_KIND = new Set([
    'DEFERRED',
    'IMMEDIATE',
    'EXCLUSIVE',
  ]);

  static TRANSACTION_CHARACTERISTICS: OptionsType = {
    ISOLATION: [
      [
        'LEVEL',
        'REPEATABLE',
        'READ',
      ],
      [
        'LEVEL',
        'READ',
        'COMMITTED',
      ],
      [
        'LEVEL',
        'READ',
        'UNCOMITTED',
      ],
      ['LEVEL', 'SERIALIZABLE'],
    ],
    READ: ['WRITE', 'ONLY'],
  };

  static CONFLICT_ACTIONS: OptionsType = {
    ...Object.fromEntries(
      [
        'ABORT',
        'FAIL',
        'IGNORE',
        'REPLACE',
        'ROLLBACK',
        'UPDATE',
      ].map((key) => [key, []]),
    ),
    DO: ['NOTHING', 'UPDATE'],
  };

  static CREATE_SEQUENCE: OptionsType = {
    SCALE: ['EXTEND', 'NOEXTEND'],
    SHARD: ['EXTEND', 'NOEXTEND'],
    NO: [
      'CYCLE',
      'CACHE',
      'MAXVALUE',
      'MINVALUE',
    ],
    ...Object.fromEntries(
      [
        'SESSION',
        'GLOBAL',
        'KEEP',
        'NOKEEP',
        'ORDER',
        'NOORDER',
        'NOCACHE',
        'CYCLE',
        'NOCYCLE',
        'NOMINVALUE',
        'NOMAXVALUE',
        'NOSCALE',
        'NOSHARD',
      ].map((key) => [key, []]),
    ),
  };

  static ISOLATED_LOADING_OPTIONS: OptionsType = {
    FOR: [
      'ALL',
      'INSERT',
      'NONE',
    ],
  };

  static USABLES: OptionsType = Object.fromEntries(
    [
      'ROLE',
      'WAREHOUSE',
      'DATABASE',
      'SCHEMA',
      'CATALOG',
    ].map((key) => [key, []]),
  );

  static CAST_ACTIONS: OptionsType = Object.fromEntries(
    ['RENAME', 'ADD'].map((key) => [key, ['FIELDS']]),
  );

  static SCHEMA_BINDING_OPTIONS: OptionsType = {
    TYPE: ['EVOLUTION'],
    ...Object.fromEntries(
      [
        'BINDING',
        'COMPENSATION',
        'EVOLUTION',
      ].map((key) => [key, []]),
    ),
  };

  static PROCEDURE_OPTIONS: OptionsType = {};

  static EXECUTE_AS_OPTIONS: OptionsType = Object.fromEntries(
    [
      'CALLER',
      'SELF',
      'OWNER',
    ].map((key) => [key, []]),
  );

  static KEY_CONSTRAINT_OPTIONS: OptionsType = {
    NOT: ['ENFORCED'],
    MATCH: [
      'FULL',
      'PARTIAL',
      'SIMPLE',
    ],
    INITIALLY: ['DEFERRED', 'IMMEDIATE'],
    USING: ['BTREE', 'HASH'],
    ...Object.fromEntries(
      [
        'DEFERRABLE',
        'NORELY',
        'RELY',
      ].map((key) => [key, []]),
    ),
  };

  static WINDOW_EXCLUDE_OPTIONS: OptionsType = {
    NO: ['OTHERS'],
    CURRENT: ['ROW'],
    ...Object.fromEntries(
      ['GROUP', 'TIES'].map((key) => [key, []]),
    ),
  };

  static INSERT_ALTERNATIVES = new Set([
    'ABORT',
    'FAIL',
    'IGNORE',
    'REPLACE',
    'ROLLBACK',
  ]);

  static CLONE_KEYWORDS = new Set(['CLONE', 'COPY']);

  static HISTORICAL_DATA_PREFIX = new Set([
    'AT',
    'BEFORE',
    'END',
  ]);

  static HISTORICAL_DATA_KIND = new Set([
    'OFFSET',
    'STATEMENT',
    'STREAM',
    'TIMESTAMP',
    'VERSION',
  ]);

  static OPCLASS_FOLLOW_KEYWORDS = new Set([
    'ASC',
    'DESC',
    'NULLS',
    'WITH',
  ]);

  static OPTYPE_FOLLOW_TOKENS = new Set([TokenType.COMMA, TokenType.R_PAREN]);

  static TABLE_INDEX_HINT_TOKENS = new Set([
    TokenType.FORCE,
    TokenType.IGNORE,
    TokenType.USE,
  ]);

  static VIEW_ATTRIBUTES = new Set([
    'ENCRYPTION',
    'SCHEMABINDING',
    'VIEW_METADATA',
  ]);

  static WINDOW_ALIAS_TOKENS = (() => {
    const result = new Set(Parser.ID_VAR_TOKENS);
    result.delete(TokenType.RANGE);
    result.delete(TokenType.ROWS);
    return result;
  })();

  static WINDOW_BEFORE_PAREN_TOKENS = new Set([TokenType.OVER]);

  static WINDOW_SIDES = new Set(['FOLLOWING', 'PRECEDING']);

  static JSON_KEY_VALUE_SEPARATOR_TOKENS = new Set([
    TokenType.COLON,
    TokenType.COMMA,
    TokenType.IS,
  ]);

  static FETCH_TOKENS = (() => {
    const result = new Set(Parser.ID_VAR_TOKENS);
    result.delete(TokenType.ROW);
    result.delete(TokenType.ROWS);
    result.delete(TokenType.PERCENT);
    return result;
  })();

  static ADD_CONSTRAINT_TOKENS = new Set([
    TokenType.CONSTRAINT,
    TokenType.FOREIGN_KEY,
    TokenType.INDEX,
    TokenType.KEY,
    TokenType.PRIMARY_KEY,
    TokenType.UNIQUE,
  ]);

  static DISTINCT_TOKENS = new Set([TokenType.DISTINCT]);

  static UNNEST_OFFSET_ALIAS_TOKENS = (() => {
    const result = new Set(Parser.TABLE_ALIAS_TOKENS);
    for (const token of Object.keys(Parser.SET_OPERATIONS).map(Number)) {
      result.delete(token as TokenType);
    }
    return result;
  })();

  static SELECT_START_TOKENS = new Set([
    TokenType.L_PAREN,
    TokenType.WITH,
    TokenType.SELECT,
  ]);

  static COPY_INTO_VARLEN_OPTIONS = new Set([
    'FILE_FORMAT',
    'COPY_OPTIONS',
    'FORMAT_OPTIONS',
    'CREDENTIAL',
  ]);

  static IS_JSON_PREDICATE_KIND = new Set([
    'VALUE',
    'SCALAR',
    'ARRAY',
    'OBJECT',
  ]);

  static ODBC_DATETIME_LITERALS: Record<string, typeof Expression> = {};

  static ON_CONDITION_TOKENS = new Set([
    'ERROR',
    'NULL',
    'TRUE',
    'FALSE',
    'EMPTY',
  ]);

  static PRIVILEGE_FOLLOW_TOKENS = new Set([
    TokenType.ON,
    TokenType.COMMA,
    TokenType.L_PAREN,
  ]);

  static DESCRIBE_STYLES = new Set([
    'ANALYZE',
    'EXTENDED',
    'FORMATTED',
    'HISTORY',
  ]);

  static SET_ASSIGNMENT_DELIMITERS = new Set([
    '=',
    ':=',
    'TO',
  ]);

  static ANALYZE_STYLES = new Set([
    'BUFFER_USAGE_LIMIT',
    'FULL',
    'LOCAL',
    'NO_WRITE_TO_BINLOG',
    'SAMPLE',
    'SKIP_LOCKED',
    'VERBOSE',
  ]);

  static ANALYZE_EXPRESSION_PARSERS = {
    ALL: (self: Parser) => self._parseAnalyzeColumns(),
    COMPUTE: (self: Parser) => self._parseAnalyzeStatistics(),
    DELETE: (self: Parser) => self._parseAnalyzeDelete(),
    DROP: (self: Parser) => self._parseAnalyzeHistogram(),
    ESTIMATE: (self: Parser) => self._parseAnalyzeStatistics(),
    LIST: (self: Parser) => self._parseAnalyzeList(),
    PREDICATE: (self: Parser) => self._parseAnalyzeColumns(),
    UPDATE: (self: Parser) => self._parseAnalyzeHistogram(),
    VALIDATE: (self: Parser) => self._parseAnalyzeValidate(),
  };

  static PARTITION_KEYWORDS = new Set(['PARTITION', 'SUBPARTITION']);

  static AMBIGUOUS_ALIAS_TOKENS = [TokenType.LIMIT, TokenType.OFFSET] as const;

  static OPERATION_MODIFIERS: Set<string> = new Set();

  static RECURSIVE_CTE_SEARCH_KIND = new Set([
    'BREADTH',
    'DEPTH',
    'CYCLE',
  ]);

  static MODIFIABLES = [
    QueryExpr,
    TableExpr,
    TableFromRowsExpr,
    ValuesExpr,
  ] as const;

  static STRICT_CAST = true;

  static PREFIXED_PIVOT_COLUMNS = false;

  static IDENTIFY_PIVOT_STRINGS = false;

  static LOG_DEFAULTS_TO_LN = false;

  static TABLESAMPLE_CSV = false;

  static DEFAULT_SAMPLING_METHOD: string | undefined = undefined;

  static SET_REQUIRES_ASSIGNMENT_DELIMITER = true;

  static TRIM_PATTERN_FIRST = false;

  static STRING_ALIASES = false;

  static MODIFIERS_ATTACHED_TO_SET_OP = true;

  static SET_OP_MODIFIERS = new Set([
    'order',
    'limit',
    'offset',
  ]);

  static NO_PAREN_IF_COMMANDS = true;

  static JSON_ARROWS_REQUIRE_JSON_TYPE = false;

  static COLON_IS_VARIANT_EXTRACT = false;

  static VALUES_FOLLOWED_BY_PAREN = true;

  static SUPPORTS_IMPLICIT_UNNEST = false;

  static INTERVAL_SPANS = true;

  static SUPPORTS_PARTITION_SELECTION = false;

  static WRAPPED_TRANSFORM_COLUMN_CONSTRAINT = true;

  static OPTIONAL_ALIAS_TOKEN_CTE = true;

  static ALTER_RENAME_REQUIRES_COLUMN = true;

  static ALTER_TABLE_PARTITIONS = false;

  static JOINS_HAVE_EQUAL_PRECEDENCE = false;

  static ZONE_AWARE_TIMESTAMP_CONSTRUCTOR = false;

  static MAP_KEYS_ARE_ARBITRARY_EXPRESSIONS = false;

  static JSON_EXTRACT_REQUIRES_JSON_EXPRESSION = false;

  static ADD_JOIN_ON_TRUE = false;

  static SUPPORTS_OMITTED_INTERVAL_SPAN_UNIT = false;

  // Instance properties
  protected sql: string;
  protected dialect: Dialect;
  protected errorLevel: ErrorLevel;
  protected errorMessageContext: number;
  protected maxErrors: number;
  protected errors: ParseError[];
  protected _tokens: Token[];
  protected _index: number;
  protected _curr: Token | undefined;
  protected _next: Token | undefined;
  protected _prev: Token | undefined;
  protected _prevComments: Token[] | undefined;
  protected _pipeCteCounter: number;

  constructor (options?: ParseOptions) {
    const opts = options ?? {};
    this.sql = '';
    this.dialect = Dialect.getOrRaise(opts.dialect);
    this.errorLevel = opts.errorLevel ?? ErrorLevel.IMMEDIATE;
    this.errorMessageContext = opts.errorMessageContext ?? 100;
    this.maxErrors = opts.maxErrors ?? 3;
    this.errors = [];
    this._tokens = [];
    this._index = 0;
    this._curr = undefined;
    this._next = undefined;
    this._prev = undefined;
    this._prevComments = undefined;
    this._pipeCteCounter = 0;
    this.reset();
  }

  reset (): void {
    this.sql = '';
    this.errors = [];
    this._tokens = [];
    this._index = 0;
    this._curr = undefined;
    this._next = undefined;
    this._prev = undefined;
    this._prevComments = undefined;
    this._pipeCteCounter = 0;
  }

  parse (rawTokens: Token[], sql?: string): (Expression | undefined)[] {
    /**
     * Parses a list of tokens and returns a list of syntax trees, one tree
     * per parsed SQL statement.
     *
     * @param rawTokens - The list of tokens.
     * @param sql - The original SQL string, used to produce helpful debug messages.
     * @returns The list of the produced syntax trees.
     */
    return this._parse({
      parseMethod: this._constructor._parseStatement,
      rawTokens,
      sql,
    });
  }

  parseInto (
    expressionTypes: ExpressionKey | ExpressionKey[],
    rawTokens: Token[],
    sql?: string,
  ): (Expression | undefined)[] {
    /**
     * Parses a list of tokens into a given Expression type. If a collection of Expression
     * types is given instead, this method will try to parse the token list into each one
     * of them, stopping at the first for which the parsing succeeds.
     *
     * @param expressionTypes - The expression type(s) to try and parse the token list into.
     * @param rawTokens - The list of tokens.
     * @param sql - The original SQL string, used to produce helpful debug messages.
     * @returns The target Expression.
     */
    const errors: ParseError[] = [];
    for (const expressionType of ensureList(expressionTypes)) {
      const parser = this._constructor.EXPRESSION_PARSERS[expressionType];
      if (!parser) {
        throw new TypeError(`No parser registered for ${expressionType}`);
      }

      try {
        return this._parse({
          parseMethod: parser,
          rawTokens,
          sql,
        });
      } catch (e) {
        if (e instanceof ParseError) {
          e.errors[0].intoExpression = expressionType;
          errors.push(e);
        } else {
          throw e;
        }
      }
    }

    throw new ParseError(
      `Failed to parse '${sql || rawTokens}' into ${expressionTypes}`,
      { errors: this.mergeErrors(errors) },
    );
  }

  protected _parse (options: {
    parseMethod: (self: Parser) => Expression | undefined;
    rawTokens: Token[];
    sql?: string;
  }): (Expression | undefined)[] {
    const {
      parseMethod, rawTokens, sql,
    } = options;

    this.reset();
    this.sql = sql || '';

    const total = rawTokens.length;
    const chunks: Token[][] = [[]];

    for (let i = 0; i < rawTokens.length; i++) {
      const token = rawTokens[i];
      if (token.tokenType === TokenType.SEMICOLON) {
        if (token.comments) {
          chunks.push([token]);
        }

        if (i < total - 1) {
          chunks.push([]);
        }
      } else {
        chunks[chunks.length - 1].push(token);
      }
    }

    const expressions: (Expression | undefined)[] = [];

    for (const tokens of chunks) {
      this._index = -1;
      this._tokens = tokens;
      this._advance();

      expressions.push(parseMethod(this));

      if (this._index < this._tokens.length) {
        this.raiseError('Invalid expression / Unexpected token');
      }

      this.checkErrors();
    }

    return expressions;
  }

  // Helper methods
  protected _findSql (start: Token, end: Token): string {
    return this.sql.slice(start.start ?? 0, (end.end ?? 0) + 1);
  }

  protected _isConnected (): boolean {
    return !!(
      this._prev
      && this._curr
      && (this._prev.end ?? 0) + 1 === (this._curr.start ?? 0)
    );
  }

  protected _advance (times: number = 1): void {
    this._index += times;
    this._curr = seqGet(this._tokens, this._index);
    this._next = seqGet(this._tokens, this._index + 1);

    if (0 < this._index) {
      this._prev = this._tokens[this._index - 1];
      this._prevComments = this._prev.comments;
    } else {
      this._prev = undefined;
      this._prevComments = undefined;
    }
  }

  protected _retreat (index: number): void {
    if (index !== this._index) {
      this._advance(index - this._index);
    }
  }

  protected _warnUnsupported (): void {
    if (this._tokens.length <= 1) {
      return;
    }

    // We use _findSql because this.sql may comprise multiple chunks, and we're only
    // interested in emitting a warning for the one being currently processed.
    const sql = this._findSql(
      this._tokens[0],
      this._tokens[this._tokens.length - 1],
    ).slice(0, this.errorMessageContext);

    console.warn(
      `'${sql}' contains unsupported syntax. Falling back to parsing as a 'Command'.`,
    );
  }

  protected _match (tokenType: TokenType, options?: { advance?: boolean }): boolean {
    const { advance = true } = options || {};
    if (this._curr?.tokenType === tokenType) {
      if (advance) {
        this._advance();
      }
      return true;
    }
    return false;
  }

  protected _parseDrop (exists: boolean = false): DropExpr | CommandExpr {
    const start = this._prev;
    const temporary = this._match(TokenType.TEMPORARY);
    const materialized = this._matchTextSeq('MATERIALIZED');

    const kind = this._matchSet(this._constructor.CREATABLES) && this._prev?.text.toUpperCase();
    if (!kind) {
      return this._parseAsCommand(start);
    }

    const concurrently = this._matchTextSeq('CONCURRENTLY');
    const ifExists = exists || this._parseExists();

    let thisExpr: Expression;
    if (kind === 'COLUMN') {
      thisExpr = this._parseColumn();
    } else {
      thisExpr = this._parseTableParts({
        schema: true,
        isDbReference: this._prev?.tokenType === TokenType.SCHEMA,
      });
    }

    const cluster = this._match(TokenType.ON) ? this._parseOnProperty() : undefined;

    let expressions: Expression[] | undefined;
    if (this._match(TokenType.L_PAREN, { advance: false })) {
      expressions = this._parseWrappedCsv(this._parseTypes);
    }

    return this.expression(DropExpr, {
      exists: ifExists,
      this: thisExpr,
      expressions,
      kind: this.dialect.creatableKindMapping().get(kind) || kind,
      temporary,
      materialized,
      cascade: this._matchTextSeq('CASCADE'),
      constraints: this._matchTextSeq('CONSTRAINTS'),
      purge: this._matchTextSeq('PURGE'),
      cluster,
      concurrently,
    });
  }

  protected _parseExists (notParam: boolean = false): boolean | undefined {
    const result = (
      this._matchTextSeq('IF')
      && (!notParam || this._match(TokenType.NOT))
      && this._match(TokenType.EXISTS)
    );
    return result ? true : undefined;
  }

  protected _parseCreate (): CreateExpr | CommandExpr {
    // Note: this can't be undefined because we've matched a statement parser
    const start = this._prev;

    const replace = (
      start?.tokenType === TokenType.REPLACE
      || this._matchPair(TokenType.OR, TokenType.REPLACE)
      || this._matchPair(TokenType.OR, TokenType.ALTER)
    );
    const refresh = this._matchPair(TokenType.OR, TokenType.REFRESH);

    const unique = this._match(TokenType.UNIQUE);

    let clustered: boolean | undefined;
    if (this._matchTextSeq('CLUSTERED', 'COLUMNSTORE')) {
      clustered = true;
    } else if (
      this._matchTextSeq('NONCLUSTERED', 'COLUMNSTORE')
      || this._matchTextSeq('COLUMNSTORE')
    ) {
      clustered = false;
    }

    if (this._matchPair(TokenType.TABLE, TokenType.FUNCTION, { advance: false })) {
      this._advance();
    }

    let properties: PropertiesExpr | undefined;
    let createToken = this._matchSet(this._constructor.CREATABLES) && this._prev;

    if (!createToken) {
      // exp.Properties.Location.POST_CREATE
      properties = this._parseProperties();
      createToken = this._matchSet(this._constructor.CREATABLES) && this._prev;

      if (!properties || !createToken) {
        return this._parseAsCommand(start);
      }
    }

    const concurrently = this._matchTextSeq('CONCURRENTLY');
    const exists = this._parseExists(true);
    let thisExpr: Expression | undefined;
    let expression: Expression | undefined;
    let indexes: Expression[] | undefined;
    let noSchemaBinding: boolean | undefined;
    let begin: boolean | undefined;
    let end: boolean | undefined;
    let clone: Expression | undefined;

    const extendProps = (tempProps: PropertiesExpr | undefined): void => {
      if (properties && tempProps) {
        properties.expressions.push(...tempProps.expressions);
      } else if (tempProps) {
        properties = tempProps;
      }
    };

    if (
      createToken.tokenType === TokenType.FUNCTION
      || createToken.tokenType === TokenType.PROCEDURE
    ) {
      thisExpr = this._parseUserDefinedFunction({ kind: createToken.tokenType });

      // exp.Properties.Location.POST_SCHEMA ("schema" here is the UDF's type signature)
      extendProps(this._parseProperties());

      expression = this._match(TokenType.ALIAS) && this._parseHeredoc();
      extendProps(this._parseProperties());

      if (!expression) {
        if (this._match(TokenType.COMMAND)) {
          expression = this._parseAsCommand(this._prev);
        } else {
          begin = this._match(TokenType.BEGIN);
          const return_ = this._matchTextSeq('RETURN');

          if (this._match(TokenType.STRING, { advance: false })) {
            // Takes care of BigQuery's JavaScript UDF definitions that end in an OPTIONS property
            // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_function_statement
            expression = this._parseString();
            extendProps(this._parseProperties());
          } else {
            expression = this._parseUserDefinedFunctionExpression();
          }

          end = this._matchTextSeq('END');

          if (return_) {
            expression = this.expression(ReturnExpr, { this: expression });
          }
        }
      }
    } else if (createToken.tokenType === TokenType.INDEX) {
      // Postgres allows anonymous indexes, eg. CREATE INDEX IF NOT EXISTS ON t(c)
      let index: Expression | undefined;
      let anonymous: boolean;

      if (!this._match(TokenType.ON)) {
        index = this._parseIdVar();
        anonymous = false;
      } else {
        index = undefined;
        anonymous = true;
      }

      thisExpr = this._parseIndex({
        index,
        anonymous,
      });
    } else if (this._constructor.DB_CREATABLES.has(createToken.tokenType)) {
      const tableParts = this._parseTableParts({
        schema: true,
        isDbReference: createToken.tokenType === TokenType.SCHEMA,
      });

      // exp.Properties.Location.POST_NAME
      this._match(TokenType.COMMA);
      extendProps(this._parseProperties({ before: true }));

      thisExpr = this._parseSchema({ this: tableParts });

      // exp.Properties.Location.POST_SCHEMA and POST_WITH
      extendProps(this._parseProperties());

      const hasAlias = this._match(TokenType.ALIAS);
      if (!this._matchSet(this._constructor.DDL_SELECT_TOKENS, { advance: false })) {
        // exp.Properties.Location.POST_ALIAS
        extendProps(this._parseProperties());
      }

      if (createToken.tokenType === TokenType.SEQUENCE) {
        expression = this._parseTypes();
        const props = this._parseProperties();

        if (props) {
          const sequenceProps = new SequencePropertiesExpr({});
          const options: Expression[] = [];

          for (const prop of props.expressions) {
            if (prop instanceof SequencePropertiesExpr) {
              for (const [arg, value] of Object.entries(prop.args)) {
                if (arg === 'options') {
                  options.push(...(value as Expression[]));
                } else {
                  sequenceProps.setArgKey(arg, value);
                }
              }
              prop.pop();
            }
          }

          if (0 < options.length) {
            sequenceProps.setArgKey('options', options);
          }

          props.expressions.push(sequenceProps);
          extendProps(props);
        }
      } else {
        expression = this._parseDdlSelect();

        // Some dialects also support using a table as an alias instead of a SELECT.
        // Here we fallback to this as an alternative.
        if (!expression && hasAlias) {
          expression = this._tryParse(() => this._parseTableParts());
        }
      }

      if (createToken.tokenType === TokenType.TABLE) {
        // exp.Properties.Location.POST_EXPRESSION
        extendProps(this._parseProperties());

        indexes = [];
        while (true) {
          const index = this._parseIndex();

          // exp.Properties.Location.POST_INDEX
          extendProps(this._parseProperties());
          if (!index) {
            break;
          } else {
            this._match(TokenType.COMMA);
            indexes.push(index);
          }
        }
      } else if (createToken.tokenType === TokenType.VIEW) {
        if (this._matchTextSeq('WITH', 'NO', 'SCHEMA', 'BINDING')) {
          noSchemaBinding = true;
        }
      } else if (
        createToken.tokenType === TokenType.SINK
        || createToken.tokenType === TokenType.SOURCE
      ) {
        extendProps(this._parseProperties());
      }

      const shallow = this._matchTextSeq('SHALLOW');

      if (this._matchTexts(this._constructor.CLONE_KEYWORDS)) {
        const copy = this._prev?.text.toLowerCase() === 'copy';
        clone = this.expression(CloneExpr, {
          this: this._parseTable({ schema: true }),
          shallow,
          copy,
        });
      }
    }

    if (
      this._curr
      && !this._matchSet(new Set([TokenType.R_PAREN, TokenType.COMMA]), { advance: false })
    ) {
      return this._parseAsCommand(start);
    }

    const createKindText = createToken.text.toUpperCase();
    return this.expression(CreateExpr, {
      this: thisExpr,
      kind: this.dialect.creatableKindMapping().get(createKindText) || createKindText,
      replace,
      refresh,
      unique,
      expression,
      exists,
      properties,
      indexes,
      noSchemaBinding,
      begin,
      end,
      clone,
      concurrently,
      clustered,
    });
  }

  protected _parseCommand (): CommandExpr {
    this._warnUnsupported();
    return this.expression(CommandExpr, {
      comments: this._prevComments,
      this: this._prev?.text.toUpperCase(),
      expression: this._parseString(),
    });
  }

  protected _parseSequenceProperties (): SequencePropertiesExpr | undefined {
    const seq = new SequencePropertiesExpr({});

    const options: Expression[] = [];
    const index = this._index;

    while (this._curr) {
      this._match(TokenType.COMMA);
      if (this._matchTextSeq('INCREMENT')) {
        this._matchTextSeq('BY');
        this._matchTextSeq('=');
        seq.set('increment', this._parseTerm());
      } else if (this._matchTextSeq('MINVALUE')) {
        seq.set('minvalue', this._parseTerm());
      } else if (this._matchTextSeq('MAXVALUE')) {
        seq.set('maxvalue', this._parseTerm());
      } else if (this._match(TokenType.START_WITH) || this._matchTextSeq('START')) {
        this._matchTextSeq('=');
        seq.set('start', this._parseTerm());
      } else if (this._matchTextSeq('CACHE')) {
        // T-SQL allows empty CACHE which is initialized dynamically
        seq.set('cache', this._parseNumber() || true);
      } else if (this._matchTextSeq('OWNED', 'BY')) {
        // "OWNED BY NONE" is the default
        seq.set('owned', this._matchTextSeq('NONE') ? undefined : this._parseColumn());
      } else {
        const opt = this._parseVarFromOptions(this._constructor.CREATE_SEQUENCE, { raiseUnmatched: false });
        if (opt) {
          options.push(opt);
        } else {
          break;
        }
      }
    }

    seq.set('options', 0 < options.length ? options : undefined);
    return this._index === index ? undefined : seq;
  }

  protected _parsePropertyBefore (): Expression | undefined {
    // only used for teradata currently
    this._match(TokenType.COMMA);

    const kwargs: Record<string, boolean | string> = {
      no: this._matchTextSeq('NO') || false,
      dual: this._matchTextSeq('DUAL') || false,
      before: this._matchTextSeq('BEFORE') || false,
      default: this._matchTextSeq('DEFAULT') || false,
      local: (this._matchTextSeq('LOCAL') && 'LOCAL')
        || (this._matchTextSeq('NOT', 'LOCAL') && 'NOT LOCAL')
        || false,
      after: this._matchTextSeq('AFTER') || false,
      minimum: this._matchTexts(['MIN', 'MINIMUM']) || false,
      maximum: this._matchTexts(['MAX', 'MAXIMUM']) || false,
    };

    if (this._matchTexts(Object.keys(this._constructor.PROPERTY_PARSERS))) {
      const parser = this._constructor.PROPERTY_PARSERS[this._prev!.text.toUpperCase()];
      try {
        const filteredKwargs = Object.fromEntries(
          Object.entries(kwargs).filter(([, v]) => v),
        );
        return parser(this, filteredKwargs);
      } catch (error) {
        this.raiseError(`Cannot parse property '${this._prev!.text}'`);
      }
    }

    return undefined;
  }

  protected _parseWrappedProperties (): Expression[] {
    return this._parseWrappedCsv(() => this._parseProperty());
  }

  protected _parseProperty (): Expression | undefined {
    if (this._matchTexts(Object.keys(this._constructor.PROPERTY_PARSERS))) {
      return this._constructor.PROPERTY_PARSERS[this._prev!.text.toUpperCase()](this);
    }

    if (this._match(TokenType.DEFAULT) && this._matchTexts(Object.keys(this._constructor.PROPERTY_PARSERS))) {
      return this._constructor.PROPERTY_PARSERS[this._prev!.text.toUpperCase()](this, { default: true });
    }

    if (this._matchTextSeq('COMPOUND', 'SORTKEY')) {
      return this._parseSortkey({ compound: true });
    }

    if (this._matchTextSeq('SQL', 'SECURITY')) {
      return this.expression(
        SqlSecurityPropertyExpr,
        { this: this._matchTexts(['DEFINER', 'INVOKER']) && this._prev!.text.toUpperCase() },
      );
    }

    const index = this._index;

    const seqProps = this._parseSequenceProperties();
    if (seqProps) {
      return seqProps;
    }

    this._retreat(index);
    let key = this._parseColumn();

    if (!this._match(TokenType.EQ)) {
      this._retreat(index);
      return undefined;
    }

    // Transform the key to exp.Dot if it's dotted identifiers wrapped in exp.Column or to exp.Var otherwise
    if (key instanceof ColumnExpr) {
      key = (key.parts && 1 < key.parts.length) ? key.toDot() : varExpr(key.name);
    }

    let value = this._parseBitwise() || this._parseVar({ anyToken: true });

    // Transform the value to exp.Var if it was parsed as exp.Column(exp.Identifier())
    if (value instanceof ColumnExpr) {
      value = varExpr(value.name);
    }

    return this.expression(PropertyExpr, {
      this: key,
      value,
    });
  }

  protected _parseStored (): FileFormatPropertyExpr | StorageHandlerPropertyExpr {
    if (this._matchTextSeq('BY')) {
      return this.expression(StorageHandlerPropertyExpr, { this: this._parseVarOrString() });
    }

    this._match(TokenType.ALIAS);
    const inputFormat = this._matchTextSeq('INPUTFORMAT') && this._parseString();
    const outputFormat = this._matchTextSeq('OUTPUTFORMAT') && this._parseString();

    return this.expression(
      FileFormatPropertyExpr,
      {
        this: (
          inputFormat || outputFormat
            ? this.expression(InputOutputFormatExpr, {
              inputFormat,
              outputFormat,
            })
            : this._parseVarOrString() || this._parseNumber() || this._parseIdVar()
        ),
        hiveFormat: true,
      },
    );
  }

  protected _parseUnquotedField (): Expression | undefined {
    const field = this._parseField();
    if (field instanceof IdentifierExpr && !field.quoted) {
      return varExpr(field);
    }

    return field;
  }

  protected _parsePropertyAssignment<E extends Expression> (
    expClass: new (args: any) => E,
    kwargs?: Record<string, any>,
  ): E {
    this._match(TokenType.EQ);
    this._match(TokenType.ALIAS);

    return this.expression(expClass, {
      this: this._parseUnquotedField(),
      ...kwargs,
    });
  }

  protected _parseProperties (options?: { before?: boolean }): PropertiesExpr | undefined {
    const properties: Expression[] = [];
    while (true) {
      const prop = options?.before
        ? this._parsePropertyBefore()
        : this._parseProperty();
      if (!prop) {
        break;
      }
      for (const p of ensureList(prop)) {
        properties.push(p);
      }
    }

    if (0 < properties.length) {
      return this.expression(PropertiesExpr, { expressions: properties });
    }

    return undefined;
  }

  protected _parseFallback (options?: { no?: boolean }): FallbackPropertyExpr {
    return this.expression(
      FallbackPropertyExpr,
      {
        no: options?.no,
        protection: this._matchTextSeq('PROTECTION'),
      },
    );
  }

  protected _parseSecurity (): SecurityPropertyExpr | undefined {
    if (this._matchTexts([
      'NONE',
      'DEFINER',
      'INVOKER',
    ])) {
      const securitySpecifier = this._prev!.text.toUpperCase();
      return this.expression(SecurityPropertyExpr, { this: securitySpecifier });
    }
    return undefined;
  }

  protected _parseSettingsProperty (): SettingsPropertyExpr {
    return this.expression(
      SettingsPropertyExpr,
      { expressions: this._parseCsv(() => this._parseAssignment()) },
    );
  }

  protected _parseVolatileProperty (): VolatilePropertyExpr | StabilityPropertyExpr {
    let preVolatileToken: Token | undefined;
    if (2 <= this._index) {
      preVolatileToken = this._tokens[this._index - 2];
    }

    if (preVolatileToken && this._constructor.PRE_VOLATILE_TOKENS.has(preVolatileToken.tokenType)) {
      return new VolatilePropertyExpr({});
    }

    return this.expression(StabilityPropertyExpr, { this: LiteralExpr.string('VOLATILE') });
  }

  protected _parseRetentionPeriod (): VarExpr {
    // Parse TSQL's HISTORY_RETENTION_PERIOD: {INFINITE | <number> DAY | DAYS | MONTH ...}
    const number = this._parseNumber();
    const numberStr = number ? `${number} ` : '';
    const unit = this._parseVar({ anyToken: true });
    return varExpr(`${numberStr}${unit}`);
  }

  protected _parseSystemVersioningProperty (options?: { with_?: boolean }): WithSystemVersioningPropertyExpr {
    this._match(TokenType.EQ);
    const prop = this.expression(
      WithSystemVersioningPropertyExpr,
      {
        on: true,
        with_: options?.with_,
      },
    );

    if (this._matchTextSeq('OFF')) {
      prop.set('on', false);
      return prop;
    }

    this._match(TokenType.ON);
    if (this._match(TokenType.L_PAREN)) {
      while (this._curr && !this._match(TokenType.R_PAREN)) {
        if (this._matchTextSeq('HISTORY_TABLE', '=')) {
          prop.set('this', this._parseTableParts());
        } else if (this._matchTextSeq('DATA_CONSISTENCY_CHECK', '=')) {
          this._advance();
          prop.set('dataConsistency', this._prev?.text.toUpperCase());
        } else if (this._matchTextSeq('HISTORY_RETENTION_PERIOD', '=')) {
          prop.set('retentionPeriod', this._parseRetentionPeriod());
        }

        this._match(TokenType.COMMA);
      }
    }

    return prop;
  }

  protected _parseDataDeletionProperty (): DataDeletionPropertyExpr {
    this._match(TokenType.EQ);
    const on = this._matchTextSeq('ON') || !this._matchTextSeq('OFF');
    const prop = this.expression(DataDeletionPropertyExpr, { on });

    if (this._match(TokenType.L_PAREN)) {
      while (this._curr && !this._match(TokenType.R_PAREN)) {
        if (this._matchTextSeq('FILTER_COLUMN', '=')) {
          prop.set('filterColumn', this._parseColumn());
        } else if (this._matchTextSeq('RETENTION_PERIOD', '=')) {
          prop.set('retentionPeriod', this._parseRetentionPeriod());
        }

        this._match(TokenType.COMMA);
      }
    }

    return prop;
  }

  protected _parseDistributedProperty (): DistributedByPropertyExpr {
    let kind = 'HASH';
    let expressions: Expression[] | undefined;
    if (this._matchTextSeq('BY', 'HASH')) {
      expressions = this._parseWrappedCsv(() => this._parseIdVar());
    } else if (this._matchTextSeq('BY', 'RANDOM')) {
      kind = 'RANDOM';
    }

    // If the BUCKETS keyword is not present, the number of buckets is AUTO
    let buckets: Expression | undefined;
    if (this._matchTextSeq('BUCKETS') && !this._matchTextSeq('AUTO')) {
      buckets = this._parseNumber();
    }

    return this.expression(
      DistributedByPropertyExpr,
      {
        expressions,
        kind,
        buckets,
        order: this._parseOrder(),
      },
    );
  }

  protected _parseCompositeKeyProperty<E extends Expression> (exprType: new (args: any) => E): E {
    this._matchTextSeq('KEY');
    const expressions = this._parseWrappedIdVars();
    return this.expression(exprType, { expressions });
  }

  protected _parseWithProperty (): Expression | Expression[] | undefined {
    if (this._matchTextSeq('(', 'SYSTEM_VERSIONING')) {
      const prop = this._parseSystemVersioningProperty({ with_: true });
      this._matchRParen();
      return prop;
    }

    if (this._match(TokenType.L_PAREN, { advance: false })) {
      return this._parseWrappedProperties();
    }

    if (this._matchTextSeq('JOURNAL')) {
      return this._parseWithjournaltable();
    }

    if (this._matchTexts(Object.keys(this._constructor.VIEW_ATTRIBUTES))) {
      return this.expression(ViewAttributePropertyExpr, { this: this._prev!.text.toUpperCase() });
    }

    if (this._matchTextSeq('DATA')) {
      return this._parseWithdata({ no: false });
    } else if (this._matchTextSeq('NO', 'DATA')) {
      return this._parseWithdata({ no: true });
    }

    if (this._match(TokenType.SERDE_PROPERTIES, { advance: false })) {
      return this._parseSerdeProperties({ with_: true });
    }

    if (this._match(TokenType.SCHEMA)) {
      return this.expression(
        WithSchemaBindingPropertyExpr,
        { this: this._parseVarFromOptions(this._constructor.SCHEMA_BINDING_OPTIONS) },
      );
    }

    if (this._matchTexts(Object.keys(this._constructor.PROCEDURE_OPTIONS), { advance: false })) {
      return this.expression(
        WithProcedureOptionsExpr,
        { expressions: this._parseCsv(() => this._parseProcedureOption()) },
      );
    }

    if (!this._next) {
      return undefined;
    }

    return this._parseWithisolatedloading();
  }

  protected _parseProcedureOption (): Expression | undefined {
    if (this._matchTextSeq('EXECUTE', 'AS')) {
      return this.expression(
        ExecuteAsPropertyExpr,
        {
          this: this._parseVarFromOptions(this._constructor.EXECUTE_AS_OPTIONS, { raiseUnmatched: false })
            || this._parseString(),
        },
      );
    }

    return this._parseVarFromOptions(this._constructor.PROCEDURE_OPTIONS);
  }

  // https://dev.mysql.com/doc/refman/8.0/en/create-view.html
  protected _parseDefiner (): DefinerPropertyExpr | undefined {
    this._match(TokenType.EQ);

    const user = this._parseIdVar();
    this._match(TokenType.PARAMETER);
    const host = this._parseIdVar() || (this._match(TokenType.MOD) && this._prev?.text);

    if (!user || !host) {
      return undefined;
    }

    return new DefinerPropertyExpr({ this: `${user}@${host}` });
  }

  protected _parseWithjournaltable (): WithJournalTablePropertyExpr {
    this._match(TokenType.TABLE);
    this._match(TokenType.EQ);
    return this.expression(WithJournalTablePropertyExpr, { this: this._parseTableParts() });
  }

  protected _parseLog (options?: { no?: boolean }): LogPropertyExpr {
    return this.expression(LogPropertyExpr, { no: options?.no });
  }

  protected _parseJournal (kwargs?: Record<string, any>): JournalPropertyExpr {
    return this.expression(JournalPropertyExpr, kwargs);
  }

  protected _parseChecksum (): ChecksumPropertyExpr {
    this._match(TokenType.EQ);

    let on: boolean | undefined;
    if (this._match(TokenType.ON)) {
      on = true;
    } else if (this._matchTextSeq('OFF')) {
      on = false;
    }

    return this.expression(ChecksumPropertyExpr, {
      on,
      default: this._match(TokenType.DEFAULT),
    });
  }

  protected _parseCluster (options?: { wrapped?: boolean }): ClusterExpr {
    return this.expression(
      ClusterExpr,
      {
        expressions: options?.wrapped
          ? this._parseWrappedCsv(() => this._parseOrdered())
          : this._parseCsv(() => this._parseOrdered()),
      },
    );
  }

  protected _parseClusteredBy (): ClusteredByPropertyExpr {
    this._matchTextSeq('BY');

    this._matchLParen();
    const expressions = this._parseCsv(() => this._parseColumn());
    this._matchRParen();

    let sortedBy: Expression[] | undefined;
    if (this._matchTextSeq('SORTED', 'BY')) {
      this._matchLParen();
      sortedBy = this._parseCsv(() => this._parseOrdered());
      this._matchRParen();
    }

    this._match(TokenType.INTO);
    const buckets = this._parseNumber();
    this._matchTextSeq('BUCKETS');

    return this.expression(
      ClusteredByPropertyExpr,
      {
        expressions,
        sortedBy,
        buckets,
      },
    );
  }

  protected _parseCopyProperty (): CopyGrantsPropertyExpr | undefined {
    if (!this._matchTextSeq('GRANTS')) {
      this._retreat(this._index - 1);
      return undefined;
    }

    return this.expression(CopyGrantsPropertyExpr, {});
  }

  protected _parseFreespace (): FreespacePropertyExpr {
    this._match(TokenType.EQ);
    return this.expression(
      FreespacePropertyExpr,
      {
        this: this._parseNumber(),
        percent: this._match(TokenType.PERCENT),
      },
    );
  }

  protected _parseMergeblockratio (options?: { no?: boolean;
    default?: boolean; }): MergeBlockRatioPropertyExpr {
    if (this._match(TokenType.EQ)) {
      return this.expression(
        MergeBlockRatioPropertyExpr,
        {
          this: this._parseNumber(),
          percent: this._match(TokenType.PERCENT),
        },
      );
    }

    return this.expression(MergeBlockRatioPropertyExpr, {
      no: options?.no,
      default: options?.default,
    });
  }

  protected _parseDatablocksize (options?: {
    default?: boolean;
    minimum?: boolean;
    maximum?: boolean;
  }): DataBlocksizePropertyExpr {
    this._match(TokenType.EQ);
    const size = this._parseNumber();

    let units: string | undefined;
    if (this._matchTexts([
      'BYTES',
      'KBYTES',
      'KILOBYTES',
    ])) {
      units = this._prev!.text;
    }

    return this.expression(
      DataBlocksizePropertyExpr,
      {
        size,
        units,
        default: options?.default,
        minimum: options?.minimum,
        maximum: options?.maximum,
      },
    );
  }

  protected _parseBlockcompression (): BlockCompressionPropertyExpr {
    this._match(TokenType.EQ);
    const always = this._matchTextSeq('ALWAYS');
    const manual = this._matchTextSeq('MANUAL');
    const never = this._matchTextSeq('NEVER');
    const default_ = this._matchTextSeq('DEFAULT');

    let autotemp: Expression | undefined;
    if (this._matchTextSeq('AUTOTEMP')) {
      autotemp = this._parseSchema();
    }

    return this.expression(
      BlockCompressionPropertyExpr,
      {
        always,
        manual,
        never,
        default: default_,
        autotemp,
      },
    );
  }

  protected _parseWithisolatedloading (): IsolatedLoadingPropertyExpr | undefined {
    const index = this._index;
    const no = this._matchTextSeq('NO');
    const concurrent = this._matchTextSeq('CONCURRENT');

    if (!this._matchTextSeq('ISOLATED', 'LOADING')) {
      this._retreat(index);
      return undefined;
    }

    const target = this._parseVarFromOptions(this._constructor.ISOLATED_LOADING_OPTIONS, { raiseUnmatched: false });
    return this.expression(
      IsolatedLoadingPropertyExpr,
      {
        no,
        concurrent,
        target,
      },
    );
  }

  protected _parseLocking (): LockingPropertyExpr {
    let kind: string | undefined;
    if (this._match(TokenType.TABLE)) {
      kind = 'TABLE';
    } else if (this._match(TokenType.VIEW)) {
      kind = 'VIEW';
    } else if (this._match(TokenType.ROW)) {
      kind = 'ROW';
    } else if (this._matchTextSeq('DATABASE')) {
      kind = 'DATABASE';
    }

    let thisExpr: Expression | undefined;
    if (kind === 'DATABASE' || kind === 'TABLE' || kind === 'VIEW') {
      thisExpr = this._parseTableParts();
    }

    let forOrIn: string | undefined;
    if (this._match(TokenType.FOR)) {
      forOrIn = 'FOR';
    } else if (this._match(TokenType.IN)) {
      forOrIn = 'IN';
    }

    let lockType: string | undefined;
    if (this._matchTextSeq('ACCESS')) {
      lockType = 'ACCESS';
    } else if (this._matchTexts(['EXCL', 'EXCLUSIVE'])) {
      lockType = 'EXCLUSIVE';
    } else if (this._matchTextSeq('SHARE')) {
      lockType = 'SHARE';
    } else if (this._matchTextSeq('READ')) {
      lockType = 'READ';
    } else if (this._matchTextSeq('WRITE')) {
      lockType = 'WRITE';
    } else if (this._matchTextSeq('CHECKSUM')) {
      lockType = 'CHECKSUM';
    }

    const override = this._matchTextSeq('OVERRIDE');

    return this.expression(
      LockingPropertyExpr,
      {
        this: thisExpr,
        kind,
        forOrIn,
        lockType,
        override,
      },
    );
  }

  protected _parsePartitionBy (): Expression[] {
    if (this._match(TokenType.PARTITION_BY)) {
      return this._parseCsv(() => this._parseDisjunction());
    }
    return [];
  }

  protected _parsePartitionBoundSpec (): PartitionBoundSpecExpr {
    const parsePartitionBoundExpr = (): Expression | undefined => {
      if (this._matchTextSeq('MINVALUE')) {
        return varExpr('MINVALUE');
      }
      if (this._matchTextSeq('MAXVALUE')) {
        return varExpr('MAXVALUE');
      }
      return this._parseBitwise();
    };

    let thisExpr: Expression | Expression[] | undefined;
    let expression: Expression | undefined;
    let fromExpressions: Expression[] | undefined;
    let toExpressions: Expression[] | undefined;

    if (this._match(TokenType.IN)) {
      thisExpr = this._parseWrappedCsv(() => this._parseBitwise());
    } else if (this._match(TokenType.FROM)) {
      fromExpressions = this._parseWrappedCsv(parsePartitionBoundExpr);
      this._matchTextSeq('TO');
      toExpressions = this._parseWrappedCsv(parsePartitionBoundExpr);
    } else if (this._matchTextSeq('WITH', '(', 'MODULUS')) {
      thisExpr = this._parseNumber();
      this._matchTextSeq(',', 'REMAINDER');
      expression = this._parseNumber();
      this._matchRParen();
    } else {
      this.raiseError('Failed to parse partition bound spec.');
    }

    return this.expression(
      PartitionBoundSpecExpr,
      {
        this: thisExpr,
        expression,
        fromExpressions,
        toExpressions,
      },
    );
  }

  // https://www.postgresql.org/docs/current/sql-createtable.html
  protected _parsePartitionedOf (): PartitionedOfPropertyExpr | undefined {
    if (!this._matchTextSeq('OF')) {
      this._retreat(this._index - 1);
      return undefined;
    }

    const thisExpr = this._parseTable({ schema: true });

    let expression: VarExpr | PartitionBoundSpecExpr;
    if (this._match(TokenType.DEFAULT)) {
      expression = varExpr('DEFAULT');
    } else if (this._matchTextSeq('FOR', 'VALUES')) {
      expression = this._parsePartitionBoundSpec();
    } else {
      this.raiseError('Expecting either DEFAULT or FOR VALUES clause.');
      expression = varExpr('DEFAULT'); // fallback
    }

    return this.expression(PartitionedOfPropertyExpr, {
      this: thisExpr,
      expression,
    });
  }

  protected _parsePartitionedBy (): PartitionedByPropertyExpr {
    this._match(TokenType.EQ);
    return this.expression(
      PartitionedByPropertyExpr,
      { this: this._parseSchema() || this._parseBracket(() => this._parseField()) },
    );
  }

  protected _parseWithdata (options?: { no?: boolean }): WithDataPropertyExpr {
    let statistics: boolean | undefined;
    if (this._matchTextSeq('AND', 'STATISTICS')) {
      statistics = true;
    } else if (this._matchTextSeq('AND', 'NO', 'STATISTICS')) {
      statistics = false;
    }

    return this.expression(WithDataPropertyExpr, {
      no: options?.no,
      statistics,
    });
  }

  protected _parseContainsProperty (): SqlReadWritePropertyExpr | undefined {
    if (this._matchTextSeq('SQL')) {
      return this.expression(SqlReadWritePropertyExpr, { this: 'CONTAINS SQL' });
    }
    return undefined;
  }

  protected _parseModifiesProperty (): SqlReadWritePropertyExpr | undefined {
    if (this._matchTextSeq('SQL', 'DATA')) {
      return this.expression(SqlReadWritePropertyExpr, { this: 'MODIFIES SQL DATA' });
    }
    return undefined;
  }

  protected _parseNoProperty (): Expression | undefined {
    if (this._matchTextSeq('PRIMARY', 'INDEX')) {
      return new NoPrimaryIndexPropertyExpr({});
    }
    if (this._matchTextSeq('SQL')) {
      return this.expression(SqlReadWritePropertyExpr, { this: 'NO SQL' });
    }
    return undefined;
  }

  protected _parseOnProperty (): Expression | undefined {
    if (this._matchTextSeq('COMMIT', 'PRESERVE', 'ROWS')) {
      return new OnCommitPropertyExpr({});
    }
    if (this._matchTextSeq('COMMIT', 'DELETE', 'ROWS')) {
      return new OnCommitPropertyExpr({ delete: true });
    }
    return this.expression(OnPropertyExpr, { this: this._parseSchema(this._parseIdVar()) });
  }

  protected _parseReadsProperty (): SqlReadWritePropertyExpr | undefined {
    if (this._matchTextSeq('SQL', 'DATA')) {
      return this.expression(SqlReadWritePropertyExpr, { this: 'READS SQL DATA' });
    }
    return undefined;
  }

  protected _parseDistkey (): DistKeyPropertyExpr {
    return this.expression(DistKeyPropertyExpr, { this: this._parseWrapped(() => this._parseIdVar()) });
  }

  protected _parseCreateLike (): LikePropertyExpr | undefined {
    const table = this._parseTable({ schema: true });

    const options: Expression[] = [];
    while (this._matchTexts(['INCLUDING', 'EXCLUDING'])) {
      const thisText = this._prev!.text.toUpperCase();

      const idVar = this._parseIdVar();
      if (!idVar) {
        return undefined;
      }

      options.push(
        this.expression(PropertyExpr, {
          this: thisText,
          value: varExpr(idVar.this.toUpperCase()),
        }),
      );
    }

    return this.expression(LikePropertyExpr, {
      this: table,
      expressions: options,
    });
  }

  protected _parseSortkey (options?: { compound?: boolean }): SortKeyPropertyExpr {
    return this.expression(
      SortKeyPropertyExpr,
      {
        this: this._parseWrappedIdVars(),
        compound: options?.compound,
      },
    );
  }

  protected _parseCharacterSet (options?: { default?: boolean }): CharacterSetPropertyExpr {
    this._match(TokenType.EQ);
    return this.expression(
      CharacterSetPropertyExpr,
      {
        this: this._parseVarOrString(),
        default: options?.default,
      },
    );
  }

  protected _parseRemoteWithConnection (): RemoteWithConnectionModelPropertyExpr {
    this._matchTextSeq('WITH', 'CONNECTION');
    return this.expression(
      RemoteWithConnectionModelPropertyExpr,
      { this: this._parseTableParts() },
    );
  }

  protected _parseReturns (): ReturnsPropertyExpr {
    let value: Expression | undefined;
    let null_: boolean | undefined;
    const isTable = this._match(TokenType.TABLE);

    if (isTable) {
      if (this._match(TokenType.LT)) {
        value = this.expression(
          SchemaExpr,
          {
            this: 'TABLE',
            expressions: this._parseCsv(() => this._parseStructTypes()),
          },
        );
        if (!this._match(TokenType.GT)) {
          this.raiseError('Expecting >');
        }
      } else {
        value = this._parseSchema(varExpr('TABLE'));
      }
    } else if (this._matchTextSeq('NULL', 'ON', 'NULL', 'INPUT')) {
      null_ = true;
      value = undefined;
    } else {
      value = this._parseTypes();
    }

    return this.expression(ReturnsPropertyExpr, {
      this: value,
      isTable,
      null: null_,
    });
  }

  protected _parseDescribe (): DescribeExpr {
    const kind = this._matchSet(this._constructor.CREATABLES) && this._prev?.text;
    let style = this._matchTexts(Array.from(this._constructor.DESCRIBE_STYLES)) && this._prev?.text.toUpperCase();
    if (this._match(TokenType.DOT)) {
      style = undefined;
      this._retreat(this._index - 2);
    }

    const format = this._match(TokenType.FORMAT, { advance: false }) ? this._parseProperty() : undefined;

    let thisExpr: Expression;
    if (this._matchSet(this._constructor.STATEMENT_PARSERS, { advance: false })) {
      thisExpr = this._parseStatement();
    } else {
      thisExpr = this._parseTable({ schema: true });
    }

    const properties = this._parseProperties();
    const expressions = properties?.expressions;
    const partition = this._parsePartition();
    return this.expression(
      DescribeExpr,
      {
        this: thisExpr,
        style,
        kind,
        expressions,
        partition,
        format,
        asJson: this._matchTextSeq('AS', 'JSON'),
      },
    );
  }

  protected _parseMultitableInserts (comments?: string[]): MultitableInsertsExpr {
    const kind = this._prev!.text.toUpperCase();
    const expressions: Expression[] = [];

    const parseConditionalInsert = (): ConditionalInsertExpr | undefined => {
      let expression: Expression | undefined;
      if (this._match(TokenType.WHEN)) {
        expression = this._parseDisjunction();
        this._match(TokenType.THEN);
      }

      const else_ = this._match(TokenType.ELSE);

      if (!this._match(TokenType.INTO)) {
        return undefined;
      }

      return this.expression(
        ConditionalInsertExpr,
        {
          this: this.expression(
            InsertExpr,
            {
              this: this._parseTable({ schema: true }),
              expression: this._parseDerivedTableValues(),
            },
          ),
          expression,
          else: else_,
        },
      );
    };

    let expr = parseConditionalInsert();
    while (expr !== undefined) {
      expressions.push(expr);
      expr = parseConditionalInsert();
    }

    return this.expression(
      MultitableInsertsExpr,
      {
        kind,
        comments,
        expressions,
        source: this._parseTable(),
      },
    );
  }

  protected _parseInsert (): InsertExpr | MultitableInsertsExpr {
    const comments: string[] = [];
    const hint = this._parseHint();
    const overwrite = this._match(TokenType.OVERWRITE);
    const ignore = this._match(TokenType.IGNORE);
    const local = this._matchTextSeq('LOCAL');
    let alternative: string | undefined;
    let isFunction: boolean | undefined;

    let thisExpr: Expression;
    if (this._matchTextSeq('DIRECTORY')) {
      thisExpr = this.expression(
        DirectoryExpr,
        {
          this: this._parseVarOrString(),
          local,
          rowFormat: this._parseRowFormat({ matchRow: true }),
        },
      );
    } else {
      if (this._matchSet(new Set([TokenType.FIRST, TokenType.ALL]))) {
        comments.push(...ensureList(this._prevComments));
        return this._parseMultitableInserts(comments);
      }

      if (this._match(TokenType.OR)) {
        alternative = this._matchTexts(Array.from(this._constructor.INSERT_ALTERNATIVES)) && this._prev?.text;
      }

      this._match(TokenType.INTO);
      comments.push(...ensureList(this._prevComments));
      this._match(TokenType.TABLE);
      isFunction = this._match(TokenType.FUNCTION);

      thisExpr = isFunction ? this._parseFunction() : this._parseInsertTable();
    }

    const returning = this._parseReturning(); // TSQL allows RETURNING before source

    return this.expression(
      InsertExpr,
      {
        comments,
        hint,
        isFunction,
        this: thisExpr,
        stored: this._matchTextSeq('STORED') && this._parseStored(),
        byName: this._matchTextSeq('BY', 'NAME'),
        exists: this._parseExists(),
        where: this._matchPair(TokenType.REPLACE, TokenType.WHERE) && this._parseDisjunction(),
        partition: this._match(TokenType.PARTITION_BY) && this._parsePartitionedBy(),
        settings: this._matchTextSeq('SETTINGS') && this._parseSettingsProperty(),
        default: this._matchTextSeq('DEFAULT', 'VALUES'),
        expression: this._parseDerivedTableValues() || this._parseDdlSelect(),
        conflict: this._parseOnConflict(),
        returning: returning || this._parseReturning(),
        overwrite,
        alternative,
        ignore,
        source: this._match(TokenType.TABLE) && this._parseTable(),
      },
    );
  }

  protected _parseInsertTable (): Expression | undefined {
    const thisExpr = this._parseTable({
      schema: true,
      parsePartition: true,
    });
    if (thisExpr instanceof TableExpr && this._match(TokenType.ALIAS, { advance: false })) {
      thisExpr.set('alias', this._parseTableAlias());
    }
    return thisExpr;
  }

  protected _parseKill (): KillExpr {
    const kind = this._matchTexts(['CONNECTION', 'QUERY']) ? varExpr(this._prev!.text) : undefined;

    return this.expression(
      KillExpr,
      {
        this: this._parsePrimary(),
        kind,
      },
    );
  }

  protected _parseOnConflict (): OnConflictExpr | undefined {
    const conflict = this._matchTextSeq('ON', 'CONFLICT');
    const duplicate = this._matchTextSeq('ON', 'DUPLICATE', 'KEY');

    if (!conflict && !duplicate) {
      return undefined;
    }

    let conflictKeys: Expression[] | undefined;
    let constraint: Expression | undefined;

    if (conflict) {
      if (this._matchTextSeq('ON', 'CONSTRAINT')) {
        constraint = this._parseIdVar();
      } else if (this._match(TokenType.L_PAREN)) {
        conflictKeys = this._parseCsv(() => this._parseIdVar());
        this._matchRParen();
      }
    }

    const indexPredicate = this._parseWhere();

    const action = this._parseVarFromOptions(this._constructor.CONFLICT_ACTIONS);
    let expressions: Expression[] | undefined;
    if (this._prev?.tokenType === TokenType.UPDATE) {
      this._match(TokenType.SET);
      expressions = this._parseCsv(() => this._parseEquality());
    }

    return this.expression(
      OnConflictExpr,
      {
        duplicate,
        expressions,
        action,
        conflictKeys,
        indexPredicate,
        constraint,
        where: this._parseWhere(),
      },
    );
  }

  protected _parseReturning (): ReturningExpr | undefined {
    if (!this._match(TokenType.RETURNING)) {
      return undefined;
    }
    return this.expression(
      ReturningExpr,
      {
        expressions: this._parseCsv(() => this._parseExpression()),
        into: this._match(TokenType.INTO) && this._parseTablePart(),
      },
    );
  }

  protected _parseRow (): RowFormatSerdePropertyExpr | RowFormatDelimitedPropertyExpr | undefined {
    if (!this._match(TokenType.FORMAT)) {
      return undefined;
    }
    return this._parseRowFormat();
  }

  protected _parseSerdeProperties (options?: { with_?: boolean }): SerdePropertiesExpr | undefined {
    const index = this._index;
    const with_ = options?.with_ || this._matchTextSeq('WITH');

    if (!this._match(TokenType.SERDE_PROPERTIES)) {
      this._retreat(index);
      return undefined;
    }
    return this.expression(
      SerdePropertiesExpr,
      {
        expressions: this._parseWrappedProperties(),
        with: with_,
      },
    );
  }

  protected _parseRowFormat (options?: {
    matchRow?: boolean;
  }): RowFormatSerdePropertyExpr | RowFormatDelimitedPropertyExpr | undefined {
    if (options?.matchRow && !this._matchPair(TokenType.ROW, TokenType.FORMAT)) {
      return undefined;
    }

    if (this._matchTextSeq('SERDE')) {
      const thisExpr = this._parseString();

      const serdeProperties = this._parseSerdeProperties();

      return this.expression(
        RowFormatSerdePropertyExpr,
        {
          this: thisExpr,
          serdeProperties,
        },
      );
    }

    this._matchTextSeq('DELIMITED');

    const kwargs: Record<string, Expression | undefined> = {};

    if (this._matchTextSeq('FIELDS', 'TERMINATED', 'BY')) {
      kwargs.fields = this._parseString();
      if (this._matchTextSeq('ESCAPED', 'BY')) {
        kwargs.escaped = this._parseString();
      }
    }
    if (this._matchTextSeq('COLLECTION', 'ITEMS', 'TERMINATED', 'BY')) {
      kwargs.collectionItems = this._parseString();
    }
    if (this._matchTextSeq('MAP', 'KEYS', 'TERMINATED', 'BY')) {
      kwargs.mapKeys = this._parseString();
    }
    if (this._matchTextSeq('LINES', 'TERMINATED', 'BY')) {
      kwargs.lines = this._parseString();
    }
    if (this._matchTextSeq('NULL', 'DEFINED', 'AS')) {
      kwargs.null = this._parseString();
    }

    return this.expression(RowFormatDelimitedPropertyExpr, kwargs);
  }

  protected _parseWrappedSelect (options?: { table?: boolean }): Expression | undefined {
    let thisExpr: Expression | undefined;

    if (this._matchSet(new Set([TokenType.PIVOT, TokenType.UNPIVOT]))) {
      thisExpr = this._parseSimplifiedPivot({
        isUnpivot: this._prev?.tokenType === TokenType.UNPIVOT,
      });
    } else if (this._match(TokenType.FROM)) {
      const from = this._parseFrom({
        skipFromToken: true,
        consumePipe: true,
      });
      // Support parentheses for duckdb FROM-first syntax
      const select = this._parseSelect({ from });
      if (select) {
        if (!select.args.from) {
          select.set('from', from);
        }
        thisExpr = select;
      } else {
        thisExpr = SelectExpr.select('*').from(from as FromExpr);
        thisExpr = this._parseQueryModifiers(this._parseSetOperations(thisExpr));
      }
    } else {
      thisExpr = options?.table
        ? this._parseTable({ consumePipe: true })
        : this._parseSelect({
          nested: true,
          parseSetOperation: false,
        });

      // Transform exp.Values into a exp.Table to pass through parse_query_modifiers
      // in case a modifier (e.g. join) is following
      if (options?.table && thisExpr instanceof ValuesExpr && thisExpr.alias) {
        const alias = thisExpr.args.alias;
        thisExpr.args.alias = undefined;
        thisExpr = new TableExpr({
          this: thisExpr,
          alias,
        });
      }

      thisExpr = this._parseQueryModifiers(this._parseSetOperations(thisExpr));
    }

    return thisExpr;
  }

  protected _parseSelect (options?: {
    nested?: boolean;
    table?: boolean;
    parseSubqueryAlias?: boolean;
    parseSetOperation?: boolean;
    consumePipe?: boolean;
    from?: FromExpr;
  }): Expression | undefined {
    let query = this._parseSelectQuery({
      nested: options?.nested,
      table: options?.table,
      parseSubqueryAlias: options?.parseSubqueryAlias,
      parseSetOperation: options?.parseSetOperation,
    });

    if ((options?.consumePipe ?? true) && this._match(TokenType.PIPE_GT, { advance: false })) {
      if (!query && options?.from) {
        query = SelectExpr.select('*').from(options.from);
      }
      if (query instanceof QueryExpr) {
        query = this._parsePipeSyntaxQuery(query);
        query = (query && options?.table) ? (query as SelectExpr).subquery({ copy: false }) : query;
      }
    }

    return query;
  }

  protected _parseSelectQuery (options?: {
    nested?: boolean;
    table?: boolean;
    parseSubqueryAlias?: boolean;
    parseSetOperation?: boolean;
  }): Expression | undefined {
    const cte = this._parseWith();

    if (cte) {
      let thisExpr = this._parseStatement();

      if (!thisExpr) {
        this.raiseError('Failed to parse any statement following CTE');
        return cte;
      }

      while (thisExpr instanceof SubqueryExpr && (thisExpr as any).isWrapper) {
        thisExpr = thisExpr.this;
      }

      if ('with' in thisExpr.argTypes) {
        thisExpr.set('with', cte);
      } else {
        this.raiseError(`${thisExpr.key} does not support CTE`);
        thisExpr = cte;
      }

      return thisExpr;
    }

    // duckdb supports leading with FROM x
    let from: FromExpr | undefined;
    if (this._match(TokenType.FROM, { advance: false })) {
      from = this._parseFrom({
        joins: true,
        consumePipe: true,
      }) as FromExpr | undefined;
    }

    if (this._match(TokenType.SELECT)) {
      const comments = this._prevComments;

      const hint = this._parseHint();

      let all_: boolean | undefined;
      let distinct: DistinctExpr | undefined;
      if (this._next && this._next.tokenType !== TokenType.DOT) {
        all_ = this._match(TokenType.ALL);
        distinct = this._matchSet(this._constructor.DISTINCT_TOKENS) ? new DistinctExpr({}) : undefined;
      }

      const kind = (
        this._match(TokenType.ALIAS)
        && this._matchTexts(['STRUCT', 'VALUE'])
        && this._prev?.text.toUpperCase()
      );

      if (distinct) {
        distinct = this.expression(
          DistinctExpr,
          { on: this._match(TokenType.ON) ? this._parseValue({ values: false }) : undefined },
        );
      }

      if (all_ && distinct) {
        this.raiseError('Cannot specify both ALL and DISTINCT after SELECT');
      }

      const operationModifiers: Expression[] = [];
      while (this._curr && this._matchTexts(Array.from(this._constructor.OPERATION_MODIFIERS))) {
        operationModifiers.push(varExpr(this._prev!.text.toUpperCase()));
      }

      const limit = this._parseLimit({ top: true });
      const projections = this._parseProjections();

      let thisExpr: SelectExpr = this.expression(
        SelectExpr,
        {
          kind,
          hint,
          distinct,
          expressions: projections,
          limit,
          operationModifiers: 0 < operationModifiers.length ? operationModifiers : undefined,
        },
      );
      thisExpr.comments = comments;

      const into = this._parseInto();
      if (into) {
        thisExpr.set('into', into);
      }

      if (!from) {
        from = this._parseFrom() as FromExpr | undefined;
      }

      if (from) {
        thisExpr.set('from', from);
      }

      thisExpr = this._parseQueryModifiers(thisExpr) as SelectExpr;
      return options?.parseSetOperation ?? true ? this._parseSetOperations(thisExpr) : thisExpr;
    } else if ((options?.table || options?.nested) && this._match(TokenType.L_PAREN)) {
      const thisExpr = this._parseWrappedSelect({ table: options?.table });

      // We return early here so that the UNION isn't attached to the subquery by the
      // following call to _parse_set_operations, but instead becomes the parent node
      this._matchRParen();
      return this._parseSubquery(thisExpr, { parseAlias: options?.parseSubqueryAlias });
    } else if (this._match(TokenType.VALUES, { advance: false })) {
      return this._parseDerivedTableValues();
    } else if (from) {
      return SelectExpr.select('*').from(from.this, { copy: false });
    } else if (this._match(TokenType.SUMMARIZE)) {
      const table = this._match(TokenType.TABLE);
      const thisExpr = this._parseSelect() || this._parseString() || this._parseTable();
      return this.expression(SummarizeExpr, {
        this: thisExpr,
        table,
      });
    } else if (this._match(TokenType.DESCRIBE)) {
      return this._parseDescribe();
    }

    return options?.parseSetOperation ?? true ? this._parseSetOperations(undefined) : undefined;
  }

  protected _parseRecursiveWithSearch (): RecursiveWithSearchExpr | undefined {
    this._matchTextSeq('SEARCH');

    const kind = this._matchTexts(Array.from(this._constructor.RECURSIVE_CTE_SEARCH_KIND)) && this._prev?.text.toUpperCase();

    if (!kind) {
      return undefined;
    }

    this._matchTextSeq('FIRST', 'BY');

    return this.expression(
      RecursiveWithSearchExpr,
      {
        kind,
        this: this._parseIdVar(),
        expression: this._matchTextSeq('SET') && this._parseIdVar(),
        using: this._matchTextSeq('USING') && this._parseIdVar(),
      },
    );
  }

  protected _parseWith (options?: { skipWithToken?: boolean }): WithExpr | undefined {
    if (!options?.skipWithToken && !this._match(TokenType.WITH)) {
      return undefined;
    }

    const comments = this._prevComments;
    const recursive = this._match(TokenType.RECURSIVE);

    let lastComments: string[] | undefined;
    const expressions: CTEExpr[] = [];
    while (true) {
      const cte = this._parseCte();
      if (cte instanceof CTEExpr) {
        expressions.push(cte);
        if (lastComments) {
          cte.addComments(lastComments);
        }
      }

      if (!this._match(TokenType.COMMA) && !this._match(TokenType.WITH)) {
        break;
      } else {
        this._match(TokenType.WITH);
      }

      lastComments = this._prevComments;
    }

    return this.expression(
      WithExpr,
      {
        comments,
        expressions,
        recursive,
        search: this._parseRecursiveWithSearch(),
      },
    );
  }

  protected _parseCte (): CTEExpr | undefined {
    const index = this._index;

    const alias = this._parseTableAlias({ aliasTokens: this._constructor.ID_VAR_TOKENS });
    if (!alias || !alias.this) {
      this.raiseError('Expected CTE to have alias');
    }

    const keyExpressions = this._matchTextSeq('USING', 'KEY')
      ? this._parseWrappedIdVars()
      : undefined;

    if (!this._match(TokenType.ALIAS) && !this._constructor.OPTIONAL_ALIAS_TOKEN_CTE) {
      this._retreat(index);
      return undefined;
    }

    const comments = this._prevComments;

    let materialized: boolean | undefined;
    if (this._matchTextSeq('NOT', 'MATERIALIZED')) {
      materialized = false;
    } else if (this._matchTextSeq('MATERIALIZED')) {
      materialized = true;
    }

    const cte = this.expression(
      CTEExpr,
      {
        this: this._parseWrapped(() => this._parseStatement()),
        alias,
        materialized,
        keyExpressions,
        comments,
      },
    );

    const values = cte.this;
    if (values instanceof ValuesExpr) {
      if (values.alias) {
        cte.set('this', SelectExpr.select('*').from(values));
      } else {
        cte.set('this', SelectExpr.select('*').from(
          (values as any).alias('_values', { table: true }),
        ));
      }
    }

    return cte;
  }

  protected _parseTableAlias (options?: {
    aliasTokens?: Set<TokenType>;
  }): TableAliasExpr | undefined {
    // In some dialects, LIMIT and OFFSET can act as both identifiers and keywords (clauses)
    // so this section tries to parse the clause version and if it fails, it treats the token
    // as an identifier (alias)
    if (this._canParseLimitOrOffset()) {
      return undefined;
    }

    const anyToken = this._match(TokenType.ALIAS);
    const alias = (
      this._parseIdVar({
        anyToken,
        tokens: options?.aliasTokens || this._constructor.TABLE_ALIAS_TOKENS,
      })
      || this._parseStringAsIdentifier()
    );

    const index = this._index;
    let columns: Expression[] | undefined;
    if (this._match(TokenType.L_PAREN)) {
      columns = this._parseCsv(() => this._parseFunctionParameter());
      if (!columns || columns.length === 0) {
        this._retreat(index);
        columns = undefined;
      } else {
        this._matchRParen();
      }
    }

    if (!alias && !columns) {
      return undefined;
    }

    const tableAlias = this.expression(TableAliasExpr, {
      this: alias,
      columns,
    });

    // We bubble up comments from the Identifier to the TableAlias
    if (alias instanceof IdentifierExpr) {
      tableAlias.addComments(alias.popComments());
    }

    return tableAlias;
  }

  protected _parseSubquery (
    thisExpr: Expression | undefined,
    options?: { parseAlias?: boolean },
  ): SubqueryExpr | undefined {
    if (!thisExpr) {
      return undefined;
    }

    return this.expression(
      SubqueryExpr,
      {
        this: thisExpr,
        pivots: this._parsePivots(),
        alias: (options?.parseAlias ?? true) ? this._parseTableAlias() : undefined,
        sample: this._parseTableSample(),
      },
    );
  }

  protected _implicitUnnestToExplicit<E extends Expression> (thisExpr: E): E {
    const refs = new Set<string>();
    const fromExpr = thisExpr.args.from as FromExpr | undefined;
    if (fromExpr?.this) {
      // Normalize and get alias/name
      const normalized = fromExpr.this; // Simplified - full normalization would use optimizer
      refs.add((normalized as any).aliasOrName || '');
    }

    const joins = thisExpr.args.joins as JoinExpr[] | undefined;
    if (joins) {
      for (const join of joins) {
        const table = join.this;
        if (table instanceof TableExpr && !join.args.on) {
          const normalized = table; // Simplified
          const tableName = normalized.parts?.[0]?.name || (table as any).aliasOrName;

          if (tableName && refs.has(tableName)) {
            const tableAsColumn = (table as any).toColumn?.();
            if (tableAsColumn) {
              const unnest = new UnnestExpr({ expressions: [tableAsColumn] });

              if (table.args.alias instanceof TableAliasExpr) {
                tableAsColumn.replace(tableAsColumn.this);
                (unnest as any).alias(undefined, {
                  table: [table.args.alias.this],
                  copy: false,
                });
              }

              (table as any).replace(unnest);
            }
          }

          refs.add(tableName || '');
        }
      }
    }

    return thisExpr;
  }

  protected _parseQueryModifiers<E extends Expression> (thisExpr: E): E;
  protected _parseQueryModifiers (thisExpr: undefined): undefined;
  protected _parseQueryModifiers (thisExpr: any): any {
    if (thisExpr && this._constructor.MODIFIABLES.has(thisExpr.constructor)) {
      for (const join of this._parseJoins()) {
        thisExpr.append('joins', join);
      }

      let lateral = this._parseLateral();
      while (lateral) {
        thisExpr.append('laterals', lateral);
        lateral = this._parseLateral();
      }

      while (true) {
        if (this._matchSet(this._constructor.QUERY_MODIFIER_PARSERS, { advance: false })) {
          const modifierToken = this._curr!;
          const parser = this._constructor.QUERY_MODIFIER_PARSERS[modifierToken.tokenType];
          const [key, expression] = parser(this);

          if (expression) {
            if (thisExpr.args[key]) {
              this.raiseError(
                `Found multiple '${modifierToken.text.toUpperCase()}' clauses`,
                modifierToken,
              );
            }

            thisExpr.set(key, expression);
            if (key === 'limit') {
              const offset = expression.args.offset;
              expression.set('offset', undefined);

              if (offset) {
                const offsetExpr = new OffsetExpr({ expression: offset });
                thisExpr.set('offset', offsetExpr);

                const limitByExpressions = expression.expressions;
                expression.set('expressions', undefined);
                offsetExpr.set('expressions', limitByExpressions);
              }
            }
            continue;
          }
        }
        break;
      }
    }

    if (this._constructor.SUPPORTS_IMPLICIT_UNNEST && thisExpr && thisExpr.args.from) {
      thisExpr = this._implicitUnnestToExplicit(thisExpr);
    }

    return thisExpr;
  }

  protected _parseHintFallbackToString (): HintExpr | undefined {
    const start = this._curr;
    while (this._curr) {
      this._advance();
    }

    const end = this._tokens[this._index - 1];
    return new HintExpr({ expressions: [this._findSql(start, end)] });
  }

  protected _parseHintFunctionCall (): Expression | undefined {
    return this._parseFunctionCall();
  }

  protected _parseHintBody (): HintExpr | undefined {
    const startIndex = this._index;
    let shouldFallbackToString = false;

    const hints: Expression[] = [];
    try {
      let hintBatch = this._parseCsv(() =>
        this._parseHintFunctionCall() || this._parseVar({ upper: true }));
      while (0 < hintBatch.length) {
        hints.push(...hintBatch);
        hintBatch = this._parseCsv(() =>
          this._parseHintFunctionCall() || this._parseVar({ upper: true }));
      }
    } catch (error) {
      shouldFallbackToString = true;
    }

    if (shouldFallbackToString || this._curr) {
      this._retreat(startIndex);
      return this._parseHintFallbackToString();
    }

    return this.expression(HintExpr, { expressions: hints });
  }

  protected _parseHint (): HintExpr | undefined {
    if (this._match(TokenType.HINT) && this._prevComments && 0 < this._prevComments.length) {
      // Parse hint from comment
      return (HintExpr as any).maybeParse?.(
        this._prevComments[0],
        {
          into: HintExpr,
          dialect: this.dialect,
        },
      );
    }

    return undefined;
  }

  protected _parseInto (): IntoExpr | undefined {
    if (!this._match(TokenType.INTO)) {
      return undefined;
    }

    const temp = this._match(TokenType.TEMPORARY);
    const unlogged = this._matchTextSeq('UNLOGGED');
    this._match(TokenType.TABLE);

    return this.expression(
      IntoExpr,
      {
        this: this._parseTable({ schema: true }),
        temporary: temp,
        unlogged,
      },
    );
  }

  protected _parseFrom (options?: {
    joins?: boolean;
    skipFromToken?: boolean;
    consumePipe?: boolean;
  }): FromExpr | undefined {
    if (!options?.skipFromToken && !this._match(TokenType.FROM)) {
      return undefined;
    }

    return this.expression(
      FromExpr,
      {
        comments: this._prevComments,
        this: this._parseTable({
          joins: options?.joins,
          consumePipe: options?.consumePipe,
        }),
      },
    );
  }

  protected _parseMatchRecognizeMeasure (): MatchRecognizeMeasureExpr {
    return this.expression(
      MatchRecognizeMeasureExpr,
      {
        windowFrame: this._matchTexts(['FINAL', 'RUNNING']) && this._prev!.text.toUpperCase(),
        this: this._parseExpression(),
      },
    );
  }

  protected _parseMatchRecognize (): MatchRecognizeExpr | undefined {
    if (!this._match(TokenType.MATCH_RECOGNIZE)) {
      return undefined;
    }

    this._matchLParen();

    const partition = this._parsePartitionBy();
    const order = this._parseOrder();

    const measures = this._matchTextSeq('MEASURES')
      ? this._parseCsv(() => this._parseMatchRecognizeMeasure())
      : undefined;

    let rows: VarExpr | undefined;
    if (this._matchTextSeq('ONE', 'ROW', 'PER', 'MATCH')) {
      rows = varExpr('ONE ROW PER MATCH');
    } else if (this._matchTextSeq('ALL', 'ROWS', 'PER', 'MATCH')) {
      let text = 'ALL ROWS PER MATCH';
      if (this._matchTextSeq('SHOW', 'EMPTY', 'MATCHES')) {
        text += ' SHOW EMPTY MATCHES';
      } else if (this._matchTextSeq('OMIT', 'EMPTY', 'MATCHES')) {
        text += ' OMIT EMPTY MATCHES';
      } else if (this._matchTextSeq('WITH', 'UNMATCHED', 'ROWS')) {
        text += ' WITH UNMATCHED ROWS';
      }
      rows = varExpr(text);
    }

    let after: VarExpr | undefined;
    if (this._matchTextSeq('AFTER', 'MATCH', 'SKIP')) {
      let text = 'AFTER MATCH SKIP';
      if (this._matchTextSeq('PAST', 'LAST', 'ROW')) {
        text += ' PAST LAST ROW';
      } else if (this._matchTextSeq('TO', 'NEXT', 'ROW')) {
        text += ' TO NEXT ROW';
      } else if (this._matchTextSeq('TO', 'FIRST')) {
        this._advance();
        text += ` TO FIRST ${this._prev?.text}`;
      } else if (this._matchTextSeq('TO', 'LAST')) {
        this._advance();
        text += ` TO LAST ${this._prev?.text}`;
      }
      after = varExpr(text);
    }

    let pattern: VarExpr | undefined;
    if (this._matchTextSeq('PATTERN')) {
      this._matchLParen();

      if (!this._curr) {
        this.raiseError('Expecting )', this._curr);
      }

      let paren = 1;
      const start = this._curr;
      let end = this._prev;

      while (this._curr && 0 < paren) {
        if (this._curr.tokenType === TokenType.L_PAREN) {
          paren++;
        }
        if (this._curr.tokenType === TokenType.R_PAREN) {
          paren--;
        }

        end = this._prev!;
        this._advance();
      }

      if (0 < paren) {
        this.raiseError('Expecting )', this._curr);
      }

      pattern = varExpr(this._findSql(start, end));
    }

    const define = this._matchTextSeq('DEFINE')
      ? this._parseCsv(() => this._parseNameAsExpression())
      : undefined;

    this._matchRParen();

    return this.expression(
      MatchRecognizeExpr,
      {
        partitionBy: partition,
        order,
        measures,
        rows,
        after,
        pattern,
        define,
        alias: this._parseTableAlias(),
      },
    );
  }

  protected _parseLateral (): LateralExpr | undefined {
    let crossApply: boolean | undefined = this._matchPair(TokenType.CROSS, TokenType.APPLY);
    if (!crossApply && this._matchPair(TokenType.OUTER, TokenType.APPLY)) {
      crossApply = false;
    }

    let thisExpr: Expression | undefined;
    let view: boolean | undefined;
    let outer: boolean | undefined;

    if (crossApply !== undefined) {
      thisExpr = this._parseSelect({ table: true });
      view = undefined;
      outer = undefined;
    } else if (this._match(TokenType.LATERAL)) {
      thisExpr = this._parseSelect({ table: true });
      view = this._match(TokenType.VIEW);
      outer = this._match(TokenType.OUTER);
    } else {
      return undefined;
    }

    if (!thisExpr) {
      thisExpr = (
        this._parseUnnest()
        || this._parseFunction()
        || this._parseIdVar({ anyToken: false })
      );

      while (this._match(TokenType.DOT)) {
        thisExpr = new DotExpr({
          this: thisExpr,
          expression: this._parseFunction() || this._parseIdVar({ anyToken: false }),
        });
      }
    }

    let ordinality: boolean | undefined;
    let tableAlias: TableAliasExpr | undefined;

    if (view) {
      const table = this._parseIdVar({ anyToken: false });
      const columns = this._match(TokenType.ALIAS)
        ? this._parseCsv(() => this._parseIdVar())
        : [];
      tableAlias = this.expression(
        TableAliasExpr,
        {
          this: table,
          columns,
        },
      );
    } else if ((thisExpr instanceof SubqueryExpr || thisExpr instanceof UnnestExpr) && thisExpr.alias) {
      // We move the alias from the lateral's child node to the lateral itself
      tableAlias = thisExpr.args.alias as TableAliasExpr;
      thisExpr.args.alias = undefined;
    } else {
      ordinality = this._matchPair(TokenType.WITH, TokenType.ORDINALITY);
      tableAlias = this._parseTableAlias();
    }

    return this.expression(
      LateralExpr,
      {
        this: thisExpr,
        view,
        outer,
        alias: tableAlias,
        crossApply,
        ordinality,
      },
    );
  }

  protected _parseStream (): StreamExpr | undefined {
    const index = this._index;
    if (this._matchTextSeq('STREAM')) {
      const thisExpr = this._tryParse(() => this._parseTable());
      if (thisExpr) {
        return this.expression(StreamExpr, { this: thisExpr });
      }
    }

    this._retreat(index);
    return undefined;
  }

  protected _parseJoinParts (): [Token | undefined, Token | undefined, Token | undefined] {
    return [
      this._matchSet(this._constructor.JOIN_METHODS) ? this._prev : undefined,
      this._matchSet(this._constructor.JOIN_SIDES) ? this._prev : undefined,
      this._matchSet(this._constructor.JOIN_KINDS) ? this._prev : undefined,
    ];
  }

  protected _parseUsingIdentifiers (): Expression[] {
    const parseColumnAsIdentifier = (): Expression | undefined => {
      const thisExpr = this._parseColumn();
      if (thisExpr instanceof ColumnExpr) {
        return thisExpr.this;
      }
      return thisExpr;
    };

    return this._parseWrappedCsv(parseColumnAsIdentifier, { optional: true });
  }

  protected _parseJoin (options?: {
    skipJoinToken?: boolean;
    parseBracket?: boolean;
  }): JoinExpr | undefined {
    if (this._match(TokenType.COMMA)) {
      const table = this._tryParse(() => this._parseTable());
      const crossJoin = table ? this.expression(JoinExpr, { this: table }) : undefined;

      if (crossJoin && this._constructor.JOINS_HAVE_EQUAL_PRECEDENCE) {
        crossJoin.set('kind', 'CROSS');
      }

      return crossJoin;
    }

    const index = this._index;
    const [
      method,
      side,
      kind,
    ] = this._parseJoinParts();
    const directed = this._matchTextSeq('DIRECTED');
    const hint = this._matchTexts(Array.from(this._constructor.JOIN_HINTS)) && this._prev?.text;
    const join = this._match(TokenType.JOIN) || (kind?.tokenType === TokenType.STRAIGHT_JOIN);
    const joinComments = this._prevComments;

    if (!options?.skipJoinToken && !join) {
      this._retreat(index);
      return undefined;
    }

    const outerApply = this._matchPair(TokenType.OUTER, TokenType.APPLY, { advance: false });
    const crossApply = this._matchPair(TokenType.CROSS, TokenType.APPLY, { advance: false });

    if (!options?.skipJoinToken && !join && !outerApply && !crossApply) {
      return undefined;
    }

    const kwargs: Record<string, any> = {
      this: this._parseTable({ parseBracket: options?.parseBracket }),
    };

    if (kind?.tokenType === TokenType.ARRAY && this._match(TokenType.COMMA)) {
      kwargs.expressions = this._parseCsv(() =>
        this._parseTable({ parseBracket: options?.parseBracket }));
    }

    if (method) kwargs.method = method.text.toUpperCase();
    if (side) kwargs.side = side.text.toUpperCase();
    if (kind) kwargs.kind = kind.text.toUpperCase();
    if (hint) kwargs.hint = hint;

    if (this._match(TokenType.MATCH_CONDITION)) {
      kwargs.matchCondition = this._parseWrapped(() => this._parseComparison());
    }

    if (this._match(TokenType.ON)) {
      kwargs.on = this._parseDisjunction();
    } else if (this._match(TokenType.USING)) {
      kwargs.using = this._parseUsingIdentifiers();
    } else if (
      !method
      && !(outerApply || crossApply)
      && !(kwargs.this instanceof UnnestExpr)
      && !(kind?.tokenType === TokenType.CROSS || kind?.tokenType === TokenType.ARRAY)
    ) {
      const nestedIndex = this._index;
      const joins = [...this._parseJoins()];

      if (0 < joins.length && this._match(TokenType.ON)) {
        kwargs.on = this._parseDisjunction();
      } else if (0 < joins.length && this._match(TokenType.USING)) {
        kwargs.using = this._parseUsingIdentifiers();
      } else {
        this._retreat(nestedIndex);
      }

      if (0 < joins.length && (kwargs.on || kwargs.using)) {
        kwargs.this.set('joins', joins);
      }
    }

    kwargs.pivots = this._parsePivots();

    const comments = [
      ...(joinComments || []),
      ...[
        method,
        side,
        kind,
      ].flatMap((token) => token?.comments || []),
    ];

    if (
      this._constructor.ADD_JOIN_ON_TRUE
      && !kwargs.on
      && !kwargs.using
      && !kwargs.method
      && (!kwargs.kind || kwargs.kind === 'INNER' || kwargs.kind === 'OUTER')
    ) {
      kwargs.on = LiteralExpr.boolean(true);
    }

    if (directed) {
      kwargs.directed = directed;
    }

    return this.expression(JoinExpr, {
      comments,
      ...kwargs,
    });
  }

  protected _parseOpclass (): Expression | undefined {
    const thisExpr = this._parseDisjunction();

    if (this._matchTexts(Array.from(this._constructor.OPCLASS_FOLLOW_KEYWORDS), { advance: false })) {
      return thisExpr;
    }

    if (!this._matchSet(this._constructor.OPTYPE_FOLLOW_TOKENS, { advance: false })) {
      return this.expression(OpclassExpr, {
        this: thisExpr,
        expression: this._parseTableParts(),
      });
    }

    return thisExpr;
  }

  protected _parseIndexParams (): IndexParametersExpr {
    const using = this._match(TokenType.USING)
      ? this._parseVar({ anyToken: true })
      : undefined;

    const columns = this._match(TokenType.L_PAREN, { advance: false })
      ? this._parseWrappedCsv(() => this._parseWithOperator())
      : undefined;

    const include = this._matchTextSeq('INCLUDE')
      ? this._parseWrappedIdVars()
      : undefined;

    const partitionBy = this._parsePartitionBy();
    const withStorage = this._match(TokenType.WITH) && this._parseWrappedProperties();
    const tablespace = this._matchTextSeq('USING', 'INDEX', 'TABLESPACE')
      ? this._parseVar({ anyToken: true })
      : undefined;

    const where = this._parseWhere();
    const on = this._match(TokenType.ON) ? this._parseField() : undefined;

    return this.expression(
      IndexParametersExpr,
      {
        using,
        columns,
        include,
        partitionBy,
        where,
        withStorage,
        tablespace,
        on,
      },
    );
  }

  protected _parseIndex (
    index?: Expression,
    anonymous: boolean = false,
  ): IndexExpr | undefined {
    let unique: boolean | undefined;
    let primary: boolean | undefined;
    let amp: boolean | undefined;
    let table: TableExpr | undefined;

    if (index || anonymous) {
      unique = undefined;
      primary = undefined;
      amp = undefined;

      this._match(TokenType.ON);
      this._match(TokenType.TABLE); // hive
      table = this._parseTableParts({ schema: true });
    } else {
      unique = this._match(TokenType.UNIQUE);
      primary = this._matchTextSeq('PRIMARY');
      amp = this._matchTextSeq('AMP');

      if (!this._match(TokenType.INDEX)) {
        return undefined;
      }

      index = this._parseIdVar();
      table = undefined;
    }

    const params = this._parseIndexParams();

    return this.expression(
      IndexExpr,
      {
        this: index,
        table,
        unique,
        primary,
        amp,
        params,
      },
    );
  }

  protected _parseTableHints (): Expression[] | undefined {
    const hints: Expression[] = [];

    if (this._matchPair(TokenType.WITH, TokenType.L_PAREN)) {
      // https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table?view=sql-server-ver16
      hints.push(
        this.expression(
          WithTableHintExpr,
          {
            expressions: this._parseCsv(() =>
              this._parseFunction() || this._parseVar({ anyToken: true })),
          },
        ),
      );
      this._matchRParen();
    } else {
      // https://dev.mysql.com/doc/refman/8.0/en/index-hints.html
      while (this._matchSet(this._constructor.TABLE_INDEX_HINT_TOKENS)) {
        const hint = new IndexTableHintExpr({ this: this._prev!.text.toUpperCase() });

        this._matchSet(new Set([TokenType.INDEX, TokenType.KEY]));
        if (this._match(TokenType.FOR)) {
          this._advanceAny();
          hint.set('target', this._prev!.text.toUpperCase());
        }

        hint.set('expressions', this._parseWrappedIdVars());
        hints.push(hint);
      }
    }

    return 0 < hints.length ? hints : undefined;
  }

  protected _parseTablePart (options?: { schema?: boolean }): Expression | undefined {
    return (
      (!options?.schema && this._parseFunction({ optionalParens: false }))
      || this._parseIdVar({ anyToken: false })
      || this._parseStringAsIdentifier()
      || this._parsePlaceholder()
    );
  }

  protected _parseTableParts (options?: {
    schema?: boolean;
    isDbReference?: boolean;
    wildcard?: boolean;
  }): TableExpr {
    let catalog: Expression | string | undefined;
    let db: Expression | string | undefined;
    let table: Expression | string | undefined = this._parseTablePart({ schema: options?.schema });

    while (this._match(TokenType.DOT)) {
      if (catalog) {
        // This allows nesting the table in arbitrarily many dot expressions if needed
        table = this.expression(
          DotExpr,
          {
            this: table,
            expression: this._parseTablePart({ schema: options?.schema }),
          },
        );
      } else {
        catalog = db;
        db = table;
        // "" used for tsql FROM a..b case
        table = this._parseTablePart({ schema: options?.schema }) || '';
      }
    }

    if (
      options?.wildcard
      && this._isConnected()
      && (table instanceof IdentifierExpr || !table)
      && this._match(TokenType.STAR)
    ) {
      if (table instanceof IdentifierExpr) {
        table.args.this += '*';
      } else {
        table = new IdentifierExpr({ this: '*' });
      }
    }

    // We bubble up comments from the Identifier to the Table
    const comments = table instanceof Expression ? table.popComments() : undefined;

    if (options?.isDbReference) {
      catalog = db;
      db = table;
      table = undefined;
    }

    if (!table && !options?.isDbReference) {
      this.raiseError(`Expected table name but got ${this._curr}`, this._curr);
    }
    if (!db && options?.isDbReference) {
      this.raiseError(`Expected database name but got ${this._curr}`, this._curr);
    }

    const tableExpr = this.expression(
      TableExpr,
      {
        comments,
        this: table,
        db,
        catalog,
      },
    );

    const changes = this._parseChanges();
    if (changes) {
      tableExpr.set('changes', changes);
    }

    const atBefore = this._parseHistoricalData();
    if (atBefore) {
      tableExpr.set('when', atBefore);
    }

    const pivots = this._parsePivots();
    if (pivots) {
      tableExpr.set('pivots', pivots);
    }

    return tableExpr;
  }

  protected _parseTable (options?: {
    schema?: boolean;
    joins?: boolean;
    aliasTokens?: Set<TokenType>;
    parseBracket?: boolean;
    isDbReference?: boolean;
    parsePartition?: boolean;
    consumePipe?: boolean;
  }): Expression | undefined {
    const stream = this._parseStream();
    if (stream) {
      return stream;
    }

    const lateral = this._parseLateral();
    if (lateral) {
      return lateral;
    }

    const unnest = this._parseUnnest();
    if (unnest) {
      return unnest;
    }

    const values = this._parseDerivedTableValues();
    if (values) {
      return values;
    }

    const subquery = this._parseSelect({
      table: true,
      consumePipe: options?.consumePipe,
    });
    if (subquery) {
      if (!subquery.args.pivots) {
        subquery.set('pivots', this._parsePivots());
      }
      return subquery;
    }

    let bracket = options?.parseBracket && this._parseBracket(undefined);
    bracket = bracket ? this.expression(TableExpr, { this: bracket }) : undefined;

    const rowsFrom = this._matchTextSeq('ROWS', 'FROM') && this._parseWrappedCsv(() => this._parseTable());
    const rowsFromExpr = rowsFrom ? this.expression(TableExpr, { rowsFrom }) : undefined;

    const only = this._match(TokenType.ONLY);

    const thisExpr: Expression = (
      bracket
      || rowsFromExpr
      || this._parseBracket(
        this._parseTableParts({
          schema: options?.schema,
          isDbReference: options?.isDbReference,
        }),
      )
    )!;

    if (only) {
      thisExpr.set('only', only);
    }

    // Postgres supports a wildcard (table) suffix operator, which is a no-op in this context
    this._matchTextSeq('*');

    const parsePartition = options?.parsePartition || this._constructor.SUPPORTS_PARTITION_SELECTION;
    if (parsePartition && this._match(TokenType.PARTITION, { advance: false })) {
      thisExpr.set('partition', this._parsePartition());
    }

    if (options?.schema) {
      return this._parseSchema({ thisExpr });
    }

    const version = this._parseVersion();
    if (version) {
      thisExpr.set('version', version);
    }

    if (this.dialect.ALIAS_POST_TABLESAMPLE) {
      thisExpr.set('sample', this._parseTableSample());
    }

    const alias = this._parseTableAlias({ aliasTokens: options?.aliasTokens || this._constructor.TABLE_ALIAS_TOKENS });
    if (alias) {
      thisExpr.set('alias', alias);
    }

    if (this._match(TokenType.INDEXED_BY)) {
      thisExpr.set('indexed', this._parseTableParts());
    } else if (this._matchTextSeq('NOT', 'INDEXED')) {
      thisExpr.set('indexed', false);
    }

    if (thisExpr instanceof TableExpr && this._matchTextSeq('AT')) {
      return this.expression(
        AtIndexExpr,
        {
          this: (thisExpr as any).toColumn?.({ copy: false }),
          expression: this._parseIdVar(),
        },
      );
    }

    thisExpr.set('hints', this._parseTableHints());

    if (!thisExpr.args.pivots) {
      thisExpr.set('pivots', this._parsePivots());
    }

    if (!this.dialect.ALIAS_POST_TABLESAMPLE) {
      thisExpr.set('sample', this._parseTableSample());
    }

    if (options?.joins) {
      for (const join of this._parseJoins()) {
        thisExpr.append('joins', join);
      }
    }

    if (this._matchPair(TokenType.WITH, TokenType.ORDINALITY)) {
      thisExpr.set('ordinality', true);
      thisExpr.set('alias', this._parseTableAlias());
    }

    return thisExpr;
  }

  protected _parseVersion (): VersionExpr | undefined {
    let thisText: string;
    if (this._match(TokenType.TIMESTAMP_SNAPSHOT)) {
      thisText = 'TIMESTAMP';
    } else if (this._match(TokenType.VERSION_SNAPSHOT)) {
      thisText = 'VERSION';
    } else {
      return undefined;
    }

    let kind: string;
    let expression: Expression | undefined;

    if (this._matchSet(new Set([TokenType.FROM, TokenType.BETWEEN]))) {
      kind = this._prev!.text.toUpperCase();
      const start = this._parseBitwise();
      this._matchTexts(['TO', 'AND']);
      const end = this._parseBitwise();
      expression = this.expression(TupleExpr, { expressions: [start, end] });
    } else if (this._matchTextSeq('CONTAINED', 'IN')) {
      kind = 'CONTAINED IN';
      expression = this.expression(
        TupleExpr,
        { expressions: this._parseWrappedCsv(() => this._parseBitwise()) },
      );
    } else if (this._match(TokenType.ALL)) {
      kind = 'ALL';
      expression = undefined;
    } else {
      this._matchTextSeq('AS', 'OF');
      kind = 'AS OF';
      expression = this._parseType();
    }

    return this.expression(VersionExpr, {
      this: thisText,
      expression,
      kind,
    });
  }

  protected _parseHistoricalData (): HistoricalDataExpr | undefined {
    // https://docs.snowflake.com/en/sql-reference/constructs/at-before
    const index = this._index;
    let historicalData: HistoricalDataExpr | undefined;

    if (this._matchTexts(Array.from(this._constructor.HISTORICAL_DATA_PREFIX))) {
      const thisText = this._prev!.text.toUpperCase();
      const kind = (
        this._match(TokenType.L_PAREN)
        && this._matchTexts(Array.from(this._constructor.HISTORICAL_DATA_KIND))
        && this._prev!.text.toUpperCase()
      );
      const expression = this._match(TokenType.FARROW) && this._parseBitwise();

      if (expression) {
        this._matchRParen();
        historicalData = this.expression(
          HistoricalDataExpr,
          {
            this: thisText,
            kind,
            expression,
          },
        );
      } else {
        this._retreat(index);
      }
    }

    return historicalData;
  }

  protected _parseChanges (): ChangesExpr | undefined {
    if (!this._matchTextSeq('CHANGES', '(', 'INFORMATION', '=>')) {
      return undefined;
    }

    const information = this._parseVar({ anyToken: true });
    this._matchRParen();

    return this.expression(
      ChangesExpr,
      {
        information,
        atBefore: this._parseHistoricalData(),
        end: this._parseHistoricalData(),
      },
    );
  }

  protected _parseUnnest (options?: { withAlias?: boolean }): UnnestExpr | undefined {
    if (!this._matchPair(TokenType.UNNEST, TokenType.L_PAREN, { advance: false })) {
      return undefined;
    }

    this._advance();

    const expressions = this._parseWrappedCsv(() => this._parseEquality());
    let offset: Expression | boolean | undefined = this._matchPair(TokenType.WITH, TokenType.ORDINALITY);

    const alias = (options?.withAlias ?? true) ? this._parseTableAlias() : undefined;

    if (alias) {
      if (this.dialect.UNNEST_COLUMN_ONLY) {
        const columns = alias.args.columns;
        if (columns) {
          this.raiseError('Unexpected extra column alias in unnest.', alias);
        }

        alias.set('columns', [alias.this]);
        alias.set('this', undefined);
      }

      const columns = alias.args.columns as Expression[] | undefined;
      if (offset && columns && expressions.length < columns.length) {
        offset = columns.pop()!;
      }
    }

    if (!offset && this._matchPair(TokenType.WITH, TokenType.OFFSET)) {
      this._match(TokenType.ALIAS);
      offset = this._parseIdVar({
        anyToken: false,
        tokens: this._constructor.UNNEST_OFFSET_ALIAS_TOKENS,
      }) || new IdentifierExpr({ this: 'offset' });
    }

    return this.expression(UnnestExpr, {
      expressions,
      alias,
      offset,
    });
  }

  protected _parseDerivedTableValues (): ValuesExpr | undefined {
    const isDerived = this._matchPair(TokenType.L_PAREN, TokenType.VALUES);
    if (!isDerived && !(
      // ClickHouse's `FORMAT Values` is equivalent to `VALUES`
      this._matchTextSeq('VALUES') || this._matchTextSeq('FORMAT', 'VALUES')
    )) {
      return undefined;
    }

    const expressions = this._parseCsv(() => this._parseValue());
    const alias = this._parseTableAlias();

    if (isDerived) {
      this._matchRParen();
    }

    return this.expression(
      ValuesExpr,
      {
        expressions,
        alias: alias || this._parseTableAlias(),
      },
    );
  }

  protected _parseTableSample (options?: { asModifier?: boolean }): TableSampleExpr | undefined {
    if (!this._match(TokenType.TABLE_SAMPLE) && !(
      options?.asModifier && this._matchTextSeq('USING', 'SAMPLE')
    )) {
      return undefined;
    }

    let bucketNumerator: Expression | undefined;
    let bucketDenominator: Expression | undefined;
    let bucketField: Expression | undefined;
    let percent: Expression | undefined;
    let size: Expression | undefined;
    let seed: Expression | undefined;

    let method = this._parseVar({
      tokens: new Set([TokenType.ROW]),
      upper: true,
    });
    const matchedLParen = this._match(TokenType.L_PAREN);

    let expressions: Expression[] | undefined;
    let num: Expression | undefined;

    if (this._constructor.TABLESAMPLE_CSV) {
      num = undefined;
      expressions = this._parseCsv(() => this._parsePrimary());
    } else {
      expressions = undefined;
      num = (
        this._match(TokenType.NUMBER, { advance: false })
          ? this._parseFactor()
          : this._parsePrimary() || this._parsePlaceholder()
      );
    }

    if (this._matchTextSeq('BUCKET')) {
      bucketNumerator = this._parseNumber();
      this._matchTextSeq('OUT', 'OF');
      bucketDenominator = this._parseNumber();
      this._match(TokenType.ON);
      bucketField = this._parseField();
    } else if (this._matchSet(new Set([TokenType.PERCENT, TokenType.MOD]))) {
      percent = num;
    } else if (this._match(TokenType.ROWS) || !this.dialect.TABLESAMPLE_SIZE_IS_PERCENT) {
      size = num;
    } else {
      percent = num;
    }

    if (matchedLParen) {
      this._matchRParen();
    }

    if (this._match(TokenType.L_PAREN)) {
      method = this._parseVar({ upper: true });
      seed = this._match(TokenType.COMMA) && this._parseNumber();
      this._matchRParen();
    } else if (this._matchTexts(['SEED', 'REPEATABLE'])) {
      seed = this._parseWrapped(() => this._parseNumber());
    }

    if (!method && this._constructor.DEFAULT_SAMPLING_METHOD) {
      method = varExpr(this._constructor.DEFAULT_SAMPLING_METHOD);
    }

    return this.expression(
      TableSampleExpr,
      {
        expressions,
        method,
        bucketNumerator,
        bucketDenominator,
        bucketField,
        percent,
        size,
        seed,
      },
    );
  }

  protected _parsePivots (): PivotExpr[] | undefined {
    const pivots: PivotExpr[] = [];
    let pivot = this._parsePivot();
    while (pivot) {
      pivots.push(pivot);
      pivot = this._parsePivot();
    }
    return 0 < pivots.length ? pivots : undefined;
  }

  protected _parseJoins (): JoinExpr[] {
    const joins: JoinExpr[] = [];
    let join = this._parseJoin();
    while (join) {
      joins.push(join);
      join = this._parseJoin();
    }
    return joins;
  }

  protected _parseUnpivotColumns (): UnpivotColumnsExpr | undefined {
    if (!this._match(TokenType.INTO)) {
      return undefined;
    }

    return this.expression(
      UnpivotColumnsExpr,
      {
        this: this._matchTextSeq('NAME') && this._parseColumn(),
        expressions: this._matchTextSeq('VALUE') && this._parseCsv(() => this._parseColumn()),
      },
    );
  }

  // https://duckdb.org/docs/sql/statements/pivot
  protected _parseSimplifiedPivot (isUnpivot?: boolean): PivotExpr {
    const parseOn = (): Expression | undefined => {
      const thisExpr = this._parseBitwise();

      if (this._match(TokenType.IN)) {
        // PIVOT ... ON col IN (row_val1, row_val2)
        return this._parseIn(thisExpr);
      }
      if (this._match(TokenType.ALIAS, { advance: false })) {
        // UNPIVOT ... ON (col1, col2, col3) AS row_val
        return this._parseAlias(thisExpr);
      }

      return thisExpr;
    };

    const thisExpr = this._parseTable();
    const expressions = this._match(TokenType.ON) && this._parseCsv(parseOn);
    const into = this._parseUnpivotColumns();
    const using = this._match(TokenType.USING) && this._parseCsv(() =>
      this._parseAlias(this._parseColumn()));
    const group = this._parseGroup();

    return this.expression(
      PivotExpr,
      {
        this: thisExpr,
        expressions,
        using,
        group,
        unpivot: isUnpivot,
        into,
      },
    );
  }

  protected _parsePivotIn (): InExpr {
    const parseAliasedExpression = (): Expression | undefined => {
      const thisExpr = this._parseSelectOrExpression();

      this._match(TokenType.ALIAS);
      const alias = this._parseBitwise();
      if (alias) {
        let aliasExpr = alias;
        if (alias instanceof ColumnExpr && !alias.args.db) {
          aliasExpr = alias.this;
        }
        return this.expression(PivotAliasExpr, {
          this: thisExpr,
          alias: aliasExpr,
        });
      }

      return thisExpr;
    };

    const value = this._parseColumn();

    if (!this._match(TokenType.IN)) {
      this.raiseError('Expecting IN', this._curr);
    }

    let exprs: Expression[];
    let field: Expression | undefined;

    if (this._match(TokenType.L_PAREN)) {
      if (this._match(TokenType.ANY)) {
        exprs = ensureList(new PivotAnyExpr({ this: this._parseOrder() }));
      } else {
        exprs = this._parseCsv(parseAliasedExpression);
      }
      this._matchRParen();
      return this.expression(InExpr, {
        this: value,
        expressions: exprs,
      });
    }

    field = this._parseIdVar();
    return this.expression(InExpr, {
      this: value,
      field,
    });
  }

  protected _parsePivotAggregation (): Expression | undefined {
    const func = this._parseFunction();
    if (!func) {
      if (this._prev && this._prev.tokenType === TokenType.COMMA) {
        return undefined;
      }
      this.raiseError('Expecting an aggregation function in PIVOT', this._curr);
    }

    return this._parseAlias(func);
  }

  protected _parsePivot (): PivotExpr | undefined {
    const index = this._index;
    let includeNulls: boolean | undefined;
    let unpivot: boolean;

    if (this._match(TokenType.PIVOT)) {
      unpivot = false;
    } else if (this._match(TokenType.UNPIVOT)) {
      unpivot = true;

      // https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-unpivot.html#syntax
      if (this._matchTextSeq('INCLUDE', 'NULLS')) {
        includeNulls = true;
      } else if (this._matchTextSeq('EXCLUDE', 'NULLS')) {
        includeNulls = false;
      }
    } else {
      return undefined;
    }

    if (!this._match(TokenType.L_PAREN)) {
      this._retreat(index);
      return undefined;
    }

    let expressions: Expression[];
    if (unpivot) {
      expressions = this._parseCsv(() => this._parseColumn());
    } else {
      expressions = this._parseCsv(() => this._parsePivotAggregation());
    }

    if (expressions.length === 0) {
      this.raiseError('Failed to parse PIVOT\'s aggregation list', this._curr);
    }

    if (!this._match(TokenType.FOR)) {
      this.raiseError('Expecting FOR', this._curr);
    }

    const fields: InExpr[] = [];
    while (true) {
      const field = this._tryParse(() => this._parsePivotIn());
      if (!field) {
        break;
      }
      fields.push(field);
    }

    const defaultOnNull = this._matchTextSeq('DEFAULT', 'ON', 'NULL') && this._parseWrapped(() =>
      this._parseBitwise());

    const group = this._parseGroup();

    this._matchRParen();

    const pivot = this.expression(
      PivotExpr,
      {
        expressions,
        fields,
        unpivot,
        includeNulls,
        defaultOnNull,
        group,
      },
    );

    if (!this._matchSet(new Set([TokenType.PIVOT, TokenType.UNPIVOT]), { advance: false })) {
      pivot.set('alias', this._parseTableAlias());
    }

    if (!unpivot) {
      const names = this._pivotColumnNames(expressions);

      const columns: Expression[] = [];
      const allFields: string[][] = [];

      for (const pivotField of pivot.args.fields as InExpr[]) {
        const pivotFieldExpressions = pivotField.expressions;

        // The `PivotAny` expression corresponds to `ANY ORDER BY <column>`; we can't infer in this case.
        if (pivotFieldExpressions?.[0] instanceof PivotAnyExpr) {
          continue;
        }

        if (pivotFieldExpressions) {
          allFields.push(
            pivotFieldExpressions.map((fld: Expression) =>
              this._constructor.IDENTIFY_PIVOT_STRINGS ? fld.sql() : (fld as any).aliasOrName || ''),
          );
        }
      }

      if (0 < allFields.length) {
        if (0 < names.length) {
          allFields.push(names);
        }

        // Generate all possible combinations of the pivot columns
        // e.g PIVOT(sum(...) as total FOR year IN (2000, 2010) FOR country IN ('NL', 'US'))
        // generates the product between [[2000, 2010], ['NL', 'US'], ['total']]
        for (const fldPartsTuple of this._product(allFields)) {
          const fldParts = [...fldPartsTuple];

          if (0 < names.length && this._constructor.PREFIXED_PIVOT_COLUMNS) {
            // Move the "name" to the front of the list
            fldParts.unshift(fldParts.pop()!);
          }

          columns.push(new IdentifierExpr({ this: fldParts.join('_') }));
        }
      }

      pivot.set('columns', columns);
    }

    return pivot;
  }

  protected _pivotColumnNames (aggregations: Expression[]): string[] {
    return aggregations.map((agg) => (agg as any).alias).filter((alias: any) => alias);
  }

  // Helper method for generating cartesian product (like Python's itertools.product)
  protected _product<T>(arrays: T[][]): T[][] {
    if (arrays.length === 0) return [[]];
    if (arrays.length === 1) return arrays[0].map((item) => [item]);

    const result: T[][] = [];
    const [first, ...rest] = arrays;
    const restProduct = this._product(rest);

    for (const item of first) {
      for (const restItems of restProduct) {
        result.push([item, ...restItems]);
      }
    }

    return result;
  }

  protected _parsePrewhere (options?: { skipWhereToken?: boolean }): PreWhereExpr | undefined {
    if (!options?.skipWhereToken && !this._match(TokenType.PREWHERE)) {
      return undefined;
    }

    return this.expression(
      PreWhereExpr,
      {
        comments: this._prevComments,
        this: this._parseDisjunction(),
      },
    );
  }

  protected _parseWhere (options?: { skipWhereToken?: boolean }): WhereExpr | undefined {
    if (!options?.skipWhereToken && !this._match(TokenType.WHERE)) {
      return undefined;
    }

    return this.expression(
      WhereExpr,
      {
        comments: this._prevComments,
        this: this._parseDisjunction(),
      },
    );
  }

  protected _parseGroup (options?: { skipGroupByToken?: boolean }): GroupExpr | undefined {
    if (!options?.skipGroupByToken && !this._match(TokenType.GROUP_BY)) {
      return undefined;
    }
    const comments = this._prevComments;

    const elements: {
      expressions: Expression[];
      rollup: RollupExpr[];
      cube: CubeExpr[];
      groupingSets: GroupingSetsExpr[];
      all?: boolean;
      totals?: boolean;
    } = {
      expressions: [],
      rollup: [],
      cube: [],
      groupingSets: [],
    };

    if (this._match(TokenType.ALL)) {
      elements.all = true;
    } else if (this._match(TokenType.DISTINCT)) {
      elements.all = false;
    }

    if (this._matchSet(this._constructor.QUERY_MODIFIER_TOKENS, { advance: false })) {
      return this.expression(GroupExpr, {
        comments,
        ...elements,
      });
    }

    while (true) {
      const index = this._index;

      elements.expressions.push(
        ...this._parseCsv(() =>
          this._matchSet(new Set([TokenType.CUBE, TokenType.ROLLUP]), { advance: false })
            ? undefined
            : this._parseDisjunction()),
      );

      const beforeWithIndex = this._index;
      const withPrefix = this._match(TokenType.WITH);

      const cubeOrRollup = this._parseCubeOrRollup({ withPrefix });
      if (cubeOrRollup) {
        const key = cubeOrRollup instanceof RollupExpr ? 'rollup' : 'cube';
        elements[key].push(cubeOrRollup);
      } else {
        const groupingSets = this._parseGroupingSets();
        if (groupingSets) {
          elements.groupingSets.push(groupingSets);
        } else if (this._matchTextSeq('TOTALS')) {
          elements.totals = true;
        }
      }

      if (beforeWithIndex <= this._index && this._index <= beforeWithIndex + 1) {
        this._retreat(beforeWithIndex);
        break;
      }

      if (index === this._index) {
        break;
      }
    }

    return this.expression(GroupExpr, {
      comments,
      ...elements,
    });
  }

  protected _parseCubeOrRollup (options?: { withPrefix?: boolean }): CubeExpr | RollupExpr | undefined {
    let kind: typeof CubeExpr | typeof RollupExpr;

    if (this._match(TokenType.CUBE)) {
      kind = CubeExpr;
    } else if (this._match(TokenType.ROLLUP)) {
      kind = RollupExpr;
    } else {
      return undefined;
    }

    return this.expression(
      kind,
      { expressions: options?.withPrefix ? [] : this._parseWrappedCsv(() => this._parseBitwise()) },
    );
  }

  protected _parseGroupingSets (): GroupingSetsExpr | undefined {
    if (this._match(TokenType.GROUPING_SETS)) {
      return this.expression(
        GroupingSetsExpr,
        { expressions: this._parseWrappedCsv(() => this._parseGroupingSet()) },
      );
    }
    return undefined;
  }

  protected _parseGroupingSet (): Expression | undefined {
    return this._parseGroupingSets() || this._parseCubeOrRollup() || this._parseBitwise();
  }

  protected _parseHaving (options?: { skipHavingToken?: boolean }): HavingExpr | undefined {
    if (!options?.skipHavingToken && !this._match(TokenType.HAVING)) {
      return undefined;
    }
    return this.expression(
      HavingExpr,
      {
        comments: this._prevComments,
        this: this._parseDisjunction(),
      },
    );
  }

  protected _parseQualify (): QualifyExpr | undefined {
    if (!this._match(TokenType.QUALIFY)) {
      return undefined;
    }
    return this.expression(QualifyExpr, { this: this._parseDisjunction() });
  }

  protected _parseConnectWithPrior (): Expression | undefined {
    this._constructor.NO_PAREN_FUNCTION_PARSERS.set('PRIOR', () =>
      this.expression(PriorExpr, { this: this._parseBitwise() }));
    const connect = this._parseDisjunction();
    this._constructor.NO_PAREN_FUNCTION_PARSERS.delete('PRIOR');
    return connect;
  }

  protected _parseConnect (options?: { skipStartToken?: boolean }): ConnectExpr | undefined {
    let start: Expression | undefined;

    if (options?.skipStartToken) {
      start = undefined;
    } else if (this._match(TokenType.START_WITH)) {
      start = this._parseDisjunction();
    } else {
      return undefined;
    }

    this._match(TokenType.CONNECT_BY);
    const nocycle = this._matchTextSeq('NOCYCLE');
    const connect = this._parseConnectWithPrior();

    if (!start && this._match(TokenType.START_WITH)) {
      start = this._parseDisjunction();
    }

    return this.expression(ConnectExpr, {
      start,
      connect,
      nocycle,
    });
  }

  protected _parseNameAsExpression (): Expression | undefined {
    let thisExpr: Expression | undefined = this._parseIdVar({ anyToken: true });
    if (this._match(TokenType.ALIAS)) {
      thisExpr = this.expression(AliasExpr, {
        alias: thisExpr,
        this: this._parseDisjunction(),
      });
    }
    return thisExpr;
  }

  protected _parseInterpolate (): Expression[] | undefined {
    if (this._matchTextSeq('INTERPOLATE')) {
      return this._parseWrappedCsv(() => this._parseNameAsExpression());
    }
    return undefined;
  }

  protected _parseOrder (
    thisExpr?: Expression,
    options?: { skipOrderToken?: boolean },
  ): OrderExpr | undefined {
    let siblings: boolean | undefined;

    if (!options?.skipOrderToken && !this._match(TokenType.ORDER_BY)) {
      if (!this._match(TokenType.ORDER_SIBLINGS_BY)) {
        return thisExpr as any;
      }

      siblings = true;
    }

    return this.expression(
      OrderExpr,
      {
        comments: this._prevComments,
        this: thisExpr,
        expressions: this._parseCsv(() => this._parseOrdered()),
        siblings,
      },
    );
  }

  protected _parseSort<E extends Expression> (
    expClass: new (args: Record<string, any>) => E,
    token: TokenType,
  ): E | undefined {
    if (!this._match(token)) {
      return undefined;
    }
    return this.expression(expClass, { expressions: this._parseCsv(() => this._parseOrdered()) });
  }

  protected _parseOrdered (parseMethod?: () => Expression | undefined): OrderedExpr | undefined {
    const thisExpr = parseMethod ? parseMethod() : this._parseDisjunction();
    if (!thisExpr) {
      return undefined;
    }

    let orderedThis = thisExpr;
    if ((thisExpr as any).name?.toUpperCase() === 'ALL' && this.dialect.SUPPORTS_ORDER_BY_ALL) {
      orderedThis = varExpr('ALL');
    }

    const asc = this._match(TokenType.ASC);
    const desc = this._match(TokenType.DESC) || (asc && false);

    const isNullsFirst = this._matchTextSeq('NULLS', 'FIRST');
    const isNullsLast = this._matchTextSeq('NULLS', 'LAST');

    let nullsFirst = isNullsFirst || false;
    const explicitlyNullOrdered = isNullsFirst || isNullsLast;

    if (
      !explicitlyNullOrdered
      && (
        (!desc && this.dialect.NULL_ORDERING === 'nulls_are_small')
        || (desc && this.dialect.NULL_ORDERING !== 'nulls_are_small')
      )
      && this.dialect.NULL_ORDERING !== 'nulls_are_last'
    ) {
      nullsFirst = true;
    }

    let withFill: WithFillExpr | undefined;
    if (this._matchTextSeq('WITH', 'FILL')) {
      withFill = this.expression(
        WithFillExpr,
        {
          from: this._match(TokenType.FROM) && this._parseBitwise(),
          to: this._matchTextSeq('TO') && this._parseBitwise(),
          step: this._matchTextSeq('STEP') && this._parseBitwise(),
          interpolate: this._parseInterpolate(),
        },
      );
    }

    return this.expression(
      OrderedExpr,
      {
        this: orderedThis,
        desc,
        nullsFirst,
        withFill,
      },
    );
  }

  protected _parseLimitOptions (): LimitOptionsExpr | undefined {
    const percent = this._matchSet(new Set([TokenType.PERCENT, TokenType.MOD]));
    const rows = this._matchSet(new Set([TokenType.ROW, TokenType.ROWS]));
    this._matchTextSeq('ONLY');
    const withTies = this._matchTextSeq('WITH', 'TIES');

    if (!(percent || rows || withTies)) {
      return undefined;
    }

    return this.expression(LimitOptionsExpr, {
      percent,
      rows,
      withTies,
    });
  }

  protected _parseLimit (
    thisExpr?: Expression,
    options?: { top?: boolean;
      skipLimitToken?: boolean; },
  ): Expression | undefined {
    if (options?.skipLimitToken || this._match(options?.top ? TokenType.TOP : TokenType.LIMIT)) {
      const comments = this._prevComments;
      let expression: Expression | undefined;

      if (options?.top) {
        const limitParen = this._match(TokenType.L_PAREN);
        expression = limitParen ? this._parseTerm() : this._parseNumber();

        if (limitParen) {
          this._matchRParen();
        }
      } else {
        // Parsing LIMIT x% (i.e x PERCENT) as a term leads to an error, since
        // we try to build an exp.Mod expr. For that matter, we backtrack and instead
        // consume the factor plus parse the percentage separately
        const index = this._index;
        expression = this._tryParse(() => this._parseTerm());
        if (expression instanceof ModExpr) {
          this._retreat(index);
          expression = this._parseFactor();
        } else if (!expression) {
          expression = this._parseFactor();
        }
      }

      const limitOptions = this._parseLimitOptions();

      let offset: Expression | undefined;
      if (this._match(TokenType.COMMA)) {
        offset = expression;
        expression = this._parseTerm();
      }

      const limitExp = this.expression(
        LimitExpr,
        {
          this: thisExpr,
          expression,
          offset,
          comments,
          limitOptions,
          expressions: this._parseLimitBy(),
        },
      );

      return limitExp;
    }

    if (this._match(TokenType.FETCH)) {
      const direction = this._matchSet(new Set([TokenType.FIRST, TokenType.NEXT]));
      const directionText = direction ? this._prev!.text.toUpperCase() : 'FIRST';

      const count = this._parseField({ tokens: this._constructor.FETCH_TOKENS });

      return this.expression(
        FetchExpr,
        {
          direction: directionText,
          count,
          limitOptions: this._parseLimitOptions(),
        },
      );
    }

    return thisExpr;
  }

  protected _parseOffset (thisExpr?: Expression): Expression | undefined {
    if (!this._match(TokenType.OFFSET)) {
      return thisExpr;
    }

    const count = this._parseTerm();
    this._matchSet(new Set([TokenType.ROW, TokenType.ROWS]));

    return this.expression(
      OffsetExpr,
      {
        this: thisExpr,
        expression: count,
        expressions: this._parseLimitBy(),
      },
    );
  }

  protected _canParseLimitOrOffset (): boolean {
    if (!this._matchSet(this._constructor.AMBIGUOUS_ALIAS_TOKENS, { advance: false })) {
      return false;
    }

    const index = this._index;
    const result = !!(
      this._tryParse(() => this._parseLimit(), true)
      || this._tryParse(() => this._parseOffset(), true)
    );
    this._retreat(index);

    // MATCH_CONDITION (...) is a special construct that should not be consumed by limit/offset
    if (this._next && this._next.tokenType === TokenType.MATCH_CONDITION) {
      return false;
    }

    return result;
  }

  protected _parseLimitBy (): Expression[] | undefined {
    return this._matchTextSeq('BY') && this._parseCsv(() => this._parseBitwise());
  }

  protected _parseLocks (): LockExpr[] {
    const locks: LockExpr[] = [];

    while (true) {
      let update: boolean | undefined;
      let key: boolean | undefined;

      if (this._matchTextSeq('FOR', 'UPDATE')) {
        update = true;
      } else if (this._matchTextSeq('FOR', 'SHARE') || this._matchTextSeq('LOCK', 'IN', 'SHARE', 'MODE')) {
        update = false;
      } else if (this._matchTextSeq('FOR', 'KEY', 'SHARE')) {
        update = false;
        key = true;
      } else if (this._matchTextSeq('FOR', 'NO', 'KEY', 'UPDATE')) {
        update = true;
        key = true;
      } else {
        break;
      }

      let expressions: Expression[] | undefined;
      if (this._matchTextSeq('OF')) {
        expressions = this._parseCsv(() => this._parseTable({ schema: true }));
      }

      let wait: boolean | Expression | undefined;
      if (this._matchTextSeq('NOWAIT')) {
        wait = true;
      } else if (this._matchTextSeq('WAIT')) {
        wait = this._parsePrimary();
      } else if (this._matchTextSeq('SKIP', 'LOCKED')) {
        wait = false;
      }

      locks.push(
        this.expression(
          LockExpr,
          {
            update,
            expressions,
            wait,
            key,
          },
        ),
      );
    }

    return locks;
  }

  protected parseSetOperation (
    thisExpr?: Expression,
    options?: { consumePipe?: boolean },
  ): Expression | undefined {
    const start = this._index;
    const [
      , sideToken,
      kindToken,
    ] = this._parseJoinParts();

    const side = sideToken?.text;
    const kind = kindToken?.text;

    if (!this._matchSet(this._constructor.SET_OPERATIONS)) {
      this._retreat(start);
      return undefined;
    }

    const tokenType = this._prev!.tokenType;

    let operation: typeof UnionExpr | typeof ExceptExpr | typeof IntersectExpr;
    if (tokenType === TokenType.UNION) {
      operation = UnionExpr;
    } else if (tokenType === TokenType.EXCEPT) {
      operation = ExceptExpr;
    } else {
      operation = IntersectExpr;
    }

    const comments = this._prev!.comments;

    let distinct: boolean | undefined;
    if (this._match(TokenType.DISTINCT)) {
      distinct = true;
    } else if (this._match(TokenType.ALL)) {
      distinct = false;
    } else {
      distinct = this.dialect.SET_OP_DISTINCT_BY_DEFAULT[operation];
      if (distinct === undefined) {
        this.raiseError(`Expected DISTINCT or ALL for ${operation.name}`, this._curr);
      }
    }

    let byName = this._matchTextSeq('BY', 'NAME') || this._matchTextSeq('STRICT', 'CORRESPONDING');
    if (this._matchTextSeq('CORRESPONDING')) {
      byName = true;
      if (!side && !kind) {
        // Set default kind
        // kind = 'INNER';  // Uncomment if needed
      }
    }

    let onColumnList: Expression[] | undefined;
    if (byName && this._matchTexts(['ON', 'BY'])) {
      onColumnList = this._parseWrappedCsv(() => this._parseColumn());
    }

    const expression = this._parseSelect({
      nested: true,
      parseSetOperation: false,
      consumePipe: options?.consumePipe,
    });

    return this.expression(
      operation,
      {
        comments,
        this: thisExpr,
        distinct,
        byName,
        expression,
        side,
        kind,
        on: onColumnList,
      },
    );
  }

  protected _parseSetOperations (thisExpr?: Expression): Expression | undefined {
    let current = thisExpr;

    while (current) {
      const setop = this.parseSetOperation(current);
      if (!setop) {
        break;
      }
      current = setop;
    }

    if (current instanceof SetOperationExpr && this._constructor.MODIFIERS_ATTACHED_TO_SET_OP) {
      const expression = current.args.expression as Expression | undefined;

      if (expression) {
        for (const arg of this._constructor.SET_OP_MODIFIERS) {
          const expr = expression.args[arg];
          if (expr instanceof Expression) {
            current.set(arg, expr.pop());
          }
        }
      }
    }

    return current;
  }

  protected _parseExpression (): Expression | undefined {
    return this._parseAlias(this._parseAssignment());
  }

  protected _parseAssignment (): Expression | undefined {
    let thisExpr: Expression | undefined = this._parseDisjunction();

    if (!thisExpr && this._next && this._constructor.ASSIGNMENT.has(this._next.tokenType)) {
      // This allows us to parse <non-identifier token> := <expr>
      this._advanceAny({ ignoreReserved: true });
      thisExpr = new ColumnExpr({ this: this._prev!.text });
    }

    while (this._matchSet(this._constructor.ASSIGNMENT)) {
      if (thisExpr instanceof ColumnExpr && thisExpr.parts.length === 1) {
        thisExpr = thisExpr.this;
      }

      const ExprClass = this._constructor.ASSIGNMENT.get(this._prev!.tokenType);
      if (ExprClass) {
        thisExpr = this.expression(
          ExprClass,
          {
            this: thisExpr,
            comments: this._prevComments,
            expression: this._parseAssignment(),
          },
        );
      }
    }

    return thisExpr;
  }

  protected _parseDisjunction (): Expression | undefined {
    return this._parseTokens(() => this._parseConjunction(), this._constructor.DISJUNCTION);
  }

  protected _parseConjunction (): Expression | undefined {
    return this._parseTokens(() => this._parseEquality(), this._constructor.CONJUNCTION);
  }

  protected _parseEquality (): Expression | undefined {
    return this._parseTokens(() => this._parseComparison(), this._constructor.EQUALITY);
  }

  protected _parseComparison (): Expression | undefined {
    return this._parseTokens(() => this._parseRange(), this._constructor.COMPARISON);
  }

  protected _parseRange (thisExpr?: Expression): Expression | undefined {
    let current = thisExpr || this._parseBitwise();
    const negate = this._match(TokenType.NOT);

    if (this._matchSet(this._constructor.RANGE_PARSERS)) {
      const parser = this._constructor.RANGE_PARSERS.get(this._prev!.tokenType);
      if (parser) {
        const expression = parser(this, current);
        if (!expression) {
          return current;
        }
        current = expression;
      }
    } else if (this._match(TokenType.ISNULL) || (negate && this._match(TokenType.NULL))) {
      current = this.expression(IsExpr, {
        this: current,
        expression: new NullExpr({}),
      });
    }

    // Postgres supports ISNULL and NOTNULL for conditions.
    // https://blog.andreiavram.ro/postgresql-null-composite-type/
    if (this._match(TokenType.NOTNULL)) {
      current = this.expression(IsExpr, {
        this: current,
        expression: new NullExpr({}),
      });
      current = this.expression(NotExpr, { this: current });
    }

    if (negate) {
      current = this._negateRange(current);
    }

    if (this._match(TokenType.IS)) {
      current = this._parseIs(current);
    }

    return current;
  }

  protected _negateRange (thisExpr?: Expression): Expression | undefined {
    if (!thisExpr) {
      return thisExpr;
    }

    return this.expression(NotExpr, { this: thisExpr });
  }

  protected _parseIs (thisExpr?: Expression): Expression | undefined {
    const index = this._index - 1;
    const negate = this._match(TokenType.NOT);

    if (this._matchTextSeq('DISTINCT', 'FROM')) {
      const klass = negate ? NullSafeEQExpr : NullSafeNEQExpr;
      return this.expression(klass, {
        this: thisExpr,
        expression: this._parseBitwise(),
      });
    }

    let expression: Expression | undefined;
    if (this._match(TokenType.JSON)) {
      const kind = this._matchTexts(Array.from(this._constructor.IS_JSON_PREDICATE_KIND)) && this._prev!.text.toUpperCase();

      let with_: boolean | undefined;
      if (this._matchTextSeq('WITH')) {
        with_ = true;
      } else if (this._matchTextSeq('WITHOUT')) {
        with_ = false;
      }

      const unique = this._match(TokenType.UNIQUE);
      this._matchTextSeq('KEYS');

      expression = this.expression(
        JSONExpr,
        {
          this: kind,
          with: with_,
          unique,
        },
      );
    } else {
      expression = this._parseNull() || this._parseBitwise();
      if (!expression) {
        this._retreat(index);
        return undefined;
      }
    }

    let result = this.expression(IsExpr, {
      this: thisExpr,
      expression,
    });
    if (negate) {
      result = this.expression(NotExpr, { this: result });
    }
    return this._parseColumnOps(result);
  }

  protected _parseIn (thisExpr?: Expression, options?: { alias?: boolean }): InExpr {
    const unnest = this._parseUnnest({ withAlias: false });
    let result: InExpr;

    if (unnest) {
      result = this.expression(InExpr, {
        this: thisExpr,
        unnest,
      });
    } else if (this._matchSet(new Set([TokenType.L_PAREN, TokenType.L_BRACKET]))) {
      const matchedLParen = this._prev!.tokenType === TokenType.L_PAREN;
      const expressions = this._parseCsv(() => this._parseSelectOrExpression({ alias: options?.alias }));

      if (expressions.length === 1 && expressions[0] instanceof QueryExpr) {
        const query = expressions[0] as QueryExpr;
        result = this.expression(
          InExpr,
          {
            this: thisExpr,
            query: this._parseQueryModifiers(query).subquery({ copy: false }) as SubqueryExpr,
          },
        );
      } else {
        result = this.expression(InExpr, {
          this: thisExpr,
          expressions,
        });
      }

      if (matchedLParen) {
        this._matchRParen(result);
      } else if (!this._match(TokenType.R_BRACKET, { expression: result })) {
        this.raiseError('Expecting ]', this._curr);
      }
    } else {
      result = this.expression(InExpr, {
        this: thisExpr,
        field: this._parseColumn(),
      });
    }

    return result;
  }

  protected _parseBetween (thisExpr?: Expression): BetweenExpr {
    let symmetric: boolean | undefined;
    if (this._matchTextSeq('SYMMETRIC')) {
      symmetric = true;
    } else if (this._matchTextSeq('ASYMMETRIC')) {
      symmetric = false;
    }

    const low = this._parseBitwise();
    this._match(TokenType.AND);
    const high = this._parseBitwise();

    return this.expression(
      BetweenExpr,
      {
        this: thisExpr,
        low,
        high,
        symmetric,
      },
    );
  }

  protected _parseEscape (thisExpr?: Expression): Expression | undefined {
    if (!this._match(TokenType.ESCAPE)) {
      return thisExpr;
    }
    return this.expression(
      EscapeExpr,
      {
        this: thisExpr,
        expression: this._parseString() || this._parseNull(),
      },
    );
  }

  protected _parseIntervalSpan (thisExpr: Expression): IntervalExpr {
    // handle day-time format interval span with omitted units:
    //   INTERVAL '<number days> hh[:][mm[:ss[.ff]]]' <maybe `unit TO unit`>
    let intervalSpanUnitsOmitted: boolean | undefined;

    if (
      thisExpr
      && (thisExpr as any).isString
      && this._constructor.SUPPORTS_OMITTED_INTERVAL_SPAN_UNIT
      && (thisExpr as any).name?.match?.(this._constructor.INTERVAL_DAY_TIME_RE)
    ) {
      const index = this._index;

      // Var "TO" Var
      const firstUnit = this._parseVar({
        anyToken: true,
        upper: true,
      });
      let secondUnit: VarExpr | undefined;
      if (firstUnit && this._matchTextSeq('TO')) {
        secondUnit = this._parseVar({
          anyToken: true,
          upper: true,
        });
      }

      intervalSpanUnitsOmitted = !(firstUnit && secondUnit);

      this._retreat(index);
    }

    let unit: Expression | undefined = intervalSpanUnitsOmitted
      ? undefined
      : (
        this._parseFunction()
        || (
          !this._match(TokenType.ALIAS, { advance: false })
          && this._parseVar({
            anyToken: true,
            upper: true,
          })
        )
      );

    // Most dialects support, e.g., the form INTERVAL '5' day, thus we try to parse
    // each INTERVAL expression into this canonical form so it's easy to transpile
    let finalThis = thisExpr;
    if (thisExpr && (thisExpr as any).isNumber) {
      finalThis = LiteralExpr.string((thisExpr as any).toPy?.() || thisExpr.sql());
    } else if (thisExpr && (thisExpr as any).isString) {
      const parts = (thisExpr as any).name?.match?.(this._constructor.INTERVAL_STRING_RE);
      if (parts && unit) {
        // Unconsume the eagerly-parsed unit, since the real unit was part of the string
        unit = undefined;
        this._retreat(this._index - 1);
      }

      if (parts && parts.length === 2) {
        finalThis = LiteralExpr.string(parts[0]);
        unit = this.expression(VarExpr, { this: parts[1].toUpperCase() });
      }
    }

    if (this._constructor.INTERVAL_SPANS && this._matchTextSeq('TO')) {
      unit = this.expression(
        IntervalSpanExpr,
        {
          this: unit,
          expression: this._parseFunction() || this._parseVar({
            anyToken: true,
            upper: true,
          }),
        },
      );
    }

    return this.expression(IntervalExpr, {
      this: finalThis,
      unit,
    });
  }

  protected _parseInterval (options?: { matchInterval?: boolean }): AddExpr | IntervalExpr | undefined {
    const index = this._index;

    if (!this._match(TokenType.INTERVAL) && (options?.matchInterval ?? true)) {
      return undefined;
    }

    let thisExpr: Expression | undefined;
    if (this._match(TokenType.STRING, { advance: false })) {
      thisExpr = this._parsePrimary();
    } else {
      thisExpr = this._parseTerm();
    }

    if (!thisExpr || (
      thisExpr instanceof ColumnExpr
      && !thisExpr.args.table
      && !(thisExpr.this as IdentifierExpr)?.quoted
      && this._curr
      && !this.dialect.VALID_INTERVAL_UNITS.has(this._curr.text.toUpperCase())
    )) {
      this._retreat(index);
      return undefined;
    }

    const interval = this._parseIntervalSpan(thisExpr);

    const index2 = this._index;
    this._match(TokenType.PLUS);

    // Convert INTERVAL 'val_1' unit_1 [+] ... [+] 'val_n' unit_n into a sum of intervals
    if (this._matchSet(new Set([TokenType.STRING, TokenType.NUMBER]), { advance: false })) {
      return this.expression(
        AddExpr,
        {
          this: interval,
          expression: this._parseInterval({ matchInterval: false }),
        },
      );
    }

    this._retreat(index2);
    return interval;
  }

  protected _parseBitwise (): Expression | undefined {
    let thisExpr = this._parseTerm();

    while (true) {
      if (this._matchSet(this._constructor.BITWISE)) {
        const ExprClass = this._constructor.BITWISE.get(this._prev!.tokenType);
        if (ExprClass) {
          thisExpr = this.expression(
            ExprClass,
            {
              this: thisExpr,
              expression: this._parseTerm(),
            },
          );
        }
      } else if (this.dialect.DPIPE_IS_STRING_CONCAT && this._match(TokenType.DPIPE)) {
        thisExpr = this.expression(
          DPipeExpr,
          {
            this: thisExpr,
            expression: this._parseTerm(),
            safe: !this.dialect.STRICT_STRING_CONCAT,
          },
        );
      } else if (this._match(TokenType.DQMARK)) {
        thisExpr = this.expression(
          CoalesceExpr,
          {
            this: thisExpr,
            expressions: ensureList(this._parseTerm()),
          },
        );
      } else if (this._matchPair(TokenType.LT, TokenType.LT)) {
        thisExpr = this.expression(
          BitwiseLeftShiftExpr,
          {
            this: thisExpr,
            expression: this._parseTerm(),
          },
        );
      } else if (this._matchPair(TokenType.GT, TokenType.GT)) {
        thisExpr = this.expression(
          BitwiseRightShiftExpr,
          {
            this: thisExpr,
            expression: this._parseTerm(),
          },
        );
      } else {
        break;
      }
    }

    return thisExpr;
  }

  protected _parseTerm (): Expression | undefined {
    let thisExpr = this._parseFactor();

    while (this._matchSet(this._constructor.TERM)) {
      const klass = this._constructor.TERM.get(this._prev!.tokenType);
      const comments = this._prevComments;
      const expression = this._parseFactor();

      if (klass) {
        thisExpr = this.expression(klass, {
          this: thisExpr,
          comments,
          expression,
        });

        if (thisExpr instanceof CollateExpr) {
          const expr = thisExpr.args.expression as Expression;

          // Preserve collations such as pg_catalog."default" (Postgres) as columns, otherwise
          // fallback to Identifier / Var
          if (expr instanceof ColumnExpr && expr.parts.length === 1) {
            const ident = expr.this;
            if (ident instanceof IdentifierExpr) {
              thisExpr.set('expression', ident.quoted ? ident : varExpr(ident.name));
            }
          }
        }
      }
    }

    return thisExpr;
  }

  protected _parseFactor (): Expression | undefined {
    const parseMethod = this._constructor.EXPONENT ? () => this._parseExponent() : () => this._parseUnary();
    let thisExpr = this._parseAtTimeZone(parseMethod());

    while (this._matchSet(this._constructor.FACTOR)) {
      const klass = this._constructor.FACTOR.get(this._prev!.tokenType);
      const comments = this._prevComments;
      const expression = parseMethod();

      if (!expression && klass === IntDivExpr && /^[a-zA-Z]/.test(this._prev!.text)) {
        this._retreat(this._index - 1);
        return thisExpr;
      }

      if (klass) {
        thisExpr = this.expression(klass, {
          this: thisExpr,
          comments,
          expression,
        });

        if (thisExpr instanceof DivExpr) {
          thisExpr.set('typed', this.dialect.TYPED_DIVISION);
          thisExpr.set('safe', this.dialect.SAFE_DIVISION);
        }
      }
    }

    return thisExpr;
  }

  protected _parseExponent (): Expression | undefined {
    return this._parseTokens(() => this._parseUnary(), this._constructor.EXPONENT);
  }

  protected _parseUnary (): Expression | undefined {
    if (this._matchSet(this._constructor.UNARY_PARSERS)) {
      const parser = this._constructor.UNARY_PARSERS.get(this._prev!.tokenType);
      return parser ? parser(this) : undefined;
    }
    return this._parseType();
  }

  protected _parseType (options?: {
    parseInterval?: boolean;
    fallbackToIdentifier?: boolean;
  }): Expression | undefined {
    const parseInterval = options?.parseInterval ?? true;
    const fallbackToIdentifier = options?.fallbackToIdentifier ?? false;

    const interval = parseInterval && this._parseInterval();
    if (interval) {
      return this._parseColumnOps(interval);
    }

    const index = this._index;
    const dataType = this._parseTypes({
      checkFunc: true,
      allowIdentifiers: false,
    });

    // parse_types() returns a Cast if we parsed BQ's inline constructor <type>(<values>) e.g.
    // STRUCT<a INT, b STRING>(1, 'foo'), which is canonicalized to CAST(<values> AS <type>)
    if (dataType instanceof CastExpr) {
      // This constructor can contain ops directly after it, for instance struct unnesting:
      // STRUCT<a INT, b STRING>(1, 'foo').* --> CAST(STRUCT(1, 'foo') AS STRUCT<a iNT, b STRING).*
      return this._parseColumnOps(dataType);
    }

    if (dataType) {
      const index2 = this._index;
      const thisExpr = this._parsePrimary();

      if (thisExpr instanceof LiteralExpr) {
        const literal = (thisExpr as any).name;
        const thisWithOps = this._parseColumnOps(thisExpr);

        const parser = this._constructor.TYPE_LITERAL_PARSERS.get((dataType as DataTypeExpr).this as any);
        if (parser) {
          return parser(this, thisWithOps, dataType as DataTypeExpr);
        }

        if (
          this._constructor.ZONE_AWARE_TIMESTAMP_CONSTRUCTOR
          && (dataType as DataTypeExpr).isType?.(DataTypeExprKind.TIMESTAMP)
          && this._constructor.TIME_ZONE_RE?.test(literal)
        ) {
          (dataType as DataTypeExpr).set('this', DataTypeExprKind.TIMESTAMPTZ);
        }

        return this.expression(CastExpr, {
          this: thisWithOps,
          to: dataType as DataTypeExpr,
        });
      }

      // The expressions arg gets set by the parser when we have something like DECIMAL(38, 0)
      // in the input SQL. In that case, we'll produce these tokens: DECIMAL ( 38 , 0 )
      //
      // If the index difference here is greater than 1, that means the parser itself must have
      // consumed additional tokens such as the DECIMAL scale and precision in the above example.
      //
      // If it's not greater than 1, then it must be 1, because we've consumed at least the type
      // keyword, meaning that the expressions arg of the DataType must have gotten set by a
      // callable in the TYPE_CONVERTERS mapping. For example, Snowflake converts DECIMAL to
      // DECIMAL(38, 0)) in order to facilitate the data type's transpilation.
      //
      // In these cases, we don't really want to return the converted type, but instead retreat
      // and try to parse a Column or Identifier in the section below.
      if ((dataType as DataTypeExpr).expressions && 1 < index2 - index) {
        this._retreat(index2);
        return this._parseColumnOps(dataType);
      }

      this._retreat(index);
    }

    if (fallbackToIdentifier) {
      return this._parseIdVar();
    }

    const thisExpr = this._parseColumn();
    return thisExpr && this._parseColumnOps(thisExpr);
  }

  protected _parseTypeSize (): DataTypeParamExpr | undefined {
    let thisExpr: Expression | undefined = this._parseType();
    if (!thisExpr) {
      return undefined;
    }

    if (thisExpr instanceof ColumnExpr && !thisExpr.args.table) {
      thisExpr = varExpr((thisExpr as any).name.toUpperCase());
    }

    return this.expression(
      DataTypeParamExpr,
      {
        this: thisExpr,
        expression: this._parseVar({ anyToken: true }),
      },
    );
  }

  protected _parseUserDefinedType (identifier: IdentifierExpr): Expression | undefined {
    let typeName = identifier.name;

    while (this._match(TokenType.DOT)) {
      this._advanceAny();
      typeName = `${typeName}.${this._prev!.text}`;
    }

    return DataTypeExpr.build(typeName, {
      dialect: this.dialect,
      udt: true,
    });
  }

  protected _parseTypes (options?: {
    checkFunc?: boolean;
    schema?: boolean;
    allowIdentifiers?: boolean;
  }): Expression | undefined {
    const index = this._index;

    let thisExpr: Expression | undefined;
    const prefix = this._matchTextSeq('SYSUDTLIB', '.');

    let typeToken: TokenType | undefined;
    if (this._matchSet(this._constructor.TYPE_TOKENS)) {
      typeToken = this._prev!.tokenType;
    } else {
      const identifier = (options?.allowIdentifiers ?? true) && this._parseIdVar({
        anyToken: false,
        tokens: new Set([TokenType.VAR]),
      });

      if (identifier instanceof IdentifierExpr) {
        let tokens: any[] | undefined;
        try {
          tokens = this.dialect.tokenize?.(identifier.name);
        } catch {
          tokens = undefined;
        }

        if (tokens && tokens.length === 1 && this._constructor.TYPE_TOKENS.has(tokens[0].tokenType)) {
          typeToken = tokens[0].tokenType;
        } else if (this.dialect.SUPPORTS_USER_DEFINED_TYPES) {
          thisExpr = this._parseUserDefinedType(identifier);
        } else {
          this._retreat(this._index - 1);
          return undefined;
        }
      } else {
        return undefined;
      }
    }

    if (typeToken === TokenType.PSEUDO_TYPE) {
      return this.expression(PseudoTypeExpr, { this: this._prev!.text.toUpperCase() });
    }

    if (typeToken === TokenType.OBJECT_IDENTIFIER) {
      return this.expression(ObjectIdentifierExpr, { this: this._prev!.text.toUpperCase() });
    }

    // https://materialize.com/docs/sql/types/map/
    if (typeToken === TokenType.MAP && this._match(TokenType.L_BRACKET)) {
      const keyType = this._parseTypes(options);
      if (!this._match(TokenType.FARROW)) {
        this._retreat(index);
        return undefined;
      }

      const valueType = this._parseTypes(options);
      if (!this._match(TokenType.R_BRACKET)) {
        this._retreat(index);
        return undefined;
      }

      return new DataTypeExpr({
        this: DataTypeExprKind.MAP,
        expressions: [keyType, valueType],
        nested: true,
        prefix,
      });
    }

    const nested = typeToken && this._constructor.NESTED_TYPE_TOKENS.has(typeToken);
    const isStruct = typeToken && this._constructor.STRUCT_TYPE_TOKENS.has(typeToken);
    const isAggregate = typeToken && this._constructor.AGGREGATE_TYPE_TOKENS.has(typeToken);
    let expressions: Expression[] | undefined;
    let maybeFunc = false;

    if (this._match(TokenType.L_PAREN)) {
      if (isStruct) {
        expressions = this._parseCsv(() => this._parseStructTypes({ typeRequired: true }));
      } else if (nested) {
        expressions = this._parseCsv(() => this._parseTypes(options));

        if (typeToken === TokenType.NULLABLE && expressions.length === 1) {
          thisExpr = expressions[0];
          thisExpr.set('nullable', true);
          this._matchRParen();
          return thisExpr;
        }
      } else if (typeToken && this._constructor.ENUM_TYPE_TOKENS.has(typeToken)) {
        expressions = this._parseCsv(() => this._parseEquality());
      } else if (isAggregate) {
        const funcOrIdent = this._parseFunction({ anonymous: true }) || this._parseIdVar({
          anyToken: false,
          tokens: new Set([TokenType.VAR, TokenType.ANY]),
        });
        if (!funcOrIdent) {
          return undefined;
        }
        expressions = [funcOrIdent];
        if (this._match(TokenType.COMMA)) {
          expressions.push(...this._parseCsv(() => this._parseTypes(options)));
        }
      } else {
        expressions = this._parseCsv(() => this._parseTypeSize());

        // https://docs.snowflake.com/en/sql-reference/data-types-vector
        if (typeToken === TokenType.VECTOR && expressions.length === 2) {
          expressions = this._parseVectorExpressions(expressions);
        }
      }

      if (!this._match(TokenType.R_PAREN)) {
        this._retreat(index);
        return undefined;
      }

      maybeFunc = true;
    }

    let values: Expression[] | undefined;

    if (nested && this._match(TokenType.LT)) {
      if (isStruct) {
        expressions = this._parseCsv(() => this._parseStructTypes({ typeRequired: true }));
      } else {
        expressions = this._parseCsv(() => this._parseTypes(options));
      }

      if (!this._match(TokenType.GT)) {
        this.raiseError('Expecting >', this._curr);
      }

      if (this._matchSet(new Set([TokenType.L_BRACKET, TokenType.L_PAREN]))) {
        values = this._parseCsv(() => this._parseDisjunction());
        if (!values && isStruct) {
          values = undefined;
          this._retreat(this._index - 1);
        } else {
          this._matchSet(new Set([TokenType.R_BRACKET, TokenType.R_PAREN]));
        }
      }
    }

    if (typeToken && this._constructor.TIMESTAMPS.has(typeToken)) {
      if (this._matchTextSeq('WITH', 'TIME', 'ZONE')) {
        maybeFunc = false;
        const tzType = this._constructor.TIMES.has(typeToken)
          ? DataTypeExprKind.TIMETZ
          : DataTypeExprKind.TIMESTAMPTZ;
        thisExpr = new DataTypeExpr({
          this: tzType,
          expressions,
        });
      } else if (this._matchTextSeq('WITH', 'LOCAL', 'TIME', 'ZONE')) {
        maybeFunc = false;
        thisExpr = new DataTypeExpr({
          this: DataTypeExprKind.TIMESTAMPLTZ,
          expressions,
        });
      } else if (this._matchTextSeq('WITHOUT', 'TIME', 'ZONE')) {
        maybeFunc = false;
      }
    } else if (typeToken === TokenType.INTERVAL) {
      if (this._curr && this.dialect.VALID_INTERVAL_UNITS.has(this._curr.text.toUpperCase())) {
        let unit: Expression | undefined = this._parseVar({ upper: true });
        if (this._matchTextSeq('TO')) {
          unit = new IntervalSpanExpr({
            this: unit,
            expression: this._parseVar({ upper: true }),
          });
        }

        thisExpr = this.expression(DataTypeExpr, { this: new IntervalExpr({ unit }) });
      } else {
        thisExpr = this.expression(DataTypeExpr, { this: DataTypeExprKind.INTERVAL });
      }
    } else if (typeToken === TokenType.VOID) {
      thisExpr = new DataTypeExpr({ this: DataTypeExprKind.NULL });
    }

    if (maybeFunc && options?.checkFunc) {
      const index2 = this._index;
      const peek = this._parseString();

      if (!peek) {
        this._retreat(index);
        return undefined;
      }

      this._retreat(index2);
    }

    if (!thisExpr) {
      if (this._matchTextSeq('UNSIGNED')) {
        const unsignedTypeToken = typeToken && this._constructor.SIGNED_TO_UNSIGNED_TYPE_TOKEN.get(typeToken);
        if (!unsignedTypeToken) {
          this.raiseError(`Cannot convert ${typeToken?.valueOf()} to unsigned.`, this._curr);
        }

        typeToken = unsignedTypeToken || typeToken;
      }

      // NULLABLE without parentheses can be a column (Presto/Trino)
      if (typeToken === TokenType.NULLABLE && !expressions) {
        this._retreat(index);
        return undefined;
      }

      if (typeToken) {
        thisExpr = new DataTypeExpr({
          this: DataTypeExprKind[typeToken.valueOf() as keyof typeof DataTypeExprKind],
          expressions,
          nested,
          prefix,
        });

        // Empty arrays/structs are allowed
        if (values !== undefined) {
          const cls = isStruct ? StructExpr : ArrayExpr;
          thisExpr = new CastExpr({
            this: new cls({ expressions: values }),
            to: thisExpr as DataTypeExpr,
            copy: false,
          });
        }
      }
    } else if (expressions) {
      thisExpr.set('expressions', expressions);
    }

    // https://materialize.com/docs/sql/types/list/#type-name
    while (this._match(TokenType.LIST)) {
      thisExpr = new DataTypeExpr({
        this: DataTypeExprKind.LIST,
        expressions: [thisExpr],
        nested: true,
      });
    }

    const index3 = this._index;

    // Postgres supports the INT ARRAY[3] syntax as a synonym for INT[3]
    let matchedArray = this._match(TokenType.ARRAY);

    while (this._curr) {
      const datatypeToken = this._prev!.tokenType;
      const matchedLBracket = this._match(TokenType.L_BRACKET);

      if ((!matchedLBracket && !matchedArray) || (
        datatypeToken === TokenType.ARRAY && this._match(TokenType.R_BRACKET)
      )) {
        // Postgres allows casting empty arrays such as ARRAY[]::INT[],
        // not to be confused with the fixed size array parsing
        break;
      }

      matchedArray = false;
      const valuesInBracket = this._parseCsv(() => this._parseDisjunction());

      if (
        valuesInBracket
        && !options?.schema
        && (
          !this.dialect.SUPPORTS_FIXED_SIZE_ARRAYS
          || datatypeToken === TokenType.ARRAY
          || !this._match(TokenType.R_BRACKET, { advance: false })
        )
      ) {
        // Retreating here means that we should not parse the following values as part of the data type, e.g. in DuckDB
        // ARRAY[1] should retreat and instead be parsed into exp.Array in contrast to INT[x][y] which denotes a fixed-size array data type
        this._retreat(index3);
        break;
      }

      thisExpr = new DataTypeExpr({
        this: DataTypeExprKind.ARRAY,
        expressions: [thisExpr],
        values: valuesInBracket,
        nested: true,
      });
      this._match(TokenType.R_BRACKET);
    }

    if (thisExpr && this._constructor.TYPE_CONVERTERS && (thisExpr as DataTypeExpr).this instanceof Object) {
      const converter = this._constructor.TYPE_CONVERTERS.get((thisExpr as DataTypeExpr).this as DataTypeExprKind);
      if (converter) {
        thisExpr = converter(thisExpr as DataTypeExpr);
      }
    }

    return thisExpr;
  }

  protected _parseVectorExpressions (expressions: Expression[]): Expression[] {
    return [DataTypeExpr.build((expressions[0] as any).name, { dialect: this.dialect }), ...expressions.slice(1)];
  }

  protected _parseStructTypes (options?: { typeRequired?: boolean }): Expression | undefined {
    const index = this._index;

    let thisExpr: Expression | undefined;
    if (
      this._curr
      && this._next
      && this._constructor.TYPE_TOKENS.has(this._curr.tokenType)
      && this._constructor.TYPE_TOKENS.has(this._next.tokenType)
    ) {
      // Takes care of special cases like `STRUCT<list ARRAY<...>>` where the identifier is also a
      // type token. Without this, the list will be parsed as a type and we'll eventually crash
      thisExpr = this._parseIdVar();
    } else {
      thisExpr = (
        this._parseType({
          parseInterval: false,
          fallbackToIdentifier: true,
        })
        || this._parseIdVar()
      );
    }

    this._match(TokenType.COLON);

    if (
      options?.typeRequired
      && !(thisExpr instanceof DataTypeExpr)
      && !this._matchSet(this._constructor.TYPE_TOKENS, { advance: false })
    ) {
      this._retreat(index);
      return this._parseTypes();
    }

    return this._parseColumnDef(thisExpr);
  }

  protected _parseAtTimeZone (thisExpr?: Expression): Expression | undefined {
    if (!this._matchTextSeq('AT', 'TIME', 'ZONE')) {
      return thisExpr;
    }
    return this._parseAtTimeZone(
      this.expression(AtTimeZoneExpr, {
        this: thisExpr,
        zone: this._parseUnary(),
      }),
    );
  }

  protected _parseColumn (): Expression | undefined {
    const thisExpr = this._parseColumnReference();
    const column = thisExpr ? this._parseColumnOps(thisExpr) : this._parseBracket(thisExpr);

    if (this.dialect.SUPPORTS_COLUMN_JOIN_MARKS && column) {
      column.set('joinMark', this._match(TokenType.JOIN_MARKER));
    }

    return column;
  }

  protected _parseColumnReference (): Expression | undefined {
    let thisExpr = this._parseField();

    if (
      !thisExpr
      && this._match(TokenType.VALUES, { advance: false })
      && this._constructor.VALUES_FOLLOWED_BY_PAREN
      && (!this._next || this._next.tokenType !== TokenType.L_PAREN)
    ) {
      thisExpr = this._parseIdVar();
    }

    if (thisExpr instanceof IdentifierExpr) {
      // We bubble up comments from the Identifier to the Column
      thisExpr = this.expression(ColumnExpr, {
        comments: thisExpr.popComments(),
        this: thisExpr,
      });
    }

    return thisExpr;
  }

  protected _parseColonAsVariantExtract (thisExpr?: Expression): Expression | undefined {
    const casts: DataTypeExpr[] = [];
    const jsonPath: string[] = [];
    let escape: boolean | undefined;

    while (this._match(TokenType.COLON)) {
      const startIndex = this._index;

      // Snowflake allows reserved keywords as json keys but advance_any() excludes TokenType.SELECT from any_tokens=True
      let path = this._parseColumnOps(
        this._parseField({
          anyToken: true,
          tokens: new Set([TokenType.SELECT]),
        }),
      );

      // The cast :: operator has a lower precedence than the extraction operator :, so
      // we rearrange the AST appropriately to avoid casting the JSON path
      while (path instanceof CastExpr) {
        casts.push(path.args.to as DataTypeExpr);
        path = path.this;
      }

      let endToken: Token;
      if (0 < casts.length) {
        const dcolonOffset = this._tokens.slice(startIndex).findIndex(
          (t) => t.tokenType === TokenType.DCOLON,
        );
        endToken = this._tokens[startIndex + dcolonOffset - 1];
      } else {
        endToken = this._prev!;
      }

      if (path) {
        // Escape single quotes from Snowflake's colon extraction (e.g. col:"a'b") as
        // it'll roundtrip to a string literal in GET_PATH
        if (path instanceof IdentifierExpr && (path as any).quoted) {
          escape = true;
        }

        jsonPath.push(this._findSql(this._tokens[startIndex], endToken));
      }
    }

    // The VARIANT extract in Snowflake/Databricks is parsed as a JSONExtract; Snowflake uses the json_path in GET_PATH() while
    // Databricks transforms it back to the colon/dot notation
    if (0 < jsonPath.length) {
      const jsonPathExpr = this.dialect.toJsonPath?.(LiteralExpr.string('.' + jsonPath.join('.')));

      if (jsonPathExpr) {
        jsonPathExpr.set('escape', escape);
      }

      thisExpr = this.expression(
        JSONExtractExpr,
        {
          this: thisExpr,
          expression: jsonPathExpr,
          variantExtract: true,
          requiresJson: this._constructor.JSON_EXTRACT_REQUIRES_JSON_EXPRESSION,
        },
      );

      while (0 < casts.length) {
        thisExpr = this.expression(CastExpr, {
          this: thisExpr,
          to: casts.pop(),
        });
      }
    }

    return thisExpr;
  }

  protected _parseDcolon (): Expression | undefined {
    return this._parseTypes();
  }

  protected _parseColumnOps (thisExpr?: Expression): Expression | undefined {
    let current = this._parseBracket(thisExpr);

    while (this._matchSet(this._constructor.COLUMN_OPERATORS)) {
      const opToken = this._prev!.tokenType;
      const op = this._constructor.COLUMN_OPERATORS.get(opToken);

      let field: Expression | undefined;
      if (this._constructor.CAST_COLUMN_OPERATORS.has(opToken)) {
        field = this._parseDcolon();
        if (!field) {
          this.raiseError('Expected type', this._curr);
        }
      } else if (op && this._curr) {
        field = this._parseColumnReference() || this._parseBitwise();
        if (field instanceof ColumnExpr && this._match(TokenType.DOT, { advance: false })) {
          field = this._parseColumnOps(field);
        }
      } else {
        field = this._parseField({
          anyToken: true,
          anonymousFunc: true,
        });
      }

      // Function calls can be qualified, e.g., x.y.FOO()
      // This converts the final AST to a series of Dots leading to the function call
      // https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-reference#function_call_rules
      if ((field instanceof FuncExpr || field instanceof WindowExpr) && current) {
        current = current.transform(
          (n: Expression) => n instanceof ColumnExpr ? (n as any).toDot?.({ includeDots: false }) || n : n,
        );
      }

      if (op) {
        current = op(this, current, field);
      } else if (current instanceof ColumnExpr && !current.args.catalog) {
        current = this.expression(
          ColumnExpr,
          {
            comments: current.comments,
            this: field,
            table: current.this,
            db: current.args.table,
            catalog: current.args.db,
          },
        );
      } else if (field instanceof WindowExpr) {
        // Move the exp.Dot's to the window's function
        const windowFunc = this.expression(DotExpr, {
          this: current,
          expression: field.this,
        });
        field.set('this', windowFunc);
        current = field;
      } else {
        current = this.expression(DotExpr, {
          this: current,
          expression: field,
        });
      }

      if (field?.comments) {
        current?.addComments?.(field.popComments());
      }

      current = this._parseBracket(current);
    }

    return this._constructor.COLON_IS_VARIANT_EXTRACT
      ? this._parseColonAsVariantExtract(current)
      : current;
  }

  protected _parseColumnConstraint (): Expression | undefined {
    if (this._match(TokenType.CHECK)) {
      const thisExpr = this._parseWrapped(this._parseConjunction.bind(this));
      return this.expression(CheckColumnConstraintExpr, { this: thisExpr });
    }

    if (this._match(TokenType.DEFAULT)) {
      return this.expression(
        DefaultColumnConstraintExpr,
        { this: this._parseConjunction() },
      );
    }

    if (this._match(TokenType.COLLATE)) {
      return this.expression(
        CollateColumnConstraintExpr,
        { this: this._parseVar() },
      );
    }

    if (this._match(TokenType.UNIQUE)) {
      return this.expression(UniqueColumnConstraintExpr);
    }

    if (this._match(TokenType.GENERATED)) {
      this._matchTextSeq('ALWAYS', 'AS');
      let generatedKind: string | undefined;

      this._match(TokenType.IDENTITY);

      if (!this._match(TokenType.L_PAREN, { advance: false })) {
        if (this._matchTextSeq('BY', 'DEFAULT')) {
          generatedKind = 'BY DEFAULT';
        }

        this._matchTextSeq('AS', 'IDENTITY');
      }

      const sequenceOptions = this._parseWrapped(this._parseSequenceOptions.bind(this));

      if (this.dialect.SUPPORTS_COLUMN_COMPUTED_AS && this._match(TokenType.ALIAS, { advance: false })) {
        // https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-table-computed-column-definition-transact-sql?view=sql-server-ver16
        this._matchTextSeq('AS');
        const expr = this._parseConjunction();
        const stored: boolean | undefined = this._matchTextSeq('PERSISTED') || undefined;
        return this.expression(ComputedColumnConstraintExpr, {
          this: expr,
          persisted: stored,
        });
      }

      return this.expression(
        GeneratedAsIdentityColumnConstraintExpr,
        {
          this: sequenceOptions,
          start: generatedKind ? undefined : true,
          kind: generatedKind,
        },
      );
    }

    if (this._match(TokenType.AUTO_INCREMENT)) {
      return this.expression(AutoIncrementColumnConstraintExpr);
    }

    if (this._matchSet(this._constructor.SUPPORTS_IMPLICIT_DEFAULT_NULL)) {
      return this.expression(DefaultColumnConstraintExpr, { this: new NullExpr() });
    }

    if (this._matchSet(new Set([TokenType.NOT, TokenType.NULL]))) {
      const nullNotNull = this._prev!.tokenType === TokenType.NULL
        ? new NotNullColumnConstraintExpr()
        : new NullColumnConstraintExpr();

      if (this._prev!.tokenType === TokenType.NOT) {
        this._match(TokenType.NULL);
      }

      return nullNotNull;
    }

    if (this._matchTextSeq('ON', 'UPDATE')) {
      return this.expression(
        OnUpdateColumnConstraintExpr,
        { this: this._parseConjunction() },
      );
    }

    if (this._matchTextSeq('ON', 'DELETE')) {
      return this.expression(
        OnDeleteColumnConstraintExpr,
        { this: this._parseConjunction() },
      );
    }

    if (this._match(TokenType.PRIMARY_KEY)) {
      let desc: boolean | undefined;
      if (this._match(TokenType.ASC)) {
        desc = false;
      } else if (this._match(TokenType.DESC)) {
        desc = true;
      }

      const onConflict = this._parseOnConflict();
      return this.expression(
        PrimaryKeyColumnConstraintExpr,
        {
          desc,
          onConflict,
        },
      );
    }

    if (this._match(TokenType.FOREIGN_KEY)) {
      return this._parseReferences();
    }

    if (this._matchTextSeq('COMMENT')) {
      return this.expression(
        CommentColumnConstraintExpr,
        { this: this._parseString() },
      );
    }

    if (this._matchTextSeq('ENCODE')) {
      return this.expression(EncodeColumnConstraintExpr, { this: this._parseVar() });
    }

    if (this._matchTextSeq('TITLE')) {
      return this.expression(TitleColumnConstraintExpr, { this: this._parseString() });
    }

    if (this._matchTextSeq('FORMAT')) {
      return this.expression(DateFormatColumnConstraintExpr, { this: this._parseString() });
    }

    if (this._matchPair(TokenType.LT, TokenType.LT)) {
      if (this._match(TokenType.GT)) {
        return this.expression(PathColumnConstraintExpr);
      }
      return this.expression(InlineLengthColumnConstraintExpr, { this: this._parseNumber() });
    }

    if (this._matchPair(TokenType.GT, TokenType.GT)) {
      return this.expression(CharacterSetColumnConstraintExpr, { this: this._parseVar() });
    }

    if (this._matchTextSeq('UPPERCASE')) {
      return this.expression(UppercaseColumnConstraintExpr);
    }

    return undefined;
  }

  protected _parseConstraint (): Expression | undefined {
    if (!this._match(TokenType.CONSTRAINT)) {
      return this._parseUnnamedConstraint();
    }

    const thisExpr = this._parseIdVar();
    const constraint = this._parseUnnamedConstraint();

    if (constraint) {
      constraint.set('this', thisExpr);
    }

    return constraint;
  }

  protected _parseUnnamedConstraints (options?: { skipUnknown?: boolean }): ColumnConstraintExpr[] | undefined {
    const constraints: ColumnConstraintExpr[] = [];

    while (this._curr) {
      const constraint = this._parseUnnamedConstraint(options);

      if (!constraint) {
        break;
      }

      constraints.push(constraint);
    }

    return 0 < constraints.length ? constraints : undefined;
  }

  protected _parseUnnamedConstraint (options?: { skipUnknown?: boolean }): Expression | undefined {
    let constraint: Expression | undefined;

    if (this._match(TokenType.CHECK)) {
      constraint = this.expression(CheckExpr, { this: this._parseWrapped(this._parseConjunction.bind(this)) });
    } else if (this._match(TokenType.FOREIGN_KEY)) {
      const expressions = this._parseWrappedIdVars();
      const reference = this._parseReferences();

      if (reference) {
        reference.set('this', new ColumnExpr({ expressions }));
      }

      constraint = reference;
    } else if (this._match(TokenType.UNIQUE)) {
      constraint = this._parseUniqueKey();
    } else if (this._matchSet(this._constructor.PRIMARY_KEY_TOKENS)) {
      const expressions = this._parseWrappedIdVars();
      const options = this._parseKeyConstraintOptions();
      constraint = this.expression(PrimaryKeyExpr, {
        expressions,
        options,
      });
    }

    // Oracle in particular allows ENABLE/DISABLE as a separate clause for constraints,
    // rather than as part of the constraint definition, so we can't rely on parse_column_constraint
    // to handle this.
    let enable: boolean | undefined;
    if (this._matchTextSeq('ENABLE') || this._matchTextSeq('VALIDATE')) {
      enable = true;
    } else if (this._matchTextSeq('DISABLE') || this._matchTextSeq('NO', 'VALIDATE')) {
      enable = false;
    }

    if (constraint && enable !== undefined) {
      if (constraint instanceof ConstraintExpr) {
        (constraint as ConstraintExpr).set('enabled', enable);
      }
    }

    if (!constraint && !options?.skipUnknown) {
      constraint = this._parseColumnConstraint();
    }

    if (constraint instanceof ColumnConstraintExpr) {
      constraint.set('kind', this._prevComments);
    }

    return constraint;
  }

  protected _parseUniqueKey (): Expression | undefined {
    const index = this._parseIndexParameters();
    const expressions = this._parseWrappedIdVars();
    const options = this._parseKeyConstraintOptions();
    return this.expression(UniqueExpr, {
      expressions,
      index,
      options,
    });
  }

  protected _parseUnique (): Expression | undefined {
    if (!this._match(TokenType.UNIQUE)) {
      return undefined;
    }

    return this._parseUniqueKey();
  }

  protected _parseKeyConstraintOptions (): Expression[] | undefined {
    const options: Expression[] = [];

    while (true) {
      if (this._matchTextSeq('NOT', 'ENFORCED')) {
        options.push(new NotEnforcedExpr());
      } else if (this._matchTextSeq('DEFERRABLE')) {
        const init = this._matchTextSeq('INITIALLY', 'DEFERRED') || undefined;
        options.push(this.expression(DeferrableExpr, { initially: init }));
      } else if (this._matchTextSeq('INITIALLY', 'DEFERRED')) {
        options.push(new InitiallyDeferredExpr());
      } else if (this._matchTextSeq('NORELY')) {
        options.push(new NorelyExpr());
      } else if (this._matchTextSeq('RELY')) {
        options.push(new RelyExpr());
      } else if (this._matchTextSeq('MATCH', 'FULL')) {
        options.push(new MatchFullExpr());
      } else if (this._matchTextSeq('USING', 'INDEX', 'TABLESPACE')) {
        options.push(this.expression(IndexTablespaceExpr, { this: this._parseIdVar() }));
      } else if (this._match(TokenType.ON)) {
        let action: Expression | undefined;
        if (this._match(TokenType.DELETE)) {
          action = this._parseReferentialAction();
          if (action) {
            options.push(this.expression(OnDeleteExpr, { this: action }));
          }
        } else if (this._match(TokenType.UPDATE)) {
          action = this._parseReferentialAction();
          if (action) {
            options.push(this.expression(OnUpdateExpr, { this: action }));
          }
        }
      } else {
        break;
      }
    }

    return 0 < options.length ? options : undefined;
  }

  protected _parseReferences (options?: { from?: Expression }): ReferenceExpr | undefined {
    if (!this._match(TokenType.REFERENCES)) {
      return undefined;
    }

    const table = this._parseTableParts({ isDbReference: true });
    const expressions = this._parseWrappedIdVars();
    const keyOptions = this._parseKeyConstraintOptions();

    return this.expression(
      ReferenceExpr,
      {
        this: options?.from,
        table,
        expressions,
        options: keyOptions,
      },
    );
  }

  protected _parseForeignKey (): ForeignKeyExpr | undefined {
    if (!this._match(TokenType.FOREIGN_KEY)) {
      return undefined;
    }

    const expressions = this._parseWrappedIdVars();
    const reference = this._parseReferences({ from: new ColumnExpr({ expressions }) });
    return new ForeignKeyExpr({ this: reference });
  }

  protected _parsePrimaryKeyPart (): Expression | undefined {
    return this._parseBracket(this._parseField());
  }

  protected _parsePeriodForSystemTime (): Expression | undefined {
    if (!this._matchTextSeq('PERIOD', 'FOR', 'SYSTEM_TIME')
      && !this._matchTextSeq('PERIOD', 'FOR', 'SYSTEM', 'TIME')) {
      return undefined;
    }

    return this.expression(
      PeriodForSystemTimeConstraintExpr,
      {
        this: this._parseWrappedIdVars(),
      },
    );
  }

  protected _parsePrimaryKey (options?: {
    wrapped?: boolean;
    constraintTokens?: Set<TokenType>;
  }): Expression | undefined {
    let desc: boolean | undefined;
    const constraintTokens = options?.constraintTokens || new Set<TokenType>();

    if (this._match(TokenType.ASC)) {
      desc = false;
    } else if (this._match(TokenType.DESC)) {
      desc = true;
    }

    const thisExpr = this._parseCsv(
      this._parsePrimaryKeyPart.bind(this),
      options?.wrapped || this._match(TokenType.L_PAREN),
    );

    if (this._matchSet(constraintTokens)) {
      return this.expression(PrimaryKeyExpr, {
        expressions: thisExpr,
        desc,
      });
    }

    if (desc !== undefined) {
      return this.expression(PrimaryKeyExpr, {
        expressions: thisExpr,
        desc,
      });
    }

    return new ColumnExpr({ expressions: thisExpr });
  }

  protected _parseBracketKeyValue (): Expression | undefined {
    let thisExpr = this._parseExpression();

    if (this._match(TokenType.COLON)) {
      const expr = this._parseExpression();
      thisExpr = this.expression(PropertyEQExpr, {
        this: thisExpr,
        expression: expr,
      });
    }

    return thisExpr;
  }

  protected _parseOdbcDatetimeLiteral (): Expression | undefined {
    if (!this._match(TokenType.L_BRACE)) {
      return undefined;
    }

    const dtype = this._parseVar({ upper: true });
    const dtypeText = dtype ? (dtype as any).name : undefined;

    if (!dtypeText || ![
      'D',
      'T',
      'TS',
    ].includes(dtypeText)) {
      this.raiseError('Unexpected ODBC datetime type', this._prev);
    }

    const value = this._parseString();

    this._match(TokenType.R_BRACE);

    let func: string;
    if (dtypeText === 'D') {
      func = 'STR_TO_DATE';
    } else if (dtypeText === 'T') {
      func = 'STR_TO_TIME';
    } else {
      func = 'STR_TO_UNIX';
    }

    return this.expression(AnonymousExpr, {
      this: func,
      expressions: [value],
    });
  }

  protected _parseBracket (thisExpr?: Expression): Expression | undefined {
    if (!this._match(TokenType.L_BRACKET)) {
      return thisExpr;
    }

    if (this._match(TokenType.COLON)) {
      const expressions: Expression[] = [new SliceExpr({ expression: this._parseConjunction() })];
      this._matchRBracket();
      return this._parseBracket(
        this.expression(BracketExpr, {
          this: thisExpr,
          expressions,
          offset: true,
        }),
      );
    }

    let expressions: Expression[] | undefined;
    if (this._matchSet(this._constructor.BRACKET_OFFSETS)) {
      const offset = this._prev!.text;
      this._match(TokenType.COLON);
      expressions = [
        this.expression(PropertyExpr, {
          this: offset,
          value: this._parseConjunction(),
        }),
      ];
    } else {
      expressions = this._parseCsv(() => this._parseBracketKeyValue());
    }

    this._matchRBracket();

    if (!thisExpr || !expressions) {
      // DuckDB uses brackets as lists
      if (this._constructor.SUPPORTS_BRACKET_LIST) {
        return this.expression(BracketExpr, {
          expressions,
          isArray: true,
        });
      }

      this.raiseError('Unexpected bracket', this._prev);
    }

    // DuckDB struct access uses brackets like tuple['x']
    // If thisExpr is not a column or doesn't have a table, it's likely a DuckDB struct access
    // We also check if the bracket expression is a string literal to confirm it's a DuckDB struct access
    if (
      this._constructor.SUPPORTS_BRACKET_STRUCT_ACCESS
      && expressions.length === 1
      && (!(thisExpr instanceof ColumnExpr) || !thisExpr.args.table)
      && expressions[0] instanceof LiteralExpr
      && (expressions[0] as any).isString
    ) {
      return this._parseBracket(
        this.expression(DotExpr, {
          this: thisExpr,
          expression: expressions[0],
        }),
      );
    }

    const exp = this.expression(BracketExpr, {
      this: thisExpr,
      expressions,
    });
    return this._parseBracket(exp);
  }

  protected _parseSlice (thisExpr?: Expression): Expression | undefined {
    const expressions: Expression[] = [thisExpr];

    while (this._match(TokenType.COLON)) {
      expressions.push(this._parseConjunction());
    }

    return new SliceExpr({
      this: expressions[0],
      expression: expressions[1],
      offset: expressions[2],
    });
  }

  protected _parseCase (): CaseExpr | undefined {
    const ifs: Expression[] = [];
    let defaultCase: Expression | undefined;

    let thisExpr: Expression | undefined;
    if (!this._matchTextSeq('WHEN')) {
      thisExpr = this._parseConjunction();
      this._matchTextSeq('WHEN');
    }

    while (this._curr) {
      const condition = this._parseConjunction();
      this._matchTextSeq('THEN');
      const trueExpr = this._parseConjunction();

      ifs.push(new IfExpr({
        this: condition,
        true: trueExpr,
      }));

      if (!this._matchTextSeq('WHEN')) {
        break;
      }
    }

    if (this._matchTextSeq('ELSE')) {
      defaultCase = this._parseConjunction();
    }

    if (!this._matchTextSeq('END')) {
      this.raiseError('Expected END after CASE', this._curr);
    }

    return new CaseExpr({
      this: thisExpr,
      ifs,
      default: defaultCase,
    });
  }

  protected _parseIf (): IfExpr | undefined {
    if (this._matchTextSeq('NULLIF')) {
      const first = this._parseConjunction();
      this._match(TokenType.COMMA);
      const second = this._parseConjunction();
      this._matchRParen();
      return new IfExpr({
        this: new EQExpr({
          this: first,
          expression: second,
        }),
        true: new NullExpr(),
        false: first,
      });
    }

    if (!this._matchTextSeq('IF')) {
      return undefined;
    }

    if (this._match(TokenType.L_PAREN)) {
      const thisExpr = this._parseConjunction();
      this._match(TokenType.COMMA);
      const trueExpr = this._parseConjunction();

      let falseExpr: Expression | undefined;
      if (this._match(TokenType.COMMA)) {
        falseExpr = this._parseConjunction();
      }

      this._matchRParen();
      return new IfExpr({
        this: thisExpr,
        true: trueExpr,
        false: falseExpr,
      });
    }

    const index = this._index - 1;
    const thisExpr = this._parseConjunction();
    this._matchTextSeq('THEN');
    const trueExpr = this._parseConjunction();

    let falseExpr: Expression | undefined;
    if (this._matchTextSeq('ELSE')) {
      falseExpr = this._parseConjunction();
    }

    const endIf = this._matchTextSeq('END', 'IF') || this._matchTextSeq('ENDIF');

    if (this._match(TokenType.SEMICOLON) && !endIf) {
      this._retreat(index);
      return undefined;
    }

    return new IfExpr({
      this: thisExpr,
      true: trueExpr,
      false: falseExpr,
    });
  }

  protected _parseNextValueFor (): NextValueForExpr | undefined {
    if (!this._matchTextSeq('NEXT', 'VALUE', 'FOR')) {
      return undefined;
    }

    return new NextValueForExpr({ this: this._parseTableParts() });
  }

  protected _parseExtract (): ExtractExpr | undefined {
    const index = this._index;
    const thisExpr = this._parseFunction() || this._parseVar({
      anyToken: true,
      upper: true,
    }) || this._parseType();

    if (!this._match(TokenType.FROM)) {
      this._retreat(index);
      return undefined;
    }

    return new ExtractExpr({
      this: thisExpr,
      expression: this._parseType(),
    });
  }

  protected _parseGapFill (): Expression | undefined {
    const index = this._index;

    if (!this._matchSet(this._constructor.DATE_PART_TOKENS)) {
      return undefined;
    }

    const part = this._prev!.text.toLowerCase();

    if (!this._match(TokenType.EQ)) {
      this._retreat(index);
      return undefined;
    }

    const thisExpr = this._parseConjunction();

    let step: Expression | undefined;
    if (this._match(TokenType.COMMA)) {
      step = this._parseConjunction();
    }

    return new GapFillExpr({
      this: thisExpr,
      part,
      step,
    });
  }

  protected _parseChar (options?: { alias?: boolean }): Expression | undefined {
    let thisExpr: Expression | undefined;
    const using = this._parseVar({
      anyToken: true,
      upper: true,
    });

    if (using && (using as any).name === 'USING') {
      const charset = this._parseVar({ upper: true });
      if (!charset) {
        this.raiseError('Expected charset', this._curr);
      }
      return this.expression(CharacterSetExpr, { this: charset });
    }

    if (using) {
      thisExpr = using;
    }

    if (options?.alias) {
      const alias = this._parseAlias(thisExpr || this._parseBitwise());
      if (alias) {
        return alias;
      }
    }

    return thisExpr || this._parseBitwise();
  }

  protected _parseCast (options?: {
    strict?: boolean;
    safe?: boolean;
  }): CastExpr | undefined {
    const index = this._index;

    const isMultiple = this._matchTextSeq('TRY', 'CAST') || this._matchTextSeq('SAFE', 'CAST');
    const isCast = isMultiple || this._match(TokenType.CAST);

    if (!isCast) {
      return undefined;
    }

    const thisExpr = this._parseConjunction();
    const actionToken = this._prev;

    if (!this._matchSet(this._constructor.CAST_ACTIONS)) {
      if (options?.strict !== false) {
        this.raiseError('Expected AS, TO, or FORMAT', this._curr);
      }

      this._retreat(index);
      return undefined;
    }

    let format: Expression | string | undefined;
    let to = this._parseTypes();

    if (this._curr) {
      const formatTokens = this._constructor.CAST_FORMAT_TOKENS;
      const action = this._constructor.CAST_ACTIONS.get(actionToken!.tokenType);

      if (this._matchSet(formatTokens)) {
        format = this._parseNumber() || this._parsePlaceholder() || this._parseString();
      }

      if (to && formatTokens.has(to.key)) {
        const formatVal = (to as DataTypeExpr).this;
        format = varExpr(formatVal as string);
        to = this._parseTypes();
      }

      if (format && formatTokens.has(to?.key)) {
        const formatFunc = this._constructor.FORMAT_CAST_MAPPING.get(actionToken!.tokenType);
        if (formatFunc) {
          return formatFunc(this, thisExpr, to);
        }
      }
    }

    const strict = options?.strict ?? true;
    const safe = isMultiple || (options?.safe ?? false);

    let castExpr: CastExpr | undefined;

    if (to instanceof DataTypeExpr) {
      if ((to as DataTypeExpr).isType?.(DataTypeExprKind.CHAR)) {
        if (thisExpr instanceof CharacterSetExpr) {
          thisExpr.set('expression', (to as DataTypeExpr).expressions?.[0]);
          return thisExpr as any;
        }
      }

      castExpr = new CastExpr({
        this: thisExpr,
        to,
        format,
        safe,
        action: actionToken?.text,
      });
    }

    return castExpr;
  }

  protected _tryParse<T> (
    parseMethod: () => T,
    retreat: boolean = false,
  ): T | undefined {
    /**
     * Attempts to backtrack if a parse function that contains a try/catch internally raises an error.
     * This behavior can be different depending on the user-set ErrorLevel, so _tryParse aims to
     * solve this by setting & resetting the parser state accordingly.
     */
    const index = this._index;
    const errorLevel = this.errorLevel;

    this.errorLevel = ErrorLevel.IMMEDIATE;
    let result: T | undefined;
    try {
      result = parseMethod();
    } catch (e) {
      if (e instanceof ParseError) {
        result = undefined;
      } else {
        throw e;
      }
    } finally {
      if (!result || retreat) {
        this._retreat(index);
      }
      this.errorLevel = errorLevel;
    }

    return result;
  }

  protected _parseComment (allowExists: boolean = true): Expression {
    const start = this._prev;
    const exists = allowExists ? this._parseExists() : undefined;

    this._match(TokenType.ON);

    const materialized = this._matchTextSeq('MATERIALIZED');
    const kind = this._matchSet(this._constructor.CREATABLES) && this._prev;
    if (!kind) {
      return this._parseAsCommand(start);
    }

    let thisExpr: Expression;
    if (kind.tokenType === TokenType.FUNCTION || kind.tokenType === TokenType.PROCEDURE) {
      thisExpr = this._parseUserDefinedFunction({ kind: kind.tokenType });
    } else if (kind.tokenType === TokenType.TABLE) {
      thisExpr = this._parseTable({ aliasTokens: this._constructor.COMMENT_TABLE_ALIAS_TOKENS });
    } else if (kind.tokenType === TokenType.COLUMN) {
      thisExpr = this._parseColumn();
    } else {
      thisExpr = this._parseIdVar();
    }

    this._match(TokenType.IS);

    return this.expression(CommentExpr, {
      this: thisExpr,
      kind: kind.text,
      expression: this._parseString(),
      exists,
      materialized,
    });
  }

  protected _parseToTable (): ToTablePropertyExpr {
    const table = this._parseTableParts({ schema: true });
    return this.expression(ToTablePropertyExpr, { this: table });
  }

  protected _parseTtl (): Expression {
    // https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#mergetree-table-ttl
    const parseTtlAction = (): Expression | undefined => {
      const thisExpr = this._parseBitwise();

      if (this._matchTextSeq('DELETE')) {
        return this.expression(MergeTreeTTLActionExpr, {
          this: thisExpr,
          delete: true,
        });
      }
      if (this._matchTextSeq('RECOMPRESS')) {
        return this.expression(MergeTreeTTLActionExpr, {
          this: thisExpr,
          recompress: this._parseBitwise(),
        });
      }
      if (this._matchTextSeq('TO', 'DISK')) {
        return this.expression(MergeTreeTTLActionExpr, {
          this: thisExpr,
          toDisk: this._parseString(),
        });
      }
      if (this._matchTextSeq('TO', 'VOLUME')) {
        return this.expression(MergeTreeTTLActionExpr, {
          this: thisExpr,
          toVolume: this._parseString(),
        });
      }

      return thisExpr;
    };

    const expressions = this._parseCsv(parseTtlAction);
    const where = this._parseWhere();
    const group = this._parseGroup();

    let aggregates: Expression[] | undefined;
    if (group && this._match(TokenType.SET)) {
      aggregates = this._parseCsv(this._parseSetItem);
    }

    return this.expression(MergeTreeTTLExpr, {
      expressions,
      where,
      group,
      aggregates,
    });
  }

  protected _parseStatement (): Expression | undefined {
    if (this._curr === undefined) {
      return undefined;
    }

    if (this._matchSet(this._constructor.STATEMENT_PARSERS)) {
      const comments = this._prevComments;
      const stmt = this._constructor.STATEMENT_PARSERS[this._prev!.tokenType](this);
      stmt.addComments(comments, { prepend: true });
      return stmt;
    }

    if (this._matchSet(this.dialect.tokenizerClass().COMMANDS)) {
      return this._parseCommand();
    }

    let expression = this._parseExpression();
    expression = expression ? this._parseSetOperations(expression) : this._parseSelect();
    return this._parseQueryModifiers(expression);
  }

  protected _parsePartitionedByBucketOrTruncate (): Expression | undefined {
    // Check for L_PAREN without advancing
    if (this._curr?.tokenType !== TokenType.L_PAREN) {
      // Partitioning by bucket or truncate follows the syntax:
      // PARTITION BY (BUCKET(..) | TRUNCATE(..))
      // If we don't have parenthesis after each keyword, we should instead parse this as an identifier
      this._index = this._index - 1;
      return undefined;
    }

    const ExprClass = (
      this._prev?.text.toUpperCase() === 'BUCKET'
        ? PartitionedByBucketExpr
        : PartitionByTruncateExpr
    );

    const args = this._parseWrappedCsv(() => this._parsePrimary() || this._parseColumn());
    let thisArg = seqGet(args, 0);
    let expression = seqGet(args, 1);

    if (thisArg instanceof LiteralExpr) {
      // Check for Iceberg partition transforms (bucket / truncate) and ensure their arguments are in the right order
      //  - For Hive, it's `bucket(<num buckets>, <col name>)` or `truncate(<num_chars>, <col_name>)`
      //  - For Trino, it's reversed - `bucket(<col name>, <num buckets>)` or `truncate(<col_name>, <num_chars>)`
      // Both variants are canonicalized in the latter i.e `bucket(<col name>, <num buckets>)`
      //
      // Hive ref: https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html#querying-iceberg-partitioning
      // Trino ref: https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html#ctas-table-properties
      [thisArg, expression] = [expression, thisArg];
    }

    return this.expression(ExprClass, {
      this: thisArg,
      expression,
    });
  }

  raiseError (message: string, token?: Token): void {
    /**
     * Appends an error in the list of recorded errors or raises it, depending on the chosen
     * error level setting.
     */
    const errorToken = token || this._curr || this._prev || Token.string('');
    const {
      formattedSql, startContext, highlight, endContext,
    } = highlightSql({
      sql: this.sql,
      positions: [[errorToken.start ?? 0, errorToken.end ?? 0]],
      contextLength: this.errorMessageContext,
    });
    const formattedMessage = `${message}. Line ${errorToken.line}, Col: ${errorToken.col}.\n  ${formattedSql}`;

    const error = new ParseError(formattedMessage, [
      {
        description: message,
        line: errorToken.line,
        col: errorToken.col,
        startContext,
        highlight,
        endContext,
      },
    ]);

    if (this.errorLevel === ErrorLevel.IMMEDIATE) {
      throw error;
    }

    this.errors.push(error);
  }

  checkErrors (): void {
    /**
     * Logs or raises any found errors, depending on the chosen error level setting.
     */
    if (this.errorLevel === ErrorLevel.WARN) {
      for (const error of this.errors) {
        console.error(error.toString());
      }
    } else if (this.errorLevel === ErrorLevel.RAISE && 0 < this.errors.length) {
      throw new ParseError(
        concatMessages(this.errors, this.maxErrors),
        { errors: mergeErrors(this.errors) },
      );
    }
  }

  protected mergeErrors (errors: ParseError[]): ParseError[] {
    return mergeErrors(errors);
  }

  expression<E extends Expression> (
    expClass: new (args: any) => E,
    options?: any,
  ): E {
    /**
     * Creates a new, validated Expression.
     *
     * @param expClass - The expression class to instantiate.
     * @param options - Optional arguments including token, comments, and other expression arguments.
     * @returns The target expression.
     */
    const {
      token, comments, ...kwargs
    } = options || {};

    let instance: E;
    if (token) {
      instance = new expClass({
        this: token.text,
        ...kwargs,
      });
      instance.updatePositions(token);
    } else {
      instance = new expClass(kwargs);
    }

    if (comments) {
      instance.addComments(comments);
    } else {
      this._addComments(instance);
    }

    return this.validateExpression(instance);
  }

  protected _addComments (expression: Expression | undefined): void {
    if (expression && this._prevComments) {
      expression.addComments(this._prevComments);
      this._prevComments = undefined;
    }
  }

  validateExpression<E extends Expression> (
    expression: E,
    args?: any[],
  ): E {
    /**
     * Validates an Expression, making sure that all its mandatory arguments are set.
     *
     * @param expression - The expression to validate.
     * @param args - An optional list of items that was used to instantiate the expression, if it's a Func.
     * @returns The validated expression.
     */
    if (this.errorLevel !== ErrorLevel.IGNORE) {
      for (const errorMessage of expression.errorMessages(args)) {
        this.raiseError(errorMessage);
      }
    }

    return expression;
  }

  private get _constructor (): typeof Parser {
    return this.constructor as typeof Parser;
  }
}
