// https://github.com/tobymao/sqlglot/blob/main/sqlglot/parser.py

import {
  AddConstraintExpr,
  AddExpr,
  AddPartitionExpr,
  AdjacentExpr,
  AggFuncExpr,
  AlgorithmPropertyExpr,
  AliasExpr,
  AliasesExpr,
  AllExpr,
  AllowedValuesPropertyExpr,
  AlterColumnExpr,
  AlterDistStyleExpr,
  AlterExpr,
  AlterRenameExpr,
  AlterSessionExpr,
  AlterSetExpr,
  AlterSortKeyExpr,
  AnalyzeColumnsExpr,
  AnalyzeDeleteExpr,
  AnalyzeExpr,
  AnalyzeHistogramExpr,
  AnalyzeListChainedRowsExpr,
  AnalyzeSampleExpr,
  AnalyzeStatisticsExpr,
  AnalyzeValidateExpr,
  AnalyzeWithExpr,
  AndExpr,
  AnonymousExpr,
  AnyExpr,
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
  AutoIncrementColumnConstraintExpr,
  AutoIncrementPropertyExpr,
  AutoRefreshPropertyExpr,
  BackupPropertyExpr,
  BetweenExpr,
  BinaryExpr,
  BitStringExpr,
  BitwiseAndExpr,
  BitwiseLeftShiftExpr,
  BitwiseNotExpr,
  BitwiseOrExpr,
  BitwiseRightShiftExpr,
  BitwiseXorExpr,
  BlockCompressionPropertyExpr,
  BooleanExpr,
  BracketExpr,
  ByteStringExpr,
  CTEExpr,
  CacheExpr,
  CaseExpr,
  CaseSpecificColumnConstraintExpr,
  CastExpr,
  CastToStrTypeExpr,
  CbrtExpr,
  CeilExpr,
  ChangesExpr,
  CharacterSetColumnConstraintExpr,
  CharacterSetExpr,
  CharacterSetPropertyExpr,
  CheckColumnConstraintExpr,
  ChecksumPropertyExpr,
  ChrExpr,
  CloneExpr,
  ClusterExpr,
  ClusteredByPropertyExpr,
  ClusteredColumnConstraintExpr,
  CoalesceExpr,
  CollateColumnConstraintExpr,
  CollateExpr,
  CollatePropertyExpr,
  ColumnConstraintExpr,
  ColumnDefExpr,
  ColumnExpr,
  ColumnPositionExpr,
  ColumnsExpr,
  CommandExpr,
  CommentColumnConstraintExpr,
  CommentExpr,
  CommitExpr,
  ComprehensionExpr,
  CompressColumnConstraintExpr,
  ComputedColumnConstraintExpr,
  ConcatExpr,
  ConcatWsExpr,
  ConditionalInsertExpr,
  ConnectByRootExpr,
  ConnectExpr,
  ConstraintExpr,
  ConvertTimezoneExpr,
  CopyExpr,
  CopyGrantsPropertyExpr,
  CopyParameterExpr,
  CountExpr,
  CreateExpr,
  CredentialsExpr,
  CubeExpr,
  CurrentDateExpr,
  CurrentRoleExpr,
  CurrentTimeExpr,
  CurrentTimestampExpr,
  CurrentUserExpr,
  DPipeExpr,
  DataBlocksizePropertyExpr,
  DataDeletionPropertyExpr,
  DataTypeExpr,
  DataTypeExprKind,
  DataTypeParamExpr,
  DateFormatColumnConstraintExpr,
  DeclareExpr,
  DeclareItemExpr,
  DecodeCaseExpr,
  DecodeExpr,
  DefaultColumnConstraintExpr,
  DefinerPropertyExpr,
  DeleteExpr,
  DescribeExpr,
  DictPropertyExpr,
  DictRangeExpr,
  DictSubPropertyExpr,
  DirectoryExpr,
  DistKeyPropertyExpr,
  DistStylePropertyExpr,
  DistanceExpr,
  DistinctExpr,
  DistributeExpr,
  DistributedByPropertyExpr,
  DivExpr,
  DotExpr,
  DropExpr,
  DropPartitionExpr,
  DuplicateKeyPropertyExpr,
  DynamicPropertyExpr,
  EQExpr,
  EmptyPropertyExpr,
  EncodeColumnConstraintExpr,
  EnginePropertyExpr,
  EnviromentPropertyExpr,
  EphemeralColumnConstraintExpr,
  EscapeExpr,
  ExceptExpr,
  ExcludeColumnConstraintExpr,
  ExecuteAsPropertyExpr,
  ExistsExpr,
  Expression, GrantPrivilegeExpr, OverlayExpr, RevokeExpr,
  ExpressionKey,
  ExtendsLeftExpr,
  ExtendsRightExpr,
  ExternalPropertyExpr,
  ExtractExpr,
  FUNCTION_BY_NAME,
  FallbackPropertyExpr,
  FetchExpr,
  FileFormatPropertyExpr,
  FilterExpr,
  FloorExpr,
  ForeignKeyExpr,
  FormatJsonExpr,
  FreespacePropertyExpr,
  FromExpr,
  FuncExpr,
  GTEExpr,
  GTExpr,
  GapFillExpr,
  GenerateDateArrayExpr,
  GeneratedAsIdentityColumnConstraintExpr,
  GeneratedAsRowColumnConstraintExpr,
  GlobExpr,
  GlobalPropertyExpr,
  GrantExpr,
  GrantPrincipalExpr,
  GreatestExpr,
  GroupExpr,
  GroupingSetsExpr,
  HavingExpr,
  HavingMaxExpr,
  HeapPropertyExpr,
  HeredocExpr,
  HexExpr,
  HexStringExpr,
  HintExpr,
  HistoricalDataExpr,
  ILikeExpr,
  IcebergPropertyExpr,
  IdentifierExpr,
  IfExpr,
  IgnoreNullsExpr,
  InExpr,
  IndexExpr,
  IndexParametersExpr,
  IndexTableHintExpr,
  InheritsPropertyExpr,
  InitcapExpr,
  InlineLengthColumnConstraintExpr,
  InputModelPropertyExpr,
  InputOutputFormatExpr,
  InsertExpr,
  IntDivExpr,
  IntersectExpr,
  IntervalExpr,
  IntervalSpanExpr,
  IntoExpr,
  IntroducerExpr,
  IsExpr,
  IsolatedLoadingPropertyExpr,
  JSONBContainsAllTopKeysExpr,
  JSONBContainsAnyTopKeysExpr,
  JSONBContainsExpr,
  JSONBDeleteAtPathExpr,
  JSONBExtractExpr,
  JSONBExtractScalarExpr,
  JSONCastExpr,
  JSONColumnDefExpr,
  JSONExpr,
  JSONExtractExpr,
  JSONExtractScalarExpr,
  JSONKeyValueExpr,
  JSONKeysExpr,
  JSONObjectAggExpr,
  JSONObjectExpr,
  JSONSchemaExpr,
  JSONTableExpr,
  JSONValueExpr,
  JoinExpr,
  JoinHintExpr,
  JournalPropertyExpr,
  KillExpr,
  KwargExpr,
  LTEExpr,
  LTExpr,
  LambdaExpr,
  LanguagePropertyExpr,
  LateralExpr,
  LeastExpr,
  LikeExpr,
  LikePropertyExpr,
  LimitExpr,
  LimitOptionsExpr,
  ListExpr,
  LiteralExpr,
  LnExpr,
  LoadDataExpr,
  LocaltimeExpr,
  LocaltimestampExpr,
  LocationPropertyExpr,
  LockExpr,
  LockingPropertyExpr,
  LogExpr,
  LogPropertyExpr,
  LowerExpr,
  LowerHexExpr,
  MatchAgainstExpr,
  MatchRecognizeExpr,
  MatchRecognizeMeasureExpr,
  MaterializedPropertyExpr,
  MergeBlockRatioPropertyExpr,
  MergeExpr,
  MergeTreeTTLActionExpr,
  MergeTreeTTLExpr,
  ModExpr,
  MulExpr,
  MultitableInsertsExpr,
  NEQExpr,
  NationalExpr,
  NegExpr,
  NextValueForExpr,
  NoPrimaryIndexPropertyExpr,
  NonClusteredColumnConstraintExpr,
  NormalizeExpr,
  NotExpr,
  NotForReplicationColumnConstraintExpr,
  NotNullColumnConstraintExpr,
  NullExpr,
  NullSafeEQExpr,
  NullSafeNEQExpr,
  ObjectIdentifierExpr,
  OffsetExpr,
  OnCommitPropertyExpr,
  OnConditionExpr,
  OnConflictExpr,
  OnPropertyExpr,
  OnUpdateColumnConstraintExpr,
  OpclassExpr,
  OpenJSONColumnDefExpr,
  OpenJSONExpr,
  OperatorExpr,
  OrExpr,
  OrderExpr,
  OrderedExpr,
  OutputModelPropertyExpr,
  OverlapsExpr,
  PadExpr,
  ParameterExpr,
  ParenExpr,
  ParseJSONExpr,
  PartitionBoundSpecExpr,
  PartitionByTruncateExpr,
  PartitionExpr,
  PartitionedByBucketExpr,
  PartitionedByPropertyExpr,
  PartitionedOfPropertyExpr,
  PathColumnConstraintExpr,
  PeriodForSystemTimeConstraintExpr,
  PivotAliasExpr,
  PivotAnyExpr,
  PivotExpr,
  PlaceholderExpr,
  PragmaExpr,
  PreWhereExpr,
  PrimaryKeyColumnConstraintExpr,
  PrimaryKeyExpr,
  PriorExpr,
  PropertiesExpr,
  PropertyEQExpr,
  PropertyExpr,
  PseudoTypeExpr,
  QualifyExpr,
  QueryExpr,
  RawStringExpr,
  RecursiveWithSearchExpr,
  ReferenceExpr,
  RefreshExpr,
  RegexpILikeExpr,
  RegexpLikeExpr,
  RemoteWithConnectionModelPropertyExpr,
  RenameColumnExpr,
  RespectNullsExpr,
  ReturnExpr,
  ReturningExpr,
  ReturnsPropertyExpr,
  RollbackExpr,
  RollupExpr,
  RowFormatDelimitedPropertyExpr,
  RowFormatPropertyExpr,
  RowFormatSerdePropertyExpr,
  SQLGLOT_ANONYMOUS,
  SamplePropertyExpr,
  SchemaCommentPropertyExpr,
  SchemaExpr,
  ScopeResolutionExpr,
  SecurePropertyExpr,
  SecurityPropertyExpr,
  SelectExpr,
  SemicolonExpr,
  SequencePropertiesExpr,
  SerdePropertiesExpr,
  SessionParameterExpr,
  SetExpr,
  SetItemExpr,
  SetOperationExpr,
  SetPropertyExpr,
  SettingsPropertyExpr,
  SharingPropertyExpr,
  SimilarToExpr,
  SliceExpr,
  SortExpr,
  SortKeyPropertyExpr,
  SqlReadWritePropertyExpr,
  SqlSecurityPropertyExpr,
  SqrtExpr,
  StabilityPropertyExpr,
  StarExpr,
  StarMapExpr,
  StorageHandlerPropertyExpr,
  StrPositionExpr,
  StrToDateExpr,
  StrToTimeExpr,
  StreamExpr,
  StreamingTablePropertyExpr,
  StrictPropertyExpr,
  StructExpr,
  SubExpr,
  SubqueryExpr,
  SubstringExpr,
  SummarizeExpr,
  SwapTableExpr,
  TableAliasExpr,
  TableExpr,
  TableFromRowsExpr,
  TableSampleExpr,
  TemporaryPropertyExpr,
  TitleColumnConstraintExpr,
  ToTablePropertyExpr,
  TransactionExpr,
  TransformModelPropertyExpr,
  TransientPropertyExpr,
  TrimExpr,
  TrimPosition,
  TruncateTableExpr,
  TryCastExpr,
  TupleExpr,
  UncacheExpr,
  UnicodeStringExpr,
  UnionExpr,
  UniqueColumnConstraintExpr,
  UnloggedPropertyExpr,
  UnnestExpr,
  UnpivotColumnsExpr,
  UpdateExpr,
  UpperExpr,
  UppercaseColumnConstraintExpr,
  UseExpr,
  UserDefinedFunctionExpr,
  UsingDataExpr,
  UuidExpr,
  ValuesExpr,
  VarExpr,
  VarMapExpr,
  VersionExpr,
  ViewAttributePropertyExpr,
  VolatilePropertyExpr,
  WhenExpr,
  WhensExpr,
  WhereExpr,
  WindowExpr,
  WindowSpecExpr,
  WithDataPropertyExpr,
  WithExpr,
  WithFillExpr,
  WithJournalTablePropertyExpr,
  WithOperatorExpr,
  WithProcedureOptionsExpr,
  WithSchemaBindingPropertyExpr,
  WithSystemVersioningPropertyExpr,
  WithTableHintExpr,
  WithinGroupExpr,
  XMLElementExpr,
  XMLNamespaceExpr,
  XMLTableExpr,
  array,
  column,
  INTERVAL_DAY_TIME_RE,
  INTERVAL_STRING_RE,
  select,
  toIdentifier,
  UNWRAPPED_QUERIES,
  var_,
} from './expressions';
import { formatTime } from './time';
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
  newTrie, type TrieNode, inTrie, TrieResult,
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
    let expression = parser.parseBitwise();
    let thisArg = thisExpr;

    if (reverseArgs) {
      [thisArg, expression] = [expression, thisArg];
    }

    return parser.parseEscape(
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
    if (!dialect._constructor.LOG_BASE_FIRST) {
      [thisArg, expression] = [expression, thisArg];
    }
    return new LogExpr({
      this: thisArg,
      expression,
    });
  }

  // Check if dialect's parser class has LOG_DEFAULTS_TO_LN property
  const parserClass = (dialect.constructor as typeof Dialect).parserClass;
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
  return dialect._constructor.HEX_LOWERCASE
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
      expression.setArgKey('scalarOnly', dialect._constructor.JSON_EXTRACT_SCALAR_SCALAR_ONLY);
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
  const arrayExpr = new exprClass({ expressions: args });

  if (arrayExpr instanceof ArrayExpr && dialect._constructor.HAS_DISTINCT_ARRAY_CONSTRUCTORS) {
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
    nullPropagation: dialect._constructor.ARRAY_FUNCS_PROPAGATES_NULLS,
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
    nullPropagation: dialect._constructor.ARRAY_FUNCS_PROPAGATES_NULLS,
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
    nullPropagation: dialect._constructor.ARRAY_FUNCS_PROPAGATES_NULLS,
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
    nullPropagation: dialect._constructor.ARRAY_FUNCS_PROPAGATES_NULLS,
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
      nullsExcluded: dialect._constructor.ARRAY_AGG_INCLUDES_NULLS === undefined ? true : undefined,
    }),

    ARRAY_AGG: (args, dialect) => new ArrayAggExpr({
      this: seqGet(args, 0)!,
      nullsExcluded: dialect._constructor.ARRAY_AGG_INCLUDES_NULLS === undefined ? true : undefined,
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
      safe: !dialect._constructor.STRICT_STRING_CONCAT,
      coalesce: dialect._constructor.CONCAT_COALESCE,
    }),

    CONCAT_WS: (args, dialect) => new ConcatWsExpr({
      expressions: args,
      safe: !dialect._constructor.STRICT_STRING_CONCAT,
      coalesce: dialect._constructor.CONCAT_COALESCE,
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
        unit: var_('DAY'),
      }),
    }),

    GENERATE_UUID: (args, dialect) => new UuidExpr({
      isString: dialect._constructor.UUID_IS_STRING_TYPE || undefined,
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
      ignoreNulls: dialect._constructor.LEAST_GREATEST_IGNORES_NULLS,
    }),

    LEAST: (args, dialect) => new LeastExpr({
      this: seqGet(args, 0)!,
      expressions: args.slice(1),
      ignoreNulls: dialect._constructor.LEAST_GREATEST_IGNORES_NULLS,
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
      isString: dialect._constructor.UUID_IS_STRING_TYPE || undefined,
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
          self.parseDisjunction(),
          expressions,
        ),
        expressions: expressions,
      },
    ),
    [TokenType.FARROW]: (self: Parser, expressions: Expression[]) => self.expression(
      KwargExpr,
      {
        this: var_(expressions[0].name),
        expression: self.parseDisjunction(),
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
      strict: self._constructor.STRICT_CAST,
      this: this_,
      to: to,
    }),
    [TokenType.ARROW]: (self: Parser, this_: Expression, path: Expression) => self.expression(
      JSONExtractExpr,
      {
        this: this_,
        expression: self.dialect.toJsonPath(path),
        onlyJsonTypes: self._constructor.JSON_ARROWS_REQUIRE_JSON_TYPE,
      },
    ),
    [TokenType.DARROW]: (self: Parser, this_: Expression, path: Expression) => self.expression(
      JSONExtractScalarExpr,
      {
        this: this_,
        expression: self.dialect.toJsonPath(path),
        onlyJsonTypes: self._constructor.JSON_ARROWS_REQUIRE_JSON_TYPE,
        scalarOnly: self._dialectConstructor.JSON_EXTRACT_SCALAR_SCALAR_ONLY,
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

  static EXPRESSION_PARSERS: Record<string, (self: Parser) => Expression | undefined> = {
    [ExpressionKey.CLUSTER]: (self: Parser) => self.parseSort(ClusterExpr, TokenType.CLUSTER_BY),
    [ExpressionKey.COLUMN]: (self: Parser) => self.parseColumn(),
    [ExpressionKey.COLUMN_DEF]: (self: Parser) => self.parseColumnDef(self.parseColumn()),
    [ExpressionKey.CONDITION]: (self: Parser) => self.parseDisjunction(),
    [ExpressionKey.DATA_TYPE]: (self: Parser) => self.parseTypes({
      allowIdentifiers: false,
      schema: true,
    }),
    [ExpressionKey.EXPRESSION]: (self: Parser) => self.parseExpression(),
    [ExpressionKey.FROM]: (self: Parser) => self.parseFrom({ joins: true }),
    [ExpressionKey.GRANT_PRINCIPAL]: (self: Parser) => self.parseGrantPrincipal(),
    [ExpressionKey.GRANT_PRIVILEGE]: (self: Parser) => self.parseGrantPrivilege(),
    [ExpressionKey.GROUP]: (self: Parser) => self.parseGroup(),
    [ExpressionKey.HAVING]: (self: Parser) => self.parseHaving(),
    [ExpressionKey.HINT]: (self: Parser) => self.parseHintBody(),
    [ExpressionKey.IDENTIFIER]: (self: Parser) => self.parseIdVar(),
    [ExpressionKey.JOIN]: (self: Parser) => self.parseJoin(),
    [ExpressionKey.LAMBDA]: (self: Parser) => self.parseLambda(),
    [ExpressionKey.LATERAL]: (self: Parser) => self.parseLateral(),
    [ExpressionKey.LIMIT]: (self: Parser) => self.parseLimit(),
    [ExpressionKey.OFFSET]: (self: Parser) => self.parseOffset(),
    [ExpressionKey.ORDER]: (self: Parser) => self.parseOrder(),
    [ExpressionKey.ORDERED]: (self: Parser) => self.parseOrdered(),
    [ExpressionKey.PROPERTIES]: (self: Parser) => self.parseProperties(),
    [ExpressionKey.PARTITIONED_BY_PROPERTY]: (self: Parser) => self.parsePartitionedBy(),
    [ExpressionKey.QUALIFY]: (self: Parser) => self.parseQualify(),
    [ExpressionKey.RETURNING]: (self: Parser) => self.parseReturning(),
    [ExpressionKey.SELECT]: (self: Parser) => self.parseSelect(),
    [ExpressionKey.SORT]: (self: Parser) => self.parseSort(SortExpr, TokenType.SORT_BY),
    [ExpressionKey.TABLE]: (self: Parser) => self.parseTableParts(),
    [ExpressionKey.TABLE_ALIAS]: (self: Parser) => self.parseTableAlias(),
    [ExpressionKey.TUPLE]: (self: Parser) => self.parseValue({ values: false }),
    [ExpressionKey.WHENS]: (self: Parser) => self.parseWhenMatched(),
    [ExpressionKey.WHERE]: (self: Parser) => self.parseWhere(),
    [ExpressionKey.WINDOW]: (self: Parser) => self.parseNamedWindow(),
    [ExpressionKey.WITH]: (self: Parser) => self.parseWith(),
  };

  static STATEMENT_PARSERS = {
    [TokenType.ALTER]: (self: Parser) => self.parseAlter(),
    [TokenType.ANALYZE]: (self: Parser) => self.parseAnalyze(),
    [TokenType.BEGIN]: (self: Parser) => self.parseTransaction(),
    [TokenType.CACHE]: (self: Parser) => self.parseCache(),
    [TokenType.COMMENT]: (self: Parser) => self.parseComment(),
    [TokenType.COMMIT]: (self: Parser) => self.parseCommitOrRollback(),
    [TokenType.COPY]: (self: Parser) => self.parseCopy(),
    [TokenType.CREATE]: (self: Parser) => self.parseCreate(),
    [TokenType.DELETE]: (self: Parser) => self.parseDelete(),
    [TokenType.DESC]: (self: Parser) => self.parseDescribe(),
    [TokenType.DESCRIBE]: (self: Parser) => self.parseDescribe(),
    [TokenType.DROP]: (self: Parser) => self.parseDrop(),
    [TokenType.GRANT]: (self: Parser) => self.parseGrant(),
    [TokenType.REVOKE]: (self: Parser) => self.parseRevoke(),
    [TokenType.INSERT]: (self: Parser) => self.parseInsert(),
    [TokenType.KILL]: (self: Parser) => self.parseKill(),
    [TokenType.LOAD]: (self: Parser) => self.parseLoad(),
    [TokenType.MERGE]: (self: Parser) => self.parseMerge(),
    [TokenType.PIVOT]: (self: Parser) => self.parseSimplifiedPivot(),
    [TokenType.PRAGMA]: (self: Parser) => self.expression(PragmaExpr, { this: self.parseExpression() }),
    [TokenType.REFRESH]: (self: Parser) => self.parseRefresh(),
    [TokenType.ROLLBACK]: (self: Parser) => self.parseCommitOrRollback(),
    [TokenType.SET]: (self: Parser) => self.parseSet(),
    [TokenType.TRUNCATE]: (self: Parser) => self.parseTruncateTable(),
    [TokenType.UNCACHE]: (self: Parser) => self.parseUncache(),
    [TokenType.UNPIVOT]: (self: Parser) => self.parseSimplifiedPivot({ isUnpivot: true }),
    [TokenType.UPDATE]: (self: Parser) => self.parseUpdate(),
    [TokenType.USE]: (self: Parser) => self.parseUse(),
    [TokenType.SEMICOLON]: (_self: Parser) => new SemicolonExpr({}),
  };

  static UNARY_PARSERS = {
    [TokenType.PLUS]: (self: Parser) => self.parseUnary(),
    [TokenType.NOT]: (self: Parser) => self.expression(NotExpr, { this: self.parseEquality() }),
    [TokenType.TILDE]: (self: Parser) => self.expression(BitwiseNotExpr, { this: self.parseUnary() }),
    [TokenType.DASH]: (self: Parser) => self.expression(NegExpr, { this: self.parseUnary() }),
    [TokenType.PIPE_SLASH]: (self: Parser) => self.expression(SqrtExpr, { this: self.parseUnary() }),
    [TokenType.DPIPE_SLASH]: (self: Parser) => self.expression(CbrtExpr, { this: self.parseUnary() }),
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
        escape: self._matchTextSeq('UESCAPE') && self.parseString(),
      },
    ),
  };

  static NUMERIC_PARSERS = {
    [TokenType.BIT_STRING]: (self: Parser, token: Token) => self.expression(BitStringExpr, { token }),
    [TokenType.BYTE_STRING]: (self: Parser, token: Token) => self.expression(
      ByteStringExpr,
      {
        token,
        isBytes: self._dialectConstructor.BYTE_STRING_IS_BYTES_TYPE || undefined,
      },
    ),
    [TokenType.HEX_STRING]: (self: Parser, token: Token) => self.expression(
      HexStringExpr,
      {
        token,
        isInteger: self._dialectConstructor.HEX_STRING_IS_INTEGER_TYPE || undefined,
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
    [TokenType.INTRODUCER]: (self: Parser, token: Token) => self.parseIntroducer(token),
    [TokenType.NULL]: (self: Parser, _: Token) => self.expression(NullExpr, {}),
    [TokenType.TRUE]: (self: Parser, _: Token) => self.expression(BooleanExpr, { this: true }),
    [TokenType.FALSE]: (self: Parser, _: Token) => self.expression(BooleanExpr, { this: false }),
    [TokenType.SESSION_PARAMETER]: (self: Parser, _: Token) => self.parseSessionParameter(),
    [TokenType.STAR]: (self: Parser, _: Token) => self.parseStarOps(),
  };

  static PLACEHOLDER_PARSERS = {
    [TokenType.PLACEHOLDER]: (self: Parser) => self.expression(PlaceholderExpr),
    [TokenType.PARAMETER]: (self: Parser) => self.parseParameter(),
    [TokenType.COLON]: (self: Parser) => (
      self._matchSet(self._constructor.COLON_PLACEHOLDER_TOKENS)
        ? self.expression(PlaceholderExpr, { this: self._prev!.text })
        : undefined
    ),
  };

  static RANGE_PARSERS = {
    [TokenType.AT_GT]: binaryRangeParser(ArrayContainsAllExpr),
    [TokenType.BETWEEN]: (self: Parser, this_: Expression) => self.parseBetween(this_),
    [TokenType.GLOB]: binaryRangeParser(GlobExpr),
    [TokenType.ILIKE]: binaryRangeParser(ILikeExpr),
    [TokenType.IN]: (self: Parser, this_: Expression) => self.parseIn(this_),
    [TokenType.IRLIKE]: binaryRangeParser(RegexpILikeExpr),
    [TokenType.IS]: (self: Parser, this_: Expression) => self.parseIs(this_),
    [TokenType.LIKE]: binaryRangeParser(LikeExpr),
    [TokenType.LT_AT]: binaryRangeParser(ArrayContainsAllExpr, { reverseArgs: true }),
    [TokenType.OVERLAPS]: binaryRangeParser(OverlapsExpr),
    [TokenType.RLIKE]: binaryRangeParser(RegexpLikeExpr),
    [TokenType.SIMILAR_TO]: binaryRangeParser(SimilarToExpr),
    [TokenType.FOR]: (self: Parser, this_: Expression) => self.parseComprehension(this_),
    [TokenType.QMARK_AMP]: binaryRangeParser(JSONBContainsAllTopKeysExpr),
    [TokenType.QMARK_PIPE]: binaryRangeParser(JSONBContainsAnyTopKeysExpr),
    [TokenType.HASH_DASH]: binaryRangeParser(JSONBDeleteAtPathExpr),
    [TokenType.ADJACENT]: binaryRangeParser(AdjacentExpr),
    [TokenType.OPERATOR]: (self: Parser, this_: Expression) => self.parseOperator(this_),
    [TokenType.AMP_LT]: binaryRangeParser(ExtendsLeftExpr),
    [TokenType.AMP_GT]: binaryRangeParser(ExtendsRightExpr),
  };

  static PIPE_SYNTAX_TRANSFORM_PARSERS = {
    'AGGREGATE': (self: Parser, query: SelectExpr) => self.parsePipeSyntaxAggregate(query),
    'AS': (self: Parser, query: SelectExpr) => self.buildPipeCte({
      query,
      expressions: [new StarExpr({})],
      aliasCte: self.parseTableAlias(),
    }),
    'EXTEND': (self: Parser, query: SelectExpr) => self.parsePipeSyntaxExtend(query),
    'LIMIT': (self: Parser, query: SelectExpr) => self.parsePipeSyntaxLimit(query),
    'ORDER BY': (self: Parser, query: SelectExpr) => query.orderBy(
      self.parseOrder(),
      {
        append: false,
        copy: false,
      },
    ),
    'PIVOT': (self: Parser, query: SelectExpr) => self.parsePipeSyntaxPivot(query),
    'SELECT': (self: Parser, query: SelectExpr) => self.parsePipeSyntaxSelect(query),
    'TABLESAMPLE': (self: Parser, query: SelectExpr) => self.parsePipeSyntaxTablesample(query),
    'UNPIVOT': (self: Parser, query: SelectExpr) => self.parsePipeSyntaxPivot(query),
    'WHERE': (self: Parser, query: SelectExpr) => query.where(self.parseWhere(), { copy: false }),
  };

  static PROPERTY_PARSERS: Record<string, (self: Parser, ...args: unknown[]) => Expression | Expression[] | undefined> = {
    'ALLOWED_VALUES': (self: Parser) => self.expression(
      AllowedValuesPropertyExpr,
      { expressions: self.parseCsv(self.parsePrimary) },
    ),
    'ALGORITHM': (self: Parser) => self.parsePropertyAssignment(AlgorithmPropertyExpr),
    'AUTO': (self: Parser) => self.parseAutoProperty(),
    'AUTO_INCREMENT': (self: Parser) => self.parsePropertyAssignment(AutoIncrementPropertyExpr),
    'BACKUP': (self: Parser) => self.expression(
      BackupPropertyExpr,
      { this: self.parseVar({ anyToken: true }) },
    ),
    'BLOCKCOMPRESSION': (self: Parser) => self.parseBlockcompression(),
    'CHARSET': (self: Parser) => self.parseCharacterSet(),
    'CHARACTER SET': (self: Parser) => self.parseCharacterSet(),
    'CHECKSUM': (self: Parser) => self.parseChecksum(),
    'CLUSTER BY': (self: Parser) => self.parseCluster(),
    'CLUSTERED': (self: Parser) => self.parseClusteredBy(),
    'COLLATE': (self: Parser) => self.parsePropertyAssignment(CollatePropertyExpr),
    'COMMENT': (self: Parser) => self.parsePropertyAssignment(SchemaCommentPropertyExpr),
    'CONTAINS': (self: Parser) => self.parseContainsProperty(),
    'COPY': (self: Parser) => self.parseCopyProperty(),
    'DATABLOCKSIZE': (self: Parser) => self.parseDatablocksize(),
    'DATA_DELETION': (self: Parser) => self.parseDataDeletionProperty(),
    'DEFINER': (self: Parser) => self.parseDefiner(),
    'DETERMINISTIC': (self: Parser) => self.expression(
      StabilityPropertyExpr,
      { this: LiteralExpr.string('IMMUTABLE') },
    ),
    'DISTRIBUTED': (self: Parser) => self.parseDistributedProperty(),
    'DUPLICATE': (self: Parser) => self.parseCompositeKeyProperty(DuplicateKeyPropertyExpr),
    'DYNAMIC': (self: Parser) => self.expression(DynamicPropertyExpr, {}),
    'DISTKEY': (self: Parser) => self.parseDistkey(),
    'DISTSTYLE': (self: Parser) => self.parsePropertyAssignment(DistStylePropertyExpr),
    'EMPTY': (self: Parser) => self.expression(EmptyPropertyExpr, {}),
    'ENGINE': (self: Parser) => self.parsePropertyAssignment(EnginePropertyExpr),
    'ENVIRONMENT': (self: Parser) => self.expression(
      EnviromentPropertyExpr,
      { expressions: self.parseWrappedCsv(self.parseAssignment) },
    ),
    'EXECUTE': (self: Parser) => self.parsePropertyAssignment(ExecuteAsPropertyExpr),
    'EXTERNAL': (self: Parser) => self.expression(ExternalPropertyExpr, {}),
    'FALLBACK': (self: Parser) => self.parseFallback(),
    'FORMAT': (self: Parser) => self.parsePropertyAssignment(FileFormatPropertyExpr),
    'FREESPACE': (self: Parser) => self.parseFreespace(),
    'GLOBAL': (self: Parser) => self.expression(GlobalPropertyExpr, {}),
    'HEAP': (self: Parser) => self.expression(HeapPropertyExpr, {}),
    'ICEBERG': (self: Parser) => self.expression(IcebergPropertyExpr, {}),
    'IMMUTABLE': (self: Parser) => self.expression(
      StabilityPropertyExpr,
      { this: LiteralExpr.string('IMMUTABLE') },
    ),
    'INHERITS': (self: Parser) => self.expression(
      InheritsPropertyExpr,
      { expressions: self.parseWrappedCsv(self.parseTable) },
    ),
    'INPUT': (self: Parser) => self.expression(InputModelPropertyExpr, { this: self.parseSchema() }),
    'JOURNAL': (self: Parser) => self.parseJournal(),
    'LANGUAGE': (self: Parser) => self.parsePropertyAssignment(LanguagePropertyExpr),
    'LAYOUT': (self: Parser) => self.parseDictProperty({ this: 'LAYOUT' }),
    'LIFETIME': (self: Parser) => self.parseDictRange({ this: 'LIFETIME' }),
    'LIKE': (self: Parser) => self.parseCreateLike(),
    'LOCATION': (self: Parser) => self.parsePropertyAssignment(LocationPropertyExpr),
    'LOCK': (self: Parser) => self.parseLocking(),
    'LOCKING': (self: Parser) => self.parseLocking(),
    'LOG': (self: Parser) => self.parseLog(),
    'MATERIALIZED': (self: Parser) => self.expression(MaterializedPropertyExpr, {}),
    'MERGEBLOCKRATIO': (self: Parser) => self.parseMergeblockratio(),
    'MODIFIES': (self: Parser) => self.parseModifiesProperty(),
    'MULTISET': (self: Parser) => self.expression(SetPropertyExpr, { multi: true }),
    'NO': (self: Parser) => self.parseNoProperty(),
    'ON': (self: Parser) => self.parseOnProperty(),
    'ORDER BY': (self: Parser) => self.parseOrder({ skipOrderToken: true }),
    'OUTPUT': (self: Parser) => self.expression(OutputModelPropertyExpr, { this: self.parseSchema() }),
    'PARTITION': (self: Parser) => self.parsePartitionedOf(),
    'PARTITION BY': (self: Parser) => self.parsePartitionedBy(),
    'PARTITIONED BY': (self: Parser) => self.parsePartitionedBy(),
    'PARTITIONED_BY': (self: Parser) => self.parsePartitionedBy(),
    'PRIMARY KEY': (self: Parser) => self.parsePrimaryKey({ inProps: true }),
    'RANGE': (self: Parser) => self.parseDictRange({ this: 'RANGE' }),
    'READS': (self: Parser) => self.parseReadsProperty(),
    'REMOTE': (self: Parser) => self.parseRemoteWithConnection(),
    'RETURNS': (self: Parser) => self.parseReturns(),
    'STRICT': (self: Parser) => self.expression(StrictPropertyExpr, {}),
    'STREAMING': (self: Parser) => self.expression(StreamingTablePropertyExpr, {}),
    'ROW': (self: Parser) => self.parseRow(),
    'ROW_FORMAT': (self: Parser) => self.parsePropertyAssignment(RowFormatPropertyExpr),
    'SAMPLE': (self: Parser) => self.expression(
      SamplePropertyExpr,
      { this: self._matchTextSeq('BY') && self.parseBitwise() },
    ),
    'SECURE': (self: Parser) => self.expression(SecurePropertyExpr, {}),
    'SECURITY': (self: Parser) => self.parseSecurity(),
    'SET': (self: Parser) => self.expression(SetPropertyExpr, { multi: false }),
    'SETTINGS': (self: Parser) => self.parseSettingsProperty(),
    'SHARING': (self: Parser) => self.parsePropertyAssignment(SharingPropertyExpr),
    'SORTKEY': (self: Parser) => self.parseSortkey(),
    'SOURCE': (self: Parser) => self.parseDictProperty({ this: 'SOURCE' }),
    'STABLE': (self: Parser) => self.expression(
      StabilityPropertyExpr,
      { this: LiteralExpr.string('STABLE') },
    ),
    'STORED': (self: Parser) => self.parseStored(),
    'SYSTEM_VERSIONING': (self: Parser) => self.parseSystemVersioningProperty(),
    'TBLPROPERTIES': (self: Parser) => self.parseWrappedProperties(),
    'TEMP': (self: Parser) => self.expression(TemporaryPropertyExpr, {}),
    'TEMPORARY': (self: Parser) => self.expression(TemporaryPropertyExpr, {}),
    'TO': (self: Parser) => self.parseToTable(),
    'TRANSIENT': (self: Parser) => self.expression(TransientPropertyExpr, {}),
    'TRANSFORM': (self: Parser) => self.expression(
      TransformModelPropertyExpr,
      { expressions: self.parseWrappedCsv(self.parseExpression) },
    ),
    'TTL': (self: Parser) => self.parseTtl(),
    'USING': (self: Parser) => self.parsePropertyAssignment(FileFormatPropertyExpr),
    'UNLOGGED': (self: Parser) => self.expression(UnloggedPropertyExpr, {}),
    'VOLATILE': (self: Parser) => self.parseVolatileProperty(),
    'WITH': (self: Parser) => self.parseWithProperty(),
  };

  static CONSTRAINT_PARSERS = {
    'AUTOINCREMENT': (self: Parser) => self.parseAutoIncrement(),
    'AUTO_INCREMENT': (self: Parser) => self.parseAutoIncrement(),
    'CASESPECIFIC': (self: Parser) => self.expression(CaseSpecificColumnConstraintExpr, { not_: false }),
    'CHARACTER SET': (self: Parser) => self.expression(
      CharacterSetColumnConstraintExpr,
      { this: self.parseVarOrString() },
    ),
    'CHECK': (self: Parser) => self.parseCheckConstraint(),
    'COLLATE': (self: Parser) => self.expression(
      CollateColumnConstraintExpr,
      { this: self.parseIdentifier() || self.parseColumn() },
    ),
    'COMMENT': (self: Parser) => self.expression(
      CommentColumnConstraintExpr,
      { this: self.parseString() },
    ),
    'COMPRESS': (self: Parser) => self.parseCompress(),
    'CLUSTERED': (self: Parser) => self.expression(
      ClusteredColumnConstraintExpr,
      { this: self.parseWrappedCsv(self.parseOrdered) },
    ),
    'NONCLUSTERED': (self: Parser) => self.expression(
      NonClusteredColumnConstraintExpr,
      { this: self.parseWrappedCsv(self.parseOrdered) },
    ),
    'DEFAULT': (self: Parser) => self.expression(
      DefaultColumnConstraintExpr,
      { this: self.parseBitwise() },
    ),
    'ENCODE': (self: Parser) => self.expression(EncodeColumnConstraintExpr, { this: self.parseVar() }),
    'EPHEMERAL': (self: Parser) => self.expression(
      EphemeralColumnConstraintExpr,
      { this: self.parseBitwise() },
    ),
    'EXCLUDE': (self: Parser) => self.expression(
      ExcludeColumnConstraintExpr,
      { this: self.parseIndexParams() },
    ),
    'FOREIGN KEY': (self: Parser) => self.parseForeignKey(),
    'FORMAT': (self: Parser) => self.expression(
      DateFormatColumnConstraintExpr,
      { this: self.parseVarOrString() },
    ),
    'GENERATED': (self: Parser) => self.parseGeneratedAsIdentity(),
    'IDENTITY': (self: Parser) => self.parseAutoIncrement(),
    'INLINE': (self: Parser) => self.parseInline(),
    'LIKE': (self: Parser) => self.parseCreateLike(),
    'NOT': (self: Parser) => self.parseNotConstraint(),
    'NULL': (self: Parser) => self.expression(NotNullColumnConstraintExpr, { allowNull: true }),
    'ON': (self: Parser) => (
      self._match(TokenType.UPDATE)
      && self.expression(OnUpdateColumnConstraintExpr, { this: self.parseFunction() })
    )
    || self.expression(OnPropertyExpr, { this: self.parseIdVar() }),
    'PATH': (self: Parser) => self.expression(PathColumnConstraintExpr, { this: self.parseString() }),
    'PERIOD': (self: Parser) => self.parsePeriodForSystemTime(),
    'PRIMARY KEY': (self: Parser) => self.parsePrimaryKey(),
    'REFERENCES': (self: Parser) => self.parseReferences({ match: false }),
    'TITLE': (self: Parser) => self.expression(
      TitleColumnConstraintExpr,
      { this: self.parseVarOrString() },
    ),
    'TTL': (self: Parser) => self.expression(MergeTreeTTLExpr, { expressions: [self.parseBitwise()] }),
    'UNIQUE': (self: Parser) => self.parseUnique(),
    'UPPERCASE': (self: Parser) => self.expression(UppercaseColumnConstraintExpr),
    'WITH': (self: Parser) => self.expression(
      PropertiesExpr,
      { expressions: self.parseWrappedProperties() },
    ),
    'BUCKET': (self: Parser) => self.parsePartitionedByBucketOrTruncate(),
    'TRUNCATE': (self: Parser) => self.parsePartitionedByBucketOrTruncate(),
  };

  static ALTER_PARSERS = {
    'ADD': (self: Parser) => self.parseAlterTableAdd(),
    'AS': (self: Parser) => self.parseSelect(),
    'ALTER': (self: Parser) => self.parseAlterTableAlter(),
    'CLUSTER BY': (self: Parser) => self.parseCluster({ wrapped: true }),
    'DELETE': (self: Parser) => self.expression(DeleteExpr, { where: self.parseWhere() }),
    'DROP': (self: Parser) => self.parseAlterTableDrop(),
    'RENAME': (self: Parser) => self.parseAlterTableRename(),
    'SET': (self: Parser) => self.parseAlterTableSet(),
    'SWAP': (self: Parser) => self.expression(
      SwapTableExpr,
      { this: self._match(TokenType.WITH) && self.parseTable({ schema: true }) },
    ),
  };

  static ALTER_ALTER_PARSERS = {
    DISTKEY: (self: Parser) => self.parseAlterDiststyle(),
    DISTSTYLE: (self: Parser) => self.parseAlterDiststyle(),
    SORTKEY: (self: Parser) => self.parseAlterSortkey(),
    COMPOUND: (self: Parser) => self.parseAlterSortkey({ compound: true }),
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
    ANY: (self: Parser) => self.expression(AnyExpr, { this: self.parseBitwise() }),
    CASE: (self: Parser) => self.parseCase(),
    CONNECT_BY_ROOT: (self: Parser) => self.expression(
      ConnectByRootExpr,
      { this: self.parseColumn() },
    ),
    IF: (self: Parser) => self.parseIf(),
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
      ArgMaxExpr.sqlNames().map((name) => [name, (self: Parser) => self.parseMaxMinBy(ArgMaxExpr)]),
    ),
    ...Object.fromEntries(
      ArgMinExpr.sqlNames().map((name) => [name, (self: Parser) => self.parseMaxMinBy(ArgMinExpr)]),
    ),
    CAST: (self: Parser) => self.parseCast(self._constructor.STRICT_CAST),
    CEIL: (self: Parser) => self.parseCeilFloor(CeilExpr),
    CONVERT: (self: Parser) => self.parseConvert(self._constructor.STRICT_CAST),
    CHAR: (self: Parser) => self.parseChar(),
    CHR: (self: Parser) => self.parseChar(),
    DECODE: (self: Parser) => self.parseDecode(),
    EXTRACT: (self: Parser) => self.parseExtract(),
    FLOOR: (self: Parser) => self.parseCeilFloor(FloorExpr),
    GAP_FILL: (self: Parser) => self.parseGapFill(),
    INITCAP: (self: Parser) => self.parseInitcap(),
    JSON_OBJECT: (self: Parser) => self.parseJsonObject(),
    JSON_OBJECTAGG: (self: Parser) => self.parseJsonObject({ agg: true }),
    JSON_TABLE: (self: Parser) => self.parseJsonTable(),
    MATCH: (self: Parser) => self.parseMatchAgainst(),
    NORMALIZE: (self: Parser) => self.parseNormalize(),
    OPENJSON: (self: Parser) => self.parseOpenJson(),
    OVERLAY: (self: Parser) => self.parseOverlay(),
    POSITION: (self: Parser) => self.parsePosition(),
    SAFE_CAST: (self: Parser) => self.parseCast(false, { safe: true }),
    STRING_AGG: (self: Parser) => self.parseStringAgg(),
    SUBSTRING: (self: Parser) => self.parseSubstring(),
    TRIM: (self: Parser) => self.parseTrim(),
    TRY_CAST: (self: Parser) => self.parseCast(false, { safe: true }),
    TRY_CONVERT: (self: Parser) => self.parseConvert(false, { safe: true }),
    XMLELEMENT: (self: Parser) => self.parseXmlElement(),
    XMLTABLE: (self: Parser) => self.parseXmlTable(),
  };

  static QUERY_MODIFIER_PARSERS = {
    [TokenType.MATCH_RECOGNIZE]: (self: Parser): [string, Expression | undefined] => ['match', self.parseMatchRecognize()],
    [TokenType.PREWHERE]: (self: Parser): [string, Expression | undefined] => ['prewhere', self.parsePrewhere()],
    [TokenType.WHERE]: (self: Parser): [string, Expression | undefined] => ['where', self.parseWhere()],
    [TokenType.GROUP_BY]: (self: Parser): [string, Expression | undefined] => ['group', self.parseGroup()],
    [TokenType.HAVING]: (self: Parser): [string, Expression | undefined] => ['having', self.parseHaving()],
    [TokenType.QUALIFY]: (self: Parser): [string, Expression | undefined] => ['qualify', self.parseQualify()],
    [TokenType.WINDOW]: (self: Parser): [string, Expression[] | undefined] => ['windows', self.parseWindowClause()],
    [TokenType.ORDER_BY]: (self: Parser): [string, Expression | undefined] => ['order', self.parseOrder()],
    [TokenType.LIMIT]: (self: Parser): [string, Expression | undefined] => ['limit', self.parseLimit()],
    [TokenType.FETCH]: (self: Parser): [string, Expression | undefined] => ['limit', self.parseLimit()],
    [TokenType.OFFSET]: (self: Parser): [string, Expression | undefined] => ['offset', self.parseOffset()],
    [TokenType.FOR]: (self: Parser): [string, Expression[] | undefined] => ['locks', self.parseLocks()],
    [TokenType.LOCK]: (self: Parser): [string, Expression[] | undefined] => ['locks', self.parseLocks()],
    [TokenType.TABLE_SAMPLE]: (self: Parser): [string, Expression | undefined] => ['sample', self.parseTableSample({ asModifier: true })],
    [TokenType.USING]: (self: Parser): [string, Expression | undefined] => ['sample', self.parseTableSample({ asModifier: true })],
    [TokenType.CLUSTER_BY]: (self: Parser): [string, Expression | undefined] => ['cluster', self.parseSort(ClusterExpr, TokenType.CLUSTER_BY)],
    [TokenType.DISTRIBUTE_BY]: (self: Parser): [string, Expression | undefined] => ['distribute', self.parseSort(DistributeExpr, TokenType.DISTRIBUTE_BY)],
    [TokenType.SORT_BY]: (self: Parser): [string, Expression | undefined] => ['sort', self.parseSort(SortExpr, TokenType.SORT_BY)],
    [TokenType.CONNECT_BY]: (self: Parser): [string, Expression | undefined] => ['connect', self.parseConnect({ skipStartToken: true })],
    [TokenType.START_WITH]: (self: Parser): [string, Expression | undefined] => ['connect', self.parseConnect()],
  };

  static QUERY_MODIFIER_TOKENS = new Set(
    Object.keys(Parser.QUERY_MODIFIER_PARSERS) as TokenType[],
  );

  static SET_PARSERS = {
    GLOBAL: (self: Parser) => self.parseSetItemAssignment({ kind: 'GLOBAL' }),
    LOCAL: (self: Parser) => self.parseSetItemAssignment({ kind: 'LOCAL' }),
    SESSION: (self: Parser) => self.parseSetItemAssignment({ kind: 'SESSION' }),
    TRANSACTION: (self: Parser) => self.parseSetTransaction(),
  };

  static SHOW_PARSERS: Record<string, (self: Parser) => Expression> = {};

  static TYPE_LITERAL_PARSERS = {
    [DataTypeExprKind.JSON]: (self: Parser, thisArg: Expression, _: unknown) => self.expression(ParseJSONExpr, { this: thisArg }),
  };

  static TYPE_CONVERTERS: Partial<Record<DataTypeExprKind, (dataType: DataTypeExpr) => DataTypeExpr>> = {};

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
    for (const token of Parser.SET_OPERATIONS) {
      result.delete(token);
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
    ALL: (self: Parser) => self.parseAnalyzeColumns(),
    COMPUTE: (self: Parser) => self.parseAnalyzeStatistics(),
    DELETE: (self: Parser) => self.parseAnalyzeDelete(),
    DROP: (self: Parser) => self.parseAnalyzeHistogram(),
    ESTIMATE: (self: Parser) => self.parseAnalyzeStatistics(),
    LIST: (self: Parser) => self.parseAnalyzeList(),
    PREDICATE: (self: Parser) => self.parseAnalyzeColumns(),
    UPDATE: (self: Parser) => self.parseAnalyzeHistogram(),
    VALIDATE: (self: Parser) => self.parseAnalyzeValidate(),
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
  protected _prevComments: string[] | undefined;
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
      parseMethod: (self: Parser) => self.parseStatement(),
      rawTokens,
      sql,
    });
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
  findSql (start: Token, end: Token): string {
    return this.sql.slice(start.start ?? 0, (end.end ?? 0) + 1);
  }

  isConnected (): boolean {
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

  warnUnsupported (): void {
    if (this._tokens.length <= 1) {
      return;
    }

    // We use findSql because this.sql may comprise multiple chunks, and we're only
    // interested in emitting a warning for the one being currently processed.
    const sql = this.findSql(
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

  tryParse<T extends Expression | undefined> (
    parseMethod: () => T,
    options: { retreat?: boolean } = {},
  ): T | undefined {
    const { retreat = false } = options;
    const index = this._index;
    const errorLevel = this.errorLevel;

    this.errorLevel = ErrorLevel.IMMEDIATE;
    let result: T | undefined;
    try {
      result = parseMethod();
    } catch (error) {
      if (error instanceof ParseError) {
        result = undefined;
      } else {
        throw error;
      }
    } finally {
      if (!result || retreat) {
        this._retreat(index);
      }
      this.errorLevel = errorLevel;
    }

    return result;
  }

  parseParen (): Expression | undefined {
    if (!this._match(TokenType.L_PAREN)) {
      return undefined;
    }

    const comments = this._prevComments;
    const query = this.parseSelect();

    let expressions: Expression[];
    if (query) {
      expressions = [query];
    } else {
      expressions = this.parseCsv(() => this.parseExpression());
    }

    let thisExpr = seqGet(expressions, 0);

    if (!thisExpr && this._match(TokenType.R_PAREN, { advance: false })) {
      thisExpr = this.expression(TupleExpr, {});
    } else if (thisExpr && UNWRAPPED_QUERIES.some((cls) => thisExpr instanceof cls)) {
      thisExpr = this.parseSubquery(thisExpr, {
        parseAlias: false,
      });
    } else if (thisExpr instanceof SubqueryExpr || thisExpr instanceof ValuesExpr) {
      thisExpr = this.parseSubquery(this.parseQueryModifiers(this.parseSetOperations(thisExpr), {
        parseAlias: false,
      });
    } else if (1 < expressions.length || this._prev?.tokenType === TokenType.COMMA) {
      thisExpr = this.expression(TupleExpr, { expressions });
    } else {
      thisExpr = this.expression(ParenExpr, { this: thisExpr });
    }

    if (thisExpr && comments) {
      thisExpr.addComments(comments);
    }

    return thisExpr;
  }

  parseDrop (options: { exists?: boolean } = {}): DropExpr | CommandExpr {
    const { exists } = options;
    const start = this._prev;
    const temporary = this._match(TokenType.TEMPORARY);
    const materialized = this._matchTextSeq('MATERIALIZED');

    const kind = this._matchSet(this._constructor.CREATABLES) && this._prev?.text.toUpperCase();
    if (!kind) {
      return this.parseAsCommand(start);
    }

    const concurrently = this._matchTextSeq('CONCURRENTLY');
    const ifExists = exists || this.parseExists();

    let thisExpr: Expression | undefined;
    if (kind === 'COLUMN') {
      thisExpr = this.parseColumn();
    } else {
      thisExpr = this.parseTableParts({
        schema: true,
        isDbReference: this._prev?.tokenType === TokenType.SCHEMA,
      });
    }

    const cluster = this._match(TokenType.ON) ? this.parseOnProperty() : undefined;

    let expressions: Expression[] | undefined;
    if (this._match(TokenType.L_PAREN, { advance: false })) {
      expressions = this.parseWrappedCsv(this.parseTypes);
    }

    return this.expression(DropExpr, {
      exists: ifExists,
      this: thisExpr,
      expressions,
      kind: this._dialectConstructor.creatableKindMapping[kind] || kind,
      temporary,
      materialized,
      cascade: this._matchTextSeq('CASCADE'),
      constraints: this._matchTextSeq('CONSTRAINTS'),
      purge: this._matchTextSeq('PURGE'),
      cluster,
      concurrently,
    });
  }

  parseExists (options: { not?: boolean } = {}): boolean | undefined {
    const { not: notParam } = options;
    const result = (
      this._matchTextSeq('IF')
      && (!notParam || this._match(TokenType.NOT))
      && this._match(TokenType.EXISTS)
    );
    return result ? true : undefined;
  }

  parseCreate (): CreateExpr | CommandExpr {
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
    if (this._matchTextSeq(['CLUSTERED', 'COLUMNSTORE'])) {
      clustered = true;
    } else if (
      this._matchTextSeq(['NONCLUSTERED', 'COLUMNSTORE'])
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
      properties = this.parseProperties();
      createToken = this._matchSet(this._constructor.CREATABLES) && this._prev;

      if (!properties || !createToken) {
        return this.parseAsCommand(start);
      }
    }

    const concurrently = this._matchTextSeq('CONCURRENTLY');
    const exists = this.parseExists({ not: true });
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
      thisExpr = this.parseUserDefinedFunction({ kind: createToken.tokenType });

      // exp.Properties.Location.POST_SCHEMA ("schema" here is the UDF's type signature)
      extendProps(this.parseProperties());

      expression = (this._match(TokenType.ALIAS) && this.parseHeredoc()) || undefined;
      extendProps(this.parseProperties());

      if (!expression) {
        if (this._match(TokenType.COMMAND)) {
          expression = this.parseAsCommand(this._prev);
        } else {
          begin = this._match(TokenType.BEGIN);
          const return_ = this._matchTextSeq('RETURN');

          if (this._match(TokenType.STRING, { advance: false })) {
            // Takes care of BigQuery's JavaScript UDF definitions that end in an OPTIONS property
            // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_function_statement
            expression = this.parseString();
            extendProps(this.parseProperties());
          } else {
            expression = this.parseUserDefinedFunctionExpression();
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
        index = this.parseIdVar();
        anonymous = false;
      } else {
        index = undefined;
        anonymous = true;
      }

      thisExpr = this.parseIndex({
        index,
        anonymous,
      });
    } else if (this._constructor.DB_CREATABLES.has(createToken.tokenType)) {
      const tableParts = this.parseTableParts({
        schema: true,
        isDbReference: createToken.tokenType === TokenType.SCHEMA,
      });

      // exp.Properties.Location.POST_NAME
      this._match(TokenType.COMMA);
      extendProps(this.parseProperties({ before: true }));

      thisExpr = this.parseSchema({ this: tableParts });

      // exp.Properties.Location.POST_SCHEMA and POST_WITH
      extendProps(this.parseProperties());

      const hasAlias = this._match(TokenType.ALIAS);
      if (!this._matchSet(this._constructor.DDL_SELECT_TOKENS, { advance: false })) {
        // exp.Properties.Location.POST_ALIAS
        extendProps(this.parseProperties());
      }

      if (createToken.tokenType === TokenType.SEQUENCE) {
        expression = this.parseTypes();
        const props = this.parseProperties();

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
        expression = this.parseDdlSelect();

        // Some dialects also support using a table as an alias instead of a SELECT.
        // Here we fallback to this as an alternative.
        if (!expression && hasAlias) {
          expression = this.tryParse(() => this.parseTableParts());
        }
      }

      if (createToken.tokenType === TokenType.TABLE) {
        // exp.Properties.Location.POST_EXPRESSION
        extendProps(this.parseProperties());

        indexes = [];
        while (true) {
          const index = this.parseIndex();

          // exp.Properties.Location.POST_INDEX
          extendProps(this.parseProperties());
          if (!index) {
            break;
          } else {
            this._match(TokenType.COMMA);
            indexes.push(index);
          }
        }
      } else if (createToken.tokenType === TokenType.VIEW) {
        if (this._matchTextSeq([
          'WITH',
          'NO',
          'SCHEMA',
          'BINDING',
        ])) {
          noSchemaBinding = true;
        }
      } else if (
        createToken.tokenType === TokenType.SINK
        || createToken.tokenType === TokenType.SOURCE
      ) {
        extendProps(this.parseProperties());
      }

      const shallow = this._matchTextSeq('SHALLOW');

      if (this._matchTexts(this._constructor.CLONE_KEYWORDS)) {
        const copy = this._prev?.text.toLowerCase() === 'copy';
        clone = this.expression(CloneExpr, {
          this: this.parseTable({ schema: true }),
          shallow,
          copy,
        });
      }
    }

    if (
      this._curr
      && !this._matchSet(new Set([TokenType.R_PAREN, TokenType.COMMA]), { advance: false })
    ) {
      return this.parseAsCommand(start);
    }

    const createKindText = createToken.text.toUpperCase();
    return this.expression(CreateExpr, {
      this: thisExpr,
      kind: this._dialectConstructor.creatableKindMapping[createKindText] || createKindText,
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

  parseCommand (): CommandExpr {
    this.warnUnsupported();
    return this.expression(CommandExpr, {
      comments: this._prevComments,
      this: this._prev?.text.toUpperCase(),
      expression: this.parseString(),
    });
  }

  parseSequenceProperties (): SequencePropertiesExpr | undefined {
    const seq = new SequencePropertiesExpr({});

    const options: Expression[] = [];
    const index = this._index;

    while (this._curr) {
      this._match(TokenType.COMMA);
      if (this._matchTextSeq('INCREMENT')) {
        this._matchTextSeq('BY');
        this._matchTextSeq('=');
        seq.setArgKey('increment', this.parseTerm());
      } else if (this._matchTextSeq('MINVALUE')) {
        seq.setArgKey('minvalue', this.parseTerm());
      } else if (this._matchTextSeq('MAXVALUE')) {
        seq.setArgKey('maxvalue', this.parseTerm());
      } else if (this._match(TokenType.START_WITH) || this._matchTextSeq('START')) {
        this._matchTextSeq('=');
        seq.setArgKey('start', this.parseTerm());
      } else if (this._matchTextSeq('CACHE')) {
        // T-SQL allows empty CACHE which is initialized dynamically
        seq.setArgKey('cache', this.parseNumber() || true);
      } else if (this._matchTextSeq(['OWNED', 'BY'])) {
        // "OWNED BY NONE" is the default
        seq.setArgKey('owned', this._matchTextSeq('NONE') ? undefined : this.parseColumn());
      } else {
        const opt = this.parseVarFromOptions(this._constructor.CREATE_SEQUENCE, { raiseUnmatched: false });
        if (opt) {
          options.push(opt);
        } else {
          break;
        }
      }
    }

    seq.setArgKey('options', 0 < options.length ? options : undefined);
    return this._index === index ? undefined : seq;
  }

  parsePropertyBefore (): Expression | undefined {
    // only used for teradata currently
    this._match(TokenType.COMMA);

    const kwargs: Record<string, boolean | string> = {
      no: this._matchTextSeq('NO') || false,
      dual: this._matchTextSeq('DUAL') || false,
      before: this._matchTextSeq('BEFORE') || false,
      default: this._matchTextSeq('DEFAULT') || false,
      local: (this._matchTextSeq('LOCAL') && 'LOCAL')
        || (this._matchTextSeq(['NOT', 'LOCAL']) && 'NOT LOCAL')
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
        const result = parser(this, filteredKwargs);
        return Array.isArray(result) ? result[0] : result;
      } catch (error) {
        this.raiseError(`Cannot parse property '${this._prev!.text}'`);
      }
    }

    return undefined;
  }

  parseWrappedProperties (): Expression[] {
    return this.parseWrappedCsv(() => this.parseProperty());
  }

  parseProperty (): Expression | undefined {
    if (this._matchTexts(Object.keys(this._constructor.PROPERTY_PARSERS))) {
      const result = this._constructor.PROPERTY_PARSERS[this._prev!.text.toUpperCase()](this);
      return Array.isArray(result) ? result[0] : result;
    }

    if (this._match(TokenType.DEFAULT) && this._matchTexts(Object.keys(this._constructor.PROPERTY_PARSERS))) {
      const result = this._constructor.PROPERTY_PARSERS[this._prev!.text.toUpperCase()](this, { default: true });
      return Array.isArray(result) ? result[0] : result;
    }

    if (this._matchTextSeq(['COMPOUND', 'SORTKEY'])) {
      return this.parseSortkey({ compound: true });
    }

    if (this._matchTextSeq(['SQL', 'SECURITY'])) {
      return this.expression(
        SqlSecurityPropertyExpr,
        { this: this._matchTexts(['DEFINER', 'INVOKER']) && this._prev!.text.toUpperCase() },
      );
    }

    const index = this._index;

    const seqProps = this.parseSequenceProperties();
    if (seqProps) {
      return seqProps;
    }

    this._retreat(index);
    let key = this.parseColumn();

    if (!this._match(TokenType.EQ)) {
      this._retreat(index);
      return undefined;
    }

    // Transform the key to exp.Dot if it's dotted identifiers wrapped in exp.Column or to exp.Var otherwise
    if (key instanceof ColumnExpr) {
      key = (key.parts && 1 < key.parts.length) ? (key.toDot() || var_(key.name)) : var_(key.name);
    }

    let value = this.parseBitwise() || this.parseVar({ anyToken: true });

    // Transform the value to exp.Var if it was parsed as exp.Column(exp.Identifier())
    if (value instanceof ColumnExpr) {
      value = var_(value.name);
    }

    return this.expression(PropertyExpr, {
      this: key,
      value,
    });
  }

  parseStored (): FileFormatPropertyExpr | StorageHandlerPropertyExpr {
    if (this._matchTextSeq('BY')) {
      return this.expression(StorageHandlerPropertyExpr, { this: this.parseVarOrString() });
    }

    this._match(TokenType.ALIAS);
    const inputFormat = this._matchTextSeq('INPUTFORMAT') && this.parseString();
    const outputFormat = this._matchTextSeq('OUTPUTFORMAT') && this.parseString();

    return this.expression(
      FileFormatPropertyExpr,
      {
        this: (
          inputFormat || outputFormat
            ? this.expression(InputOutputFormatExpr, {
              inputFormat,
              outputFormat,
            })
            : this.parseVarOrString() || this.parseNumber() || this.parseIdVar()
        ),
        hiveFormat: true,
      },
    );
  }

  parseUnquotedField (): Expression | undefined {
    const field = this.parseField();
    if (field instanceof IdentifierExpr && !field.$quoted) {
      return var_(field);
    }

    return field;
  }

  parsePropertyAssignment<E extends Expression> (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expClass: new (args: any) => E,
    kwargs?: Record<string, unknown>,
  ): E {
    this._match(TokenType.EQ);
    this._match(TokenType.ALIAS);

    return this.expression(expClass, {
      this: this.parseUnquotedField(),
      ...kwargs,
    });
  }

  parseProperties (options?: { before?: boolean }): PropertiesExpr | undefined {
    const properties: Expression[] = [];
    while (true) {
      const prop = options?.before
        ? this.parsePropertyBefore()
        : this.parseProperty();
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

  parseFallback (options?: { no?: boolean }): FallbackPropertyExpr {
    return this.expression(
      FallbackPropertyExpr,
      {
        no: options?.no,
        protection: this._matchTextSeq('PROTECTION'),
      },
    );
  }

  parseSecurity (): SecurityPropertyExpr | undefined {
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

  parseSettingsProperty (): SettingsPropertyExpr {
    return this.expression(
      SettingsPropertyExpr,
      { expressions: this.parseCsv(() => this.parseAssignment()) },
    );
  }

  parseVolatileProperty (): VolatilePropertyExpr | StabilityPropertyExpr {
    let preVolatileToken: Token | undefined;
    if (2 <= this._index) {
      preVolatileToken = this._tokens[this._index - 2];
    }

    if (preVolatileToken && this._constructor.PRE_VOLATILE_TOKENS.has(preVolatileToken.tokenType)) {
      return new VolatilePropertyExpr({});
    }

    return this.expression(StabilityPropertyExpr, { this: LiteralExpr.string('VOLATILE') });
  }

  parseRetentionPeriod (): VarExpr {
    // Parse TSQL's HISTORY_RETENTION_PERIOD: {INFINITE | <number> DAY | DAYS | MONTH ...}
    const number = this.parseNumber();
    const numberStr = number ? `${number} ` : '';
    const unit = this.parseVar({ anyToken: true });
    return var_(`${numberStr}${unit}`);
  }

  parseSystemVersioningProperty (options?: { with_?: boolean }): WithSystemVersioningPropertyExpr {
    this._match(TokenType.EQ);
    const prop = this.expression(
      WithSystemVersioningPropertyExpr,
      {
        on: true,
        with_: options?.with_,
      },
    );

    if (this._matchTextSeq('OFF')) {
      prop.setArgKey('on', false);
      return prop;
    }

    this._match(TokenType.ON);
    if (this._match(TokenType.L_PAREN)) {
      while (this._curr && !this._match(TokenType.R_PAREN)) {
        if (this._matchTextSeq(['HISTORY_TABLE', '='])) {
          prop.setArgKey('this', this.parseTableParts());
        } else if (this._matchTextSeq(['DATA_CONSISTENCY_CHECK', '='])) {
          this._advance();
          prop.setArgKey('dataConsistency', this._prev?.text.toUpperCase());
        } else if (this._matchTextSeq(['HISTORY_RETENTION_PERIOD', '='])) {
          prop.setArgKey('retentionPeriod', this.parseRetentionPeriod());
        }

        this._match(TokenType.COMMA);
      }
    }

    return prop;
  }

  parseDataDeletionProperty (): DataDeletionPropertyExpr {
    this._match(TokenType.EQ);
    const on = this._matchTextSeq('ON') || !this._matchTextSeq('OFF');
    const prop = this.expression(DataDeletionPropertyExpr, { on });

    if (this._match(TokenType.L_PAREN)) {
      while (this._curr && !this._match(TokenType.R_PAREN)) {
        if (this._matchTextSeq(['FILTER_COLUMN', '='])) {
          prop.setArgKey('filterColumn', this.parseColumn());
        } else if (this._matchTextSeq(['RETENTION_PERIOD', '='])) {
          prop.setArgKey('retentionPeriod', this.parseRetentionPeriod());
        }

        this._match(TokenType.COMMA);
      }
    }

    return prop;
  }

  parseDistributedProperty (): DistributedByPropertyExpr {
    let kind = 'HASH';
    let expressions: Expression[] | undefined;
    if (this._matchTextSeq(['BY', 'HASH'])) {
      expressions = this.parseWrappedCsv(() => this.parseIdVar());
    } else if (this._matchTextSeq(['BY', 'RANDOM'])) {
      kind = 'RANDOM';
    }

    // If the BUCKETS keyword is not present, the number of buckets is AUTO
    let buckets: Expression | undefined;
    if (this._matchTextSeq('BUCKETS') && !this._matchTextSeq('AUTO')) {
      buckets = this.parseNumber();
    }

    return this.expression(
      DistributedByPropertyExpr,
      {
        expressions,
        kind,
        buckets,
        order: this.parseOrder(),
      },
    );
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  parseCompositeKeyProperty<E extends Expression> (exprType: new (args: any) => E): E {
    this._matchTextSeq('KEY');
    const expressions = this.parseWrappedIdVars();
    return this.expression(exprType, { expressions });
  }

  parseWithProperty (): Expression | Expression[] | undefined {
    if (this._matchTextSeq(['(', 'SYSTEM_VERSIONING'])) {
      const prop = this.parseSystemVersioningProperty({ with_: true });
      this._matchRParen();
      return prop;
    }

    if (this._match(TokenType.L_PAREN, { advance: false })) {
      return this.parseWrappedProperties();
    }

    if (this._matchTextSeq('JOURNAL')) {
      return this.parseWithjournaltable();
    }

    if (this._matchTexts(Object.keys(this._constructor.VIEW_ATTRIBUTES))) {
      return this.expression(ViewAttributePropertyExpr, { this: this._prev!.text.toUpperCase() });
    }

    if (this._matchTextSeq('DATA')) {
      return this.parseWithdata({ no: false });
    } else if (this._matchTextSeq(['NO', 'DATA'])) {
      return this.parseWithdata({ no: true });
    }

    if (this._match(TokenType.SERDE_PROPERTIES, { advance: false })) {
      return this.parseSerdeProperties({ with_: true });
    }

    if (this._match(TokenType.SCHEMA)) {
      return this.expression(
        WithSchemaBindingPropertyExpr,
        { this: this.parseVarFromOptions(this._constructor.SCHEMA_BINDING_OPTIONS) },
      );
    }

    if (this._matchTexts(Object.keys(this._constructor.PROCEDURE_OPTIONS), { advance: false })) {
      return this.expression(
        WithProcedureOptionsExpr,
        { expressions: this.parseCsv(() => this.parseProcedureOption()) },
      );
    }

    if (!this._next) {
      return undefined;
    }

    return this.parseWithisolatedloading();
  }

  parseProcedureOption (): Expression | undefined {
    if (this._matchTextSeq(['EXECUTE', 'AS'])) {
      return this.expression(
        ExecuteAsPropertyExpr,
        {
          this: this.parseVarFromOptions(this._constructor.EXECUTE_AS_OPTIONS, { raiseUnmatched: false })
            || this.parseString(),
        },
      );
    }

    return this.parseVarFromOptions(this._constructor.PROCEDURE_OPTIONS);
  }

  // https://dev.mysql.com/doc/refman/8.0/en/create-view.html
  parseDefiner (): DefinerPropertyExpr | undefined {
    this._match(TokenType.EQ);

    const user = this.parseIdVar();
    this._match(TokenType.PARAMETER);
    const host = this.parseIdVar() || (this._match(TokenType.MOD) && this._prev?.text);

    if (!user || !host) {
      return undefined;
    }

    return new DefinerPropertyExpr({ this: `${user}@${host}` });
  }

  parseWithjournaltable (): WithJournalTablePropertyExpr {
    this._match(TokenType.TABLE);
    this._match(TokenType.EQ);
    return this.expression(WithJournalTablePropertyExpr, { this: this.parseTableParts() });
  }

  parseLog (options?: { no?: boolean }): LogPropertyExpr {
    return this.expression(LogPropertyExpr, { no: options?.no });
  }

  parseJournal (kwargs?: Record<string, unknown>): JournalPropertyExpr {
    return this.expression(JournalPropertyExpr, kwargs);
  }

  parseChecksum (): ChecksumPropertyExpr {
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

  parseCluster (options?: { wrapped?: boolean }): ClusterExpr {
    return this.expression(
      ClusterExpr,
      {
        expressions: options?.wrapped
          ? this.parseWrappedCsv(() => this.parseOrdered())
          : this.parseCsv(() => this.parseOrdered()),
      },
    );
  }

  parseClusteredBy (): ClusteredByPropertyExpr {
    this._matchTextSeq('BY');

    this._matchLParen();
    const expressions = this.parseCsv(() => this.parseColumn());
    this._matchRParen();

    let sortedBy: Expression[] | undefined;
    if (this._matchTextSeq(['SORTED', 'BY'])) {
      this._matchLParen();
      sortedBy = this.parseCsv(() => this.parseOrdered());
      this._matchRParen();
    }

    this._match(TokenType.INTO);
    const buckets = this.parseNumber();
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

  parseCopyProperty (): CopyGrantsPropertyExpr | undefined {
    if (!this._matchTextSeq('GRANTS')) {
      this._retreat(this._index - 1);
      return undefined;
    }

    return this.expression(CopyGrantsPropertyExpr, {});
  }

  parseFreespace (): FreespacePropertyExpr {
    this._match(TokenType.EQ);
    return this.expression(
      FreespacePropertyExpr,
      {
        this: this.parseNumber(),
        percent: this._match(TokenType.PERCENT),
      },
    );
  }

  parseMergeblockratio (options?: { no?: boolean;
    default?: boolean; }): MergeBlockRatioPropertyExpr {
    if (this._match(TokenType.EQ)) {
      return this.expression(
        MergeBlockRatioPropertyExpr,
        {
          this: this.parseNumber(),
          percent: this._match(TokenType.PERCENT),
        },
      );
    }

    return this.expression(MergeBlockRatioPropertyExpr, {
      no: options?.no,
      default: options?.default,
    });
  }

  parseDatablocksize (options?: {
    default?: boolean;
    minimum?: boolean;
    maximum?: boolean;
  }): DataBlocksizePropertyExpr {
    this._match(TokenType.EQ);
    const size = this.parseNumber();

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

  parseBlockcompression (): BlockCompressionPropertyExpr {
    this._match(TokenType.EQ);
    const always = this._matchTextSeq('ALWAYS');
    const manual = this._matchTextSeq('MANUAL');
    const never = this._matchTextSeq('NEVER');
    const default_ = this._matchTextSeq('DEFAULT');

    let autotemp: Expression | undefined;
    if (this._matchTextSeq('AUTOTEMP')) {
      autotemp = this.parseSchema();
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

  parseWithisolatedloading (): IsolatedLoadingPropertyExpr | undefined {
    const index = this._index;
    const no = this._matchTextSeq('NO');
    const concurrent = this._matchTextSeq('CONCURRENT');

    if (!this._matchTextSeq(['ISOLATED', 'LOADING'])) {
      this._retreat(index);
      return undefined;
    }

    const target = this.parseVarFromOptions(this._constructor.ISOLATED_LOADING_OPTIONS, { raiseUnmatched: false });
    return this.expression(
      IsolatedLoadingPropertyExpr,
      {
        no,
        concurrent,
        target,
      },
    );
  }

  parseLocking (): LockingPropertyExpr {
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
      thisExpr = this.parseTableParts();
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

  parsePartitionBy (): Expression[] {
    if (this._match(TokenType.PARTITION_BY)) {
      return this.parseCsv(() => this.parseDisjunction());
    }
    return [];
  }

  parsePartitionBoundSpec (): PartitionBoundSpecExpr {
    const parsePartitionBoundExpr = (): Expression | undefined => {
      if (this._matchTextSeq('MINVALUE')) {
        return var_('MINVALUE');
      }
      if (this._matchTextSeq('MAXVALUE')) {
        return var_('MAXVALUE');
      }
      return this.parseBitwise();
    };

    let thisExpr: Expression | Expression[] | undefined;
    let expression: Expression | undefined;
    let fromExpressions: Expression[] | undefined;
    let toExpressions: Expression[] | undefined;

    if (this._match(TokenType.IN)) {
      thisExpr = this.parseWrappedCsv(() => this.parseBitwise());
    } else if (this._match(TokenType.FROM)) {
      fromExpressions = this.parseWrappedCsv(parsePartitionBoundExpr);
      this._matchTextSeq('TO');
      toExpressions = this.parseWrappedCsv(parsePartitionBoundExpr);
    } else if (this._matchTextSeq([
      'WITH',
      '(',
      'MODULUS',
    ])) {
      thisExpr = this.parseNumber();
      this._matchTextSeq([',', 'REMAINDER']);
      expression = this.parseNumber();
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
  parsePartitionedOf (): PartitionedOfPropertyExpr | undefined {
    if (!this._matchTextSeq('OF')) {
      this._retreat(this._index - 1);
      return undefined;
    }

    const thisExpr = this.parseTable({ schema: true });

    let expression: VarExpr | PartitionBoundSpecExpr;
    if (this._match(TokenType.DEFAULT)) {
      expression = var_('DEFAULT');
    } else if (this._matchTextSeq(['FOR', 'VALUES'])) {
      expression = this.parsePartitionBoundSpec();
    } else {
      this.raiseError('Expecting either DEFAULT or FOR VALUES clause.');
      expression = var_('DEFAULT'); // fallback
    }

    return this.expression(PartitionedOfPropertyExpr, {
      this: thisExpr,
      expression,
    });
  }

  parsePartitionedBy (): PartitionedByPropertyExpr {
    this._match(TokenType.EQ);
    return this.expression(
      PartitionedByPropertyExpr,
      { this: this.parseSchema() || this.parseBracket(() => this.parseField()) },
    );
  }

  parseWithdata (options?: { no?: boolean }): WithDataPropertyExpr {
    let statistics: boolean | undefined;
    if (this._matchTextSeq(['AND', 'STATISTICS'])) {
      statistics = true;
    } else if (this._matchTextSeq([
      'AND',
      'NO',
      'STATISTICS',
    ])) {
      statistics = false;
    }

    return this.expression(WithDataPropertyExpr, {
      no: options?.no,
      statistics,
    });
  }

  parseContainsProperty (): SqlReadWritePropertyExpr | undefined {
    if (this._matchTextSeq('SQL')) {
      return this.expression(SqlReadWritePropertyExpr, { this: 'CONTAINS SQL' });
    }
    return undefined;
  }

  parseModifiesProperty (): SqlReadWritePropertyExpr | undefined {
    if (this._matchTextSeq(['SQL', 'DATA'])) {
      return this.expression(SqlReadWritePropertyExpr, { this: 'MODIFIES SQL DATA' });
    }
    return undefined;
  }

  parseNoProperty (): Expression | undefined {
    if (this._matchTextSeq(['PRIMARY', 'INDEX'])) {
      return new NoPrimaryIndexPropertyExpr({});
    }
    if (this._matchTextSeq('SQL')) {
      return this.expression(SqlReadWritePropertyExpr, { this: 'NO SQL' });
    }
    return undefined;
  }

  parseOnProperty (): Expression | undefined {
    if (this._matchTextSeq([
      'COMMIT',
      'PRESERVE',
      'ROWS',
    ])) {
      return new OnCommitPropertyExpr({});
    }
    if (this._matchTextSeq([
      'COMMIT',
      'DELETE',
      'ROWS',
    ])) {
      return new OnCommitPropertyExpr({ delete: true });
    }
    return this.expression(OnPropertyExpr, { this: this.parseSchema({ this: this.parseIdVar() }) });
  }

  parseReadsProperty (): SqlReadWritePropertyExpr | undefined {
    if (this._matchTextSeq(['SQL', 'DATA'])) {
      return this.expression(SqlReadWritePropertyExpr, { this: 'READS SQL DATA' });
    }
    return undefined;
  }

  parseDistkey (): DistKeyPropertyExpr {
    return this.expression(DistKeyPropertyExpr, { this: this.parseWrapped(() => this.parseIdVar()) });
  }

  parseCreateLike (): LikePropertyExpr | undefined {
    const table = this.parseTable({ schema: true });

    const options: Expression[] = [];
    while (this._matchTexts(['INCLUDING', 'EXCLUDING'])) {
      const thisText = this._prev!.text.toUpperCase();

      const idVar = this.parseIdVar();
      if (!idVar) {
        return undefined;
      }

      options.push(
        this.expression(PropertyExpr, {
          this: thisText,
          value: var_(idVar.this.toUpperCase()),
        }),
      );
    }

    return this.expression(LikePropertyExpr, {
      this: table,
      expressions: options,
    });
  }

  parseSortkey (options?: { compound?: boolean }): SortKeyPropertyExpr {
    return this.expression(
      SortKeyPropertyExpr,
      {
        this: this.parseWrappedIdVars(),
        compound: options?.compound,
      },
    );
  }

  parseCharacterSet (options?: { default?: boolean }): CharacterSetPropertyExpr {
    this._match(TokenType.EQ);
    return this.expression(
      CharacterSetPropertyExpr,
      {
        this: this.parseVarOrString(),
        default: options?.default,
      },
    );
  }

  parseRemoteWithConnection (): RemoteWithConnectionModelPropertyExpr {
    this._matchTextSeq(['WITH', 'CONNECTION']);
    return this.expression(
      RemoteWithConnectionModelPropertyExpr,
      { this: this.parseTableParts() },
    );
  }

  parseReturns (): ReturnsPropertyExpr {
    let value: Expression | undefined;
    let null_: boolean | undefined;
    const isTable = this._match(TokenType.TABLE);

    if (isTable) {
      if (this._match(TokenType.LT)) {
        value = this.expression(
          SchemaExpr,
          {
            this: 'TABLE',
            expressions: this.parseCsv(() => this.parseStructTypes()),
          },
        );
        if (!this._match(TokenType.GT)) {
          this.raiseError('Expecting >');
        }
      } else {
        value = this.parseSchema({ this: var_('TABLE') });
      }
    } else if (this._matchTextSeq([
      'NULL',
      'ON',
      'NULL',
      'INPUT',
    ])) {
      null_ = true;
      value = undefined;
    } else {
      value = this.parseTypes();
    }

    return this.expression(ReturnsPropertyExpr, {
      this: value,
      isTable,
      null: null_,
    });
  }

  parseDescribe (): DescribeExpr {
    const kind = this._matchSet(this._constructor.CREATABLES) && this._prev?.text;
    let style = this._matchTexts(Array.from(this._constructor.DESCRIBE_STYLES)) && this._prev?.text.toUpperCase();
    if (this._match(TokenType.DOT)) {
      style = undefined;
      this._retreat(this._index - 2);
    }

    const format = this._match(TokenType.FORMAT, { advance: false }) ? this.parseProperty() : undefined;

    let thisExpr: Expression;
    if (this._matchSet(new Set(Object.keys(this._constructor.STATEMENT_PARSERS) as TokenType[]), { advance: false })) {
      thisExpr = this.parseStatement();
    } else {
      thisExpr = this.parseTable({ schema: true });
    }

    const properties = this.parseProperties();
    const expressions = properties?.expressions;
    const partition = this.parsePartition();
    return this.expression(
      DescribeExpr,
      {
        this: thisExpr,
        style,
        kind,
        expressions,
        partition,
        format,
        asJson: this._matchTextSeq(['AS', 'JSON']),
      },
    );
  }

  parseMultitableInserts (comments?: string[]): MultitableInsertsExpr {
    const kind = this._prev!.text.toUpperCase();
    const expressions: Expression[] = [];

    const parseConditionalInsert = (): ConditionalInsertExpr | undefined => {
      let expression: Expression | undefined;
      if (this._match(TokenType.WHEN)) {
        expression = this.parseDisjunction();
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
              this: this.parseTable({ schema: true }),
              expression: this.parseDerivedTableValues(),
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
        source: this.parseTable(),
      },
    );
  }

  parseInsert (): InsertExpr | MultitableInsertsExpr {
    const comments: string[] = [];
    const hint = this.parseHint();
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
          this: this.parseVarOrString(),
          local,
          rowFormat: this.parseRowFormat({ matchRow: true }),
        },
      );
    } else {
      if (this._matchSet(new Set([TokenType.FIRST, TokenType.ALL]))) {
        comments.push(...ensureList(this._prevComments));
        return this.parseMultitableInserts(comments);
      }

      if (this._match(TokenType.OR)) {
        alternative = this._matchTexts(Array.from(this._constructor.INSERT_ALTERNATIVES)) && this._prev?.text;
      }

      this._match(TokenType.INTO);
      comments.push(...ensureList(this._prevComments));
      this._match(TokenType.TABLE);
      isFunction = this._match(TokenType.FUNCTION);

      thisExpr = isFunction ? this.parseFunction() : this.parseInsertTable();
    }

    const returning = this.parseReturning(); // TSQL allows RETURNING before source

    return this.expression(
      InsertExpr,
      {
        comments,
        hint,
        isFunction,
        this: thisExpr,
        stored: this._matchTextSeq('STORED') && this.parseStored(),
        byName: this._matchTextSeq(['BY', 'NAME']),
        exists: this.parseExists(),
        where: this._matchPair(TokenType.REPLACE, TokenType.WHERE) && this.parseDisjunction(),
        partition: this._match(TokenType.PARTITION_BY) && this.parsePartitionedBy(),
        settings: this._matchTextSeq('SETTINGS') && this.parseSettingsProperty(),
        default: this._matchTextSeq(['DEFAULT', 'VALUES']),
        expression: this.parseDerivedTableValues() || this.parseDdlSelect(),
        conflict: this.parseOnConflict(),
        returning: returning || this.parseReturning(),
        overwrite,
        alternative,
        ignore,
        source: this._match(TokenType.TABLE) && this.parseTable(),
      },
    );
  }

  parseInsertTable (): Expression | undefined {
    const thisExpr = this.parseTable({
      schema: true,
      parsePartition: true,
    });
    if (thisExpr instanceof TableExpr && this._match(TokenType.ALIAS, { advance: false })) {
      thisExpr.setArgKey('alias', this.parseTableAlias());
    }
    return thisExpr;
  }

  parseKill (): KillExpr {
    const kind = this._matchTexts(['CONNECTION', 'QUERY']) ? var_(this._prev!.text) : undefined;

    return this.expression(
      KillExpr,
      {
        this: this.parsePrimary(),
        kind,
      },
    );
  }

  parseOnConflict (): OnConflictExpr | undefined {
    const conflict = this._matchTextSeq(['ON', 'CONFLICT']);
    const duplicate = this._matchTextSeq([
      'ON',
      'DUPLICATE',
      'KEY',
    ]);

    if (!conflict && !duplicate) {
      return undefined;
    }

    let conflictKeys: Expression[] | undefined;
    let constraint: Expression | undefined;

    if (conflict) {
      if (this._matchTextSeq(['ON', 'CONSTRAINT'])) {
        constraint = this.parseIdVar();
      } else if (this._match(TokenType.L_PAREN)) {
        conflictKeys = this.parseCsv(() => this.parseIdVar());
        this._matchRParen();
      }
    }

    const indexPredicate = this.parseWhere();

    const action = this.parseVarFromOptions(this._constructor.CONFLICT_ACTIONS);
    let expressions: Expression[] | undefined;
    if (this._prev?.tokenType === TokenType.UPDATE) {
      this._match(TokenType.SET);
      expressions = this.parseCsv(() => this.parseEquality());
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
        where: this.parseWhere(),
      },
    );
  }

  parseReturning (): ReturningExpr | undefined {
    if (!this._match(TokenType.RETURNING)) {
      return undefined;
    }
    return this.expression(
      ReturningExpr,
      {
        expressions: this.parseCsv(() => this.parseExpression()),
        into: this._match(TokenType.INTO) && this.parseTablePart(),
      },
    );
  }

  parseRow (): RowFormatSerdePropertyExpr | RowFormatDelimitedPropertyExpr | undefined {
    if (!this._match(TokenType.FORMAT)) {
      return undefined;
    }
    return this.parseRowFormat();
  }

  parseSerdeProperties (options?: { with_?: boolean }): SerdePropertiesExpr | undefined {
    const index = this._index;
    const with_ = options?.with_ || this._matchTextSeq('WITH');

    if (!this._match(TokenType.SERDE_PROPERTIES)) {
      this._retreat(index);
      return undefined;
    }
    return this.expression(
      SerdePropertiesExpr,
      {
        expressions: this.parseWrappedProperties(),
        with: with_,
      },
    );
  }

  parseRowFormat (options?: {
    matchRow?: boolean;
  }): RowFormatSerdePropertyExpr | RowFormatDelimitedPropertyExpr | undefined {
    if (options?.matchRow && !this._matchPair(TokenType.ROW, TokenType.FORMAT)) {
      return undefined;
    }

    if (this._matchTextSeq('SERDE')) {
      const thisExpr = this.parseString();

      const serdeProperties = this.parseSerdeProperties();

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

    if (this._matchTextSeq([
      'FIELDS',
      'TERMINATED',
      'BY',
    ])) {
      kwargs.fields = this.parseString();
      if (this._matchTextSeq(['ESCAPED', 'BY'])) {
        kwargs.escaped = this.parseString();
      }
    }
    if (this._matchTextSeq([
      'COLLECTION',
      'ITEMS',
      'TERMINATED',
      'BY',
    ])) {
      kwargs.collectionItems = this.parseString();
    }
    if (this._matchTextSeq([
      'MAP',
      'KEYS',
      'TERMINATED',
      'BY',
    ])) {
      kwargs.mapKeys = this.parseString();
    }
    if (this._matchTextSeq([
      'LINES',
      'TERMINATED',
      'BY',
    ])) {
      kwargs.lines = this.parseString();
    }
    if (this._matchTextSeq([
      'NULL',
      'DEFINED',
      'AS',
    ])) {
      kwargs.null = this.parseString();
    }

    return this.expression(RowFormatDelimitedPropertyExpr, kwargs);
  }

  parseSelect (options?: {
    nested?: boolean;
    table?: boolean;
    parseSubqueryAlias?: boolean;
    parseSetOperation?: boolean;
    consumePipe?: boolean;
    from?: FromExpr;
  }): Expression | undefined {
    let query = this.parseSelectQuery({
      nested: options?.nested,
      table: options?.table,
      parseSubqueryAlias: options?.parseSubqueryAlias,
      parseSetOperation: options?.parseSetOperation,
    });

    if ((options?.consumePipe ?? true) && this._match(TokenType.PIPE_GT, { advance: false })) {
      if (!query && options?.from) {
        query = select('*').from(options.from);
      }
      if (query instanceof QueryExpr) {
        query = this.parsePipeSyntaxQuery(query);
        query = (query && options?.table) ? (query as SelectExpr).subquery({ copy: false }) : query;
      }
    }

    return query;
  }

  parseSelectQuery (options?: {
    nested?: boolean;
    table?: boolean;
    parseSubqueryAlias?: boolean;
    parseSetOperation?: boolean;
  }): Expression | undefined {
    const cte = this.parseWith();

    if (cte) {
      let thisExpr = this.parseStatement();

      if (!thisExpr) {
        this.raiseError('Failed to parse any statement following CTE');
        return cte;
      }

      while (thisExpr instanceof SubqueryExpr && thisExpr.isWrapper) {
        thisExpr = thisExpr.this;
      }

      if ('with' in thisExpr.argTypes) {
        thisExpr.setArgKey('with', cte);
      } else {
        this.raiseError(`${thisExpr.key} does not support CTE`);
        thisExpr = cte;
      }

      return thisExpr;
    }

    // duckdb supports leading with FROM x
    let from: FromExpr | undefined;
    if (this._match(TokenType.FROM, { advance: false })) {
      from = this.parseFrom({
        joins: true,
        consumePipe: true,
      }) as FromExpr | undefined;
    }

    if (this._match(TokenType.SELECT)) {
      const comments = this._prevComments;

      const hint = this.parseHint();

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
          { on: this._match(TokenType.ON) ? this.parseValue({ values: false }) : undefined },
        );
      }

      if (all_ && distinct) {
        this.raiseError('Cannot specify both ALL and DISTINCT after SELECT');
      }

      const operationModifiers: Expression[] = [];
      while (this._curr && this._matchTexts(Array.from(this._constructor.OPERATION_MODIFIERS))) {
        operationModifiers.push(var_(this._prev!.text.toUpperCase()));
      }

      const limit = this.parseLimit({ top: true });
      const projections = this.parseProjections();

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

      const into = this.parseInto();
      if (into) {
        thisExpr.setArgKey('into', into);
      }

      if (!from) {
        from = this.parseFrom() as FromExpr | undefined;
      }

      if (from) {
        thisExpr.setArgKey('from', from);
      }

      thisExpr = this.parseQueryModifiers(thisExpr) as SelectExpr;
      return options?.parseSetOperation ?? true ? this.parseSetOperations(thisExpr) : thisExpr;
    } else if ((options?.table || options?.nested) && this._match(TokenType.L_PAREN)) {
      const thisExpr = this.parseWrappedSelect({ table: options?.table });

      // We return early here so that the UNION isn't attached to the subquery by the
      // following call to _parse_set_operations, but instead becomes the parent node
      this._matchRParen();
      return this.parseSubquery(thisExpr, { parseAlias: options?.parseSubqueryAlias });
    } else if (this._match(TokenType.VALUES, { advance: false })) {
      return this.parseDerivedTableValues();
    } else if (from) {
      return select('*').from(from.this, { copy: false });
    } else if (this._match(TokenType.SUMMARIZE)) {
      const table = this._match(TokenType.TABLE);
      const thisExpr = this.parseSelect() || this.parseString() || this.parseTable();
      return this.expression(SummarizeExpr, {
        this: thisExpr,
        table,
      });
    } else if (this._match(TokenType.DESCRIBE)) {
      return this.parseDescribe();
    }

    return options?.parseSetOperation ?? true ? this.parseSetOperations(undefined) : undefined;
  }

  parseRecursiveWithSearch (): RecursiveWithSearchExpr | undefined {
    this._matchTextSeq('SEARCH');

    const kind = this._matchTexts(Array.from(this._constructor.RECURSIVE_CTE_SEARCH_KIND)) && this._prev?.text.toUpperCase();

    if (!kind) {
      return undefined;
    }

    this._matchTextSeq(['FIRST', 'BY']);

    return this.expression(
      RecursiveWithSearchExpr,
      {
        kind,
        this: this.parseIdVar(),
        expression: this._matchTextSeq('SET') && this.parseIdVar(),
        using: this._matchTextSeq('USING') && this.parseIdVar(),
      },
    );
  }

  parseWith (options?: { skipWithToken?: boolean }): WithExpr | undefined {
    if (!options?.skipWithToken && !this._match(TokenType.WITH)) {
      return undefined;
    }

    const comments = this._prevComments;
    const recursive = this._match(TokenType.RECURSIVE);

    let lastComments: string[] | undefined;
    const expressions: CTEExpr[] = [];
    while (true) {
      const cte = this.parseCte();
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
        search: this.parseRecursiveWithSearch(),
      },
    );
  }

  parseCte (): CTEExpr | undefined {
    const index = this._index;

    const alias = this.parseTableAlias({ aliasTokens: this._constructor.ID_VAR_TOKENS });
    if (!alias || !alias.this) {
      this.raiseError('Expected CTE to have alias');
    }

    const keyExpressions = this._matchTextSeq(['USING', 'KEY'])
      ? this.parseWrappedIdVars()
      : undefined;

    if (!this._match(TokenType.ALIAS) && !this._constructor.OPTIONAL_ALIAS_TOKEN_CTE) {
      this._retreat(index);
      return undefined;
    }

    const comments = this._prevComments;

    let materialized: boolean | undefined;
    if (this._matchTextSeq(['NOT', 'MATERIALIZED'])) {
      materialized = false;
    } else if (this._matchTextSeq('MATERIALIZED')) {
      materialized = true;
    }

    const cte = this.expression(
      CTEExpr,
      {
        this: this.parseWrapped(() => this.parseStatement()),
        alias,
        materialized,
        keyExpressions,
        comments,
      },
    );

    const values = cte.this;
    if (values instanceof ValuesExpr) {
      if (values.alias) {
        cte.setArgKey('this', select('*').from(values));
      } else {
        cte.setArgKey('this', select('*').from(
          values.alias('_values', { table: true }),
        ));
      }
    }

    return cte;
  }

  parseTableAlias (options?: {
    aliasTokens?: Set<TokenType>;
  }): TableAliasExpr | undefined {
    // In some dialects, LIMIT and OFFSET can act as both identifiers and keywords (clauses)
    // so this section tries to parse the clause version and if it fails, it treats the token
    // as an identifier (alias)
    if (this.canParseLimitOrOffset()) {
      return undefined;
    }

    const anyToken = this._match(TokenType.ALIAS);
    const alias = (
      this.parseIdVar({
        anyToken,
        tokens: options?.aliasTokens || this._constructor.TABLE_ALIAS_TOKENS,
      })
      || this.parseStringAsIdentifier()
    );

    const index = this._index;
    let columns: Expression[] | undefined;
    if (this._match(TokenType.L_PAREN)) {
      columns = this.parseCsv(() => this.parseFunctionParameter());
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

  parseSubquery (
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
        pivots: this.parsePivots(),
        alias: (options?.parseAlias ?? true) ? this.parseTableAlias() : undefined,
        sample: this.parseTableSample(),
      },
    );
  }

  protected _implicitUnnestToExplicit<E extends Expression> (thisExpr: E): E {
    const refs = new Set<string>();
    const args: Record<string, unknown> = thisExpr.args;
    if ('from' in args) {
      const fromExpr: FromExpr | undefined = args['from'];
      if (fromExpr?.this) {
        // Normalize and get alias/name
        const normalized = fromExpr.this; // Simplified - full normalization would use optimizer
        refs.add(normalized.aliasOrName || '');
      }
    }

    if ('joins' in args) {
      const joins: JoinExpr[] | undefined = args['joins'];
      if (joins) {
        for (const join of joins) {
          const table = join.this;
          if (table instanceof TableExpr && !join.args.on) {
            const normalized = table; // Simplified
            const tableName = normalized.parts?.[0]?.name || table.aliasOrName;

            if (tableName && refs.has(tableName)) {
              const tableAsColumn = table.toColumn();
              if (tableAsColumn) {
                const unnest = new UnnestExpr({ expressions: [tableAsColumn] });

                if (table.args.alias instanceof TableAliasExpr) {
                  tableAsColumn.replace(tableAsColumn.this);
                  const aliasArgs: Record<string, unknown> = {
                    table: [table.args.alias.this],
                    copy: false,
                  };
                  if ('alias' in unnest && typeof unnest.alias === 'function') {
                    unnest.alias(undefined, aliasArgs);
                  }
                }

                table.replace(unnest);
              }
            }

            refs.add(tableName || '');
          }
        }
      }
    }

    return thisExpr;
  }

  parseQueryModifiers<E extends Expression> (thisExpr: E): E;
  parseQueryModifiers (thisExpr: undefined): undefined;
  parseQueryModifiers (thisExpr: unknown): unknown {
    if (thisExpr && this._constructor.MODIFIABLES.includes(thisExpr.constructor)) {
      for (const join of this.parseJoins()) {
        thisExpr.append('joins', join);
      }

      let lateral = this.parseLateral();
      while (lateral) {
        thisExpr.append('laterals', lateral);
        lateral = this.parseLateral();
      }

      while (true) {
        if (this._matchSet(new Set(Object.keys(this._constructor.QUERY_MODIFIER_PARSERS) as TokenType[]), { advance: false })) {
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

            thisExpr.setArgKey(key, expression);
            if (key === 'limit') {
              const offset = expression.args.offset;
              expression.setArgKey('offset', undefined);

              if (offset) {
                const offsetExpr = new OffsetExpr({ expression: offset });
                thisExpr.setArgKey('offset', offsetExpr);

                const limitByExpressions = expression.expressions;
                expression.setArgKey('expressions', undefined);
                offsetExpr.setArgKey('expressions', limitByExpressions);
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

  parseHintFallbackToString (): HintExpr | undefined {
    const start = this._curr;
    while (this._curr) {
      this._advance();
    }

    const end = this._tokens[this._index - 1];
    return new HintExpr({ expressions: [this.findSql(start, end)] });
  }

  parseHintFunctionCall (): Expression | undefined {
    return this.parseFunctionCall();
  }

  parseHintBody (): HintExpr | undefined {
    const startIndex = this._index;
    let shouldFallbackToString = false;

    const hints: Expression[] = [];
    try {
      let hintBatch = this.parseCsv(() =>
        this.parseHintFunctionCall() || this.parseVar({ upper: true }));
      while (0 < hintBatch.length) {
        hints.push(...hintBatch);
        hintBatch = this.parseCsv(() =>
          this.parseHintFunctionCall() || this.parseVar({ upper: true }));
      }
    } catch (error) {
      shouldFallbackToString = true;
    }

    if (shouldFallbackToString || this._curr) {
      this._retreat(startIndex);
      return this.parseHintFallbackToString();
    }

    return this.expression(HintExpr, { expressions: hints });
  }

  parseHint (): HintExpr | undefined {
    if (this._match(TokenType.HINT) && this._prevComments && 0 < this._prevComments.length) {
      // Parse hint from comment
      return HintExpr.maybeParse?.(
        this._prevComments[0],
        {
          into: HintExpr,
          dialect: this.dialect,
        },
      );
    }

    return undefined;
  }

  parseInto (): IntoExpr | undefined {
    if (!this._match(TokenType.INTO)) {
      return undefined;
    }

    const temp = this._match(TokenType.TEMPORARY);
    const unlogged = this._matchTextSeq('UNLOGGED');
    this._match(TokenType.TABLE);

    return this.expression(
      IntoExpr,
      {
        this: this.parseTable({ schema: true }),
        temporary: temp,
        unlogged,
      },
    );
  }

  parseFrom (options?: {
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
        this: this.parseTable({
          joins: options?.joins,
          consumePipe: options?.consumePipe,
        }),
      },
    );
  }

  parseMatchRecognizeMeasure (): MatchRecognizeMeasureExpr {
    return this.expression(
      MatchRecognizeMeasureExpr,
      {
        windowFrame: this._matchTexts(['FINAL', 'RUNNING']) && this._prev!.text.toUpperCase(),
        this: this.parseExpression(),
      },
    );
  }

  parseMatchRecognize (): MatchRecognizeExpr | undefined {
    if (!this._match(TokenType.MATCH_RECOGNIZE)) {
      return undefined;
    }

    this._matchLParen();

    const partition = this.parsePartitionBy();
    const order = this.parseOrder();

    const measures = this._matchTextSeq('MEASURES')
      ? this.parseCsv(() => this.parseMatchRecognizeMeasure())
      : undefined;

    let rows: VarExpr | undefined;
    if (this._matchTextSeq([
      'ONE',
      'ROW',
      'PER',
      'MATCH',
    ])) {
      rows = var_('ONE ROW PER MATCH');
    } else if (this._matchTextSeq([
      'ALL',
      'ROWS',
      'PER',
      'MATCH',
    ])) {
      let text = 'ALL ROWS PER MATCH';
      if (this._matchTextSeq([
        'SHOW',
        'EMPTY',
        'MATCHES',
      ])) {
        text += ' SHOW EMPTY MATCHES';
      } else if (this._matchTextSeq([
        'OMIT',
        'EMPTY',
        'MATCHES',
      ])) {
        text += ' OMIT EMPTY MATCHES';
      } else if (this._matchTextSeq([
        'WITH',
        'UNMATCHED',
        'ROWS',
      ])) {
        text += ' WITH UNMATCHED ROWS';
      }
      rows = var_(text);
    }

    let after: VarExpr | undefined;
    if (this._matchTextSeq([
      'AFTER',
      'MATCH',
      'SKIP',
    ])) {
      let text = 'AFTER MATCH SKIP';
      if (this._matchTextSeq([
        'PAST',
        'LAST',
        'ROW',
      ])) {
        text += ' PAST LAST ROW';
      } else if (this._matchTextSeq([
        'TO',
        'NEXT',
        'ROW',
      ])) {
        text += ' TO NEXT ROW';
      } else if (this._matchTextSeq(['TO', 'FIRST'])) {
        this._advance();
        text += ` TO FIRST ${this._prev?.text}`;
      } else if (this._matchTextSeq(['TO', 'LAST'])) {
        this._advance();
        text += ` TO LAST ${this._prev?.text}`;
      }
      after = var_(text);
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

      pattern = var_(this.findSql(start, end));
    }

    const define = this._matchTextSeq('DEFINE')
      ? this.parseCsv(() => this.parseNameAsExpression())
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
        alias: this.parseTableAlias(),
      },
    );
  }

  parseLateral (): LateralExpr | undefined {
    let crossApply: boolean | undefined = this._matchPair(TokenType.CROSS, TokenType.APPLY);
    if (!crossApply && this._matchPair(TokenType.OUTER, TokenType.APPLY)) {
      crossApply = false;
    }

    let thisExpr: Expression | undefined;
    let view: boolean | undefined;
    let outer: boolean | undefined;

    if (crossApply !== undefined) {
      thisExpr = this.parseSelect({ table: true });
      view = undefined;
      outer = undefined;
    } else if (this._match(TokenType.LATERAL)) {
      thisExpr = this.parseSelect({ table: true });
      view = this._match(TokenType.VIEW);
      outer = this._match(TokenType.OUTER);
    } else {
      return undefined;
    }

    if (!thisExpr) {
      thisExpr = (
        this.parseUnnest()
        || this.parseFunction()
        || this.parseIdVar({ anyToken: false })
      );

      while (this._match(TokenType.DOT)) {
        const expression = this.parseFunction() || this.parseIdVar({ anyToken: false });
        if (expression) {
          thisExpr = new DotExpr({
            this: thisExpr,
            expression,
          });
        } else {
          break;
        }
      }
    }

    let ordinality: boolean | undefined;
    let tableAlias: TableAliasExpr | undefined;

    if (view) {
      const table = this.parseIdVar({ anyToken: false });
      const columns = this._match(TokenType.ALIAS)
        ? this.parseCsv(() => this.parseIdVar())
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
      tableAlias = this.parseTableAlias();
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

  parseStream (): StreamExpr | undefined {
    const index = this._index;
    if (this._matchTextSeq('STREAM')) {
      const thisExpr = this._parse({
        parseMethod: (self: Parser) => self.parseTable(),
        rawTokens: this._tokens,
      });
      if (thisExpr) {
        return this.expression(StreamExpr, { this: thisExpr });
      }
    }

    this._retreat(index);
    return undefined;
  }

  parseJoinParts (): [Token | undefined, Token | undefined, Token | undefined] {
    return [
      this._matchSet(this._constructor.JOIN_METHODS) ? this._prev : undefined,
      this._matchSet(this._constructor.JOIN_SIDES) ? this._prev : undefined,
      this._matchSet(this._constructor.JOIN_KINDS) ? this._prev : undefined,
    ];
  }

  parseUsingIdentifiers (): Expression[] {
    const parseColumnAsIdentifier = (): Expression | undefined => {
      const thisExpr = this.parseColumn();
      if (thisExpr instanceof ColumnExpr) {
        return thisExpr.this;
      }
      return thisExpr;
    };

    return this.parseWrappedCsv(parseColumnAsIdentifier, { optional: true });
  }

  parseJoin (options?: {
    skipJoinToken?: boolean;
    parseBracket?: boolean;
  }): JoinExpr | undefined {
    if (this._match(TokenType.COMMA)) {
      const table = this._parse({
        parseMethod: (self: Parser) => self.parseTable(),
        rawTokens: this._tokens,
      });
      const crossJoin = table ? this.expression(JoinExpr, { this: table }) : undefined;

      if (crossJoin && this._constructor.JOINS_HAVE_EQUAL_PRECEDENCE) {
        crossJoin.setArgKey('kind', 'CROSS');
      }

      return crossJoin;
    }

    const index = this._index;
    const [
      method,
      side,
      kind,
    ] = this.parseJoinParts();
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

    const kwargs: Record<string, unknown> = {
      this: this.parseTable({ parseBracket: options?.parseBracket }),
    };

    if (kind?.tokenType === TokenType.ARRAY && this._match(TokenType.COMMA)) {
      kwargs.expressions = this.parseCsv(() =>
        this.parseTable({ parseBracket: options?.parseBracket }));
    }

    if (method) kwargs.method = method.text.toUpperCase();
    if (side) kwargs.side = side.text.toUpperCase();
    if (kind) kwargs.kind = kind.text.toUpperCase();
    if (hint) kwargs.hint = hint;

    if (this._match(TokenType.MATCH_CONDITION)) {
      kwargs.matchCondition = this.parseWrapped(() => this.parseComparison());
    }

    if (this._match(TokenType.ON)) {
      kwargs.on = this.parseDisjunction();
    } else if (this._match(TokenType.USING)) {
      kwargs.using = this.parseUsingIdentifiers();
    } else if (
      !method
      && !(outerApply || crossApply)
      && !(kwargs.this instanceof UnnestExpr)
      && !(kind?.tokenType === TokenType.CROSS || kind?.tokenType === TokenType.ARRAY)
    ) {
      const nestedIndex = this._index;
      const joins = [...this.parseJoins()];

      if (0 < joins.length && this._match(TokenType.ON)) {
        kwargs.on = this.parseDisjunction();
      } else if (0 < joins.length && this._match(TokenType.USING)) {
        kwargs.using = this.parseUsingIdentifiers();
      } else {
        this._retreat(nestedIndex);
      }

      if (0 < joins.length && (kwargs.on || kwargs.using)) {
        kwargs.this.setArgKey('joins', joins);
      }
    }

    kwargs.pivots = this.parsePivots();

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
      kwargs.on = new BooleanExpr({ this: true });
    }

    if (directed) {
      kwargs.directed = directed;
    }

    return this.expression(JoinExpr, {
      comments,
      ...kwargs,
    });
  }

  parseOpclass (): Expression | undefined {
    const thisExpr = this.parseDisjunction();

    if (this._matchTexts(Array.from(this._constructor.OPCLASS_FOLLOW_KEYWORDS), { advance: false })) {
      return thisExpr;
    }

    if (!this._matchSet(this._constructor.OPTYPE_FOLLOW_TOKENS, { advance: false })) {
      return this.expression(OpclassExpr, {
        this: thisExpr,
        expression: this.parseTableParts(),
      });
    }

    return thisExpr;
  }

  parseIndexParams (): IndexParametersExpr {
    const using = this._match(TokenType.USING)
      ? this.parseVar({ anyToken: true })
      : undefined;

    const columns = this._match(TokenType.L_PAREN, { advance: false })
      ? this.parseWrappedCsv(() => this.parseWithOperator())
      : undefined;

    const include = this._matchTextSeq('INCLUDE')
      ? this.parseWrappedIdVars()
      : undefined;

    const partitionBy = this.parsePartitionBy();
    const withStorage = this._match(TokenType.WITH) && this.parseWrappedProperties();
    const tablespace = this._matchTextSeq([
      'USING',
      'INDEX',
      'TABLESPACE',
    ])
      ? this.parseVar({ anyToken: true })
      : undefined;

    const where = this.parseWhere();
    const on = this._match(TokenType.ON) ? this.parseField() : undefined;

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

  parseIndex (
    options: { index?: Expression;
      anonymous?: boolean; } = {},
  ): IndexExpr | undefined {
    const { anonymous = false } = options;
    let index = options.index;
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
      table = this.parseTableParts({ schema: true });
    } else {
      unique = this._match(TokenType.UNIQUE);
      primary = this._matchTextSeq('PRIMARY');
      amp = this._matchTextSeq('AMP');

      if (!this._match(TokenType.INDEX)) {
        return undefined;
      }

      index = this.parseIdVar();
      table = undefined;
    }

    const params = this.parseIndexParams();

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

  parseTableHints (): Expression[] | undefined {
    const hints: Expression[] = [];

    if (this._matchPair(TokenType.WITH, TokenType.L_PAREN)) {
      // https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table?view=sql-server-ver16
      hints.push(
        this.expression(
          WithTableHintExpr,
          {
            expressions: this.parseCsv(() =>
              this.parseFunction() || this.parseVar({ anyToken: true })),
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
          this.advanceAny();
          hint.setArgKey('target', this._prev!.text.toUpperCase());
        }

        hint.setArgKey('expressions', this.parseWrappedIdVars());
        hints.push(hint);
      }
    }

    return 0 < hints.length ? hints : undefined;
  }

  parseTablePart (options?: { schema?: boolean }): Expression | undefined {
    return (
      (!options?.schema && this.parseFunction({ optionalParens: false }))
      || this.parseIdVar({ anyToken: false })
      || this.parseStringAsIdentifier()
      || this.parsePlaceholder()
    );
  }

  parseTableParts (options?: {
    schema?: boolean;
    isDbReference?: boolean;
    wildcard?: boolean;
  }): TableExpr {
    let catalog: Expression | string | undefined;
    let db: Expression | string | undefined;
    let table: Expression | string | undefined = this.parseTablePart({ schema: options?.schema });

    while (this._match(TokenType.DOT)) {
      if (catalog) {
        // This allows nesting the table in arbitrarily many dot expressions if needed
        table = this.expression(
          DotExpr,
          {
            this: table,
            expression: this.parseTablePart({ schema: options?.schema }),
          },
        );
      } else {
        catalog = db;
        db = table;
        // "" used for tsql FROM a..b case
        table = this.parseTablePart({ schema: options?.schema }) || '';
      }
    }

    if (
      options?.wildcard
      && this.isConnected()
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

    const changes = this.parseChanges();
    if (changes) {
      tableExpr.setArgKey('changes', changes);
    }

    const atBefore = this.parseHistoricalData();
    if (atBefore) {
      tableExpr.setArgKey('when', atBefore);
    }

    const pivots = this.parsePivots();
    if (pivots) {
      tableExpr.setArgKey('pivots', pivots);
    }

    return tableExpr;
  }

  parseTable (options?: {
    schema?: boolean;
    joins?: boolean;
    aliasTokens?: Set<TokenType>;
    parseBracket?: boolean;
    isDbReference?: boolean;
    parsePartition?: boolean;
    consumePipe?: boolean;
  }): Expression | undefined {
    const stream = this.parseStream();
    if (stream) {
      return stream;
    }

    const lateral = this.parseLateral();
    if (lateral) {
      return lateral;
    }

    const unnest = this.parseUnnest();
    if (unnest) {
      return unnest;
    }

    const values = this.parseDerivedTableValues();
    if (values) {
      return values;
    }

    const subquery = this.parseSelect({
      table: true,
      consumePipe: options?.consumePipe,
    });
    if (subquery) {
      if (!subquery.args.pivots) {
        subquery.setArgKey('pivots', this.parsePivots());
      }
      return subquery;
    }

    let bracket = options?.parseBracket && this.parseBracket(undefined);
    bracket = bracket ? this.expression(TableExpr, { this: bracket }) : undefined;

    const rowsFrom = this._matchTextSeq(['ROWS', 'FROM']) && this.parseWrappedCsv(() => this.parseTable());
    const rowsFromExpr = rowsFrom ? this.expression(TableExpr, { rowsFrom }) : undefined;

    const only = this._match(TokenType.ONLY);

    const thisExpr: Expression = (
      bracket
      || rowsFromExpr
      || this.parseBracket(
        this.parseTableParts({
          schema: options?.schema,
          isDbReference: options?.isDbReference,
        }),
      )
    )!;

    if (only) {
      thisExpr.setArgKey('only', only);
    }

    // Postgres supports a wildcard (table) suffix operator, which is a no-op in this context
    this._matchTextSeq('*');

    const parsePartition = options?.parsePartition || this._constructor.SUPPORTS_PARTITION_SELECTION;
    if (parsePartition && this._match(TokenType.PARTITION, { advance: false })) {
      thisExpr.setArgKey('partition', this.parsePartition());
    }

    if (options?.schema) {
      return this.parseSchema({ this: thisExpr });
    }

    const version = this.parseVersion();
    if (version) {
      thisExpr.setArgKey('version', version);
    }

    if (this._dialectConstructor.ALIAS_POST_TABLESAMPLE) {
      thisExpr.setArgKey('sample', this.parseTableSample());
    }

    const alias = this.parseTableAlias({ aliasTokens: options?.aliasTokens || this._constructor.TABLE_ALIAS_TOKENS });
    if (alias) {
      thisExpr.setArgKey('alias', alias);
    }

    if (this._match(TokenType.INDEXED_BY)) {
      thisExpr.setArgKey('indexed', this.parseTableParts());
    } else if (this._matchTextSeq(['NOT', 'INDEXED'])) {
      thisExpr.setArgKey('indexed', false);
    }

    if (thisExpr instanceof TableExpr && this._matchTextSeq('AT')) {
      return this.expression(
        AtIndexExpr,
        {
          this: thisExpr.toColumn?.({ copy: false }),
          expression: this.parseIdVar(),
        },
      );
    }

    thisExpr.setArgKey('hints', this.parseTableHints());

    if (!thisExpr.args.pivots) {
      thisExpr.setArgKey('pivots', this.parsePivots());
    }

    if (!this._dialectConstructor.ALIAS_POST_TABLESAMPLE) {
      thisExpr.setArgKey('sample', this.parseTableSample());
    }

    if (options?.joins) {
      for (const join of this.parseJoins()) {
        thisExpr.append('joins', join);
      }
    }

    if (this._matchPair(TokenType.WITH, TokenType.ORDINALITY)) {
      thisExpr.setArgKey('ordinality', true);
      thisExpr.setArgKey('alias', this.parseTableAlias());
    }

    return thisExpr;
  }

  parseVersion (): VersionExpr | undefined {
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
      const start = this.parseBitwise();
      this._matchTexts(['TO', 'AND']);
      const end = this.parseBitwise();
      expression = this.expression(TupleExpr, { expressions: [start, end] });
    } else if (this._matchTextSeq(['CONTAINED', 'IN'])) {
      kind = 'CONTAINED IN';
      expression = this.expression(
        TupleExpr,
        { expressions: this.parseWrappedCsv(() => this.parseBitwise()) },
      );
    } else if (this._match(TokenType.ALL)) {
      kind = 'ALL';
      expression = undefined;
    } else {
      this._matchTextSeq(['AS', 'OF']);
      kind = 'AS OF';
      expression = this.parseType();
    }

    return this.expression(VersionExpr, {
      this: thisText,
      expression,
      kind,
    });
  }

  parseHistoricalData (): HistoricalDataExpr | undefined {
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
      const expression = this._match(TokenType.FARROW) && this.parseBitwise();

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

  parseChanges (): ChangesExpr | undefined {
    if (!this._matchTextSeq([
      'CHANGES',
      '(',
      'INFORMATION',
      '=>',
    ])) {
      return undefined;
    }

    const information = this.parseVar({ anyToken: true });
    this._matchRParen();

    return this.expression(
      ChangesExpr,
      {
        information,
        atBefore: this.parseHistoricalData(),
        end: this.parseHistoricalData(),
      },
    );
  }

  parseUnnest (options?: { withAlias?: boolean }): UnnestExpr | undefined {
    if (!this._matchPair(TokenType.UNNEST, TokenType.L_PAREN, { advance: false })) {
      return undefined;
    }

    this._advance();

    const expressions = this.parseWrappedCsv(() => this.parseEquality());
    let offset: Expression | boolean | undefined = this._matchPair(TokenType.WITH, TokenType.ORDINALITY);

    const alias = (options?.withAlias ?? true) ? this.parseTableAlias() : undefined;

    if (alias) {
      if (this._dialectConstructor.UNNEST_COLUMN_ONLY) {
        const columns = alias.args.columns;
        if (columns) {
          this.raiseError('Unexpected extra column alias in unnest.', alias);
        }

        alias.setArgKey('columns', [alias.this]);
        alias.setArgKey('this', undefined);
      }

      const columns = alias.args.columns as Expression[] | undefined;
      if (offset && columns && expressions.length < columns.length) {
        offset = columns.pop()!;
      }
    }

    if (!offset && this._matchPair(TokenType.WITH, TokenType.OFFSET)) {
      this._match(TokenType.ALIAS);
      offset = this.parseIdVar({
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

  parseDerivedTableValues (): ValuesExpr | undefined {
    const isDerived = this._matchPair(TokenType.L_PAREN, TokenType.VALUES);
    if (!isDerived && !(
      // ClickHouse's `FORMAT Values` is equivalent to `VALUES`
      this._matchTextSeq('VALUES') || this._matchTextSeq(['FORMAT', 'VALUES'])
    )) {
      return undefined;
    }

    const expressions = this.parseCsv(() => this.parseValue());
    const alias = this.parseTableAlias();

    if (isDerived) {
      this._matchRParen();
    }

    return this.expression(
      ValuesExpr,
      {
        expressions,
        alias: alias || this.parseTableAlias(),
      },
    );
  }

  parseTableSample (options?: { asModifier?: boolean }): TableSampleExpr | undefined {
    if (!this._match(TokenType.TABLE_SAMPLE) && !(
      options?.asModifier && this._matchTextSeq(['USING', 'SAMPLE'])
    )) {
      return undefined;
    }

    let bucketNumerator: Expression | undefined;
    let bucketDenominator: Expression | undefined;
    let bucketField: Expression | undefined;
    let percent: Expression | undefined;
    let size: Expression | undefined;
    let seed: Expression | undefined;

    let method = this.parseVar({
      tokens: new Set([TokenType.ROW]),
      upper: true,
    });
    const matchedLParen = this._match(TokenType.L_PAREN);

    let expressions: Expression[] | undefined;
    let num: Expression | undefined;

    if (this._constructor.TABLESAMPLE_CSV) {
      num = undefined;
      expressions = this.parseCsv(() => this.parsePrimary());
    } else {
      expressions = undefined;
      num = (
        this._match(TokenType.NUMBER, { advance: false })
          ? this.parseFactor()
          : this.parsePrimary() || this.parsePlaceholder()
      );
    }

    if (this._matchTextSeq('BUCKET')) {
      bucketNumerator = this.parseNumber();
      this._matchTextSeq(['OUT', 'OF']);
      bucketDenominator = this.parseNumber();
      this._match(TokenType.ON);
      bucketField = this.parseField();
    } else if (this._matchSet(new Set([TokenType.PERCENT, TokenType.MOD]))) {
      percent = num;
    } else if (this._match(TokenType.ROWS) || !this._dialectConstructor.TABLESAMPLE_SIZE_IS_PERCENT) {
      size = num;
    } else {
      percent = num;
    }

    if (matchedLParen) {
      this._matchRParen();
    }

    if (this._match(TokenType.L_PAREN)) {
      method = this.parseVar({ upper: true });
      seed = this._match(TokenType.COMMA) && this.parseNumber();
      this._matchRParen();
    } else if (this._matchTexts(['SEED', 'REPEATABLE'])) {
      seed = this.parseWrapped(() => this.parseNumber());
    }

    if (!method && this._constructor.DEFAULT_SAMPLING_METHOD) {
      method = var_(this._constructor.DEFAULT_SAMPLING_METHOD);
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

  parsePivots (): PivotExpr[] | undefined {
    const pivots: PivotExpr[] = [];
    let pivot = this.parsePivot();
    while (pivot) {
      pivots.push(pivot);
      pivot = this.parsePivot();
    }
    return 0 < pivots.length ? pivots : undefined;
  }

  parseJoins (): JoinExpr[] {
    const joins: JoinExpr[] = [];
    let join = this.parseJoin();
    while (join) {
      joins.push(join);
      join = this.parseJoin();
    }
    return joins;
  }

  parseUnpivotColumns (): UnpivotColumnsExpr | undefined {
    if (!this._match(TokenType.INTO)) {
      return undefined;
    }

    return this.expression(
      UnpivotColumnsExpr,
      {
        this: this._matchTextSeq('NAME') && this.parseColumn(),
        expressions: this._matchTextSeq('VALUE') && this.parseCsv(() => this.parseColumn()),
      },
    );
  }

  // https://duckdb.org/docs/sql/statements/pivot
  parseSimplifiedPivot (options: { isUnpivot?: boolean } = {}): PivotExpr {
    const { isUnpivot } = options;
    const parseOn = (): Expression | undefined => {
      const thisExpr = this.parseBitwise();

      if (this._match(TokenType.IN)) {
        // PIVOT ... ON col IN (row_val1, row_val2)
        return this.parseIn(thisExpr);
      }
      if (this._match(TokenType.ALIAS, { advance: false })) {
        // UNPIVOT ... ON (col1, col2, col3) AS row_val
        return this.parseAlias(thisExpr);
      }

      return thisExpr;
    };

    const thisExpr = this.parseTable();
    const expressions = this._match(TokenType.ON) && this.parseCsv(parseOn);
    const into = this.parseUnpivotColumns();
    const using = this._match(TokenType.USING) && this.parseCsv(() =>
      this.parseAlias(this.parseColumn()));
    const group = this.parseGroup();

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

  parsePivotIn (): InExpr {
    const parseAliasedExpression = (): Expression | undefined => {
      const thisExpr = this.parseSelectOrExpression();

      this._match(TokenType.ALIAS);
      const alias = this.parseBitwise();
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

    const value = this.parseColumn();

    if (!this._match(TokenType.IN)) {
      this.raiseError('Expecting IN', this._curr);
    }

    let exprs: Expression[];
    let field: Expression | undefined;

    if (this._match(TokenType.L_PAREN)) {
      if (this._match(TokenType.ANY)) {
        exprs = ensureList(new PivotAnyExpr({ this: this.parseOrder() }));
      } else {
        exprs = this.parseCsv(parseAliasedExpression);
      }
      this._matchRParen();
      return this.expression(InExpr, {
        this: value,
        expressions: exprs,
      });
    }

    field = this.parseIdVar();
    return this.expression(InExpr, {
      this: value,
      field,
    });
  }

  parsePivotAggregation (): Expression | undefined {
    const func = this.parseFunction();
    if (!func) {
      if (this._prev && this._prev.tokenType === TokenType.COMMA) {
        return undefined;
      }
      this.raiseError('Expecting an aggregation function in PIVOT', this._curr);
    }

    return this.parseAlias(func);
  }

  parsePivot (): PivotExpr | undefined {
    const index = this._index;
    let includeNulls: boolean | undefined;
    let unpivot: boolean;

    if (this._match(TokenType.PIVOT)) {
      unpivot = false;
    } else if (this._match(TokenType.UNPIVOT)) {
      unpivot = true;

      // https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-unpivot.html#syntax
      if (this._matchTextSeq(['INCLUDE', 'NULLS'])) {
        includeNulls = true;
      } else if (this._matchTextSeq(['EXCLUDE', 'NULLS'])) {
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
      expressions = this.parseCsv(() => this.parseColumn());
    } else {
      expressions = this.parseCsv(() => this.parsePivotAggregation());
    }

    if (expressions.length === 0) {
      this.raiseError('Failed to parse PIVOT\'s aggregation list', this._curr);
    }

    if (!this._match(TokenType.FOR)) {
      this.raiseError('Expecting FOR', this._curr);
    }

    const fields: InExpr[] = [];
    while (true) {
      const results = this._parse({
        parseMethod: (self: Parser) => self.parsePivotIn(),
        rawTokens: this._tokens,
      });
      const field = results[0];
      if (!field || !(field instanceof InExpr)) {
        break;
      }
      fields.push(field);
    }

    const defaultOnNull = this._matchTextSeq([
      'DEFAULT',
      'ON',
      'NULL',
    ]) && this.parseWrapped(() =>
      this.parseBitwise());

    const group = this.parseGroup();

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
      pivot.setArgKey('alias', this.parseTableAlias());
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
              this._constructor.IDENTIFY_PIVOT_STRINGS ? fld.sql() : fld.aliasOrName || ''),
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

      pivot.setArgKey('columns', columns);
    }

    return pivot;
  }

  protected _pivotColumnNames (aggregations: Expression[]): string[] {
    return aggregations.map((agg) => agg.alias).filter((alias) => alias);
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

  parsePrewhere (options?: { skipWhereToken?: boolean }): PreWhereExpr | undefined {
    if (!options?.skipWhereToken && !this._match(TokenType.PREWHERE)) {
      return undefined;
    }

    return this.expression(
      PreWhereExpr,
      {
        comments: this._prevComments,
        this: this.parseDisjunction(),
      },
    );
  }

  parseWhere (options?: { skipWhereToken?: boolean }): WhereExpr | undefined {
    if (!options?.skipWhereToken && !this._match(TokenType.WHERE)) {
      return undefined;
    }

    return this.expression(
      WhereExpr,
      {
        comments: this._prevComments,
        this: this.parseDisjunction(),
      },
    );
  }

  parseGroup (options?: { skipGroupByToken?: boolean }): GroupExpr | undefined {
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
        ...this.parseCsv(() =>
          this._matchSet(new Set([TokenType.CUBE, TokenType.ROLLUP]), { advance: false })
            ? undefined
            : this.parseDisjunction()),
      );

      const beforeWithIndex = this._index;
      const withPrefix = this._match(TokenType.WITH);

      const cubeOrRollup = this.parseCubeOrRollup({ withPrefix });
      if (cubeOrRollup) {
        const key = cubeOrRollup instanceof RollupExpr ? 'rollup' : 'cube';
        elements[key].push(cubeOrRollup);
      } else {
        const groupingSets = this.parseGroupingSets();
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

  parseCubeOrRollup (options?: { withPrefix?: boolean }): CubeExpr | RollupExpr | undefined {
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
      { expressions: options?.withPrefix ? [] : this.parseWrappedCsv(() => this.parseBitwise()) },
    );
  }

  parseGroupingSets (): GroupingSetsExpr | undefined {
    if (this._match(TokenType.GROUPING_SETS)) {
      return this.expression(
        GroupingSetsExpr,
        { expressions: this.parseWrappedCsv(() => this.parseGroupingSet()) },
      );
    }
    return undefined;
  }

  parseGroupingSet (): Expression | undefined {
    return this.parseGroupingSets() || this.parseCubeOrRollup() || this.parseBitwise();
  }

  parseHaving (options?: { skipHavingToken?: boolean }): HavingExpr | undefined {
    if (!options?.skipHavingToken && !this._match(TokenType.HAVING)) {
      return undefined;
    }
    return this.expression(
      HavingExpr,
      {
        comments: this._prevComments,
        this: this.parseDisjunction(),
      },
    );
  }

  parseQualify (): QualifyExpr | undefined {
    if (!this._match(TokenType.QUALIFY)) {
      return undefined;
    }
    return this.expression(QualifyExpr, { this: this.parseDisjunction() });
  }

  parseConnectWithPrior (): Expression | undefined {
    this._constructor.NO_PAREN_FUNCTION_PARSERS['PRIOR'] = (self: Parser) =>
      self.expression(PriorExpr, { this: self.parseBitwise() });
    const connect = this.parseDisjunction();
    delete this._constructor.NO_PAREN_FUNCTION_PARSERS['PRIOR'];
    return connect;
  }

  parseConnect (options?: { skipStartToken?: boolean }): ConnectExpr | undefined {
    let start: Expression | undefined;

    if (options?.skipStartToken) {
      start = undefined;
    } else if (this._match(TokenType.START_WITH)) {
      start = this.parseDisjunction();
    } else {
      return undefined;
    }

    this._match(TokenType.CONNECT_BY);
    const nocycle = this._matchTextSeq('NOCYCLE');
    const connect = this.parseConnectWithPrior();

    if (!start && this._match(TokenType.START_WITH)) {
      start = this.parseDisjunction();
    }

    return this.expression(ConnectExpr, {
      start,
      connect,
      nocycle,
    });
  }

  parseNameAsExpression (): Expression | undefined {
    let thisExpr: Expression | undefined = this.parseIdVar({ anyToken: true });
    if (this._match(TokenType.ALIAS)) {
      thisExpr = this.expression(AliasExpr, {
        alias: thisExpr,
        this: this.parseDisjunction(),
      });
    }
    return thisExpr;
  }

  parseInterpolate (): Expression[] | undefined {
    if (this._matchTextSeq('INTERPOLATE')) {
      return this.parseWrappedCsv(() => this.parseNameAsExpression());
    }
    return undefined;
  }

  parseOrder (
    options: { thisExpr?: Expression;
      skipOrderToken?: boolean; } = {},
  ): OrderExpr | undefined {
    const {
      thisExpr, skipOrderToken,
    } = options;
    let siblings: boolean | undefined;

    if (!skipOrderToken && !this._match(TokenType.ORDER_BY)) {
      if (!this._match(TokenType.ORDER_SIBLINGS_BY)) {
        return thisExpr as OrderExpr | undefined;
      }

      siblings = true;
    }

    return this.expression(
      OrderExpr,
      {
        comments: this._prevComments,
        this: thisExpr,
        expressions: this.parseCsv(() => this.parseOrdered()),
        siblings,
      },
    );
  }

  parseSort<E extends Expression> (
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expClass: new (args: any) => E,
    token: TokenType,
  ): E | undefined {
    if (!this._match(token)) {
      return undefined;
    }
    return this.expression(expClass, { expressions: this.parseCsv(() => this.parseOrdered()) });
  }

  parseOrdered (parseMethod?: () => Expression | undefined): OrderedExpr | undefined {
    const thisExpr = parseMethod ? parseMethod() : this.parseDisjunction();
    if (!thisExpr) {
      return undefined;
    }

    let orderedThis = thisExpr;
    if (thisExpr.name?.toUpperCase() === 'ALL' && this._dialectConstructor.SUPPORTS_ORDER_BY_ALL) {
      orderedThis = var_('ALL');
    }

    const asc = this._match(TokenType.ASC);
    const desc = this._match(TokenType.DESC) || (asc && false);

    const isNullsFirst = this._matchTextSeq(['NULLS', 'FIRST']);
    const isNullsLast = this._matchTextSeq(['NULLS', 'LAST']);

    let nullsFirst = isNullsFirst || false;
    const explicitlyNullOrdered = isNullsFirst || isNullsLast;

    if (
      !explicitlyNullOrdered
      && (
        (!desc && this._dialectConstructor.NULL_ORDERING === 'nulls_are_small')
        || (desc && this._dialectConstructor.NULL_ORDERING !== 'nulls_are_small')
      )
      && this._dialectConstructor.NULL_ORDERING !== 'nulls_are_last'
    ) {
      nullsFirst = true;
    }

    let withFill: WithFillExpr | undefined;
    if (this._matchTextSeq(['WITH', 'FILL'])) {
      withFill = this.expression(
        WithFillExpr,
        {
          from: this._match(TokenType.FROM) && this.parseBitwise(),
          to: this._matchTextSeq('TO') && this.parseBitwise(),
          step: this._matchTextSeq('STEP') && this.parseBitwise(),
          interpolate: this.parseInterpolate(),
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

  parseLimitOptions (): LimitOptionsExpr | undefined {
    const percent = this._matchSet(new Set([TokenType.PERCENT, TokenType.MOD]));
    const rows = this._matchSet(new Set([TokenType.ROW, TokenType.ROWS]));
    this._matchTextSeq('ONLY');
    const withTies = this._matchTextSeq(['WITH', 'TIES']);

    if (!(percent || rows || withTies)) {
      return undefined;
    }

    return this.expression(LimitOptionsExpr, {
      percent,
      rows,
      withTies,
    });
  }

  parseLimit (
    thisExpr?: Expression,
    options?: { top?: boolean;
      skipLimitToken?: boolean; },
  ): Expression | undefined {
    if (options?.skipLimitToken || this._match(options?.top ? TokenType.TOP : TokenType.LIMIT)) {
      const comments = this._prevComments;
      let expression: Expression | undefined;

      if (options?.top) {
        const limitParen = this._match(TokenType.L_PAREN);
        expression = limitParen ? this.parseTerm() : this.parseNumber();

        if (limitParen) {
          this._matchRParen();
        }
      } else {
        // Parsing LIMIT x% (i.e x PERCENT) as a term leads to an error, since
        // we try to build an exp.Mod expr. For that matter, we backtrack and instead
        // consume the factor plus parse the percentage separately
        const index = this._index;
        const results = this._parse({
          parseMethod: (self: Parser) => self.parseTerm(),
          rawTokens: this._tokens,
        });
        expression = results[0];
        if (expression instanceof ModExpr) {
          this._retreat(index);
          expression = this.parseFactor();
        } else if (!expression) {
          expression = this.parseFactor();
        }
      }

      const limitOptions = this.parseLimitOptions();

      let offset: Expression | undefined;
      if (this._match(TokenType.COMMA)) {
        offset = expression;
        expression = this.parseTerm();
      }

      const limitExp = this.expression(
        LimitExpr,
        {
          this: thisExpr,
          expression,
          offset,
          comments,
          limitOptions,
          expressions: this.parseLimitBy(),
        },
      );

      return limitExp;
    }

    if (this._match(TokenType.FETCH)) {
      const direction = this._matchSet(new Set([TokenType.FIRST, TokenType.NEXT]));
      const directionText = direction ? this._prev!.text.toUpperCase() : 'FIRST';

      const count = this.parseField({ tokens: this._constructor.FETCH_TOKENS });

      return this.expression(
        FetchExpr,
        {
          direction: directionText,
          count,
          limitOptions: this.parseLimitOptions(),
        },
      );
    }

    return thisExpr;
  }

  parseOffset (thisExpr?: Expression): Expression | undefined {
    if (!this._match(TokenType.OFFSET)) {
      return thisExpr;
    }

    const count = this.parseTerm();
    this._matchSet(new Set([TokenType.ROW, TokenType.ROWS]));

    return this.expression(
      OffsetExpr,
      {
        this: thisExpr,
        expression: count,
        expressions: this.parseLimitBy(),
      },
    );
  }

  canParseLimitOrOffset (): boolean {
    if (!this._matchSet(new Set(this._constructor.AMBIGUOUS_ALIAS_TOKENS), { advance: false })) {
      return false;
    }

    const index = this._index;
    const limitResults = this._parse({
      parseMethod: (self: Parser) => self.parseLimit(),
      rawTokens: this._tokens,
    });
    const offsetResults = this._parse({
      parseMethod: (self: Parser) => self.parseOffset(),
      rawTokens: this._tokens,
    });
    const result = !!(limitResults[0] || offsetResults[0]);
    this._retreat(index);

    // MATCH_CONDITION (...) is a special construct that should not be consumed by limit/offset
    if (this._next && this._next.tokenType === TokenType.MATCH_CONDITION) {
      return false;
    }

    return result;
  }

  parseLimitBy (): Expression[] | undefined {
    return this._matchTextSeq('BY') ? this.parseCsv(() => this.parseBitwise()) : undefined;
  }

  parseLocks (): LockExpr[] {
    const locks: LockExpr[] = [];

    while (true) {
      let update: boolean | undefined;
      let key: boolean | undefined;

      if (this._matchTextSeq(['FOR', 'UPDATE'])) {
        update = true;
      } else if (this._matchTextSeq(['FOR', 'SHARE']) || this._matchTextSeq([
        'LOCK',
        'IN',
        'SHARE',
        'MODE',
      ])) {
        update = false;
      } else if (this._matchTextSeq([
        'FOR',
        'KEY',
        'SHARE',
      ])) {
        update = false;
        key = true;
      } else if (this._matchTextSeq([
        'FOR',
        'NO',
        'KEY',
        'UPDATE',
      ])) {
        update = true;
        key = true;
      } else {
        break;
      }

      let expressions: Expression[] | undefined;
      if (this._matchTextSeq('OF')) {
        expressions = this.parseCsv(() => this.parseTable({ schema: true }));
      }

      let wait: boolean | Expression | undefined;
      if (this._matchTextSeq('NOWAIT')) {
        wait = true;
      } else if (this._matchTextSeq('WAIT')) {
        wait = this.parsePrimary();
      } else if (this._matchTextSeq(['SKIP', 'LOCKED'])) {
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
    ] = this.parseJoinParts();

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
      distinct = this._dialectConstructor.SET_OP_DISTINCT_BY_DEFAULT[operation];
      if (distinct === undefined) {
        this.raiseError(`Expected DISTINCT or ALL for ${operation.name}`, this._curr);
      }
    }

    let byName = this._matchTextSeq(['BY', 'NAME']) || this._matchTextSeq(['STRICT', 'CORRESPONDING']);
    if (this._matchTextSeq('CORRESPONDING')) {
      byName = true;
      if (!side && !kind) {
        // Set default kind
        // kind = 'INNER';  // Uncomment if needed
      }
    }

    let onColumnList: Expression[] | undefined;
    if (byName && this._matchTexts(['ON', 'BY'])) {
      onColumnList = this.parseWrappedCsv(() => this.parseColumn());
    }

    const expression = this.parseSelect({
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

  parseSetOperations (thisExpr?: Expression): Expression | undefined {
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
            current.setArgKey(arg, expr.pop());
          }
        }
      }
    }

    return current;
  }

  parseExpression (): Expression | undefined {
    return this.parseAlias(this.parseAssignment());
  }

  parseAssignment (): Expression | undefined {
    let thisExpr: Expression | undefined = this.parseDisjunction();

    if (!thisExpr && this._next && this._next.tokenType in this._constructor.ASSIGNMENT) {
      // This allows us to parse <non-identifier token> := <expr>
      this.advanceAny({ ignoreReserved: true });
      thisExpr = new ColumnExpr({ this: this._prev!.text });
    }

    while (this._matchSet(this._constructor.ASSIGNMENT)) {
      if (thisExpr instanceof ColumnExpr && thisExpr.parts.length === 1) {
        thisExpr = thisExpr.this;
      }

      const ExprClass = this._constructor.ASSIGNMENT[this._prev!.tokenType];
      if (ExprClass) {
        thisExpr = this.expression(
          ExprClass,
          {
            this: thisExpr,
            comments: this._prevComments,
            expression: this.parseAssignment(),
          },
        );
      }
    }

    return thisExpr;
  }

  parseDisjunction (): Expression | undefined {
    return this.parseTokens(() => this.parseConjunction(), this._constructor.DISJUNCTION);
  }

  parseConjunction (): Expression | undefined {
    return this.parseTokens(() => this.parseEquality(), this._constructor.CONJUNCTION);
  }

  parseEquality (): Expression | undefined {
    return this.parseTokens(() => this.parseComparison(), this._constructor.EQUALITY);
  }

  parseComparison (): Expression | undefined {
    return this.parseTokens(() => this.parseRange(), this._constructor.COMPARISON);
  }

  parseRange (thisExpr?: Expression): Expression | undefined {
    let current = thisExpr || this.parseBitwise();
    const negate = this._match(TokenType.NOT);

    if (this._matchSet(this._constructor.RANGE_PARSERS)) {
      const parser = this._constructor.RANGE_PARSERS[this._prev!.tokenType];
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
      current = this.parseIs(current);
    }

    return current;
  }

  protected _negateRange (thisExpr?: Expression): Expression | undefined {
    if (!thisExpr) {
      return thisExpr;
    }

    return this.expression(NotExpr, { this: thisExpr });
  }

  parseIs (thisExpr?: Expression): Expression | undefined {
    const index = this._index - 1;
    const negate = this._match(TokenType.NOT);

    if (this._matchTextSeq(['DISTINCT', 'FROM'])) {
      const klass = negate ? NullSafeEQExpr : NullSafeNEQExpr;
      return this.expression(klass, {
        this: thisExpr,
        expression: this.parseBitwise(),
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
      expression = this.parseNull() || this.parseBitwise();
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
    return this.parseColumnOps(result);
  }

  parseIn (thisExpr?: Expression, options?: { alias?: boolean }): InExpr {
    const unnest = this.parseUnnest({ withAlias: false });
    let result: InExpr;

    if (unnest) {
      result = this.expression(InExpr, {
        this: thisExpr,
        unnest,
      });
    } else if (this._matchSet(new Set([TokenType.L_PAREN, TokenType.L_BRACKET]))) {
      const matchedLParen = this._prev!.tokenType === TokenType.L_PAREN;
      const expressions = this.parseCsv(() => this.parseSelectOrExpression({ alias: options?.alias }));

      if (expressions.length === 1 && expressions[0] instanceof QueryExpr) {
        const query = expressions[0] as QueryExpr;
        result = this.expression(
          InExpr,
          {
            this: thisExpr,
            query: this.parseQueryModifiers(query).subquery({ copy: false }) as SubqueryExpr,
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
        field: this.parseColumn(),
      });
    }

    return result;
  }

  parseBetween (thisExpr?: Expression): BetweenExpr {
    let symmetric: boolean | undefined;
    if (this._matchTextSeq('SYMMETRIC')) {
      symmetric = true;
    } else if (this._matchTextSeq('ASYMMETRIC')) {
      symmetric = false;
    }

    const low = this.parseBitwise();
    this._match(TokenType.AND);
    const high = this.parseBitwise();

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

  parseEscape (thisExpr?: Expression): Expression | undefined {
    if (!this._match(TokenType.ESCAPE)) {
      return thisExpr;
    }
    return this.expression(
      EscapeExpr,
      {
        this: thisExpr,
        expression: this.parseString() || this.parseNull(),
      },
    );
  }

  parseIntervalSpan (thisExpr: Expression): IntervalExpr {
    // handle day-time format interval span with omitted units:
    //   INTERVAL '<number days> hh[:][mm[:ss[.ff]]]' <maybe `unit TO unit`>
    let intervalSpanUnitsOmitted: boolean | undefined;

    if (
      thisExpr
      && thisExpr.isString
      && this._constructor.SUPPORTS_OMITTED_INTERVAL_SPAN_UNIT
      && thisExpr.name?.match?.(INTERVAL_DAY_TIME_RE)
    ) {
      const index = this._index;

      // Var "TO" Var
      const firstUnit = this.parseVar({
        anyToken: true,
        upper: true,
      });
      let secondUnit: VarExpr | undefined;
      if (firstUnit && this._matchTextSeq('TO')) {
        secondUnit = this.parseVar({
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
        this.parseFunction()
        || (
          !this._match(TokenType.ALIAS, { advance: false })
          && this.parseVar({
            anyToken: true,
            upper: true,
          })
        )
      );

    // Most dialects support, e.g., the form INTERVAL '5' day, thus we try to parse
    // each INTERVAL expression into this canonical form so it's easy to transpile
    let finalThis = thisExpr;
    if (thisExpr && thisExpr.isNumber) {
      finalThis = LiteralExpr.string(thisExpr.toPy?.() || thisExpr.sql());
    } else if (thisExpr && thisExpr.isString) {
      const parts = thisExpr.name?.match?.(INTERVAL_STRING_RE);
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
          expression: this.parseFunction() || this.parseVar({
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

  parseInterval (options?: { matchInterval?: boolean }): AddExpr | IntervalExpr | undefined {
    const index = this._index;

    if (!this._match(TokenType.INTERVAL) && (options?.matchInterval ?? true)) {
      return undefined;
    }

    let thisExpr: Expression | undefined;
    if (this._match(TokenType.STRING, { advance: false })) {
      thisExpr = this.parsePrimary();
    } else {
      thisExpr = this.parseTerm();
    }

    if (!thisExpr || (
      thisExpr instanceof ColumnExpr
      && !thisExpr.args.table
      && !(thisExpr.this as IdentifierExpr)?.quoted
      && this._curr
      && !this._dialectConstructor.VALID_INTERVAL_UNITS.has(this._curr.text.toUpperCase())
    )) {
      this._retreat(index);
      return undefined;
    }

    const interval = this.parseIntervalSpan(thisExpr);

    const index2 = this._index;
    this._match(TokenType.PLUS);

    // Convert INTERVAL 'val_1' unit_1 [+] ... [+] 'val_n' unit_n into a sum of intervals
    if (this._matchSet(new Set([TokenType.STRING, TokenType.NUMBER]), { advance: false })) {
      return this.expression(
        AddExpr,
        {
          this: interval,
          expression: this.parseInterval({ matchInterval: false }),
        },
      );
    }

    this._retreat(index2);
    return interval;
  }

  parseBitwise (): Expression | undefined {
    let thisExpr = this.parseTerm();

    while (true) {
      if (this._matchSet(this._constructor.BITWISE)) {
        const ExprClass = this._constructor.BITWISE[this._prev!.tokenType];
        if (ExprClass) {
          thisExpr = this.expression(
            ExprClass,
            {
              this: thisExpr,
              expression: this.parseTerm(),
            },
          );
        }
      } else if (this._dialectConstructor.DPIPE_IS_STRING_CONCAT && this._match(TokenType.DPIPE)) {
        thisExpr = this.expression(
          DPipeExpr,
          {
            this: thisExpr,
            expression: this.parseTerm(),
            safe: !this._dialectConstructor.STRICT_STRING_CONCAT,
          },
        );
      } else if (this._match(TokenType.DQMARK)) {
        thisExpr = this.expression(
          CoalesceExpr,
          {
            this: thisExpr,
            expressions: ensureList(this.parseTerm()),
          },
        );
      } else if (this._matchPair(TokenType.LT, TokenType.LT)) {
        thisExpr = this.expression(
          BitwiseLeftShiftExpr,
          {
            this: thisExpr,
            expression: this.parseTerm(),
          },
        );
      } else if (this._matchPair(TokenType.GT, TokenType.GT)) {
        thisExpr = this.expression(
          BitwiseRightShiftExpr,
          {
            this: thisExpr,
            expression: this.parseTerm(),
          },
        );
      } else {
        break;
      }
    }

    return thisExpr;
  }

  parseTerm (): Expression | undefined {
    let thisExpr = this.parseFactor();

    while (this._matchSet(this._constructor.TERM)) {
      const klass = this._constructor.TERM[this._prev!.tokenType];
      const comments = this._prevComments;
      const expression = this.parseFactor();

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
              thisExpr.setArgKey('expression', ident.$quoted ? ident : var_(ident.name));
            }
          }
        }
      }
    }

    return thisExpr;
  }

  parseFactor (): Expression | undefined {
    const parseMethod = this._constructor.EXPONENT ? () => this.parseExponent() : () => this.parseUnary();
    let thisExpr = this.parseAtTimeZone(parseMethod());

    while (this._matchSet(this._constructor.FACTOR)) {
      const klass = this._constructor.FACTOR[this._prev!.tokenType];
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
          thisExpr.setArgKey('typed', this._dialectConstructor.TYPED_DIVISION);
          thisExpr.setArgKey('safe', this._dialectConstructor.SAFE_DIVISION);
        }
      }
    }

    return thisExpr;
  }

  parseExponent (): Expression | undefined {
    return this.parseTokens(() => this.parseUnary(), this._constructor.EXPONENT);
  }

  parseUnary (): Expression | undefined {
    if (this._matchSet(this._constructor.UNARY_PARSERS)) {
      const parser = this._constructor.UNARY_PARSERS[this._prev!.tokenType];
      return parser ? parser(this) : undefined;
    }
    return this.parseType();
  }

  parseType (options?: {
    parseInterval?: boolean;
    fallbackToIdentifier?: boolean;
  }): Expression | undefined {
    const parseInterval = options?.parseInterval ?? true;
    const fallbackToIdentifier = options?.fallbackToIdentifier ?? false;

    const interval = parseInterval && this.parseInterval();
    if (interval) {
      return this.parseColumnOps(interval);
    }

    const index = this._index;
    const dataType = this.parseTypes({
      checkFunc: true,
      allowIdentifiers: false,
    });

    // parse_types() returns a Cast if we parsed BQ's inline constructor <type>(<values>) e.g.
    // STRUCT<a INT, b STRING>(1, 'foo'), which is canonicalized to CAST(<values> AS <type>)
    if (dataType instanceof CastExpr) {
      // This constructor can contain ops directly after it, for instance struct unnesting:
      // STRUCT<a INT, b STRING>(1, 'foo').* --> CAST(STRUCT(1, 'foo') AS STRUCT<a iNT, b STRING).*
      return this.parseColumnOps(dataType);
    }

    if (dataType) {
      const index2 = this._index;
      const thisExpr = this.parsePrimary();

      if (thisExpr instanceof LiteralExpr) {
        const literal = thisExpr.name;
        const thisWithOps = this.parseColumnOps(thisExpr);

        const parser = this._constructor.TYPE_LITERAL_PARSERS[(dataType as DataTypeExpr).this];
        if (parser) {
          return parser(this, thisWithOps, dataType as DataTypeExpr);
        }

        if (
          this._constructor.ZONE_AWARE_TIMESTAMP_CONSTRUCTOR
          && (dataType as DataTypeExpr).isType?.(DataTypeExprKind.TIMESTAMP)
          && this._constructor.TIME_ZONE_RE?.test(literal)
        ) {
          (dataType as DataTypeExpr).setArgKey('this', DataTypeExprKind.TIMESTAMPTZ);
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
        return this.parseColumnOps(dataType);
      }

      this._retreat(index);
    }

    if (fallbackToIdentifier) {
      return this.parseIdVar();
    }

    const thisExpr = this.parseColumn();
    return thisExpr && this.parseColumnOps(thisExpr);
  }

  parseTypeSize (): DataTypeParamExpr | undefined {
    let thisExpr: Expression | undefined = this.parseType();
    if (!thisExpr) {
      return undefined;
    }

    if (thisExpr instanceof ColumnExpr && !thisExpr.args.table) {
      thisExpr = var_(thisExpr.name.toUpperCase());
    }

    return this.expression(
      DataTypeParamExpr,
      {
        this: thisExpr,
        expression: this.parseVar({ anyToken: true }),
      },
    );
  }

  parseUserDefinedType (identifier: IdentifierExpr): Expression | undefined {
    let typeName = identifier.name;

    while (this._match(TokenType.DOT)) {
      this.advanceAny();
      typeName = `${typeName}.${this._prev!.text}`;
    }

    return DataTypeExpr.build(typeName, {
      dialect: this.dialect,
      udt: true,
    });
  }

  parseTypes (options?: {
    checkFunc?: boolean;
    schema?: boolean;
    allowIdentifiers?: boolean;
  }): Expression | undefined {
    const index = this._index;

    let thisExpr: Expression | undefined;
    const prefix = this._matchTextSeq(['SYSUDTLIB', '.']);

    let typeToken: TokenType | undefined;
    if (this._matchSet(this._constructor.TYPE_TOKENS)) {
      typeToken = this._prev!.tokenType;
    } else {
      const identifier = (options?.allowIdentifiers ?? true) && this.parseIdVar({
        anyToken: false,
        tokens: new Set([TokenType.VAR]),
      });

      if (identifier instanceof IdentifierExpr) {
        let tokens: unknown[] | undefined;
        try {
          tokens = this.dialect.tokenize?.(identifier.name);
        } catch {
          tokens = undefined;
        }

        if (tokens && tokens.length === 1 && this._constructor.TYPE_TOKENS.has(tokens[0].tokenType)) {
          typeToken = tokens[0].tokenType;
        } else if (this._dialectConstructor.SUPPORTS_USER_DEFINED_TYPES) {
          thisExpr = this.parseUserDefinedType(identifier);
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
      const keyType = this.parseTypes(options);
      if (!this._match(TokenType.FARROW)) {
        this._retreat(index);
        return undefined;
      }

      const valueType = this.parseTypes(options);
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
        expressions = this.parseCsv(() => this.parseStructTypes({ typeRequired: true }));
      } else if (nested) {
        expressions = this.parseCsv(() => this.parseTypes(options));

        if (typeToken === TokenType.NULLABLE && expressions.length === 1) {
          thisExpr = expressions[0];
          thisExpr.setArgKey('nullable', true);
          this._matchRParen();
          return thisExpr;
        }
      } else if (typeToken && this._constructor.ENUM_TYPE_TOKENS.has(typeToken)) {
        expressions = this.parseCsv(() => this.parseEquality());
      } else if (isAggregate) {
        const funcOrIdent = this.parseFunction({ anonymous: true }) || this.parseIdVar({
          anyToken: false,
          tokens: new Set([TokenType.VAR, TokenType.ANY]),
        });
        if (!funcOrIdent) {
          return undefined;
        }
        expressions = [funcOrIdent];
        if (this._match(TokenType.COMMA)) {
          expressions.push(...this.parseCsv(() => this.parseTypes(options)));
        }
      } else {
        expressions = this.parseCsv(() => this.parseTypeSize());

        // https://docs.snowflake.com/en/sql-reference/data-types-vector
        if (typeToken === TokenType.VECTOR && expressions.length === 2) {
          expressions = this.parseVectorExpressions(expressions);
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
        expressions = this.parseCsv(() => this.parseStructTypes({ typeRequired: true }));
      } else {
        expressions = this.parseCsv(() => this.parseTypes(options));
      }

      if (!this._match(TokenType.GT)) {
        this.raiseError('Expecting >', this._curr);
      }

      if (this._matchSet(new Set([TokenType.L_BRACKET, TokenType.L_PAREN]))) {
        values = this.parseCsv(() => this.parseDisjunction());
        if (!values && isStruct) {
          values = undefined;
          this._retreat(this._index - 1);
        } else {
          this._matchSet(new Set([TokenType.R_BRACKET, TokenType.R_PAREN]));
        }
      }
    }

    if (typeToken && this._constructor.TIMESTAMPS.has(typeToken)) {
      if (this._matchTextSeq([
        'WITH',
        'TIME',
        'ZONE',
      ])) {
        maybeFunc = false;
        const tzType = this._constructor.TIMES.has(typeToken)
          ? DataTypeExprKind.TIMETZ
          : DataTypeExprKind.TIMESTAMPTZ;
        thisExpr = new DataTypeExpr({
          this: tzType,
          expressions,
        });
      } else if (this._matchTextSeq([
        'WITH',
        'LOCAL',
        'TIME',
        'ZONE',
      ])) {
        maybeFunc = false;
        thisExpr = new DataTypeExpr({
          this: DataTypeExprKind.TIMESTAMPLTZ,
          expressions,
        });
      } else if (this._matchTextSeq([
        'WITHOUT',
        'TIME',
        'ZONE',
      ])) {
        maybeFunc = false;
      }
    } else if (typeToken === TokenType.INTERVAL) {
      if (this._curr && this._dialectConstructor.VALID_INTERVAL_UNITS.has(this._curr.text.toUpperCase())) {
        let unit: Expression | undefined = this.parseVar({ upper: true });
        if (this._matchTextSeq('TO')) {
          unit = new IntervalSpanExpr({
            this: unit,
            expression: this.parseVar({ upper: true }),
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
      const peek = this.parseString();

      if (!peek) {
        this._retreat(index);
        return undefined;
      }

      this._retreat(index2);
    }

    if (!thisExpr) {
      if (this._matchTextSeq('UNSIGNED')) {
        const unsignedTypeToken = typeToken && this._constructor.SIGNED_TO_UNSIGNED_TYPE_TOKEN[typeToken];
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
      thisExpr.setArgKey('expressions', expressions);
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
      const valuesInBracket = this.parseCsv(() => this.parseDisjunction());

      if (
        valuesInBracket
        && !options?.schema
        && (
          !this._dialectConstructor.SUPPORTS_FIXED_SIZE_ARRAYS
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
      const converter = this._constructor.TYPE_CONVERTERS[(thisExpr as DataTypeExpr).this as DataTypeExprKind];
      if (converter) {
        thisExpr = converter(thisExpr as DataTypeExpr);
      }
    }

    return thisExpr;
  }

  parseVectorExpressions (expressions: Expression[]): Expression[] {
    return [DataTypeExpr.build(expressions[0].name, { dialect: this.dialect }), ...expressions.slice(1)];
  }

  parseStructTypes (options?: { typeRequired?: boolean }): Expression | undefined {
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
      thisExpr = this.parseIdVar();
    } else {
      thisExpr = (
        this.parseType({
          parseInterval: false,
          fallbackToIdentifier: true,
        })
        || this.parseIdVar()
      );
    }

    this._match(TokenType.COLON);

    if (
      options?.typeRequired
      && !(thisExpr instanceof DataTypeExpr)
      && !this._matchSet(this._constructor.TYPE_TOKENS, { advance: false })
    ) {
      this._retreat(index);
      return this.parseTypes();
    }

    return this.parseColumnDef(thisExpr);
  }

  parseAtTimeZone (thisExpr?: Expression): Expression | undefined {
    if (!this._matchTextSeq([
      'AT',
      'TIME',
      'ZONE',
    ])) {
      return thisExpr;
    }
    return this.parseAtTimeZone(
      this.expression(AtTimeZoneExpr, {
        this: thisExpr,
        zone: this.parseUnary(),
      }),
    );
  }

  parseColumn (): Expression | undefined {
    const thisExpr = this.parseColumnReference();
    const column = thisExpr ? this.parseColumnOps(thisExpr) : this.parseBracket(thisExpr);

    if (this._dialectConstructor.SUPPORTS_COLUMN_JOIN_MARKS && column) {
      column.setArgKey('joinMark', this._match(TokenType.JOIN_MARKER));
    }

    return column;
  }

  parseColumnReference (): Expression | undefined {
    let thisExpr = this.parseField();

    if (
      !thisExpr
      && this._match(TokenType.VALUES, { advance: false })
      && this._constructor.VALUES_FOLLOWED_BY_PAREN
      && (!this._next || this._next.tokenType !== TokenType.L_PAREN)
    ) {
      thisExpr = this.parseIdVar();
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

  parseColonAsVariantExtract (thisExpr?: Expression): Expression | undefined {
    const casts: DataTypeExpr[] = [];
    const jsonPath: string[] = [];
    let escape: boolean | undefined;

    while (this._match(TokenType.COLON)) {
      const startIndex = this._index;

      // Snowflake allows reserved keywords as json keys but advance_any() excludes TokenType.SELECT from any_tokens=True
      let path = this.parseColumnOps(
        this.parseField({
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
        if (path instanceof IdentifierExpr && path.$quoted) {
          escape = true;
        }

        jsonPath.push(this.findSql(this._tokens[startIndex], endToken));
      }
    }

    // The VARIANT extract in Snowflake/Databricks is parsed as a JSONExtract; Snowflake uses the json_path in GET_PATH() while
    // Databricks transforms it back to the colon/dot notation
    if (0 < jsonPath.length) {
      const jsonPathExpr = this.dialect.toJsonPath?.(LiteralExpr.string('.' + jsonPath.join('.')));

      if (jsonPathExpr) {
        jsonPathExpr.setArgKey('escape', escape);
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

  parseDcolon (): Expression | undefined {
    return this.parseTypes();
  }

  parseColumnOps (thisExpr?: Expression): Expression | undefined {
    let current = this.parseBracket(thisExpr);

    while (this._matchSet(this._constructor.COLUMN_OPERATORS)) {
      const opToken = this._prev!.tokenType;
      const op = this._constructor.COLUMN_OPERATORS[opToken];

      let field: Expression | undefined;
      if (this._constructor.CAST_COLUMN_OPERATORS.has(opToken)) {
        field = this.parseDcolon();
        if (!field) {
          this.raiseError('Expected type', this._curr);
        }
      } else if (op && this._curr) {
        field = this.parseColumnReference() || this.parseBitwise();
        if (field instanceof ColumnExpr && this._match(TokenType.DOT, { advance: false })) {
          field = this.parseColumnOps(field);
        }
      } else {
        field = this.parseField({
          anyToken: true,
          anonymousFunc: true,
        });
      }

      // Function calls can be qualified, e.g., x.y.FOO()
      // This converts the final AST to a series of Dots leading to the function call
      // https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-reference#function_call_rules
      if ((field instanceof FuncExpr || field instanceof WindowExpr) && current) {
        current = current.transform(
          (n: Expression) => n instanceof ColumnExpr ? n.toDot?.({ includeDots: false }) || n : n,
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
        field.setArgKey('this', windowFunc);
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

      current = this.parseBracket(current);
    }

    return this._constructor.COLON_IS_VARIANT_EXTRACT
      ? this.parseColonAsVariantExtract(current)
      : current;
  }

  parseComment (options: { allowExists?: boolean } = {}): Expression {
    const { allowExists = true } = options;
    const start = this._prev;
    const exists = allowExists ? this.parseExists() : undefined;

    this._match(TokenType.ON);

    const materialized = this._matchTextSeq('MATERIALIZED');
    const kind = this._matchSet(this._constructor.CREATABLES) && this._prev;
    if (!kind) {
      return this.parseAsCommand(start);
    }

    let thisExpr: Expression;
    if (kind.tokenType === TokenType.FUNCTION || kind.tokenType === TokenType.PROCEDURE) {
      thisExpr = this.parseUserDefinedFunction({ kind: kind.tokenType });
    } else if (kind.tokenType === TokenType.TABLE) {
      thisExpr = this.parseTable({ aliasTokens: this._constructor.COMMENT_TABLE_ALIAS_TOKENS });
    } else if (kind.tokenType === TokenType.COLUMN) {
      thisExpr = this.parseColumn();
    } else {
      thisExpr = this.parseIdVar();
    }

    this._match(TokenType.IS);

    return this.expression(CommentExpr, {
      this: thisExpr,
      kind: kind.text,
      expression: this.parseString(),
      exists,
      materialized,
    });
  }

  parseToTable (): ToTablePropertyExpr {
    const table = this.parseTableParts({ schema: true });
    return this.expression(ToTablePropertyExpr, { this: table });
  }

  parseTtl (): Expression {
    // https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#mergetree-table-ttl
    const parseTtlAction = (): Expression | undefined => {
      const thisExpr = this.parseBitwise();

      if (this._matchTextSeq('DELETE')) {
        return this.expression(MergeTreeTTLActionExpr, {
          this: thisExpr,
          delete: true,
        });
      }
      if (this._matchTextSeq('RECOMPRESS')) {
        return this.expression(MergeTreeTTLActionExpr, {
          this: thisExpr,
          recompress: this.parseBitwise(),
        });
      }
      if (this._matchTextSeq(['TO', 'DISK'])) {
        return this.expression(MergeTreeTTLActionExpr, {
          this: thisExpr,
          toDisk: this.parseString(),
        });
      }
      if (this._matchTextSeq(['TO', 'VOLUME'])) {
        return this.expression(MergeTreeTTLActionExpr, {
          this: thisExpr,
          toVolume: this.parseString(),
        });
      }

      return thisExpr;
    };

    const expressions = this.parseCsv(parseTtlAction);
    const where = this.parseWhere();
    const group = this.parseGroup();

    let aggregates: Expression[] | undefined;
    if (group && this._match(TokenType.SET)) {
      aggregates = this.parseCsv(this.parseSetItem);
    }

    return this.expression(MergeTreeTTLExpr, {
      expressions,
      where,
      group,
      aggregates,
    });
  }

  parseStatement (): Expression | undefined {
    if (this._curr === undefined) {
      return undefined;
    }

    if (this._matchSet(this._constructor.STATEMENT_PARSERS)) {
      const comments = this._prevComments;
      const stmt = this._constructor.STATEMENT_PARSERS[this._prev!.tokenType](this);
      stmt.addComments(comments, { prepend: true });
      return stmt;
    }

    if (this._matchSet(this._dialectConstructor.tokenizerClass().COMMANDS)) {
      return this.parseCommand();
    }

    let expression = this.parseExpression();
    expression = expression ? this.parseSetOperations(expression) : this.parseSelect();
    return this.parseQueryModifiers(expression);
  }

  parsePartitionedByBucketOrTruncate (): Expression | undefined {
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

    const args = this.parseWrappedCsv(() => this.parsePrimary() || this.parseColumn());
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
        mergeErrors(this.errors),
      );
    }
  }

  expression<E extends Expression> (
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expClass: new (args: any) => E,
    options?: unknown,
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

  parseIdVar (options?: { anyToken?: boolean;
    tokens?: Set<TokenType>; }): Expression | undefined {
    const anyToken = options?.anyToken !== undefined ? options.anyToken : true;
    const tokens = options?.tokens;

    let expression = this.parseIdentifier();
    if (!expression && (
      (anyToken && this.advanceAny()) || this._matchSet(tokens || this._constructor.ID_VAR_TOKENS)
    )) {
      const quoted = this._prev?.tokenType === TokenType.STRING;
      expression = this.identifierExpression(undefined, { quoted });
    }

    return expression;
  }

  parseGrantPrincipal (): GrantPrincipalExpr | undefined {
    const kind = this._matchTexts(['ROLE', 'GROUP']) && this._prev?.text.toUpperCase();
    const principal = this.parseIdVar();

    if (!principal) {
      return undefined;
    }

    return this.expression(GrantPrincipalExpr, {
      this: principal,
      kind,
    });
  }

  parseGrantPrivilege (): GrantPrivilegeExpr | undefined {
    const privilegeParts: string[] = [];

    while (this._curr && !this._matchSet(this._constructor.PRIVILEGE_FOLLOW_TOKENS, { advance: false })) {
      privilegeParts.push(this._curr.text.toUpperCase());
      this._advance();
    }

    const thisVar = this.expression(VarExpr, { this: privilegeParts.join(' ') });
    const expressions = this._match(TokenType.L_PAREN, { advance: false })
      ? this.parseWrappedCsv(() => this.parseColumn())
      : undefined;

    return this.expression(GrantPrivilegeExpr, {
      this: thisVar,
      expressions,
    });
  }

  _matchTexts (texts: string[] | Set<string>, options: { advance?: boolean } = {}): boolean | undefined {
    const { advance = true } = options;
    const textsArray = texts instanceof Set ? Array.from(texts) : texts;
    if (
      this._curr
      && this._curr.tokenType !== TokenType.STRING
      && textsArray.includes(this._curr.text.toUpperCase())
    ) {
      if (advance) {
        this._advance();
      }
      return true;
    }
    return undefined;
  }

  _matchSet (types: Set<TokenType>, options: { advance?: boolean } = {}): boolean | undefined {
    const { advance = true } = options;
    if (!this._curr) {
      return undefined;
    }

    if (types.has(this._curr.tokenType)) {
      if (advance) {
        this._advance();
      }
      return true;
    }

    return undefined;
  }

  _matchPair (tokenTypeA: TokenType, tokenTypeB: TokenType, options: { advance?: boolean } = {}): boolean | undefined {
    const { advance = true } = options;
    if (!this._curr || !this._next) {
      return undefined;
    }

    if (this._curr.tokenType === tokenTypeA && this._next.tokenType === tokenTypeB) {
      if (advance) {
        this._advance(2);
      }
      return true;
    }

    return undefined;
  }

  _matchLParen (expression?: Expression): void {
    if (!this._match(TokenType.L_PAREN, true, expression)) {
      this.raiseError('Expecting (');
    }
  }

  _matchRParen (expression?: Expression): void {
    if (!this._match(TokenType.R_PAREN, true, expression)) {
      this.raiseError('Expecting )');
    }
  }

  _matchTextSeq (texts: string | string[], options: { advance?: boolean } = {}): boolean | undefined {
    const { advance = true } = options;
    const textArray = ensureList(texts);

    const index = this._index;
    for (const text of textArray) {
      if (
        this._curr
        && this._curr.tokenType !== TokenType.STRING
        && this._curr.text.toUpperCase() === text
      ) {
        this._advance();
      } else {
        this._retreat(index);
        return undefined;
      }
    }

    if (!advance) {
      this._retreat(index);
    }

    return true;
  }

  _replaceLambda (node: Expression | undefined, expressions: Expression[]): Expression | undefined {
    if (!node) {
      return node;
    }

    const lambdaTypes: Record<string, Expression | false> = {};
    for (const e of expressions) {
      lambdaTypes[e.name] = e.$to || false;
    }

    for (const column of node.findAll(ColumnExpr)) {
      const typ = lambdaTypes[column.parts[0].name];
      if (typ !== undefined) {
        let dotOrId = column.table ? column.toDot() : column.this;

        if (typ) {
          dotOrId = this.expression(
            CastExpr,
            {
              this: dotOrId,
              to: typ,
            },
          );
        }

        let parent = column.parent;

        while (parent instanceof DotExpr) {
          if (!(parent.parent instanceof DotExpr)) {
            parent.replace(dotOrId);
            break;
          }
          parent = parent.parent;
        }

        if (!parent || !(parent instanceof DotExpr)) {
          if (column === node) {
            node = dotOrId;
          } else {
            column.replace(dotOrId);
          }
        }
      }
    }

    return node;
  }

  parseTruncateTable (): TruncateTableExpr | Expression | undefined {
    const start = this._prev;

    if (this._match(TokenType.L_PAREN)) {
      this._retreat(this._index - 2);
      return this.parseFunction();
    }

    const isDatabase = this._match(TokenType.DATABASE);

    this._match(TokenType.TABLE);

    const exists = this.parseExists({ not: false });

    const expressions = this.parseCsv(
      () => this.parseTable({
        schema: true,
        isDbReference: isDatabase,
      }),
    );

    const cluster = this._match(TokenType.ON) ? this.parseOnProperty() : undefined;

    let identity: string | undefined;
    if (this._matchTextSeq(['RESTART', 'IDENTITY'])) {
      identity = 'RESTART';
    } else if (this._matchTextSeq(['CONTINUE', 'IDENTITY'])) {
      identity = 'CONTINUE';
    } else {
      identity = undefined;
    }

    let option: string | undefined;
    if (this._matchTextSeq('CASCADE') || this._matchTextSeq('RESTRICT')) {
      option = this._prev?.text;
    } else {
      option = undefined;
    }

    const partition = this.parsePartition();

    if (this._curr) {
      return this.parseAsCommand(start);
    }

    return this.expression(
      TruncateTableExpr,
      {
        expressions,
        isDatabase,
        exists,
        cluster,
        identity,
        option,
        partition,
      },
    );
  }

  parseWithOperator (): Expression | undefined {
    const thisExpr = this.parseOrdered(() => this.parseOpclass());

    if (!this._match(TokenType.WITH)) {
      return thisExpr;
    }

    const op = this.parseVar({
      anyToken: true,
      tokens: this._constructor.RESERVED_TOKENS,
    });

    return this.expression(WithOperatorExpr, {
      this: thisExpr,
      op,
    });
  }

  parseWrappedOptions (): Expression[] {
    this._match(TokenType.EQ);
    this._match(TokenType.L_PAREN);

    const opts: Expression[] = [];
    let option: Expression | undefined;
    while (this._curr && !this._match(TokenType.R_PAREN)) {
      if (this._matchTextSeq(['FORMAT_NAME', '='])) {
        option = this.parseFormatName();
      } else {
        option = this.parseProperty();
      }

      if (option === undefined) {
        this.raiseError('Unable to parse option');
        break;
      }

      opts.push(option);
    }

    return opts;
  }

  parseCopyParameters (): CopyParameterExpr[] {
    const sep = this._dialectConstructor.COPY_PARAMS_ARE_CSV ? TokenType.COMMA : undefined;

    const options: CopyParameterExpr[] = [];
    while (this._curr && !this._match(TokenType.R_PAREN, { advance: false })) {
      const option = this.parseVar({ anyToken: true });
      const prev = this._prev?.text.toUpperCase();

      this._match(TokenType.EQ);
      this._match(TokenType.ALIAS);

      const param = this.expression(CopyParameterExpr, { this: option });

      if (prev && this._constructor.COPY_INTO_VARLEN_OPTIONS.has(prev) && this._match(TokenType.L_PAREN, { advance: false })) {
        param.setArgKey('expressions', this.parseWrappedOptions());
      } else if (prev === 'FILE_FORMAT') {
        param.setArgKey('expression', this.parseField());
      } else if (
        prev === 'FORMAT'
        && this._prev?.tokenType === TokenType.ALIAS
        && this._matchTexts(['AVRO', 'JSON'])
      ) {
        param.setArgKey('this', this.expression(VarExpr, { this: `FORMAT AS ${this._prev?.text.toUpperCase()}` }));
        param.setArgKey('expression', this.parseField());
      } else {
        param.setArgKey('expression', this.parseUnquotedField() || this.parseBracket());
      }

      options.push(param);
      if (sep) {
        this._match(sep);
      }
    }

    return options;
  }

  parseCredentials (): CredentialsExpr | undefined {
    const expr = this.expression(CredentialsExpr, {});

    if (this._matchTextSeq(['STORAGE_INTEGRATION', '='])) {
      expr.setArgKey('storage', this.parseField());
    }
    if (this._matchTextSeq('CREDENTIALS')) {
      const creds = this._match(TokenType.EQ) ? this.parseWrappedOptions() : this.parseField();
      expr.setArgKey('credentials', creds);
    }
    if (this._matchTextSeq('ENCRYPTION')) {
      expr.setArgKey('encryption', this.parseWrappedOptions());
    }
    if (this._matchTextSeq('IAM_ROLE')) {
      expr.setArgKey(
        'iamRole',
        this._match(TokenType.DEFAULT)
          ? this.expression(VarExpr, { this: this._prev?.text })
          : this.parseField(),
      );
    }
    if (this._matchTextSeq('REGION')) {
      expr.setArgKey('region', this.parseField());
    }

    return expr;
  }

  parseFileLocation (): Expression | undefined {
    return this.parseField();
  }

  parseCopy (): CopyExpr | CommandExpr {
    const start = this._prev;

    this._match(TokenType.INTO);

    const thisExpr = this._match(TokenType.L_PAREN, { advance: false })
      ? this.parseSelect({
        nested: true,
        parseSubqueryAlias: false,
      })
      : this.parseTable({ schema: true });

    const kind = this._match(TokenType.FROM) || !this._matchTextSeq('TO');

    let files = this.parseCsv(() => this.parseFileLocation());
    if (this._match(TokenType.EQ, { advance: false })) {
      this._advance(-1);
      files = [];
    }

    const credentials = this.parseCredentials();

    this._matchTextSeq('WITH');

    const params = this.parseWrapped(() => this.parseCopyParameters(), { optional: true });

    if (this._curr) {
      return this.parseAsCommand(start);
    }

    return this.expression(
      CopyExpr,
      {
        this: thisExpr,
        kind,
        credentials,
        files,
        params,
      },
    );
  }

  parseNormalize (): NormalizeExpr {
    return this.expression(
      NormalizeExpr,
      {
        this: this.parseBitwise(),
        form: this._match(TokenType.COMMA) && this.parseVar(),
      },
    );
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  parseCeilFloor<T extends Expression> (exprType: new (args: any) => T): T {
    const args = this.parseCsv(() => this.parseLambda());

    const thisExpr = seqGet(args, 0);
    const decimals = seqGet(args, 1);

    return new exprType({
      this: thisExpr,
      decimals,
      to: this._matchTextSeq('TO') && this.parseVar(),
    });
  }

  parseStarOps (): Expression | undefined {
    const starToken = this._prev;

    if (this._matchTextSeq('COLUMNS', '(', { advance: false })) {
      const thisExpr = this.parseFunction();
      if (thisExpr instanceof ColumnsExpr) {
        thisExpr.setArgKey('unpack', true);
      }
      return thisExpr;
    }

    return this.expression(
      StarExpr,
      {
        except: this.parseStarOp('EXCEPT') || this.parseStarOp('EXCLUDE'),
        replace: this.parseStarOp('REPLACE'),
        rename: this.parseStarOp('RENAME'),
      },
    ).updatePositions(starToken);
  }

  parseGrantRevokeCommon (): [GrantPrivilegeExpr[] | undefined, string | undefined, Expression | undefined] {
    const privileges = this.parseCsv(() => this.parseGrantPrivilege());

    this._match(TokenType.ON);
    const kind = this._matchSet(this._constructor.CREATABLES) && this._prev?.text.toUpperCase();

    const securable = this.tryParse(() => this.parseTableParts());

    return [
      privileges,
      kind,
      securable,
    ];
  }

  parseGrant (): GrantExpr | CommandExpr {
    const start = this._prev;

    const [
      privileges,
      kind,
      securable,
    ] = this.parseGrantRevokeCommon();

    if (!securable || !this._matchTextSeq('TO')) {
      return this.parseAsCommand(start);
    }

    const principals = this.parseCsv(() => this.parseGrantPrincipal());

    const grantOption = this._matchTextSeq([
      'WITH',
      'GRANT',
      'OPTION',
    ]);

    if (this._curr) {
      return this.parseAsCommand(start);
    }

    return this.expression(
      GrantExpr,
      {
        privileges,
        kind,
        securable,
        principals,
        grantOption,
      },
    );
  }

  parseRevoke (): RevokeExpr | CommandExpr {
    const start = this._prev;

    const grantOption = this._matchTextSeq([
      'GRANT',
      'OPTION',
      'FOR',
    ]);

    const [
      privileges,
      kind,
      securable,
    ] = this.parseGrantRevokeCommon();

    if (!securable || !this._matchTextSeq('FROM')) {
      return this.parseAsCommand(start);
    }

    const principals = this.parseCsv(() => this.parseGrantPrincipal());

    let cascade: string | undefined;
    if (this._matchTexts(['CASCADE', 'RESTRICT'])) {
      cascade = this._prev?.text.toUpperCase();
    }

    if (this._curr) {
      return this.parseAsCommand(start);
    }

    return this.expression(
      RevokeExpr,
      {
        privileges,
        kind,
        securable,
        principals,
        grantOption,
        cascade,
      },
    );
  }

  parseOverlay (): OverlayExpr {
    const parseOverlayArg = (text: string): Expression | undefined => {
      return (this._match(TokenType.COMMA) || this._matchTextSeq(text)) && this.parseBitwise();
    };

    return this.expression(
      OverlayExpr,
      {
        this: this.parseBitwise(),
        expression: parseOverlayArg('PLACING'),
        from: parseOverlayArg('FROM'),
        for: parseOverlayArg('FOR'),
      },
    );
  }

  parseFormatName (): PropertyExpr {
    return this.expression(
      PropertyExpr,
      {
        this: this.expression(VarExpr, { this: 'FORMAT_NAME' }),
        value: this.parseString() || this.parseTableParts(),
      },
    );
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  parseMaxMinBy (exprType: new (args: any) => AggFuncExpr): AggFuncExpr {
    const args: Expression[] = [];

    if (this._match(TokenType.DISTINCT)) {
      args.push(this.expression(DistinctExpr, { expressions: [this.parseLambda()] }));
      this._match(TokenType.COMMA);
    }

    args.push(...this.parseFunctionArgs());

    return this.expression(
      exprType,
      {
        this: seqGet(args, 0),
        expression: seqGet(args, 1),
        count: seqGet(args, 2),
      },
    );
  }

  identifierExpression (token?: Token, kwargs?: unknown): IdentifierExpr {
    return this.expression(IdentifierExpr, {
      token: token || this._prev,
      ...kwargs,
    });
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  parseTokens (parseMethod: () => Expression | undefined, expressions: Record<TokenType, new (args: any) => Expression>): Expression | undefined {
    let thisExpr = parseMethod();

    while (this._matchSet(new Set(Object.keys(expressions).map((k) => parseInt(k) as TokenType)))) {
      const exprType = expressions[this._prev!.tokenType];
      thisExpr = this.expression(
        exprType,
        {
          this: thisExpr,
          comments: this._prevComments,
          expression: parseMethod(),
        },
      );
    }

    return thisExpr;
  }

  parseWrappedIdVars (options: { optional?: boolean } = {}): Expression[] {
    const { optional } = options;
    return this.parseWrappedCsv(() => this.parseIdVar(), { optional });
  }

  parseWrappedCsv (parseMethod: () => Expression | undefined, options?: { sep?: TokenType;
    optional?: boolean; }): Expression[] {
    const sep = options?.sep || TokenType.COMMA;
    const optional = options?.optional || false;
    return this.parseWrapped(
      () => this.parseCsv(parseMethod, sep),
      { optional },
    );
  }

  parseWrapped<T> (parseMethod: () => T, options?: { optional?: boolean }): T {
    const optional = options?.optional || false;
    const wrapped = this._match(TokenType.L_PAREN);
    if (!wrapped && !optional) {
      this.raiseError('Expecting (');
    }
    const parseResult = parseMethod();
    if (wrapped) {
      this._matchRParen();
    }
    return parseResult;
  }

  parseExpressions (): Expression[] {
    return this.parseCsv(() => this.parseExpression());
  }

  parseSelectOrExpression (options?: { alias?: boolean }): Expression | undefined {
    const alias = options?.alias || false;
    return (
      this.parseSetOperations(
        alias
          ? this.parseAlias(this.parseAssignment(), { explicit: true })
          : this.parseAssignment(),
      ) || this.parseSelect()
    );
  }

  parseDdlSelect (): Expression | undefined {
    return this.parseQueryModifiers(
      this.parseSetOperations(this.parseSelect({
        nested: true,
        parseSubqueryAlias: false,
      })),
    );
  }

  parseTransaction (): TransactionExpr | CommandExpr {
    let thisText: string | undefined;
    if (this._matchTexts(this._constructor.TRANSACTION_KIND)) {
      thisText = this._prev?.text;
    }

    this._matchTexts(['TRANSACTION', 'WORK']);

    const modes: string[] = [];
    while (true) {
      const mode: string[] = [];
      while (this._match(TokenType.VAR) || this._match(TokenType.NOT)) {
        mode.push(this._prev!.text);
      }

      if (0 < mode.length) {
        modes.push(mode.join(' '));
      }
      if (!this._match(TokenType.COMMA)) {
        break;
      }
    }

    return this.expression(TransactionExpr, {
      this: thisText,
      modes,
    });
  }

  parseStar (): Expression | undefined {
    if (this._match(TokenType.STAR)) {
      return this._constructor.PRIMARY_PARSERS[TokenType.STAR](this, this._prev!);
    }
    return this.parsePlaceholder();
  }

  parseParameter (): ParameterExpr {
    const thisExpr = this.parseIdentifier() || this.parsePrimaryOrVar();
    return this.expression(ParameterExpr, { this: thisExpr });
  }

  parsePlaceholder (): Expression | undefined {
    if (this._matchSet(new Set(Object.keys(this._constructor.PLACEHOLDER_PARSERS).map((k) => parseInt(k) as TokenType)))) {
      const placeholder = this._constructor.PLACEHOLDER_PARSERS[this._prev!.tokenType]?.(this);
      if (placeholder) {
        return placeholder;
      }
      this._advance(-1);
    }
    return undefined;
  }

  parseStarOp (...keywords: string[]): Expression[] | undefined {
    if (!this._matchTexts(keywords)) {
      return undefined;
    }
    if (this._match(TokenType.L_PAREN, { advance: false })) {
      return this.parseWrappedCsv(() => this.parseExpression());
    }

    const expression = this.parseAlias(this.parseDisjunction(), { explicit: true });
    return expression ? [expression] : undefined;
  }

  parseCsv (parseMethod: () => Expression | undefined, sep: TokenType = TokenType.COMMA): Expression[] {
    let parseResult = parseMethod();
    const items: Expression[] = parseResult !== undefined ? [parseResult] : [];

    while (this._match(sep)) {
      this._addComments(parseResult);
      parseResult = parseMethod();
      if (parseResult !== undefined) {
        items.push(parseResult);
      }
    }

    return items;
  }

  parseIdentifier (): Expression | undefined {
    if (this._match(TokenType.IDENTIFIER)) {
      return this.identifierExpression(undefined, { quoted: true });
    }
    return this.parsePlaceholder();
  }

  parseVar (options?: { anyToken?: boolean;
    tokens?: Set<TokenType>;
    upper?: boolean; }): Expression | undefined {
    const anyToken = options?.anyToken || false;
    const tokens = options?.tokens;
    const upper = options?.upper || false;

    if (
      (anyToken && this.advanceAny())
      || this._match(TokenType.VAR)
      || (tokens && this._matchSet(tokens))
    ) {
      const text = upper ? this._prev!.text.toUpperCase() : this._prev!.text;
      return this.expression(VarExpr, { this: text });
    }
    return this.parsePlaceholder();
  }

  advanceAny (options: { ignoreReserved?: boolean } = {}): Token | undefined {
    const { ignoreReserved = false } = options;
    if (this._curr && (ignoreReserved || !this._constructor.RESERVED_TOKENS.has(this._curr.tokenType))) {
      this._advance();
      return this._prev;
    }
    return undefined;
  }

  parseVarOrString (options: { upper?: boolean } = {}): Expression | undefined {
    const { upper } = options;
    return this.parseString() || this.parseVar({
      anyToken: true,
      upper,
    });
  }

  parsePrimaryOrVar (): Expression | undefined {
    return this.parsePrimary() || this.parseVar({ anyToken: true });
  }

  parseNull (): Expression | undefined {
    if (this._matchSet(new Set([TokenType.NULL, TokenType.UNKNOWN]))) {
      return this._constructor.PRIMARY_PARSERS[TokenType.NULL](this, this._prev!);
    }
    return this.parsePlaceholder();
  }

  parseBoolean (): Expression | undefined {
    if (this._match(TokenType.TRUE)) {
      return this._constructor.PRIMARY_PARSERS[TokenType.TRUE](this, this._prev!);
    }
    if (this._match(TokenType.FALSE)) {
      return this._constructor.PRIMARY_PARSERS[TokenType.FALSE](this, this._prev!);
    }
    return this.parsePlaceholder();
  }

  parseString (): Expression | undefined {
    if (this._matchSet(new Set(Object.keys(this._constructor.STRING_PARSERS).map((k) => parseInt(k) as TokenType)))) {
      return this._constructor.STRING_PARSERS[this._prev!.tokenType]?.(this, this._prev!);
    }
    return this.parsePlaceholder();
  }

  parseStringAsIdentifier (): IdentifierExpr | undefined {
    const output = toIdentifier(this._match(TokenType.STRING) && this._prev?.text, { quoted: true });
    if (output && this._prev) {
      output.updatePositions(this._prev);
    }
    return output;
  }

  parseNumber (): Expression | undefined {
    if (this._matchSet(new Set(Object.keys(this._constructor.NUMERIC_PARSERS).map((k) => parseInt(k) as TokenType)))) {
      return this._constructor.NUMERIC_PARSERS[this._prev!.tokenType]?.(this, this._prev!);
    }
    return this.parsePlaceholder();
  }

  parseHavingMax (thisExpr: Expression | undefined): Expression | undefined {
    if (this._match(TokenType.HAVING)) {
      this._matchTexts(['MAX', 'MIN']);
      const max = this._prev?.text.toUpperCase() !== 'MIN';
      return this.expression(
        HavingMaxExpr,
        {
          this: thisExpr,
          expression: this.parseColumn(),
          max,
        },
      );
    }

    return thisExpr;
  }

  parseWindow (thisExpr: Expression | undefined, options: { alias?: boolean } = {}): Expression | undefined {
    const { alias } = options;
    const func = thisExpr;
    const comments = func instanceof Expression ? func.comments : undefined;

    if (this._matchTextSeq(['WITHIN', 'GROUP'])) {
      const order = this.parseWrapped(() => this.parseOrder());
      thisExpr = this.expression(WithinGroupExpr, {
        this: thisExpr,
        expression: order,
      });
    }

    if (this._matchPair(TokenType.FILTER, TokenType.L_PAREN)) {
      this._match(TokenType.WHERE);
      thisExpr = this.expression(
        FilterExpr,
        {
          this: thisExpr,
          expression: this.parseWhere({ skipWhereToken: true }),
        },
      );
      this._matchRParen();
    }

    if (thisExpr instanceof AggFuncExpr) {
      const ignoreRespect = thisExpr.find(IgnoreNullsExpr, RespectNullsExpr);

      if (ignoreRespect && ignoreRespect !== thisExpr) {
        ignoreRespect.replace(ignoreRespect.this);
        thisExpr = this.expression(ignoreRespect.constructor, { this: thisExpr });
      }
    }

    thisExpr = this.parseRespectOrIgnoreNulls(thisExpr);

    let over: string | undefined;
    if (alias) {
      over = undefined;
      this._match(TokenType.ALIAS);
    } else if (!this._matchSet(this._constructor.WINDOW_BEFORE_PAREN_TOKENS)) {
      return thisExpr;
    } else {
      over = this._prev?.text.toUpperCase();
    }

    if (comments && func instanceof Expression) {
      func.popComments();
    }

    if (!this._match(TokenType.L_PAREN)) {
      return this.expression(
        WindowExpr,
        {
          comments,
          this: thisExpr,
          alias: this.parseIdVar({ anyToken: false }),
          over,
        },
      );
    }

    const windowAlias = this.parseIdVar({
      anyToken: false,
      tokens: this._constructor.WINDOW_ALIAS_TOKENS,
    });

    let first: boolean | undefined = this._match(TokenType.FIRST);
    if (this._matchTextSeq('LAST')) {
      first = false;
    }

    const [partition, order] = this.parsePartitionAndOrder();
    const kind = this._matchSet(new Set([TokenType.ROWS, TokenType.RANGE])) && this._prev?.text;

    let spec: WindowSpecExpr | undefined;
    if (kind) {
      this._match(TokenType.BETWEEN);
      const start = this.parseWindowSpec();

      const end = this._match(TokenType.AND) ? this.parseWindowSpec() : {};
      const exclude = this._matchTextSeq('EXCLUDE')
        ? this.parseVarFromOptions(this._constructor.WINDOW_EXCLUDE_OPTIONS)
        : undefined;

      spec = this.expression(
        WindowSpecExpr,
        {
          kind,
          start: start.value,
          startSide: start.side,
          end: end.value,
          endSide: end.side,
          exclude,
        },
      );
    } else {
      spec = undefined;
    }

    this._matchRParen();

    const window = this.expression(
      WindowExpr,
      {
        comments,
        this: thisExpr,
        partitionBy: partition,
        order,
        spec,
        alias: windowAlias,
        over,
        first,
      },
    );

    if (this._matchSet(this._constructor.WINDOW_BEFORE_PAREN_TOKENS, { advance: false })) {
      return this.parseWindow(window, { alias });
    }

    return window;
  }

  parsePartitionAndOrder (): [Expression[], Expression | undefined] {
    return [this.parsePartitionBy(), this.parseOrder()];
  }

  parseWindowSpec (): { value?: string | Expression;
    side?: string; } {
    this._match(TokenType.BETWEEN);

    return {
      value:
        (this._matchTextSeq('UNBOUNDED') && 'UNBOUNDED')
        || (this._matchTextSeq(['CURRENT', 'ROW']) && 'CURRENT ROW')
        || this.parseBitwise(),
      side: this._matchTexts(this._constructor.WINDOW_SIDES) && this._prev?.text,
    };
  }

  parseAlias (thisExpr: Expression | undefined, options?: { explicit?: boolean }): Expression | undefined {
    const explicit = options?.explicit || false;

    if (this.canParseLimitOrOffset()) {
      return thisExpr;
    }

    const anyToken = this._match(TokenType.ALIAS);
    const comments = this._prevComments || [];

    if (explicit && !anyToken) {
      return thisExpr;
    }

    if (this._match(TokenType.L_PAREN)) {
      const aliases = this.expression(
        AliasesExpr,
        {
          comments,
          this: thisExpr,
          expressions: this.parseCsv(() => this.parseIdVar({ anyToken })),
        },
      );
      this._matchRParen(aliases);
      return aliases;
    }

    const alias =
      this.parseIdVar({
        anyToken,
        tokens: this._constructor.ALIAS_TOKENS,
      })
      || (this._constructor.STRING_ALIASES && this.parseStringAsIdentifier());

    if (alias) {
      comments.push(...alias.popComments());
      thisExpr = this.expression(AliasExpr, {
        comments,
        this: thisExpr,
        alias,
      });
      const column = thisExpr.this;

      if (!thisExpr.comments && column && column.comments) {
        thisExpr.comments = column.popComments();
      }
    }

    return thisExpr;
  }

  parseOpenJson (): OpenJSONExpr {
    const thisExpr = this.parseBitwise();
    const path = this._match(TokenType.COMMA) && this.parseString();

    const parseOpenJsonColumnDef = (): OpenJSONColumnDefExpr => {
      const thisCol = this.parseField({ anyToken: true });
      const kind = this.parseTypes();
      const pathCol = this.parseString();
      const asJson = this._matchPair(TokenType.ALIAS, TokenType.JSON);

      return this.expression(
        OpenJSONColumnDefExpr,
        {
          this: thisCol,
          kind,
          path: pathCol,
          asJson,
        },
      );
    };

    let expressions: OpenJSONColumnDefExpr[] | undefined;
    if (this._matchPair(TokenType.R_PAREN, TokenType.WITH)) {
      this._matchLParen();
      expressions = this.parseCsv(parseOpenJsonColumnDef);
    }

    return this.expression(OpenJSONExpr, {
      this: thisExpr,
      path,
      expressions,
    });
  }

  parsePosition (options: { haystackFirst?: boolean } = {}): StrPositionExpr {
    const { haystackFirst } = options;
    const args = this.parseCsv(() => this.parseBitwise());

    if (this._match(TokenType.IN)) {
      return this.expression(
        StrPositionExpr,
        {
          this: this.parseBitwise(),
          substr: seqGet(args, 0),
        },
      );
    }

    const haystack = haystackFirst ? seqGet(args, 0) : seqGet(args, 1);
    const needle = haystackFirst ? seqGet(args, 1) : seqGet(args, 0);

    return this.expression(
      StrPositionExpr,
      {
        this: haystack,
        substr: needle,
        position: seqGet(args, 2),
      },
    );
  }

  parseJoinHint (funcName: string): JoinHintExpr {
    const args = this.parseCsv(() => this.parseTable());
    return new JoinHintExpr({
      this: funcName.toUpperCase(),
      expressions: args,
    });
  }

  parseSubstring (): SubstringExpr {
    const args = this.parseCsv(() => this.parseBitwise()) as (Expression | undefined)[];

    let start: Expression | undefined;
    let length: Expression | undefined;

    while (this._curr) {
      if (this._match(TokenType.FROM)) {
        start = this.parseBitwise();
      } else if (this._match(TokenType.FOR)) {
        if (!start) {
          start = LiteralExpr.number(1);
        }
        length = this.parseBitwise();
      } else {
        break;
      }
    }

    if (start) {
      args.push(start);
    }
    if (length) {
      args.push(length);
    }

    return this.validateExpression(SubstringExpr.fromArgList(args), args);
  }

  parseTrim (): TrimExpr {
    let position: string | undefined;
    let collation: Expression | undefined;
    let expression: Expression | undefined;

    if (this._matchTexts(this._constructor.TRIM_TYPES)) {
      position = this._prev?.text.toUpperCase();
    }

    let thisExpr = this.parseBitwise();
    if (this._matchSet(new Set([TokenType.FROM, TokenType.COMMA]))) {
      const invertOrder = this._prev?.tokenType === TokenType.FROM || this._constructor.TRIM_PATTERN_FIRST;
      expression = this.parseBitwise();

      if (invertOrder) {
        [thisExpr, expression] = [expression, thisExpr];
      }
    }

    if (this._match(TokenType.COLLATE)) {
      collation = this.parseBitwise();
    }

    return this.expression(
      TrimExpr,
      {
        this: thisExpr,
        position,
        expression,
        collation,
      },
    );
  }

  parseWindowClause (): Expression[] | undefined {
    return this._match(TokenType.WINDOW) && this.parseCsv(() => this.parseNamedWindow());
  }

  parseNamedWindow (): Expression | undefined {
    return this.parseWindow(this.parseIdVar(), { alias: true });
  }

  parseRespectOrIgnoreNulls (thisExpr: Expression | undefined): Expression | undefined {
    if (this._matchTextSeq(['IGNORE', 'NULLS'])) {
      return this.expression(IgnoreNullsExpr, { this: thisExpr });
    }
    if (this._matchTextSeq(['RESPECT', 'NULLS'])) {
      return this.expression(RespectNullsExpr, { this: thisExpr });
    }
    return thisExpr;
  }

  parseJsonObject (options: { agg?: boolean } = {}): JSONObjectExpr | JSONObjectAggExpr {
    const { agg = false } = options;
    const star = this.parseStar();
    const expressions = star
      ? [star]
      : this.parseCsv(() => this.parseFormatJson(this.parseJsonKeyValue()));

    const nullHandling = this.parseOnHandling('NULL', 'NULL', 'ABSENT');

    let uniqueKeys: boolean | undefined;
    if (this._matchTextSeq(['WITH', 'UNIQUE'])) {
      uniqueKeys = true;
    } else if (this._matchTextSeq(['WITHOUT', 'UNIQUE'])) {
      uniqueKeys = false;
    }

    this._matchTextSeq('KEYS');

    const returnType = this._matchTextSeq('RETURNING') && this.parseFormatJson(this.parseType());
    const encoding = this._matchTextSeq('ENCODING') && this.parseVar();

    return this.expression(
      agg ? JSONObjectAggExpr : JSONObjectExpr,
      {
        expressions,
        nullHandling,
        uniqueKeys,
        returnType,
        encoding,
      },
    );
  }

  parseJsonColumnDef (): JSONColumnDefExpr {
    let thisExpr: Expression | undefined;
    let ordinality: boolean | undefined;
    let kind: Expression | undefined;
    let nested: boolean | undefined;

    if (!this._matchTextSeq('NESTED')) {
      thisExpr = this.parseIdVar();
      ordinality = this._matchPair(TokenType.FOR, TokenType.ORDINALITY);
      kind = this.parseTypes({ allowIdentifiers: false });
      nested = undefined;
    } else {
      thisExpr = undefined;
      ordinality = undefined;
      kind = undefined;
      nested = true;
    }

    const path = this._matchTextSeq('PATH') && this.parseString();
    const nestedSchema = nested && this.parseJsonSchema();

    return this.expression(
      JSONColumnDefExpr,
      {
        this: thisExpr,
        kind,
        path,
        nestedSchema,
        ordinality,
      },
    );
  }

  parseJsonSchema (): JSONSchemaExpr {
    this._matchTextSeq('COLUMNS');
    return this.expression(
      JSONSchemaExpr,
      {
        expressions: this.parseWrappedCsv(() => this.parseJsonColumnDef(), { optional: true }),
      },
    );
  }

  parseJsonTable (): JSONTableExpr {
    const thisExpr = this.parseFormatJson(this.parseBitwise());
    const path = this._match(TokenType.COMMA) && this.parseString();
    const errorHandling = this.parseOnHandling('ERROR', 'ERROR', 'NULL');
    const emptyHandling = this.parseOnHandling('EMPTY', 'ERROR', 'NULL');
    const schema = this.parseJsonSchema();

    return new JSONTableExpr({
      this: thisExpr,
      schema,
      path,
      errorHandling,
      emptyHandling,
    });
  }

  parseMatchAgainst (): MatchAgainstExpr {
    let expressions: Expression[];

    if (this._matchTextSeq('TABLE')) {
      expressions = [];
      const table = this.parseTable();
      if (table) {
        expressions = [table];
      }
    } else {
      expressions = this.parseCsv(() => this.parseColumn());
    }

    this._matchTextSeq(')', 'AGAINST', '(');

    const thisExpr = this.parseString();

    let modifier: string | undefined;
    if (this._matchTextSeq([
      'IN',
      'NATURAL',
      'LANGUAGE',
      'MODE',
    ])) {
      modifier = 'IN NATURAL LANGUAGE MODE';
      if (this._matchTextSeq([
        'WITH',
        'QUERY',
        'EXPANSION',
      ])) {
        modifier = `${modifier} WITH QUERY EXPANSION`;
      }
    } else if (this._matchTextSeq([
      'IN',
      'BOOLEAN',
      'MODE',
    ])) {
      modifier = 'IN BOOLEAN MODE';
    } else if (this._matchTextSeq([
      'WITH',
      'QUERY',
      'EXPANSION',
    ])) {
      modifier = 'WITH QUERY EXPANSION';
    } else {
      modifier = undefined;
    }

    return this.expression(
      MatchAgainstExpr,
      {
        this: thisExpr,
        expressions,
        modifier,
      },
    );
  }

  parseJsonKeyValue (): JSONKeyValueExpr | undefined {
    this._matchTextSeq('KEY');
    const key = this.parseColumn();
    this._matchSet(this._constructor.JSON_KEY_VALUE_SEPARATOR_TOKENS);
    this._matchTextSeq('VALUE');
    const value = this.parseBitwise();

    if (!key && !value) {
      return undefined;
    }
    return this.expression(JSONKeyValueExpr, {
      this: key,
      expression: value,
    });
  }

  parseFormatJson (thisExpr: Expression | undefined): Expression | undefined {
    if (!thisExpr || !this._matchTextSeq(['FORMAT', 'JSON'])) {
      return thisExpr;
    }

    return this.expression(FormatJsonExpr, { this: thisExpr });
  }

  parseOnCondition (): OnConditionExpr | undefined {
    let empty: string | Expression | undefined;
    let error: string | Expression | undefined;

    if (this._dialectConstructor.ON_CONDITION_EMPTY_BEFORE_ERROR) {
      empty = this.parseOnHandling('EMPTY', ...this._constructor.ON_CONDITION_TOKENS);
      error = this.parseOnHandling('ERROR', ...this._constructor.ON_CONDITION_TOKENS);
    } else {
      error = this.parseOnHandling('ERROR', ...this._constructor.ON_CONDITION_TOKENS);
      empty = this.parseOnHandling('EMPTY', ...this._constructor.ON_CONDITION_TOKENS);
    }

    const nullHandling = this.parseOnHandling('NULL', ...this._constructor.ON_CONDITION_TOKENS);

    if (!empty && !error && !nullHandling) {
      return undefined;
    }

    return this.expression(
      OnConditionExpr,
      {
        empty,
        error,
        null: nullHandling,
      },
    );
  }

  parseOnHandling (on: string, ...values: string[]): string | Expression | undefined {
    for (const value of values) {
      if (this._matchTextSeq([
        value,
        'ON',
        on,
      ])) {
        return `${value} ON ${on}`;
      }
    }

    const index = this._index;
    if (this._match(TokenType.DEFAULT)) {
      const defaultValue = this.parseBitwise();
      if (this._matchTextSeq(['ON', on])) {
        return defaultValue;
      }

      this._retreat(index);
    }

    return undefined;
  }

  parseConvert (strict: boolean, options: { safe?: boolean } = {}): Expression | undefined {
    const { safe } = options;
    const thisExpr = this.parseBitwise();

    let to: Expression | undefined;
    if (this._match(TokenType.USING)) {
      to = this.expression(
        CharacterSetExpr,
        { this: this.parseVar({ tokens: new Set([TokenType.BINARY]) }) },
      );
    } else if (this._match(TokenType.COMMA)) {
      to = this.parseTypes();
    } else {
      to = undefined;
    }

    return this.buildCast({
      strict,
      this: thisExpr,
      to,
      safe,
    });
  }

  parseXmlElement (): XMLElementExpr {
    let evalname: boolean | undefined;
    let thisExpr: Expression | undefined;

    if (this._matchTextSeq('EVALNAME')) {
      evalname = true;
      thisExpr = this.parseBitwise();
    } else {
      evalname = undefined;
      this._matchTextSeq('NAME');
      thisExpr = this.parseIdVar();
    }

    return this.expression(
      XMLElementExpr,
      {
        this: thisExpr,
        expressions: this._match(TokenType.COMMA) && this.parseCsv(() => this.parseBitwise()),
        evalname,
      },
    );
  }

  parseXmlTable (): XMLTableExpr {
    let namespaces: XMLNamespaceExpr[] | undefined;
    let passing: Expression[] | undefined;
    let columns: Expression[] | undefined;

    if (this._matchTextSeq(['XMLNAMESPACES', '('])) {
      namespaces = this.parseXmlNamespace();
      this._matchTextSeq(')', ',');
    }

    const thisExpr = this.parseString();

    if (this._matchTextSeq('PASSING')) {
      this._matchTextSeq(['BY', 'VALUE']);
      passing = this.parseCsv(() => this.parseColumn());
    }

    const byRef = this._matchTextSeq([
      'RETURNING',
      'SEQUENCE',
      'BY',
      'REF',
    ]);

    if (this._matchTextSeq('COLUMNS')) {
      columns = this.parseCsv(() => this.parseFieldDef());
    }

    return this.expression(
      XMLTableExpr,
      {
        this: thisExpr,
        namespaces,
        passing,
        columns,
        byRef,
      },
    );
  }

  parseXmlNamespace (): XMLNamespaceExpr[] {
    const namespaces: XMLNamespaceExpr[] = [];

    while (true) {
      let uri: Expression | undefined;
      if (this._match(TokenType.DEFAULT)) {
        uri = this.parseString();
      } else {
        uri = this.parseAlias(this.parseString());
      }
      namespaces.push(this.expression(XMLNamespaceExpr, { this: uri }));
      if (!this._match(TokenType.COMMA)) {
        break;
      }
    }

    return namespaces;
  }

  parseDecode (): DecodeExpr | DecodeCaseExpr | undefined {
    const args = this.parseCsv(() => this.parseDisjunction());

    if (args.length < 3) {
      return this.expression(DecodeExpr, {
        this: seqGet(args, 0),
        charset: seqGet(args, 1),
      });
    }

    return this.expression(DecodeCaseExpr, { expressions: args });
  }

  parseGapFill (): GapFillExpr {
    this._match(TokenType.TABLE);
    const thisExpr = this.parseTable();

    this._match(TokenType.COMMA);
    const args = [thisExpr, ...this.parseCsv(() => this.parseLambda())];

    const gapFill = GapFillExpr.fromArgList(args);
    return this.validateExpression(gapFill, args);
  }

  parseChar (): ChrExpr {
    return this.expression(
      ChrExpr,
      {
        expressions: this.parseCsv(() => this.parseAssignment()),
        charset: this._match(TokenType.USING) && this.parseVar(),
      },
    );
  }

  parseCast (strict: boolean, options: { safe?: boolean } = {}): Expression {
    const { safe } = options;
    const thisExpr = this.parseDisjunction();

    if (!this._match(TokenType.ALIAS)) {
      if (this._match(TokenType.COMMA)) {
        return this.expression(CastToStrTypeExpr, {
          this: thisExpr,
          to: this.parseString(),
        });
      }

      this.raiseError('Expected AS after CAST');
    }

    let fmt: Expression | undefined;
    let to = this.parseTypes();

    let defaultValue: Expression | undefined;
    if (this._match(TokenType.DEFAULT)) {
      defaultValue = this.parseBitwise();
      this._matchTextSeq([
        'ON',
        'CONVERSION',
        'ERROR',
      ]);
    }

    if (this._matchSet(new Set([TokenType.FORMAT, TokenType.COMMA]))) {
      const fmtString = this.parseString();
      fmt = this.parseAtTimeZone(fmtString);

      if (!to) {
        to = DataTypeExpr.build(DataTypeExpr.Type.UNKNOWN);
      }
      if (DataTypeExpr.TEMPORAL_TYPES.has(to.this)) {
        thisExpr = this.expression(
          to.this === DataTypeExpr.Type.DATE ? StrToDateExpr : StrToTimeExpr,
          {
            this: thisExpr,
            format: LiteralExpr.string(
              formatTime(
                fmtString ? fmtString.this : '',
                this._dialectConstructor.FORMAT_MAPPING || this._dialectConstructor.TIME_MAPPING,
                this._dialectConstructor.FORMAT_TRIE || this._dialectConstructor.TIME_TRIE,
              ),
            ),
            safe,
          },
        );

        if (fmt instanceof AtTimeZoneExpr && thisExpr instanceof StrToTimeExpr) {
          thisExpr.setArgKey('zone', fmt.args.zone);
        }
        return thisExpr;
      }
    } else if (!to) {
      this.raiseError('Expected TYPE after CAST');
    } else if (to instanceof IdentifierExpr) {
      to = DataTypeExpr.build(to.name, {
        dialect: this.dialect,
        udt: true,
      });
    } else if (to.this === DataTypeExpr.Type.CHAR) {
      if (this._match(TokenType.CHARACTER_SET)) {
        to = this.expression(CharacterSetExpr, { this: this.parseVarOrString() });
      }
    }

    return this.buildCast({
      strict,
      this: thisExpr,
      to,
      format: fmt,
      safe,
      action: this.parseVarFromOptions(this._constructor.CAST_ACTIONS, { raiseUnmatched: false }),
      default: defaultValue,
    });
  }

  parseStringAgg (): GroupConcatExpr {
    let args: (Expression | undefined)[];

    if (this._match(TokenType.DISTINCT)) {
      args = [this.expression(DistinctExpr, { expressions: [this.parseDisjunction()] })];
      if (this._match(TokenType.COMMA)) {
        args.push(...this.parseCsv(() => this.parseDisjunction()));
      }
    } else {
      args = this.parseCsv(() => this.parseDisjunction());
    }

    let onOverflow: Expression | undefined;
    if (this._matchTextSeq(['ON', 'OVERFLOW'])) {
      if (this._matchTextSeq('ERROR')) {
        onOverflow = var_('ERROR');
      } else {
        this._matchTextSeq('TRUNCATE');
        onOverflow = this.expression(
          OverflowTruncateBehaviorExpr,
          {
            this: this.parseString(),
            withCount:
              this._matchTextSeq(['WITH', 'COUNT'])
              || !this._matchTextSeq(['WITHOUT', 'COUNT']),
          },
        );
      }
    } else {
      onOverflow = undefined;
    }

    const index = this._index;
    if (!this._match(TokenType.R_PAREN) && 0 < args.length) {
      args[0] = this.parseLimit({ this: this.parseOrder({ thisExpr: args[0] }) });
      return this.expression(GroupConcatExpr, {
        this: args[0],
        separator: seqGet(args, 1),
      });
    }

    if (!this._matchTextSeq(['WITHIN', 'GROUP'])) {
      this._retreat(index);
      return this.validateExpression(GroupConcatExpr.fromArgList(args), args);
    }

    this._matchLParen();

    return this.expression(
      GroupConcatExpr,
      {
        this: this.parseOrder({ thisExpr: seqGet(args, 0) }),
        separator: seqGet(args, 1),
        onOverflow,
      },
    );
  }

  parseBracket (thisExpr?: Expression): Expression | undefined {
    if (!this._matchSet(new Set([TokenType.L_BRACKET, TokenType.L_BRACE]))) {
      return thisExpr;
    }

    let parseMap: boolean;
    if (this._constructor.MAP_KEYS_ARE_ARBITRARY_EXPRESSIONS) {
      const mapToken = seqGet(this._tokens, this._index - 2);
      parseMap = mapToken !== undefined && mapToken.text.toUpperCase() === 'MAP';
    } else {
      parseMap = false;
    }

    const bracketKind = this._prev!.tokenType;
    if (
      bracketKind === TokenType.L_BRACE
      && this._curr
      && this._curr.tokenType === TokenType.VAR
      && this._constructor.ODBC_DATETIME_LITERALS.has(this._curr.text.toLowerCase())
    ) {
      return this.parseOdbcDatetimeLiteral();
    }

    const expressions = this.parseCsv(() =>
      this.parseBracketKeyValue({ isMap: bracketKind === TokenType.L_BRACE }));

    if (bracketKind === TokenType.L_BRACKET && !this._match(TokenType.R_BRACKET)) {
      this.raiseError('Expected ]');
    } else if (bracketKind === TokenType.L_BRACE && !this._match(TokenType.R_BRACE)) {
      this.raiseError('Expected }');
    }

    if (bracketKind === TokenType.L_BRACE) {
      thisExpr = this.expression(
        StructExpr,
        {
          expressions: this.kvToPropEq({
            expressions,
            parseMap,
          }),
        },
      );
    } else if (!thisExpr) {
      thisExpr = buildArrayConstructor(ArrayExpr, {
        args: expressions,
        bracketKind,
        dialect: this.dialect,
      });
    } else {
      const constructorType = this._constructor.ARRAY_CONSTRUCTORS[thisExpr.name?.toUpperCase()];
      if (constructorType) {
        return buildArrayConstructor(constructorType, {
          args: expressions,
          bracketKind,
          dialect: this.dialect,
        });
      }

      const adjustedExpressions = applyIndexOffset(
        thisExpr,
        expressions,
        -this._dialectConstructor.INDEX_OFFSET,
        { dialect: this.dialect },
      );
      thisExpr = this.expression(
        BracketExpr,
        {
          this: thisExpr,
          expressions: adjustedExpressions,
          comments: thisExpr.popComments(),
        },
      );
    }

    this._addComments(thisExpr);
    return this.parseBracket(thisExpr);
  }

  parseSlice (thisExpr: Expression | undefined): Expression | undefined {
    if (!this._match(TokenType.COLON)) {
      return thisExpr;
    }

    let end: Expression | undefined;
    if (this._matchPair(TokenType.DASH, TokenType.COLON, { advance: false })) {
      this._advance();
      end = -(LiteralExpr.number('1'));
    } else {
      end = this.parseAssignment();
    }

    const step = this._match(TokenType.COLON) ? this.parseUnary() : undefined;
    return this.expression(SliceExpr, {
      this: thisExpr,
      expression: end,
      step,
    });
  }

  parseCase (): Expression | undefined {
    if (this._match(TokenType.DOT, { advance: false })) {
      this._retreat(this._index - 1);
      return undefined;
    }

    const ifs: IfExpr[] = [];
    let defaultCase: Expression | undefined;

    const comments = this._prevComments;
    const expression = this.parseDisjunction();

    while (this._match(TokenType.WHEN)) {
      const thisExpr = this.parseDisjunction();
      this._match(TokenType.THEN);
      const then = this.parseDisjunction();
      ifs.push(this.expression(IfExpr, {
        this: thisExpr,
        true: then,
      }));
    }

    if (this._match(TokenType.ELSE)) {
      defaultCase = this.parseDisjunction();
    }

    if (!this._match(TokenType.END)) {
      if (defaultCase instanceof IntervalExpr && defaultCase.this.sql().toUpperCase() === 'END') {
        defaultCase = column('interval');
      } else {
        this.raiseError('Expected END after CASE', this._prev);
      }
    }

    return this.expression(
      CaseExpr,
      {
        comments,
        this: expression,
        ifs,
        default: defaultCase,
      },
    );
  }

  parseIf (): Expression | undefined {
    if (this._match(TokenType.L_PAREN)) {
      const args = this.parseCsv(() =>
        this.parseAlias(this.parseAssignment(), { explicit: true }));
      const thisExpr = this.validateExpression(IfExpr.fromArgList(args), args);
      this._matchRParen();
      return thisExpr;
    } else {
      const index = this._index - 1;

      if (this._constructor.NO_PAREN_IF_COMMANDS && index === 0) {
        return this.parseAsCommand(this._prev);
      }

      const condition = this.parseDisjunction();

      if (!condition) {
        this._retreat(index);
        return undefined;
      }

      this._match(TokenType.THEN);
      const trueExpr = this.parseDisjunction();
      const falseExpr = this._match(TokenType.ELSE) ? this.parseDisjunction() : undefined;
      this._match(TokenType.END);
      return this.expression(IfExpr, {
        this: condition,
        true: trueExpr,
        false: falseExpr,
      });
    }
  }

  parseNextValueFor (): Expression | undefined {
    if (!this._matchTextSeq(['VALUE', 'FOR'])) {
      this._retreat(this._index - 1);
      return undefined;
    }

    return this.expression(
      NextValueForExpr,
      {
        this: this.parseColumn(),
        order: this._match(TokenType.OVER) && this.parseWrapped(() => this.parseOrder()),
      },
    );
  }

  parseExtract (): ExtractExpr | AnonymousExpr {
    const thisExpr = this.parseFunction() || this.parseVarOrString({ upper: true });

    if (this._match(TokenType.FROM)) {
      return this.expression(ExtractExpr, {
        this: thisExpr,
        expression: this.parseBitwise(),
      });
    }

    if (!this._match(TokenType.COMMA)) {
      this.raiseError('Expected FROM or comma after EXTRACT', this._prev);
    }

    return this.expression(ExtractExpr, {
      this: thisExpr,
      expression: this.parseBitwise(),
    });
  }

  parsePrimaryKeyPart (): Expression | undefined {
    return this.parseField();
  }

  parsePeriodForSystemTime (): PeriodForSystemTimeConstraintExpr | undefined {
    if (!this._match(TokenType.TIMESTAMP_SNAPSHOT)) {
      this._retreat(this._index - 1);
      return undefined;
    }

    const idVars = this.parseWrappedIdVars();
    return this.expression(
      PeriodForSystemTimeConstraintExpr,
      {
        this: seqGet(idVars, 0),
        expression: seqGet(idVars, 1),
      },
    );
  }

  parsePrimaryKey (options?: {
    wrappedOptional?: boolean;
    inProps?: boolean;
    namedPrimaryKey?: boolean;
  }): PrimaryKeyColumnConstraintExpr | PrimaryKeyExpr {
    const wrappedOptional = options?.wrappedOptional || false;
    const inProps = options?.inProps || false;
    const namedPrimaryKey = options?.namedPrimaryKey || false;

    const desc =
      this._matchSet(new Set([TokenType.ASC, TokenType.DESC]))
      && this._prev?.tokenType === TokenType.DESC;

    let thisExpr: Expression | undefined;
    if (
      namedPrimaryKey
      && !this._constructor.CONSTRAINT_PARSERS.has(this._curr?.text.toUpperCase())
      && this._next
      && this._next.tokenType === TokenType.L_PAREN
    ) {
      thisExpr = this.parseIdVar();
    }

    if (!inProps && !this._match(TokenType.L_PAREN, { advance: false })) {
      return this.expression(
        PrimaryKeyColumnConstraintExpr,
        {
          desc,
          options: this.parseKeyConstraintOptions(),
        },
      );
    }

    const expressions = this.parseWrappedCsv(
      () => this.parsePrimaryKeyPart(),
      { optional: wrappedOptional },
    );

    return this.expression(
      PrimaryKeyExpr,
      {
        this: thisExpr,
        expressions,
        include: this.parseIndexParams(),
        options: this.parseKeyConstraintOptions(),
      },
    );
  }

  parseBracketKeyValue (options?: { isMap?: boolean }): Expression | undefined {
    return this.parseSlice(this.parseAlias(this.parseDisjunction(), { explicit: true }));
  }

  parseOdbcDatetimeLiteral (): Expression {
    this._match(TokenType.VAR);
    const expClass = this._constructor.ODBC_DATETIME_LITERALS[this._prev!.text.toLowerCase()];
    const expression = this.expression(expClass, { this: this.parseString() });
    if (!this._match(TokenType.R_BRACE)) {
      this.raiseError('Expected }');
    }
    return expression;
  }

  parseUniqueKey (): Expression | undefined {
    return this.parseIdVar({ anyToken: false });
  }

  parseUnique (): UniqueColumnConstraintExpr {
    this._matchTexts(['KEY', 'INDEX']);
    return this.expression(
      UniqueColumnConstraintExpr,
      {
        nulls: this._matchTextSeq([
          'NULLS',
          'NOT',
          'DISTINCT',
        ]),
        this: this.parseSchema({ this: this.parseUniqueKey() }),
        indexType: this._match(TokenType.USING) && this.advanceAny() && this._prev?.text,
        onConflict: this.parseOnConflict(),
        options: this.parseKeyConstraintOptions(),
      },
    );
  }

  parseKeyConstraintOptions (): string[] {
    const options: string[] = [];
    while (true) {
      if (!this._curr) {
        break;
      }

      if (this._match(TokenType.ON)) {
        let action: string | undefined;
        const on = this.advanceAny() && this._prev?.text;

        if (this._matchTextSeq(['NO', 'ACTION'])) {
          action = 'NO ACTION';
        } else if (this._matchTextSeq('CASCADE')) {
          action = 'CASCADE';
        } else if (this._matchTextSeq('RESTRICT')) {
          action = 'RESTRICT';
        } else if (this._matchPair(TokenType.SET, TokenType.NULL)) {
          action = 'SET NULL';
        } else if (this._matchPair(TokenType.SET, TokenType.DEFAULT)) {
          action = 'SET DEFAULT';
        } else {
          this.raiseError('Invalid key constraint');
        }

        options.push(`ON ${on} ${action}`);
      } else {
        const var_ = this.parseVarFromOptions(this._constructor.KEY_CONSTRAINT_OPTIONS, { raiseUnmatched: false });
        if (!var_) {
          break;
        }
        options.push(var_.name);
      }
    }

    return options;
  }

  parseReferences (options: { match?: boolean } = {}): ReferenceExpr | undefined {
    const { match = true } = options;
    if (match && !this._match(TokenType.REFERENCES)) {
      return undefined;
    }

    let expressions: Expression[] | undefined;
    const thisExpr = this.parseTable({ schema: true });
    const constraintOptions = this.parseKeyConstraintOptions();
    return this.expression(ReferenceExpr, {
      this: thisExpr,
      expressions,
      options: constraintOptions,
    });
  }

  parseForeignKey (): ForeignKeyExpr {
    const expressions = !this._match(TokenType.REFERENCES, { advance: false })
      ? this.parseWrappedIdVars()
      : undefined;

    const reference = this.parseReferences();
    const onOptions: Record<string, string> = {};

    while (this._match(TokenType.ON)) {
      if (!this._matchSet(new Set([TokenType.DELETE, TokenType.UPDATE]))) {
        this.raiseError('Expected DELETE or UPDATE');
      }

      const kind = this._prev!.text.toLowerCase();

      let action: string;
      if (this._matchTextSeq(['NO', 'ACTION'])) {
        action = 'NO ACTION';
      } else if (this._match(TokenType.SET)) {
        this._matchSet(new Set([TokenType.NULL, TokenType.DEFAULT]));
        action = 'SET ' + this._prev!.text.toUpperCase();
      } else {
        this._advance();
        action = this._prev!.text.toUpperCase();
      }

      onOptions[kind] = action;
    }

    return this.expression(
      ForeignKeyExpr,
      {
        expressions,
        reference,
        options: this.parseKeyConstraintOptions(),
        ...onOptions,
      },
    );
  }

  parsePrimary (): Expression | undefined {
    if (this._matchSet(this._constructor.PRIMARY_PARSERS)) {
      const tokenType = this._prev!.tokenType;
      const primary = this._constructor.PRIMARY_PARSERS[tokenType](this, this._prev!);

      if (tokenType === TokenType.STRING) {
        const expressions = [primary];
        while (this._match(TokenType.STRING)) {
          expressions.push(LiteralExpr.string(this._prev!.text));
        }

        if (1 < expressions.length) {
          return this.expression(
            ConcatExpr,
            {
              expressions,
              coalesce: this._dialectConstructor.CONCAT_COALESCE,
            },
          );
        }
      }

      return primary;
    }

    if (this._matchPair(TokenType.DOT, TokenType.NUMBER)) {
      return LiteralExpr.number(`0.${this._prev!.text}`);
    }

    return this.parseParen();
  }

  parseField (options?: { anyToken?: boolean;
    tokens?: Iterable<TokenType>;
    anonymousFunc?: boolean; }): Expression | undefined {
    const {
      anyToken, tokens, anonymousFunc,
    } = options || {};

    let field: Expression | undefined;
    if (anonymousFunc) {
      field = (
        this.parseFunction({
          anonymous: anonymousFunc,
          anyToken,
        })
        || this.parsePrimary()
      );
    } else {
      field = this.parsePrimary() || this.parseFunction({
        anonymous: anonymousFunc,
        anyToken,
      });
    }
    return field || this.parseIdVar({
      anyToken,
      tokens,
    });
  }

  parseGeneratedAsIdentity (): GeneratedAsIdentityColumnConstraintExpr | ComputedColumnConstraintExpr | GeneratedAsRowColumnConstraintExpr {
    let thisExpr: GeneratedAsIdentityColumnConstraintExpr;

    if (this._matchTextSeq(['BY', 'DEFAULT'])) {
      const onNull = this._matchPair(TokenType.ON, TokenType.NULL);
      thisExpr = this.expression(
        GeneratedAsIdentityColumnConstraintExpr,
        {
          this: false,
          onNull,
        },
      );
    } else {
      this._matchTextSeq('ALWAYS');
      thisExpr = this.expression(GeneratedAsIdentityColumnConstraintExpr, { this: true });
    }

    this._match(TokenType.ALIAS);

    if (this._matchTextSeq('ROW')) {
      const start = this._matchTextSeq('START');
      if (!start) {
        this._match(TokenType.END);
      }
      const hidden = this._matchTextSeq('HIDDEN');
      return this.expression(GeneratedAsRowColumnConstraintExpr, {
        start,
        hidden,
      });
    }

    const identity = this._matchTextSeq('IDENTITY');

    if (this._match(TokenType.L_PAREN)) {
      if (this._match(TokenType.START_WITH)) {
        thisExpr.setArgKey('start', this.parseBitwise());
      }
      if (this._matchTextSeq(['INCREMENT', 'BY'])) {
        thisExpr.setArgKey('increment', this.parseBitwise());
      }
      if (this._matchTextSeq('MINVALUE')) {
        thisExpr.setArgKey('minvalue', this.parseBitwise());
      }
      if (this._matchTextSeq('MAXVALUE')) {
        thisExpr.setArgKey('maxvalue', this.parseBitwise());
      }

      if (this._matchTextSeq('CYCLE')) {
        thisExpr.setArgKey('cycle', true);
      } else if (this._matchTextSeq(['NO', 'CYCLE'])) {
        thisExpr.setArgKey('cycle', false);
      }

      if (!identity) {
        thisExpr.setArgKey('expression', this.parseRange());
      } else if (!thisExpr.args.start && this._match(TokenType.NUMBER, { advance: false })) {
        const args = this.parseCsv(this.parseBitwise.bind(this));
        thisExpr.setArgKey('start', seqGet(args, 0));
        thisExpr.setArgKey('increment', seqGet(args, 1));
      }

      this._matchRParen();
    }

    return thisExpr;
  }

  parseInline (): InlineLengthColumnConstraintExpr {
    this._matchTextSeq('LENGTH');
    return this.expression(InlineLengthColumnConstraintExpr, { this: this.parseBitwise() });
  }

  parseNotConstraint (): Expression | undefined {
    if (this._matchTextSeq('NULL')) {
      return this.expression(NotNullColumnConstraintExpr);
    }
    if (this._matchTextSeq('CASESPECIFIC')) {
      return this.expression(CaseSpecificColumnConstraintExpr, { not_: true });
    }
    if (this._matchTextSeq(['FOR', 'REPLICATION'])) {
      return this.expression(NotForReplicationColumnConstraintExpr);
    }

    this._retreat(this._index - 1);
    return undefined;
  }

  parseColumnConstraint (): Expression | undefined {
    const thisExpr = this._match(TokenType.CONSTRAINT) && this.parseIdVar();

    const procedureOptionFollows =
      this._match(TokenType.WITH, { advance: false })
      && this._next
      && this._next.text.toUpperCase() in this._constructor.PROCEDURE_OPTIONS;

    if (!procedureOptionFollows && this._matchTexts(this._constructor.CONSTRAINT_PARSERS)) {
      const constraint = this._constructor.CONSTRAINT_PARSERS[this._prev!.text.toUpperCase()](this);
      if (!constraint) {
        this._retreat(this._index - 1);
        return undefined;
      }

      return this.expression(ColumnConstraintExpr, {
        this: thisExpr,
        kind: constraint,
      });
    }

    return thisExpr;
  }

  parseConstraint (): Expression | undefined {
    if (!this._match(TokenType.CONSTRAINT)) {
      return this.parseUnnamedConstraint({ constraints: this._constructor.SCHEMA_UNNAMED_CONSTRAINTS });
    }

    return this.expression(
      ConstraintExpr,
      {
        this: this.parseIdVar(),
        expressions: this.parseUnnamedConstraints(),
      },
    );
  }

  parseUnnamedConstraints (): Expression[] {
    const constraints: Expression[] = [];
    while (true) {
      const constraint = this.parseUnnamedConstraint() || this.parseFunction();
      if (!constraint) {
        break;
      }
      constraints.push(constraint);
    }

    return constraints;
  }

  parseUnnamedConstraint (options?: { constraints?: Set<string> | string[] }): Expression | undefined {
    const index = this._index;
    const constraints = options?.constraints;

    if (
      this._match(TokenType.IDENTIFIER, { advance: false })
      || !this._matchTexts(constraints || this._constructor.CONSTRAINT_PARSERS)
    ) {
      return undefined;
    }

    const constraintName = this._prev!.text.toUpperCase();
    if (!(constraintName in this._constructor.CONSTRAINT_PARSERS)) {
      this.raiseError(`No parser found for schema constraint ${constraintName}.`);
    }

    const constraint = this._constructor.CONSTRAINT_PARSERS[constraintName](this);
    if (!constraint) {
      this._retreat(index);
    }

    return constraint;
  }

  parseFieldDef (): Expression | undefined {
    return this.parseColumnDef(this.parseField({ anyToken: true }));
  }

  parseColumnDef (
    thisExpr: Expression | undefined,
    options?: { computedColumn?: boolean },
  ): Expression | undefined {
    const computedColumn = options?.computedColumn ?? true;
    let thisResult = thisExpr;

    if (thisResult instanceof ColumnExpr) {
      thisResult = thisResult.this;
    }

    if (!computedColumn) {
      this._match(TokenType.ALIAS);
    }

    let kind = this.parseTypes({ schema: true });

    if (this._matchTextSeq(['FOR', 'ORDINALITY'])) {
      return this.expression(ColumnDefExpr, {
        this: thisResult,
        ordinality: true,
      });
    }

    const constraints: Expression[] = [];

    if ((!kind && this._match(TokenType.ALIAS)) || this._matchTexts(['ALIAS', 'MATERIALIZED'])) {
      const persisted = this._prev!.text.toUpperCase() === 'MATERIALIZED';
      const constraintKind = new ComputedColumnConstraintExpr({
        this: this.parseDisjunction(),
        persisted: persisted || this._matchTextSeq('PERSISTED'),
        dataType: this._matchTextSeq('AUTO')
          ? new VarExpr({ this: 'AUTO' })
          : this.parseTypes(),
        notNull: this._matchPair(TokenType.NOT, TokenType.NULL),
      });
      constraints.push(this.expression(ColumnConstraintExpr, { kind: constraintKind }));
    } else if (!kind && this._matchSet([TokenType.IN, TokenType.OUT], { advance: false })) {
      const inOutConstraint = this.expression(
        InOutColumnConstraintExpr,
        {
          input: this._match(TokenType.IN),
          output: this._match(TokenType.OUT),
        },
      );
      constraints.push(inOutConstraint);
      kind = this.parseTypes();
    } else if (
      kind
      && this._match(TokenType.ALIAS, { advance: false })
      && (!this._constructor.WRAPPED_TRANSFORM_COLUMN_CONSTRAINT
        || (this._next && this._next.tokenType === TokenType.L_PAREN))
    ) {
      this._advance();
      constraints.push(
        this.expression(
          ColumnConstraintExpr,
          {
            kind: new ComputedColumnConstraintExpr({
              this: this.parseDisjunction(),
              persisted:
                this._matchTexts(['STORED', 'VIRTUAL'])
                && this._prev!.text.toUpperCase() === 'STORED',
            }),
          },
        ),
      );
    }

    while (true) {
      const constraint = this.parseColumnConstraint();
      if (!constraint) {
        break;
      }
      constraints.push(constraint);
    }

    if (!kind && constraints.length === 0) {
      return thisResult;
    }

    return this.expression(ColumnDefExpr, {
      this: thisResult,
      kind,
      constraints,
    });
  }

  parseAutoIncrement (): GeneratedAsIdentityColumnConstraintExpr | AutoIncrementColumnConstraintExpr {
    let start: Expression | undefined;
    let increment: Expression | undefined;
    let order: boolean | undefined;

    if (this._match(TokenType.L_PAREN, { advance: false })) {
      const args = this.parseWrappedCsv(this.parseBitwise.bind(this));
      start = seqGet(args, 0);
      increment = seqGet(args, 1);
    } else if (this._matchTextSeq('START')) {
      start = this.parseBitwise();
      this._matchTextSeq('INCREMENT');
      increment = this.parseBitwise();
      if (this._matchTextSeq('ORDER')) {
        order = true;
      } else if (this._matchTextSeq('NOORDER')) {
        order = false;
      }
    }

    if (start && increment) {
      return new GeneratedAsIdentityColumnConstraintExpr({
        start,
        increment,
        this: false,
        order,
      });
    }

    return new AutoIncrementColumnConstraintExpr();
  }

  parseCheckConstraint (): CheckColumnConstraintExpr | undefined {
    if (!this._match(TokenType.L_PAREN, { advance: false })) {
      return undefined;
    }

    return this.expression(
      CheckColumnConstraintExpr,
      {
        this: this.parseWrapped(this.parseAssignment.bind(this)),
        enforced: this._matchTextSeq('ENFORCED'),
      },
    );
  }

  parseAutoProperty (): AutoRefreshPropertyExpr | undefined {
    if (!this._matchTextSeq('REFRESH')) {
      this._retreat(this._index - 1);
      return undefined;
    }
    return this.expression(AutoRefreshPropertyExpr, { this: this.parseVar({ upper: true }) });
  }

  parseCompress (): CompressColumnConstraintExpr {
    if (this._match(TokenType.L_PAREN, { advance: false })) {
      return this.expression(
        CompressColumnConstraintExpr,
        { this: this.parseWrappedCsv(this.parseBitwise.bind(this)) },
      );
    }

    return this.expression(CompressColumnConstraintExpr, { this: this.parseBitwise() });
  }

  parseFunction (options?: {
    functions?: Record<string, Function>;
    anonymous?: boolean;
    optionalParens?: boolean;
    anyToken?: boolean;
  }): Expression | undefined {
    const functions = options?.functions;
    const anonymous = options?.anonymous ?? false;
    const optionalParens = options?.optionalParens ?? true;
    const anyToken = options?.anyToken ?? false;

    let fnSyntax = false;
    if (
      this._match(TokenType.L_BRACE, { advance: false })
      && this._next
      && this._next.text.toUpperCase() === 'FN'
    ) {
      this._advance(2);
      fnSyntax = true;
    }

    const func = this.parseFunctionCall({
      functions,
      anonymous,
      optionalParens,
      anyToken,
    });

    if (fnSyntax) {
      this._match(TokenType.R_BRACE);
    }

    return func;
  }

  parseFunctionArgs (options?: { alias?: boolean }): Expression[] {
    const alias = options?.alias ?? false;
    return this.parseCsv(() => this.parseLambda({ alias }));
  }

  parseFunctionCall (options?: {
    functions?: Record<string, Function>;
    anonymous?: boolean;
    optionalParens?: boolean;
    anyToken?: boolean;
  }): Expression | undefined {
    const functions = options?.functions;
    const anonymous = options?.anonymous ?? false;
    const optionalParens = options?.optionalParens ?? true;
    const anyToken = options?.anyToken ?? false;

    if (!this._curr) {
      return undefined;
    }

    const comments = this._curr.comments;
    const prev = this._prev;
    const token = this._curr;
    const tokenType = this._curr.tokenType;
    const thisText = this._curr.text;
    const upper = thisText.toUpperCase();

    const parser = this._constructor.NO_PAREN_FUNCTION_PARSERS[upper];
    if (parser && optionalParens && !this._constructor.INVALID_FUNC_NAME_TOKENS.has(tokenType)) {
      this._advance();
      return this.parseWindow(parser(this));
    }

    if (!this._next || this._next.tokenType !== TokenType.L_PAREN) {
      if (optionalParens && this._constructor.NO_PAREN_FUNCTIONS[tokenType]) {
        this._advance();
        return this.expression(this._constructor.NO_PAREN_FUNCTIONS[tokenType]);
      }

      return undefined;
    }

    if (anyToken) {
      if (this._constructor.RESERVED_TOKENS.has(tokenType)) {
        return undefined;
      }
    } else if (!this._constructor.FUNC_TOKENS.has(tokenType)) {
      return undefined;
    }

    this._advance(2);

    const funcParser = this._constructor.FUNCTION_PARSERS[upper];
    if (funcParser && !anonymous) {
      const thisExpr: Expression = funcParser(this);

      if (thisExpr instanceof Expression) {
        thisExpr.addComments(comments);
      }

      this._matchRParen(thisExpr);
      return this.parseWindow(thisExpr);
    } else {
      const subqueryPredicate = this._constructor.SUBQUERY_PREDICATES[tokenType];

      if (subqueryPredicate) {
        let expr: Expression | undefined;
        if (
          this._curr.tokenType === TokenType.SELECT
          || this._curr.tokenType === TokenType.WITH
        ) {
          expr = this.parseSelect();
          this._matchRParen();
        } else if (
          prev
          && (prev.tokenType === TokenType.LIKE || prev.tokenType === TokenType.ILIKE)
        ) {
          this._advance(-1);
          expr = this.parseBitwise();
        }

        if (expr) {
          return this.expression(subqueryPredicate, {
            comments,
            this: expr,
          });
        }
      }

      const functionsMap = functions ?? this._constructor.FUNCTIONS;

      const functionBuilder = functionsMap[upper];
      const knownFunction = functionBuilder && !anonymous;

      const alias =
        !knownFunction || this._constructor.FUNCTIONS_WITH_ALIASED_ARGS.has(upper);
      const args = this.parseFunctionArgs({ alias });

      const postFuncComments = this._curr?.comments;
      let isKnownFunction = knownFunction;
      if (isKnownFunction && postFuncComments) {
        if (
          postFuncComments.some((comment) =>
            comment.trimStart().startsWith(SQLGLOT_ANONYMOUS))
        ) {
          isKnownFunction = false;
        }
      }

      const argsWithPropEq = alias && isKnownFunction ? this.kvToPropEq(args) : args;

      let thisExpr: Expression;

      if (isKnownFunction) {
        const funcBuilder = functionBuilder as Function;

        let func: Expression;
        if (1 < funcBuilder.length) {
          func = funcBuilder(argsWithPropEq, this.dialect);
        } else {
          func = funcBuilder(argsWithPropEq);
        }

        func = this.validateExpression(func, argsWithPropEq);
        if (this._dialectConstructor.PRESERVE_ORIGINAL_NAMES) {
          func.meta.name = thisText;
        }

        thisExpr = func;
      } else {
        let thisValue: Expression | string = thisText;
        if (tokenType === TokenType.IDENTIFIER) {
          thisValue = new IdentifierExpr({
            this: thisText,
            quoted: true,
          }).updatePositions(token);
        }

        thisExpr = this.expression(AnonymousExpr, {
          this: thisValue,
          expressions: args,
        });
      }

      thisExpr = thisExpr.updatePositions(token);

      if (thisExpr instanceof Expression) {
        thisExpr.addComments(comments);
      }

      this._matchRParen(thisExpr);
      return this.parseWindow(thisExpr);
    }
  }

  toPropEq (expression: Expression, index: number): Expression {
    return expression;
  }

  kvToPropEq (expressions: Expression[], options?: { parseMap?: boolean }): Expression[] {
    const parseMap = options?.parseMap ?? false;
    const transformed: Expression[] = [];

    for (let index = 0; index < expressions.length; index++) {
      let e = expressions[index];

      if (this._constructor.KEY_VALUE_DEFINITIONS.some((def) => e instanceof def)) {
        if (e instanceof AliasExpr) {
          e = this.expression(PropertyEQExpr, {
            this: e.args.alias,
            expression: e.this,
          });
        }

        if (!(e instanceof PropertyEQExpr)) {
          e = this.expression(PropertyEQExpr, {
            this: parseMap ? e.this : toIdentifier(e.this.name),
            expression: e.expression,
          });
        }

        if (e.this instanceof ColumnExpr) {
          e.this.replace(e.this.this);
        }
      } else {
        e = this.toPropEq(e, index);
      }

      transformed.push(e);
    }

    return transformed;
  }

  parseUserDefinedFunctionExpression (): Expression | undefined {
    return this.parseStatement();
  }

  parseFunctionParameter (): Expression | undefined {
    return this.parseColumnDef(this.parseIdVar(), { computedColumn: false });
  }

  parseUserDefinedFunction (options?: { kind?: TokenType }): Expression | undefined {
    const thisExpr = this.parseTableParts({ schema: true });

    if (!this._match(TokenType.L_PAREN)) {
      return thisExpr;
    }

    const expressions = this.parseCsv(this.parseFunctionParameter.bind(this));
    this._matchRParen();
    return this.expression(UserDefinedFunctionExpr, {
      this: thisExpr,
      expressions,
      wrapped: true,
    });
  }

  parseIntroducer (token: Token): IntroducerExpr | IdentifierExpr {
    const literal = this.parsePrimary();
    if (literal) {
      return this.expression(IntroducerExpr, {
        token,
        expression: literal,
      });
    }

    return this.identifierExpression(token);
  }

  parseSessionParameter (): SessionParameterExpr {
    let kind: string | undefined;
    let thisExpr = this.parseIdVar() || this.parsePrimary();

    if (thisExpr && this._match(TokenType.DOT)) {
      kind = thisExpr.name;
      thisExpr = this.parseVar() || this.parsePrimary();
    }

    return this.expression(SessionParameterExpr, {
      this: thisExpr,
      kind,
    });
  }

  parseLambdaArg (): Expression | undefined {
    return this.parseIdVar();
  }

  parseLambda (options?: { alias?: boolean }): Expression | undefined {
    const alias = options?.alias ?? false;
    const index = this._index;

    let expressions: (Expression | undefined)[];

    if (this._match(TokenType.L_PAREN)) {
      expressions = this.parseCsv(this.parseLambdaArg.bind(this));

      if (!this._match(TokenType.R_PAREN)) {
        this._retreat(index);
      }
    } else {
      expressions = [this.parseLambdaArg()];
    }

    if (this._matchSet(this._constructor.LAMBDAS)) {
      return this._constructor.LAMBDAS[this._prev!.tokenType](this, expressions);
    }

    this._retreat(index);

    let thisExpr: Expression | undefined;

    if (this._match(TokenType.DISTINCT)) {
      thisExpr = this.expression(DistinctExpr, {
        expressions: this.parseCsv(this.parseDisjunction.bind(this)),
      });
    } else {
      thisExpr = this.parseSelectOrExpression({ alias });
    }

    return this.parseLimit(
      this.parseOrder({ thisExpr: this.parseHavingMax(this.parseRespectOrIgnoreNulls(thisExpr)) }),
    );
  }

  parseSchema (options: { this?: Expression } = {}): Expression | undefined {
    const { this: thisExpr } = options;

    const index = this._index;
    if (!this._match(TokenType.L_PAREN)) {
      return thisExpr;
    }

    if (this._matchSet(this._constructor.SELECT_START_TOKENS)) {
      this._retreat(index);
      return thisExpr;
    }

    const args = this.parseCsv(() => this.parseConstraint() || this.parseFieldDef());
    this._matchRParen();
    return this.expression(SchemaExpr, {
      this: thisExpr,
      expressions: args,
    });
  }

  parseAlterTableSet (): AlterSetExpr {
    const alterSet = this.expression(AlterSetExpr);

    if (
      this._match(TokenType.L_PAREN, { advance: false })
      || this._matchTextSeq(['TABLE', 'PROPERTIES'])
    ) {
      alterSet.setArgKey('expressions', this.parseWrappedCsv(this.parseAssignment.bind(this)));
    } else if (this._matchTextSeq('FILESTREAM_ON', { advance: false })) {
      alterSet.setArgKey('expressions', [this.parseAssignment()]);
    } else if (this._matchTexts(['LOGGED', 'UNLOGGED'])) {
      alterSet.setArgKey('option', var_(this._prev!.text.toUpperCase()));
    } else if (this._matchTextSeq('WITHOUT') && this._matchTexts(['CLUSTER', 'OIDS'])) {
      alterSet.setArgKey('option', var_(`WITHOUT ${this._prev!.text.toUpperCase()}`));
    } else if (this._matchTextSeq('LOCATION')) {
      alterSet.setArgKey('location', this.parseField());
    } else if (this._matchTextSeq(['ACCESS', 'METHOD'])) {
      alterSet.setArgKey('accessMethod', this.parseField());
    } else if (this._matchTextSeq('TABLESPACE')) {
      alterSet.setArgKey('tablespace', this.parseField());
    } else if (this._matchTextSeq(['FILE', 'FORMAT']) || this._matchTextSeq('FILEFORMAT')) {
      alterSet.setArgKey('fileFormat', [this.parseField()]);
    } else if (this._matchTextSeq('STAGE_FILE_FORMAT')) {
      alterSet.setArgKey('fileFormat', this.parseWrappedOptions());
    } else if (this._matchTextSeq('STAGE_COPY_OPTIONS')) {
      alterSet.setArgKey('copyOptions', this.parseWrappedOptions());
    } else if (this._matchTextSeq('TAG') || this._matchTextSeq('TAGS')) {
      alterSet.setArgKey('tag', this.parseCsv(this.parseAssignment.bind(this)));
    } else {
      if (this._matchTextSeq('SERDE')) {
        alterSet.setArgKey('serde', this.parseField());
      }

      const properties = this.parseWrapped(this.parseProperties.bind(this), { optional: true });
      alterSet.setArgKey('expressions', [properties]);
    }

    return alterSet;
  }

  parseAlterSession (): AlterSessionExpr {
    if (this._match(TokenType.SET)) {
      const expressions = this.parseCsv(() => this.parseSetItemAssignment());
      return this.expression(AlterSessionExpr, {
        expressions,
        unset: false,
      });
    }

    this._matchTextSeq('UNSET');
    const expressions = this.parseCsv(() =>
      this.expression(SetItemExpr, { this: this.parseIdVar({ anyToken: true }) }));
    return this.expression(AlterSessionExpr, {
      expressions,
      unset: true,
    });
  }

  parseAlter (): AlterExpr | CommandExpr {
    const start = this._prev;

    const alterToken = this._matchSet(this._constructor.ALTERABLES) && this._prev;
    if (!alterToken) {
      return this.parseAsCommand(start);
    }

    const exists = this.parseExists();
    const only = this._matchTextSeq('ONLY');

    let thisExpr: Expression | undefined;
    let check: boolean | undefined;
    let cluster: Expression | undefined;

    if (alterToken.tokenType === TokenType.SESSION) {
      thisExpr = undefined;
      check = undefined;
      cluster = undefined;
    } else {
      thisExpr = this.parseTable({
        schema: true,
        parsePartition: this._constructor.ALTER_TABLE_PARTITIONS,
      });
      check = this._matchTextSeq(['WITH', 'CHECK']);
      cluster = this._match(TokenType.ON) ? this.parseOnProperty() : undefined;

      if (this._next) {
        this._advance();
      }
    }

    const parser = this._prev
      ? this._constructor.ALTER_PARSERS[this._prev.text.toUpperCase()]
      : undefined;
    if (parser) {
      const actions = ensureList(parser(this));
      const notValid = this._matchTextSeq(['NOT', 'VALID']);
      const options = this.parseCsv(this.parseProperty.bind(this));
      const cascade =
        this._dialectConstructor.ALTER_TABLE_SUPPORTS_CASCADE
        && this._matchTextSeq('CASCADE');

      if (!this._curr && actions) {
        return this.expression(AlterExpr, {
          this: thisExpr,
          kind: alterToken.text.toUpperCase(),
          exists,
          actions,
          only,
          options,
          cluster,
          notValid,
          check,
          cascade,
        });
      }
    }

    return this.parseAsCommand(start);
  }

  parseAnalyze (): AnalyzeExpr | CommandExpr {
    const start = this._prev;

    if (!this._curr) {
      return this.expression(AnalyzeExpr);
    }

    const options: string[] = [];
    while (this._matchTexts(this._constructor.ANALYZE_STYLES)) {
      if (this._prev!.text.toUpperCase() === 'BUFFER_USAGE_LIMIT') {
        options.push(`BUFFER_USAGE_LIMIT ${this.parseNumber()}`);
      } else {
        options.push(this._prev!.text.toUpperCase());
      }
    }

    let thisExpr: Expression | undefined;
    let innerExpression: Expression | undefined;

    let kind = this._curr?.text.toUpperCase();

    if (this._match(TokenType.TABLE) || this._match(TokenType.INDEX)) {
      thisExpr = this.parseTableParts();
    } else if (this._matchTextSeq('TABLES')) {
      if (this._matchSet([TokenType.FROM, TokenType.IN])) {
        kind = `${kind} ${this._prev!.text.toUpperCase()}`;
        thisExpr = this.parseTable({
          schema: true,
          isDbReference: true,
        });
      }
    } else if (this._matchTextSeq('DATABASE')) {
      thisExpr = this.parseTable({
        schema: true,
        isDbReference: true,
      });
    } else if (this._matchTextSeq('CLUSTER')) {
      thisExpr = this.parseTable();
    } else if (this._matchTexts(this._constructor.ANALYZE_EXPRESSION_PARSERS)) {
      kind = undefined;
      innerExpression =
        this._constructor.ANALYZE_EXPRESSION_PARSERS[this._prev!.text.toUpperCase()](this);
    } else {
      kind = undefined;
      thisExpr = this.parseTableParts();
    }

    const partition = this.tryParse(this.parsePartition.bind(this));
    if (!partition && this._matchTexts(this._constructor.PARTITION_KEYWORDS)) {
      return this.parseAsCommand(start);
    }

    let mode: string | undefined;
    if (
      this._matchTextSeq([
        'WITH',
        'SYNC',
        'MODE',
      ])
      || this._matchTextSeq([
        'WITH',
        'ASYNC',
        'MODE',
      ])
    ) {
      mode = `WITH ${this._tokens[this._index - 2].text.toUpperCase()} MODE`;
    } else {
      mode = undefined;
    }

    if (this._matchTexts(this._constructor.ANALYZE_EXPRESSION_PARSERS)) {
      innerExpression =
        this._constructor.ANALYZE_EXPRESSION_PARSERS[this._prev!.text.toUpperCase()](this);
    }

    const properties = this.parseProperties();
    return this.expression(AnalyzeExpr, {
      kind,
      this: thisExpr,
      mode,
      partition,
      properties,
      expression: innerExpression,
      options,
    });
  }

  parseAnalyzeStatistics (): AnalyzeStatisticsExpr {
    let thisValue: string | undefined;
    const kind = this._prev!.text.toUpperCase();
    const option = this._matchTextSeq('DELTA') ? this._prev!.text.toUpperCase() : undefined;
    let expressions: Expression[] = [];

    if (!this._matchTextSeq('STATISTICS')) {
      this.raiseError('Expecting token STATISTICS');
    }

    if (this._matchTextSeq('NOSCAN')) {
      thisValue = 'NOSCAN';
    } else if (this._match(TokenType.FOR)) {
      if (this._matchTextSeq(['ALL', 'COLUMNS'])) {
        thisValue = 'FOR ALL COLUMNS';
      }
      if (this._matchTexts('COLUMNS')) {
        thisValue = 'FOR COLUMNS';
        expressions = this.parseCsv(this.parseColumnReference.bind(this));
      }
    } else if (this._matchTextSeq('SAMPLE')) {
      const sample = this.parseNumber();
      expressions = [
        this.expression(AnalyzeSampleExpr, {
          sample,
          kind: this._match(TokenType.PERCENT) ? this._prev!.text.toUpperCase() : undefined,
        }),
      ];
    }

    return this.expression(AnalyzeStatisticsExpr, {
      kind,
      option,
      this: thisValue,
      expressions,
    });
  }

  parseAnalyzeValidate (): AnalyzeValidateExpr {
    let kind: string | undefined;
    let thisValue: string | undefined;
    let expression: Expression | undefined;

    if (this._matchTextSeq(['REF', 'UPDATE'])) {
      kind = 'REF';
      thisValue = 'UPDATE';
      if (this._matchTextSeq([
        'SET',
        'DANGLING',
        'TO',
        'NULL',
      ])) {
        thisValue = 'UPDATE SET DANGLING TO NULL';
      }
    } else if (this._matchTextSeq('STRUCTURE')) {
      kind = 'STRUCTURE';
      if (this._matchTextSeq(['CASCADE', 'FAST'])) {
        thisValue = 'CASCADE FAST';
      } else if (
        this._matchTextSeq(['CASCADE', 'COMPLETE'])
        && this._matchTexts(['ONLINE', 'OFFLINE'])
      ) {
        thisValue = `CASCADE COMPLETE ${this._prev!.text.toUpperCase()}`;
        expression = this.parseInto();
      }
    }

    return this.expression(AnalyzeValidateExpr, {
      kind,
      this: thisValue,
      expression,
    });
  }

  parseAnalyzeColumns (): AnalyzeColumnsExpr | undefined {
    const thisValue = this._prev!.text.toUpperCase();
    if (this._matchTextSeq('COLUMNS')) {
      return this.expression(AnalyzeColumnsExpr, {
        this: `${thisValue} ${this._prev!.text.toUpperCase()}`,
      });
    }
    return undefined;
  }

  parseAnalyzeDelete (): AnalyzeDeleteExpr | undefined {
    const kind = this._matchTextSeq('SYSTEM') ? this._prev!.text.toUpperCase() : undefined;
    if (this._matchTextSeq('STATISTICS')) {
      return this.expression(AnalyzeDeleteExpr, { kind });
    }
    return undefined;
  }

  parseAnalyzeList (): AnalyzeListChainedRowsExpr | undefined {
    if (this._matchTextSeq(['CHAINED', 'ROWS'])) {
      return this.expression(AnalyzeListChainedRowsExpr, { expression: this.parseInto() });
    }
    return undefined;
  }

  parseAnalyzeHistogram (): AnalyzeHistogramExpr {
    const thisValue = this._prev!.text.toUpperCase();
    let expression: Expression | undefined;
    let expressions: Expression[] = [];
    let updateOptions: string | undefined;

    if (this._matchTextSeq(['HISTOGRAM', 'ON'])) {
      expressions = this.parseCsv(this.parseColumnReference.bind(this));
      const withExpressions: string[] = [];
      while (this._match(TokenType.WITH)) {
        if (this._matchTexts(['SYNC', 'ASYNC'])) {
          if (this._matchTextSeq('MODE', { advance: false })) {
            withExpressions.push(`${this._prev!.text.toUpperCase()} MODE`);
            this._advance();
          }
        } else {
          const buckets = this.parseNumber();
          if (this._matchTextSeq('BUCKETS')) {
            withExpressions.push(`${buckets} BUCKETS`);
          }
        }
      }
      if (0 < withExpressions.length) {
        expression = this.expression(AnalyzeWithExpr, { expressions: withExpressions });
      }

      if (this._matchTexts(['MANUAL', 'AUTO']) && this._match(TokenType.UPDATE, { advance: false })) {
        updateOptions = this._prev!.text.toUpperCase();
        this._advance();
      } else if (this._matchTextSeq(['USING', 'DATA'])) {
        expression = this.expression(UsingDataExpr, { this: this.parseString() });
      }
    }

    return this.expression(AnalyzeHistogramExpr, {
      this: thisValue,
      expressions,
      expression,
      updateOptions,
    });
  }

  parseMerge (): MergeExpr {
    this._match(TokenType.INTO);
    const target = this.parseTable();

    if (target && this._match(TokenType.ALIAS, { advance: false })) {
      target.setArgKey('alias', this.parseTableAlias());
    }

    this._match(TokenType.USING);
    const using = this.parseTable();

    return this.expression(MergeExpr, {
      this: target,
      using,
      on: this._match(TokenType.ON) && this.parseDisjunction(),
      usingCond: this._match(TokenType.USING) && this.parseUsingIdentifiers(),
      whens: this.parseWhenMatched(),
      returning: this.parseReturning(),
    });
  }

  parseWhenMatched (): WhensExpr {
    const whens: WhenExpr[] = [];

    while (this._match(TokenType.WHEN)) {
      const matched = !this._match(TokenType.NOT);
      this._matchTextSeq('MATCHED');
      const source = this._matchTextSeq(['BY', 'TARGET'])
        ? false
        : this._matchTextSeq(['BY', 'SOURCE']);
      const condition = this._match(TokenType.AND) ? this.parseDisjunction() : undefined;

      this._match(TokenType.THEN);

      let then: Expression | undefined;

      if (this._match(TokenType.INSERT)) {
        const thisValue = this.parseStar();
        if (thisValue) {
          then = this.expression(InsertExpr, { this: thisValue });
        } else {
          then = this.expression(InsertExpr, {
            this: this._matchTextSeq('ROW')
              ? var_('ROW')
              : this.parseValue({ values: false }),
            expression: this._matchTextSeq('VALUES') && this.parseValue(),
          });
        }
      } else if (this._match(TokenType.UPDATE)) {
        const expressions = this.parseStar();
        if (expressions) {
          then = this.expression(UpdateExpr, { expressions });
        } else {
          then = this.expression(UpdateExpr, {
            expressions: this._match(TokenType.SET) && this.parseCsv(this.parseEquality.bind(this)),
          });
        }
      } else if (this._match(TokenType.DELETE)) {
        then = this.expression(VarExpr, { this: this._prev!.text });
      } else {
        then = this.parseVarFromOptions(this._constructor.CONFLICT_ACTIONS);
      }

      whens.push(
        this.expression(WhenExpr, {
          matched,
          source,
          condition,
          then,
        }),
      );
    }
    return this.expression(WhensExpr, { expressions: whens });
  }

  parseShow (): Expression | undefined {
    const parser = this.findParser(
      this._constructor.SHOW_PARSERS,
      this._constructor.SHOW_TRIE,
    );
    if (parser) {
      return parser(this);
    }
    return this.parseAsCommand(this._prev);
  }

  parseSetItemAssignment (options?: { kind?: string }): Expression | undefined {
    const kind = options?.kind;
    const index = this._index;

    if (
      (kind === 'GLOBAL' || kind === 'SESSION')
      && this._matchTextSeq('TRANSACTION')
    ) {
      return this.parseSetTransaction({ global_: kind === 'GLOBAL' });
    }

    const left = this.parsePrimary() || this.parseColumn();
    const assignmentDelimiter = this._matchTexts(
      this._constructor.SET_ASSIGNMENT_DELIMITERS,
    );

    if (
      !left
      || (this._constructor.SET_REQUIRES_ASSIGNMENT_DELIMITER && !assignmentDelimiter)
    ) {
      this._retreat(index);
      return undefined;
    }

    let right = this.parseStatement() || this.parseIdVar();
    if (right instanceof ColumnExpr || right instanceof IdentifierExpr) {
      right = var_(right.name);
    }

    const thisValue = this.expression(EQExpr, {
      this: left,
      expression: right,
    });
    return this.expression(SetItemExpr, {
      this: thisValue,
      kind,
    });
  }

  parseSetTransaction (options?: { global_?: boolean }): Expression {
    const global_ = options?.global_ ?? false;
    this._matchTextSeq('TRANSACTION');
    const characteristics = this.parseCsv(() =>
      this.parseVarFromOptions(this._constructor.TRANSACTION_CHARACTERISTICS));
    return this.expression(SetItemExpr, {
      expressions: characteristics,
      kind: 'TRANSACTION',
      global_,
    });
  }

  parseSetItem (): Expression | undefined {
    const parser = this.findParser(
      this._constructor.SET_PARSERS,
      this._constructor.SET_TRIE,
    );
    return parser ? parser(this) : this.parseSetItemAssignment({ kind: undefined });
  }

  parseSet (options?: { unset?: boolean;
    tag?: boolean; }): SetExpr | CommandExpr {
    const unset = options?.unset ?? false;
    const tag = options?.tag ?? false;
    const index = this._index;
    const set = this.expression(SetExpr, {
      expressions: this.parseCsv(this.parseSetItem.bind(this)),
      unset,
      tag,
    });

    if (this._curr) {
      this._retreat(index);
      return this.parseAsCommand(this._prev);
    }

    return set;
  }

  parseVarFromOptions (
    options: Record<string, (string | string[])[] | null>,
    parseOptions: { raiseUnmatched?: boolean } = {},
  ): VarExpr | undefined {
    const { raiseUnmatched = true } = parseOptions;
    const start = this._curr;
    if (!start) {
      return undefined;
    }

    let option = start.text.toUpperCase();
    const continuations = options[option];

    const index = this._index;
    this._advance();
    let matched = false;
    for (const keywords of continuations || []) {
      const keywordArray = typeof keywords === 'string' ? [keywords] : keywords;

      if (this._matchTextSeq(...keywordArray)) {
        option = `${option} ${keywordArray.join(' ')}`;
        matched = true;
        break;
      }
    }

    if (!matched && (continuations || continuations === null)) {
      if (raiseUnmatched) {
        this.raiseError(`Unknown option ${option}`);
      }

      this._retreat(index);
      return undefined;
    }

    return var_(option);
  }

  parseCache (): CacheExpr {
    const lazy = this._matchTextSeq('LAZY');
    this._match(TokenType.TABLE);
    const table = this.parseTable({ schema: true });

    let options: Expression[] = [];
    if (this._matchTextSeq('OPTIONS')) {
      this._matchLParen();
      const k = this.parseString();
      this._match(TokenType.EQ);
      const v = this.parseString();
      options = [k, v];
      this._matchRParen();
    }

    this._match(TokenType.ALIAS);
    return this.expression(CacheExpr, {
      this: table,
      lazy,
      options,
      expression: this.parseSelect({ nested: true }),
    });
  }

  parsePartition (): PartitionExpr | undefined {
    if (!this._matchTexts(this._constructor.PARTITION_KEYWORDS)) {
      return undefined;
    }

    return this.expression(PartitionExpr, {
      subpartition: this._prev!.text.toUpperCase() === 'SUBPARTITION',
      expressions: this.parseWrappedCsv(this.parseDisjunction.bind(this)),
    });
  }

  parseValue (options?: { values?: boolean }): TupleExpr | undefined {
    const values = options?.values ?? true;

    const parseValueExpression = (): Expression | undefined => {
      if (
        this._dialectConstructor.SUPPORTS_VALUES_DEFAULT
        && this._match(TokenType.DEFAULT)
      ) {
        return var_(this._prev!.text.toUpperCase());
      }
      return this.parseExpression();
    };

    if (this._match(TokenType.L_PAREN)) {
      const expressions = this.parseCsv(parseValueExpression);
      this._matchRParen();
      return this.expression(TupleExpr, { expressions });
    }

    const expression = this.parseExpression();
    if (expression) {
      return this.expression(TupleExpr, { expressions: [expression] });
    }
    return undefined;
  }

  parseProjections (): Expression[] {
    return this.parseExpressions();
  }

  parseWrappedSelect (options?: { table?: boolean }): Expression | undefined {
    const table = options?.table ?? false;
    let thisExpr: Expression | undefined;

    if (this._matchSet([TokenType.PIVOT, TokenType.UNPIVOT])) {
      thisExpr = this.parseSimplifiedPivot({
        isUnpivot: this._prev!.tokenType === TokenType.UNPIVOT,
      });
    } else if (this._match(TokenType.FROM)) {
      const from = this.parseFrom({
        skipFromToken: true,
        consumePipe: true,
      });
      const select = this.parseSelect({ from });
      if (select) {
        if (!select.args.from) {
          select.setArgKey('from', from);
        }
        thisExpr = select;
      } else {
        thisExpr = select('*').from(from as FromExpr);
        thisExpr = this.parseQueryModifiers(this.parseSetOperations(thisExpr));
      }
    } else {
      thisExpr = table
        ? this.parseTable({ consumePipe: true })
        : this.parseSelect({
          nested: true,
          parseSetOperation: false,
        });

      if (table && thisExpr instanceof ValuesExpr && thisExpr.alias) {
        const alias = thisExpr.args.alias?.pop();
        thisExpr = new TableExpr({
          this: thisExpr,
          alias,
        });
      }

      thisExpr = this.parseQueryModifiers(this.parseSetOperations(thisExpr));
    }

    return thisExpr;
  }

  parseCommitOrRollback (): CommitExpr | RollbackExpr {
    let chain: boolean | undefined;
    let savepoint: Expression | undefined;
    const isRollback = this._prev!.tokenType === TokenType.ROLLBACK;

    this._matchTexts(['TRANSACTION', 'WORK']);

    if (this._matchTextSeq('TO')) {
      this._matchTextSeq('SAVEPOINT');
      savepoint = this.parseIdVar();
    }

    if (this._match(TokenType.AND)) {
      chain = !this._matchTextSeq('NO');
      this._matchTextSeq('CHAIN');
    }

    if (isRollback) {
      return this.expression(RollbackExpr, { savepoint });
    }

    return this.expression(CommitExpr, { chain });
  }

  parseRefresh (): RefreshExpr | CommandExpr {
    let kind: string;

    if (this._match(TokenType.TABLE)) {
      kind = 'TABLE';
    } else if (this._matchTextSeq(['MATERIALIZED', 'VIEW'])) {
      kind = 'MATERIALIZED VIEW';
    } else {
      kind = '';
    }

    const thisValue = this.parseString() || this.parseTable();
    if (!kind && !(thisValue instanceof LiteralExpr)) {
      return this.parseAsCommand(this._prev);
    }

    return this.expression(RefreshExpr, {
      this: thisValue,
      kind,
    });
  }

  parseColumnDefWithExists (): ColumnDefExpr | undefined {
    const start = this._index;
    this._match(TokenType.COLUMN);

    const existsColumn = this.parseExists({ not: true });
    const expression = this.parseFieldDef();

    if (!(expression instanceof ColumnDefExpr)) {
      this._retreat(start);
      return undefined;
    }

    expression.setArgKey('exists', existsColumn);

    return expression;
  }

  parseAddColumn (): ColumnDefExpr | undefined {
    if (this._prev!.text.toUpperCase() !== 'ADD') {
      return undefined;
    }

    const expression = this.parseColumnDefWithExists();
    if (!expression) {
      return undefined;
    }

    if (this._matchTexts(['FIRST', 'AFTER'])) {
      const position = this._prev!.text;
      const columnPosition = this.expression(ColumnPositionExpr, {
        this: this.parseColumn(),
        position,
      });
      expression.setArgKey('position', columnPosition);
    }

    return expression;
  }

  parseDropColumn (): DropExpr | CommandExpr | undefined {
    const drop = this._match(TokenType.DROP) && this.parseDrop();
    if (drop && !(drop instanceof CommandExpr)) {
      drop.setArgKey('kind', drop.args.kind || 'COLUMN');
    }
    return drop;
  }

  parseDropPartition (options?: { exists?: boolean }): DropPartitionExpr {
    const exists = options?.exists;
    return this.expression(DropPartitionExpr, {
      expressions: this.parseCsv(this.parsePartition.bind(this)),
      exists,
    });
  }

  parseAlterTableAdd (): Expression[] {
    const parseAddAlteration = (): Expression | undefined => {
      this._matchTextSeq('ADD');
      if (this._matchSet(this._constructor.ADD_CONSTRAINT_TOKENS, { advance: false })) {
        return this.expression(AddConstraintExpr, {
          expressions: this.parseCsv(this.parseConstraint.bind(this)),
        });
      }

      const columnDef = this.parseAddColumn();
      if (columnDef instanceof ColumnDefExpr) {
        return columnDef;
      }

      const exists = this.parseExists({ not: true });
      if (this._matchPair(TokenType.PARTITION, TokenType.L_PAREN, { advance: false })) {
        return this.expression(AddPartitionExpr, {
          exists,
          this: this.parseField({ anyToken: true }),
          location:
            this._matchTextSeq('LOCATION', { advance: false }) && this.parseProperty(),
        });
      }

      return undefined;
    };

    if (
      !this._matchSet(this._constructor.ADD_CONSTRAINT_TOKENS, { advance: false })
      && (!this._dialectConstructor.ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN
        || this._matchTextSeq('COLUMNS'))
    ) {
      const schema = this.parseSchema();

      return schema
        ? ensureList(schema)
        : this.parseCsv(this.parseColumnDefWithExists.bind(this));
    }

    return this.parseCsv(parseAddAlteration);
  }

  parseAlterTableAlter (): Expression | undefined {
    if (this._matchTexts(this._constructor.ALTER_ALTER_PARSERS)) {
      return this._constructor.ALTER_ALTER_PARSERS[this._prev!.text.toUpperCase()](this);
    }

    this._match(TokenType.COLUMN);
    const column = this.parseField({ anyToken: true });

    if (this._matchPair(TokenType.DROP, TokenType.DEFAULT)) {
      return this.expression(AlterColumnExpr, {
        this: column,
        drop: true,
      });
    }
    if (this._matchPair(TokenType.SET, TokenType.DEFAULT)) {
      return this.expression(AlterColumnExpr, {
        this: column,
        default: this.parseDisjunction(),
      });
    }
    if (this._match(TokenType.COMMENT)) {
      return this.expression(AlterColumnExpr, {
        this: column,
        comment: this.parseString(),
      });
    }
    if (this._matchTextSeq([
      'DROP',
      'NOT',
      'NULL',
    ])) {
      return this.expression(AlterColumnExpr, {
        this: column,
        drop: true,
        allowNull: true,
      });
    }
    if (this._matchTextSeq([
      'SET',
      'NOT',
      'NULL',
    ])) {
      return this.expression(AlterColumnExpr, {
        this: column,
        allowNull: false,
      });
    }

    if (this._matchTextSeq(['SET', 'VISIBLE'])) {
      return this.expression(AlterColumnExpr, {
        this: column,
        visible: 'VISIBLE',
      });
    }
    if (this._matchTextSeq(['SET', 'INVISIBLE'])) {
      return this.expression(AlterColumnExpr, {
        this: column,
        visible: 'INVISIBLE',
      });
    }

    this._matchTextSeq(['SET', 'DATA']);
    this._matchTextSeq('TYPE');
    return this.expression(AlterColumnExpr, {
      this: column,
      dtype: this.parseTypes(),
      collate: this._match(TokenType.COLLATE) && this.parseTerm(),
      using: this._match(TokenType.USING) && this.parseDisjunction(),
    });
  }

  parseAlterDiststyle (): AlterDistStyleExpr {
    if (this._matchTexts([
      'ALL',
      'EVEN',
      'AUTO',
    ])) {
      return this.expression(AlterDistStyleExpr, { this: var_(this._prev!.text.toUpperCase()) });
    }

    this._matchTextSeq(['KEY', 'DISTKEY']);
    return this.expression(AlterDistStyleExpr, { this: this.parseColumn() });
  }

  parseAlterSortkey (options?: { compound?: boolean }): AlterSortKeyExpr {
    const compound = options?.compound;

    if (compound) {
      this._matchTextSeq('SORTKEY');
    }

    if (this._match(TokenType.L_PAREN, { advance: false })) {
      return this.expression(AlterSortKeyExpr, {
        expressions: this.parseWrappedIdVars(),
        compound,
      });
    }

    this._matchTexts(['AUTO', 'NONE']);
    return this.expression(AlterSortKeyExpr, {
      this: var_(this._prev!.text.toUpperCase()),
      compound,
    });
  }

  parseAlterTableDrop (): Expression[] {
    const index = this._index - 1;

    const partitionExists = this.parseExists();
    if (this._match(TokenType.PARTITION, { advance: false })) {
      return this.parseCsv(() => this.parseDropPartition({ exists: partitionExists }));
    }

    this._retreat(index);
    return this.parseCsv(this.parseDropColumn.bind(this));
  }

  parseAlterTableRename (): AlterRenameExpr | RenameColumnExpr | undefined {
    if (this._match(TokenType.COLUMN) || !this._constructor.ALTER_RENAME_REQUIRES_COLUMN) {
      const exists = this.parseExists();
      const oldColumn = this.parseColumn();
      const to = this._matchTextSeq('TO');
      const newColumn = this.parseColumn();

      if (!oldColumn || !to || !newColumn) {
        return undefined;
      }

      return this.expression(RenameColumnExpr, {
        this: oldColumn,
        to: newColumn,
        exists,
      });
    }

    this._matchTextSeq('TO');
    return this.expression(AlterRenameExpr, { this: this.parseTable({ schema: true }) });
  }

  parseLoad (): LoadDataExpr | CommandExpr {
    if (this._matchTextSeq('DATA')) {
      const local = this._matchTextSeq('LOCAL');
      this._matchTextSeq('INPATH');
      const inpath = this.parseString();
      const overwrite = this._match(TokenType.OVERWRITE);
      this._matchPair(TokenType.INTO, TokenType.TABLE);

      return this.expression(LoadDataExpr, {
        this: this.parseTable({ schema: true }),
        local,
        overwrite,
        inpath,
        partition: this.parsePartition(),
        inputFormat: this._matchTextSeq('INPUTFORMAT') && this.parseString(),
        serde: this._matchTextSeq('SERDE') && this.parseString(),
      });
    }
    return this.parseAsCommand(this._prev);
  }

  parseDelete (): DeleteExpr {
    let tables: Expression[] | undefined;
    if (!this._match(TokenType.FROM, { advance: false })) {
      tables = this.parseCsv(this.parseTable.bind(this)) || undefined;
    }

    const returning = this.parseReturning();

    return this.expression(DeleteExpr, {
      tables,
      this: this._match(TokenType.FROM) && this.parseTable({ joins: true }),
      using:
        this._match(TokenType.USING)
        && this.parseCsv(() => this.parseTable({ joins: true })),
      cluster: this._match(TokenType.ON) && this.parseOnProperty(),
      where: this.parseWhere(),
      returning: returning || this.parseReturning(),
      order: this.parseOrder(),
      limit: this.parseLimit(),
    });
  }

  parseUpdate (): UpdateExpr {
    const kwargs: Record<string, unknown> = {
      this: this.parseTable({
        joins: true,
        aliasTokens: this._constructor.UPDATE_ALIAS_TOKENS,
      }),
    };

    while (this._curr) {
      if (this._match(TokenType.SET)) {
        kwargs.expressions = this.parseCsv(this.parseEquality.bind(this));
      } else if (this._match(TokenType.RETURNING, { advance: false })) {
        kwargs.returning = this.parseReturning();
      } else if (this._match(TokenType.FROM, { advance: false })) {
        const from = this.parseFrom({ joins: true });
        const table = from?.this;
        if (table instanceof SubqueryExpr && this._match(TokenType.JOIN, { advance: false })) {
          const joins = this.parseJoins();
          table.setArgKey('joins', 0 < joins.length ? joins : undefined);
        }

        kwargs.from = from;
      } else if (this._match(TokenType.WHERE, { advance: false })) {
        kwargs.where = this.parseWhere();
      } else if (this._match(TokenType.ORDER_BY, { advance: false })) {
        kwargs.order = this.parseOrder();
      } else if (this._match(TokenType.LIMIT, { advance: false })) {
        kwargs.limit = this.parseLimit();
      } else {
        break;
      }
    }

    return this.expression(UpdateExpr, kwargs);
  }

  parseUse (): UseExpr {
    return this.expression(UseExpr, {
      kind: this.parseVarFromOptions(this._constructor.USABLES, false),
      this: this.parseTable({ schema: false }),
    });
  }

  parseUncache (): UncacheExpr {
    if (!this._match(TokenType.TABLE)) {
      this.raiseError('Expecting TABLE after UNCACHE');
    }

    return this.expression(UncacheExpr, {
      exists: this.parseExists(),
      this: this.parseTable({ schema: true }),
    });
  }

  parseAsCommand (start?: Token): CommandExpr {
    while (this._curr) {
      this._advance();
    }
    const text = this.findSql(start, this._prev);
    const size = start?.text.length || 0;
    this.warnUnsupported();
    return new CommandExpr({
      this: text.substring(0, size),
      expression: text.substring(size),
    });
  }

  parseDictProperty (options: { this: string }): DictPropertyExpr {
    const settings: DictSubPropertyExpr[] = [];

    this._matchLParen();
    const kind = this.parseIdVar();

    if (this._match(TokenType.L_PAREN)) {
      while (true) {
        const key = this.parseIdVar();
        const value = this.parsePrimary();
        if (!key && !value) {
          break;
        }
        settings.push(this.expression(DictSubPropertyExpr, {
          this: key,
          value,
        }));
      }
      this._match(TokenType.R_PAREN);
    }

    this._matchRParen();

    return this.expression(DictPropertyExpr, {
      this: options.this,
      kind: kind?.this,
      settings,
    });
  }

  parseDictRange (options: { this: string }): DictRangeExpr {
    this._matchLParen();
    const hasMin = this._matchTextSeq('MIN');
    let min: Expression;
    let max: Expression;

    if (hasMin) {
      min = this.parseVar() || this.parsePrimary();
      this._matchTextSeq('MAX');
      max = this.parseVar() || this.parsePrimary();
    } else {
      max = this.parseVar() || this.parsePrimary();
      min = LiteralExpr.number(0);
    }
    this._matchRParen();
    return this.expression(DictRangeExpr, {
      this: options.this,
      min,
      max,
    });
  }

  parseComprehension (thisValue?: Expression): ComprehensionExpr | undefined {
    const index = this._index;
    const expression = this.parseColumn();
    const position = this._match(TokenType.COMMA) && this.parseColumn();

    if (!this._match(TokenType.IN)) {
      this._retreat(index - 1);
      return undefined;
    }
    const iterator = this.parseColumn();
    const condition = this._matchTextSeq('IF') ? this.parseDisjunction() : undefined;
    return this.expression(ComprehensionExpr, {
      this: thisValue,
      expression,
      position,
      iterator,
      condition,
    });
  }

  parseHeredoc (): HeredocExpr | undefined {
    if (this._match(TokenType.HEREDOC_STRING)) {
      return this.expression(HeredocExpr, { this: this._prev!.text });
    }

    if (!this._matchTextSeq('$')) {
      return undefined;
    }

    const tags = ['$'];
    let tagText: string | undefined;

    if (this.isConnected()) {
      this._advance();
      tags.push(this._prev!.text.toUpperCase());
    } else {
      this.raiseError('No closing $ found');
    }

    if (tags[tags.length - 1] !== '$') {
      if (this.isConnected() && this._matchTextSeq('$')) {
        tagText = tags[tags.length - 1];
        tags.push('$');
      } else {
        this.raiseError('No closing $ found');
      }
    }

    const heredocStart = this._curr;

    while (this._curr) {
      if (this._matchTextSeq(...tags, { advance: false })) {
        const thisValue = this.findSql(heredocStart, this._prev);
        this._advance(tags.length);
        return this.expression(HeredocExpr, {
          this: thisValue,
          tag: tagText,
        });
      }

      this._advance();
    }

    this.raiseError(`No closing ${tags.join('')} found`);
    return undefined;
  }

  findParser (
    parsers: Record<string, Function>,
    trie: Record<string, unknown>,
  ): Function | undefined {
    if (!this._curr) {
      return undefined;
    }

    const index = this._index;
    const thisPath: string[] = [];
    while (true) {
      const curr = this._curr.text.toUpperCase();
      const key = curr.split(' ');
      thisPath.push(curr);

      this._advance();
      const [result, newTrie] = inTrie(trie, key);
      trie = newTrie;
      if (result === TrieResult.FAILED) {
        break;
      }

      if (result === TrieResult.EXISTS) {
        const subparser = parsers[thisPath.join(' ')];
        return subparser;
      }
    }

    this._retreat(index);
    return undefined;
  }

  parseGroupConcat (): Expression | undefined {
    const concatExprs = (
      node: Expression | undefined,
      exprs: Expression[],
    ): Expression => {
      if (node instanceof DistinctExpr && 1 < node.expressions.length) {
        const concatExpressions = [
          this.expression(ConcatExpr, {
            expressions: node.expressions,
            safe: true,
            coalesce: this._dialectConstructor.CONCAT_COALESCE,
          }),
        ];
        node.setArgKey('expressions', concatExpressions);
        return node;
      }
      if (exprs.length === 1) {
        return exprs[0];
      }
      return this.expression(ConcatExpr, {
        expressions: exprs,
        safe: true,
        coalesce: this._dialectConstructor.CONCAT_COALESCE,
      });
    };

    const args = this.parseCsv(this.parseLambda.bind(this));

    let thisValue: Expression | undefined;
    if (args) {
      const lastArg = args[args.length - 1];
      const order: OrderExpr | undefined =
        lastArg instanceof OrderExpr ? lastArg : undefined;

      if (order) {
        args[args.length - 1] = order.this;
        order.setArgKey('this', concatExprs(order.this, args));
      }

      thisValue = order || concatExprs(args[0], args);
    } else {
      thisValue = undefined;
    }

    const separator = this._match(TokenType.SEPARATOR) ? this.parseField() : undefined;

    return this.expression(GroupConcatExpr, {
      this: thisValue,
      separator,
    });
  }

  parseInitcap (): InitcapExpr {
    const expr = InitcapExpr.fromArgList(this.parseFunctionArgs());

    if (!expr.args.expression) {
      expr.setArgKey(
        'expression',
        LiteralExpr.string(this._dialectConstructor.INITCAP_DEFAULT_DELIMITER_CHARS),
      );
    }

    return expr;
  }

  parseOperator (thisValue?: Expression): Expression | undefined {
    let result = thisValue;

    while (true) {
      if (!this._match(TokenType.L_PAREN)) {
        break;
      }

      let op = '';
      while (this._curr && !this._match(TokenType.R_PAREN)) {
        op += this._curr.text;
        this._advance();
      }

      result = this.expression(OperatorExpr, {
        comments: this._prevComments,
        this: result,
        operator: op,
        expression: this.parseBitwise(),
      });

      if (!this._match(TokenType.OPERATOR)) {
        break;
      }
    }

    return result;
  }

  buildPipeCte (options: {
    query: QueryExpr;
    expressions: Expression[];
    aliasCte?: TableAliasExpr;
  }): SelectExpr {
    const {
      query, expressions, aliasCte,
    } = options;
    let newCte: string | TableAliasExpr;
    if (aliasCte) {
      newCte = aliasCte;
    } else {
      this._pipeCteCounter += 1;
      newCte = `__tmp${this._pipeCteCounter}`;
    }

    const with_ = query.args.with;
    const ctes = with_?.pop();

    const newSelect = select(...expressions, { copy: false }).from(newCte, { copy: false });
    if (ctes) {
      newSelect.setArgKey('with', ctes);
    }

    return newSelect.with(newCte, {
      as_: query,
      copy: false,
    });
  }

  parsePipeSyntaxSelect (query: SelectExpr): SelectExpr {
    const select = this.parseSelect({ consumePipe: false });
    if (!select) {
      return query;
    }

    return this.buildPipeCte({
      query: query.select(select.expressions, { append: false }),
      expressions: [new StarExpr({})],
    });
  }

  parsePipeSyntaxLimit (query: SelectExpr): SelectExpr {
    const limit = this.parseLimit();
    const offset = this.parseOffset();
    if (limit) {
      const currLimit = query.args.limit || limit;
      if (limit.expression.toValue() <= currLimit.expression.toValue()) {
        query.limit(limit, { copy: false });
      }
    }
    if (offset) {
      const currOffset = query.args.offset;
      const currOffsetVal = currOffset ? currOffset.expression.toValue() : 0;
      query.offset(LiteralExpr.number(currOffsetVal + offset.expression.toValue()), { copy: false });
    }

    return query;
  }

  parsePipeSyntaxAggregateFields (): Expression | undefined {
    let thisValue = this.parseDisjunction();
    if (this._matchTextSeq('GROUP', 'AND', { advance: false })) {
      return thisValue;
    }

    thisValue = this.parseAlias(thisValue);

    if (this._matchSet([TokenType.ASC, TokenType.DESC], { advance: false })) {
      return this.parseOrdered(() => thisValue);
    }

    return thisValue;
  }

  parsePipeSyntaxAggregateGroupOrderBy (
    query: SelectExpr,
    options: { groupByExists?: boolean } = {},
  ): SelectExpr {
    const { groupByExists = true } = options;
    const expr = this.parseCsv(this.parsePipeSyntaxAggregateFields.bind(this));
    const aggregatesOrGroups: Expression[] = [];
    const orders: OrderedExpr[] = [];

    for (const element of expr) {
      if (element instanceof OrderedExpr) {
        const thisValue = element.this;
        if (thisValue instanceof AliasExpr) {
          element.setArgKey('this', thisValue.args.alias);
        }
        orders.push(element);
      } else {
        thisValue = element;
      }
      aggregatesOrGroups.push(thisValue);
    }

    if (groupByExists) {
      query
        .select(aggregatesOrGroups, { copy: false })
        .groupBy(
          ...aggregatesOrGroups.map((proj) => proj.args.alias || proj),
          { copy: false },
        );
    } else {
      query.select(aggregatesOrGroups, {
        append: false,
        copy: false,
      });
    }

    if (0 < orders.length) {
      return query.orderBy(...orders, {
        append: false,
        copy: false,
      });
    }

    return query;
  }

  parsePipeSyntaxAggregate (query: SelectExpr): SelectExpr {
    this._matchTextSeq('AGGREGATE');
    query = this.parsePipeSyntaxAggregateGroupOrderBy(query, { groupByExists: false });

    if (
      this._match(TokenType.GROUP_BY)
      || (this._matchTextSeq(['GROUP', 'AND']) && this._match(TokenType.ORDER_BY))
    ) {
      query = this.parsePipeSyntaxAggregateGroupOrderBy(query);
    }

    return this.buildPipeCte({
      query,
      expressions: [new StarExpr({})],
    });
  }

  parsePipeSyntaxSetOperator (query: QueryExpr): QueryExpr | undefined {
    const firstSetop = this.parseSetOperation({ this: query });
    if (!firstSetop) {
      return undefined;
    }

    const parseAndUnwrapQuery = (): SelectExpr | undefined => {
      const expr = this.parseParen();
      return expr ? expr.assertIs(SubqueryExpr).unnest() : undefined;
    };

    firstSetop.this.pop();

    const setops = [
      firstSetop.expression.pop().assertIs(SubqueryExpr)
        .unnest(),
      ...this.parseCsv(parseAndUnwrapQuery),
    ];

    query = this.buildPipeCte({
      query,
      expressions: [new StarExpr({})],
    });
    const with_ = query.args.with;
    const ctes = with_?.pop();

    if (firstSetop instanceof UnionExpr) {
      query = query.union(...setops, {
        copy: false,
        ...firstSetop.args,
      });
    } else if (firstSetop instanceof ExceptExpr) {
      query = query.except(...setops, {
        copy: false,
        ...firstSetop.args,
      });
    } else {
      query = query.intersect(...setops, {
        copy: false,
        ...firstSetop.args,
      });
    }

    query.setArgKey('with_', ctes);

    return this.buildPipeCte({
      query,
      expressions: [new StarExpr()],
    });
  }

  parsePipeSyntaxJoin (query: QueryExpr): QueryExpr | undefined {
    const join = this.parseJoin();
    if (!join) {
      return undefined;
    }

    if (query instanceof SelectExpr) {
      return query.join(join, { copy: false });
    }

    return query;
  }

  parsePipeSyntaxPivot (query: SelectExpr): SelectExpr {
    const pivots = this.parsePivots();
    if (!pivots) {
      return query;
    }

    const from = query.args.from;
    if (from) {
      from.this.setArgKey('pivots', pivots);
    }

    return this.buildPipeCte({
      query,
      expressions: [new StarExpr()],
    });
  }

  parsePipeSyntaxExtend (query: SelectExpr): SelectExpr {
    this._matchTextSeq('EXTEND');
    query.select(new StarExpr({}), ...this.parseExpressions(), {
      append: false,
      copy: false,
    });
    return this.buildPipeCte({
      query,
      expressions: [new StarExpr({})],
    });
  }

  parsePipeSyntaxTablesample (query: SelectExpr): SelectExpr {
    const sample = this.parseTableSample();

    const with_ = query.args.with;
    if (with_) {
      with_.expressions[with_.expressions.length - 1].this.setArgKey('sample', sample);
    } else {
      query.setArgKey('sample', sample);
    }

    return query;
  }

  parsePipeSyntaxQuery (query: QueryExpr): QueryExpr | undefined {
    let result = query;

    if (result instanceof SubqueryExpr) {
      result = select('*').from(result, { copy: false });
    }

    if (!result.args.from) {
      result = select('*').from(result.subquery({ copy: false }), { copy: false });
    }

    while (this._match(TokenType.PIPE_GT)) {
      const start = this._curr;
      const parser =
        this._constructor.PIPE_SYNTAX_TRANSFORM_PARSERS[this._curr!.text.toUpperCase()];
      if (!parser) {
        let parsedQuery = this.parsePipeSyntaxSetOperator(result);
        parsedQuery = parsedQuery || this.parsePipeSyntaxJoin(result);
        if (!parsedQuery) {
          this._retreat(start);
          this.raiseError(`Unsupported pipe syntax operator: '${start!.text.toUpperCase()}'.`);
          break;
        }
        result = parsedQuery;
      } else {
        result = parser(this, result);
      }
    }

    return result;
  }

  parseDeclareitem (): DeclareItemExpr | undefined {
    const vars = this.parseCsv(this.parseIdVar.bind(this));
    if (!vars) {
      return undefined;
    }

    return this.expression(DeclareItemExpr, {
      this: vars,
      kind: this.parseTypes(),
      default: this._match(TokenType.DEFAULT) && this.parseBitwise(),
    });
  }

  parseDeclare (): DeclareExpr | CommandExpr {
    const start = this._prev;
    const expressions = this.tryParse(() => this.parseCsv(this.parseDeclareitem.bind(this)));

    if (!expressions || this._curr) {
      return this.parseAsCommand(start);
    }

    return this.expression(DeclareExpr, { expressions });
  }

  buildCast (options: {
    strict: boolean;
    [key: string]: unknown;
  }): CastExpr | TryCastExpr {
    const strict = options.strict;
    const ExpClass = strict ? CastExpr : TryCastExpr;

    const kwargs: Record<string, unknown> = { ...options };
    delete kwargs.strict;

    if (ExpClass === TryCastExpr) {
      kwargs.requiresString = this._dialectConstructor.TRY_CAST_REQUIRES_STRING;
    }

    return this.expression(ExpClass, kwargs);
  }

  parseJsonValue (): JSONValueExpr {
    const thisValue = this.parseBitwise();
    this._match(TokenType.COMMA);
    const path = this.parseBitwise();

    const returning = this._match(TokenType.RETURNING) && this.parseType();

    return this.expression(JSONValueExpr, {
      this: thisValue,
      path: this._dialectConstructor.toJsonPath(path),
      returning,
      onCondition: this.parseOnCondition(),
    });
  }

  validateExpression<E extends Expression> (
    expression: E,
    args?: unknown[],
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

  private get _dialectConstructor (): typeof Dialect {
    return this.dialect.constructor as typeof Dialect;
  }
}
