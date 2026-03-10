// https://github.com/tobymao/sqlglot/blob/main/sqlglot/parser.py

import {
  cache,
  assertIsInstanceOf, filterInstanceOf, isInstanceOf, enumFromString,
} from '../port_internals';
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
  CteExpr,
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
  EqExpr,
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
  GteExpr,
  GtExpr,
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
  GroupConcatExpr,
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
  InOutColumnConstraintExpr,
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
  JsonbContainsAllTopKeysExpr,
  JsonbContainsAnyTopKeysExpr,
  JsonbContainsExpr,
  JsonbDeleteAtPathExpr,
  JsonbExtractExpr,
  JsonbExtractScalarExpr,
  JsonCastExpr,
  JsonColumnDefExpr,
  JsonExpr,
  JsonExtractExpr,
  JsonExtractScalarExpr,
  JsonKeyValueExpr,
  JsonKeysExpr,
  JsonObjectAggExpr,
  JsonObjectExpr,
  JsonSchemaExpr,
  JsonTableExpr,
  JsonValueExpr,
  JoinExpr,
  JoinHintExpr,
  JournalPropertyExpr,
  KillExpr,
  KwargExpr,
  LteExpr,
  LtExpr,
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
  MergeTreeTtlActionExpr,
  MergeTreeTtlExpr,
  ModExpr,
  MulExpr,
  MultitableInsertsExpr,
  NeqExpr,
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
  NullSafeEqExpr,
  NullSafeNeqExpr,
  ObjectIdentifierExpr,
  OffsetExpr,
  OnCommitPropertyExpr,
  OnConditionExpr,
  OnConflictExpr,
  OnPropertyExpr,
  OnUpdateColumnConstraintExpr,
  OpclassExpr,
  OpenJsonColumnDefExpr,
  OpenJsonExpr,
  OperatorExpr,
  OrExpr,
  OrderExpr,
  OrderedExpr,
  OutputModelPropertyExpr,
  OverlapsExpr,
  OverflowTruncateBehaviorExpr,
  PadExpr,
  ParameterExpr,
  ParenExpr,
  ParseJsonExpr,
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
  PropertyEqExpr,
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
  XmlElementExpr,
  XmlNamespaceExpr,
  XmlTableExpr,
  array,
  column,
  INTERVAL_DAY_TIME_RE,
  INTERVAL_STRING_RE,
  select,
  toIdentifier,
  UNWRAPPED_QUERIES,
  var_,
  alias,
  cast,
  maybeParse,
  CreateExprKind,
  JoinExprKind,
  null_,
} from '../expressions';
import type {
  JoinExprArgs, StringExpr,

  ExpressionValue,
} from '../expressions';
import { formatTime } from '../time';
import {
  applyIndexOffset, ensureList, seqGet,
} from '../helper';
import {
  Dialect, type DialectType, NullOrdering,
} from '../dialects/dialect';
import {
  concatMessages,
  ErrorLevel,
  highlightSql,
  mergeErrors,
  ParseError,
} from '../errors';
import {
  Token,
  Tokenizer, TokenType,
} from '../tokens';
import {
  newTrie, type TrieNode, inTrie, TrieResult,
} from '../trie';
import { normalizeIdentifiers } from '../optimizer/normalize_identifiers';
import { FUNCTION_BY_NAME } from './function_registry';

/**
 * Parses the given SQL string into a collection of syntax trees, one per parsed SQL statement.
 *
 * @param sql - The SQL code string to parse
 * @param opts - Parse options including:
 *   - read: the SQL dialect to apply during parsing (eg. "spark", "hive", "presto", "mysql")
 *   - dialect: the SQL dialect (alias for read)
 *   - into: the SQLGlot Expression type to parse into
 *   - other Parser options
 * @returns The resulting syntax tree collection
 */
export function parse<IntoT extends Expression = Expression> (
  sql: string,
  opts?: ParseOptions<IntoT>,
): (IntoT | undefined)[] {
  return Dialect.getOrRaise(opts?.read ?? opts?.dialect).parse(sql, opts) as (IntoT | undefined)[];
}

/**
 * Parses the given SQL string and returns a syntax tree for the first parsed SQL statement.
 *
 * @param sql - The SQL code string to parse
 * @param options - Parse options including:
 *   - read: the SQL dialect to apply during parsing (eg. "spark", "hive", "presto", "mysql")
 *   - dialect: the SQL dialect (alias for read)
 *   - into: the SQLGlot Expression type to parse into
 *   - other Parser options
 * @returns The syntax tree for the first parsed statement
 * @throws ParseError if no expression was parsed
 */
export function parseOne<IntoT extends Expression = Expression> (
  sql: string,
  options?: ParseOptions<IntoT>,
): IntoT {
  const dialect = Dialect.getOrRaise(options?.read ?? options?.dialect);

  const result = options?.into
    ? dialect.parseIntoTypes(options.into, sql, options)
    : dialect.parse(sql, options);

  for (const expression of result) {
    if (!expression) {
      throw new ParseError(`No expression was parsed from '${sql}'`);
    }
    return expression as IntoT;
  }

  throw new ParseError(`No expression was parsed from '${sql}'`);
}

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
    this: args[1],
    expression: args[0],
  });
  return 2 < args.length
    ? new EscapeExpr({
      this: like,
      expression: args[2],
    })
    : like;
}

export function binaryRangeParser (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  exprType: new (args: any) => Expression,
  options: { reverseArgs?: boolean } = {},
): (this: Parser, thisExpr: Expression | undefined) => Expression | undefined {
  const { reverseArgs = false } = options;

  return function parseBinaryRange (
    this: Parser,
    thisExpr: Expression | undefined,
  ): Expression | undefined {
    let expression = this.parseBitwise();
    let thisArg = thisExpr;

    if (reverseArgs) {
      [thisArg, expression] = [expression, thisArg];
    }

    return this.parseEscape(
      this.expression(exprType, {
        this: thisArg,
        expression,
      }),
    );
  };
}

export function buildLogarithm (args: Expression[], { dialect }: { dialect: Dialect }): LogExpr | LnExpr {
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
  const parserClass = dialect._constructor.parserClass;
  const logDefaultsToLn = parserClass?.LOG_DEFAULTS_TO_LN ?? false;

  return logDefaultsToLn
    ? new LnExpr({ this: thisArg })
    : new LogExpr({ this: thisArg });
}

export function buildHex (args: Expression[], { dialect }: { dialect: Dialect }): HexExpr | LowerHexExpr {
  if (args.length < 1) {
    throw new Error('buildHex only accepts an expression list with at least one expression');
  }
  const arg = args[0];
  return dialect._constructor.HEX_LOWERCASE
    ? new LowerHexExpr({ this: arg })
    : new HexExpr({ this: arg });
}

export function buildLower (args: Expression[]): LowerExpr | LowerHexExpr {
  if (args.length < 1) {
    throw new Error('buildLower only accepts an expression list with at least one expression');
  }
  // LOWER(HEX(..)) can be simplified to LowerHex to simplify its transpilation
  const arg = args[0];
  return arg instanceof HexExpr
    ? new LowerHexExpr({ this: arg.args.this })
    : new LowerExpr({ this: arg });
}

export function buildUpper (args: Expression[]): UpperExpr | HexExpr {
  if (args.length < 1) {
    throw new Error('buildUpper only accepts an expression list with at least one expression');
  }
  // UPPER(HEX(..)) can be simplified to Hex to simplify its transpilation
  const arg = args[0];
  return arg instanceof LowerHexExpr
    ? new HexExpr({ this: arg.args.this })
    : new UpperExpr({ this: arg });
}

export function buildExtractJsonWithPath<E extends Expression> (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  exprType: new (args: any) => E,
): (args: Expression[], options: { dialect: Dialect }) => E {
  return function builder (args: Expression[], { dialect }: { dialect: Dialect }): E {
    if (args.length < 2) {
      throw new Error('buildExtractJsonWithPath only accepts an expression list with at least two expressions');
    }
    const expression = new exprType({
      this: args[0],
      expression: dialect.toJsonPath(seqGet(args, 1)),
    });

    if (2 < args.length && expression instanceof JsonExtractExpr) {
      expression.setArgKey('expressions', args.slice(2));
    }

    if (expression instanceof JsonExtractScalarExpr) {
      expression.setArgKey('scalarOnly', dialect._constructor.JSON_EXTRACT_SCALAR_SCALAR_ONLY);
    }

    return expression;
  };
}

export function buildMod (args: Expression[]): ModExpr {
  if (args.length < 2) {
    throw new Error('buildMod only accepts an expression list with at least two expressions');
  }
  let thisArg = args[0];
  let expression = args[1];

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
    this: seqGet(args, 0),
    expression: seqGet(args, 1),
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
  args: (Expression | string)[],
  options: { defaultSourceTz?: string } = {},
): ConvertTimezoneExpr {
  if (args.length < 2) {
    throw new Error('buildConvertTimezone only accepts an expression list with at least two expressions');
  }
  const { defaultSourceTz } = options;

  if (args.length === 2) {
    const sourceTz = defaultSourceTz ? LiteralExpr.string(defaultSourceTz) : undefined;
    const firstArg = seqGet(args, 0);
    const targetTz = typeof firstArg === 'string' ? LiteralExpr.string(firstArg) : firstArg;
    const secondArg = seqGet(args, 0);
    const timestamp = typeof secondArg === 'string' ? LiteralExpr.string(secondArg) : secondArg;
    return new ConvertTimezoneExpr({
      sourceTz,
      targetTz,
      timestamp,
    });
  }

  return ConvertTimezoneExpr.fromArgList(args);
}

export function buildTrim (
  args: Expression[],
  options: {
    isLeft?: boolean;
    reverseArgs?: boolean;
  } = {},
): TrimExpr {
  if (args.length < 1) {
    throw new Error('buildTrim only accepts an expression list with at least one expression');
  }
  const {
    isLeft = true, reverseArgs = false,
  } = options;

  let thisArg = seqGet(args, 0);
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
    this: seqGet(args, 0),
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
    this: seqGet(args, 1),
    substr: seqGet(args, 0),
    position: seqGet(args, 2),
  });
}

export function buildArrayAppend (args: Expression[], { dialect }: { dialect: Dialect }): ArrayAppendExpr {
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
    this: seqGet(args, 0),
    expression: seqGet(args, 1),
    nullPropagation: dialect._constructor.ARRAY_FUNCS_PROPAGATES_NULLS,
  });
}

export function buildArrayPrepend (args: Expression[], { dialect }: { dialect: Dialect }): ArrayPrependExpr {
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
    this: args[0],
    expression: args[1],
    nullPropagation: dialect._constructor.ARRAY_FUNCS_PROPAGATES_NULLS,
  });
}

export function buildArrayConcat (args: Expression[], { dialect }: { dialect: Dialect }): ArrayConcatExpr {
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
    this: args[0],
    expressions: args.slice(1),
    nullPropagation: dialect._constructor.ARRAY_FUNCS_PROPAGATES_NULLS,
  });
}

export function buildArrayRemove (args: Expression[], { dialect }: { dialect: Dialect }): ArrayRemoveExpr {
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
    this: args[0],
    expression: args[1],
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
  into?: string | (new (args: any) => IntoT) | string[] | (new (args: any) => IntoT)[];
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
  @cache
  static get FUNCTIONS (): Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression> {
    return {
      // Spread all fromArgList functions from FUNCTION_BY_NAME
      ...Object.fromEntries(
        Array.from(FUNCTION_BY_NAME.entries()).map(([name, func]) => [name, (args: Expression[], _options: { dialect: Dialect }) => func.fromArgList(args)]),
      ),

      // Coalesce variants
      ...Object.fromEntries(
        [
          'COALESCE',
          'IFNULL',
          'NVL',
        ].map((name) => [name, (args: Expression[], _options: { dialect: Dialect }) => buildCoalesce(args)]),
      ),

      // Array functions
      ARRAY: (args, _options) => new ArrayExpr({ expressions: args }),

      ARRAYAGG: (args, { dialect }) => new ArrayAggExpr({
        this: args[0],
        nullsExcluded: dialect._constructor.ARRAY_AGG_INCLUDES_NULLS === undefined ? true : undefined,
      }),

      ARRAY_AGG: (args, { dialect }) => new ArrayAggExpr({
        this: args[0],
        nullsExcluded: dialect._constructor.ARRAY_AGG_INCLUDES_NULLS === undefined ? true : undefined,
      }),

      ARRAY_APPEND: buildArrayAppend,
      ARRAY_CAT: buildArrayConcat,
      ARRAY_CONCAT: buildArrayConcat,
      ARRAY_PREPEND: buildArrayPrepend,
      ARRAY_REMOVE: buildArrayRemove,

      // Aggregate functions
      COUNT: (args, _options) => new CountExpr({
        this: args[0],
        expressions: args.slice(1),
        bigInt: true,
      }),

      // String functions
      CONCAT: (args, { dialect }) => new ConcatExpr({
        expressions: args,
        safe: !dialect._constructor.STRICT_STRING_CONCAT,
        coalesce: dialect._constructor.CONCAT_COALESCE,
      }),

      CONCAT_WS: (args, { dialect }) => new ConcatWsExpr({
        expressions: args,
        safe: !dialect._constructor.STRICT_STRING_CONCAT,
        coalesce: dialect._constructor.CONCAT_COALESCE,
      }),

      // Conversion functions
      CONVERT_TIMEZONE: (args, _options) => buildConvertTimezone(args),

      DATE_TO_DATE_STR: (args, _options) => new CastExpr({
        this: args[0],
        to: new DataTypeExpr({ this: DataTypeExprKind.TEXT }),
      }),

      TIME_TO_TIME_STR: (args, _options) => new CastExpr({
        this: args[0],
        to: new DataTypeExpr({ this: DataTypeExprKind.TEXT }),
      }),

      // Generator functions
      GENERATE_DATE_ARRAY: (args, _options) => new GenerateDateArrayExpr({
        start: args[0],
        end: args[1],
        step: seqGet(args, 2) || new IntervalExpr({
          this: LiteralExpr.string('1'),
          unit: var_('DAY'),
        }),
      }),

      GENERATE_UUID: (args, { dialect }) => new UuidExpr({
        isString: dialect._constructor.UUID_IS_STRING_TYPE || undefined,
      }),

      // Pattern matching
      GLOB: (args, _options) => new GlobExpr({
        this: args[1],
        expression: args[0],
      }),

      LIKE: (args, _options) => buildLike(args),

      // Comparison functions
      GREATEST: (args, { dialect }) => new GreatestExpr({
        this: args[0],
        expressions: args.slice(1),
        ignoreNulls: dialect._constructor.LEAST_GREATEST_IGNORES_NULLS,
      }),

      LEAST: (args, { dialect }) => new LeastExpr({
        this: args[0],
        expressions: args.slice(1),
        ignoreNulls: dialect._constructor.LEAST_GREATEST_IGNORES_NULLS,
      }),

      // Encoding functions
      HEX: (args, { dialect }) => buildHex(args, { dialect }),
      TO_HEX: (args, { dialect }) => buildHex(args, { dialect }),

      // JSON functions
      JSON_EXTRACT: buildExtractJsonWithPath(JsonExtractExpr),
      JSON_EXTRACT_SCALAR: buildExtractJsonWithPath(JsonExtractScalarExpr),
      JSON_EXTRACT_PATH_TEXT: buildExtractJsonWithPath(JsonExtractScalarExpr),

      JSON_KEYS: (args, { dialect }) => new JsonKeysExpr({
        this: args[0],
        expression: dialect.toJsonPath(seqGet(args, 1)),
      }),

      // Math functions
      LOG: (args, { dialect }) => buildLogarithm(args, { dialect }),
      LOG2: (args, _options) => new LogExpr({
        this: LiteralExpr.number(2),
        expression: seqGet(args, 0),
      }),
      LOG10: (args, _options) => new LogExpr({
        this: LiteralExpr.number(10),
        expression: seqGet(args, 0),
      }),
      MOD: (args, _options) => buildMod(args),

      // String manipulation
      LOWER: (args, _options) => buildLower(args),
      UPPER: (args, _options) => buildUpper(args),

      LPAD: (args, _options) => buildPad(args),
      LEFTPAD: (args, _options) => buildPad(args),
      RPAD: (args, _options) => buildPad(args, { isLeft: false }),
      RIGHTPAD: (args, _options) => buildPad(args, { isLeft: false }),

      LTRIM: (args, _options) => buildTrim(args),
      RTRIM: (args, _options) => buildTrim(args, { isLeft: false }),

      // String search
      STRPOS: (args, _options) => StrPositionExpr.fromArgList(args),
      INSTR: (args, _options) => StrPositionExpr.fromArgList(args),
      CHARINDEX: (args, _options) => buildLocateStrposition(args),
      LOCATE: (args, _options) => buildLocateStrposition(args),

      // Scope resolution
      SCOPE_RESOLUTION: (args, _options) => args.length !== 2
        ? new ScopeResolutionExpr({ expression: args[0] })
        : new ScopeResolutionExpr({
          this: seqGet(args, 0),
          expression: args[1],
        }),

      // String operations
      TS_OR_DS_TO_DATE_STR: (args, _options) => new SubstringExpr({
        this: new CastExpr({
          this: args[0],
          to: new DataTypeExpr({ this: DataTypeExprKind.TEXT }),
        }),
        start: LiteralExpr.number(1),
        length: LiteralExpr.number(10),
      }),

      // Array operations
      UNNEST: (args, _options) => new UnnestExpr({
        expressions: [args[0]],
      }),

      // UUID
      UUID: (args, { dialect }) => new UuidExpr({
        isString: dialect._constructor.UUID_IS_STRING_TYPE || undefined,
      }),

      // Map operations
      VAR_MAP: (args, _options) => buildVarMap(args),
    };
  }

  // Function expressions that don't require parentheses
  @cache
  static get NO_PAREN_FUNCTIONS (): Partial<Record<TokenType, typeof Expression>> {
    return {
      [TokenType.CURRENT_DATE]: CurrentDateExpr,
      [TokenType.CURRENT_DATETIME]: CurrentDateExpr,
      [TokenType.CURRENT_TIME]: CurrentTimeExpr,
      [TokenType.CURRENT_TIMESTAMP]: CurrentTimestampExpr,
      [TokenType.CURRENT_USER]: CurrentUserExpr,
      [TokenType.LOCALTIME]: LocaltimeExpr,
      [TokenType.LOCALTIMESTAMP]: LocaltimestampExpr,
      [TokenType.CURRENT_ROLE]: CurrentRoleExpr,
    };
  }

  @cache
  static get STRUCT_TYPE_TOKENS (): Set<TokenType> {
    return new Set([
      TokenType.FILE,
      TokenType.NESTED,
      TokenType.OBJECT,
      TokenType.STRUCT,
      TokenType.UNION,
    ]);
  }

  @cache
  static get NESTED_TYPE_TOKENS (): Set<TokenType> {
    return new Set([
      TokenType.ARRAY,
      TokenType.LIST,
      TokenType.LOWCARDINALITY,
      TokenType.MAP,
      TokenType.NULLABLE,
      TokenType.RANGE,
      ...Parser.STRUCT_TYPE_TOKENS,
    ]);
  }

  @cache
  static get ENUM_TYPE_TOKENS (): Set<TokenType> {
    return new Set([
      TokenType.DYNAMIC,
      TokenType.ENUM,
      TokenType.ENUM8,
      TokenType.ENUM16,
    ]);
  }

  @cache
  static get AGGREGATE_TYPE_TOKENS (): Set<TokenType> {
    return new Set([TokenType.AGGREGATEFUNCTION, TokenType.SIMPLEAGGREGATEFUNCTION]);
  }

  @cache
  static get TYPE_TOKENS (): Set<TokenType> {
    return new Set([
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
  }

  @cache
  static get SIGNED_TO_UNSIGNED_TYPE_TOKEN (): Partial<Record<TokenType, TokenType>> {
    return {
      [TokenType.BIGINT]: TokenType.UBIGINT,
      [TokenType.INT]: TokenType.UINT,
      [TokenType.MEDIUMINT]: TokenType.UMEDIUMINT,
      [TokenType.SMALLINT]: TokenType.USMALLINT,
      [TokenType.TINYINT]: TokenType.UTINYINT,
      [TokenType.DECIMAL]: TokenType.UDECIMAL,
      [TokenType.DOUBLE]: TokenType.UDOUBLE,
    };
  }

  @cache
  static get SUBQUERY_PREDICATES (): Partial<Record<TokenType, typeof Expression>> {
    return {
      [TokenType.ANY]: AnyExpr,
      [TokenType.ALL]: AllExpr,
      [TokenType.EXISTS]: ExistsExpr,
      [TokenType.SOME]: AnyExpr,
    };
  }

  @cache
  static get RESERVED_TOKENS (): Set<TokenType> {
    return new Set(
      [...Object.values(Tokenizer.SINGLE_TOKENS), TokenType.SELECT].filter((t) => t !== TokenType.IDENTIFIER),
    );
  }

  @cache
  static get DB_CREATABLES (): Set<TokenType> {
    return new Set([
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
  }

  @cache
  static get CREATABLES (): Set<TokenType> {
    return new Set([
      TokenType.COLUMN,
      TokenType.CONSTRAINT,
      TokenType.FOREIGN_KEY,
      TokenType.FUNCTION,
      TokenType.INDEX,
      TokenType.PROCEDURE,
      ...Parser.DB_CREATABLES,
    ]);
  }

  @cache
  static get ALTERABLES (): Set<TokenType> {
    return new Set([
      TokenType.INDEX,
      TokenType.TABLE,
      TokenType.VIEW,
      TokenType.SESSION,
    ]);
  }

  @cache
  static get ID_VAR_TOKENS (): Set<TokenType> {
    return (() => {
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
  }

  @cache
  static get TABLE_ALIAS_TOKENS (): Set<TokenType> {
    return new Set(
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
  }

  @cache
  static get ALIAS_TOKENS (): Set<TokenType> {
    return Parser.ID_VAR_TOKENS;
  }

  @cache
  static get COLON_PLACEHOLDER_TOKENS (): Set<TokenType> {
    return Parser.ID_VAR_TOKENS;
  }

  @cache
  static get ARRAY_CONSTRUCTORS (): Record<string, typeof Expression> {
    return {
      ARRAY: ArrayExpr,
      LIST: ListExpr,
    };
  }

  @cache
  static get COMMENT_TABLE_ALIAS_TOKENS (): Set<TokenType> {
    return new Set(
      [...Parser.TABLE_ALIAS_TOKENS].filter((t) => t !== TokenType.IS),
    );
  }

  @cache
  static get UPDATE_ALIAS_TOKENS (): Set<TokenType> {
    return new Set(
      [...Parser.TABLE_ALIAS_TOKENS].filter((t) => t !== TokenType.SET),
    );
  }

  @cache
  static get TRIM_TYPES (): Set<TrimPosition> {
    return new Set([
      TrimPosition.LEADING,
      TrimPosition.TRAILING,
      TrimPosition.BOTH,
    ]);
  }

  @cache
  static get FUNC_TOKENS (): Set<TokenType> {
    return new Set([
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
      ...Object.keys(Parser.SUBQUERY_PREDICATES) as TokenType[],
    ]);
  }

  @cache
  static get CONJUNCTION (): Partial<Record<TokenType, typeof Expression>> {
    return {
      [TokenType.AND]: AndExpr,
    };
  }

  @cache
  static get ASSIGNMENT (): Partial<Record<TokenType, typeof Expression>> {
    return {
      [TokenType.COLON_EQ]: PropertyEqExpr,
    };
  }

  @cache
  static get DISJUNCTION (): Partial<Record<TokenType, typeof Expression>> {
    return {
      [TokenType.OR]: OrExpr,
    };
  }

  @cache
  static get EQUALITY (): Partial<Record<TokenType, typeof Expression>> {
    return {
      [TokenType.EQ]: EqExpr,
      [TokenType.NEQ]: NeqExpr,
      [TokenType.NULLSAFE_EQ]: NullSafeEqExpr,
    };
  }

  @cache
  static get COMPARISON (): Partial<Record<TokenType, typeof Expression>> {
    return {
      [TokenType.GT]: GtExpr,
      [TokenType.GTE]: GteExpr,
      [TokenType.LT]: LtExpr,
      [TokenType.LTE]: LteExpr,
    };
  }

  @cache
  static get BITWISE (): Partial<Record<TokenType, typeof Expression>> {
    return {
      [TokenType.AMP]: BitwiseAndExpr,
      [TokenType.CARET]: BitwiseXorExpr,
      [TokenType.PIPE]: BitwiseOrExpr,
    };
  }

  @cache
  static get TERM (): Partial<Record<TokenType, typeof Expression>> {
    return {
      [TokenType.DASH]: SubExpr,
      [TokenType.PLUS]: AddExpr,
      [TokenType.MOD]: ModExpr,
      [TokenType.COLLATE]: CollateExpr,
    };
  }

  @cache
  static get FACTOR (): Partial<Record<TokenType, typeof Expression>> {
    return {
      [TokenType.DIV]: IntDivExpr,
      [TokenType.LR_ARROW]: DistanceExpr,
      [TokenType.SLASH]: DivExpr,
      [TokenType.STAR]: MulExpr,
    };
  }

  @cache
  static get EXPONENT (): Partial<Record<TokenType, typeof Expression>> {
    return {};
  }

  @cache
  static get TIMES (): Set<TokenType> {
    return new Set([TokenType.TIME, TokenType.TIMETZ]);
  }

  @cache
  static get TIMESTAMPS (): Set<TokenType> {
    return new Set([
      TokenType.TIMESTAMP,
      TokenType.TIMESTAMPNTZ,
      TokenType.TIMESTAMPTZ,
      TokenType.TIMESTAMPLTZ,
      ...Parser.TIMES,
    ]);
  }

  @cache
  static get SET_OPERATIONS (): Set<TokenType> {
    return new Set([
      TokenType.UNION,
      TokenType.INTERSECT,
      TokenType.EXCEPT,
    ]);
  }

  @cache
  static get JOIN_METHODS (): Set<TokenType> {
    return new Set([
      TokenType.ASOF,
      TokenType.NATURAL,
      TokenType.POSITIONAL,
    ]);
  }

  @cache
  static get JOIN_SIDES (): Set<TokenType> {
    return new Set([
      TokenType.LEFT,
      TokenType.RIGHT,
      TokenType.FULL,
    ]);
  }

  @cache
  static get JOIN_KINDS (): Set<TokenType> {
    return new Set([
      TokenType.ANTI,
      TokenType.CROSS,
      TokenType.INNER,
      TokenType.OUTER,
      TokenType.SEMI,
      TokenType.STRAIGHT_JOIN,
    ]);
  }

  static JOIN_HINTS: Set<string> = new Set();
  @cache
  static get LAMBDAS (): Partial<Record<TokenType, (this: Parser, expressions: Expression[]) => Expression>> {
    return {
      [TokenType.ARROW]: function (this: Parser, expressions: Expression[]) {
        return this.expression(
          LambdaExpr,
          {
            this: this.replaceLambda(
              this.parseDisjunction(),
              expressions,
            ),
            expressions: expressions,
          },
        );
      },
      [TokenType.FARROW]: function (this: Parser, expressions: Expression[]) {
        return this.expression(
          KwargExpr,
          {
            this: var_(expressions[0].name),
            expression: this.parseDisjunction(),
          },
        );
      },
    };
  }

  @cache
  static get COLUMN_OPERATORS (): Partial<Record<TokenType, undefined | ((this: Parser, this_?: Expression, to?: Expression) => Expression)>> {
    return {
      [TokenType.DOT]: undefined,
      [TokenType.DOTCOLON]: function (this: Parser, this_?: Expression, to?: Expression) {
        return this.expression(
          JsonCastExpr,
          {
            this: this_,
            to: to,
          },
        );
      },
      [TokenType.DCOLON]: function (this: Parser, this_?: Expression, to?: Expression) {
        return this.buildCast({
          strict: this._constructor.STRICT_CAST,
          this: this_,
          to: to,
        });
      },
      [TokenType.ARROW]: function (this: Parser, this_?: Expression, path?: Expression) {
        return this.expression(
          JsonExtractExpr,
          {
            this: this_,
            expression: this.dialect.toJsonPath(path),
            onlyJsonTypes: this._constructor.JSON_ARROWS_REQUIRE_JSON_TYPE,
          },
        );
      },
      [TokenType.DARROW]: function (this: Parser, this_?: Expression, path?: Expression) {
        return this.expression(
          JsonExtractScalarExpr,
          {
            this: this_,
            expression: this.dialect.toJsonPath(path),
            onlyJsonTypes: this._constructor.JSON_ARROWS_REQUIRE_JSON_TYPE,
            scalarOnly: this._dialectConstructor.JSON_EXTRACT_SCALAR_SCALAR_ONLY,
          },
        );
      },
      [TokenType.HASH_ARROW]: function (this: Parser, this_?: Expression, path?: Expression) {
        return this.expression(
          JsonbExtractExpr,
          {
            this: this_,
            expression: path,
          },
        );
      },
      [TokenType.DHASH_ARROW]: function (this: Parser, this_?: Expression, path?: Expression) {
        return this.expression(
          JsonbExtractScalarExpr,
          {
            this: this_,
            expression: path,
          },
        );
      },
      [TokenType.PLACEHOLDER]: function (this: Parser, this_?: Expression, key?: Expression) {
        return this.expression(
          JsonbContainsExpr,
          {
            this: this_,
            expression: key,
          },
        );
      },
    };
  }

  @cache
  static get CAST_COLUMN_OPERATORS (): Set<TokenType> {
    return new Set([TokenType.DOTCOLON, TokenType.DCOLON]);
  }

  @cache
  static get EXPRESSION_PARSERS (): Record<string, (this: Parser) => Expression | undefined> {
    return {
      [ExpressionKey.CLUSTER]: function (this: Parser) {
        return this.parseSort(ClusterExpr, TokenType.CLUSTER_BY);
      },
      [ExpressionKey.COLUMN]: function (this: Parser) {
        return this.parseColumn();
      },
      [ExpressionKey.COLUMN_DEF]: function (this: Parser) {
        return this.parseColumnDef(this.parseColumn());
      },
      [ExpressionKey.CONDITION]: function (this: Parser) {
        return this.parseDisjunction();
      },
      [ExpressionKey.DATA_TYPE]: function (this: Parser) {
        return this.parseTypes({
          allowIdentifiers: false,
          schema: true,
        });
      },
      [ExpressionKey.EXPRESSION]: function (this: Parser) {
        return this.parseExpression();
      },
      [ExpressionKey.FROM]: function (this: Parser) {
        return this.parseFrom({ joins: true });
      },
      [ExpressionKey.GRANT_PRINCIPAL]: function (this: Parser) {
        return this.parseGrantPrincipal();
      },
      [ExpressionKey.GRANT_PRIVILEGE]: function (this: Parser) {
        return this.parseGrantPrivilege();
      },
      [ExpressionKey.GROUP]: function (this: Parser) {
        return this.parseGroup();
      },
      [ExpressionKey.HAVING]: function (this: Parser) {
        return this.parseHaving();
      },
      [ExpressionKey.HINT]: function (this: Parser) {
        return this.parseHintBody();
      },
      [ExpressionKey.IDENTIFIER]: function (this: Parser) {
        return this.parseIdVar();
      },
      [ExpressionKey.JOIN]: function (this: Parser) {
        return this.parseJoin();
      },
      [ExpressionKey.LAMBDA]: function (this: Parser) {
        return this.parseLambda();
      },
      [ExpressionKey.LATERAL]: function (this: Parser) {
        return this.parseLateral();
      },
      [ExpressionKey.LIMIT]: function (this: Parser) {
        return this.parseLimit();
      },
      [ExpressionKey.OFFSET]: function (this: Parser) {
        return this.parseOffset();
      },
      [ExpressionKey.ORDER]: function (this: Parser) {
        return this.parseOrder();
      },
      [ExpressionKey.ORDERED]: function (this: Parser) {
        return this.parseOrdered();
      },
      [ExpressionKey.PROPERTIES]: function (this: Parser) {
        return this.parseProperties();
      },
      [ExpressionKey.PARTITIONED_BY_PROPERTY]: function (this: Parser) {
        return this.parsePartitionedBy();
      },
      [ExpressionKey.QUALIFY]: function (this: Parser) {
        return this.parseQualify();
      },
      [ExpressionKey.RETURNING]: function (this: Parser) {
        return this.parseReturning();
      },
      [ExpressionKey.SELECT]: function (this: Parser) {
        return this.parseSelect();
      },
      [ExpressionKey.SORT]: function (this: Parser) {
        return this.parseSort(SortExpr, TokenType.SORT_BY);
      },
      [ExpressionKey.TABLE]: function (this: Parser) {
        return this.parseTableParts();
      },
      [ExpressionKey.TABLE_ALIAS]: function (this: Parser) {
        return this.parseTableAlias();
      },
      [ExpressionKey.TUPLE]: function (this: Parser) {
        return this.parseValue({ values: false });
      },
      [ExpressionKey.WHENS]: function (this: Parser) {
        return this.parseWhenMatched();
      },
      [ExpressionKey.WHERE]: function (this: Parser) {
        return this.parseWhere();
      },
      [ExpressionKey.WINDOW]: function (this: Parser) {
        return this.parseNamedWindow();
      },
      [ExpressionKey.WITH]: function (this: Parser) {
        return this.parseWith();
      },
    };
  }

  @cache
  static get STATEMENT_PARSERS (): Partial<Record<TokenType, (this: Parser) => Expression | undefined>> {
    return {
      [TokenType.ALTER]: function (this: Parser) {
        return this.parseAlter();
      },
      [TokenType.ANALYZE]: function (this: Parser) {
        return this.parseAnalyze();
      },
      [TokenType.BEGIN]: function (this: Parser) {
        return this.parseTransaction();
      },
      [TokenType.CACHE]: function (this: Parser) {
        return this.parseCache();
      },
      [TokenType.COMMENT]: function (this: Parser) {
        return this.parseComment();
      },
      [TokenType.COMMIT]: function (this: Parser) {
        return this.parseCommitOrRollback();
      },
      [TokenType.COPY]: function (this: Parser) {
        return this.parseCopy();
      },
      [TokenType.CREATE]: function (this: Parser) {
        return this.parseCreate();
      },
      [TokenType.DELETE]: function (this: Parser) {
        return this.parseDelete();
      },
      [TokenType.DESC]: function (this: Parser) {
        return this.parseDescribe();
      },
      [TokenType.DESCRIBE]: function (this: Parser) {
        return this.parseDescribe();
      },
      [TokenType.DROP]: function (this: Parser) {
        return this.parseDrop();
      },
      [TokenType.GRANT]: function (this: Parser) {
        return this.parseGrant();
      },
      [TokenType.REVOKE]: function (this: Parser) {
        return this.parseRevoke();
      },
      [TokenType.INSERT]: function (this: Parser) {
        return this.parseInsert();
      },
      [TokenType.KILL]: function (this: Parser) {
        return this.parseKill();
      },
      [TokenType.LOAD]: function (this: Parser) {
        return this.parseLoad();
      },
      [TokenType.MERGE]: function (this: Parser) {
        return this.parseMerge();
      },
      [TokenType.PIVOT]: function (this: Parser) {
        return this.parseSimplifiedPivot();
      },
      [TokenType.PRAGMA]: function (this: Parser) {
        return this.expression(PragmaExpr, { this: this.parseExpression() });
      },
      [TokenType.REFRESH]: function (this: Parser) {
        return this.parseRefresh();
      },
      [TokenType.ROLLBACK]: function (this: Parser) {
        return this.parseCommitOrRollback();
      },
      [TokenType.SET]: function (this: Parser) {
        return this.parseSet();
      },
      [TokenType.TRUNCATE]: function (this: Parser) {
        return this.parseTruncateTable();
      },
      [TokenType.UNCACHE]: function (this: Parser) {
        return this.parseUncache();
      },
      [TokenType.UNPIVOT]: function (this: Parser) {
        return this.parseSimplifiedPivot({ isUnpivot: true });
      },
      [TokenType.UPDATE]: function (this: Parser) {
        return this.parseUpdate();
      },
      [TokenType.USE]: function (this: Parser) {
        return this.parseUse();
      },
      [TokenType.SEMICOLON]: () => new SemicolonExpr({}),
    };
  }

  @cache
  static get UNARY_PARSERS (): Partial<Record<TokenType, (this: Parser) => Expression | undefined>> {
    return {
      [TokenType.PLUS]: function (this: Parser) {
        return this.parseUnary();
      },
      [TokenType.NOT]: function (this: Parser) {
        return this.expression(NotExpr, { this: this.parseEquality() });
      },
      [TokenType.TILDE]: function (this: Parser) {
        return this.expression(BitwiseNotExpr, { this: this.parseUnary() });
      },
      [TokenType.DASH]: function (this: Parser) {
        return this.expression(NegExpr, { this: this.parseUnary() });
      },
      [TokenType.PIPE_SLASH]: function (this: Parser) {
        return this.expression(SqrtExpr, { this: this.parseUnary() });
      },
      [TokenType.DPIPE_SLASH]: function (this: Parser) {
        return this.expression(CbrtExpr, { this: this.parseUnary() });
      },
    };
  }

  @cache
  static get STRING_PARSERS (): Partial<Record<TokenType, (this: Parser, token: Token) => Expression>> {
    return {
      [TokenType.HEREDOC_STRING]: function (this: Parser, token: Token) {
        return this.expression(RawStringExpr, { token });
      },
      [TokenType.NATIONAL_STRING]: function (this: Parser, token: Token) {
        return this.expression(NationalExpr, { token });
      },
      [TokenType.RAW_STRING]: function (this: Parser, token: Token) {
        return this.expression(RawStringExpr, { token });
      },
      [TokenType.STRING]: function (this: Parser, token: Token) {
        return this.expression(LiteralExpr, {
          token,
          isString: true,
        });
      },
      [TokenType.UNICODE_STRING]: function (this: Parser, token: Token) {
        return this.expression(
          UnicodeStringExpr,
          {
            token,
            escape: this.matchTextSeq('UESCAPE') && this.parseString(),
          },
        );
      },
    };
  }

  @cache
  static get NUMERIC_PARSERS (): Partial<Record<TokenType, (this: Parser, token: Token) => Expression>> {
    return {
      [TokenType.BIT_STRING]: function (this: Parser, token: Token) {
        return this.expression(BitStringExpr, { token });
      },
      [TokenType.BYTE_STRING]: function (this: Parser, token: Token) {
        return this.expression(
          ByteStringExpr,
          {
            token,
            isBytes: this._dialectConstructor.BYTE_STRING_IS_BYTES_TYPE || undefined,
          },
        );
      },
      [TokenType.HEX_STRING]: function (this: Parser, token: Token) {
        return this.expression(
          HexStringExpr,
          {
            token,
            isInteger: this._dialectConstructor.HEX_STRING_IS_INTEGER_TYPE || undefined,
          },
        );
      },
      [TokenType.NUMBER]: function (this: Parser, token: Token) {
        return this.expression(LiteralExpr, {
          token,
          isString: false,
        });
      },
    };
  }

  @cache
  static get PRIMARY_PARSERS (): Partial<Record<TokenType, (this: Parser, token: Token) => Expression | undefined>> {
    return {
      ...Parser.STRING_PARSERS,
      ...Parser.NUMERIC_PARSERS,
      [TokenType.INTRODUCER]: function (this: Parser, token: Token) {
        return this.parseIntroducer(token);
      },
      [TokenType.NULL]: function (this: Parser, _: Token) {
        return this.expression(NullExpr, {});
      },
      [TokenType.TRUE]: function (this: Parser, _: Token) {
        return this.expression(BooleanExpr, { this: true });
      },
      [TokenType.FALSE]: function (this: Parser, _: Token) {
        return this.expression(BooleanExpr, { this: false });
      },
      [TokenType.SESSION_PARAMETER]: function (this: Parser, _: Token) {
        return this.parseSessionParameter();
      },
      [TokenType.STAR]: function (this: Parser, _: Token) {
        return this.parseStarOps();
      },
    };
  }

  @cache
  static get PLACEHOLDER_PARSERS (): Partial<Record<TokenType, (this: Parser) => Expression | undefined>> {
    return {
      [TokenType.PLACEHOLDER]: function (this: Parser) {
        return this.expression(PlaceholderExpr);
      },
      [TokenType.PARAMETER]: function (this: Parser) {
        return this.parseParameter();
      },
      [TokenType.COLON]: function (this: Parser) {
        return (
          this.matchSet(this._constructor.COLON_PLACEHOLDER_TOKENS)
            ? this.expression(PlaceholderExpr, { this: this.prev?.text })
            : undefined
        );
      },
    };
  }

  @cache
  static get RANGE_PARSERS (): Partial<Record<TokenType, (this: Parser, this_: Expression) => Expression | undefined>> {
    return {
      [TokenType.AT_GT]: binaryRangeParser(ArrayContainsAllExpr),
      [TokenType.BETWEEN]: function (this: Parser, this_: Expression) {
        return this.parseBetween(this_);
      },
      [TokenType.GLOB]: binaryRangeParser(GlobExpr),
      [TokenType.ILIKE]: binaryRangeParser(ILikeExpr),
      [TokenType.IN]: function (this: Parser, this_: Expression) {
        return this.parseIn(this_);
      },
      [TokenType.IRLIKE]: binaryRangeParser(RegexpILikeExpr),
      [TokenType.IS]: function (this: Parser, this_: Expression) {
        return this.parseIs(this_);
      },
      [TokenType.LIKE]: binaryRangeParser(LikeExpr),
      [TokenType.LT_AT]: binaryRangeParser(ArrayContainsAllExpr, { reverseArgs: true }),
      [TokenType.OVERLAPS]: binaryRangeParser(OverlapsExpr),
      [TokenType.RLIKE]: binaryRangeParser(RegexpLikeExpr),
      [TokenType.SIMILAR_TO]: binaryRangeParser(SimilarToExpr),
      [TokenType.FOR]: function (this: Parser, this_: Expression) {
        return this.parseComprehension(this_);
      },
      [TokenType.QMARK_AMP]: binaryRangeParser(JsonbContainsAllTopKeysExpr),
      [TokenType.QMARK_PIPE]: binaryRangeParser(JsonbContainsAnyTopKeysExpr),
      [TokenType.HASH_DASH]: binaryRangeParser(JsonbDeleteAtPathExpr),
      [TokenType.ADJACENT]: binaryRangeParser(AdjacentExpr),
      [TokenType.OPERATOR]: function (this: Parser, this_: Expression) {
        return this.parseOperator(this_);
      },
      [TokenType.AMP_LT]: binaryRangeParser(ExtendsLeftExpr),
      [TokenType.AMP_GT]: binaryRangeParser(ExtendsRightExpr),
    };
  }

  @cache
  static get PIPE_SYNTAX_TRANSFORM_PARSERS (): Partial<Record<string, (this: Parser, query: SelectExpr) => SelectExpr>> {
    return {
      'AGGREGATE': function (this: Parser, query: SelectExpr) {
        return this.parsePipeSyntaxAggregate(query);
      },
      'AS': function (this: Parser, query: SelectExpr) {
        return this.buildPipeCte({
          query,
          expressions: [new StarExpr({})],
          aliasCte: this.parseTableAlias(),
        });
      },
      'EXTEND': function (this: Parser, query: SelectExpr) {
        return this.parsePipeSyntaxExtend(query);
      },
      'LIMIT': function (this: Parser, query: SelectExpr) {
        return this.parsePipeSyntaxLimit(query);
      },
      'ORDER BY': function (this: Parser, query: SelectExpr) {
        return query.orderBy(
          this.parseOrder(),
          {
            append: false,
            copy: false,
          },
        );
      },
      'PIVOT': function (this: Parser, query: SelectExpr) {
        return this.parsePipeSyntaxPivot(query);
      },
      'SELECT': function (this: Parser, query: SelectExpr) {
        return this.parsePipeSyntaxSelect(query);
      },
      'TABLESAMPLE': function (this: Parser, query: SelectExpr) {
        return this.parsePipeSyntaxTablesample(query);
      },
      'UNPIVOT': function (this: Parser, query: SelectExpr) {
        return this.parsePipeSyntaxPivot(query);
      },
      'WHERE': function (this: Parser, query: SelectExpr) {
        return query.where(this.parseWhere(), { copy: false });
      },
    };
  }

  @cache
  static get PROPERTY_PARSERS (): Record<string, (this: Parser, ...args: unknown[]) => Expression | Expression[] | undefined> {
    return {
      'ALLOWED_VALUES': function (this: Parser) {
        return this.expression(
          AllowedValuesPropertyExpr,
          { expressions: this.parseCsv(this.parsePrimary) },
        );
      },
      'ALGORITHM': function (this: Parser) {
        return this.parsePropertyAssignment(AlgorithmPropertyExpr);
      },
      'AUTO': function (this: Parser) {
        return this.parseAutoProperty();
      },
      'AUTO_INCREMENT': function (this: Parser) {
        return this.parsePropertyAssignment(AutoIncrementPropertyExpr);
      },
      'BACKUP': function (this: Parser) {
        return this.expression(
          BackupPropertyExpr,
          { this: this.parseVar({ anyToken: true }) },
        );
      },
      'BLOCKCOMPRESSION': function (this: Parser) {
        return this.parseBlockCompression();
      },
      'CHARSET': function (this: Parser) {
        return this.parseCharacterSet();
      },
      'CHARACTER SET': function (this: Parser) {
        return this.parseCharacterSet();
      },
      'CHECKSUM': function (this: Parser) {
        return this.parseChecksum();
      },
      'CLUSTER BY': function (this: Parser) {
        return this.parseCluster();
      },
      'CLUSTERED': function (this: Parser) {
        return this.parseClusteredBy();
      },
      'COLLATE': function (this: Parser) {
        return this.parsePropertyAssignment(CollatePropertyExpr);
      },
      'COMMENT': function (this: Parser) {
        return this.parsePropertyAssignment(SchemaCommentPropertyExpr);
      },
      'CONTAINS': function (this: Parser) {
        return this.parseContainsProperty();
      },
      'COPY': function (this: Parser) {
        return this.parseCopyProperty();
      },
      'DATABLOCKSIZE': function (this: Parser) {
        return this.parseDataBlocksize();
      },
      'DATA_DELETION': function (this: Parser) {
        return this.parseDataDeletionProperty();
      },
      'DEFINER': function (this: Parser) {
        return this.parseDefiner();
      },
      'DETERMINISTIC': function (this: Parser) {
        return this.expression(
          StabilityPropertyExpr,
          { this: LiteralExpr.string('IMMUTABLE') },
        );
      },
      'DISTRIBUTED': function (this: Parser) {
        return this.parseDistributedProperty();
      },
      'DUPLICATE': function (this: Parser) {
        return this.parseCompositeKeyProperty(DuplicateKeyPropertyExpr);
      },
      'DYNAMIC': function (this: Parser) {
        return this.expression(DynamicPropertyExpr, {});
      },
      'DISTKEY': function (this: Parser) {
        return this.parseDistkey();
      },
      'DISTSTYLE': function (this: Parser) {
        return this.parsePropertyAssignment(DistStylePropertyExpr);
      },
      'EMPTY': function (this: Parser) {
        return this.expression(EmptyPropertyExpr, {});
      },
      'ENGINE': function (this: Parser) {
        return this.parsePropertyAssignment(EnginePropertyExpr);
      },
      'ENVIRONMENT': function (this: Parser) {
        return this.expression(
          EnviromentPropertyExpr,
          { expressions: this.parseWrappedCsv(this.parseAssignment) },
        );
      },
      'EXECUTE': function (this: Parser) {
        return this.parsePropertyAssignment(ExecuteAsPropertyExpr);
      },
      'EXTERNAL': function (this: Parser) {
        return this.expression(ExternalPropertyExpr, {});
      },
      'FALLBACK': function (this: Parser) {
        return this.parseFallback();
      },
      'FORMAT': function (this: Parser) {
        return this.parsePropertyAssignment(FileFormatPropertyExpr);
      },
      'FREESPACE': function (this: Parser) {
        return this.parseFreespace();
      },
      'GLOBAL': function (this: Parser) {
        return this.expression(GlobalPropertyExpr, {});
      },
      'HEAP': function (this: Parser) {
        return this.expression(HeapPropertyExpr, {});
      },
      'ICEBERG': function (this: Parser) {
        return this.expression(IcebergPropertyExpr, {});
      },
      'IMMUTABLE': function (this: Parser) {
        return this.expression(
          StabilityPropertyExpr,
          { this: LiteralExpr.string('IMMUTABLE') },
        );
      },
      'INHERITS': function (this: Parser) {
        return this.expression(
          InheritsPropertyExpr,
          { expressions: this.parseWrappedCsv(this.parseTable) },
        );
      },
      'INPUT': function (this: Parser) {
        return this.expression(InputModelPropertyExpr, { this: this.parseSchema() });
      },
      'JOURNAL': function (this: Parser) {
        return this.parseJournal();
      },
      'LANGUAGE': function (this: Parser) {
        return this.parsePropertyAssignment(LanguagePropertyExpr);
      },
      'LAYOUT': function (this: Parser) {
        return this.parseDictProperty({ this: 'LAYOUT' });
      },
      'LIFETIME': function (this: Parser) {
        return this.parseDictRange({ this: 'LIFETIME' });
      },
      'LIKE': function (this: Parser) {
        return this.parseCreateLike();
      },
      'LOCATION': function (this: Parser) {
        return this.parsePropertyAssignment(LocationPropertyExpr);
      },
      'LOCK': function (this: Parser) {
        return this.parseLocking();
      },
      'LOCKING': function (this: Parser) {
        return this.parseLocking();
      },
      'LOG': function (this: Parser) {
        return this.parseLog();
      },
      'MATERIALIZED': function (this: Parser) {
        return this.expression(MaterializedPropertyExpr, {});
      },
      'MERGEBLOCKRATIO': function (this: Parser) {
        return this.parseMergeBlockRatio();
      },
      'MODIFIES': function (this: Parser) {
        return this.parseModifiesProperty();
      },
      'MULTISET': function (this: Parser) {
        return this.expression(SetPropertyExpr, { multi: true });
      },
      'NO': function (this: Parser) {
        return this.parseNoProperty();
      },
      'ON': function (this: Parser) {
        return this.parseOnProperty();
      },
      'ORDER BY': function (this: Parser) {
        return this.parseOrder({ skipOrderToken: true });
      },
      'OUTPUT': function (this: Parser) {
        return this.expression(OutputModelPropertyExpr, { this: this.parseSchema() });
      },
      'PARTITION': function (this: Parser) {
        return this.parsePartitionedOf();
      },
      'PARTITION BY': function (this: Parser) {
        return this.parsePartitionedBy();
      },
      'PARTITIONED BY': function (this: Parser) {
        return this.parsePartitionedBy();
      },
      'PARTITIONED_BY': function (this: Parser) {
        return this.parsePartitionedBy();
      },
      'PRIMARY KEY': function (this: Parser) {
        return this.parsePrimaryKey({ inProps: true });
      },
      'RANGE': function (this: Parser) {
        return this.parseDictRange({ this: 'RANGE' });
      },
      'READS': function (this: Parser) {
        return this.parseReadsProperty();
      },
      'REMOTE': function (this: Parser) {
        return this.parseRemoteWithConnection();
      },
      'RETURNS': function (this: Parser) {
        return this.parseReturns();
      },
      'STRICT': function (this: Parser) {
        return this.expression(StrictPropertyExpr, {});
      },
      'STREAMING': function (this: Parser) {
        return this.expression(StreamingTablePropertyExpr, {});
      },
      'ROW': function (this: Parser) {
        return this.parseRow();
      },
      'ROW_FORMAT': function (this: Parser) {
        return this.parsePropertyAssignment(RowFormatPropertyExpr);
      },
      'SAMPLE': function (this: Parser) {
        return this.expression(
          SamplePropertyExpr,
          { this: this.matchTextSeq('BY') && this.parseBitwise() },
        );
      },
      'SECURE': function (this: Parser) {
        return this.expression(SecurePropertyExpr, {});
      },
      'SECURITY': function (this: Parser) {
        return this.parseSecurity();
      },
      'SET': function (this: Parser) {
        return this.expression(SetPropertyExpr, { multi: false });
      },
      'SETTINGS': function (this: Parser) {
        return this.parseSettingsProperty();
      },
      'SHARING': function (this: Parser) {
        return this.parsePropertyAssignment(SharingPropertyExpr);
      },
      'SORTKEY': function (this: Parser) {
        return this.parseSortkey();
      },
      'SOURCE': function (this: Parser) {
        return this.parseDictProperty({ this: 'SOURCE' });
      },
      'STABLE': function (this: Parser) {
        return this.expression(
          StabilityPropertyExpr,
          { this: LiteralExpr.string('STABLE') },
        );
      },
      'STORED': function (this: Parser) {
        return this.parseStored();
      },
      'SYSTEM_VERSIONING': function (this: Parser) {
        return this.parseSystemVersioningProperty();
      },
      'TBLPROPERTIES': function (this: Parser) {
        return this.parseWrappedProperties();
      },
      'TEMP': function (this: Parser) {
        return this.expression(TemporaryPropertyExpr, {});
      },
      'TEMPORARY': function (this: Parser) {
        return this.expression(TemporaryPropertyExpr, {});
      },
      'TO': function (this: Parser) {
        return this.parseToTable();
      },
      'TRANSIENT': function (this: Parser) {
        return this.expression(TransientPropertyExpr, {});
      },
      'TRANSFORM': function (this: Parser) {
        return this.expression(
          TransformModelPropertyExpr,
          { expressions: this.parseWrappedCsv(this.parseExpression) },
        );
      },
      'TTL': function (this: Parser) {
        return this.parseTtl();
      },
      'USING': function (this: Parser) {
        return this.parsePropertyAssignment(FileFormatPropertyExpr);
      },
      'UNLOGGED': function (this: Parser) {
        return this.expression(UnloggedPropertyExpr, {});
      },
      'VOLATILE': function (this: Parser) {
        return this.parseVolatileProperty();
      },
      'WITH': function (this: Parser) {
        return this.parseWithProperty();
      },
    };
  }

  @cache
  static get CONSTRAINT_PARSERS (): Partial<Record<string, (this: Parser, ...args: unknown[]) => Expression | Expression[] | undefined>> {
    return {
      'AUTOINCREMENT': function (this: Parser) {
        return this.parseAutoIncrement();
      },
      'AUTO_INCREMENT': function (this: Parser) {
        return this.parseAutoIncrement();
      },
      'CASESPECIFIC': function (this: Parser) {
        return this.expression(CaseSpecificColumnConstraintExpr, { not: false });
      },
      'CHARACTER SET': function (this: Parser) {
        return this.expression(
          CharacterSetColumnConstraintExpr,
          { this: this.parseVarOrString() },
        );
      },
      'CHECK': function (this: Parser) {
        return this.parseCheckConstraint();
      },
      'COLLATE': function (this: Parser) {
        return this.expression(
          CollateColumnConstraintExpr,
          { this: this.parseIdentifier() || this.parseColumn() },
        );
      },
      'COMMENT': function (this: Parser) {
        return this.expression(
          CommentColumnConstraintExpr,
          { this: this.parseString() },
        );
      },
      'COMPRESS': function (this: Parser) {
        return this.parseCompress();
      },
      'CLUSTERED': function (this: Parser) {
        return this.expression(
          ClusteredColumnConstraintExpr,
          { this: this.parseWrappedCsv(this.parseOrdered) },
        );
      },
      'NONCLUSTERED': function (this: Parser) {
        return this.expression(
          NonClusteredColumnConstraintExpr,
          { this: this.parseWrappedCsv(this.parseOrdered) },
        );
      },
      'DEFAULT': function (this: Parser) {
        return this.expression(
          DefaultColumnConstraintExpr,
          { this: this.parseBitwise() },
        );
      },
      'ENCODE': function (this: Parser) {
        return this.expression(EncodeColumnConstraintExpr, { this: this.parseVar() });
      },
      'EPHEMERAL': function (this: Parser) {
        return this.expression(
          EphemeralColumnConstraintExpr,
          { this: this.parseBitwise() },
        );
      },
      'EXCLUDE': function (this: Parser) {
        return this.expression(
          ExcludeColumnConstraintExpr,
          { this: this.parseIndexParams() },
        );
      },
      'FOREIGN KEY': function (this: Parser) {
        return this.parseForeignKey();
      },
      'FORMAT': function (this: Parser) {
        return this.expression(
          DateFormatColumnConstraintExpr,
          { this: this.parseVarOrString() },
        );
      },
      'GENERATED': function (this: Parser) {
        return this.parseGeneratedAsIdentity();
      },
      'IDENTITY': function (this: Parser) {
        return this.parseAutoIncrement();
      },
      'INLINE': function (this: Parser) {
        return this.parseInline();
      },
      'LIKE': function (this: Parser) {
        return this.parseCreateLike();
      },
      'NOT': function (this: Parser) {
        return this.parseNotConstraint();
      },
      'NULL': function (this: Parser) {
        return this.expression(NotNullColumnConstraintExpr, { allowNull: true });
      },
      'ON': function (this: Parser) {
        return (
          this.match(TokenType.UPDATE)
          && this.expression(OnUpdateColumnConstraintExpr, { this: this.parseFunction() })
        )
        || this.expression(OnPropertyExpr, { this: this.parseIdVar() });
      },
      'PATH': function (this: Parser) {
        return this.expression(PathColumnConstraintExpr, { this: this.parseString() });
      },
      'PERIOD': function (this: Parser) {
        return this.parsePeriodForSystemTime();
      },
      'PRIMARY KEY': function (this: Parser) {
        return this.parsePrimaryKey();
      },
      'REFERENCES': function (this: Parser) {
        return this.parseReferences({ match: false });
      },
      'TITLE': function (this: Parser) {
        return this.expression(
          TitleColumnConstraintExpr,
          { this: this.parseVarOrString() },
        );
      },
      'TTL': function (this: Parser) {
        return this.expression(MergeTreeTtlExpr, { expressions: [this.parseBitwise()] });
      },
      'UNIQUE': function (this: Parser) {
        return this.parseUnique();
      },
      'UPPERCASE': function (this: Parser) {
        return this.expression(UppercaseColumnConstraintExpr);
      },
      'WITH': function (this: Parser) {
        return this.expression(
          PropertiesExpr,
          { expressions: this.parseWrappedProperties() },
        );
      },
      'BUCKET': function (this: Parser) {
        return this.parsePartitionedByBucketOrTruncate();
      },
      'TRUNCATE': function (this: Parser) {
        return this.parsePartitionedByBucketOrTruncate();
      },
    };
  }

  @cache
  static get ALTER_PARSERS (): Partial<Record<string, (this: Parser) => Expression | Expression[] | undefined>> {
    return {
      'ADD': function (this: Parser) {
        return this.parseAlterTableAdd();
      },
      'AS': function (this: Parser) {
        return this.parseSelect();
      },
      'ALTER': function (this: Parser) {
        return this.parseAlterTableAlter();
      },
      'CLUSTER BY': function (this: Parser) {
        return this.parseCluster({ wrapped: true });
      },
      'DELETE': function (this: Parser) {
        return this.expression(DeleteExpr, { where: this.parseWhere() });
      },
      'DROP': function (this: Parser) {
        return this.parseAlterTableDrop();
      },
      'RENAME': function (this: Parser) {
        return this.parseAlterTableRename();
      },
      'SET': function (this: Parser) {
        return this.parseAlterTableSet();
      },
      'SWAP': function (this: Parser) {
        return this.expression(
          SwapTableExpr,
          { this: this.match(TokenType.WITH) && this.parseTable({ schema: true }) },
        );
      },
    };
  }

  static ALTER_ALTER_PARSERS: Partial<Record<string, (this: Parser) => Expression>> = {
    DISTKEY: function (this: Parser) {
      return this.parseAlterDiststyle();
    },
    DISTSTYLE: function (this: Parser) {
      return this.parseAlterDiststyle();
    },
    SORTKEY: function (this: Parser) {
      return this.parseAlterSortkey();
    },
    COMPOUND: function (this: Parser) {
      return this.parseAlterSortkey({ compound: true });
    },
  };

  static SCHEMA_UNNAMED_CONSTRAINTS: Set<string> = new Set([
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

  @cache
  static get NO_PAREN_FUNCTION_PARSERS (): Partial<Record<string, (this: Parser) => Expression | undefined>> {
    return {
      ANY: function (this: Parser) {
        return this.expression(AnyExpr, { this: this.parseBitwise() });
      },
      CASE: function (this: Parser) {
        return this.parseCase();
      },
      CONNECT_BY_ROOT: function (this: Parser) {
        return this.expression(
          ConnectByRootExpr,
          { this: this.parseColumn() },
        );
      },
      IF: function (this: Parser) {
        return this.parseIf();
      },
    };
  }

  @cache
  static get INVALID_FUNC_NAME_TOKENS (): Set<TokenType> {
    return new Set([TokenType.IDENTIFIER, TokenType.STRING]);
  }

  static FUNCTIONS_WITH_ALIASED_ARGS: Set<string> = new Set(['STRUCT']);
  @cache
  static get KEY_VALUE_DEFINITIONS (): Set<typeof Expression> {
    return new Set([
      AliasExpr,
      EqExpr,
      PropertyEqExpr,
      SliceExpr,
    ]);
  }

  @cache
  static get FUNCTION_PARSERS (): Partial<Record<string, (this: Parser) => Expression | undefined>> {
    return {
      ...Object.fromEntries(
        ArgMaxExpr.sqlNames().map((name) => [
          name,
          function (this: Parser) {
            return this.parseMaxMinBy(ArgMaxExpr);
          },
        ]),
      ),
      ...Object.fromEntries(
        ArgMinExpr.sqlNames().map((name) => [
          name,
          function (this: Parser) {
            return this.parseMaxMinBy(ArgMinExpr);
          },
        ]),
      ),
      CAST: function (this: Parser) {
        return this.parseCast(this._constructor.STRICT_CAST);
      },
      CEIL: function (this: Parser) {
        return this.parseCeilFloor(CeilExpr);
      },
      CONVERT: function (this: Parser) {
        return this.parseConvert(this._constructor.STRICT_CAST);
      },
      CHAR: function (this: Parser) {
        return this.parseChar();
      },
      CHR: function (this: Parser) {
        return this.parseChar();
      },
      DECODE: function (this: Parser) {
        return this.parseDecode();
      },
      EXTRACT: function (this: Parser) {
        return this.parseExtract();
      },
      FLOOR: function (this: Parser) {
        return this.parseCeilFloor(FloorExpr);
      },
      GAP_FILL: function (this: Parser) {
        return this.parseGapFill();
      },
      INITCAP: function (this: Parser) {
        return this.parseInitcap();
      },
      JSON_OBJECT: function (this: Parser) {
        return this.parseJsonObject();
      },
      JSON_OBJECTAGG: function (this: Parser) {
        return this.parseJsonObject({ agg: true });
      },
      JSON_TABLE: function (this: Parser) {
        return this.parseJsonTable();
      },
      MATCH: function (this: Parser) {
        return this.parseMatchAgainst();
      },
      NORMALIZE: function (this: Parser) {
        return this.parseNormalize();
      },
      OPENJSON: function (this: Parser) {
        return this.parseOpenJson();
      },
      OVERLAY: function (this: Parser) {
        return this.parseOverlay();
      },
      POSITION: function (this: Parser) {
        return this.parsePosition();
      },
      SAFE_CAST: function (this: Parser) {
        return this.parseCast(false, { safe: true });
      },
      STRING_AGG: function (this: Parser) {
        return this.parseStringAgg();
      },
      SUBSTRING: function (this: Parser) {
        return this.parseSubstring();
      },
      TRIM: function (this: Parser) {
        return this.parseTrim();
      },
      TRY_CAST: function (this: Parser) {
        return this.parseCast(false, { safe: true });
      },
      TRY_CONVERT: function (this: Parser) {
        return this.parseConvert(false, { safe: true });
      },
      XMLELEMENT: function (this: Parser) {
        return this.parseXmlElement();
      },
      XMLTABLE: function (this: Parser) {
        return this.parseXmlTable();
      },
    };
  }

  @cache
  static get QUERY_MODIFIER_PARSERS (): Partial<Record<TokenType, (this: Parser) => [string, Expression | Expression[] | undefined]>> {
    return {
      [TokenType.MATCH_RECOGNIZE]: function (this: Parser): [string, Expression | undefined] {
        return ['match', this.parseMatchRecognize()];
      },
      [TokenType.PREWHERE]: function (this: Parser): [string, Expression | undefined] {
        return ['prewhere', this.parsePrewhere()];
      },
      [TokenType.WHERE]: function (this: Parser): [string, Expression | undefined] {
        return ['where', this.parseWhere()];
      },
      [TokenType.GROUP_BY]: function (this: Parser): [string, Expression | undefined] {
        return ['group', this.parseGroup()];
      },
      [TokenType.HAVING]: function (this: Parser): [string, Expression | undefined] {
        return ['having', this.parseHaving()];
      },
      [TokenType.QUALIFY]: function (this: Parser): [string, Expression | undefined] {
        return ['qualify', this.parseQualify()];
      },
      [TokenType.WINDOW]: function (this: Parser): [string, Expression[] | undefined] {
        return ['windows', this.parseWindowClause()];
      },
      [TokenType.ORDER_BY]: function (this: Parser): [string, Expression | undefined] {
        return ['order', this.parseOrder()];
      },
      [TokenType.LIMIT]: function (this: Parser): [string, Expression | undefined] {
        return ['limit', this.parseLimit()];
      },
      [TokenType.FETCH]: function (this: Parser): [string, Expression | undefined] {
        return ['limit', this.parseLimit()];
      },
      [TokenType.OFFSET]: function (this: Parser): [string, Expression | undefined] {
        return ['offset', this.parseOffset()];
      },
      [TokenType.FOR]: function (this: Parser): [string, Expression[] | undefined] {
        return ['locks', this.parseLocks()];
      },
      [TokenType.LOCK]: function (this: Parser): [string, Expression[] | undefined] {
        return ['locks', this.parseLocks()];
      },
      [TokenType.TABLE_SAMPLE]: function (this: Parser): [string, Expression | undefined] {
        return ['sample', this.parseTableSample({ asModifier: true })];
      },
      [TokenType.USING]: function (this: Parser): [string, Expression | undefined] {
        return ['sample', this.parseTableSample({ asModifier: true })];
      },
      [TokenType.CLUSTER_BY]: function (this: Parser): [string, Expression | undefined] {
        return ['cluster', this.parseSort(ClusterExpr, TokenType.CLUSTER_BY)];
      },
      [TokenType.DISTRIBUTE_BY]: function (this: Parser): [string, Expression | undefined] {
        return ['distribute', this.parseSort(DistributeExpr, TokenType.DISTRIBUTE_BY)];
      },
      [TokenType.SORT_BY]: function (this: Parser): [string, Expression | undefined] {
        return ['sort', this.parseSort(SortExpr, TokenType.SORT_BY)];
      },
      [TokenType.CONNECT_BY]: function (this: Parser): [string, Expression | undefined] {
        return ['connect', this.parseConnect({ skipStartToken: true })];
      },
      [TokenType.START_WITH]: function (this: Parser): [string, Expression | undefined] {
        return ['connect', this.parseConnect()];
      },
    };
  }

  @cache
  static get QUERY_MODIFIER_TOKENS (): Set<TokenType> {
    return new Set(
      Object.keys(Parser.QUERY_MODIFIER_PARSERS) as TokenType[],
    );
  }

  static SET_PARSERS: Record<string, (this: Parser) => Expression | undefined> = {
    GLOBAL: function (this: Parser) {
      return this.parseSetItemAssignment({ kind: 'GLOBAL' });
    },
    LOCAL: function (this: Parser) {
      return this.parseSetItemAssignment({ kind: 'LOCAL' });
    },
    SESSION: function (this: Parser) {
      return this.parseSetItemAssignment({ kind: 'SESSION' });
    },
    TRANSACTION: function (this: Parser) {
      return this.parseSetTransaction();
    },
  };

  static SHOW_PARSERS: Record<string, (this: Parser) => Expression> = {};
  @cache
  static get TYPE_LITERAL_PARSERS (): Partial<Record<DataTypeExprKind, (this: Parser, thisArg?: Expression, _?: unknown) => Expression>> {
    return {
      [DataTypeExprKind.JSON]: function (this: Parser, thisArg?: Expression, _?: unknown) {
        return this.expression(ParseJsonExpr, { this: thisArg });
      },
    };
  }

  @cache
  static get TYPE_CONVERTERS (): Partial<Record<DataTypeExprKind, (dataType: DataTypeExpr) => DataTypeExpr>> {
    return {};
  }

  @cache
  static get DDL_SELECT_TOKENS (): Set<TokenType> {
    return new Set([
      TokenType.SELECT,
      TokenType.WITH,
      TokenType.L_PAREN,
    ]);
  }

  @cache
  static get PRE_VOLATILE_TOKENS (): Set<TokenType> {
    return new Set([
      TokenType.CREATE,
      TokenType.REPLACE,
      TokenType.UNIQUE,
    ]);
  }

  static TRANSACTION_KIND: Set<string> = new Set([
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
    ShaRD: ['EXTEND', 'NOEXTEND'],
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

  static INSERT_ALTERNATIVES: Set<string> = new Set([
    'ABORT',
    'FAIL',
    'IGNORE',
    'REPLACE',
    'ROLLBACK',
  ]);

  static CLONE_KEYWORDS: Set<string> = new Set(['CLONE', 'COPY']);

  static HISTORICAL_DATA_PREFIX: Set<string> = new Set([
    'AT',
    'BEFORE',
    'END',
  ]);

  static HISTORICAL_DATA_KIND: Set<string> = new Set([
    'OFFSET',
    'STATEMENT',
    'STREAM',
    'TIMESTAMP',
    'VERSION',
  ]);

  static OPCLASS_FOLLOW_KEYWORDS: Set<string> = new Set([
    'ASC',
    'DESC',
    'NULLS',
    'WITH',
  ]);

  @cache
  static get OPTYPE_FOLLOW_TOKENS (): Set<TokenType> {
    return new Set([TokenType.COMMA, TokenType.R_PAREN]);
  }

  @cache
  static get TABLE_INDEX_HINT_TOKENS (): Set<TokenType> {
    return new Set([
      TokenType.FORCE,
      TokenType.IGNORE,
      TokenType.USE,
    ]);
  }

  static VIEW_ATTRIBUTES: Set<string> = new Set([
    'ENCRYPTION',
    'SCHEMABINDING',
    'VIEW_METADATA',
  ]);

  @cache
  static get WINDOW_ALIAS_TOKENS (): Set<TokenType> {
    return (() => {
      const result = new Set(Parser.ID_VAR_TOKENS);
      result.delete(TokenType.RANGE);
      result.delete(TokenType.ROWS);
      return result;
    })();
  }

  @cache
  static get WINDOW_BEFORE_PAREN_TOKENS (): Set<TokenType> {
    return new Set([TokenType.OVER]);
  }

  static WINDOW_SIDES: Set<string> = new Set(['FOLLOWING', 'PRECEDING']);
  @cache
  static get JSON_KEY_VALUE_SEPARATOR_TOKENS (): Set<TokenType> {
    return new Set([
      TokenType.COLON,
      TokenType.COMMA,
      TokenType.IS,
    ]);
  }

  @cache
  static get FETCH_TOKENS (): Set<TokenType> {
    return (() => {
      const result = new Set(Parser.ID_VAR_TOKENS);
      result.delete(TokenType.ROW);
      result.delete(TokenType.ROWS);
      result.delete(TokenType.PERCENT);
      return result;
    })();
  }

  @cache
  static get ADD_CONSTRAINT_TOKENS (): Set<TokenType> {
    return new Set([
      TokenType.CONSTRAINT,
      TokenType.FOREIGN_KEY,
      TokenType.INDEX,
      TokenType.KEY,
      TokenType.PRIMARY_KEY,
      TokenType.UNIQUE,
    ]);
  }

  @cache
  static get DISTINCT_TOKENS (): Set<TokenType> {
    return new Set([TokenType.DISTINCT]);
  }

  @cache
  static get UNNEST_OFFSET_ALIAS_TOKENS (): Set<TokenType> {
    return (() => {
      const result = new Set(Parser.TABLE_ALIAS_TOKENS);
      for (const token of Parser.SET_OPERATIONS) {
        result.delete(token);
      }
      return result;
    })();
  }

  @cache
  static get SELECT_START_TOKENS (): Set<TokenType> {
    return new Set([
      TokenType.L_PAREN,
      TokenType.WITH,
      TokenType.SELECT,
    ]);
  }

  static COPY_INTO_VARLEN_OPTIONS: Set<string> = new Set([
    'FILE_FORMAT',
    'COPY_OPTIONS',
    'FORMAT_OPTIONS',
    'CREDENTIAL',
  ]);

  static IS_JSON_PREDICATE_KIND: Set<string> = new Set([
    'VALUE',
    'SCALAR',
    'ARRAY',
    'OBJECT',
  ]);

  static ODBC_DATETIME_LITERALS: Record<string, typeof Expression> = {};

  static ON_CONDITION_TOKENS: Set<string> = new Set([
    'ERROR',
    'NULL',
    'TRUE',
    'FALSE',
    'EMPTY',
  ]);

  @cache
  static get PRIVILEGE_FOLLOW_TOKENS (): Set<TokenType> {
    return new Set([
      TokenType.ON,
      TokenType.COMMA,
      TokenType.L_PAREN,
    ]);
  }

  static DESCRIBE_STYLES: Set<string> = new Set([
    'ANALYZE',
    'EXTENDED',
    'FORMATTED',
    'HISTORY',
  ]);

  static SET_ASSIGNMENT_DELIMITERS: Set<string> = new Set([
    '=',
    ':=',
    'TO',
  ]);

  static ANALYZE_STYLES: Set<string> = new Set([
    'BUFFER_USAGE_LIMIT',
    'FULL',
    'LOCAL',
    'NO_WRITE_TO_BINLOG',
    'SAMPLE',
    'SKIP_LOCKED',
    'VERBOSE',
  ]);

  static ANALYZE_EXPRESSION_PARSERS: Record<string, (this: Parser) => Expression | undefined> = {
    ALL: function (this: Parser) {
      return this.parseAnalyzeColumns();
    },
    COMPUTE: function (this: Parser) {
      return this.parseAnalyzeStatistics();
    },
    DELETE: function (this: Parser) {
      return this.parseAnalyzeDelete();
    },
    DROP: function (this: Parser) {
      return this.parseAnalyzeHistogram();
    },
    ESTIMATE: function (this: Parser) {
      return this.parseAnalyzeStatistics();
    },
    LIST: function (this: Parser) {
      return this.parseAnalyzeList();
    },
    PREDICATE: function (this: Parser) {
      return this.parseAnalyzeColumns();
    },
    UPDATE: function (this: Parser) {
      return this.parseAnalyzeHistogram();
    },
    VALIDATE: function (this: Parser) {
      return this.parseAnalyzeValidate();
    },
  };

  static PARTITION_KEYWORDS: Set<string> = new Set(['PARTITION', 'SUBPARTITION']);
  @cache
  static get AMBIGUOUS_ALIAS_TOKENS () {
    return [TokenType.LIMIT, TokenType.OFFSET] as const;
  }

  static OPERATION_MODIFIERS: Set<string> = new Set();

  static RECURSIVE_CTE_SEARCH_KIND: Set<string> = new Set([
    'BREADTH',
    'DEPTH',
    'CYCLE',
  ]);

  @cache
  static get MODIFIABLES (): (typeof Expression)[] {
    return [
      QueryExpr,
      TableExpr,
      TableFromRowsExpr,
      ValuesExpr,
    ];
  }

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

  static SET_OP_MODIFIERS: Set<string> = new Set([
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
  protected tokens: Token[];
  protected index: number;
  protected curr: Token | undefined;
  protected next: Token | undefined;
  protected prev: Token | undefined;
  protected prevComments: string[] | undefined;
  protected pipeCteCounter: number;

  constructor (options: ParseOptions = {}) {
    const {
      errorLevel = ErrorLevel.IMMEDIATE,
      errorMessageContext = 100,
      maxErrors = 3,
    } = options;
    this.sql = '';
    this.dialect = Dialect.getOrRaise(options.dialect);
    this.errorLevel = errorLevel;
    this.errorMessageContext = errorMessageContext;
    this.maxErrors = maxErrors;
    this.errors = [];
    this.tokens = [];
    this.index = 0;
    this.curr = undefined;
    this.next = undefined;
    this.prev = undefined;
    this.prevComments = undefined;
    this.pipeCteCounter = 0;
    this.reset();
  }

  reset (): void {
    this.sql = '';
    this.errors = [];
    this.tokens = [];
    this.index = 0;
    this.curr = undefined;
    this.next = undefined;
    this.prev = undefined;
    this.prevComments = undefined;
    this.pipeCteCounter = 0;
  }

  /**
    * Parses a list of tokens and returns a list of syntax trees, one tree
    * per parsed SQL statement.
    *
    * @param rawTokens - The list of tokens.
    * @param sql - The original SQL string, used to produce helpful debug messages.
    * @returns The list of the produced syntax trees.
    */
  parse (rawTokens: Token[], sql?: string): (Expression | undefined)[] {
    return this._parse({
      parseMethod: function (this: Parser) {
        return this.parseStatement();
      },
      rawTokens,
      sql,
    });
  }

  protected _parse (options: {
    parseMethod: (this: Parser) => Expression | undefined;
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
      this.index = -1;
      this.tokens = tokens;
      this.advance();

      expressions.push(parseMethod.call(this));

      if (this.index < this.tokens.length) {
        this.raiseError('Invalid expression / Unexpected token');
      }

      this.checkErrors();
    }

    return expressions;
  }

  parseIntoTypes<T extends Expression> (
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expressionTypes: string | (new (args: any) => T) | Iterable<string | (new (args: any) => T)>,
    rawTokens: Token[],
    sql?: string,
  ): (Expression | undefined)[] {
    const errors: ParseError[] = [];
    const types = ensureList(expressionTypes);

    for (const expressionType of types) {
      const parser = typeof expressionType === 'string' ? this._constructor.EXPRESSION_PARSERS[expressionType] : (args?: unknown[]) => new expressionType(args);

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
          e.errors[0].intoExpression = typeof expressionType === 'string' ? expressionType : expressionType.name;
          errors.push(e);
        } else {
          throw e;
        }
      }
    }

    throw new ParseError(
      `Failed to parse '${sql || rawTokens}' into ${expressionTypes}`,
      mergeErrors(errors),
    );
  }

  // Helper methods
  findSql (start?: Token, end?: Token): string {
    return this.sql.slice(start?.start ?? 0, (end?.end ?? 0) + 1);
  }

  isConnected (): boolean {
    return !!(
      this.prev
      && this.curr
      && (this.prev.end ?? 0) + 1 === (this.curr.start ?? 0)
    );
  }

  protected advance (times: number = 1): void {
    this.index += times;
    this.curr = seqGet(this.tokens, this.index);
    this.next = seqGet(this.tokens, this.index + 1);

    if (0 < this.index) {
      this.prev = this.tokens[this.index - 1];
      this.prevComments = this.prev.comments;
    } else {
      this.prev = undefined;
      this.prevComments = undefined;
    }
  }

  protected retreat (index: number): void {
    if (index !== this.index) {
      this.advance(index - this.index);
    }
  }

  warnUnsupported (): void {
    if (this.tokens.length <= 1) {
      return;
    }

    // We use findSql because this.sql may comprise multiple chunks, and we're only
    // interested in emitting a warning for the one being currently processed.
    const sql = this.findSql(
      this.tokens[0],
      this.tokens[this.tokens.length - 1],
    ).slice(0, this.errorMessageContext);

    console.warn(
      `'${sql}' contains unsupported syntax. Falling back to parsing as a 'Command'.`,
    );
  }

  protected match (tokenType: TokenType, options: {
    advance?: boolean;
    expression?: Expression;
  } = {}): boolean {
    const {
      advance = true, expression,
    } = options;

    if (this.curr?.tokenType === tokenType) {
      if (advance) {
        this.advance();
      }
      this.addComments(expression);
      return true;
    }
    return false;
  }

  tryParse<T extends Expression | Expression[] | undefined> (
    parseMethod: () => T,
    options: { retreat?: boolean } = {},
  ): T | undefined {
    const { retreat = false } = options;
    const index = this.index;
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
        this.retreat(index);
      }
      this.errorLevel = errorLevel;
    }

    return result;
  }

  parseParen (): Expression | undefined {
    if (!this.match(TokenType.L_PAREN)) {
      return undefined;
    }

    const comments = this.prevComments;
    const query = this.parseSelect();

    let expressions: Expression[];
    if (query) {
      expressions = [query];
    } else {
      expressions = this.parseExpressions();
    }

    let thisExpr = seqGet(expressions, 0);

    if (!thisExpr && this.match(TokenType.R_PAREN, { advance: false })) {
      thisExpr = this.expression(TupleExpr, {});
    } else if (thisExpr && UNWRAPPED_QUERIES.some((cls) => thisExpr instanceof cls)) {
      thisExpr = this.parseSubquery(thisExpr, {
        parseAlias: false,
      });
    } else if (thisExpr instanceof SubqueryExpr || thisExpr instanceof ValuesExpr) {
      thisExpr = this.parseSubquery(this.parseQueryModifiers(this.parseSetOperations(thisExpr)), {
        parseAlias: false,
      });
    } else if (1 < expressions.length || this.prev?.tokenType === TokenType.COMMA) {
      thisExpr = this.expression(TupleExpr, { expressions });
    } else {
      thisExpr = this.expression(ParenExpr, { this: thisExpr });
    }

    if (thisExpr && comments) {
      thisExpr.addComments(comments);
    }

    this.matchRParen(thisExpr);

    if (thisExpr instanceof ParenExpr && thisExpr.args.this instanceof AggFuncExpr) {
      return this.parseWindow(thisExpr);
    }

    return thisExpr;
  }

  parseDrop (options: { exists?: boolean } = {}): DropExpr | CommandExpr {
    const { exists } = options;
    const start = this.prev;
    const temporary = this.match(TokenType.TEMPORARY) || undefined;
    const materialized = this.matchTextSeq('MATERIALIZED') || undefined;

    const kind = (this.matchSet(this._constructor.CREATABLES) || undefined) && this.prev?.text.toUpperCase();
    if (!kind) {
      return this.parseAsCommand(start);
    }

    const concurrently = this.matchTextSeq('CONCURRENTLY') || undefined;
    const ifExists = exists || this.parseExists();

    let thisExpr: Expression | undefined;
    if (kind === 'COLUMN') {
      thisExpr = this.parseColumn();
    } else {
      thisExpr = this.parseTableParts({
        schema: true,
        isDbReference: this.prev?.tokenType === TokenType.SCHEMA,
      });
    }

    const cluster = this.match(TokenType.ON) ? this.parseOnProperty() : undefined;

    let expressions: Expression[] | undefined;
    if (this.match(TokenType.L_PAREN, { advance: false })) {
      expressions = this.parseWrappedCsv(() => this.parseTypes());
    }

    return this.expression(DropExpr, {
      exists: ifExists,
      this: thisExpr,
      expressions,
      kind: this._dialectConstructor.CREATABLE_KIND_MAPPING[kind] || kind,
      temporary,
      materialized,
      cascade: this.matchTextSeq('CASCADE'),
      constraints: this.matchTextSeq('CONSTRAINTS'),
      purge: this.matchTextSeq('PURGE'),
      cluster,
      concurrently,
    });
  }

  parseExists (options: { not?: boolean } = {}): boolean | undefined {
    const { not: notParam } = options;
    const result = (
      this.matchTextSeq('IF')
      && (!notParam || this.match(TokenType.NOT))
      && this.match(TokenType.EXISTS)
    );
    return result ? true : undefined;
  }

  parseCreate (): CreateExpr | CommandExpr {
    // Note: this can't be undefined because we've matched a statement parser
    const start = this.prev;

    const replace = (
      start?.tokenType === TokenType.REPLACE
      || this.matchPair(TokenType.OR, TokenType.REPLACE)
      || this.matchPair(TokenType.OR, TokenType.ALTER)
    );
    const refresh = this.matchPair(TokenType.OR, TokenType.REFRESH) || undefined;

    const unique = this.match(TokenType.UNIQUE) || undefined;

    let clustered: boolean | undefined;
    if (this.matchTextSeq(['CLUSTERED', 'COLUMNSTORE'])) {
      clustered = true;
    } else if (
      this.matchTextSeq(['NONCLUSTERED', 'COLUMNSTORE'])
      || this.matchTextSeq('COLUMNSTORE')
    ) {
      clustered = false;
    }

    if (this.matchPair(TokenType.TABLE, TokenType.FUNCTION, { advance: false })) {
      this.advance();
    }

    let properties: PropertiesExpr | undefined;
    let createToken = (this.matchSet(this._constructor.CREATABLES) || undefined) && this.prev;

    if (!createToken) {
      // exp.Properties.Location.POST_CREATE
      properties = this.parseProperties();
      createToken = (this.matchSet(this._constructor.CREATABLES) || undefined) && this.prev;

      if (!properties || !createToken) {
        return this.parseAsCommand(start);
      }
    }

    const concurrently = this.matchTextSeq('CONCURRENTLY') || undefined;
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
        properties.args.expressions?.push(...tempProps.args.expressions ?? []);
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

      expression = (this.match(TokenType.ALIAS) && this.parseHeredoc()) || undefined;
      extendProps(this.parseProperties());

      if (!expression) {
        if (this.match(TokenType.COMMAND)) {
          expression = this.parseAsCommand(this.prev);
        } else {
          begin = this.match(TokenType.BEGIN) || undefined;
          const return_ = this.matchTextSeq('RETURN') || undefined;

          if (this.match(TokenType.STRING, { advance: false })) {
            // Takes care of BigQuery's JavaScript UDF definitions that end in an OPTIONS property
            // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_function_statement
            expression = this.parseString();
            extendProps(this.parseProperties());
          } else {
            expression = this.parseUserDefinedFunctionExpression();
          }

          end = this.matchTextSeq('END') || undefined;

          if (return_) {
            expression = this.expression(ReturnExpr, { this: expression });
          }
        }
      }
    } else if (createToken.tokenType === TokenType.INDEX) {
      // Postgres allows anonymous indexes, eg. CREATE INDEX IF NOT EXISTS ON t(c)
      let index: Expression | undefined;
      let anonymous: boolean;

      if (!this.match(TokenType.ON)) {
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
      this.match(TokenType.COMMA);
      extendProps(this.parseProperties({ before: true }));

      thisExpr = this.parseSchema({ this: tableParts });

      // exp.Properties.Location.POST_SCHEMA and POST_WITH
      extendProps(this.parseProperties());

      const hasAlias = this.match(TokenType.ALIAS) || undefined;
      if (!this.matchSet(this._constructor.DDL_SELECT_TOKENS, { advance: false })) {
        // exp.Properties.Location.POST_ALIAS
        extendProps(this.parseProperties());
      }

      if (createToken.tokenType === TokenType.SEQUENCE) {
        expression = this.parseTypes();
        const props = this.parseProperties();

        if (props) {
          const sequenceProps = new SequencePropertiesExpr({});
          const options: Expression[] = [];

          for (const prop of props.args.expressions ?? []) {
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

          props.args.expressions?.push(sequenceProps);
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
            this.match(TokenType.COMMA);
            indexes.push(index);
          }
        }
      } else if (createToken.tokenType === TokenType.VIEW) {
        if (this.matchTextSeq([
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

      const shallow = this.matchTextSeq('SHALLOW') || undefined;

      if (this.matchTexts(this._constructor.CLONE_KEYWORDS)) {
        const copy = this.prev?.text.toLowerCase() === 'copy';
        clone = this.expression(CloneExpr, {
          this: this.parseTable({ schema: true }),
          shallow,
          copy,
        });
      }
    }

    if (
      this.curr
      && !this.matchSet(new Set([TokenType.R_PAREN, TokenType.COMMA]), { advance: false })
    ) {
      return this.parseAsCommand(start);
    }

    const createKindText = createToken.text.toUpperCase();
    const createKindRaw = this._dialectConstructor.CREATABLE_KIND_MAPPING[createKindText] || createKindText;
    return this.expression(CreateExpr, {
      this: thisExpr,
      kind: enumFromString(CreateExprKind, createKindRaw) ?? createKindRaw,
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
      comments: this.prevComments,
      this: this.prev?.text.toUpperCase(),
      expression: this.parseString(),
    });
  }

  parseSequenceProperties (): SequencePropertiesExpr | undefined {
    const seq = new SequencePropertiesExpr({});

    const options: Expression[] = [];
    const index = this.index;

    while (this.curr) {
      this.match(TokenType.COMMA);
      if (this.matchTextSeq('INCREMENT')) {
        this.matchTextSeq('BY');
        this.matchTextSeq('=');
        seq.setArgKey('increment', this.parseTerm());
      } else if (this.matchTextSeq('MINVALUE')) {
        seq.setArgKey('minvalue', this.parseTerm());
      } else if (this.matchTextSeq('MAXVALUE')) {
        seq.setArgKey('maxvalue', this.parseTerm());
      } else if (this.match(TokenType.START_WITH) || this.matchTextSeq('START')) {
        this.matchTextSeq('=');
        seq.setArgKey('start', this.parseTerm());
      } else if (this.matchTextSeq('CACHE')) {
        // T-SQL allows empty CACHE which is initialized dynamically
        seq.setArgKey('cache', this.parseNumber() || true);
      } else if (this.matchTextSeq(['OWNED', 'BY'])) {
        // "OWNED BY NONE" is the default
        seq.setArgKey('owned', this.matchTextSeq('NONE') ? undefined : this.parseColumn());
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
    return this.index === index ? undefined : seq;
  }

  parsePropertyBefore (): Expression | Expression[] | undefined {
    // only used for teradata currently
    this.match(TokenType.COMMA);

    const kwargs: Record<string, boolean | string> = {
      no: this.matchTextSeq('NO') || false,
      dual: this.matchTextSeq('DUAL') || false,
      before: this.matchTextSeq('BEFORE') || false,
      default: this.matchTextSeq('DEFAULT') || false,
      local: (this.matchTextSeq('LOCAL') && 'LOCAL')
        || (this.matchTextSeq(['NOT', 'LOCAL']) && 'NOT LOCAL')
        || false,
      after: this.matchTextSeq('AFTER') || false,
      minimum: this.matchTexts(['MIN', 'MINIMUM']) || false,
      maximum: this.matchTexts(['MAX', 'MAXIMUM']) || false,
    };

    if (this.matchTexts(Object.keys(this._constructor.PROPERTY_PARSERS))) {
      const parser = this._constructor.PROPERTY_PARSERS[(this.prev?.text ?? '').toUpperCase()];
      try {
        const filteredKwargs = Object.fromEntries(
          Object.entries(kwargs).filter(([, v]) => v),
        );
        const result = parser.call(this, filteredKwargs);
        return result ?? undefined;
      } catch {
        this.raiseError(`Cannot parse property '${this.prev?.text ?? ''}'`);
      }
    }

    return undefined;
  }

  parseWrappedProperties (): Expression[] {
    return this.parseWrappedCsv(() => this.parseProperty());
  }

  parseProperty (): Expression | Expression[] | undefined {
    if (this.matchTexts(Object.keys(this._constructor.PROPERTY_PARSERS))) {
      const result = this._constructor.PROPERTY_PARSERS[(this.prev?.text ?? '').toUpperCase()].call(this);
      return result ?? undefined;
    }

    if (this.match(TokenType.DEFAULT) && this.matchTexts(Object.keys(this._constructor.PROPERTY_PARSERS))) {
      const result = this._constructor.PROPERTY_PARSERS[(this.prev?.text ?? '').toUpperCase()].call(this, { default: true });
      return result ?? undefined;
    }

    if (this.matchTextSeq(['COMPOUND', 'SORTKEY'])) {
      return this.parseSortkey({ compound: true });
    }

    if (this.matchTextSeq(['SQL', 'SECURITY'])) {
      return this.expression(
        SqlSecurityPropertyExpr,
        { this: this.matchTexts(['DEFINER', 'INVOKER']) && (this.prev?.text ?? '').toUpperCase() },
      );
    }

    const index = this.index;

    const seqProps = this.parseSequenceProperties();
    if (seqProps) {
      return seqProps;
    }

    this.retreat(index);
    let key = this.parseColumn();

    if (!this.match(TokenType.EQ)) {
      this.retreat(index);
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
    if (this.matchTextSeq('BY')) {
      return this.expression(StorageHandlerPropertyExpr, { this: this.parseVarOrString() });
    }

    this.match(TokenType.ALIAS);
    const inputFormat = (this.matchTextSeq('INPUTFORMAT') || undefined) && this.parseString();
    const outputFormat = (this.matchTextSeq('OUTPUTFORMAT') || undefined) && this.parseString();

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
    if (field instanceof IdentifierExpr && !field.args.quoted) {
      return var_(field);
    }

    return field;
  }

  parsePropertyAssignment<E extends Expression> (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expClass: new (args: any) => E,
    kwargs?: Record<string, unknown>,
  ): E {
    this.match(TokenType.EQ);
    this.match(TokenType.ALIAS);

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
      if (prop === undefined || prop === null || (Array.isArray(prop) && prop.length === 0)) {
        break;
      }
      const list = Array.isArray(prop) ? prop : [prop];
      for (const p of list) {
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
        protection: this.matchTextSeq('PROTECTION'),
      },
    );
  }

  parseSecurity (): SecurityPropertyExpr | undefined {
    if (this.matchTexts([
      'NONE',
      'DEFINER',
      'INVOKER',
    ])) {
      const securitySpecifier = this.prev?.text.toUpperCase();
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
    if (2 <= this.index) {
      preVolatileToken = this.tokens[this.index - 2];
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

  parseSystemVersioningProperty (options: { with?: boolean } = {}): WithSystemVersioningPropertyExpr {
    const { with: with_ = false } = options;
    this.match(TokenType.EQ);
    const prop = this.expression(
      WithSystemVersioningPropertyExpr,
      {
        on: true,
        with: with_,
      },
    );

    if (this.matchTextSeq('OFF')) {
      prop.setArgKey('on', false);
      return prop;
    }

    this.match(TokenType.ON);
    if (this.match(TokenType.L_PAREN)) {
      while (this.curr && !this.match(TokenType.R_PAREN)) {
        if (this.matchTextSeq(['HISTORY_TABLE', '='])) {
          prop.setArgKey('this', this.parseTableParts());
        } else if (this.matchTextSeq(['DATA_CONSISTENCY_CHECK', '='])) {
          this.advance();
          prop.setArgKey('dataConsistency', this.prev?.text.toUpperCase());
        } else if (this.matchTextSeq(['HISTORY_RETENTION_PERIOD', '='])) {
          prop.setArgKey('retentionPeriod', this.parseRetentionPeriod());
        }

        this.match(TokenType.COMMA);
      }
    }

    return prop;
  }

  parseDataDeletionProperty (): DataDeletionPropertyExpr {
    this.match(TokenType.EQ);
    const on = this.matchTextSeq('ON') || !this.matchTextSeq('OFF');
    const prop = this.expression(DataDeletionPropertyExpr, { on });

    if (this.match(TokenType.L_PAREN)) {
      while (this.curr && !this.match(TokenType.R_PAREN)) {
        if (this.matchTextSeq(['FILTER_COLUMN', '='])) {
          prop.setArgKey('filterColumn', this.parseColumn());
        } else if (this.matchTextSeq(['RETENTION_PERIOD', '='])) {
          prop.setArgKey('retentionPeriod', this.parseRetentionPeriod());
        }

        this.match(TokenType.COMMA);
      }
    }

    return prop;
  }

  parseDistributedProperty (): DistributedByPropertyExpr {
    let kind = 'HASH';
    let expressions: Expression[] | undefined;
    if (this.matchTextSeq(['BY', 'HASH'])) {
      expressions = this.parseWrappedCsv(() => this.parseIdVar());
    } else if (this.matchTextSeq(['BY', 'RANDOM'])) {
      kind = 'RANDOM';
    }

    // If the BUCKETS keyword is not present, the number of buckets is AUTO
    let buckets: Expression | undefined;
    if (this.matchTextSeq('BUCKETS') && !this.matchTextSeq('AUTO')) {
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
    this.matchTextSeq('KEY');
    const expressions = this.parseWrappedIdVars();
    return this.expression(exprType, { expressions });
  }

  parseWithProperty (): Expression | Expression[] | undefined {
    if (this.matchTextSeq(['(', 'SYSTEM_VERSIONING'])) {
      const prop = this.parseSystemVersioningProperty({ with: true });
      this.matchRParen();
      return prop;
    }

    if (this.match(TokenType.L_PAREN, { advance: false })) {
      return this.parseWrappedProperties();
    }

    if (this.matchTextSeq('JOURNAL')) {
      return this.parseWithjournaltable();
    }

    if (this.matchTexts(Object.keys(this._constructor.VIEW_ATTRIBUTES))) {
      return this.expression(ViewAttributePropertyExpr, { this: this.prev?.text.toUpperCase() });
    }

    if (this.matchTextSeq('DATA')) {
      return this.parseWithdata({ no: false });
    } else if (this.matchTextSeq(['NO', 'DATA'])) {
      return this.parseWithdata({ no: true });
    }

    if (this.match(TokenType.SERDE_PROPERTIES, { advance: false })) {
      return this.parseSerdeProperties({ with: true });
    }

    if (this.match(TokenType.SCHEMA)) {
      return this.expression(
        WithSchemaBindingPropertyExpr,
        { this: this.parseVarFromOptions(this._constructor.SCHEMA_BINDING_OPTIONS) },
      );
    }

    if (this.matchTexts(Object.keys(this._constructor.PROCEDURE_OPTIONS), { advance: false })) {
      return this.expression(
        WithProcedureOptionsExpr,
        { expressions: this.parseCsv(() => this.parseProcedureOption()) },
      );
    }

    if (!this.next) {
      return undefined;
    }

    return this.parseWithIsolatedLoading();
  }

  parseProcedureOption (): Expression | undefined {
    if (this.matchTextSeq(['EXECUTE', 'AS'])) {
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
    this.match(TokenType.EQ);

    const user = this.parseIdVar();
    this.match(TokenType.PARAMETER);
    const host = this.parseIdVar() || (this.match(TokenType.MOD) && this.prev?.text);

    if (!user || !host) {
      return undefined;
    }

    return new DefinerPropertyExpr({ this: `${user}@${host}` });
  }

  parseWithjournaltable (): WithJournalTablePropertyExpr {
    this.match(TokenType.TABLE);
    this.match(TokenType.EQ);
    return this.expression(WithJournalTablePropertyExpr, { this: this.parseTableParts() });
  }

  parseLog (options: { no?: boolean } = {}): LogPropertyExpr {
    const { no = false } = options;
    return this.expression(LogPropertyExpr, { no });
  }

  parseJournal (kwargs?: Record<string, unknown>): JournalPropertyExpr {
    return this.expression(JournalPropertyExpr, kwargs);
  }

  parseChecksum (): ChecksumPropertyExpr {
    this.match(TokenType.EQ);

    let on: boolean | undefined;
    if (this.match(TokenType.ON)) {
      on = true;
    } else if (this.matchTextSeq('OFF')) {
      on = false;
    }

    return this.expression(ChecksumPropertyExpr, {
      on,
      default: this.match(TokenType.DEFAULT),
    });
  }

  parseCluster (options: { wrapped?: boolean } = {}): ClusterExpr {
    const {
      wrapped = false,
    } = options;
    return this.expression(
      ClusterExpr,
      {
        expressions: wrapped
          ? this.parseWrappedCsv(() => this.parseOrdered())
          : this.parseCsv(() => this.parseOrdered()),
      },
    );
  }

  parseClusteredBy (): ClusteredByPropertyExpr {
    this.matchTextSeq('BY');

    this.matchLParen();
    const expressions = this.parseCsv(() => this.parseColumn());
    this.matchRParen();

    let sortedBy: Expression[] | undefined;
    if (this.matchTextSeq(['SORTED', 'BY'])) {
      this.matchLParen();
      sortedBy = this.parseCsv(() => this.parseOrdered());
      this.matchRParen();
    }

    this.match(TokenType.INTO);
    const buckets = this.parseNumber();
    this.matchTextSeq('BUCKETS');

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
    if (!this.matchTextSeq('GRANTS')) {
      this.retreat(this.index - 1);
      return undefined;
    }

    return this.expression(CopyGrantsPropertyExpr, {});
  }

  parseFreespace (): FreespacePropertyExpr {
    this.match(TokenType.EQ);
    return this.expression(
      FreespacePropertyExpr,
      {
        this: this.parseNumber(),
        percent: this.match(TokenType.PERCENT),
      },
    );
  }

  parseMergeBlockRatio (options: {
    no?: boolean;
    default?: boolean;
  } = {}): MergeBlockRatioPropertyExpr {
    const {
      no = false,
      default: default_ = false,
    } = options;

    if (this.match(TokenType.EQ)) {
      return this.expression(
        MergeBlockRatioPropertyExpr,
        {
          this: this.parseNumber(),
          percent: this.match(TokenType.PERCENT),
        },
      );
    }

    return this.expression(MergeBlockRatioPropertyExpr, {
      no,
      default: default_,
    });
  }

  parseDataBlocksize (options: {
    default?: boolean;
    minimum?: boolean;
    maximum?: boolean;
  } = {}): DataBlocksizePropertyExpr {
    const {
      default: default_,
      minimum,
      maximum,
    } = options;
    this.match(TokenType.EQ);
    const size = this.parseNumber();

    let units: string | undefined;
    if (this.matchTexts([
      'BYTES',
      'KBYTES',
      'KILOBYTES',
    ])) {
      units = this.prev?.text ?? '';
    }

    return this.expression(
      DataBlocksizePropertyExpr,
      {
        size,
        units,
        default: default_,
        minimum,
        maximum,
      },
    );
  }

  parseBlockCompression (): BlockCompressionPropertyExpr {
    this.match(TokenType.EQ);
    const always = this.matchTextSeq('ALWAYS') || undefined;
    const manual = this.matchTextSeq('MANUAL') || undefined;
    const never = this.matchTextSeq('NEVER') || undefined;
    const default_ = this.matchTextSeq('DEFAULT') || undefined;

    let autotemp: Expression | undefined;
    if (this.matchTextSeq('AUTOTEMP')) {
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

  parseWithIsolatedLoading (): IsolatedLoadingPropertyExpr | undefined {
    const index = this.index;
    const no = this.matchTextSeq('NO') || undefined;
    const concurrent = this.matchTextSeq('CONCURRENT') || undefined;

    if (!this.matchTextSeq(['ISOLATED', 'LOADING'])) {
      this.retreat(index);
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
    if (this.match(TokenType.TABLE)) {
      kind = 'TABLE';
    } else if (this.match(TokenType.VIEW)) {
      kind = 'VIEW';
    } else if (this.match(TokenType.ROW)) {
      kind = 'ROW';
    } else if (this.matchTextSeq('DATABASE')) {
      kind = 'DATABASE';
    }

    let thisExpr: Expression | undefined;
    if (kind === 'DATABASE' || kind === 'TABLE' || kind === 'VIEW') {
      thisExpr = this.parseTableParts();
    }

    let forOrIn: string | undefined;
    if (this.match(TokenType.FOR)) {
      forOrIn = 'FOR';
    } else if (this.match(TokenType.IN)) {
      forOrIn = 'IN';
    }

    let lockType: string | undefined;
    if (this.matchTextSeq('ACCESS')) {
      lockType = 'ACCESS';
    } else if (this.matchTexts(['EXCL', 'EXCLUSIVE'])) {
      lockType = 'EXCLUSIVE';
    } else if (this.matchTextSeq('SHARE')) {
      lockType = 'SHARE';
    } else if (this.matchTextSeq('READ')) {
      lockType = 'READ';
    } else if (this.matchTextSeq('WRITE')) {
      lockType = 'WRITE';
    } else if (this.matchTextSeq('CHECKSUM')) {
      lockType = 'CHECKSUM';
    }

    const override = this.matchTextSeq('OVERRIDE') || undefined;

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
    if (this.match(TokenType.PARTITION_BY)) {
      return this.parseCsv(() => this.parseDisjunction());
    }
    return [];
  }

  parsePartitionBoundSpec (): PartitionBoundSpecExpr {
    const parsePartitionBoundExpr = (): Expression | undefined => {
      if (this.matchTextSeq('MINVALUE')) {
        return var_('MINVALUE');
      }
      if (this.matchTextSeq('MAXVALUE')) {
        return var_('MAXVALUE');
      }
      return this.parseBitwise();
    };

    let thisExpr: Expression | Expression[] | undefined;
    let expression: Expression | undefined;
    let fromExpressions: Expression[] | undefined;
    let toExpressions: Expression[] | undefined;

    if (this.match(TokenType.IN)) {
      thisExpr = this.parseWrappedCsv(() => this.parseBitwise());
    } else if (this.match(TokenType.FROM)) {
      fromExpressions = this.parseWrappedCsv(parsePartitionBoundExpr);
      this.matchTextSeq('TO');
      toExpressions = this.parseWrappedCsv(parsePartitionBoundExpr);
    } else if (this.matchTextSeq([
      'WITH',
      '(',
      'MODULUS',
    ])) {
      thisExpr = this.parseNumber();
      this.matchTextSeq([',', 'REMAINDER']);
      expression = this.parseNumber();
      this.matchRParen();
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
    if (!this.matchTextSeq('OF')) {
      this.retreat(this.index - 1);
      return undefined;
    }

    const thisExpr = this.parseTable({ schema: true });

    let expression: VarExpr | PartitionBoundSpecExpr;
    if (this.match(TokenType.DEFAULT)) {
      expression = var_('DEFAULT');
    } else if (this.matchTextSeq(['FOR', 'VALUES'])) {
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
    this.match(TokenType.EQ);
    return this.expression(
      PartitionedByPropertyExpr,
      { this: this.parseSchema() || this.parseBracket(this.parseField()) },
    );
  }

  parseWithdata (options: { no?: boolean } = {}): WithDataPropertyExpr {
    let statistics: boolean | undefined;
    if (this.matchTextSeq(['AND', 'STATISTICS'])) {
      statistics = true;
    } else if (this.matchTextSeq([
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
    if (this.matchTextSeq('SQL')) {
      return this.expression(SqlReadWritePropertyExpr, { this: 'CONTAINS SQL' });
    }
    return undefined;
  }

  parseModifiesProperty (): SqlReadWritePropertyExpr | undefined {
    if (this.matchTextSeq(['SQL', 'DATA'])) {
      return this.expression(SqlReadWritePropertyExpr, { this: 'MODIFIES SQL DATA' });
    }
    return undefined;
  }

  parseNoProperty (): Expression | undefined {
    if (this.matchTextSeq(['PRIMARY', 'INDEX'])) {
      return new NoPrimaryIndexPropertyExpr({});
    }
    if (this.matchTextSeq('SQL')) {
      return this.expression(SqlReadWritePropertyExpr, { this: 'NO SQL' });
    }
    return undefined;
  }

  parseOnProperty (): Expression | undefined {
    if (this.matchTextSeq([
      'COMMIT',
      'PRESERVE',
      'ROWS',
    ])) {
      return new OnCommitPropertyExpr({});
    }
    if (this.matchTextSeq([
      'COMMIT',
      'DELETE',
      'ROWS',
    ])) {
      return new OnCommitPropertyExpr({ delete: true });
    }
    return this.expression(OnPropertyExpr, { this: this.parseSchema({ this: this.parseIdVar() }) });
  }

  parseReadsProperty (): SqlReadWritePropertyExpr | undefined {
    if (this.matchTextSeq(['SQL', 'DATA'])) {
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
    while (this.matchTexts(['INCLUDING', 'EXCLUDING'])) {
      const thisText = (this.prev?.text ?? '').toUpperCase();

      const idVar = this.parseIdVar();
      if (!idVar) {
        return undefined;
      }

      const thisId = idVar.args.this;
      options.push(
        this.expression(PropertyExpr, {
          this: thisText,
          value: var_(typeof thisId === 'string' ? thisId.toUpperCase() : ''),
        }),
      );
    }

    return this.expression(LikePropertyExpr, {
      this: table,
      expressions: options,
    });
  }

  parseSortkey (options: { compound?: boolean } = {}): SortKeyPropertyExpr {
    return this.expression(
      SortKeyPropertyExpr,
      {
        this: this.parseWrappedIdVars(),
        compound: options?.compound,
      },
    );
  }

  parseCharacterSet (options: { default?: boolean } = {}): CharacterSetPropertyExpr {
    this.match(TokenType.EQ);
    return this.expression(
      CharacterSetPropertyExpr,
      {
        this: this.parseVarOrString(),
        default: options?.default,
      },
    );
  }

  parseRemoteWithConnection (): RemoteWithConnectionModelPropertyExpr {
    this.matchTextSeq(['WITH', 'CONNECTION']);
    return this.expression(
      RemoteWithConnectionModelPropertyExpr,
      { this: this.parseTableParts() },
    );
  }

  parseReturns (): ReturnsPropertyExpr {
    let value: Expression | undefined;
    let null_: boolean | undefined;
    const isTable = this.match(TokenType.TABLE) || undefined;

    if (isTable) {
      if (this.match(TokenType.LT)) {
        value = this.expression(
          SchemaExpr,
          {
            this: 'TABLE',
            expressions: this.parseCsv(() => this.parseStructTypes()),
          },
        );
        if (!this.match(TokenType.GT)) {
          this.raiseError('Expecting >');
        }
      } else {
        value = this.parseSchema({ this: var_('TABLE') });
      }
    } else if (this.matchTextSeq([
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
    const kind = (this.matchSet(this._constructor.CREATABLES) || undefined) && this.prev?.text;
    let style = (this.matchTexts(Array.from(this._constructor.DESCRIBE_STYLES)) || undefined) && this.prev?.text.toUpperCase();
    if (this.match(TokenType.DOT)) {
      style = undefined;
      this.retreat(this.index - 2);
    }

    const format = this.match(TokenType.FORMAT, { advance: false }) ? this.parseProperty() : undefined;

    let thisExpr: Expression | undefined;
    if (this.matchSet(new Set(Object.keys(this._constructor.STATEMENT_PARSERS)) as Set<TokenType>, { advance: false })) {
      thisExpr = this.parseStatement();
    } else {
      thisExpr = this.parseTable({ schema: true });
    }

    const properties = this.parseProperties();
    const expressions = properties?.args.expressions;
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
        asJson: this.matchTextSeq(['AS', 'JSON']),
      },
    );
  }

  parseMultitableInserts (comments?: string[]): MultitableInsertsExpr {
    const kind = (this.prev?.text ?? '').toUpperCase();
    const expressions: Expression[] = [];

    const parseConditionalInsert = (): ConditionalInsertExpr | undefined => {
      let expression: Expression | undefined;
      if (this.match(TokenType.WHEN)) {
        expression = this.parseDisjunction();
        this.match(TokenType.THEN);
      }

      const else_ = this.match(TokenType.ELSE) || undefined;

      if (!this.match(TokenType.INTO)) {
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
    const overwrite = this.match(TokenType.OVERWRITE) || undefined;
    const ignore = this.match(TokenType.IGNORE) || undefined;
    const local = this.matchTextSeq('LOCAL') || undefined;
    let alternative: string | undefined;
    let isFunction: boolean | undefined;

    let thisExpr: Expression | undefined;
    if (this.matchTextSeq('DIRECTORY')) {
      thisExpr = this.expression(
        DirectoryExpr,
        {
          this: this.parseVarOrString(),
          local,
          rowFormat: this.parseRowFormat({ matchRow: true }),
        },
      );
    } else {
      if (this.matchSet(new Set([TokenType.FIRST, TokenType.ALL]))) {
        comments.push(...ensureList(this.prevComments));
        return this.parseMultitableInserts(comments);
      }

      if (this.match(TokenType.OR)) {
        alternative = (this.matchTexts(Array.from(this._constructor.INSERT_ALTERNATIVES)) && this.prev?.text) || undefined;
      }

      this.match(TokenType.INTO);
      comments.push(...ensureList(this.prevComments));
      this.match(TokenType.TABLE);
      isFunction = this.match(TokenType.FUNCTION) || undefined;

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
        stored: this.matchTextSeq('STORED') && this.parseStored(),
        byName: this.matchTextSeq(['BY', 'NAME']),
        exists: this.parseExists(),
        where: this.matchPair(TokenType.REPLACE, TokenType.WHERE) && this.parseDisjunction(),
        partition: this.match(TokenType.PARTITION_BY) && this.parsePartitionedBy(),
        settings: this.matchTextSeq('SETTINGS') && this.parseSettingsProperty(),
        default: this.matchTextSeq(['DEFAULT', 'VALUES']),
        expression: this.parseDerivedTableValues() || this.parseDdlSelect(),
        conflict: this.parseOnConflict(),
        returning: returning || this.parseReturning(),
        overwrite,
        alternative,
        ignore,
        source: this.match(TokenType.TABLE) && this.parseTable(),
      },
    );
  }

  parseInsertTable (): Expression | undefined {
    const thisExpr = this.parseTable({
      schema: true,
      parsePartition: true,
    });
    if (thisExpr instanceof TableExpr && this.match(TokenType.ALIAS, { advance: false })) {
      thisExpr.setArgKey('alias', this.parseTableAlias());
    }
    return thisExpr;
  }

  parseKill (): KillExpr {
    const kind = this.matchTexts(['CONNECTION', 'QUERY']) ? var_(this.prev?.text ?? '') : undefined;

    return this.expression(
      KillExpr,
      {
        this: this.parsePrimary(),
        kind,
      },
    );
  }

  parseOnConflict (): OnConflictExpr | undefined {
    const conflict = this.matchTextSeq(['ON', 'CONFLICT']) || undefined;
    const duplicate = this.matchTextSeq([
      'ON',
      'DUPLICATE',
      'KEY',
    ]) || undefined;

    if (!conflict && !duplicate) {
      return undefined;
    }

    let conflictKeys: Expression[] | undefined;
    let constraint: Expression | undefined;

    if (conflict) {
      if (this.matchTextSeq(['ON', 'CONSTRAINT'])) {
        constraint = this.parseIdVar();
      } else if (this.match(TokenType.L_PAREN)) {
        conflictKeys = this.parseCsv(() => this.parseIdVar());
        this.matchRParen();
      }
    }

    const indexPredicate = this.parseWhere();

    const action = this.parseVarFromOptions(this._constructor.CONFLICT_ACTIONS);
    let expressions: Expression[] | undefined;
    if (this.prev?.tokenType === TokenType.UPDATE) {
      this.match(TokenType.SET);
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
    if (!this.match(TokenType.RETURNING)) {
      return undefined;
    }
    return this.expression(
      ReturningExpr,
      {
        expressions: this.parseCsv(() => this.parseExpression()),
        into: this.match(TokenType.INTO) && this.parseTablePart(),
      },
    );
  }

  parseRow (): RowFormatSerdePropertyExpr | RowFormatDelimitedPropertyExpr | undefined {
    if (!this.match(TokenType.FORMAT)) {
      return undefined;
    }
    return this.parseRowFormat();
  }

  parseSerdeProperties (options: { with?: boolean } = {}): SerdePropertiesExpr | undefined {
    const index = this.index;
    const with_ = options?.with || this.matchTextSeq('WITH');

    if (!this.match(TokenType.SERDE_PROPERTIES)) {
      this.retreat(index);
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

  parseRowFormat (options: {
    matchRow?: boolean;
  } = {}): RowFormatSerdePropertyExpr | RowFormatDelimitedPropertyExpr | undefined {
    if (options?.matchRow && !this.matchPair(TokenType.ROW, TokenType.FORMAT)) {
      return undefined;
    }

    if (this.matchTextSeq('SERDE')) {
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

    this.matchTextSeq('DELIMITED');

    const kwargs: Record<string, Expression | undefined> = {};

    if (this.matchTextSeq([
      'FIELDS',
      'TERMINATED',
      'BY',
    ])) {
      kwargs.fields = this.parseString();
      if (this.matchTextSeq(['ESCAPED', 'BY'])) {
        kwargs.escaped = this.parseString();
      }
    }
    if (this.matchTextSeq([
      'COLLECTION',
      'ITEMS',
      'TERMINATED',
      'BY',
    ])) {
      kwargs.collectionItems = this.parseString();
    }
    if (this.matchTextSeq([
      'MAP',
      'KEYS',
      'TERMINATED',
      'BY',
    ])) {
      kwargs.mapKeys = this.parseString();
    }
    if (this.matchTextSeq([
      'LINES',
      'TERMINATED',
      'BY',
    ])) {
      kwargs.lines = this.parseString();
    }
    if (this.matchTextSeq([
      'NULL',
      'DEFINED',
      'AS',
    ])) {
      kwargs.null = this.parseString();
    }

    return this.expression(RowFormatDelimitedPropertyExpr, kwargs);
  }

  parseSelect (options: {
    nested?: boolean;
    table?: boolean;
    parseSubqueryAlias?: boolean;
    parseSetOperation?: boolean;
    consumePipe?: boolean;
    from?: FromExpr;
  } = {}): Expression | undefined {
    const {
      nested = false,
      table = false,
      parseSubqueryAlias = true,
      parseSetOperation = true,
      consumePipe = true,
      from,
    } = options;

    let query = this.parseSelectQuery({
      nested,
      table,
      parseSubqueryAlias,
      parseSetOperation,
    });

    if (consumePipe && this.match(TokenType.PIPE_GT, { advance: false })) {
      if (!query && from) {
        query = select('*').from(from);
      }
      if (query instanceof QueryExpr) {
        query = this.parsePipeSyntaxQuery(query);
        query = (query && table) ? (query as SelectExpr).subquery(undefined, { copy: false }) : query;
      }
    }

    return query;
  }

  parseSelectQuery (options: {
    nested?: boolean;
    table?: boolean;
    parseSubqueryAlias?: boolean;
    parseSetOperation?: boolean;
  } = {}): Expression | undefined {
    const {
      nested = false,
      table = false,
      parseSubqueryAlias = true,
      parseSetOperation = true,
    } = options;

    const cte = this.parseWith();
    let thisExpr: Expression | undefined;

    if (cte) {
      thisExpr = this.parseStatement();

      if (!thisExpr) {
        this.raiseError('Failed to parse any statement following CTE');
        return cte;
      }

      while (thisExpr instanceof SubqueryExpr && thisExpr.isWrapper) {
        thisExpr = thisExpr.args.this;
      }

      if (thisExpr?._constructor.availableArgs.has('with')) {
        thisExpr.setArgKey('with', cte);
      } else {
        this.raiseError(`${thisExpr?._constructor.key || 'Unknown expression'} does not support CTE`);
        thisExpr = cte;
      }

      return thisExpr;
    }

    // duckdb supports leading with FROM x
    let from: FromExpr | undefined;
    if (this.match(TokenType.FROM, { advance: false })) {
      from = this.parseFrom({
        joins: true,
        consumePipe: true,
      }) as FromExpr | undefined;
    }

    if (this.match(TokenType.SELECT)) {
      const comments = this.prevComments;

      const hint = this.parseHint();

      let all: boolean | undefined;
      let distinct: DistinctExpr | undefined;
      if (this.next && this.next.tokenType !== TokenType.DOT) {
        all = this.match(TokenType.ALL) || undefined;
        distinct = this.matchSet(this._constructor.DISTINCT_TOKENS) ? new DistinctExpr() : undefined;
      }

      const kind = (
        this.match(TokenType.ALIAS)
        && this.matchTexts(['STRUCT', 'VALUE'])
        && this.prev?.text.toUpperCase()
      ) || undefined;

      if (distinct) {
        distinct = this.expression(
          DistinctExpr,
          { on: this.match(TokenType.ON) ? this.parseValue({ values: false }) : undefined },
        );
      }

      if (all && distinct) {
        this.raiseError('Cannot specify both ALL and DISTINCT after SELECT');
      }

      const operationModifiers: Expression[] = [];
      while (this.curr && this.matchTexts(Array.from(this._constructor.OPERATION_MODIFIERS))) {
        operationModifiers.push(var_((this.prev?.text ?? '').toUpperCase()));
      }

      const limit = this.parseLimit(undefined, { top: true });
      const projections = this.parseProjections();

      thisExpr = this.expression(
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
    } else if ((table || nested) && this.match(TokenType.L_PAREN)) {
      thisExpr = this.parseWrappedSelect({ table });

      // We return early here so that the UNION isn't attached to the subquery by the
      // following call to _parse_set_operations, but instead becomes the parent node
      this.matchRParen();
      return this.parseSubquery(thisExpr, { parseAlias: parseSubqueryAlias });
    } else if (this.match(TokenType.VALUES, { advance: false })) {
      thisExpr = this.parseDerivedTableValues();
    } else if (from) {
      return select('*').from(from.args.this, { copy: false });
    } else if (this.match(TokenType.SUMMARIZE)) {
      const table = this.match(TokenType.TABLE) || undefined;
      thisExpr = this.parseSelect() || this.parseString() || this.parseTable();
      return this.expression(SummarizeExpr, {
        this: thisExpr,
        table,
      });
    } else if (this.match(TokenType.DESCRIBE)) {
      return this.parseDescribe();
    } else {
      thisExpr = undefined;
    }

    return parseSetOperation ? this.parseSetOperations(thisExpr) : thisExpr;
  }

  parseRecursiveWithSearch (): RecursiveWithSearchExpr | undefined {
    this.matchTextSeq('SEARCH');

    const kind = (this.matchTexts(Array.from(this._constructor.RECURSIVE_CTE_SEARCH_KIND)) || undefined) && this.prev?.text.toUpperCase();

    if (!kind) {
      return undefined;
    }

    this.matchTextSeq(['FIRST', 'BY']);

    return this.expression(
      RecursiveWithSearchExpr,
      {
        kind,
        this: this.parseIdVar(),
        expression: this.matchTextSeq('SET') && this.parseIdVar(),
        using: this.matchTextSeq('USING') && this.parseIdVar(),
      },
    );
  }

  parseWith (options: { skipWithToken?: boolean } = {}): WithExpr | undefined {
    const {
      skipWithToken = false,
    } = options;
    if (!skipWithToken && !this.match(TokenType.WITH)) {
      return undefined;
    }

    const comments = this.prevComments;
    const recursive = this.match(TokenType.RECURSIVE) || undefined;

    let lastComments: string[] | undefined;
    const expressions: CteExpr[] = [];
    while (true) {
      const cte = this.parseCte();
      if (cte instanceof CteExpr) {
        expressions.push(cte);
        if (lastComments) {
          cte.addComments(lastComments);
        }
      }

      if (!this.match(TokenType.COMMA) && !this.match(TokenType.WITH)) {
        break;
      } else {
        this.match(TokenType.WITH);
      }

      lastComments = this.prevComments;
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

  parseCte (): CteExpr | undefined {
    const index = this.index;

    const aliasExpr = this.parseTableAlias({ aliasTokens: this._constructor.ID_VAR_TOKENS });
    if (!aliasExpr || !aliasExpr.args.this) {
      this.raiseError('Expected CTE to have alias');
    }

    const keyExpressions = this.matchTextSeq(['USING', 'KEY'])
      ? this.parseWrappedIdVars()
      : undefined;

    if (!this.match(TokenType.ALIAS) && !this._constructor.OPTIONAL_ALIAS_TOKEN_CTE) {
      this.retreat(index);
      return undefined;
    }

    const comments = this.prevComments;

    let materialized: boolean | undefined;
    if (this.matchTextSeq(['NOT', 'MATERIALIZED'])) {
      materialized = false;
    } else if (this.matchTextSeq('MATERIALIZED')) {
      materialized = true;
    }

    const cte = this.expression(
      CteExpr,
      {
        this: this.parseWrapped(() => this.parseStatement()),
        alias: aliasExpr,
        materialized,
        keyExpressions,
        comments,
      },
    );

    const values = cte.args.this;
    if (values instanceof ValuesExpr) {
      if (values.alias) {
        cte.setArgKey('this', select('*').from(values));
      } else {
        cte.setArgKey('this', select('*').from(
          alias(values, '_values', { table: true }),
        ));
      }
    }

    return cte;
  }

  parseTableAlias (options: {
    aliasTokens?: Set<TokenType>;
  } = {}): TableAliasExpr | undefined {
    const {
      aliasTokens,
    } = options;
    // In some dialects, LIMIT and OFFSET can act as both identifiers and keywords (clauses)
    // so this section tries to parse the clause version and if it fails, it treats the token
    // as an identifier (alias)
    if (this.canParseLimitOrOffset()) {
      return undefined;
    }

    const anyToken = this.match(TokenType.ALIAS);
    const alias = (
      this.parseIdVar({
        anyToken,
        tokens: aliasTokens || this._constructor.TABLE_ALIAS_TOKENS,
      })
      || this.parseStringAsIdentifier()
    );

    const index = this.index;
    let columns: Expression[] | undefined;
    if (this.match(TokenType.L_PAREN)) {
      columns = this.parseCsv(() => this.parseFunctionParameter());
      if (!columns || columns.length === 0) {
        this.retreat(index);
      } else {
        this.matchRParen();
      }
    } else {
      columns = undefined;
    }

    if (!alias && (!columns || columns.length === 0)) {
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
    options: { parseAlias?: boolean } = {},
  ): SubqueryExpr | undefined {
    const {
      parseAlias = true,
    } = options;
    if (!thisExpr) {
      return undefined;
    }

    return this.expression(
      SubqueryExpr,
      {
        this: thisExpr,
        pivots: this.parsePivots(),
        alias: parseAlias ? this.parseTableAlias() : undefined,
        sample: this.parseTableSample(),
      },
    );
  }

  protected implicitUnnestsToExplicit<E extends Expression> (thisExpr: E): E {
    const refs = new Set<string>();
    const args = thisExpr.args;

    // Add the FROM clause table to refs
    if ('from' in args) {
      const fromExpr = args['from'] as FromExpr;
      const fromExprThis = fromExpr?.args.this;
      if (fromExprThis) {
        const normalized = normalizeIdentifiers(fromExprThis.copy(), { dialect: this.dialect });
        refs.add(normalized.aliasOrName || '');
      }
    }

    // Process JOINs
    if ('joins' in args) {
      const joins: JoinExpr[] | undefined = args['joins'] ? filterInstanceOf(args['joins'] as Expression[], JoinExpr) : undefined;
      if (joins) {
        for (const join of joins) {
          const table = join.args.this;

          if (table instanceof TableExpr && !join.args.on) {
            // Normalize the table with maybeColumn meta flag
            const normalizedTable = table.copy();
            normalizedTable.meta['maybeColumn'] = true;
            const normalized = normalizeIdentifiers(normalizedTable, { dialect: this.dialect });

            // Check if the first part of the table name is in refs
            const parts = normalized.parts;
            if (parts && 0 < parts.length && refs.has(parts[0].name || '')) {
              const tableAsColumn = table.toColumn();
              if (tableAsColumn) {
                const unnest = new UnnestExpr({ expressions: [tableAsColumn] });

                // Convert Alias to TableAlias if needed
                if (table.args.alias instanceof TableAliasExpr && tableAsColumn.args.this instanceof Expression) {
                  tableAsColumn.replace(tableAsColumn.args.this);
                  const aliasThis = table.args.alias?.args.this;
                  const aliasArgs = {
                    table: aliasThis ? (typeof aliasThis === 'string' ? [aliasThis] : (assertIsInstanceOf(aliasThis, IdentifierExpr), [aliasThis])) : undefined,
                    copy: false,
                  };
                  alias(unnest, undefined, aliasArgs);
                }

                table.replace(unnest);
              }
            }

            refs.add(normalized.aliasOrName || '');
          }
        }
      }
    }

    return thisExpr;
  }

  parseQueryModifiers<E extends Expression> (thisExpr: E): E;
  parseQueryModifiers (thisExpr: undefined): undefined;
  parseQueryModifiers<E extends Expression> (thisExpr: E | undefined): E | undefined;
  parseQueryModifiers (thisExpr: Expression | undefined): Expression | undefined {
    if (thisExpr && this._constructor.MODIFIABLES.some((cls) => thisExpr instanceof cls)) {
      for (const join of this.parseJoins()) {
        thisExpr.append('joins', join);
      }

      let lateral = this.parseLateral();
      while (lateral) {
        thisExpr.append('laterals', lateral);
        lateral = this.parseLateral();
      }

      while (true) {
        if (this.matchSet(new Set(Object.keys(this._constructor.QUERY_MODIFIER_PARSERS)) as Set<TokenType>, { advance: false })) {
          const modifierToken = this.curr as Token;
          const parser = this._constructor.QUERY_MODIFIER_PARSERS[modifierToken.tokenType];
          const [key, expression] = parser?.call(this) || [];

          if (key !== undefined && expression !== undefined) {
            if (thisExpr.getArgKey(key)) {
              this.raiseError(
                `Found multiple '${modifierToken.text.toUpperCase()}' clauses`,
                modifierToken,
              );
            }

            thisExpr.setArgKey(key, expression);
            if (key === 'limit') {
              const limitExpression = expression as LimitExpr;
              const offset = (limitExpression as LimitExpr).args.offset;
              limitExpression.setArgKey('offset', undefined);

              if (offset) {
                const offsetExpr = new OffsetExpr({ expression: offset });
                thisExpr.setArgKey('offset', offsetExpr);

                const limitByExpressions = (expression as LimitExpr).args.expressions;
                limitExpression.setArgKey('expressions', undefined);
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
      thisExpr = this.implicitUnnestsToExplicit(thisExpr);
    }

    return thisExpr;
  }

  parseHintFallbackToString (): HintExpr | undefined {
    const start = this.curr;
    while (this.curr) {
      this.advance();
    }

    const end = this.tokens[this.index - 1];
    return new HintExpr({ expressions: [this.findSql(start, end)] });
  }

  parseHintFunctionCall (): Expression | undefined {
    return this.parseFunctionCall();
  }

  parseHintBody (): HintExpr | undefined {
    const startIndex = this.index;
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
    } catch {
      shouldFallbackToString = true;
    }

    if (shouldFallbackToString || this.curr) {
      this.retreat(startIndex);
      return this.parseHintFallbackToString();
    }

    return this.expression(HintExpr, { expressions: hints });
  }

  parseHint (): HintExpr | undefined {
    if (this.match(TokenType.HINT) && this.prevComments && 0 < this.prevComments.length) {
      // Parse hint from comment
      return maybeParse(
        this.prevComments[0],
        {
          into: ExpressionKey.HINT,
          dialect: this.dialect,
        },
      ) as HintExpr;
    }

    return undefined;
  }

  parseInto (): IntoExpr | undefined {
    if (!this.match(TokenType.INTO)) {
      return undefined;
    }

    const temp = this.match(TokenType.TEMPORARY) || undefined;
    const unlogged = this.matchTextSeq('UNLOGGED') || undefined;
    this.match(TokenType.TABLE);

    return this.expression(
      IntoExpr,
      {
        this: this.parseTable({ schema: true }),
        temporary: temp,
        unlogged,
      },
    );
  }

  parseFrom (options: {
    joins?: boolean;
    skipFromToken?: boolean;
    consumePipe?: boolean;
  } = {}): FromExpr | undefined {
    const {
      joins = false,
      skipFromToken = false,
      consumePipe = false,
    } = options;
    if (!skipFromToken && !this.match(TokenType.FROM)) {
      return undefined;
    }

    return this.expression(
      FromExpr,
      {
        comments: this.prevComments,
        this: this.parseTable({
          joins,
          consumePipe,
        }),
      },
    );
  }

  parseMatchRecognizeMeasure (): MatchRecognizeMeasureExpr {
    return this.expression(
      MatchRecognizeMeasureExpr,
      {
        windowFrame: this.matchTexts(['FINAL', 'RUNNING']) && (this.prev?.text ?? '').toUpperCase(),
        this: this.parseExpression(),
      },
    );
  }

  parseMatchRecognize (): MatchRecognizeExpr | undefined {
    if (!this.match(TokenType.MATCH_RECOGNIZE)) {
      return undefined;
    }

    this.matchLParen();

    const partition = this.parsePartitionBy();
    const order = this.parseOrder();

    const measures = this.matchTextSeq('MEASURES')
      ? this.parseCsv(() => this.parseMatchRecognizeMeasure())
      : undefined;

    let rows: VarExpr | undefined;
    if (this.matchTextSeq([
      'ONE',
      'ROW',
      'PER',
      'MATCH',
    ])) {
      rows = var_('ONE ROW PER MATCH');
    } else if (this.matchTextSeq([
      'ALL',
      'ROWS',
      'PER',
      'MATCH',
    ])) {
      let text = 'ALL ROWS PER MATCH';
      if (this.matchTextSeq([
        'SHOW',
        'EMPTY',
        'MATCHES',
      ])) {
        text += ' SHOW EMPTY MATCHES';
      } else if (this.matchTextSeq([
        'OMIT',
        'EMPTY',
        'MATCHES',
      ])) {
        text += ' OMIT EMPTY MATCHES';
      } else if (this.matchTextSeq([
        'WITH',
        'UNMATCHED',
        'ROWS',
      ])) {
        text += ' WITH UNMATCHED ROWS';
      }
      rows = var_(text);
    }

    let after: VarExpr | undefined;
    if (this.matchTextSeq([
      'AFTER',
      'MATCH',
      'SKIP',
    ])) {
      let text = 'AFTER MATCH SKIP';
      if (this.matchTextSeq([
        'PAST',
        'LAST',
        'ROW',
      ])) {
        text += ' PAST LAST ROW';
      } else if (this.matchTextSeq([
        'TO',
        'NEXT',
        'ROW',
      ])) {
        text += ' TO NEXT ROW';
      } else if (this.matchTextSeq(['TO', 'FIRST'])) {
        this.advance();
        text += ` TO FIRST ${this.prev?.text}`;
      } else if (this.matchTextSeq(['TO', 'LAST'])) {
        this.advance();
        text += ` TO LAST ${this.prev?.text}`;
      }
      after = var_(text);
    }

    let pattern: VarExpr | undefined;
    if (this.matchTextSeq('PATTERN')) {
      this.matchLParen();

      if (!this.curr) {
        this.raiseError('Expecting )', this.curr);
      }

      let paren = 1;
      const start = this.curr;
      let end = this.prev;

      while (this.curr && 0 < paren) {
        if (this.curr.tokenType === TokenType.L_PAREN) {
          paren++;
        }
        if (this.curr.tokenType === TokenType.R_PAREN) {
          paren--;
        }

        end = this.prev as Token;
        this.advance();
      }

      if (0 < paren) {
        this.raiseError('Expecting )', this.curr);
      }

      pattern = var_(this.findSql(start, end));
    }

    const define = this.matchTextSeq('DEFINE')
      ? this.parseCsv(() => this.parseNameAsExpression())
      : undefined;

    this.matchRParen();

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
    let crossApply: boolean | undefined = this.matchPair(TokenType.CROSS, TokenType.APPLY) || undefined;
    if (!crossApply && this.matchPair(TokenType.OUTER, TokenType.APPLY)) {
      crossApply = false;
    }

    let thisExpr: Expression | undefined;
    let view: boolean | undefined;
    let outer: boolean | undefined;

    if (crossApply) {
      thisExpr = this.parseSelect({ table: true });
      view = undefined;
      outer = undefined;
    } else if (this.match(TokenType.LATERAL)) {
      thisExpr = this.parseSelect({ table: true });
      view = this.match(TokenType.VIEW) || undefined;
      outer = this.match(TokenType.OUTER) || undefined;
    } else {
      return undefined;
    }

    if (!thisExpr) {
      thisExpr = (
        this.parseUnnest()
        || this.parseFunction()
        || this.parseIdVar({ anyToken: false })
      );

      while (this.match(TokenType.DOT)) {
        const expression = this.parseFunction() || this.parseIdVar({ anyToken: false });
        thisExpr = new DotExpr({
          this: thisExpr,
          expression,
        });
      }
    }

    let ordinality: boolean | undefined;
    let tableAlias: TableAliasExpr | undefined;

    if (view) {
      const table = this.parseIdVar({ anyToken: false });
      const columns = this.match(TokenType.ALIAS)
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
      tableAlias = thisExpr.args.alias?.pop() as TableAliasExpr | undefined;
    } else {
      ordinality = this.matchPair(TokenType.WITH, TokenType.ORDINALITY) || undefined;
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
    const index = this.index;
    if (this.matchTextSeq('STREAM')) {
      const thisExpr = this._parse({
        parseMethod: function (this: Parser) {
          return this.parseTable();
        },
        rawTokens: this.tokens,
      });
      if (thisExpr) {
        return this.expression(StreamExpr, { this: thisExpr });
      }
    }

    this.retreat(index);
    return undefined;
  }

  parseJoinParts (): { method: Token | undefined;
    side: Token | undefined;
    kind: Token | undefined; } {
    return {
      method: this.matchSet(this._constructor.JOIN_METHODS) ? this.prev : undefined,
      side: this.matchSet(this._constructor.JOIN_SIDES) ? this.prev : undefined,
      kind: this.matchSet(this._constructor.JOIN_KINDS) ? this.prev : undefined,
    };
  }

  parseUsingIdentifiers (): Expression[] {
    const parseColumnAsIdentifier = (): Expression | undefined => {
      const thisExpr = this.parseColumn();
      if (thisExpr instanceof ColumnExpr) {
        return thisExpr.args.this instanceof Expression ? thisExpr.args.this : toIdentifier(thisExpr.args.this);
      }
      return thisExpr;
    };

    return this.parseWrappedCsv(parseColumnAsIdentifier, { optional: true });
  }

  parseJoin (options: {
    skipJoinToken?: boolean;
    parseBracket?: boolean;
  } = {}): JoinExpr | undefined {
    const {
      skipJoinToken = false,
      parseBracket = false,
    } = options;
    if (this.match(TokenType.COMMA)) {
      const table = this.tryParse(this.parseTable.bind(this));
      const crossJoin = table ? this.expression(JoinExpr, { this: table }) : undefined;

      if (crossJoin && this._constructor.JOINS_HAVE_EQUAL_PRECEDENCE) {
        crossJoin.setArgKey('kind', JoinExprKind.CROSS);
      }

      return crossJoin;
    }

    const index = this.index;
    const {
      method,
      side,
      kind,
    } = this.parseJoinParts();
    const directed = this.matchTextSeq('DIRECTED') || undefined;
    const hint = (this.matchTexts(this._constructor.JOIN_HINTS) || undefined) && this.prev?.text;
    const join = this.match(TokenType.JOIN) || (kind?.tokenType === TokenType.STRAIGHT_JOIN);
    const joinComments = this.prevComments;

    if (!skipJoinToken && !join) {
      this.retreat(index);
      return undefined;
    }

    const outerApply = this.matchPair(TokenType.OUTER, TokenType.APPLY, { advance: false }) || undefined;
    const crossApply = this.matchPair(TokenType.CROSS, TokenType.APPLY, { advance: false }) || undefined;

    if (!skipJoinToken && !join && !outerApply && !crossApply) {
      return undefined;
    }

    const kwargs: JoinExprArgs = {
      this: this.parseTable({ parseBracket }),
    };

    if (kind?.tokenType === TokenType.ARRAY && this.match(TokenType.COMMA)) {
      kwargs.expressions = this.parseCsv(() =>
        this.parseTable({ parseBracket }));
    }

    if (method) kwargs.method = method.text.toUpperCase();
    if (side) kwargs.side = enumFromString(JoinExprKind, side.text);
    if (kind) kwargs.kind = kind.tokenType === TokenType.STRAIGHT_JOIN ? JoinExprKind.STRAIGHT_JOIN : enumFromString(JoinExprKind, kind.text);
    if (hint) kwargs.hint = hint;

    if (this.match(TokenType.MATCH_CONDITION)) {
      kwargs.matchCondition = this.parseWrapped(() => this.parseComparison());
    }

    if (this.match(TokenType.ON)) {
      kwargs.on = this.parseDisjunction();
    } else if (this.match(TokenType.USING)) {
      kwargs.using = this.parseUsingIdentifiers();
    } else if (
      !method
      && !(outerApply || crossApply)
      && !(kwargs.this instanceof UnnestExpr)
      && !(kind?.tokenType === TokenType.CROSS || kind?.tokenType === TokenType.ARRAY)
    ) {
      const nestedIndex = this.index;
      const joins = [...this.parseJoins()];

      if (0 < joins.length && this.match(TokenType.ON)) {
        kwargs.on = this.parseDisjunction();
      } else if (0 < joins.length && this.match(TokenType.USING)) {
        kwargs.using = this.parseUsingIdentifiers();
      } else {
        this.retreat(nestedIndex);
      }

      if (0 < joins.length && (kwargs.on || kwargs.using)) {
        kwargs.this?.setArgKey('joins', joins);
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
      && (!kwargs.kind || kwargs.kind === JoinExprKind.INNER || kwargs.kind === JoinExprKind.OUTER)
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

  parseOpClass (): Expression | undefined {
    const thisExpr = this.parseDisjunction();

    if (this.matchTexts(Array.from(this._constructor.OPCLASS_FOLLOW_KEYWORDS), { advance: false })) {
      return thisExpr;
    }

    if (!this.matchSet(this._constructor.OPTYPE_FOLLOW_TOKENS, { advance: false })) {
      return this.expression(OpclassExpr, {
        this: thisExpr,
        expression: this.parseTableParts(),
      });
    }

    return thisExpr;
  }

  parseIndexParams (): IndexParametersExpr {
    const using = this.match(TokenType.USING)
      ? this.parseVar({ anyToken: true })
      : undefined;

    const columns = this.match(TokenType.L_PAREN, { advance: false })
      ? this.parseWrappedCsv(() => this.parseWithOperator())
      : undefined;

    const include = this.matchTextSeq('INCLUDE')
      ? this.parseWrappedIdVars()
      : undefined;

    const partitionBy = this.parsePartitionBy();
    const withStorage = (this.match(TokenType.WITH) || undefined) && this.parseWrappedProperties();
    const tablespace = this.matchTextSeq([
      'USING',
      'INDEX',
      'TABLESPACE',
    ])
      ? this.parseVar({ anyToken: true })
      : undefined;

    const where = this.parseWhere();
    const on = this.match(TokenType.ON) ? this.parseField() : undefined;

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
    options: {
      index?: Expression;
      anonymous?: boolean;
    } = {},
  ): IndexExpr | undefined {
    let {
      anonymous = false,
      index,
    } = options;
    let unique: boolean | undefined;
    let primary: boolean | undefined;
    let amp: boolean | undefined;
    let table: TableExpr | undefined;

    if (index || anonymous) {
      unique = undefined;
      primary = undefined;
      amp = undefined;

      this.match(TokenType.ON);
      this.match(TokenType.TABLE); // hive
      table = this.parseTableParts({ schema: true });
    } else {
      unique = this.match(TokenType.UNIQUE) || undefined;
      primary = this.matchTextSeq('PRIMARY') || undefined;
      amp = this.matchTextSeq('AMP') || undefined;

      if (!this.match(TokenType.INDEX)) {
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

    if (this.matchPair(TokenType.WITH, TokenType.L_PAREN)) {
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
      this.matchRParen();
    } else {
      // https://dev.mysql.com/doc/refman/8.0/en/index-hints.html
      while (this.matchSet(this._constructor.TABLE_INDEX_HINT_TOKENS)) {
        const hint = new IndexTableHintExpr({ this: (this.prev?.text ?? '').toUpperCase() });

        this.matchSet(new Set([TokenType.INDEX, TokenType.KEY]));
        if (this.match(TokenType.FOR)) {
          this.advanceAny();
          hint.setArgKey('target', (this.prev?.text ?? '').toUpperCase());
        }

        hint.setArgKey('expressions', this.parseWrappedIdVars());
        hints.push(hint);
      }
    }

    return 0 < hints.length ? hints : undefined;
  }

  parseTablePart (options: { schema?: boolean } = {}): Expression | undefined {
    const {
      schema = false,
    } = options;
    return (
      (!schema && this.parseFunction({ optionalParens: false }))
      || this.parseIdVar({ anyToken: false })
      || this.parseStringAsIdentifier()
      || this.parsePlaceholder()
    );
  }

  parseTableParts (options: {
    schema?: boolean;
    isDbReference?: boolean;
    wildcard?: boolean;
  } = {}): TableExpr {
    const {
      schema = false,
      isDbReference = false,
      wildcard = false,
    } = options;
    let catalog: Expression | string | undefined;
    let db: Expression | string | undefined;
    let table: Expression | string | undefined = this.parseTablePart({ schema });

    while (this.match(TokenType.DOT)) {
      if (catalog) {
        // This allows nesting the table in arbitrarily many dot expressions if needed
        table = this.expression(
          DotExpr,
          {
            this: table,
            expression: this.parseTablePart({ schema }),
          },
        );
      } else {
        catalog = db;
        db = table;
        // "" used for tsql FROM a..b case
        table = this.parseTablePart({ schema }) || '';
      }
    }

    if (
      wildcard
      && this.isConnected()
      && (table instanceof IdentifierExpr || !table)
      && this.match(TokenType.STAR)
    ) {
      if (table instanceof IdentifierExpr) {
        table.args.this += '*';
      } else {
        table = new IdentifierExpr({ this: '*' });
      }
    }

    // We bubble up comments from the Identifier to the Table
    const comments = table instanceof Expression ? table.popComments() : undefined;

    if (isDbReference) {
      catalog = db;
      db = table;
      table = undefined;
    }

    if (!table && !isDbReference) {
      this.raiseError(`Expected table name but got ${this.curr}`, this.curr);
    }
    if (!db && isDbReference) {
      this.raiseError(`Expected database name but got ${this.curr}`, this.curr);
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

  parseTable (options: {
    schema?: boolean;
    joins?: boolean;
    aliasTokens?: Set<TokenType>;
    parseBracket?: boolean;
    isDbReference?: boolean;
    parsePartition?: boolean;
    consumePipe?: boolean;
  } = {}): Expression | undefined {
    const {
      schema = false,
      joins = false,
      aliasTokens,
      parseBracket = false,
      isDbReference = false,
      parsePartition = false,
      consumePipe = false,
    } = options;
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
      consumePipe,
    });
    if (subquery) {
      if (!(subquery as SelectExpr).args.pivots) {
        subquery.setArgKey('pivots', this.parsePivots());
      }
      return subquery;
    }

    let bracket = parseBracket && this.parseBracket(undefined);
    bracket = bracket ? this.expression(TableExpr, { this: bracket }) : undefined;

    let rowsFrom: Expression | Expression[] | undefined = (this.matchTextSeq(['ROWS', 'FROM']) || undefined) && this.parseWrappedCsv(() => this.parseTable());
    rowsFrom = rowsFrom ? this.expression(TableExpr, { rowsFrom }) : undefined;

    const only = this.match(TokenType.ONLY) || undefined;

    const thisExpr = (
      bracket
      || rowsFrom
      || this.parseBracket(
        this.parseTableParts({
          schema,
          isDbReference,
        }),
      )
    );

    if (only) {
      thisExpr?.setArgKey('only', only);
    }

    // Postgres supports a wildcard (table) suffix operator, which is a no-op in this context
    this.matchTextSeq('*');

    const shouldParsePartition = parsePartition || this._constructor.SUPPORTS_PARTITION_SELECTION;
    if (shouldParsePartition && this.match(TokenType.PARTITION, { advance: false })) {
      thisExpr?.setArgKey('partition', this.parsePartition());
    }

    if (schema) {
      return this.parseSchema({ this: thisExpr });
    }

    const version = this.parseVersion();
    if (version) {
      thisExpr?.setArgKey('version', version);
    }

    if (this._dialectConstructor.ALIAS_POST_TABLESAMPLE) {
      thisExpr?.setArgKey('sample', this.parseTableSample());
    }

    const alias = this.parseTableAlias({ aliasTokens: aliasTokens || this._constructor.TABLE_ALIAS_TOKENS });
    if (alias) {
      thisExpr?.setArgKey('alias', alias);
    }

    if (this.match(TokenType.INDEXED_BY)) {
      thisExpr?.setArgKey('indexed', this.parseTableParts());
    } else if (this.matchTextSeq(['NOT', 'INDEXED'])) {
      thisExpr?.setArgKey('indexed', false);
    }

    if (thisExpr instanceof TableExpr && this.matchTextSeq('AT')) {
      return this.expression(
        AtIndexExpr,
        {
          this: thisExpr.toColumn?.({ copy: false }),
          expression: this.parseIdVar(),
        },
      );
    }

    thisExpr?.setArgKey('hints', this.parseTableHints());

    if (!thisExpr?.args.pivots) {
      thisExpr?.setArgKey('pivots', this.parsePivots());
    }

    if (!this._dialectConstructor.ALIAS_POST_TABLESAMPLE) {
      thisExpr?.setArgKey('sample', this.parseTableSample());
    }

    if (joins) {
      for (const join of this.parseJoins()) {
        thisExpr?.append('joins', join);
      }
    }

    if (this.matchPair(TokenType.WITH, TokenType.ORDINALITY)) {
      thisExpr?.setArgKey('ordinality', true);
      thisExpr?.setArgKey('alias', this.parseTableAlias());
    }

    return thisExpr;
  }

  parseVersion (): VersionExpr | undefined {
    let thisText: string;
    if (this.match(TokenType.TIMESTAMP_SNAPSHOT)) {
      thisText = 'TIMESTAMP';
    } else if (this.match(TokenType.VERSION_SNAPSHOT)) {
      thisText = 'VERSION';
    } else {
      return undefined;
    }

    let kind: string;
    let expression: Expression | undefined;

    if (this.matchSet(new Set([TokenType.FROM, TokenType.BETWEEN]))) {
      kind = (this.prev?.text ?? '').toUpperCase();
      const start = this.parseBitwise();
      this.matchTexts(['TO', 'AND']);
      const end = this.parseBitwise();
      expression = this.expression(TupleExpr, { expressions: [start, end] });
    } else if (this.matchTextSeq(['CONTAINED', 'IN'])) {
      kind = 'CONTAINED IN';
      expression = this.expression(
        TupleExpr,
        { expressions: this.parseWrappedCsv(() => this.parseBitwise()) },
      );
    } else if (this.match(TokenType.ALL)) {
      kind = 'ALL';
      expression = undefined;
    } else {
      this.matchTextSeq(['AS', 'OF']);
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
    const index = this.index;
    let historicalData: HistoricalDataExpr | undefined;

    if (this.matchTexts(Array.from(this._constructor.HISTORICAL_DATA_PREFIX))) {
      const thisText = (this.prev?.text ?? '').toUpperCase();
      const kind = (
        this.match(TokenType.L_PAREN)
        && this.matchTexts(Array.from(this._constructor.HISTORICAL_DATA_KIND))
        && (this.prev?.text ?? '').toUpperCase()
      );
      const expression = (this.match(TokenType.FARROW) || undefined) && this.parseBitwise();

      if (expression) {
        this.matchRParen();
        historicalData = this.expression(
          HistoricalDataExpr,
          {
            this: thisText,
            kind,
            expression,
          },
        );
      } else {
        this.retreat(index);
      }
    }

    return historicalData;
  }

  parseChanges (): ChangesExpr | undefined {
    if (!this.matchTextSeq([
      'CHANGES',
      '(',
      'INFORMATION',
      '=>',
    ])) {
      return undefined;
    }

    const information = this.parseVar({ anyToken: true });
    this.matchRParen();

    return this.expression(
      ChangesExpr,
      {
        information,
        atBefore: this.parseHistoricalData(),
        end: this.parseHistoricalData(),
      },
    );
  }

  parseUnnest (options: { withAlias?: boolean } = {}): UnnestExpr | undefined {
    const {
      withAlias = true,
    } = options;

    if (!this.matchPair(TokenType.UNNEST, TokenType.L_PAREN, { advance: false })) {
      return undefined;
    }

    this.advance();

    const expressions = this.parseWrappedCsv(() => this.parseEquality());
    let offset: Expression | boolean | undefined = this.matchPair(TokenType.WITH, TokenType.ORDINALITY) || undefined;

    const alias = withAlias ? this.parseTableAlias() : undefined;

    if (alias) {
      if (this._dialectConstructor.UNNEST_COLUMN_ONLY) {
        const columns = alias.args.columns;
        if (columns) {
          this.raiseError('Unexpected extra column alias in unnest.');
        }

        alias.setArgKey('columns', [alias.args.this!]);
        alias.setArgKey('this', undefined);
      }

      const columns = alias.args.columns as Expression[] | undefined;
      if (offset && columns && expressions.length < columns.length) {
        offset = columns.pop()!;
      }
    }

    if (!offset && this.matchPair(TokenType.WITH, TokenType.OFFSET)) {
      this.match(TokenType.ALIAS);
      offset = this.parseIdVar({
        anyToken: false,
        tokens: this._constructor.UNNEST_OFFSET_ALIAS_TOKENS,
      }) || toIdentifier('offset');
    }

    return this.expression(UnnestExpr, {
      expressions,
      alias,
      offset,
    });
  }

  parseDerivedTableValues (): ValuesExpr | undefined {
    const isDerived = this.matchPair(TokenType.L_PAREN, TokenType.VALUES) || undefined;
    if (!isDerived && !(
      // ClickHouse's `FORMAT Values` is equivalent to `VALUES`
      this.matchTextSeq('VALUES') || this.matchTextSeq(['FORMAT', 'VALUES'])
    )) {
      return undefined;
    }

    const expressions = this.parseCsv(() => this.parseValue());
    const alias = this.parseTableAlias();

    if (isDerived) {
      this.matchRParen();
    }

    return this.expression(
      ValuesExpr,
      {
        expressions,
        alias: alias || this.parseTableAlias(),
      },
    );
  }

  parseTableSample (options: { asModifier?: boolean } = {}): TableSampleExpr | undefined {
    const {
      asModifier = false,
    } = options;
    if (!this.match(TokenType.TABLE_SAMPLE) && !(
      asModifier && this.matchTextSeq(['USING', 'SAMPLE'])
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
    const matchedLParen = this.match(TokenType.L_PAREN) || undefined;

    let expressions: Expression[] | undefined;
    let num: Expression | undefined;

    if (this._constructor.TABLESAMPLE_CSV) {
      num = undefined;
      expressions = this.parseCsv(() => this.parsePrimary());
    } else {
      expressions = undefined;
      num = (
        this.match(TokenType.NUMBER, { advance: false })
          ? this.parseFactor()
          : this.parsePrimary() || this.parsePlaceholder()
      );
    }

    if (this.matchTextSeq('BUCKET')) {
      bucketNumerator = this.parseNumber();
      this.matchTextSeq(['OUT', 'OF']);
      bucketDenominator = this.parseNumber();
      this.match(TokenType.ON);
      bucketField = this.parseField();
    } else if (this.matchSet(new Set([TokenType.PERCENT, TokenType.MOD]))) {
      percent = num;
    } else if (this.match(TokenType.ROWS) || !this._dialectConstructor.TABLESAMPLE_SIZE_IS_PERCENT) {
      size = num;
    } else {
      percent = num;
    }

    if (matchedLParen) {
      this.matchRParen();
    }

    if (this.match(TokenType.L_PAREN)) {
      method = this.parseVar({ upper: true });
      seed = (this.match(TokenType.COMMA) || undefined) && this.parseNumber();
      this.matchRParen();
    } else if (this.matchTexts(['SEED', 'REPEATABLE'])) {
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
    if (!this.match(TokenType.INTO)) {
      return undefined;
    }

    return this.expression(
      UnpivotColumnsExpr,
      {
        this: this.matchTextSeq('NAME') && this.parseColumn(),
        expressions: this.matchTextSeq('VALUE') && this.parseCsv(() => this.parseColumn()),
      },
    );
  }

  // https://duckdb.org/docs/sql/statements/pivot
  parseSimplifiedPivot (options: { isUnpivot?: boolean } = {}): PivotExpr {
    const { isUnpivot } = options;
    const parseOn = (): Expression | undefined => {
      const thisExpr = this.parseBitwise();

      if (this.match(TokenType.IN)) {
        // PIVOT ... ON col IN (row_val1, row_val2)
        return this.parseIn(thisExpr);
      }
      if (this.match(TokenType.ALIAS, { advance: false })) {
        // UNPIVOT ... ON (col1, col2, col3) AS row_val
        return this.parseAlias(thisExpr);
      }

      return thisExpr;
    };

    const thisExpr = this.parseTable();
    const expressions = (this.match(TokenType.ON) || undefined) && this.parseCsv(parseOn);
    const into = this.parseUnpivotColumns();
    const using = (this.match(TokenType.USING) || undefined) && this.parseCsv(() =>
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

      this.match(TokenType.ALIAS);
      const alias = this.parseBitwise();
      if (alias) {
        let aliasExpr = alias;
        if (alias instanceof ColumnExpr && !alias.args.db) {
          assertIsInstanceOf(alias.args.this, Expression);
          aliasExpr = alias.args.this;
        }
        return this.expression(PivotAliasExpr, {
          this: thisExpr,
          alias: aliasExpr,
        });
      }

      return thisExpr;
    };

    const value = this.parseColumn();

    if (!this.match(TokenType.IN)) {
      this.raiseError('Expecting IN', this.curr);
    }

    let exprs: Expression[];

    if (this.match(TokenType.L_PAREN)) {
      if (this.match(TokenType.ANY)) {
        exprs = [...ensureList<Expression>(new PivotAnyExpr({ this: this.parseOrder() }))];
      } else {
        exprs = this.parseCsv(parseAliasedExpression);
      }
      this.matchRParen();
      return this.expression(InExpr, {
        this: value,
        expressions: exprs,
      });
    }

    const field = this.parseIdVar();
    return this.expression(InExpr, {
      this: value,
      field,
    });
  }

  parsePivotAggregation (): Expression | undefined {
    const func = this.parseFunction();
    if (!func) {
      if (this.prev && this.prev.tokenType === TokenType.COMMA) {
        return undefined;
      }
      this.raiseError('Expecting an aggregation function in PIVOT', this.curr);
    }

    return this.parseAlias(func);
  }

  parsePivot (): PivotExpr | undefined {
    const index = this.index;
    let includeNulls: boolean | undefined;
    let unpivot: boolean;

    if (this.match(TokenType.PIVOT)) {
      unpivot = false;
    } else if (this.match(TokenType.UNPIVOT)) {
      unpivot = true;

      // https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-unpivot.html#syntax
      if (this.matchTextSeq(['INCLUDE', 'NULLS'])) {
        includeNulls = true;
      } else if (this.matchTextSeq(['EXCLUDE', 'NULLS'])) {
        includeNulls = false;
      }
    } else {
      return undefined;
    }

    if (!this.match(TokenType.L_PAREN)) {
      this.retreat(index);
      return undefined;
    }

    let expressions: Expression[];
    if (unpivot) {
      expressions = this.parseCsv(() => this.parseColumn());
    } else {
      expressions = this.parseCsv(() => this.parsePivotAggregation());
    }

    if (expressions.length === 0) {
      this.raiseError('Failed to parse PIVOT\'s aggregation list', this.curr);
    }

    if (!this.match(TokenType.FOR)) {
      this.raiseError('Expecting FOR', this.curr);
    }

    const fields: InExpr[] = [];
    while (true) {
      const field = this.tryParse(() => this.parsePivotIn());
      if (!field) {
        break;
      }
      fields.push(field);
    }

    const defaultOnNull = (this.matchTextSeq([
      'DEFAULT',
      'ON',
      'NULL',
    ]) || undefined) && this.parseWrapped(() =>
      this.parseBitwise());

    const group = this.parseGroup();

    this.matchRParen();

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

    if (!this.matchSet(new Set([TokenType.PIVOT, TokenType.UNPIVOT]), { advance: false })) {
      pivot.setArgKey('alias', this.parseTableAlias());
    }

    if (!unpivot) {
      const names = this.pivotColumnNames(expressions);

      const columns: Expression[] = [];
      const allFields: string[][] = [];

      for (const pivotField of pivot.args.fields as InExpr[]) {
        const pivotFieldExpressions = pivotField.args.expressions;

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
        for (const fldPartsTuple of this.product(allFields)) {
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

  protected pivotColumnNames (aggregations: Expression[]): string[] {
    return aggregations.map((agg) => agg.alias).filter((alias) => alias);
  }

  // Helper method for generating cartesian product (like Python's itertools.product)
  protected product<T>(arrays: T[][]): T[][] {
    if (arrays.length === 0) return [[]];
    if (arrays.length === 1) return arrays[0].map((item) => [item]);

    const result: T[][] = [];
    const [first, ...rest] = arrays;
    const restProduct = this.product(rest);

    for (const item of first) {
      for (const restItems of restProduct) {
        result.push([item, ...restItems]);
      }
    }

    return result;
  }

  parsePrewhere (options: { skipWhereToken?: boolean } = {}): PreWhereExpr | undefined {
    const {
      skipWhereToken = false,
    } = options;
    if (!skipWhereToken && !this.match(TokenType.PREWHERE)) {
      return undefined;
    }

    return this.expression(
      PreWhereExpr,
      {
        comments: this.prevComments,
        this: this.parseDisjunction(),
      },
    );
  }

  parseWhere (options: { skipWhereToken?: boolean } = {}): WhereExpr | undefined {
    const {
      skipWhereToken = false,
    } = options;
    if (!skipWhereToken && !this.match(TokenType.WHERE)) {
      return undefined;
    }

    return this.expression(
      WhereExpr,
      {
        comments: this.prevComments,
        this: this.parseDisjunction(),
      },
    );
  }

  parseGroup (options: { skipGroupByToken?: boolean } = {}): GroupExpr | undefined {
    const {
      skipGroupByToken = false,
    } = options;
    if (!skipGroupByToken && !this.match(TokenType.GROUP_BY)) {
      return undefined;
    }
    const comments = this.prevComments;

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

    if (this.match(TokenType.ALL)) {
      elements.all = true;
    } else if (this.match(TokenType.DISTINCT)) {
      elements.all = false;
    }

    if (this.matchSet(this._constructor.QUERY_MODIFIER_TOKENS, { advance: false })) {
      return this.expression(GroupExpr, {
        comments,
        ...elements,
      });
    }

    while (true) {
      const index = this.index;

      elements.expressions.push(
        ...this.parseCsv(() =>
          this.matchSet(new Set([TokenType.CUBE, TokenType.ROLLUP]), { advance: false })
            ? undefined
            : this.parseDisjunction()),
      );

      const beforeWithIndex = this.index;
      const withPrefix = this.match(TokenType.WITH) || undefined;

      const cubeOrRollup = this.parseCubeOrRollup({ withPrefix });
      if (cubeOrRollup) {
        const key = cubeOrRollup instanceof RollupExpr ? 'rollup' : 'cube';
        elements[key].push(cubeOrRollup);
      } else {
        const groupingSets = this.parseGroupingSets();
        if (groupingSets) {
          elements.groupingSets.push(groupingSets);
        } else if (this.matchTextSeq('TOTALS')) {
          elements.totals = true;
        }
      }

      if (beforeWithIndex <= this.index && this.index <= beforeWithIndex + 1) {
        this.retreat(beforeWithIndex);
        break;
      }

      if (index === this.index) {
        break;
      }
    }

    return this.expression(GroupExpr, {
      comments,
      ...elements,
    });
  }

  parseCubeOrRollup (options: { withPrefix?: boolean } = {}): CubeExpr | RollupExpr | undefined {
    const {
      withPrefix = false,
    } = options;
    let kind: typeof CubeExpr | typeof RollupExpr;

    if (this.match(TokenType.CUBE)) {
      kind = CubeExpr;
    } else if (this.match(TokenType.ROLLUP)) {
      kind = RollupExpr;
    } else {
      return undefined;
    }

    return this.expression(
      kind,
      { expressions: withPrefix ? [] : this.parseWrappedCsv(() => this.parseBitwise()) },
    );
  }

  parseGroupingSets (): GroupingSetsExpr | undefined {
    if (this.match(TokenType.GROUPING_SETS)) {
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

  parseHaving (options: { skipHavingToken?: boolean } = {}): HavingExpr | undefined {
    const {
      skipHavingToken = false,
    } = options;
    if (!skipHavingToken && !this.match(TokenType.HAVING)) {
      return undefined;
    }
    return this.expression(
      HavingExpr,
      {
        comments: this.prevComments,
        this: this.parseDisjunction(),
      },
    );
  }

  parseQualify (): QualifyExpr | undefined {
    if (!this.match(TokenType.QUALIFY)) {
      return undefined;
    }
    return this.expression(QualifyExpr, { this: this.parseDisjunction() });
  }

  parseConnectWithPrior (): Expression | undefined {
    this._constructor.NO_PAREN_FUNCTION_PARSERS['PRIOR'] = function (this: Parser) {
      return this.expression(PriorExpr, { this: this.parseBitwise() });
    };
    const connect = this.parseDisjunction();
    delete this._constructor.NO_PAREN_FUNCTION_PARSERS['PRIOR'];
    return connect;
  }

  parseConnect (options: { skipStartToken?: boolean } = {}): ConnectExpr | undefined {
    const {
      skipStartToken = false,
    } = options;
    let start: Expression | undefined;

    if (skipStartToken) {
      start = undefined;
    } else if (this.match(TokenType.START_WITH)) {
      start = this.parseDisjunction();
    } else {
      return undefined;
    }

    this.match(TokenType.CONNECT_BY);
    const nocycle = this.matchTextSeq('NOCYCLE') || undefined;
    const connect = this.parseConnectWithPrior();

    if (!start && this.match(TokenType.START_WITH)) {
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
    if (this.match(TokenType.ALIAS)) {
      thisExpr = this.expression(AliasExpr, {
        alias: thisExpr,
        this: this.parseDisjunction(),
      });
    }
    return thisExpr;
  }

  parseInterpolate (): Expression[] | undefined {
    if (this.matchTextSeq('INTERPOLATE')) {
      return this.parseWrappedCsv(() => this.parseNameAsExpression());
    }
    return undefined;
  }

  parseOrder (
    options: {
      thisExpr?: Expression;
      skipOrderToken?: boolean;
    } = {},
  ): OrderExpr | undefined {
    const {
      thisExpr, skipOrderToken = false,
    } = options;
    let siblings: boolean | undefined;

    if (!skipOrderToken && !this.match(TokenType.ORDER_BY)) {
      if (!this.match(TokenType.ORDER_SIBLINGS_BY)) {
        return thisExpr as OrderExpr | undefined;
      }

      siblings = true;
    }

    return this.expression(
      OrderExpr,
      {
        comments: this.prevComments,
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
    if (!this.match(token)) {
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

    const asc = this.match(TokenType.ASC) || undefined;
    const desc = (this.match(TokenType.DESC) || undefined) || (asc && false);

    const isNullsFirst = this.matchTextSeq(['NULLS', 'FIRST']) || undefined;
    const isNullsLast = this.matchTextSeq(['NULLS', 'LAST']) || undefined;

    let nullsFirst = isNullsFirst || false;
    const explicitlyNullOrdered = isNullsFirst || isNullsLast;

    if (
      !explicitlyNullOrdered
      && (
        (!desc && this._dialectConstructor.NULL_ORDERING === NullOrdering.NULLS_ARE_SMALL)
        || (desc && this._dialectConstructor.NULL_ORDERING !== NullOrdering.NULLS_ARE_SMALL)
      )
      && this._dialectConstructor.NULL_ORDERING !== NullOrdering.NULLS_ARE_LAST
    ) {
      nullsFirst = true;
    }

    let withFill: WithFillExpr | undefined;
    if (this.matchTextSeq(['WITH', 'FILL'])) {
      withFill = this.expression(
        WithFillExpr,
        {
          fromValue: this.match(TokenType.FROM) && this.parseBitwise(),
          to: this.matchTextSeq('TO') && this.parseBitwise(),
          step: this.matchTextSeq('STEP') && this.parseBitwise(),
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
    const percent = this.matchSet(new Set([TokenType.PERCENT, TokenType.MOD])) || undefined;
    const rows = this.matchSet(new Set([TokenType.ROW, TokenType.ROWS])) || undefined;
    this.matchTextSeq('ONLY');
    const withTies = this.matchTextSeq(['WITH', 'TIES']) || undefined;

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
    options: {
      top?: boolean;
      skipLimitToken?: boolean;
    } = {},
  ): Expression | undefined {
    const {
      top = false,
      skipLimitToken = false,
    } = options;
    if (skipLimitToken || this.match(top ? TokenType.TOP : TokenType.LIMIT)) {
      const comments = this.prevComments;
      let expression: Expression | undefined;

      if (top) {
        const limitParen = this.match(TokenType.L_PAREN) || undefined;
        expression = limitParen ? this.parseTerm() : this.parseNumber();

        if (limitParen) {
          this.matchRParen();
        }
      } else {
        // Parsing LIMIT x% (i.e x PERCENT) as a term leads to an error, since
        // we try to build an exp.Mod expr. For that matter, we backtrack and instead
        // consume the factor plus parse the percentage separately
        const index = this.index;
        expression = this.tryParse(() => this.parseTerm());
        if (expression instanceof ModExpr) {
          this.retreat(index);
          expression = this.parseFactor();
        } else if (!expression) {
          expression = this.parseFactor();
        }
      }

      const limitOptions = this.parseLimitOptions();

      let offset: Expression | undefined;
      if (this.match(TokenType.COMMA)) {
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

    if (this.match(TokenType.FETCH)) {
      const direction = this.matchSet(new Set([TokenType.FIRST, TokenType.NEXT])) || undefined;
      const directionText = direction ? (this.prev?.text ?? '').toUpperCase() : 'FIRST';

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
    if (!this.match(TokenType.OFFSET)) {
      return thisExpr;
    }

    const count = this.parseTerm();
    this.matchSet(new Set([TokenType.ROW, TokenType.ROWS]));

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
    if (!this.matchSet(new Set(this._constructor.AMBIGUOUS_ALIAS_TOKENS), { advance: false })) {
      return false;
    }

    const index = this.index;
    const result = !!(
      this.tryParse(() => this.parseLimit(), { retreat: true })
      || this.tryParse(() => this.parseOffset(), { retreat: true })
    );
    this.retreat(index);

    // MATCH_CONDITION (...) is a special construct that should not be consumed by limit/offset
    if (this.next && this.next.tokenType === TokenType.MATCH_CONDITION) {
      return false;
    }

    return result;
  }

  parseLimitBy (): Expression[] | undefined {
    return this.matchTextSeq('BY') ? this.parseCsv(() => this.parseBitwise()) : undefined;
  }

  parseLocks (): LockExpr[] {
    const locks: LockExpr[] = [];

    while (true) {
      let update: boolean | undefined;
      let key: boolean | undefined;

      if (this.matchTextSeq(['FOR', 'UPDATE'])) {
        update = true;
      } else if (this.matchTextSeq(['FOR', 'SHARE']) || this.matchTextSeq([
        'LOCK',
        'IN',
        'SHARE',
        'MODE',
      ])) {
        update = false;
      } else if (this.matchTextSeq([
        'FOR',
        'KEY',
        'SHARE',
      ])) {
        update = false;
        key = true;
      } else if (this.matchTextSeq([
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
      if (this.matchTextSeq('OF')) {
        expressions = this.parseCsv(() => this.parseTable({ schema: true }));
      }

      let wait: boolean | Expression | undefined;
      if (this.matchTextSeq('NOWAIT')) {
        wait = true;
      } else if (this.matchTextSeq('WAIT')) {
        wait = this.parsePrimary();
      } else if (this.matchTextSeq(['SKIP', 'LOCKED'])) {
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
    options: { consumePipe?: boolean } = {},
  ): Expression | undefined {
    const {
      consumePipe = false,
    } = options;
    const start = this.index;
    const {
      side: sideToken,
      kind: kindToken,
    } = this.parseJoinParts();

    const side = sideToken?.text;
    const kind = kindToken?.text;

    if (!this.matchSet(this._constructor.SET_OPERATIONS)) {
      this.retreat(start);
      return undefined;
    }

    const tokenType = this.prev?.tokenType;

    let operation;
    if (tokenType === TokenType.UNION) {
      operation = UnionExpr;
    } else if (tokenType === TokenType.EXCEPT) {
      operation = ExceptExpr;
    } else {
      operation = IntersectExpr;
    }

    const comments = this.prev?.comments;

    let distinct: boolean | undefined;
    if (this.match(TokenType.DISTINCT)) {
      distinct = true;
    } else if (this.match(TokenType.ALL)) {
      distinct = false;
    } else {
      distinct = this._dialectConstructor.SET_OP_DISTINCT_BY_DEFAULT[operation.key];
      if (distinct === undefined) {
        this.raiseError(`Expected DISTINCT or ALL for ${operation.name}`, this.curr);
      }
    }

    let byName = this.matchTextSeq(['BY', 'NAME']) || this.matchTextSeq(['STRICT', 'CORRESPONDING']) || undefined;
    if (this.matchTextSeq('CORRESPONDING')) {
      byName = true;
      if (!side && !kind) {
        // Set default kind
        // kind = 'INNER';  // Uncomment if needed
      }
    }

    let onColumnList: Expression[] | undefined;
    if (byName && this.matchTexts(['ON', 'BY'])) {
      onColumnList = this.parseWrappedCsv(() => this.parseColumn());
    }

    const expression = this.parseSelect({
      nested: true,
      parseSetOperation: false,
      consumePipe,
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
          const expr = expression.args[arg as keyof typeof expression.args];
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
    let thisExpr: ExpressionValue | undefined = this.parseDisjunction();

    if (!thisExpr && this.next && this.next.tokenType in this._constructor.ASSIGNMENT) {
      // This allows us to parse <non-identifier token> := <expr>
      const token = this.advanceAny({ ignoreReserved: true });
      thisExpr = column({ col: token && '' });
    }

    const assignmentTokens = Object.keys(this._constructor.ASSIGNMENT) as TokenType[];
    while (this.matchSet(assignmentTokens)) {
      if (thisExpr instanceof ColumnExpr && thisExpr.parts.length === 1) {
        thisExpr = thisExpr.args.this;
      }

      const ExprClass = this._constructor.ASSIGNMENT[this.prev?.tokenType ?? TokenType.UNKNOWN];
      if (ExprClass) {
        thisExpr = this.expression(
          ExprClass,
          {
            this: thisExpr,
            comments: this.prevComments,
            expression: this.parseAssignment(),
          },
        );
      }
    }
    return thisExpr as Expression | undefined;
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
    const negate = this.match(TokenType.NOT) || undefined;

    if (this.matchSet(Object.keys(this._constructor.RANGE_PARSERS) as TokenType[])) {
      const parser = this._constructor.RANGE_PARSERS[this.prev?.tokenType ?? TokenType.UNKNOWN];
      if (parser && current) {
        const expression = parser.call(this, current);
        if (!expression) {
          return current;
        }
        current = expression;
      }
    } else if (this.match(TokenType.ISNULL) || (negate && this.match(TokenType.NULL))) {
      current = this.expression(IsExpr, {
        this: current,
        expression: null_(),
      });
    }

    // Postgres supports ISNULL and NOTNULL for conditions.
    // https://blog.andreiavram.ro/postgresql-null-composite-type/
    if (this.match(TokenType.NOTNULL)) {
      current = this.expression(IsExpr, {
        this: current,
        expression: null_(),
      });
      current = this.expression(NotExpr, { this: current });
    }

    if (negate) {
      current = this.negateRange(current);
    }

    if (this.match(TokenType.IS)) {
      current = this.parseIs(current);
    }

    return current;
  }

  protected negateRange (thisExpr?: Expression): Expression | undefined {
    if (!thisExpr) {
      return thisExpr;
    }

    return this.expression(NotExpr, { this: thisExpr });
  }

  parseIs (thisExpr?: Expression): Expression | undefined {
    const index = this.index - 1;
    const negate = this.match(TokenType.NOT) || undefined;

    if (this.matchTextSeq(['DISTINCT', 'FROM'])) {
      const klass = negate ? NullSafeEqExpr : NullSafeNeqExpr;
      return this.expression(klass, {
        this: thisExpr,
        expression: this.parseBitwise(),
      });
    }

    let expression: Expression | undefined;
    if (this.match(TokenType.JSON)) {
      const kind = (this.matchTexts(Array.from(this._constructor.IS_JSON_PREDICATE_KIND)) || undefined) && (this.prev?.text ?? '').toUpperCase();

      let with_: boolean | undefined;
      if (this.matchTextSeq('WITH')) {
        with_ = true;
      } else if (this.matchTextSeq('WITHOUT')) {
        with_ = false;
      }

      const unique = this.match(TokenType.UNIQUE) || undefined;
      this.matchTextSeq('KEYS');

      expression = this.expression(
        JsonExpr,
        {
          this: kind,
          with: with_,
          unique,
        },
      );
    } else {
      expression = this.parseNull() || this.parseBitwise();
      if (!expression) {
        this.retreat(index);
        return undefined;
      }
    }

    const result = this.expression(IsExpr, {
      this: thisExpr,
      expression,
    });
    if (negate) {
      return this.expression(NotExpr, { this: result });
    }
    return this.parseColumnOps(result);
  }

  parseIn (thisExpr?: Expression, options: {
    alias?: boolean;
    [index: string]: unknown;
  } = {}): InExpr {
    const {
      alias = false,
    } = options;
    const unnest = this.parseUnnest({ withAlias: false });
    let result: InExpr;

    if (unnest) {
      result = this.expression(InExpr, {
        this: thisExpr,
        unnest,
      });
    } else if (this.matchSet(new Set([TokenType.L_PAREN, TokenType.L_BRACKET]))) {
      const matchedLParen = this.prev?.tokenType === TokenType.L_PAREN;
      const expressions = this.parseCsv(() => this.parseSelectOrExpression({ alias }));

      if (expressions.length === 1 && expressions[0] instanceof QueryExpr) {
        const query = expressions[0];
        const queryModifiers = this.parseQueryModifiers(query);
        const subquery = queryModifiers.subquery(undefined, { copy: false });
        result = this.expression(
          InExpr,
          {
            this: thisExpr,
            query: subquery,
          },
        );
      } else {
        result = this.expression(InExpr, {
          this: thisExpr,
          expressions,
        });
      }

      if (matchedLParen) {
        this.matchRParen(result);
      } else if (!this.match(TokenType.R_BRACKET, { expression: result })) {
        this.raiseError('Expecting ]', this.curr);
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
    if (this.matchTextSeq('SYMMETRIC')) {
      symmetric = true;
    } else if (this.matchTextSeq('ASYMMETRIC')) {
      symmetric = false;
    }

    const low = this.parseBitwise();
    this.match(TokenType.AND);
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
    if (!this.match(TokenType.ESCAPE)) {
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
      const index = this.index;

      // Var "TO" Var
      const firstUnit = this.parseVar({
        anyToken: true,
        upper: true,
      });
      let secondUnit: VarExpr | undefined;
      if (firstUnit && this.matchTextSeq('TO')) {
        secondUnit = this.parseVar({
          anyToken: true,
          upper: true,
        });
      }

      intervalSpanUnitsOmitted = !(firstUnit && secondUnit);

      this.retreat(index);
    }

    let unit: Expression | undefined = intervalSpanUnitsOmitted
      ? undefined
      : (
        this.parseFunction()
        || (
          !this.match(TokenType.ALIAS, { advance: false })
          && this.parseVar({
            anyToken: true,
            upper: true,
          })
        )
        || undefined
      );

    // Most dialects support, e.g., the form INTERVAL '5' day, thus we try to parse
    // each INTERVAL expression into this canonical form so it's easy to transpile
    let finalThis = thisExpr;
    if (thisExpr && thisExpr.isNumber) {
      finalThis = LiteralExpr.string(thisExpr.toValue() || thisExpr.sql());
    } else if (thisExpr && thisExpr.isString) {
      const parts = thisExpr.name?.match?.(INTERVAL_STRING_RE);
      if (parts && unit) {
        // Unconsume the eagerly-parsed unit, since the real unit was part of the string
        unit = undefined;
        this.retreat(this.index - 1);
      }

      if (parts && 3 <= parts.length) {
        finalThis = LiteralExpr.string(parts[1]);
        unit = this.expression(VarExpr, { this: parts[2].toUpperCase() });
      }
    }

    if (this._constructor.INTERVAL_SPANS && this.matchTextSeq('TO')) {
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

  parseInterval (options: { matchInterval?: boolean } = {}): AddExpr | IntervalExpr | undefined {
    const {
      matchInterval = true,
    } = options;

    const index = this.index;

    if (!this.match(TokenType.INTERVAL) && matchInterval) {
      return undefined;
    }

    let thisExpr: Expression | undefined;
    if (this.match(TokenType.STRING, { advance: false })) {
      thisExpr = this.parsePrimary();
    } else {
      thisExpr = this.parseTerm();
    }

    if (!thisExpr || (
      thisExpr instanceof ColumnExpr
      && !thisExpr.args.table
      && thisExpr.args.this instanceof IdentifierExpr
      && !thisExpr.args.this.args.quoted
      && this.curr
      && !this._dialectConstructor.VALID_INTERVAL_UNITS.has(this.curr.text.toUpperCase())
    )) {
      this.retreat(index);
      return undefined;
    }

    const interval = this.parseIntervalSpan(thisExpr);

    const index2 = this.index;
    this.match(TokenType.PLUS);

    // Convert INTERVAL 'val_1' unit_1 [+] ... [+] 'val_n' unit_n into a sum of intervals
    if (this.matchSet(new Set([TokenType.STRING, TokenType.NUMBER]), { advance: false })) {
      return this.expression(
        AddExpr,
        {
          this: interval,
          expression: this.parseInterval({ matchInterval: false }),
        },
      );
    }

    this.retreat(index2);
    return interval;
  }

  parseBitwise (): Expression | undefined {
    let thisExpr = this.parseTerm();

    const bitwiseTokens = Object.keys(this._constructor.BITWISE) as TokenType[];
    while (true) {
      if (this.matchSet(bitwiseTokens)) {
        const ExprClass = this._constructor.BITWISE[this.prev?.tokenType ?? TokenType.UNKNOWN];
        if (ExprClass) {
          thisExpr = this.expression(
            ExprClass,
            {
              this: thisExpr,
              expression: this.parseTerm(),
            },
          );
        }
      } else if (this._dialectConstructor.DPIPE_IS_STRING_CONCAT && this.match(TokenType.DPIPE)) {
        thisExpr = this.expression(
          DPipeExpr,
          {
            this: thisExpr,
            expression: this.parseTerm(),
            safe: !this._dialectConstructor.STRICT_STRING_CONCAT,
          },
        );
      } else if (this.match(TokenType.DQMARK)) {
        thisExpr = this.expression(
          CoalesceExpr,
          {
            this: thisExpr,
            expressions: ensureList(this.parseTerm()),
          },
        );
      } else if (this.matchPair(TokenType.LT, TokenType.LT)) {
        thisExpr = this.expression(
          BitwiseLeftShiftExpr,
          {
            this: thisExpr,
            expression: this.parseTerm(),
          },
        );
      } else if (this.matchPair(TokenType.GT, TokenType.GT)) {
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

    const termTokens = Object.keys(this._constructor.TERM) as TokenType[];
    while (this.matchSet(termTokens)) {
      const klass = this._constructor.TERM[this.prev?.tokenType ?? TokenType.UNKNOWN];
      const comments = this.prevComments;
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
            const ident = expr.args.this;
            if (ident instanceof IdentifierExpr) {
              thisExpr.setArgKey('expression', ident.args.quoted ? ident : var_(ident.name));
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

    const factorTokens = Object.keys(this._constructor.FACTOR) as TokenType[];
    while (this.matchSet(factorTokens)) {
      const klass = this._constructor.FACTOR[this.prev?.tokenType ?? TokenType.UNKNOWN];
      const comments = this.prevComments;
      const expression = parseMethod();

      if (!expression && klass === IntDivExpr && /^[a-zA-Z]/.test(this.prev?.text ?? '')) {
        this.retreat(this.index - 1);
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
    if (this.matchSet(Object.keys(this._constructor.UNARY_PARSERS) as TokenType[])) {
      const parser = this._constructor.UNARY_PARSERS[this.prev?.tokenType ?? TokenType.UNKNOWN];
      return parser ? parser.call(this) : undefined;
    }
    return this.parseType();
  }

  parseType (options: {
    parseInterval?: boolean;
    fallbackToIdentifier?: boolean;
  } = {}): Expression | undefined {
    const {
      parseInterval = true,
      fallbackToIdentifier = false,
    } = options;

    const interval = parseInterval && this.parseInterval();
    if (interval) {
      return this.parseColumnOps(interval);
    }

    const index = this.index;
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
      const index2 = this.index;
      const thisExpr = this.parsePrimary();

      if (thisExpr instanceof LiteralExpr) {
        const literal = thisExpr.name;
        const thisWithOps = this.parseColumnOps(thisExpr);

        const parser = typeof dataType.args.this === 'string' ? this._constructor.TYPE_LITERAL_PARSERS[dataType.args.this as DataTypeExprKind] : undefined;
        if (parser) {
          return parser.call(this, thisWithOps, dataType as DataTypeExpr);
        }

        if (
          this._constructor.ZONE_AWARE_TIMESTAMP_CONSTRUCTOR
          && (dataType as DataTypeExpr).isType?.([DataTypeExprKind.TIMESTAMP])
          && TIME_ZONE_RE.test(literal)
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
      if ((dataType as DataTypeExpr).args.expressions && 1 < index2 - index) {
        this.retreat(index2);
        return this.parseColumnOps(dataType);
      }

      this.retreat(index);
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

    while (this.match(TokenType.DOT)) {
      this.advanceAny();
      typeName = `${typeName}.${this.prev?.text ?? ''}`;
    }

    return DataTypeExpr.build(typeName, {
      dialect: this.dialect,
      udt: true,
    });
  }

  parseTypes (options: {
    checkFunc?: boolean;
    schema?: boolean;
    allowIdentifiers?: boolean;
  } = {}): Expression | undefined {
    const {
      checkFunc = false,
      schema = false,
      allowIdentifiers = true,
    } = options;

    const index = this.index;

    let thisExpr: Expression | undefined;
    const prefix = this.matchTextSeq(['SYSUDTLIB', '.']) || undefined;

    let typeToken: TokenType | undefined;
    if (this.matchSet(this._constructor.TYPE_TOKENS)) {
      typeToken = this.prev?.tokenType;
    } else {
      const identifier = allowIdentifiers && this.parseIdVar({
        anyToken: false,
        tokens: new Set([TokenType.VAR]),
      });

      if (identifier instanceof IdentifierExpr) {
        let tokens: Token[] | undefined;
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
          this.retreat(this.index - 1);
          return undefined;
        }
      } else {
        return undefined;
      }
    }

    if (typeToken === TokenType.PSEUDO_TYPE) {
      return this.expression(PseudoTypeExpr, { this: (this.prev?.text ?? '').toUpperCase() });
    }

    if (typeToken === TokenType.OBJECT_IDENTIFIER) {
      return this.expression(ObjectIdentifierExpr, { this: (this.prev?.text ?? '').toUpperCase() });
    }

    // https://materialize.com/docs/sql/types/map/
    if (typeToken === TokenType.MAP && this.match(TokenType.L_BRACKET)) {
      const keyType = this.parseTypes({
        checkFunc,
        schema,
        allowIdentifiers,
      });
      if (!this.match(TokenType.FARROW)) {
        this.retreat(index);
        return undefined;
      }

      const valueType = this.parseTypes({
        checkFunc,
        schema,
        allowIdentifiers,
      });
      if (!this.match(TokenType.R_BRACKET)) {
        this.retreat(index);
        return undefined;
      }

      return new DataTypeExpr({
        this: DataTypeExprKind.MAP,
        expressions: [keyType as DataTypeExpr, valueType as DataTypeExpr],
        nested: true,
        prefix,
      });
    }

    const nested = typeToken && this._constructor.NESTED_TYPE_TOKENS.has(typeToken);
    const isStruct = typeToken && this._constructor.STRUCT_TYPE_TOKENS.has(typeToken);
    const isAggregate = typeToken && this._constructor.AGGREGATE_TYPE_TOKENS.has(typeToken);
    let expressions: Expression[] | undefined;
    let maybeFunc = false;

    if (this.match(TokenType.L_PAREN)) {
      if (isStruct) {
        expressions = this.parseCsv(() => this.parseStructTypes({ typeRequired: true }));
      } else if (nested) {
        expressions = this.parseCsv(() => this.parseTypes({
          checkFunc,
          schema,
          allowIdentifiers,
        }));

        if (typeToken === TokenType.NULLABLE && expressions.length === 1) {
          thisExpr = expressions[0];
          thisExpr.setArgKey('nullable', true);
          this.matchRParen();
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
        if (this.match(TokenType.COMMA)) {
          expressions.push(...this.parseCsv(() => this.parseTypes({
            checkFunc,
            schema,
            allowIdentifiers,
          })));
        }
      } else {
        expressions = this.parseCsv(() => this.parseTypeSize());

        // https://docs.snowflake.com/en/sql-reference/data-types-vector
        if (typeToken === TokenType.VECTOR && expressions.length === 2) {
          expressions = this.parseVectorExpressions(expressions);
        }
      }

      if (!this.match(TokenType.R_PAREN)) {
        this.retreat(index);
        return undefined;
      }

      maybeFunc = true;
    }

    let values: Expression[] | undefined;

    if (nested && this.match(TokenType.LT)) {
      if (isStruct) {
        expressions = this.parseCsv(() => this.parseStructTypes({ typeRequired: true }));
      } else {
        expressions = this.parseCsv(() => this.parseTypes({
          checkFunc,
          schema,
          allowIdentifiers,
        }));
      }

      if (!this.match(TokenType.GT)) {
        this.raiseError('Expecting >', this.curr);
      }

      if (this.matchSet(new Set([TokenType.L_BRACKET, TokenType.L_PAREN]))) {
        values = this.parseCsv(() => this.parseDisjunction());
        if (!values && isStruct) {
          values = undefined;
          this.retreat(this.index - 1);
        } else {
          this.matchSet(new Set([TokenType.R_BRACKET, TokenType.R_PAREN]));
        }
      }
    }

    if (typeToken && this._constructor.TIMESTAMPS.has(typeToken)) {
      if (this.matchTextSeq([
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
          expressions: expressions as DataTypeExpr[],
        });
      } else if (this.matchTextSeq([
        'WITH',
        'LOCAL',
        'TIME',
        'ZONE',
      ])) {
        maybeFunc = false;
        thisExpr = new DataTypeExpr({
          this: DataTypeExprKind.TIMESTAMPLTZ,
          expressions: expressions as DataTypeExpr[],
        });
      } else if (this.matchTextSeq([
        'WITHOUT',
        'TIME',
        'ZONE',
      ])) {
        maybeFunc = false;
      }
    } else if (typeToken === TokenType.INTERVAL) {
      if (this.curr && this._dialectConstructor.VALID_INTERVAL_UNITS.has(this.curr.text.toUpperCase())) {
        let unit = this.parseVar({ upper: true });
        if (this.matchTextSeq('TO')) {
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

    if (maybeFunc && checkFunc) {
      const index2 = this.index;
      const peek = this.parseString();

      if (!peek) {
        this.retreat(index);
        return undefined;
      }

      this.retreat(index2);
    }

    if (!thisExpr) {
      if (this.matchTextSeq('UNSIGNED')) {
        const unsignedTypeToken = typeToken && this._constructor.SIGNED_TO_UNSIGNED_TYPE_TOKEN[typeToken];
        if (!unsignedTypeToken) {
          this.raiseError(`Cannot convert ${typeToken?.valueOf()} to unsigned.`, this.curr);
        }

        typeToken = unsignedTypeToken || typeToken;
      }

      // NULLABLE without parentheses can be a column (Presto/Trino)
      if (typeToken === TokenType.NULLABLE && !expressions) {
        this.retreat(index);
        return undefined;
      }

      if (typeToken) {
        thisExpr = new DataTypeExpr({
          this: DataTypeExprKind[typeToken.valueOf().toUpperCase() as keyof typeof DataTypeExprKind],
          expressions: expressions as DataTypeExpr[],
          nested,
          prefix,
        });

        // Empty arrays/structs are allowed
        if (values !== undefined) {
          const cls = isStruct ? StructExpr : ArrayExpr;
          thisExpr = cast(
            new cls({ expressions: values }),
            thisExpr as DataTypeExpr,
            { copy: false },
          );
        }
      }
    } else if (expressions) {
      thisExpr.setArgKey('expressions', expressions);
    }

    // https://materialize.com/docs/sql/types/list/#type-name
    while (this.match(TokenType.LIST)) {
      thisExpr = new DataTypeExpr({
        this: DataTypeExprKind.LIST,
        expressions: [thisExpr as DataTypeExpr],
        nested: true,
      });
    }

    const index3 = this.index;

    // Postgres supports the INT ARRAY[3] syntax as a synonym for INT[3]
    let matchedArray = this.match(TokenType.ARRAY) || false;

    while (this.curr) {
      const datatypeToken = this.prev?.tokenType;
      const matchedLBracket = this.match(TokenType.L_BRACKET) || undefined;

      if ((!matchedLBracket && !matchedArray) || (
        datatypeToken === TokenType.ARRAY && this.match(TokenType.R_BRACKET)
      )) {
        // Postgres allows casting empty arrays such as ARRAY[]::INT[],
        // not to be confused with the fixed size array parsing
        break;
      }

      matchedArray = false;
      const valuesInBracket = this.parseCsv(() => this.parseDisjunction());

      if (
        valuesInBracket
        && !schema
        && (
          !this._dialectConstructor.SUPPORTS_FIXED_SIZE_ARRAYS
          || datatypeToken === TokenType.ARRAY
          || !this.match(TokenType.R_BRACKET, { advance: false })
        )
      ) {
        // Retreating here means that we should not parse the following values as part of the data type, e.g. in DuckDB
        // ARRAY[1] should retreat and instead be parsed into exp.Array in contrast to INT[x][y] which denotes a fixed-size array data type
        this.retreat(index3);
        break;
      }

      thisExpr = new DataTypeExpr({
        this: DataTypeExprKind.ARRAY,
        expressions: [thisExpr as DataTypeExpr],
        values: valuesInBracket,
        nested: true,
      });
      this.match(TokenType.R_BRACKET);
    }

    if (thisExpr && this._constructor.TYPE_CONVERTERS && typeof (thisExpr as DataTypeExpr).args.this === 'object') {
      const converter = this._constructor.TYPE_CONVERTERS[(thisExpr as DataTypeExpr).args.this as DataTypeExprKind];
      if (converter) {
        thisExpr = converter(thisExpr as DataTypeExpr);
      }
    }

    return thisExpr;
  }

  parseVectorExpressions (expressions: Expression[]): Expression[] {
    const dataType = DataTypeExpr.build(expressions[0].name, { dialect: this.dialect });
    return [...(dataType ? [dataType] : []), ...expressions.slice(1)];
  }

  parseStructTypes (options: { typeRequired?: boolean } = {}): Expression | undefined {
    const {
      typeRequired = false,
    } = options;

    const index = this.index;

    let thisExpr: Expression | undefined;
    if (
      this.curr
      && this.next
      && this._constructor.TYPE_TOKENS.has(this.curr.tokenType)
      && this._constructor.TYPE_TOKENS.has(this.next.tokenType)
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

    this.match(TokenType.COLON);

    if (
      typeRequired
      && !(thisExpr instanceof DataTypeExpr)
      && !this.matchSet(this._constructor.TYPE_TOKENS, { advance: false })
    ) {
      this.retreat(index);
      return this.parseTypes();
    }

    return this.parseColumnDef(thisExpr);
  }

  parseAtTimeZone (thisExpr?: Expression): Expression | undefined {
    if (!this.matchTextSeq([
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
      column.setArgKey('joinMark', this.match(TokenType.JOIN_MARKER));
    }

    return column;
  }

  parseColumnReference (): Expression | undefined {
    let thisExpr = this.parseField();

    if (
      !thisExpr
      && this.match(TokenType.VALUES, { advance: false })
      && this._constructor.VALUES_FOLLOWED_BY_PAREN
      && (!this.next || this.next.tokenType !== TokenType.L_PAREN)
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

    while (this.match(TokenType.COLON)) {
      const startIndex = this.index;

      // Snowflake allows reserved keywords as json keys but advance_any() excludes TokenType.SELECT from any_tokens=True
      let path: ExpressionValue | undefined = this.parseColumnOps(
        this.parseField({
          anyToken: true,
          tokens: new Set([TokenType.SELECT]),
        }),
      );

      // The cast :: operator has a lower precedence than the extraction operator :, so
      // we rearrange the AST appropriately to avoid casting the JSON path
      while (path instanceof CastExpr) {
        casts.push(path.args.to as DataTypeExpr);
        path = path.args.this;
      }

      let endToken: Token;
      if (0 < casts.length) {
        const dcolonOffset = this.tokens.slice(startIndex).findIndex(
          (t) => t.tokenType === TokenType.DCOLON,
        );
        endToken = this.tokens[startIndex + dcolonOffset - 1];
      } else {
        endToken = this.prev as Token;
      }

      if (path) {
        // Escape single quotes from Snowflake's colon extraction (e.g. col:"a'b") as
        // it'll roundtrip to a string literal in GET_PATH
        if (path instanceof IdentifierExpr && path.args.quoted) {
          escape = true;
        }

        jsonPath.push(this.findSql(this.tokens[startIndex], endToken));
      }
    }

    // The VARIANT extract in Snowflake/Databricks is parsed as a JsonExtract; Snowflake uses the json_path in GET_PATH() while
    // Databricks transforms it back to the colon/dot notation
    if (0 < jsonPath.length) {
      const jsonPathExpr = this.dialect.toJsonPath?.(LiteralExpr.string('.' + jsonPath.join('.')));

      if (jsonPathExpr) {
        jsonPathExpr.setArgKey('escape', escape);
      }

      thisExpr = this.expression(
        JsonExtractExpr,
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

    while (this.matchSet(Object.keys(this._constructor.COLUMN_OPERATORS) as TokenType[])) {
      const opToken = this.prev?.tokenType ?? TokenType.UNKNOWN;
      const op = this._constructor.COLUMN_OPERATORS[opToken];

      let field: Expression | undefined;
      if (this._constructor.CAST_COLUMN_OPERATORS.has(opToken)) {
        field = this.parseDcolon();
        if (!field) {
          this.raiseError('Expected type', this.curr);
        }
      } else if (op && this.curr) {
        field = this.parseColumnReference() || this.parseBitwise();
        if (field instanceof ColumnExpr && this.match(TokenType.DOT, { advance: false })) {
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
        current = op.call(this, current, field);
      } else if (current instanceof ColumnExpr && !current.args.catalog) {
        current = this.expression(
          ColumnExpr,
          {
            comments: current.comments,
            this: field,
            table: current.args.this,
            db: current.args.table,
            catalog: current.args.db,
          },
        );
      } else if (field instanceof WindowExpr) {
        // Move the exp.Dot's to the window's function
        const windowFunc = this.expression(DotExpr, {
          this: current,
          expression: field.args.this,
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
    const start = this.prev;
    const exists = allowExists ? this.parseExists() : undefined;

    this.match(TokenType.ON);

    const materialized = this.matchTextSeq('MATERIALIZED') || undefined;
    const kind = (this.matchSet(this._constructor.CREATABLES) || undefined) && this.prev;
    if (!kind) {
      return this.parseAsCommand(start);
    }

    let thisExpr: Expression | undefined;
    if (kind.tokenType === TokenType.FUNCTION || kind.tokenType === TokenType.PROCEDURE) {
      thisExpr = this.parseUserDefinedFunction({ kind: kind.tokenType });
    } else if (kind.tokenType === TokenType.TABLE) {
      thisExpr = this.parseTable({ aliasTokens: this._constructor.COMMENT_TABLE_ALIAS_TOKENS });
    } else if (kind.tokenType === TokenType.COLUMN) {
      thisExpr = this.parseColumn();
    } else {
      thisExpr = this.parseIdVar();
    }

    this.match(TokenType.IS);

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

      if (this.matchTextSeq('DELETE')) {
        return this.expression(MergeTreeTtlActionExpr, {
          this: thisExpr,
          delete: true,
        });
      }
      if (this.matchTextSeq('RECOMPRESS')) {
        return this.expression(MergeTreeTtlActionExpr, {
          this: thisExpr,
          recompress: this.parseBitwise(),
        });
      }
      if (this.matchTextSeq(['TO', 'DISK'])) {
        return this.expression(MergeTreeTtlActionExpr, {
          this: thisExpr,
          toDisk: this.parseString(),
        });
      }
      if (this.matchTextSeq(['TO', 'VOLUME'])) {
        return this.expression(MergeTreeTtlActionExpr, {
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
    if (group && this.match(TokenType.SET)) {
      aggregates = this.parseCsv(this.parseSetItem);
    }

    return this.expression(MergeTreeTtlExpr, {
      expressions,
      where,
      group,
      aggregates,
    });
  }

  parseStatement (): Expression | undefined {
    if (this.curr === undefined) {
      return undefined;
    }

    const statementTokens = Object.keys(this._constructor.STATEMENT_PARSERS) as TokenType[];
    if (this.matchSet(statementTokens)) {
      const comments = this.prevComments;
      const stmt = this._constructor.STATEMENT_PARSERS[this.prev?.tokenType ?? TokenType.UNKNOWN]?.call(this);
      stmt?.addComments(comments, { prepend: true });
      return stmt;
    }

    if (this.matchSet(this._dialectConstructor.tokenizerClass.COMMANDS)) {
      return this.parseCommand();
    }

    let expression = this.parseExpression();
    expression = expression ? this.parseSetOperations(expression) : this.parseSelect();
    return this.parseQueryModifiers(expression);
  }

  parsePartitionedByBucketOrTruncate (): Expression | undefined {
    // Check for L_PAREN without advancing
    if (this.curr?.tokenType !== TokenType.L_PAREN) {
      // Partitioning by bucket or truncate follows the syntax:
      // PARTITION BY (BUCKET(..) | TRUNCATE(..))
      // If we don't have parenthesis after each keyword, we should instead parse this as an identifier
      this.retreat(this.index - 1);
      return undefined;
    }

    const ExprClass = (
      this.prev?.text.toUpperCase() === 'BUCKET'
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

  /**
   * Appends an error in the list of recorded errors or raises it, depending on the chosen
   * error level setting.
   */
  raiseError (message: string, token?: Token) {
    const errorToken = token || this.curr || this.prev || Token.string('');
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

  /**
   * Logs or raises any found errors, depending on the chosen error level setting.
   */
  checkErrors (): void {
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

  /**
   * Creates a new, validated Expression.
   *
   * @param expClass - The expression class to instantiate.
   * @param options - Optional arguments including token, comments, and other expression arguments.
   * @returns The target expression.
   */
  expression<E extends Expression> (
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expClass: new (args: any) => E,
    options: {
      token?: Token;
      comments?: string[];
      [index: string]: unknown;
    } = {},
  ): E {
    const {
      token, comments, ...kwargs
    } = options;

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
      this.addComments(instance);
    }

    return this.validateExpression(instance);
  }

  protected addComments (expression: Expression | undefined): void {
    if (expression && this.prevComments) {
      expression.addComments(this.prevComments);
      this.prevComments = undefined;
    }
  }

  parseIdVar (options: {
    anyToken?: boolean;
    tokens?: Set<TokenType>;
  } = {}): Expression | undefined {
    const {
      anyToken = true,
      tokens,
    } = options;

    let expression = this.parseIdentifier();
    if (!expression && (
      (anyToken && this.advanceAny()) || this.matchSet(tokens || this._constructor.ID_VAR_TOKENS)
    )) {
      const quoted = this.prev?.tokenType === TokenType.STRING;
      expression = this.identifierExpression(undefined, { quoted });
    }

    return expression;
  }

  parseGrantPrincipal (): GrantPrincipalExpr | undefined {
    const kind = (this.matchTexts(['ROLE', 'GROUP']) || undefined) && this.prev?.text.toUpperCase();
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

    while (this.curr && !this.matchSet(this._constructor.PRIVILEGE_FOLLOW_TOKENS, { advance: false })) {
      privilegeParts.push(this.curr.text.toUpperCase());
      this.advance();
    }

    const thisVar = this.expression(VarExpr, { this: privilegeParts.join(' ') });
    const expressions = this.match(TokenType.L_PAREN, { advance: false })
      ? this.parseWrappedCsv(() => this.parseColumn())
      : undefined;

    return this.expression(GrantPrivilegeExpr, {
      this: thisVar,
      expressions,
    });
  }

  matchTexts (texts: string | string[] | Set<string>, options: { advance?: boolean } = {}): boolean {
    const { advance = true } = options;
    const textsArray = Array.from(texts instanceof Set ? texts : ensureList(texts));
    if (
      this.curr
      && this.curr.tokenType !== TokenType.STRING
      && textsArray.includes(this.curr.text.toUpperCase())
    ) {
      if (advance) {
        this.advance();
      }
      return true;
    }
    return false;
  }

  matchSet (types: Set<TokenType> | TokenType[], options: { advance?: boolean } = {}): boolean {
    const { advance = true } = options;
    if (!this.curr) {
      return false;
    }

    const hasType = Array.isArray(types)
      ? types.includes(this.curr.tokenType)
      : types.has(this.curr.tokenType);

    if (hasType) {
      if (advance) {
        this.advance();
      }
      return true;
    }

    return false;
  }

  matchPair (tokenTypeA: TokenType, tokenTypeB: TokenType, options: { advance?: boolean } = {}): boolean {
    const { advance = true } = options;
    if (!this.curr || !this.next) {
      return false;
    }

    if (this.curr.tokenType === tokenTypeA && this.next.tokenType === tokenTypeB) {
      if (advance) {
        this.advance(2);
      }
      return true;
    }

    return false;
  }

  matchLParen (expression?: Expression): void {
    if (!this.match(TokenType.L_PAREN, {
      advance: true,
      expression,
    })) {
      this.raiseError('Expecting (');
    }
  }

  matchRParen (expression?: Expression): void {
    if (!this.match(TokenType.R_PAREN, {
      advance: true,
      expression,
    })) {
      this.raiseError('Expecting )');
    }
  }

  matchTextSeq (texts: string | string[], options: { advance?: boolean } = {}): boolean {
    const { advance = true } = options;
    const textArray = ensureList(texts);

    const index = this.index;
    for (const text of textArray) {
      if (
        this.curr
        && this.curr.tokenType !== TokenType.STRING
        && this.curr.text.toUpperCase() === text
      ) {
        this.advance();
      } else {
        this.retreat(index);
        return false;
      }
    }

    if (!advance) {
      this.retreat(index);
    }

    return true;
  }

  replaceLambda (node: Expression | undefined, expressions: Expression[]): Expression | undefined {
    if (!node) {
      return node;
    }

    const lambdaTypes: Record<string, ExpressionValue | false> = {};
    for (const e of expressions) {
      lambdaTypes[e.name] = e.args.to || false;
    }

    for (const column of node.findAll(ColumnExpr)) {
      const typ = lambdaTypes[column.parts[0]?.name || ''];
      if (typ !== undefined) {
        const colThis = column.args.this;
        assertIsInstanceOf(colThis, IdentifierExpr);
        let dotOrId: DotExpr | IdentifierExpr | StarExpr | CastExpr = column.table ? column.toDot() : colThis;

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
    const start = this.prev;

    if (this.match(TokenType.L_PAREN)) {
      this.retreat(this.index - 2);
      return this.parseFunction();
    }

    const isDatabase = this.match(TokenType.DATABASE) || undefined;

    this.match(TokenType.TABLE);

    const exists = this.parseExists({ not: false });

    const expressions = this.parseCsv(
      () => this.parseTable({
        schema: true,
        isDbReference: isDatabase,
      }),
    );

    const cluster = this.match(TokenType.ON) ? this.parseOnProperty() : undefined;

    let identity: string | undefined;
    if (this.matchTextSeq(['RESTART', 'IDENTITY'])) {
      identity = 'RESTART';
    } else if (this.matchTextSeq(['CONTINUE', 'IDENTITY'])) {
      identity = 'CONTINUE';
    } else {
      identity = undefined;
    }

    let option: string | undefined;
    if (this.matchTextSeq('CASCADE') || this.matchTextSeq('RESTRICT')) {
      option = this.prev?.text;
    } else {
      option = undefined;
    }

    const partition = this.parsePartition();

    if (this.curr) {
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
    const thisExpr = this.parseOrdered(() => this.parseOpClass());

    if (!this.match(TokenType.WITH)) {
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
    this.match(TokenType.EQ);
    this.match(TokenType.L_PAREN);

    const opts: Expression[] = [];
    let option: Expression | undefined;
    while (this.curr && !this.match(TokenType.R_PAREN)) {
      if (this.matchTextSeq(['FORMAT_NAME', '='])) {
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
    while (this.curr && !this.match(TokenType.R_PAREN, { advance: false })) {
      const option = this.parseVar({ anyToken: true });
      const prev = this.prev?.text.toUpperCase();

      this.match(TokenType.EQ);
      this.match(TokenType.ALIAS);

      const param = this.expression(CopyParameterExpr, { this: option });

      if (prev && this._constructor.COPY_INTO_VARLEN_OPTIONS.has(prev) && this.match(TokenType.L_PAREN, { advance: false })) {
        param.setArgKey('expressions', this.parseWrappedOptions());
      } else if (prev === 'FILE_FORMAT') {
        param.setArgKey('expression', this.parseField());
      } else if (
        prev === 'FORMAT'
        && this.prev?.tokenType === TokenType.ALIAS
        && this.matchTexts(['AVRO', 'JSON'])
      ) {
        param.setArgKey('this', this.expression(VarExpr, { this: `FORMAT AS ${this.prev?.text.toUpperCase()}` }));
        param.setArgKey('expression', this.parseField());
      } else {
        param.setArgKey('expression', this.parseUnquotedField() || this.parseBracket());
      }

      options.push(param);
      if (sep) {
        this.match(sep);
      }
    }

    return options;
  }

  parseCredentials (): CredentialsExpr | undefined {
    const expr = this.expression(CredentialsExpr, {});

    if (this.matchTextSeq(['STORAGE_INTEGRATION', '='])) {
      expr.setArgKey('storage', this.parseField());
    }
    if (this.matchTextSeq('CREDENTIALS')) {
      const creds = this.match(TokenType.EQ) ? this.parseWrappedOptions() : this.parseField();
      expr.setArgKey('credentials', creds);
    }
    if (this.matchTextSeq('ENCRYPTION')) {
      expr.setArgKey('encryption', this.parseWrappedOptions());
    }
    if (this.matchTextSeq('IAM_ROLE')) {
      expr.setArgKey(
        'iamRole',
        this.match(TokenType.DEFAULT)
          ? this.expression(VarExpr, { this: this.prev?.text })
          : this.parseField(),
      );
    }
    if (this.matchTextSeq('REGION')) {
      expr.setArgKey('region', this.parseField());
    }

    return expr;
  }

  parseFileLocation (): Expression | undefined {
    return this.parseField();
  }

  parseCopy (): CopyExpr | CommandExpr {
    const start = this.prev;

    this.match(TokenType.INTO);

    const thisExpr = this.match(TokenType.L_PAREN, { advance: false })
      ? this.parseSelect({
        nested: true,
        parseSubqueryAlias: false,
      })
      : this.parseTable({ schema: true });

    const kind = this.match(TokenType.FROM) || !this.matchTextSeq('TO') || undefined;

    let files = this.parseCsv(() => this.parseFileLocation());
    if (this.match(TokenType.EQ, { advance: false })) {
      this.advance(-1);
      files = [];
    }

    const credentials = this.parseCredentials();

    this.matchTextSeq('WITH');

    const params = this.parseWrapped(() => this.parseCopyParameters(), { optional: true });

    if (this.curr) {
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
        form: this.match(TokenType.COMMA) && this.parseVar(),
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
      to: this.matchTextSeq('TO') && this.parseVar(),
    });
  }

  parseStarOps (): Expression | undefined {
    const starToken = this.prev;

    if (this.matchTextSeq(['COLUMNS', '('], { advance: false })) {
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

  parseGrantRevokeCommon (): {
    privileges: GrantPrivilegeExpr[] | undefined;
    kind: string | undefined;
    securable: Expression | undefined;
  } {
    const privileges = this.parseCsv(() => this.parseGrantPrivilege());

    this.match(TokenType.ON);
    const kind = (this.matchSet(this._constructor.CREATABLES) || undefined) && this.prev?.text.toUpperCase();

    const securable = this.tryParse(() => this.parseTableParts());

    return {
      privileges,
      kind,
      securable,
    };
  }

  parseGrant (): GrantExpr | CommandExpr {
    const start = this.prev;

    const {
      privileges,
      kind,
      securable,
    } = this.parseGrantRevokeCommon();

    if (!securable || !this.matchTextSeq('TO')) {
      return this.parseAsCommand(start);
    }

    const principals = this.parseCsv(() => this.parseGrantPrincipal());

    const grantOption = this.matchTextSeq([
      'WITH',
      'GRANT',
      'OPTION',
    ]) || undefined;

    if (this.curr) {
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
    const start = this.prev;

    const grantOption = this.matchTextSeq([
      'GRANT',
      'OPTION',
      'FOR',
    ]) || undefined;

    const {
      privileges,
      kind,
      securable,
    } = this.parseGrantRevokeCommon();

    if (!securable || !this.matchTextSeq('FROM')) {
      return this.parseAsCommand(start);
    }

    const principals = this.parseCsv(() => this.parseGrantPrincipal());

    let cascade: string | undefined;
    if (this.matchTexts(['CASCADE', 'RESTRICT'])) {
      cascade = this.prev?.text.toUpperCase();
    }

    if (this.curr) {
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
      return (this.match(TokenType.COMMA) || this.matchTextSeq(text)) && this.parseBitwise();
    };

    return this.expression(
      OverlayExpr,
      {
        this: this.parseBitwise(),
        expression: parseOverlayArg('PLACING'),
        fromPosition: parseOverlayArg('FROM'),
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

    if (this.match(TokenType.DISTINCT)) {
      args.push(this.expression(DistinctExpr, { expressions: [this.parseLambda()] }));
      this.match(TokenType.COMMA);
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

  identifierExpression (token?: Token, kwargs?: { [index: string]: unknown }): IdentifierExpr {
    return this.expression(IdentifierExpr, {
      token: token || this.prev,
      ...kwargs,
    });
  }

  parseTokens (parseMethod: () => Expression | undefined, expressions: Partial<Record<TokenType, typeof Expression>>): Expression | undefined {
    let thisExpr = parseMethod();

    const expressionTokens = new Set(Object.keys(expressions)) as Set<TokenType>;
    while (this.matchSet(expressionTokens)) {
      const exprType = expressions[this.prev?.tokenType ?? TokenType.UNKNOWN];
      if (!exprType) break;
      thisExpr = this.expression(
        exprType,
        {
          this: thisExpr,
          comments: this.prevComments,
          expression: parseMethod(),
        },
      );
    }

    return thisExpr;
  }

  parseWrappedIdVars (options: { optional?: boolean } = {}): Expression[] {
    const { optional = false } = options;
    return this.parseWrappedCsv(() => this.parseIdVar(), { optional });
  }

  parseWrappedCsv (parseMethod: () => Expression | undefined, options: {
    sep?: TokenType;
    optional?: boolean;
  } = {}): Expression[] {
    const {
      sep = TokenType.COMMA,
      optional = false,
    } = options;
    return this.parseWrapped(
      () => this.parseCsv(parseMethod, { sep }),
      { optional },
    );
  }

  parseWrapped<T> (parseMethod: () => T, options: { optional?: boolean } = {}): T {
    const { optional = false } = options;
    const wrapped = this.match(TokenType.L_PAREN) || undefined;
    if (!wrapped && !optional) {
      this.raiseError('Expecting (');
    }
    const parseResult = parseMethod();
    if (wrapped) {
      this.matchRParen();
    }
    return parseResult;
  }

  parseExpressions (): Expression[] {
    return this.parseCsv(() => this.parseExpression());
  }

  parseSelectOrExpression (options: { alias?: boolean } = {}): Expression | undefined {
    const { alias = false } = options;
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
    if (this.matchTexts(this._constructor.TRANSACTION_KIND)) {
      thisText = this.prev?.text;
    }

    this.matchTexts(['TRANSACTION', 'WORK']);

    const modes: string[] = [];
    while (true) {
      const mode: string[] = [];
      while (this.match(TokenType.VAR) || this.match(TokenType.NOT)) {
        mode.push(this.prev?.text ?? '');
      }

      if (0 < mode.length) {
        modes.push(mode.join(' '));
      }
      if (!this.match(TokenType.COMMA)) {
        break;
      }
    }

    return this.expression(TransactionExpr, {
      this: thisText,
      modes,
    });
  }

  parseStar (): Expression | undefined {
    if (this.match(TokenType.STAR)) {
      return this._constructor.PRIMARY_PARSERS[TokenType.STAR]?.call(this, this.prev as Token);
    }
    return this.parsePlaceholder();
  }

  parseParameter (): ParameterExpr {
    const thisExpr = this.parseIdentifier() || this.parsePrimaryOrVar();
    return this.expression(ParameterExpr, { this: thisExpr });
  }

  parsePlaceholder (): Expression | undefined {
    if (this.matchSet(new Set(Object.keys(this._constructor.PLACEHOLDER_PARSERS)) as Set<TokenType>)) {
      const placeholder = this._constructor.PLACEHOLDER_PARSERS[this.prev?.tokenType ?? TokenType.UNKNOWN]?.call(this);
      if (placeholder) {
        return placeholder;
      }
      this.advance(-1);
    }
    return undefined;
  }

  parseStarOp (...keywords: string[]): Expression[] | undefined {
    if (!this.matchTexts(keywords)) {
      return undefined;
    }
    if (this.match(TokenType.L_PAREN, { advance: false })) {
      return this.parseWrappedCsv(() => this.parseExpression());
    }

    const expression = this.parseAlias(this.parseDisjunction(), { explicit: true });
    return expression ? [expression] : undefined;
  }

  parseCsv<E extends Expression> (parseMethod: () => E | undefined, options: { sep?: TokenType } = {}): E[] {
    const { sep = TokenType.COMMA } = options;
    let parseResult = parseMethod();
    const items: E[] = parseResult !== undefined ? [parseResult] : [];

    while (this.match(sep)) {
      this.addComments(parseResult);
      parseResult = parseMethod();
      if (parseResult !== undefined) {
        items.push(parseResult);
      }
    }

    return items;
  }

  parseIdentifier (): Expression | undefined {
    if (this.match(TokenType.IDENTIFIER)) {
      return this.identifierExpression(undefined, { quoted: true });
    }
    return this.parsePlaceholder();
  }

  parseVar (options: {
    anyToken?: boolean;
    tokens?: Set<TokenType>;
    upper?: boolean;
  } = {}): Expression | undefined {
    const {
      anyToken = false,
      tokens,
      upper = false,
    } = options;

    if (
      (anyToken && this.advanceAny())
      || this.match(TokenType.VAR)
      || (tokens && this.matchSet(tokens))
    ) {
      const text = upper ? (this.prev?.text ?? '').toUpperCase() : this.prev?.text ?? '';
      return this.expression(VarExpr, { this: text });
    }
    return this.parsePlaceholder();
  }

  advanceAny (options: { ignoreReserved?: boolean } = {}): Token | undefined {
    const { ignoreReserved = false } = options;
    if (this.curr && (ignoreReserved || !this._constructor.RESERVED_TOKENS.has(this.curr.tokenType))) {
      this.advance();
      return this.prev;
    }
    return undefined;
  }

  parseVarOrString (options: { upper?: boolean } = {}): Expression | undefined {
    const { upper = false } = options;
    return this.parseString() || this.parseVar({
      anyToken: true,
      upper,
    });
  }

  parsePrimaryOrVar (): Expression | undefined {
    return this.parsePrimary() || this.parseVar({ anyToken: true });
  }

  parseNull (): Expression | undefined {
    if (this.matchSet(new Set([TokenType.NULL, TokenType.UNKNOWN]))) {
      return this._constructor.PRIMARY_PARSERS[TokenType.NULL]?.call(this, this.prev as Token);
    }
    return this.parsePlaceholder();
  }

  parseBoolean (): Expression | undefined {
    if (this.match(TokenType.TRUE)) {
      return this._constructor.PRIMARY_PARSERS[TokenType.TRUE]?.call(this, this.prev as Token);
    }
    if (this.match(TokenType.FALSE)) {
      return this._constructor.PRIMARY_PARSERS[TokenType.FALSE]?.call(this, this.prev as Token);
    }
    return this.parsePlaceholder();
  }

  parseString (): Expression | undefined {
    if (this.matchSet(new Set(Object.keys(this._constructor.STRING_PARSERS)) as Set<TokenType>)) {
      return this._constructor.STRING_PARSERS[this.prev?.tokenType ?? TokenType.UNKNOWN]?.call(this, this.prev as Token);
    }
    return this.parsePlaceholder();
  }

  parseStringAsIdentifier (): IdentifierExpr | undefined {
    const id = (this.match(TokenType.STRING) || undefined) && this.prev?.text;
    const output = id === false ? undefined : toIdentifier(id, { quoted: true });
    if (output && this.prev) {
      output.updatePositions(this.prev);
    }
    return output;
  }

  parseNumber (): Expression | undefined {
    if (this.matchSet(new Set(Object.keys(this._constructor.NUMERIC_PARSERS)) as Set<TokenType>)) {
      return this._constructor.NUMERIC_PARSERS[this.prev?.tokenType ?? TokenType.UNKNOWN]?.call(this, this.prev as Token);
    }
    return this.parsePlaceholder();
  }

  parseHavingMax (thisExpr: Expression | undefined): Expression | undefined {
    if (this.match(TokenType.HAVING)) {
      this.matchTexts(['MAX', 'MIN']);
      const max = this.prev?.text.toUpperCase() !== 'MIN';
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
    const { alias = false } = options;
    const func = thisExpr;
    const comments = func instanceof Expression ? func.comments : undefined;

    if (this.matchTextSeq(['WITHIN', 'GROUP'])) {
      const order = this.parseWrapped(() => this.parseOrder());
      thisExpr = this.expression(WithinGroupExpr, {
        this: thisExpr,
        expression: order,
      });
    }

    if (this.matchPair(TokenType.FILTER, TokenType.L_PAREN)) {
      this.match(TokenType.WHERE);
      thisExpr = this.expression(
        FilterExpr,
        {
          this: thisExpr,
          expression: this.parseWhere({ skipWhereToken: true }),
        },
      );
      this.matchRParen();
    }

    if (thisExpr instanceof AggFuncExpr) {
      const ignoreRespect = thisExpr.find([IgnoreNullsExpr, RespectNullsExpr]);

      if (ignoreRespect && ignoreRespect !== thisExpr) {
        ignoreRespect.replace(ignoreRespect.args.this);
        thisExpr = this.expression(ignoreRespect._constructor, { this: thisExpr });
      }
    }

    thisExpr = this.parseRespectOrIgnoreNulls(thisExpr);

    let over: string | undefined;
    if (alias) {
      over = undefined;
      this.match(TokenType.ALIAS);
    } else if (!this.matchSet(this._constructor.WINDOW_BEFORE_PAREN_TOKENS)) {
      return thisExpr;
    } else {
      over = this.prev?.text.toUpperCase();
    }

    if (comments && func instanceof Expression) {
      func.popComments();
    }

    if (!this.match(TokenType.L_PAREN)) {
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

    let first: boolean | undefined = this.match(TokenType.FIRST) || undefined;
    if (this.matchTextSeq('LAST')) {
      first = false;
    }

    const [partition, order] = this.parsePartitionAndOrder();
    const kind = (this.matchSet(new Set([TokenType.ROWS, TokenType.RANGE])) || undefined) && this.prev?.text;

    let spec: WindowSpecExpr | undefined;
    if (kind) {
      this.match(TokenType.BETWEEN);
      const start = this.parseWindowSpec();

      const end = this.match(TokenType.AND) ? this.parseWindowSpec() : {};
      const exclude = this.matchTextSeq('EXCLUDE')
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

    this.matchRParen();

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

    if (this.matchSet(this._constructor.WINDOW_BEFORE_PAREN_TOKENS, { advance: false })) {
      return this.parseWindow(window, { alias });
    }

    return window;
  }

  parsePartitionAndOrder (): [Expression[], Expression | undefined] {
    return [this.parsePartitionBy(), this.parseOrder()];
  }

  parseWindowSpec (): {
    value?: string | Expression;
    side?: string;
  } {
    this.match(TokenType.BETWEEN);

    return {
      value:
        (this.matchTextSeq('UNBOUNDED') && 'UNBOUNDED')
        || (this.matchTextSeq(['CURRENT', 'ROW']) && 'CURRENT ROW')
        || this.parseBitwise()
        || undefined,
      side: (this.matchTexts(this._constructor.WINDOW_SIDES) || undefined) && this.prev?.text,
    };
  }

  parseAlias (thisExpr: Expression | undefined, options: { explicit?: boolean } = {}): Expression | undefined {
    const { explicit = false } = options;

    if (this.canParseLimitOrOffset()) {
      return thisExpr;
    }

    const anyToken = this.match(TokenType.ALIAS);
    const comments = this.prevComments ?? [];

    if (explicit && !anyToken) {
      return thisExpr;
    }

    if (this.match(TokenType.L_PAREN)) {
      const aliases = this.expression(
        AliasesExpr,
        {
          comments,
          this: thisExpr,
          expressions: this.parseCsv(() => this.parseIdVar({ anyToken })),
        },
      );
      this.matchRParen(aliases);
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
      const column = (thisExpr as AliasExpr).args.this;

      if ((!thisExpr.comments || thisExpr.comments.length === 0) && column && column.comments && 0 < column.comments.length) {
        thisExpr.comments = column.popComments();
      }
    }

    return thisExpr;
  }

  parseOpenJson (): OpenJsonExpr {
    const thisExpr = this.parseBitwise();
    const path = (this.match(TokenType.COMMA) || undefined) && this.parseString();

    const parseOpenJsonColumnDef = (): OpenJsonColumnDefExpr => {
      const thisCol = this.parseField({ anyToken: true });
      const kind = this.parseTypes();
      const pathCol = this.parseString();
      const asJson = this.matchPair(TokenType.ALIAS, TokenType.JSON) || undefined;

      return this.expression(
        OpenJsonColumnDefExpr,
        {
          this: thisCol,
          kind,
          path: pathCol,
          asJson,
        },
      );
    };

    let expressions: OpenJsonColumnDefExpr[] | undefined;
    if (this.matchPair(TokenType.R_PAREN, TokenType.WITH)) {
      this.matchLParen();
      expressions = this.parseCsv(parseOpenJsonColumnDef);
    }

    return this.expression(OpenJsonExpr, {
      this: thisExpr,
      path,
      expressions,
    });
  }

  parsePosition (options: { haystackFirst?: boolean } = {}): StrPositionExpr {
    const { haystackFirst = false } = options;
    const args = this.parseCsv(() => this.parseBitwise());

    if (this.match(TokenType.IN)) {
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
    const args = this.parseCsv(() => this.parseBitwise()) as Expression[];

    let start: Expression | undefined;
    let length: Expression | undefined;

    while (this.curr) {
      if (this.match(TokenType.FROM)) {
        start = this.parseBitwise();
      } else if (this.match(TokenType.FOR)) {
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

    if (this.matchTexts(this._constructor.TRIM_TYPES)) {
      position = this.prev?.text.toUpperCase();
    }

    let thisExpr = this.parseBitwise();
    if (this.matchSet(new Set([TokenType.FROM, TokenType.COMMA]))) {
      const invertOrder = this.prev?.tokenType === TokenType.FROM || this._constructor.TRIM_PATTERN_FIRST;
      expression = this.parseBitwise();

      if (invertOrder) {
        [thisExpr, expression] = [expression, thisExpr];
      }
    }

    if (this.match(TokenType.COLLATE)) {
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
    return (this.match(TokenType.WINDOW) || undefined) && this.parseCsv(() => this.parseNamedWindow());
  }

  parseNamedWindow (): Expression | undefined {
    return this.parseWindow(this.parseIdVar(), { alias: true });
  }

  parseRespectOrIgnoreNulls (thisExpr: Expression | undefined): Expression | undefined {
    if (this.matchTextSeq(['IGNORE', 'NULLS'])) {
      return this.expression(IgnoreNullsExpr, { this: thisExpr });
    }
    if (this.matchTextSeq(['RESPECT', 'NULLS'])) {
      return this.expression(RespectNullsExpr, { this: thisExpr });
    }
    return thisExpr;
  }

  parseJsonObject (options: { agg?: boolean } = {}): JsonObjectExpr | JsonObjectAggExpr {
    const { agg = false } = options;
    const star = this.parseStar();
    const expressions = star
      ? [star]
      : this.parseCsv(() => this.parseFormatJson(this.parseJsonKeyValue()));

    const nullHandling = this.parseOnHandling('NULL', ['NULL', 'ABSENT']);

    let uniqueKeys: boolean | undefined;
    if (this.matchTextSeq(['WITH', 'UNIQUE'])) {
      uniqueKeys = true;
    } else if (this.matchTextSeq(['WITHOUT', 'UNIQUE'])) {
      uniqueKeys = false;
    }

    this.matchTextSeq('KEYS');

    const returnType = (this.matchTextSeq('RETURNING') || undefined) && this.parseFormatJson(this.parseType());
    const encoding = (this.matchTextSeq('ENCODING') || undefined) && this.parseVar();

    return this.expression(
      agg ? JsonObjectAggExpr : JsonObjectExpr,
      {
        expressions,
        nullHandling,
        uniqueKeys,
        returnType,
        encoding,
      },
    );
  }

  parseJsonColumnDef (): JsonColumnDefExpr {
    let thisExpr: Expression | undefined;
    let ordinality: boolean | undefined;
    let kind: Expression | undefined;
    let nested: boolean | undefined;

    if (!this.matchTextSeq('NESTED')) {
      thisExpr = this.parseIdVar();
      ordinality = this.matchPair(TokenType.FOR, TokenType.ORDINALITY) || undefined;
      kind = this.parseTypes({ allowIdentifiers: false });
      nested = undefined;
    } else {
      thisExpr = undefined;
      ordinality = undefined;
      kind = undefined;
      nested = true;
    }

    const path = (this.matchTextSeq('PATH') || undefined) && this.parseString();
    const nestedSchema = nested && this.parseJsonSchema();

    return this.expression(
      JsonColumnDefExpr,
      {
        this: thisExpr,
        kind,
        path,
        nestedSchema,
        ordinality,
      },
    );
  }

  parseJsonSchema (): JsonSchemaExpr {
    this.matchTextSeq('COLUMNS');
    return this.expression(
      JsonSchemaExpr,
      {
        expressions: this.parseWrappedCsv(() => this.parseJsonColumnDef(), { optional: true }),
      },
    );
  }

  parseJsonTable (): JsonTableExpr {
    const thisExpr = this.parseFormatJson(this.parseBitwise());
    if (!thisExpr) {
      this.raiseError('Expected expression for JSON_TABLE');
    }
    const path = (this.match(TokenType.COMMA) || undefined) && this.parseString();
    const errorHandling = this.parseOnHandling('ERROR', ['ERROR', 'NULL']);
    const emptyHandling = this.parseOnHandling('EMPTY', ['ERROR', 'NULL']);
    const schema = this.parseJsonSchema();

    return new JsonTableExpr({
      this: thisExpr,
      schema,
      path,
      errorHandling,
      emptyHandling,
    });
  }

  parseMatchAgainst (): MatchAgainstExpr {
    let expressions: Expression[];

    if (this.matchTextSeq('TABLE')) {
      expressions = [];
      const table = this.parseTable();
      if (table) {
        expressions = [table];
      }
    } else {
      expressions = this.parseCsv(() => this.parseColumn());
    }

    this.matchTextSeq([
      ')',
      'AGAINST',
      '(',
    ]);

    const thisExpr = this.parseString();

    let modifier: string | undefined;
    if (this.matchTextSeq([
      'IN',
      'NATURAL',
      'LANGUAGE',
      'MODE',
    ])) {
      modifier = 'IN NATURAL LANGUAGE MODE';
      if (this.matchTextSeq([
        'WITH',
        'QUERY',
        'EXPANSION',
      ])) {
        modifier = `${modifier} WITH QUERY EXPANSION`;
      }
    } else if (this.matchTextSeq([
      'IN',
      'BOOLEAN',
      'MODE',
    ])) {
      modifier = 'IN BOOLEAN MODE';
    } else if (this.matchTextSeq([
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

  parseJsonKeyValue (): JsonKeyValueExpr | undefined {
    this.matchTextSeq('KEY');
    const key = this.parseColumn();
    this.matchSet(this._constructor.JSON_KEY_VALUE_SEPARATOR_TOKENS);
    this.matchTextSeq('VALUE');
    const value = this.parseBitwise();

    if (!key && !value) {
      return undefined;
    }
    return this.expression(JsonKeyValueExpr, {
      this: key,
      expression: value,
    });
  }

  parseFormatJson (thisExpr: Expression | undefined): Expression | undefined {
    if (!thisExpr || !this.matchTextSeq(['FORMAT', 'JSON'])) {
      return thisExpr;
    }

    return this.expression(FormatJsonExpr, { this: thisExpr });
  }

  parseOnCondition (): OnConditionExpr | undefined {
    let empty: string | Expression | undefined;
    let error: string | Expression | undefined;

    if (this._dialectConstructor.ON_CONDITION_EMPTY_BEFORE_ERROR) {
      empty = this.parseOnHandling('EMPTY', [...this._constructor.ON_CONDITION_TOKENS]);
      error = this.parseOnHandling('ERROR', [...this._constructor.ON_CONDITION_TOKENS]);
    } else {
      error = this.parseOnHandling('ERROR', [...this._constructor.ON_CONDITION_TOKENS]);
      empty = this.parseOnHandling('EMPTY', [...this._constructor.ON_CONDITION_TOKENS]);
    }

    const nullHandling = this.parseOnHandling('NULL', [...this._constructor.ON_CONDITION_TOKENS]);

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

  parseOnHandling (on: string, values: string[]): string | Expression | undefined {
    for (const value of values) {
      if (this.matchTextSeq([
        value,
        'ON',
        on,
      ])) {
        return `${value} ON ${on}`;
      }
    }

    const index = this.index;
    if (this.match(TokenType.DEFAULT)) {
      const defaultValue = this.parseBitwise();
      if (this.matchTextSeq(['ON', on])) {
        return defaultValue;
      }

      this.retreat(index);
    }

    return undefined;
  }

  parseConvert (strict: boolean, options: { safe?: boolean } = {}): Expression | undefined {
    const { safe } = options;
    const thisExpr = this.parseBitwise();

    let to: Expression | undefined;
    if (this.match(TokenType.USING)) {
      to = this.expression(
        CharacterSetExpr,
        { this: this.parseVar({ tokens: new Set([TokenType.BINARY]) }) },
      );
    } else if (this.match(TokenType.COMMA)) {
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

  parseXmlElement (): XmlElementExpr {
    let evalname: boolean | undefined;
    let thisExpr: Expression | undefined;

    if (this.matchTextSeq('EVALNAME')) {
      evalname = true;
      thisExpr = this.parseBitwise();
    } else {
      evalname = undefined;
      this.matchTextSeq('NAME');
      thisExpr = this.parseIdVar();
    }

    return this.expression(
      XmlElementExpr,
      {
        this: thisExpr,
        expressions: (this.match(TokenType.COMMA) || undefined) && this.parseCsv(() => this.parseBitwise()),
        evalname,
      },
    );
  }

  parseXmlTable (): XmlTableExpr {
    let namespaces: XmlNamespaceExpr[] | undefined;
    let passing: Expression[] | undefined;
    let columns: Expression[] | undefined;

    if (this.matchTextSeq(['XmlNAMESPACES', '('])) {
      namespaces = this.parseXmlNamespace();
      this.matchTextSeq([')', ',']);
    }

    const thisExpr = this.parseString();

    if (this.matchTextSeq('PASSING')) {
      this.matchTextSeq(['BY', 'VALUE']);
      passing = this.parseCsv(() => this.parseColumn());
    }

    const byRef = this.matchTextSeq([
      'RETURNING',
      'SEQUENCE',
      'BY',
      'REF',
    ]) || undefined;

    if (this.matchTextSeq('COLUMNS')) {
      columns = this.parseCsv(() => this.parseFieldDef());
    }

    return this.expression(
      XmlTableExpr,
      {
        this: thisExpr,
        namespaces,
        passing,
        columns,
        byRef,
      },
    );
  }

  parseXmlNamespace (): XmlNamespaceExpr[] {
    const namespaces: XmlNamespaceExpr[] = [];

    while (true) {
      let uri: Expression | undefined;
      if (this.match(TokenType.DEFAULT)) {
        uri = this.parseString();
      } else {
        uri = this.parseAlias(this.parseString());
      }
      namespaces.push(this.expression(XmlNamespaceExpr, { this: uri }));
      if (!this.match(TokenType.COMMA)) {
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
    this.match(TokenType.TABLE);
    const thisExpr = this.parseTable();

    this.match(TokenType.COMMA);
    const args = [thisExpr, ...this.parseCsv(() => this.parseLambda())].filter((e): e is Expression => e instanceof Expression);

    const gapFill = GapFillExpr.fromArgList(args);
    return this.validateExpression(gapFill, args);
  }

  parseChar (): ChrExpr {
    return this.expression(
      ChrExpr,
      {
        expressions: this.parseCsv(() => this.parseAssignment()),
        charset: (this.match(TokenType.USING) || undefined) && this.parseVar(),
      },
    );
  }

  parseCast (strict: boolean, options: { safe?: boolean } = {}): Expression {
    const { safe } = options;
    let thisExpr = this.parseDisjunction();

    if (!this.match(TokenType.ALIAS)) {
      if (this.match(TokenType.COMMA)) {
        return this.expression(CastToStrTypeExpr, {
          this: thisExpr,
          to: this.parseString(),
        });
      }

      this.raiseError('Expected AS after CAST');
    }

    let fmt: Expression | undefined;
    let to = this.parseTypes() as DataTypeExpr | CharacterSetExpr | undefined;

    let defaultValue: Expression | undefined;
    if (this.match(TokenType.DEFAULT)) {
      defaultValue = this.parseBitwise();
      this.matchTextSeq([
        'ON',
        'CONVERSION',
        'ERROR',
      ]);
    }

    if (this.matchSet(new Set([TokenType.FORMAT, TokenType.COMMA]))) {
      const fmtString = this.parseString() as StringExpr;
      fmt = this.parseAtTimeZone(fmtString);

      if (!to) {
        to = DataTypeExpr.build(DataTypeExprKind.UNKNOWN);
      }
      if (to && DataTypeExpr.TEMPORAL_TYPES.has(to.args.this as DataTypeExprKind)) {
        thisExpr = this.expression(
          to.args.this === DataTypeExprKind.DATE ? StrToDateExpr : StrToTimeExpr,
          {
            this: thisExpr,
            format: LiteralExpr.string(
              formatTime(
                fmtString?.args.this ?? '',
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
    } else if (to.args.this === DataTypeExprKind.CHAR) {
      if (this.match(TokenType.CHARACTER_SET)) {
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

    if (this.match(TokenType.DISTINCT)) {
      args = [this.expression(DistinctExpr, { expressions: [this.parseDisjunction()] })];
      if (this.match(TokenType.COMMA)) {
        args.push(...this.parseCsv(() => this.parseDisjunction()));
      }
    } else {
      args = this.parseCsv(() => this.parseDisjunction());
    }

    let onOverflow: Expression | undefined;
    if (this.matchTextSeq(['ON', 'OVERFLOW'])) {
      if (this.matchTextSeq('ERROR')) {
        onOverflow = var_('ERROR');
      } else {
        this.matchTextSeq('TRUNCATE');
        onOverflow = this.expression(
          OverflowTruncateBehaviorExpr,
          {
            this: this.parseString(),
            withCount:
              this.matchTextSeq(['WITH', 'COUNT'])
              || !this.matchTextSeq(['WITHOUT', 'COUNT']),
          },
        );
      }
    } else {
      onOverflow = undefined;
    }

    const index = this.index;
    if (!this.match(TokenType.R_PAREN) && 0 < args.length) {
      args[0] = this.parseLimit(this.parseOrder({ thisExpr: args[0] }));
      return this.expression(GroupConcatExpr, {
        this: args[0],
        separator: seqGet(args, 1),
      });
    }

    if (!this.matchTextSeq(['WITHIN', 'GROUP'])) {
      this.retreat(index);
      return this.validateExpression(GroupConcatExpr.fromArgList(args.filter((a): a is Expression => a instanceof Expression)), args);
    }

    this.matchLParen();

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
    if (!this.matchSet(new Set([TokenType.L_BRACKET, TokenType.L_BRACE]))) {
      return thisExpr;
    }

    let parseMap: boolean;
    if (this._constructor.MAP_KEYS_ARE_ARBITRARY_EXPRESSIONS) {
      const mapToken = seqGet(this.tokens, this.index - 2);
      parseMap = mapToken !== undefined && mapToken.text.toUpperCase() === 'MAP';
    } else {
      parseMap = false;
    }

    const bracketKind = this.prev?.tokenType;
    if (
      bracketKind === TokenType.L_BRACE
      && this.curr
      && this.curr.tokenType === TokenType.VAR
      && this._constructor.ODBC_DATETIME_LITERALS[this.curr.text.toLowerCase()]
    ) {
      return this.parseOdbcDatetimeLiteral();
    }

    const expressions = this.parseCsv(() =>
      this.parseBracketKeyValue({ isMap: bracketKind === TokenType.L_BRACE }));

    if (bracketKind === TokenType.L_BRACKET && !this.match(TokenType.R_BRACKET)) {
      this.raiseError('Expected ]');
    } else if (bracketKind === TokenType.L_BRACE && !this.match(TokenType.R_BRACE)) {
      this.raiseError('Expected }');
    }

    if (bracketKind === TokenType.L_BRACE) {
      thisExpr = this.expression(
        StructExpr,
        {
          expressions: this.kvToPropEq(expressions, {
            parseMap,
          }),
        },
      );
    } else if (!thisExpr && bracketKind) {
      thisExpr = buildArrayConstructor(ArrayExpr, expressions, bracketKind, this.dialect);
    } else if (thisExpr && bracketKind) {
      const constructorType = this._constructor.ARRAY_CONSTRUCTORS[thisExpr?.name.toUpperCase() ?? ''];
      if (constructorType) {
        return buildArrayConstructor(constructorType, expressions, bracketKind, this.dialect);
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

    this.addComments(thisExpr);
    return this.parseBracket(thisExpr);
  }

  parseSlice (thisExpr: Expression | undefined): Expression | undefined {
    if (!this.match(TokenType.COLON)) {
      return thisExpr;
    }

    let end: Expression | undefined;
    if (this.matchPair(TokenType.DASH, TokenType.COLON, { advance: false })) {
      this.advance();
      end = LiteralExpr.number('1').neg();
    } else {
      end = this.parseAssignment();
    }

    const step = this.match(TokenType.COLON) ? this.parseUnary() : undefined;
    return this.expression(SliceExpr, {
      this: thisExpr,
      expression: end,
      step,
    });
  }

  parseCase (): Expression | undefined {
    if (this.match(TokenType.DOT, { advance: false })) {
      this.retreat(this.index - 1);
      return undefined;
    }

    const ifs: IfExpr[] = [];
    let defaultCase: Expression | undefined;

    const comments = this.prevComments;
    const expression = this.parseDisjunction();

    while (this.match(TokenType.WHEN)) {
      const thisExpr = this.parseDisjunction();
      this.match(TokenType.THEN);
      const then = this.parseDisjunction();
      ifs.push(this.expression(IfExpr, {
        this: thisExpr,
        true: then,
      }));
    }

    if (this.match(TokenType.ELSE)) {
      defaultCase = this.parseDisjunction();
    }

    if (!this.match(TokenType.END)) {
      if (
        defaultCase instanceof IntervalExpr
        && (typeof defaultCase.args.this === 'string' ? defaultCase.args.this : defaultCase.args.this?.sql())?.toUpperCase() === 'END'
      ) {
        defaultCase = column({ col: 'interval' });
      } else {
        this.raiseError('Expected END after CASE', this.prev);
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
    if (this.match(TokenType.L_PAREN)) {
      const args = this.parseCsv(() =>
        this.parseAlias(this.parseAssignment(), { explicit: true }));
      const thisExpr = this.validateExpression(IfExpr.fromArgList(args), args);
      this.matchRParen();
      return thisExpr;
    } else {
      const index = this.index - 1;

      if (this._constructor.NO_PAREN_IF_COMMANDS && index === 0) {
        return this.parseAsCommand(this.prev);
      }

      const condition = this.parseDisjunction();

      if (!condition) {
        this.retreat(index);
        return undefined;
      }

      this.match(TokenType.THEN);
      const trueExpr = this.parseDisjunction();
      const falseExpr = this.match(TokenType.ELSE) ? this.parseDisjunction() : undefined;
      this.match(TokenType.END);
      return this.expression(IfExpr, {
        this: condition,
        true: trueExpr,
        false: falseExpr,
      });
    }
  }

  parseNextValueFor (): Expression | undefined {
    if (!this.matchTextSeq(['VALUE', 'FOR'])) {
      this.retreat(this.index - 1);
      return undefined;
    }

    return this.expression(
      NextValueForExpr,
      {
        this: this.parseColumn(),
        order: (this.match(TokenType.OVER) || undefined) && this.parseWrapped(() => this.parseOrder()),
      },
    );
  }

  parseExtract (): ExtractExpr | AnonymousExpr {
    const thisExpr = this.parseFunction() || this.parseVarOrString({ upper: true });

    if (this.match(TokenType.FROM)) {
      return this.expression(ExtractExpr, {
        this: thisExpr,
        expression: this.parseBitwise(),
      });
    }

    if (!this.match(TokenType.COMMA)) {
      this.raiseError('Expected FROM or comma after EXTRACT', this.prev);
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
    if (!this.match(TokenType.TIMESTAMP_SNAPSHOT)) {
      this.retreat(this.index - 1);
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

  parsePrimaryKey (options: {
    wrappedOptional?: boolean;
    inProps?: boolean;
    namedPrimaryKey?: boolean;
  } = {}): PrimaryKeyColumnConstraintExpr | PrimaryKeyExpr {
    const {
      wrappedOptional = false,
      inProps = false,
      namedPrimaryKey = false,
    } = options;

    const desc =
      this.matchSet(new Set([TokenType.ASC, TokenType.DESC]))
        ? this.prev?.tokenType === TokenType.DESC
        : undefined;

    let thisExpr: Expression | undefined;
    if (
      namedPrimaryKey
      && !((this.curr?.text.toUpperCase() || '') in this._constructor.CONSTRAINT_PARSERS)
      && this.next
      && this.next.tokenType === TokenType.L_PAREN
    ) {
      thisExpr = this.parseIdVar();
    }

    if (!inProps && !this.match(TokenType.L_PAREN, { advance: false })) {
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

  parseBracketKeyValue (_options: { isMap?: boolean } = {}): Expression | undefined {
    return this.parseSlice(this.parseAlias(this.parseDisjunction(), { explicit: true }));
  }

  parseOdbcDatetimeLiteral (): Expression {
    this.match(TokenType.VAR);
    const expClass = this._constructor.ODBC_DATETIME_LITERALS[this.prev?.text ?? ''.toLowerCase()];
    const expression = this.expression(expClass, { this: this.parseString() });
    if (!this.match(TokenType.R_BRACE)) {
      this.raiseError('Expected }');
    }
    return expression;
  }

  parseUniqueKey (): Expression | undefined {
    return this.parseIdVar({ anyToken: false });
  }

  parseUnique (): UniqueColumnConstraintExpr {
    this.matchTexts(['KEY', 'INDEX']);
    return this.expression(
      UniqueColumnConstraintExpr,
      {
        nulls: this.matchTextSeq([
          'NULLS',
          'NOT',
          'DISTINCT',
        ]) || undefined,
        this: this.parseSchema({ this: this.parseUniqueKey() }),
        indexType: (this.match(TokenType.USING) || undefined) && this.advanceAny() && this.prev?.text,
        onConflict: this.parseOnConflict(),
        options: this.parseKeyConstraintOptions(),
      },
    );
  }

  parseKeyConstraintOptions (): string[] {
    const options: string[] = [];
    while (true) {
      if (!this.curr) {
        break;
      }

      if (this.match(TokenType.ON)) {
        let action: string | undefined;
        const on = this.advanceAny() && this.prev?.text;

        if (this.matchTextSeq(['NO', 'ACTION'])) {
          action = 'NO ACTION';
        } else if (this.matchTextSeq('CASCADE')) {
          action = 'CASCADE';
        } else if (this.matchTextSeq('RESTRICT')) {
          action = 'RESTRICT';
        } else if (this.matchPair(TokenType.SET, TokenType.NULL)) {
          action = 'SET NULL';
        } else if (this.matchPair(TokenType.SET, TokenType.DEFAULT)) {
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
    if (match && !this.match(TokenType.REFERENCES)) {
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
    const expressions = !this.match(TokenType.REFERENCES, { advance: false })
      ? this.parseWrappedIdVars()
      : undefined;

    const reference = this.parseReferences();
    const onOptions: Record<string, string> = {};

    while (this.match(TokenType.ON)) {
      if (!this.matchSet(new Set([TokenType.DELETE, TokenType.UPDATE]))) {
        this.raiseError('Expected DELETE or UPDATE');
      }

      const kind = this.prev?.text ?? ''.toLowerCase();

      let action: string;
      if (this.matchTextSeq(['NO', 'ACTION'])) {
        action = 'NO ACTION';
      } else if (this.match(TokenType.SET)) {
        this.matchSet(new Set([TokenType.NULL, TokenType.DEFAULT]));
        action = 'SET ' + (this.prev?.text ?? '').toUpperCase();
      } else {
        this.advance();
        action = (this.prev?.text ?? '').toUpperCase();
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
    if (this.matchSet(Object.keys(this._constructor.PRIMARY_PARSERS) as TokenType[])) {
      const tokenType = this.prev?.tokenType ?? TokenType.UNKNOWN;
      const primary = this._constructor.PRIMARY_PARSERS[tokenType]?.call(this, this.prev as Token);

      if (tokenType === TokenType.STRING) {
        const expressions = [primary];
        while (this.match(TokenType.STRING)) {
          expressions.push(LiteralExpr.string(this.prev?.text ?? ''));
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

    if (this.matchPair(TokenType.DOT, TokenType.NUMBER)) {
      return LiteralExpr.number(`0.${this.prev?.text ?? ''}`);
    }

    return this.parseParen();
  }

  parseField (options: {
    anyToken?: boolean;
    tokens?: Set<TokenType>;
    anonymousFunc?: boolean;
  } = {}): Expression | undefined {
    const {
      anyToken = false, tokens, anonymousFunc = false,
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

    if (this.matchTextSeq(['BY', 'DEFAULT'])) {
      const onNull = this.matchPair(TokenType.ON, TokenType.NULL) || undefined;
      thisExpr = this.expression(
        GeneratedAsIdentityColumnConstraintExpr,
        {
          this: false,
          onNull,
        },
      );
    } else {
      this.matchTextSeq('ALWAYS');
      thisExpr = this.expression(GeneratedAsIdentityColumnConstraintExpr, { this: true });
    }

    this.match(TokenType.ALIAS);

    if (this.matchTextSeq('ROW')) {
      const start = this.matchTextSeq('START') || undefined;
      if (!start) {
        this.match(TokenType.END);
      }
      const hidden = this.matchTextSeq('HIDDEN') || undefined;
      return this.expression(GeneratedAsRowColumnConstraintExpr, {
        start,
        hidden,
      });
    }

    const identity = this.matchTextSeq('IDENTITY') || undefined;

    if (this.match(TokenType.L_PAREN)) {
      if (this.match(TokenType.START_WITH)) {
        thisExpr.setArgKey('start', this.parseBitwise());
      }
      if (this.matchTextSeq(['INCREMENT', 'BY'])) {
        thisExpr.setArgKey('increment', this.parseBitwise());
      }
      if (this.matchTextSeq('MINVALUE')) {
        thisExpr.setArgKey('minvalue', this.parseBitwise());
      }
      if (this.matchTextSeq('MAXVALUE')) {
        thisExpr.setArgKey('maxvalue', this.parseBitwise());
      }

      if (this.matchTextSeq('CYCLE')) {
        thisExpr.setArgKey('cycle', true);
      } else if (this.matchTextSeq(['NO', 'CYCLE'])) {
        thisExpr.setArgKey('cycle', false);
      }

      if (!identity) {
        thisExpr.setArgKey('expression', this.parseRange());
      } else if (!thisExpr.args.start && this.match(TokenType.NUMBER, { advance: false })) {
        const args = this.parseCsv(this.parseBitwise.bind(this));
        thisExpr.setArgKey('start', seqGet(args, 0));
        thisExpr.setArgKey('increment', seqGet(args, 1));
      }

      this.matchRParen();
    }

    return thisExpr;
  }

  parseInline (): InlineLengthColumnConstraintExpr {
    this.matchTextSeq('LENGTH');
    return this.expression(InlineLengthColumnConstraintExpr, { this: this.parseBitwise() });
  }

  parseNotConstraint (): Expression | undefined {
    if (this.matchTextSeq('NULL')) {
      return this.expression(NotNullColumnConstraintExpr);
    }
    if (this.matchTextSeq('CASESPECIFIC')) {
      return this.expression(CaseSpecificColumnConstraintExpr, { not: true });
    }
    if (this.matchTextSeq(['FOR', 'REPLICATION'])) {
      return this.expression(NotForReplicationColumnConstraintExpr);
    }

    this.retreat(this.index - 1);
    return undefined;
  }

  parseColumnConstraint (): Expression | undefined {
    const thisExpr = (this.match(TokenType.CONSTRAINT) || undefined) && this.parseIdVar();

    const procedureOptionFollows =
      (this.match(TokenType.WITH, { advance: false }) || undefined)
      && this.next
      && this.next.text.toUpperCase() in this._constructor.PROCEDURE_OPTIONS;

    if (!procedureOptionFollows && this.matchTexts(Object.keys(this._constructor.CONSTRAINT_PARSERS))) {
      const constraint = this._constructor.CONSTRAINT_PARSERS[(this.prev?.text ?? '').toUpperCase()]?.call(this);
      if (!constraint) {
        this.retreat(this.index - 1);
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
    if (!this.match(TokenType.CONSTRAINT)) {
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

  parseUnnamedConstraint (options: { constraints?: Set<string> | string[] } = {}): Expression | undefined {
    const index = this.index;
    const { constraints } = options;

    if (
      this.match(TokenType.IDENTIFIER, { advance: false })
      || !this.matchTexts(constraints || Object.keys(this._constructor.CONSTRAINT_PARSERS))
    ) {
      return undefined;
    }

    const constraintName = (this.prev?.text ?? '').toUpperCase();
    if (!(constraintName in this._constructor.CONSTRAINT_PARSERS)) {
      this.raiseError(`No parser found for schema constraint ${constraintName}.`);
    }

    const constraint = this._constructor.CONSTRAINT_PARSERS[constraintName]?.call(this);
    if (!constraint || Array.isArray(constraint)) {
      this.retreat(index);
      return undefined;
    }

    return constraint;
  }

  parseFieldDef (): Expression | undefined {
    return this.parseColumnDef(this.parseField({ anyToken: true }));
  }

  parseColumnDef (
    thisExpr: Expression | undefined,
    options: { computedColumn?: boolean } = {},
  ): Expression | undefined {
    const {
      computedColumn = true,
    } = options;

    let thisResult: ExpressionValue | undefined = thisExpr;

    if (thisResult instanceof ColumnExpr) {
      thisResult = thisResult.args.this;
    }

    if (!computedColumn) {
      this.match(TokenType.ALIAS);
    }

    let kind = this.parseTypes({ schema: true });

    if (this.matchTextSeq(['FOR', 'ORDINALITY'])) {
      return this.expression(ColumnDefExpr, {
        this: thisResult,
        ordinality: true,
      });
    }

    const constraints: Expression[] = [];

    if ((!kind && this.match(TokenType.ALIAS)) || this.matchTexts(['ALIAS', 'MATERIALIZED'])) {
      const persisted = (this.prev?.text ?? '').toUpperCase() === 'MATERIALIZED';
      const constraintKind = new ComputedColumnConstraintExpr({
        this: this.parseDisjunction(),
        persisted: persisted || (this.matchTextSeq('PERSISTED') || undefined),
        dataType: (this.matchTextSeq('AUTO')
          ? new DataTypeExpr({ this: 'AUTO' })
          : this.parseTypes()) as DataTypeExpr,
        notNull: this.matchPair(TokenType.NOT, TokenType.NULL) || undefined,
      });
      constraints.push(this.expression(ColumnConstraintExpr, { kind: constraintKind }));
    } else if (!kind && this.matchSet([TokenType.IN, TokenType.OUT], { advance: false })) {
      const inOutConstraint = this.expression(
        InOutColumnConstraintExpr,
        {
          input: this.match(TokenType.IN) || undefined,
          output: this.match(TokenType.OUT) || undefined,
        },
      );
      constraints.push(inOutConstraint);
      kind = this.parseTypes();
    } else if (
      kind
      && this.match(TokenType.ALIAS, { advance: false })
      && (!this._constructor.WRAPPED_TRANSFORM_COLUMN_CONSTRAINT
        || (this.next && this.next.tokenType === TokenType.L_PAREN))
    ) {
      this.advance();
      constraints.push(
        this.expression(
          ColumnConstraintExpr,
          {
            kind: new ComputedColumnConstraintExpr({
              this: this.parseDisjunction(),
              persisted:
                (this.matchTexts(['STORED', 'VIRTUAL']) || undefined)
                && (this.prev?.text ?? '').toUpperCase() === 'STORED',
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
      return thisResult as Expression | undefined;
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

    if (this.match(TokenType.L_PAREN, { advance: false })) {
      const args = this.parseWrappedCsv(this.parseBitwise.bind(this));
      start = seqGet(args, 0);
      increment = seqGet(args, 1);
    } else if (this.matchTextSeq('START')) {
      start = this.parseBitwise();
      this.matchTextSeq('INCREMENT');
      increment = this.parseBitwise();
      if (this.matchTextSeq('ORDER')) {
        order = true;
      } else if (this.matchTextSeq('NOORDER')) {
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

    return new AutoIncrementColumnConstraintExpr({});
  }

  parseCheckConstraint (): CheckColumnConstraintExpr | undefined {
    if (!this.match(TokenType.L_PAREN, { advance: false })) {
      return undefined;
    }

    return this.expression(
      CheckColumnConstraintExpr,
      {
        this: this.parseWrapped(this.parseAssignment.bind(this)),
        enforced: this.matchTextSeq('ENFORCED') || undefined,
      },
    );
  }

  parseAutoProperty (): AutoRefreshPropertyExpr | undefined {
    if (!this.matchTextSeq('REFRESH')) {
      this.retreat(this.index - 1);
      return undefined;
    }
    return this.expression(AutoRefreshPropertyExpr, { this: this.parseVar({ upper: true }) });
  }

  parseCompress (): CompressColumnConstraintExpr {
    if (this.match(TokenType.L_PAREN, { advance: false })) {
      return this.expression(
        CompressColumnConstraintExpr,
        { this: this.parseWrappedCsv(this.parseBitwise.bind(this)) },
      );
    }

    return this.expression(CompressColumnConstraintExpr, { this: this.parseBitwise() });
  }

  parseFunction (options: {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
    functions?: Record<string, Function>;
    anonymous?: boolean;
    optionalParens?: boolean;
    anyToken?: boolean;
  } = {}): Expression | undefined {
    const {
      functions,
      anonymous = false,
      optionalParens = true,
      anyToken = false,
    } = options;

    let fnSyntax = false;
    if (
      this.match(TokenType.L_BRACE, { advance: false })
      && this.next
      && this.next.text.toUpperCase() === 'FN'
    ) {
      this.advance(2);
      fnSyntax = true;
    }

    const func = this.parseFunctionCall({
      functions,
      anonymous,
      optionalParens,
      anyToken,
    });

    if (fnSyntax) {
      this.match(TokenType.R_BRACE);
    }

    return func;
  }

  parseFunctionArgs (options: { alias?: boolean } = {}): Expression[] {
    const {
      alias = false,
    } = options;
    return this.parseCsv(() => this.parseLambda({ alias }));
  }

  parseFunctionCall (options: {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
    functions?: Record<string, Function>;
    anonymous?: boolean;
    optionalParens?: boolean;
    anyToken?: boolean;
  } = {}): Expression | undefined {
    const {
      functions,
      anonymous = false,
      optionalParens = true,
      anyToken = false,
    } = options;

    if (!this.curr) {
      return undefined;
    }

    const comments = this.curr.comments;
    const prev = this.prev;
    const token = this.curr;
    const tokenType = this.curr.tokenType;
    const thisText = this.curr.text;
    const upper = thisText.toUpperCase();

    const parser = this._constructor.NO_PAREN_FUNCTION_PARSERS[upper];
    if (parser && optionalParens && !this._constructor.INVALID_FUNC_NAME_TOKENS.has(tokenType)) {
      this.advance();
      return this.parseWindow(parser.call(this));
    }

    if (!this.next || this.next.tokenType !== TokenType.L_PAREN) {
      const noParen = this._constructor.NO_PAREN_FUNCTIONS[tokenType as keyof typeof this._constructor.NO_PAREN_FUNCTIONS];
      if (optionalParens && noParen) {
        this.advance();
        return this.expression(noParen);
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

    this.advance(2);

    const funcParser = this._constructor.FUNCTION_PARSERS[upper];
    if (funcParser && !anonymous) {
      const thisExpr: Expression | undefined = funcParser.call(this);

      if (thisExpr instanceof Expression) {
        thisExpr.addComments(comments);
      }

      this.matchRParen(thisExpr);
      return this.parseWindow(thisExpr);
    } else {
      const subqueryPredicate = this._constructor.SUBQUERY_PREDICATES[tokenType];

      if (subqueryPredicate) {
        let expr: Expression | undefined;
        if (
          this.curr.tokenType === TokenType.SELECT
          || this.curr.tokenType === TokenType.WITH
        ) {
          expr = this.parseSelect();
          this.matchRParen();
        } else if (
          prev
          && (prev.tokenType === TokenType.LIKE || prev.tokenType === TokenType.ILIKE)
        ) {
          this.advance(-1);
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

      const postFuncComments = this.curr?.comments;
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
        let func: Expression;
        if (1 < functionBuilder.length) {
          func = functionBuilder(argsWithPropEq, { dialect: this.dialect });
        } else {
          func = functionBuilder(argsWithPropEq);
        }

        func = this.validateExpression(func, argsWithPropEq);
        if (this._dialectConstructor.PRESERVE_ORIGINAL_NAMES) {
          func.meta.name = thisText;
        }

        thisExpr = func;
      } else {
        let thisValue: Expression | string = upper;
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

      this.matchRParen(thisExpr);
      return this.parseWindow(thisExpr);
    }
  }

  toPropEq (expression: Expression, _index: number): Expression {
    return expression;
  }

  kvToPropEq (
    expressions: Expression[],
    options: { parseMap?: boolean } = {},
  ): Expression[] {
    const {
      parseMap = false,
    } = options;
    const transformed: Expression[] = [];

    for (let index = 0; index < expressions.length; index++) {
      let e = expressions[index];

      if ([...this._constructor.KEY_VALUE_DEFINITIONS.keys()].some((def) => e instanceof def)) {
        if (e instanceof AliasExpr) {
          e = this.expression(PropertyEqExpr, {
            this: e.args.alias,
            expression: e.args.this,
          });
        }

        if (!(e instanceof PropertyEqExpr)) {
          const eThis = e.args.this;
          e = this.expression(PropertyEqExpr, {
            this: parseMap ? e.args.this : toIdentifier(eThis instanceof Expression ? eThis.name : eThis?.toString()),
            expression: e.args.expression,
          });
        }

        if (e.args.this instanceof ColumnExpr) {
          e.args.this.replace(e.args.this.args.this);
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

  parseUserDefinedFunction (_options: { kind?: TokenType } = {}): Expression | undefined {
    const thisExpr = this.parseTableParts({ schema: true });

    if (!this.match(TokenType.L_PAREN)) {
      return thisExpr;
    }

    const expressions = this.parseCsv(this.parseFunctionParameter.bind(this));
    this.matchRParen();
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

    if (thisExpr && this.match(TokenType.DOT)) {
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

  parseLambda (options: { alias?: boolean } = {}): Expression | undefined {
    const { alias = false } = options;
    const index = this.index;

    let expressions: (Expression | undefined)[];

    if (this.match(TokenType.L_PAREN)) {
      expressions = this.parseCsv(this.parseLambdaArg.bind(this));

      if (!this.match(TokenType.R_PAREN)) {
        this.retreat(index);
      }
    } else {
      expressions = [this.parseLambdaArg()];
    }

    if (this.matchSet(Object.keys(this._constructor.LAMBDAS) as TokenType[])) {
      const lambdaTokenType = this.prev?.tokenType;
      return lambdaTokenType && this._constructor.LAMBDAS[lambdaTokenType]?.call(this, expressions.filter((e): e is Expression => Boolean(e)));
    }

    this.retreat(index);

    let thisExpr: Expression | undefined;

    if (this.match(TokenType.DISTINCT)) {
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

    const index = this.index;
    if (!this.match(TokenType.L_PAREN)) {
      return thisExpr;
    }

    if (this.matchSet(this._constructor.SELECT_START_TOKENS)) {
      this.retreat(index);
      return thisExpr;
    }

    const args = this.parseCsv(() => this.parseConstraint() || this.parseFieldDef());
    this.matchRParen();
    return this.expression(SchemaExpr, {
      this: thisExpr,
      expressions: args,
    });
  }

  parseAlterTableSet (): AlterSetExpr {
    const alterSet = this.expression(AlterSetExpr);

    if (
      this.match(TokenType.L_PAREN, { advance: false })
      || this.matchTextSeq(['TABLE', 'PROPERTIES'])
    ) {
      alterSet.setArgKey('expressions', this.parseWrappedCsv(this.parseAssignment.bind(this)));
    } else if (this.matchTextSeq('FILESTREAM_ON', { advance: false })) {
      alterSet.setArgKey('expressions', [this.parseAssignment() as Expression]);
    } else if (this.matchTexts(['LOGGED', 'UNLOGGED'])) {
      alterSet.setArgKey('option', var_((this.prev?.text ?? '').toUpperCase()));
    } else if (this.matchTextSeq('WITHOUT') && this.matchTexts(['CLUSTER', 'OIDS'])) {
      alterSet.setArgKey('option', var_(`WITHOUT ${(this.prev?.text ?? '').toUpperCase()}`));
    } else if (this.matchTextSeq('LOCATION')) {
      alterSet.setArgKey('location', this.parseField());
    } else if (this.matchTextSeq(['ACCESS', 'METHOD'])) {
      alterSet.setArgKey('accessMethod', this.parseField());
    } else if (this.matchTextSeq('TABLESPACE')) {
      alterSet.setArgKey('tablespace', this.parseField());
    } else if (this.matchTextSeq(['FILE', 'FORMAT']) || this.matchTextSeq('FILEFORMAT')) {
      alterSet.setArgKey('fileFormat', [this.parseField()!]);
    } else if (this.matchTextSeq('STAGE_FILE_FORMAT')) {
      alterSet.setArgKey('fileFormat', this.parseWrappedOptions());
    } else if (this.matchTextSeq('STAGE_COPY_OPTIONS')) {
      alterSet.setArgKey('copyOptions', this.parseWrappedOptions());
    } else if (this.matchTextSeq('TAG') || this.matchTextSeq('TAGS')) {
      alterSet.setArgKey('tag', this.parseCsv(this.parseAssignment.bind(this)));
    } else {
      if (this.matchTextSeq('SERDE')) {
        alterSet.setArgKey('serde', this.parseField());
      }

      const properties = this.parseWrapped(this.parseProperties.bind(this), { optional: true });
      alterSet.setArgKey('expressions', properties ? [properties] : []);
    }

    return alterSet;
  }

  parseAlterSession (): AlterSessionExpr {
    if (this.match(TokenType.SET)) {
      const expressions = this.parseCsv(() => this.parseSetItemAssignment());
      return this.expression(AlterSessionExpr, {
        expressions,
        unset: false,
      });
    }

    this.matchTextSeq('UNSET');
    const expressions = this.parseCsv(() =>
      this.expression(SetItemExpr, { this: this.parseIdVar({ anyToken: true }) }));
    return this.expression(AlterSessionExpr, {
      expressions,
      unset: true,
    });
  }

  parseAlter (): AlterExpr | CommandExpr {
    const start = this.prev;

    const alterToken = (this.matchSet(this._constructor.ALTERABLES) || undefined) && this.prev;
    if (!alterToken) {
      return this.parseAsCommand(start);
    }

    const exists = this.parseExists();
    const only = this.matchTextSeq('ONLY') || undefined;

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
      check = this.matchTextSeq(['WITH', 'CHECK']) || undefined;
      cluster = this.match(TokenType.ON) ? this.parseOnProperty() : undefined;

      if (this.next) {
        this.advance();
      }
    }

    const parser = this.prev
      ? this._constructor.ALTER_PARSERS[this.prev.text.toUpperCase()]
      : undefined;
    if (parser) {
      const actions = ensureList(parser.call(this));
      const notValid = this.matchTextSeq(['NOT', 'VALID']) || undefined;
      const options = this.parseCsv(this.parseProperty.bind(this));
      const cascade =
        this._dialectConstructor.ALTER_TABLE_SUPPORTS_CASCADE
        && this.matchTextSeq('CASCADE');

      if (!this.curr && actions) {
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
    const start = this.prev;

    if (!this.curr) {
      return this.expression(AnalyzeExpr);
    }

    const options: string[] = [];
    while (this.matchTexts(this._constructor.ANALYZE_STYLES)) {
      if ((this.prev?.text ?? '').toUpperCase() === 'BUFFER_USAGE_LIMIT') {
        options.push(`BUFFER_USAGE_LIMIT ${this.parseNumber()}`);
      } else {
        options.push((this.prev?.text ?? '').toUpperCase());
      }
    }

    let thisExpr: Expression | undefined;
    let innerExpression: Expression | undefined;

    let kind: string | undefined = this.curr?.text.toUpperCase();

    if (this.match(TokenType.TABLE) || this.match(TokenType.INDEX)) {
      thisExpr = this.parseTableParts();
    } else if (this.matchTextSeq('TABLES')) {
      if (this.matchSet([TokenType.FROM, TokenType.IN])) {
        kind = `${kind} ${(this.prev?.text ?? '').toUpperCase()}`;
        thisExpr = this.parseTable({
          schema: true,
          isDbReference: true,
        });
      }
    } else if (this.matchTextSeq('DATABASE')) {
      thisExpr = this.parseTable({
        schema: true,
        isDbReference: true,
      });
    } else if (this.matchTextSeq('CLUSTER')) {
      thisExpr = this.parseTable();
    } else if (this.matchTexts(Object.keys(this._constructor.ANALYZE_EXPRESSION_PARSERS))) {
      kind = undefined;
      innerExpression =
        this._constructor.ANALYZE_EXPRESSION_PARSERS[(this.prev?.text ?? '').toUpperCase()].call(this);
    } else {
      kind = undefined;
      thisExpr = this.parseTableParts();
    }

    const partition = this.tryParse(this.parsePartition.bind(this));
    if (!partition && this.matchTexts(this._constructor.PARTITION_KEYWORDS)) {
      return this.parseAsCommand(start);
    }

    let mode: string | undefined;
    if (
      this.matchTextSeq([
        'WITH',
        'SYNC',
        'MODE',
      ])
      || this.matchTextSeq([
        'WITH',
        'ASYNC',
        'MODE',
      ])
    ) {
      mode = `WITH ${this.tokens[this.index - 2].text.toUpperCase()} MODE`;
    } else {
      mode = undefined;
    }

    if (this.matchTexts(Object.keys(this._constructor.ANALYZE_EXPRESSION_PARSERS))) {
      innerExpression =
        this._constructor.ANALYZE_EXPRESSION_PARSERS[(this.prev?.text ?? '').toUpperCase()].call(this);
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
    const kind = (this.prev?.text ?? '').toUpperCase();
    const option = this.matchTextSeq('DELTA') ? (this.prev?.text ?? '').toUpperCase() : undefined;
    let expressions: Expression[] = [];

    if (!this.matchTextSeq('STATISTICS')) {
      this.raiseError('Expecting token STATISTICS');
    }

    if (this.matchTextSeq('NOSCAN')) {
      thisValue = 'NOSCAN';
    } else if (this.match(TokenType.FOR)) {
      if (this.matchTextSeq(['ALL', 'COLUMNS'])) {
        thisValue = 'FOR ALL COLUMNS';
      }
      if (this.matchTexts('COLUMNS')) {
        thisValue = 'FOR COLUMNS';
        expressions = this.parseCsv(this.parseColumnReference.bind(this));
      }
    } else if (this.matchTextSeq('SAMPLE')) {
      const sample = this.parseNumber();
      expressions = [
        this.expression(AnalyzeSampleExpr, {
          sample,
          kind: this.match(TokenType.PERCENT) ? (this.prev?.text ?? '').toUpperCase() : undefined,
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

    if (this.matchTextSeq(['REF', 'UPDATE'])) {
      kind = 'REF';
      thisValue = 'UPDATE';
      if (this.matchTextSeq([
        'SET',
        'DANGLING',
        'TO',
        'NULL',
      ])) {
        thisValue = 'UPDATE SET DANGLING TO NULL';
      }
    } else if (this.matchTextSeq('STRUCTURE')) {
      kind = 'STRUCTURE';
      if (this.matchTextSeq(['CASCADE', 'FAST'])) {
        thisValue = 'CASCADE FAST';
      } else if (
        this.matchTextSeq(['CASCADE', 'COMPLETE'])
        && this.matchTexts(['ONLINE', 'OFFLINE'])
      ) {
        thisValue = `CASCADE COMPLETE ${(this.prev?.text ?? '').toUpperCase()}`;
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
    const thisValue = (this.prev?.text ?? '').toUpperCase();
    if (this.matchTextSeq('COLUMNS')) {
      return this.expression(AnalyzeColumnsExpr, {
        this: `${thisValue} ${(this.prev?.text ?? '').toUpperCase()}`,
      });
    }
    return undefined;
  }

  parseAnalyzeDelete (): AnalyzeDeleteExpr | undefined {
    const kind = this.matchTextSeq('SYSTEM') ? (this.prev?.text ?? '').toUpperCase() : undefined;
    if (this.matchTextSeq('STATISTICS')) {
      return this.expression(AnalyzeDeleteExpr, { kind });
    }
    return undefined;
  }

  parseAnalyzeList (): AnalyzeListChainedRowsExpr | undefined {
    if (this.matchTextSeq(['CHAINED', 'ROWS'])) {
      return this.expression(AnalyzeListChainedRowsExpr, { expression: this.parseInto() });
    }
    return undefined;
  }

  parseAnalyzeHistogram (): AnalyzeHistogramExpr {
    const thisValue = (this.prev?.text ?? '').toUpperCase();
    let expression: Expression | undefined;
    let expressions: Expression[] = [];
    let updateOptions: string | undefined;

    if (this.matchTextSeq(['HISTOGRAM', 'ON'])) {
      expressions = this.parseCsv(this.parseColumnReference.bind(this));
      const withExpressions: string[] = [];
      while (this.match(TokenType.WITH)) {
        if (this.matchTexts(['SYNC', 'ASYNC'])) {
          if (this.matchTextSeq('MODE', { advance: false })) {
            withExpressions.push(`${(this.prev?.text ?? '').toUpperCase()} MODE`);
            this.advance();
          }
        } else {
          const buckets = this.parseNumber();
          if (this.matchTextSeq('BUCKETS')) {
            withExpressions.push(`${buckets} BUCKETS`);
          }
        }
      }
      if (0 < withExpressions.length) {
        expression = this.expression(AnalyzeWithExpr, { expressions: withExpressions });
      }

      if (this.matchTexts(['MANUAL', 'AUTO']) && this.match(TokenType.UPDATE, { advance: false })) {
        updateOptions = (this.prev?.text ?? '').toUpperCase();
        this.advance();
      } else if (this.matchTextSeq(['USING', 'DATA'])) {
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
    this.match(TokenType.INTO);
    const target = this.parseTable();

    if (target && this.match(TokenType.ALIAS, { advance: false })) {
      target.setArgKey('alias', this.parseTableAlias());
    }

    this.match(TokenType.USING);
    const using = this.parseTable();

    return this.expression(MergeExpr, {
      this: target,
      using,
      on: (this.match(TokenType.ON) || undefined) && this.parseDisjunction(),
      usingCond: (this.match(TokenType.USING) || undefined) && this.parseUsingIdentifiers(),
      whens: this.parseWhenMatched(),
      returning: this.parseReturning(),
    });
  }

  parseWhenMatched (): WhensExpr {
    const whens: WhenExpr[] = [];

    while (this.match(TokenType.WHEN)) {
      const matched = !this.match(TokenType.NOT);
      this.matchTextSeq('MATCHED');
      const source = this.matchTextSeq(['BY', 'TARGET'])
        ? false
        : this.matchTextSeq(['BY', 'SOURCE']);
      const condition = this.match(TokenType.AND) ? this.parseDisjunction() : undefined;

      this.match(TokenType.THEN);

      let then: Expression | undefined;

      if (this.match(TokenType.INSERT)) {
        const thisValue = this.parseStar();
        if (thisValue) {
          then = this.expression(InsertExpr, { this: thisValue });
        } else {
          then = this.expression(InsertExpr, {
            this: this.matchTextSeq('ROW')
              ? var_('ROW')
              : this.parseValue({ values: false }),
            expression: this.matchTextSeq('VALUES') && this.parseValue(),
          });
        }
      } else if (this.match(TokenType.UPDATE)) {
        const expressions = this.parseStar();
        if (expressions) {
          then = this.expression(UpdateExpr, { expressions });
        } else {
          then = this.expression(UpdateExpr, {
            expressions: this.match(TokenType.SET) && this.parseCsv(this.parseEquality.bind(this)),
          });
        }
      } else if (this.match(TokenType.DELETE)) {
        then = this.expression(VarExpr, { this: this.prev?.text ?? '' });
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
      return parser.call(this);
    }
    return this.parseAsCommand(this.prev);
  }

  parseSetItemAssignment (options: { kind?: string } = {}): Expression | undefined {
    const { kind } = options;
    const index = this.index;

    if (
      (kind === 'GLOBAL' || kind === 'SESSION')
      && this.matchTextSeq('TRANSACTION')
    ) {
      return this.parseSetTransaction({ global: kind === 'GLOBAL' });
    }

    const left = this.parsePrimary() || this.parseColumn();
    const assignmentDelimiter = this.matchTexts(
      this._constructor.SET_ASSIGNMENT_DELIMITERS,
    ) || undefined;

    if (
      !left
      || (this._constructor.SET_REQUIRES_ASSIGNMENT_DELIMITER && !assignmentDelimiter)
    ) {
      this.retreat(index);
      return undefined;
    }

    let right = this.parseStatement() || this.parseIdVar();
    if (right instanceof ColumnExpr || right instanceof IdentifierExpr) {
      right = var_(right.name);
    }

    const thisValue = this.expression(EqExpr, {
      this: left,
      expression: right,
    });
    return this.expression(SetItemExpr, {
      this: thisValue,
      kind,
    });
  }

  parseSetTransaction (options: { global?: boolean } = {}): Expression {
    const { global = false } = options;
    this.matchTextSeq('TRANSACTION');
    const characteristics = this.parseCsv(() =>
      this.parseVarFromOptions(this._constructor.TRANSACTION_CHARACTERISTICS));
    return this.expression(SetItemExpr, {
      expressions: characteristics,
      kind: 'TRANSACTION',
      global: global,
    });
  }

  parseSetItem (): Expression | undefined {
    const parser = this.findParser(
      this._constructor.SET_PARSERS,
      this._constructor.SET_TRIE,
    );
    return parser ? parser.call(this) : this.parseSetItemAssignment({ kind: undefined });
  }

  parseSet (options: {
    unset?: boolean;
    tag?: boolean;
  } = {}): SetExpr | CommandExpr {
    const {
      unset = false,
      tag = false,
    } = options;
    const index = this.index;
    const set = this.expression(SetExpr, {
      expressions: this.parseCsv(this.parseSetItem.bind(this)),
      unset,
      tag,
    });

    if (this.curr) {
      this.retreat(index);
      return this.parseAsCommand(this.prev);
    }

    return set;
  }

  parseVarFromOptions (
    options: Record<string, (string | string[])[] | undefined>,
    parseOptions: { raiseUnmatched?: boolean } = {},
  ): VarExpr | undefined {
    const { raiseUnmatched = true } = parseOptions;
    const start = this.curr;
    if (!start) {
      return undefined;
    }

    let option = start.text.toUpperCase();
    const continuations = options[option];

    const index = this.index;
    this.advance();
    let matched = false;
    for (const keywords of continuations || []) {
      const keywordArray = typeof keywords === 'string' ? [keywords] : keywords;

      if (this.matchTextSeq(keywordArray)) {
        option = `${option} ${keywordArray.join(' ')}`;
        matched = true;
        break;
      }
    }

    if (!matched && (continuations === undefined || 0 < continuations.length)) {
      if (raiseUnmatched) {
        this.raiseError(`Unknown option ${option}`);
      }

      this.retreat(index);
      return undefined;
    }

    return var_(option);
  }

  parseCache (): CacheExpr {
    const lazy = this.matchTextSeq('LAZY') || undefined;
    this.match(TokenType.TABLE);
    const table = this.parseTable({ schema: true });

    let options: Expression[] = [];
    if (this.matchTextSeq('OPTIONS')) {
      this.matchLParen();
      const k = this.parseString();
      if (!k) {
        this.raiseError('Expected option key');
      }
      this.match(TokenType.EQ);
      const v = this.parseString();
      if (!v) {
        this.raiseError('Expected option value');
      }
      options = [k, v].filter(Boolean) as Expression[];
      this.matchRParen();
    }

    this.match(TokenType.ALIAS);
    return this.expression(CacheExpr, {
      this: table,
      lazy,
      options,
      expression: this.parseSelect({ nested: true }),
    });
  }

  parsePartition (): PartitionExpr | undefined {
    if (!this.matchTexts(this._constructor.PARTITION_KEYWORDS)) {
      return undefined;
    }

    return this.expression(PartitionExpr, {
      subpartition: (this.prev?.text ?? '').toUpperCase() === 'SUBPARTITION',
      expressions: this.parseWrappedCsv(this.parseDisjunction.bind(this)),
    });
  }

  parseValue (_options?: { values?: boolean }): TupleExpr | undefined {
    const parseValueExpression = (): Expression | undefined => {
      if (
        this._dialectConstructor.SUPPORTS_VALUES_DEFAULT
        && this.match(TokenType.DEFAULT)
      ) {
        return var_((this.prev?.text ?? '').toUpperCase());
      }
      return this.parseExpression();
    };

    if (this.match(TokenType.L_PAREN)) {
      const expressions = this.parseCsv(parseValueExpression);
      this.matchRParen();
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

  parseWrappedSelect (options: { table?: boolean } = {}): Expression | undefined {
    const { table = false } = options;
    let thisExpr: Expression | undefined;

    if (this.matchSet([TokenType.PIVOT, TokenType.UNPIVOT])) {
      thisExpr = this.parseSimplifiedPivot({
        isUnpivot: this.prev?.tokenType === TokenType.UNPIVOT,
      });
    } else if (this.match(TokenType.FROM)) {
      const from = this.parseFrom({
        skipFromToken: true,
        consumePipe: true,
      });
      const selectExpr = this.parseSelect({ from });
      if (selectExpr) {
        if (!selectExpr.args.from) {
          selectExpr.setArgKey('from', from);
        }
        thisExpr = selectExpr;
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
    const isRollback = this.prev?.tokenType === TokenType.ROLLBACK;

    this.matchTexts(['TRANSACTION', 'WORK']);

    if (this.matchTextSeq('TO')) {
      this.matchTextSeq('SAVEPOINT');
      savepoint = this.parseIdVar();
    }

    if (this.match(TokenType.AND)) {
      chain = !this.matchTextSeq('NO');
      this.matchTextSeq('CHAIN');
    }

    if (isRollback) {
      return this.expression(RollbackExpr, { savepoint });
    }

    return this.expression(CommitExpr, { chain });
  }

  parseRefresh (): RefreshExpr | CommandExpr {
    let kind: string;

    if (this.match(TokenType.TABLE)) {
      kind = 'TABLE';
    } else if (this.matchTextSeq(['MATERIALIZED', 'VIEW'])) {
      kind = 'MATERIALIZED VIEW';
    } else {
      kind = '';
    }

    const thisValue = this.parseString() || this.parseTable();
    if (!kind && !(thisValue instanceof LiteralExpr)) {
      return this.parseAsCommand(this.prev);
    }

    return this.expression(RefreshExpr, {
      this: thisValue,
      kind,
    });
  }

  parseColumnDefWithExists (): ColumnDefExpr | undefined {
    const start = this.index;
    this.match(TokenType.COLUMN);

    const existsColumn = this.parseExists({ not: true });
    const expression = this.parseFieldDef();

    if (!(expression instanceof ColumnDefExpr)) {
      this.retreat(start);
      return undefined;
    }

    expression.setArgKey('exists', existsColumn);

    return expression;
  }

  parseAddColumn (): ColumnDefExpr | undefined {
    if ((this.prev?.text ?? '').toUpperCase() !== 'ADD') {
      return undefined;
    }

    const expression = this.parseColumnDefWithExists();
    if (!expression) {
      return undefined;
    }

    if (this.matchTexts(['FIRST', 'AFTER'])) {
      const position = this.prev?.text ?? '';
      const columnPosition = this.expression(ColumnPositionExpr, {
        this: this.parseColumn(),
        position,
      });
      expression.setArgKey('position', columnPosition);
    }

    return expression;
  }

  parseDropColumn (): DropExpr | CommandExpr | undefined {
    const drop = (this.match(TokenType.DROP) || undefined) && this.parseDrop();
    if (drop && !(drop instanceof CommandExpr)) {
      drop.setArgKey('kind', drop.args.kind || 'COLUMN');
    }
    return drop;
  }

  parseDropPartition (options: { exists?: boolean } = {}): DropPartitionExpr {
    const { exists } = options;
    return this.expression(DropPartitionExpr, {
      expressions: this.parseCsv(this.parsePartition.bind(this)),
      exists,
    });
  }

  parseAlterTableAdd (): Expression[] {
    const parseAddAlteration = (): Expression | undefined => {
      this.matchTextSeq('ADD');
      if (this.matchSet(this._constructor.ADD_CONSTRAINT_TOKENS, { advance: false })) {
        return this.expression(AddConstraintExpr, {
          expressions: this.parseCsv(this.parseConstraint.bind(this)),
        });
      }

      const columnDef = this.parseAddColumn();
      if (columnDef instanceof ColumnDefExpr) {
        return columnDef;
      }

      const exists = this.parseExists({ not: true });
      if (this.matchPair(TokenType.PARTITION, TokenType.L_PAREN, { advance: false })) {
        return this.expression(AddPartitionExpr, {
          exists,
          this: this.parseField({ anyToken: true }),
          location:
            this.matchTextSeq('LOCATION', { advance: false }) && this.parseProperty(),
        });
      }

      return undefined;
    };

    if (
      !this.matchSet(this._constructor.ADD_CONSTRAINT_TOKENS, { advance: false })
      && (!this._dialectConstructor.ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN
        || this.matchTextSeq('COLUMNS'))
    ) {
      const schema = this.parseSchema();

      return schema
        ? Array.from(ensureList<Expression>(schema))
        : this.parseCsv(this.parseColumnDefWithExists.bind(this));
    }

    return this.parseCsv(parseAddAlteration);
  }

  parseAlterTableAlter (): Expression | undefined {
    if (this.matchTexts(Object.keys(this._constructor.ALTER_ALTER_PARSERS))) {
      return this._constructor.ALTER_ALTER_PARSERS[(this.prev?.text ?? '').toUpperCase()]?.call(this);
    }

    this.match(TokenType.COLUMN);
    const column = this.parseField({ anyToken: true });

    if (this.matchPair(TokenType.DROP, TokenType.DEFAULT)) {
      return this.expression(AlterColumnExpr, {
        this: column,
        drop: true,
      });
    }
    if (this.matchPair(TokenType.SET, TokenType.DEFAULT)) {
      return this.expression(AlterColumnExpr, {
        this: column,
        default: this.parseDisjunction(),
      });
    }
    if (this.match(TokenType.COMMENT)) {
      return this.expression(AlterColumnExpr, {
        this: column,
        comment: this.parseString(),
      });
    }
    if (this.matchTextSeq([
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
    if (this.matchTextSeq([
      'SET',
      'NOT',
      'NULL',
    ])) {
      return this.expression(AlterColumnExpr, {
        this: column,
        allowNull: false,
      });
    }

    if (this.matchTextSeq(['SET', 'VISIBLE'])) {
      return this.expression(AlterColumnExpr, {
        this: column,
        visible: 'VISIBLE',
      });
    }
    if (this.matchTextSeq(['SET', 'INVISIBLE'])) {
      return this.expression(AlterColumnExpr, {
        this: column,
        visible: 'INVISIBLE',
      });
    }

    this.matchTextSeq(['SET', 'DATA']);
    this.matchTextSeq('TYPE');
    return this.expression(AlterColumnExpr, {
      this: column,
      dtype: this.parseTypes(),
      collate: this.match(TokenType.COLLATE) && this.parseTerm(),
      using: this.match(TokenType.USING) && this.parseDisjunction(),
    });
  }

  parseAlterDiststyle (): AlterDistStyleExpr {
    if (this.matchTexts([
      'ALL',
      'EVEN',
      'AUTO',
    ])) {
      return this.expression(AlterDistStyleExpr, { this: var_((this.prev?.text ?? '').toUpperCase()) });
    }

    this.matchTextSeq(['KEY', 'DISTKEY']);
    return this.expression(AlterDistStyleExpr, { this: this.parseColumn() });
  }

  parseAlterSortkey (options: { compound?: boolean } = {}): AlterSortKeyExpr {
    const { compound } = options;

    if (compound) {
      this.matchTextSeq('SORTKEY');
    }

    if (this.match(TokenType.L_PAREN, { advance: false })) {
      return this.expression(AlterSortKeyExpr, {
        expressions: this.parseWrappedIdVars(),
        compound,
      });
    }

    this.matchTexts(['AUTO', 'NONE']);
    return this.expression(AlterSortKeyExpr, {
      this: var_((this.prev?.text ?? '').toUpperCase()),
      compound,
    });
  }

  parseAlterTableDrop (): Expression[] {
    const index = this.index - 1;

    const partitionExists = this.parseExists();
    if (this.match(TokenType.PARTITION, { advance: false })) {
      return this.parseCsv(() => this.parseDropPartition({ exists: partitionExists }));
    }

    this.retreat(index);
    return this.parseCsv(this.parseDropColumn.bind(this));
  }

  parseAlterTableRename (): AlterRenameExpr | RenameColumnExpr | undefined {
    if (this.match(TokenType.COLUMN) || !this._constructor.ALTER_RENAME_REQUIRES_COLUMN) {
      const exists = this.parseExists();
      const oldColumn = this.parseColumn();
      const to = this.matchTextSeq('TO') || undefined;
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

    this.matchTextSeq('TO');
    return this.expression(AlterRenameExpr, { this: this.parseTable({ schema: true }) });
  }

  parseLoad (): LoadDataExpr | CommandExpr {
    if (this.matchTextSeq('DATA')) {
      const local = this.matchTextSeq('LOCAL') || undefined;
      this.matchTextSeq('INPATH');
      const inpath = this.parseString();
      const overwrite = this.match(TokenType.OVERWRITE) || undefined;
      this.matchPair(TokenType.INTO, TokenType.TABLE);

      return this.expression(LoadDataExpr, {
        this: this.parseTable({ schema: true }),
        local,
        overwrite,
        inpath,
        partition: this.parsePartition(),
        inputFormat: this.matchTextSeq('INPUTFORMAT') && this.parseString(),
        serde: this.matchTextSeq('SERDE') && this.parseString(),
      });
    }
    return this.parseAsCommand(this.prev);
  }

  parseDelete (): DeleteExpr {
    let tables: Expression[] | undefined;
    if (!this.match(TokenType.FROM, { advance: false })) {
      tables = this.parseCsv(this.parseTable.bind(this)) || undefined;
    }

    const returning = this.parseReturning();

    return this.expression(DeleteExpr, {
      tables,
      this: this.match(TokenType.FROM) && this.parseTable({ joins: true }),
      using:
        this.match(TokenType.USING)
        && this.parseCsv(() => this.parseTable({ joins: true })),
      cluster: this.match(TokenType.ON) && this.parseOnProperty(),
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

    while (this.curr) {
      if (this.match(TokenType.SET)) {
        kwargs.expressions = this.parseCsv(this.parseEquality.bind(this));
      } else if (this.match(TokenType.RETURNING, { advance: false })) {
        kwargs.returning = this.parseReturning();
      } else if (this.match(TokenType.FROM, { advance: false })) {
        const from = this.parseFrom({ joins: true });
        const table = from?.args.this;
        if (table instanceof SubqueryExpr && this.match(TokenType.JOIN, { advance: false })) {
          const joins = this.parseJoins();
          table.setArgKey('joins', 0 < joins.length ? joins : undefined);
        }

        kwargs.from = from;
      } else if (this.match(TokenType.WHERE, { advance: false })) {
        kwargs.where = this.parseWhere();
      } else if (this.match(TokenType.ORDER_BY, { advance: false })) {
        kwargs.order = this.parseOrder();
      } else if (this.match(TokenType.LIMIT, { advance: false })) {
        kwargs.limit = this.parseLimit();
      } else {
        break;
      }
    }

    return this.expression(UpdateExpr, kwargs);
  }

  parseUse (): UseExpr {
    return this.expression(UseExpr, {
      kind: this.parseVarFromOptions(this._constructor.USABLES, { raiseUnmatched: false }),
      this: this.parseTable({ schema: false }),
    });
  }

  parseUncache (): UncacheExpr {
    if (!this.match(TokenType.TABLE)) {
      this.raiseError('Expecting TABLE after UNCACHE');
    }

    return this.expression(UncacheExpr, {
      exists: this.parseExists(),
      this: this.parseTable({ schema: true }),
    });
  }

  parseAsCommand (start?: Token): CommandExpr {
    while (this.curr) {
      this.advance();
    }
    const text = this.findSql(start, this.prev);
    const size = start?.text.length || 0;
    this.warnUnsupported();
    return new CommandExpr({
      this: text.substring(0, size),
      expression: text.substring(size),
    });
  }

  parseDictProperty (options: { this: string }): DictPropertyExpr {
    const settings: DictSubPropertyExpr[] = [];

    this.matchLParen();
    const kind = this.parseIdVar();

    if (this.match(TokenType.L_PAREN)) {
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
      this.match(TokenType.R_PAREN);
    }

    this.matchRParen();

    return this.expression(DictPropertyExpr, {
      this: options.this,
      kind: kind?.args.this,
      settings,
    });
  }

  parseDictRange (options: { this: string }): DictRangeExpr {
    this.matchLParen();
    const hasMin = this.matchTextSeq('MIN') || undefined;
    let min: Expression | undefined;
    let max: Expression | undefined;

    if (hasMin) {
      min = this.parseVar() || this.parsePrimary();
      if (!min) {
        this.raiseError('Expected MIN value');
      }
      this.matchTextSeq('MAX');
      max = this.parseVar() || this.parsePrimary();
      if (!max) {
        this.raiseError('Expected MAX value');
      }
    } else {
      max = this.parseVar() || this.parsePrimary();
      if (!max) {
        this.raiseError('Expected MAX value');
      }
      min = LiteralExpr.number(0);
    }
    this.matchRParen();
    return this.expression(DictRangeExpr, {
      this: options.this,
      min: min,
      max: max,
    });
  }

  parseComprehension (thisValue?: Expression): ComprehensionExpr | undefined {
    const index = this.index;
    const expression = this.parseColumn();
    const position = (this.match(TokenType.COMMA) || undefined) && this.parseColumn();

    if (!this.match(TokenType.IN)) {
      this.retreat(index - 1);
      return undefined;
    }
    const iterator = this.parseColumn();
    const condition = this.matchTextSeq('IF') ? this.parseDisjunction() : undefined;
    return this.expression(ComprehensionExpr, {
      this: thisValue,
      expression,
      position,
      iterator,
      condition,
    });
  }

  parseHeredoc (): HeredocExpr | undefined {
    if (this.match(TokenType.HEREDOC_STRING)) {
      return this.expression(HeredocExpr, { this: this.prev?.text ?? '' });
    }

    if (!this.matchTextSeq('$')) {
      return undefined;
    }

    const tags = ['$'];
    let tagText: string | undefined;

    if (this.isConnected()) {
      this.advance();
      tags.push((this.prev?.text ?? '').toUpperCase());
    } else {
      this.raiseError('No closing $ found');
    }

    if (tags[tags.length - 1] !== '$') {
      if (this.isConnected() && this.matchTextSeq('$')) {
        tagText = tags[tags.length - 1];
        tags.push('$');
      } else {
        this.raiseError('No closing $ found');
      }
    }

    const heredocStart = this.curr;

    while (this.curr) {
      if (this.matchTextSeq(tags, { advance: false })) {
        const thisValue = this.findSql(heredocStart, this.prev);
        this.advance(tags.length);
        return this.expression(HeredocExpr, {
          this: thisValue,
          tag: tagText,
        });
      }

      this.advance();
    }

    this.raiseError(`No closing ${tags.join('')} found`);
    return undefined;
  }

  findParser (
    // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
    parsers: Record<string, Function>,
    trie: TrieNode,
  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
  ): Function | undefined {
    if (!this.curr) {
      return undefined;
    }

    const index = this.index;
    const thisPath: string[] = [];
    while (true) {
      const curr = this.curr.text.toUpperCase();
      const key = curr.split(' ');
      thisPath.push(curr);

      this.advance();
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

    this.retreat(index);
    return undefined;
  }

  parseGroupConcat (): Expression | undefined {
    const concatExprs = (
      node: Expression | undefined,
      exprs: Expression[],
    ): Expression => {
      if (node instanceof DistinctExpr && 1 < (node.args.expressions?.length ?? 0)) {
        const concatExpressions = [
          this.expression(ConcatExpr, {
            expressions: node.args.expressions,
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

      if (order?.args.this) {
        args[args.length - 1] = order.args.this;
        order.setArgKey('this', concatExprs(order.args.this, args));
      }

      thisValue = order || concatExprs(args[0], args);
    } else {
      thisValue = undefined;
    }

    const separator = this.match(TokenType.SEPARATOR) ? this.parseField() : undefined;

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
      if (!this.match(TokenType.L_PAREN)) {
        break;
      }

      let op = '';
      while (this.curr && !this.match(TokenType.R_PAREN)) {
        op += this.curr.text;
        this.advance();
      }

      result = this.expression(OperatorExpr, {
        comments: this.prevComments,
        this: result,
        operator: op,
        expression: this.parseBitwise(),
      });

      if (!this.match(TokenType.OPERATOR)) {
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
      this.pipeCteCounter += 1;
      newCte = `__tmp${this.pipeCteCounter}`;
    }

    const with_ = query.args.with;
    const ctes = with_?.pop();

    const newSelect = select(...expressions, { copy: false }).from(newCte, { copy: false });
    if (ctes) {
      newSelect.setArgKey('with', ctes);
    }

    return newSelect.with(newCte, query, {
      copy: false,
    });
  }

  parsePipeSyntaxSelect (query: SelectExpr): SelectExpr {
    const select = this.parseSelect({ consumePipe: false }) as SelectExpr;
    if (!select) {
      return query;
    }

    return this.buildPipeCte({
      query: query.select(select.args.expressions, { append: false }),
      expressions: [new StarExpr({})],
    });
  }

  parsePipeSyntaxLimit (query: SelectExpr): SelectExpr {
    const limit = this.parseLimit() as LimitExpr;
    const offset = this.parseOffset() as OffsetExpr;
    if (limit) {
      const currLimit = query.args.limit || limit;
      let currLimitValue: number;
      if (typeof currLimit === 'number') {
        currLimitValue = currLimit;
      } else {
        assertIsInstanceOf(currLimit, LimitExpr);
        const expr = currLimit.args.expression;
        assertIsInstanceOf(expr, Expression);
        currLimitValue = expr.toValue() as number;
      }
      const limitValue = (limit.args.expression?.toValue() ?? 0) as number;
      if (limitValue <= currLimitValue) {
        query.limit(limit, { copy: false });
      }
    }
    if (offset) {
      const currOffset = query.args.offset;
      let currOffsetVal: number;
      if (currOffset === undefined) {
        currOffsetVal = 0;
      } else if (typeof currOffset === 'number') {
        currOffsetVal = currOffset;
      } else {
        assertIsInstanceOf(currOffset, OffsetExpr);
        const expr = currOffset.args.expression;
        currOffsetVal = typeof expr === 'number' ? expr : (assertIsInstanceOf(expr, Expression), expr.toValue() as number);
      }
      const offsetExpr = offset.args.expression;
      const offsetVal: number = typeof offsetExpr === 'number' ? offsetExpr : (assertIsInstanceOf(offsetExpr, Expression), offsetExpr.toValue() as number);

      query.offset(LiteralExpr.number(currOffsetVal + offsetVal));
    }

    return query;
  }

  parsePipeSyntaxAggregateFields (): Expression | undefined {
    let thisValue = this.parseDisjunction();
    if (this.matchTextSeq(['GROUP', 'AND'], { advance: false })) {
      return thisValue;
    }

    thisValue = this.parseAlias(thisValue);

    if (this.matchSet([TokenType.ASC, TokenType.DESC], { advance: false })) {
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
      let thisValue: Expression | undefined;
      if (element instanceof OrderedExpr) {
        thisValue = element.args.this;
        if (thisValue instanceof AliasExpr) {
          element.setArgKey('this', thisValue.args.alias);
        }
        orders.push(element);
      } else {
        thisValue = element;
      }
      if (thisValue) aggregatesOrGroups.push(thisValue);
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
    this.matchTextSeq('AGGREGATE');
    query = this.parsePipeSyntaxAggregateGroupOrderBy(query, { groupByExists: false });

    if (
      this.match(TokenType.GROUP_BY)
      || (this.matchTextSeq(['GROUP', 'AND']) && this.match(TokenType.ORDER_BY))
    ) {
      query = this.parsePipeSyntaxAggregateGroupOrderBy(query);
    }

    return this.buildPipeCte({
      query,
      expressions: [new StarExpr({})],
    });
  }

  parsePipeSyntaxSetOperator (query?: QueryExpr): QueryExpr | undefined {
    if (!query) {
      return undefined;
    }
    const firstSetop = this.parseSetOperation(query) as SetOperationExpr;
    if (!firstSetop) {
      return undefined;
    }

    const parseAndUnwrapQuery = (): SelectExpr | undefined => {
      const expr = this.parseParen();
      return expr ? expr.assertIs(SubqueryExpr).unnest() as SelectExpr : undefined;
    };

    firstSetop.args.this?.pop();

    const setops = [
      firstSetop.args.expression?.pop().assertIs(SubqueryExpr)
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

    if (!query) {
      return undefined;
    }

    query.setArgKey('with', ctes);

    return this.buildPipeCte({
      query,
      expressions: [new StarExpr({})],
    });
  }

  parsePipeSyntaxJoin (query?: QueryExpr): QueryExpr | undefined {
    if (!query) return undefined;
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
      const fromThis = from.args.this;
      if (isInstanceOf(fromThis, Expression)) {
        fromThis.setArgKey('pivots', pivots);
      }
    }

    return this.buildPipeCte({
      query,
      expressions: [new StarExpr({})],
    });
  }

  parsePipeSyntaxExtend (query: SelectExpr): SelectExpr {
    this.matchTextSeq('EXTEND');
    query.select([new StarExpr({}), ...this.parseExpressions()], {
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
      const expression = with_.args.expressions?.[with_.args.expressions.length - 1];
      if (expression instanceof Expression && expression.args.this instanceof Expression) {
        expression.args.this.setArgKey('sample', sample);
      }
    } else {
      query.setArgKey('sample', sample);
    }

    return query;
  }

  parsePipeSyntaxQuery (query: QueryExpr): QueryExpr | undefined {
    let result: QueryExpr | undefined = query;

    if (result instanceof SubqueryExpr) {
      result = select('*').from(result, { copy: false });
    }

    if (!result.args.from) {
      result = select('*').from(result.subquery(undefined, { copy: false }), { copy: false });
    }

    while (this.match(TokenType.PIPE_GT)) {
      const start = this.curr;
      const startIdx = start ? this.tokens.indexOf(start) : -1;
      const parser =
        this._constructor.PIPE_SYNTAX_TRANSFORM_PARSERS[this.curr?.text.toUpperCase() ?? ''];
      if (!parser) {
        let parsedQuery = this.parsePipeSyntaxSetOperator(result);
        parsedQuery = parsedQuery || this.parsePipeSyntaxJoin(result);
        if (!parsedQuery && 0 <= startIdx) {
          this.retreat(startIdx);
          this.raiseError(`Unsupported pipe syntax operator: '${start?.text.toUpperCase()}'.`);
          break;
        }
        result = parsedQuery;
      } else {
        result = parser.call(this, result as SelectExpr);
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
      default: this.match(TokenType.DEFAULT) && this.parseBitwise(),
    });
  }

  parseDeclare (): DeclareExpr | CommandExpr {
    const start = this.prev;
    const expressions = this.tryParse(() => this.parseCsv(this.parseDeclareitem.bind(this)));

    if (!expressions || this.curr) {
      return this.parseAsCommand(start);
    }

    return this.expression(DeclareExpr, { expressions });
  }

  buildCast (options: {
    strict: boolean;
    [key: string]: unknown;
  }): CastExpr | TryCastExpr {
    const { strict } = options;
    const ExpClass = strict ? CastExpr : TryCastExpr;

    const kwargs: Record<string, unknown> = { ...options };
    delete kwargs.strict;

    if (ExpClass === TryCastExpr) {
      kwargs.requiresString = this._dialectConstructor.TRY_CAST_REQUIRES_STRING;
    }

    return this.expression(ExpClass, kwargs);
  }

  parseJsonValue (): JsonValueExpr {
    const thisValue = this.parseBitwise();
    this.match(TokenType.COMMA);
    const path = this.parseBitwise();

    const returning = (this.match(TokenType.RETURNING) || undefined) && this.parseType();

    return this.expression(JsonValueExpr, {
      this: thisValue,
      path: this.dialect.toJsonPath(path),
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

  get _constructor (): typeof Parser {
    return this.constructor as typeof Parser;
  }

  get _dialectConstructor (): typeof Dialect {
    return this.dialect._constructor;
  }
}
