export interface CacheExprArgs {
  lazy?: Expression;
  options?: Expression[];
}

export interface UncacheExprArgs {
  exists?: Expression;
}

export interface RefreshExprArgs {
  kind: RefreshExprKind | undefined;
}

export interface SequencePropertiesExprArgs {
  increment?: Expression;
  minvalue?: string;
  maxvalue?: string;
  cache?: Expression;
  start?: Expression;
  owned?: Expression;
  options?: Expression[];
}

export interface TruncateTableExprArgs {
  isDatabase?: string;
  exists?: Expression;
  only?: Expression;
  cluster?: Expression;
  identity?: Expression;
  option?: Expression;
  partition?: Expression;
}

export interface CloneExprArgs {
  shallow?: Expression;
  copy?: unknown;
}

export interface DescribeExprArgs {
  style?: Expression;
  kind?: DescribeExprKind | undefined;
  partition?: Expression;
  format?: string;
  asJson?: Expression;
}

export interface AttachExprArgs {
  exists?: Expression;
}

export interface DetachExprArgs {
  exists?: Expression;
}

export interface InstallExprArgs {
  from?: Expression;
  force?: Expression;
}

export interface SummarizeExprArgs {
  table?: Expression;
}

export interface KillExprArgs {
  kind?: KillExprKind | undefined;
}

export interface DeclareItemExprArgs {
  kind?: DeclareItemExprKind | undefined;
  default?: Expression;
}

export interface SetExprArgs {
  unset?: Expression;
  tag?: Expression;
}

export interface HeredocExprArgs {
  tag?: Expression;
}

export interface SetItemExprArgs {
  kind?: SetItemExprKind | undefined;
  collate?: string;
  global?: boolean;
}

export interface QueryBandExprArgs {
  scope?: Expression;
  update?: Expression;
}

export interface ShowExprArgs {
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
}

export interface UserDefinedFunctionExprArgs {
  wrapped?: Expression;
}

export interface CharacterSetExprArgs {
  default?: Expression;
}

export interface RecursiveWithSearchExprArgs {
  kind: RecursiveWithSearchExprKind | undefined;
  using?: string;
}

export interface WithExprArgs {
  recursive?: boolean;
  search?: Expression;
}

export interface TableAliasExprArgs {
  columns?: Expression[];
}

export interface ColumnPositionExprArgs {
  position: Expression;
}

export interface ColumnDefExprArgs {
  kind?: ColumnDefExprKind | undefined;
  constraints?: Expression[];
  exists?: Expression;
  position?: Expression;
  default?: Expression;
  output?: Expression;
}

export interface AlterColumnExprArgs {
  dtype?: DataTypeExpr;
  collate?: string;
  using?: string;
  default?: Expression;
  drop?: Expression;
  comment?: string;
  allowNull?: Expression;
  visible?: Expression;
  renameTo?: string;
}

export interface AlterIndexExprArgs {
  visible: Expression;
}

export interface AlterSortKeyExprArgs {
  compound?: Expression;
}

export interface AlterSetExprArgs {
  option?: Expression;
  tablespace?: Expression;
  accessMethod?: string;
  fileFormat?: string;
  copyOptions?: Expression[];
  tag?: Expression;
  location?: Expression;
  serde?: Expression;
}

export interface RenameColumnExprArgs {
  to: Expression;
  exists?: Expression;
}

export interface CommentExprArgs {
  kind: CommentExprKind | undefined;
  exists?: Expression;
  materialized?: boolean;
}

export interface ComprehensionExprArgs {
  position?: Expression;
  iterator: Expression;
  condition?: Expression;
}

export interface MergeTreeTTLActionExprArgs {
  delete?: Expression;
  recompress?: Expression[];
  toDisk?: Expression;
  toVolume?: Expression;
}

export interface MergeTreeTTLExprArgs {
  where?: Expression;
  group?: Expression;
  aggregates?: Expression[];
}

export interface IndexConstraintOptionExprArgs {
  keyBlockSize?: number | Expression;
  using?: string;
  parser?: Expression;
  comment?: string;
  visible?: Expression;
  engineAttr?: string;
  secondaryEngineAttr?: string;
}

export interface ColumnConstraintExprArgs {
  kind: ColumnConstraintExprKind | undefined;
}

export interface WithOperatorExprArgs {
  op: Expression;
}

export interface DropExprArgs {
  kind?: DropExprKind | undefined;
  exists?: Expression;
  temporary?: boolean;
  materialized?: boolean;
  cascade?: Expression;
  constraints?: Expression[];
  purge?: Expression;
  cluster?: Expression;
  concurrently?: Expression;
}

export interface ExportExprArgs {
  connection?: Expression;
  options: Expression[];
}

export interface ChangesExprArgs {
  information: string;
  atBefore?: Expression;
  end?: Expression;
}

export interface ConnectExprArgs {
  start?: Expression;
  connect: Expression;
  nocycle?: Expression;
}

export interface CredentialsExprArgs {
  credentials?: Expression[];
  encryption?: Expression;
  storage?: Expression;
  iamRole?: Expression;
  region?: Expression;
}

export interface DirectoryExprArgs {
  local?: Expression;
  rowFormat?: string;
}

export interface ForeignKeyExprArgs {
  reference?: Expression;
  delete?: Expression;
  update?: Expression;
  options?: Expression[];
}

export interface PrimaryKeyExprArgs {
  options?: Expression[];
  include?: Expression;
}

export interface IntoExprArgs {
  temporary?: boolean;
  unlogged?: Expression;
  bulkCollect?: Expression;
}

export interface IdentifierExprArgs {
  quoted?: boolean;
  global?: boolean;
  temporary?: boolean;
}

export interface IndexExprArgs {
  table?: Expression;
  unique?: boolean;
  primary?: boolean;
  amp?: Expression;
  params?: Expression[];
}

export interface IndexParametersExprArgs {
  using?: string;
  include?: Expression;
  columns?: Expression[];
  withStorage?: Expression;
  partitionBy?: Expression;
  tablespace?: Expression;
  where?: Expression;
  on?: Expression;
}

export interface ConditionalInsertExprArgs {
  else?: Expression;
}

export interface MultitableInsertsExprArgs {
  kind: MultitableInsertsExprKind | undefined;
  source: Expression;
}

export interface OnConflictExprArgs {
  duplicate?: Expression;
  action?: Expression;
  conflictKeys?: Expression[];
  indexPredicate?: Expression;
  constraint?: Expression;
  where?: Expression;
}

export interface OnConditionExprArgs {
  error?: Expression;
  empty?: Expression;
  null?: Expression;
}

export interface ReturningExprArgs {
  into?: Expression;
}

export interface LoadDataExprArgs {
  local?: Expression;
  overwrite?: Expression;
  inpath: Expression;
  partition?: Expression;
  inputFormat?: string;
  serde?: Expression;
}

export interface PartitionExprArgs {
  subpartition?: Expression;
}

export interface FetchExprArgs {
  direction?: Expression;
  count?: Expression;
  limitOptions?: Expression[];
}

export interface GrantExprArgs {
  privileges: Expression[];
  kind?: GrantExprKind | undefined;
  securable: Expression;
  principals: Expression[];
  grantOption?: Expression;
}

export interface RevokeExprArgs {
  cascade?: Expression;
}

export interface GroupExprArgs {
  groupingSets?: Expression[];
  cube?: Expression;
  rollup?: Expression;
  totals?: Expression[];
  all?: Expression;
}

export interface LambdaExprArgs {
  colon?: Expression;
}

export interface LimitExprArgs {
  offset?: boolean;
  limitOptions?: Expression[];
}

export interface LimitOptionsExprArgs {
  percent?: Expression;
  rows?: Expression[];
  withTies?: Expression[];
}

export interface JoinExprArgs {
  on?: Expression;
  side?: Expression;
  kind?: JoinExprKind | undefined;
  using?: string;
  method?: string;
  global?: boolean;
  hint?: Expression;
  matchCondition?: Expression;
  directed?: Expression;
  pivots?: Expression[];
}

export interface MatchRecognizeMeasureExprArgs {
  windowFrame?: Expression;
}

export interface MatchRecognizeExprArgs {
  partitionBy?: Expression;
  order?: Expression;
  measures?: Expression[];
  rows?: Expression[];
  after?: Expression;
  pattern?: Expression;
  define?: Expression;
}

export interface OrderExprArgs {
  siblings?: Expression[];
}

export interface WithFillExprArgs {
  from?: Expression;
  to?: Expression;
  step?: Expression;
  interpolate?: Expression;
}

export interface OrderedExprArgs {
  desc?: Expression;
  nullsFirst: Expression;
  withFill?: Expression;
}

export interface PropertyExprArgs {
  value: string;
}

export interface GrantPrincipalExprArgs {
  kind?: GrantPrincipalExprKind | undefined;
}

export interface PartitionByRangePropertyDynamicExprArgs {
  start: Expression;
  end: Expression;
  every: Expression;
}

export interface RollupIndexExprArgs {
  fromIndex?: Expression;
  properties?: Expression[];
}

export interface PartitionBoundSpecExprArgs {
  fromExpressions?: Expression[];
  toExpressions?: Expression[];
}

export interface QueryTransformExprArgs {
  commandScript: Expression;
  schema?: Expression;
  rowFormatBefore?: string;
  recordWriter?: Expression;
  rowFormatAfter?: string;
  recordReader?: Expression;
}

export interface SemanticViewExprArgs {
  metrics?: Expression[];
  dimensions?: Expression[];
  facts?: Expression[];
  where?: Expression;
}

export interface InputOutputFormatExprArgs {
  inputFormat?: string;
  outputFormat?: string;
}

export interface ReferenceExprArgs {
  options?: Expression[];
}

export interface IndexTableHintExprArgs {
  target?: Expression;
}

export interface HistoricalDataExprArgs {
  kind: HistoricalDataExprKind | undefined;
}

export interface PutExprArgs {
  target: Expression;
  properties?: Expression[];
}

export interface GetExprArgs {
  target: Expression;
  properties?: Expression[];
}

export interface TableExprArgs {
  db?: string;
  catalog?: string;
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
  only?: Expression;
  partition?: Expression;
  changes?: Expression[];
  rowsFrom?: number | Expression;
  sample?: number | Expression;
  indexed?: Expression;
}

export interface VersionExprArgs {
  kind: VersionExprKind | undefined;
}

export interface LockExprArgs {
  update: Expression;
  wait?: Expression;
  key?: unknown;
}

export interface TableSampleExprArgs {
  method?: string;
  bucketNumerator?: Expression;
  bucketDenominator?: Expression;
  bucketField?: Expression;
  percent?: Expression;
  rows?: Expression[];
  size?: number | Expression;
  seed?: Expression;
}

export interface TagExprArgs {
  prefix?: Expression;
  postfix?: Expression;
}

export interface PivotExprArgs {
  fields?: Expression[];
  unpivot?: Expression;
  using?: string;
  group?: Expression;
  columns?: Expression[];
  includeNulls?: Expression[];
  defaultOnNull?: Expression;
  into?: Expression;
  with?: Expression;
}

export interface WindowSpecExprArgs {
  kind?: WindowSpecExprKind | undefined;
  start?: Expression;
  startSide?: Expression;
  end?: Expression;
  endSide?: Expression;
  exclude?: Expression;
}

export interface StarExprArgs {
  except?: Expression;
  replace?: boolean;
  rename?: string;
}

export interface DataTypeExprArgs {
  nested?: Expression;
  values?: Expression[];
  prefix?: Expression;
  kind?: DataTypeExprKind | string | undefined;
  nullable?: Expression;
}

export interface TransactionExprArgs {
  modes?: Expression[];
  mark?: Expression;
}

export interface CommitExprArgs {
  chain?: Expression;
  durability?: Expression;
}

export interface RollbackExprArgs {
  savepoint?: Expression;
}

export interface AlterExprArgs {
  kind: AlterExprKind | undefined;
  actions: Expression[];
  exists?: Expression;
  only?: Expression;
  options?: Expression[];
  cluster?: Expression;
  notValid?: Expression;
  check?: Expression;
  cascade?: Expression;
}

export interface AlterSessionExprArgs {
  unset?: Expression;
}

export interface AnalyzeExprArgs {
  kind?: AnalyzeExprKind | undefined;
  options?: Expression[];
  mode?: Expression;
  partition?: Expression;
  properties?: Expression[];
}

export interface AnalyzeStatisticsExprArgs {
  kind: AnalyzeStatisticsExprKind | undefined;
  option?: Expression;
}

export interface AnalyzeHistogramExprArgs {
  updateOptions?: Expression[];
}

export interface AnalyzeSampleExprArgs {
  kind: AnalyzeSampleExprKind | undefined;
  sample: number | Expression;
}

export interface AnalyzeDeleteExprArgs {
  kind?: AnalyzeDeleteExprKind | undefined;
}

export interface AnalyzeValidateExprArgs {
  kind: AnalyzeValidateExprKind | undefined;
}

export interface AddPartitionExprArgs {
  exists?: Expression;
  location?: Expression;
}

export interface DropPartitionExprArgs {
  exists?: Expression;
}

export interface ReplacePartitionExprArgs {
  source: Expression;
}

export interface AtTimeZoneExprArgs {
  zone: Expression;
}

export interface FromTimeZoneExprArgs {
  zone: Expression;
}

export interface FormatPhraseExprArgs {
  format: string;
}

export interface DistinctExprArgs {
  on?: Expression;
}

export interface TimeUnitExprArgs {
  unit?: Expression;
}

export interface HavingMaxExprArgs {
  max: Expression;
}

export interface TranslateCharactersExprArgs {
  withError?: Expression;
}

export interface OverflowTruncateBehaviorExprArgs {
  withCount: Expression;
}

export interface JSONExprArgs {
  with?: Expression;
  unique?: boolean;
}

export interface JSONPathExprArgs {
  escape?: Expression;
}

export interface JSONColumnDefExprArgs {
  kind?: JSONColumnDefExprKind | undefined;
  path?: Expression;
  nestedSchema?: Expression;
  ordinality?: boolean;
}

export interface JSONValueExprArgs {
  path: Expression;
  returning?: Expression;
  onCondition?: Expression;
}

export interface OpenJSONColumnDefExprArgs {
  kind: OpenJSONColumnDefExprKind | undefined;
  path?: Expression;
  asJson?: Expression;
}

export interface JSONExtractQuoteExprArgs {
  option: Expression;
  scalar?: Expression;
}

export interface SliceExprArgs {
  step?: Expression;
}

export interface UseExprArgs {
  kind?: UseExprKind | undefined;
}

export interface WhenExprArgs {
  matched: Expression;
  source?: Expression;
  condition?: Expression;
  then: Expression;
}

export interface PredicateExprArgs {
  with?: Expression;
  kind: CreateExprKind | undefined;
  exists?: Expression;
  properties?: Expression[];
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
  scalar?: Expression;
  materialized?: boolean;
  keyExpressions?: Expression[];
}

export interface HexStringExprArgs {
  isInteger?: Expression;
}

export interface ByteStringExprArgs {
  isBytes?: Expression[];
}

export interface UnicodeStringExprArgs {
  escape?: Expression;
}

export interface ColumnExprArgs {
  table?: Expression;
  db?: string;
  catalog?: string;
  joinMark?: Expression;
  not: Expression;
  enforced?: Expression;
  onNull?: Expression;
  start?: Expression;
  increment?: Expression;
  minvalue?: string;
  maxvalue?: string;
  cycle?: Expression;
  order?: Expression;
  hidden?: Expression;
  kind: IndexColumnConstraintExprKind | undefined;
  indexType?: DataTypeExpr;
  options?: Expression[];
  granularity?: Expression;
  allowNull?: Expression;
  desc?: Expression;
  onConflict?: Expression;
  nulls?: Expression[];
  persisted?: Expression;
  notNull?: Expression;
  dataType?: DataTypeExpr;
  input?: Expression;
  output?: Expression;
  variadic?: Expression;
  with?: Expression;
  using?: string;
  where?: Expression;
  returning?: Expression;
  limit?: number | Expression;
  tables?: Expression[];
  cluster?: Expression;
  files?: Expression[];
  credentials?: Expression[];
  format?: string;
  params?: Expression[];
  hint?: Expression;
  isFunction?: Expression;
  conflict?: Expression;
  overwrite?: Expression;
  exists?: Expression;
  alternative?: Expression;
  ignore?: Expression;
  byName?: string;
  stored?: Expression;
  partition?: Expression;
  settings?: Expression[];
  source?: Expression;
  default?: Expression;
}

export interface LiteralExprArgs {
  isString: unknown;
  value?: string;
  autotemp?: Expression;
  always?: Expression[];
  default?: Expression;
  manual?: Expression;
  never?: Expression;
  on?: Expression;
  size?: number | Expression;
  units?: Expression[];
  minimum?: Expression;
  maximum?: Expression;
  filterColumn?: Expression;
  retentionPeriod?: Expression;
  kind?: DistributedByPropertyExprKind | undefined;
  buckets: Expression[];
  order?: Expression;
  no: Expression;
  protection?: Expression;
  hiveFormat?: string;
  percent?: Expression;
  concurrent?: Expression;
  target?: Expression;
  dual?: Expression;
  before?: Expression;
  local?: Expression;
  after?: Expression;
  sortedBy?: string;
  settings?: Expression[];
  min: Expression;
  max: Expression;
  forOrIn?: Expression;
  lockType: DataTypeExpr;
  override?: Expression;
  delete?: Expression;
  partitionExpressions: Expression[];
  createExpressions: Expression[];
  method?: string;
  every?: Expression;
  unit?: Expression;
  starts?: Expression[];
  isTable?: Expression;
  table?: Expression;
  null?: Expression;
  fields?: Expression[];
  escaped?: Expression;
  collectionItems?: Expression[];
  mapKeys?: Expression[];
  lines?: Expression[];
  serde?: Expression;
  serdeProperties?: Expression[];
  with?: Expression;
  multi: Expression;
  compound?: Expression;
  statistics?: Expression[];
  dataConsistency?: Expression;
  properties?: Expression[];
  key?: unknown;
  columnDef?: Expression;
  distinct?: boolean;
  byName?: string;
  side?: Expression;
  from?: Expression;
  where?: Expression;
  returning?: Expression;
  limit?: number | Expression;
  options?: Expression[];
  hint?: Expression;
  into?: Expression;
  operationModifiers?: Expression[];
}

export interface WindowExprArgs {
  partitionBy?: Expression;
  order?: Expression;
  spec?: Expression;
  over?: Expression;
  first?: Expression;
}

export interface SessionParameterExprArgs {
  kind?: SessionParameterExprKind | undefined;
}

export interface PlaceholderExprArgs {
  kind?: PlaceholderExprKind | undefined;
  widget?: Expression;
  jdbc?: string;
}

export interface BracketExprArgs {
  offset?: boolean;
  safe?: boolean;
  returnsListForMaps?: Expression[];
  unit?: Expression;
}

export interface FuncExprArgs {
  start: Expression;
  end: Expression;
  step: Expression;
  using: string;
  on?: Expression;
  usingCond?: string;
  whens: Expression[];
  with?: Expression;
  returning?: Expression;
  view?: Expression;
  outer?: Expression;
  crossApply?: boolean;
  ordinality?: boolean;
  joins?: Expression[];
  pivots?: Expression[];
  sample?: number | Expression;
  order?: Expression;
  limit?: number | Expression;
  offset: boolean;
  padside?: Expression;
  requiresInt128?: Expression;
  typed?: DataTypeExpr;
  safe?: boolean;
  operator: Expression;
  low: Expression;
  high: Expression;
  symmetric?: Expression;
  query?: Expression;
  unnest?: boolean;
  field?: Expression;
  isGlobal?: Expression;
  roundInput?: Expression;
  nullPropagation?: Expression;
  from: Expression;
  to?: Expression;
  bracketNotation?: Expression;
  structNameInheritance?: string;
  fillPattern?: Expression;
  isLeft: Expression;
  format?: string;
  nlsparam?: Expression;
  isNumeric?: Expression;
  precision?: number | Expression;
  scale?: number | Expression;
  safeName?: boolean;
  path: Expression;
  unpack?: Expression;
  style?: Expression;
  dest: Expression;
  source?: Expression;
  sourceTz?: Expression;
  targetTz: Expression;
  timestamp: Expression;
  options?: Expression[];
  isEndExclusive?: Expression;
  rowcount?: Expression;
  timelimit?: number | Expression;
  categories: Expression[];
  config?: Expression;
  position?: Expression;
  ensureVariant?: Expression;
  null?: Expression;
  useSpheroid?: Expression;
  zone?: Expression;
  ifs: Expression[];
  default?: Expression;
  action?: Expression;
  disableAutoConvert?: Expression;
  decimals?: Expression[];
  isNvl?: Expression;
  isNull?: Expression;
  charset: string;
  coalesce?: Expression;
  jsonScope?: Expression;
  sysdate?: Expression;
  unit?: Expression;
  origin?: Expression;
  bigInt?: Expression;
  datePartBoundary?: Expression;
  inputTypePreserved?: DataTypeExpr;
  abbreviated?: Expression;
  roundoff?: Expression;
  year: Expression;
  month?: Expression;
  week?: Expression;
  day?: Expression;
  hour: Expression;
  minute?: Expression;
  second?: Expression;
  withTz?: Expression;
  kind?: TimeSliceExprKind | undefined;
  allowOverflow?: Expression;
  min: Expression;
  sec: Expression;
  nano?: Expression;
  fractions?: Expression[];
  overflow?: Expression;
  replace?: boolean;
  passphrase: Expression;
  aad?: Expression;
  encryptionMethod?: string;
  key?: unknown;
  iv: Expression;
  aead?: Expression;
  explodeArray?: Expression;
  alphabet?: Expression;
  maxLineLength?: number | Expression;
  tsColumn: Expression;
  bucketWidth: Expression;
  partitioningColumns?: Expression[];
  valueColumns?: Expression[];
  ignoreNulls: Expression[];
  zeroIsMsb?: Expression;
  case?: Expression;
  method: string;
  true: Expression;
  false?: Expression;
  mode?: Expression;
  nullHandling?: Expression;
  uniqueKeys?: Expression[];
  returnType?: DataTypeExpr;
  encoding?: Expression;
  strict?: Expression;
  passing?: Expression;
  onCondition?: Expression;
  fromDcolonqmark?: Expression;
  includeArrays?: Expression[];
  removeEmpty?: Expression;
  schema: Expression;
  errorHandling?: Expression;
  emptyHandling?: Expression;
  value: string;
  updateFlag?: Expression;
  onlyJsonTypes?: Expression[];
  variantExtract?: string;
  jsonQuery?: Expression;
  option?: Expression;
  quote?: Expression;
  requiresJson?: Expression;
  jsonType?: Expression;
  scalarOnly?: Expression;
  isJson?: Expression;
  toJson?: Expression;
  partToExtract?: Expression;
  permissive?: Expression;
  binary?: Expression;
  insCost?: Expression;
  delCost?: Expression;
  subCost?: Expression;
  maxDist?: Expression;
  keys: Expression[];
  values: Expression[];
  modifier?: Expression;
  preserveEndOfMonth?: Expression;
  form?: Expression;
  isCasefold?: Expression;
  stddev: Expression;
  gen: Expression;
  for?: Expression;
  paramsStruct?: Expression;
  time?: Expression;
  numRows?: Expression[];
  ignoreFeatureNulls?: Expression[];
  isText?: string;
  columnToSearch: Expression;
  queryTable: Expression;
  queryColumnToSearch?: Expression;
  topK?: Expression;
  distanceType?: DataTypeExpr;
  percentile: Expression;
  lower?: Expression;
  upper?: Expression;
  generator?: Expression;
  each?: Expression;
  initial: Expression;
  merge: Expression;
  finish?: Expression;
  occurrence?: Expression;
  parameters?: Expression[];
  group?: Expression;
  nullIfPosOverflow?: Expression;
  replacement?: boolean;
  modifiers?: Expression[];
  singleReplace?: Expression;
  flag?: Expression;
  times: Expression[];
  truncate?: Expression;
  castsNonIntegerDecimals?: Expression[];
  length: number | Expression;
  asc?: Expression;
  nullsFirst?: Expression;
  delimiter: number | Expression;
  partIndex?: Expression;
  count: Expression;
  substr: Expression;
  analyzer?: Expression;
  analyzerOptions?: Expression[];
  searchMode?: Expression;
  targetType?: DataTypeExpr;
  pairDelim?: Expression;
  keyValueDelim?: string;
  duplicateResolutionCallback?: Expression;
  culture?: Expression;
  collation?: Expression;
  seed?: Expression;
  hours?: Expression[];
  minutes?: Expression[];
  name?: unknown;
  isString?: unknown;
  milli?: Expression;
  nullOnZeroVariance?: Expression;
  minValue?: string;
  maxValue?: string;
  numBuckets?: Expression[];
  threshold?: Expression;
  evalname?: string;
  instance?: Expression;
  namespaces?: Expression[];
  columns?: Expression[];
  byRef?: Expression;
  elementcount: Expression;
  params: Expression[];
  counters?: Expression[];
  accuracy?: Expression;
  nullsExcluded?: Expression;
  fromFirst?: Expression;
  requiresString?: Expression;
  separator?: Expression;
  onOverflow?: Expression;
  deterministic?: Expression;
  quantile: Expression;
  weight?: Expression;
  errorTolerance?: Expression;
}