import type {
  AlterSetExpr,
  ClusterExpr,
  CommandExpr,
  CurrentDateExpr,
  DateSubExpr,
  DescribeExpr,
  ExpressionOrString,
  ExpressionValue,
  FormatExpr,
  FuncExpr,
  GeneratedAsIdentityColumnConstraintExpr,
  LogExpr,
  PropertiesExpr,
  SearchExprArgs,
  TimestampSubExpr,
  TimeToStrExpr,
  UniformExpr,
  ValuesExpr,
} from '../expressions';
import {
  DotExpr,
  isType,
  EndsWithExpr,
  LowerHexExpr,
  PartitionedByPropertyExpr,
  PercentileContExpr,
  PercentileDiscExpr,
  RandExpr,
  StartsWithExpr,
  StPointExpr,
  StringToArrayExpr,
  StrPositionExpr,
  StrToDateExpr,
  StuffExpr,
  TimeSliceExpr,
  DirectoryStageExpr,
  MaskingPolicyColumnConstraintExpr,
  NotExpr,
  ProjectionPolicyColumnConstraintExpr,
  TagsExpr,
  UseExpr,
  NthValueExpr, RegexpILikeExpr,
  SetExpr,
  SetItemExpr,
  ShowExpr,
  PowExpr, RegexpExtractAllExpr, RegexpLikeExpr,
  Sha1DigestExpr,
  Sha2DigestExpr,
  Sha2Expr,
  ShaExpr,
  StddevExpr,
  TableFromRowsExpr,
  TimeAddExpr,
  TimeFromPartsExpr,
  ArrayAggExpr,
  ArrayExpr,
  Expression, FromExpr, GenerateDateArrayExpr, JsonExtractExpr, JsonValueArrayExpr, LambdaExpr, LateralExpr, ParseJsonExpr, RegexpExtractExpr,
  select,
  StarExpr,
  toIdentifier,
  DateTruncExpr, IfExpr, KwargExpr, PivotAnyExpr, PivotExpr, RegexpReplaceExpr, SearchExpr, TimestampTruncExpr,
  AnonymousExpr,
  ApproxTopKExpr,
  BinaryExpr,
  BitwiseAndExpr,
  BitwiseLeftShiftExpr,
  BitwiseOrExpr,
  BitwiseRightShiftExpr,
  BitwiseXorExpr,
  CastExpr,
  DataTypeExpr,
  DataTypeExprKind,
  DateDiffExpr,
  DateFromPartsExpr,
  DivExpr,
  EqExpr,
  IdentifierExpr,
  IsExpr,
  LiteralExpr,
  NegExpr,
  null_,
  PropertyEqExpr,
  SplitPartExpr,
  StarMapExpr,
  StrToTimeExpr,
  StructExpr, TryCastExpr, TsOrDsToDateExpr, TsOrDsToTimeExpr, UnixToTimeExpr,
  wrap,
  UnnestExpr,
  SelectExpr,
  alias,
  CreateExpr,
  IcebergPropertyExpr,
  SchemaExpr,
  ColumnDefExpr,
  IntervalExpr,
  DateAddExpr,
  TableAliasExpr,
  JoinExpr,
  ToNumberExpr,
  GeneratorExpr,
  RoundExpr,
  TimestampFromPartsExpr,
  BracketExpr,
  ColumnExpr,
  TableExpr,
  LocaltimeExpr,
  AddMonthsExpr,
  ApproxQuantileExpr,
  ArrayContainsExpr,
  GenerateSeriesExpr,
  SubExpr,
  SortArrayExpr,
  FlattenExpr,
  BitwiseNotExpr,
  BitwiseAndAggExpr,
  BitwiseOrAggExpr,
  BitwiseXorAggExpr,
  BitmapOrAggExpr,
  BoolandExpr,
  BoolorExpr,
  XorExpr,
  CorrExpr,
  DayOfWeekIsoExpr,
  DaynameExpr,
  LevenshteinExpr,
  ExplodeExpr,
  GetExtractExpr,
  CurrentTimestampExpr,
  GreatestExpr,
  LeastExpr,
  UnhexExpr,
  Md5Expr,
  Md5DigestExpr,
  Md5NumberLower64Expr,
  Md5NumberUpper64Expr,
  MonthnameExpr,
  LastDayExpr,
  LengthExpr,
  JsonKeysExpr,
  ByteLengthExpr,
  ParseUrlExpr,
  DecryptExpr,
  DecryptRawExpr,
  ToBinaryExpr,
  ToBooleanExpr,
  ToDoubleExpr,
  ToFileExpr,
  JsonFormatExpr,
  CosineDistanceExpr,
  DotProductExpr,
  ManhattanDistanceExpr,
  EuclideanDistanceExpr,
  LikeExpr,
  ILikeExpr,
  SkewnessExpr,
  WeekOfYearExpr,
  WeekExpr,
  ModelAttributeExpr,
  ApproxDistinctExpr,
  ArgMaxExpr,
  ArgMinExpr,
  ArrayConcatExpr,
  ArrayAppendExpr,
  ArrayPrependExpr,
  ArrayIntersectExpr,
  AtTimeZoneExpr,
  LocaltimestampExpr,
  DatetimeAddExpr,
  DatetimeDiffExpr,
  DateStrToDateExpr,
  DayOfMonthExpr,
  DayOfWeekExpr,
  DayOfYearExpr,
  ExtractExpr,
  FileFormatPropertyExpr,
  FromTimeZoneExpr,
  GroupConcatExpr,
  JsonExtractScalarExpr,
  JsonObjectExpr,
  JsonPathRootExpr,
  JsonPathKeyExpr,
  JsonPathSubscriptExpr,
  LocationPropertyExpr,
  LogicalAndExpr,
  LogicalOrExpr,
  MapExpr,
  PutExpr,
  GetExpr,
  VarExpr,
  ForeignKeyExpr,
  SemanticViewExpr,
  CredentialsPropertyExpr,
  BoolnotExpr,
  JsonExtractArrayExpr,
  InExpr,
  QueryExpr,
  NeqExpr,
  AllExpr,
  UsingTemplatePropertyExpr,
  MakeIntervalExpr,
  MaxExpr,
  MinExpr,
  ArrayConcatAggExpr,
  YearOfWeekExpr,
  YearOfWeekIsoExpr,
  TsOrDsDiffExpr,
  ToCharExpr,
  TsOrDsAddExpr,
  ToArrayExpr,
  TimeToUnixExpr,
  TimeStrToTimeExpr,
  TimestampDiffExpr,
  TimestampAddExpr,
  TimestampExpr,
  UuidExpr,
  VarMapExpr,
  PropertiesLocation,
  VolatilePropertyExpr,
  SetPropertyExpr,
  MaterializedPropertyExpr,
  CopyGrantsPropertyExpr,
  CreateExprKind,
  OrderExpr,
  WithinGroupExpr,
  AliasExpr,
  ConvertTimezoneExpr,
} from '../expressions';
import {
  Generator, unsupportedArgs,
} from '../generator';
import {
  findNewName,
  isDateUnit,
  isInt, seqGet,
} from '../helper';
import { JsonPathTokenizer } from '../jsonpath';
import {
  annotateTypes,
  buildScope, findAllInScope,
} from '../optimizer';
import {
  Parser, buildVarMap,
} from '../parser';
import {
  cache, narrowInstanceOf,
} from '../port_internals';
import type { TokenPair } from '../tokens';
import {
  Tokenizer, TokenType,
} from '../tokens';
import {
  addWithinGroupForPercentiles,
  eliminateDistinctOn,
  eliminateSemiAndAntiJoins,
  eliminateWindowClause,
  explodeProjectionToUnnest,
  inheritStructFieldNames,
  preprocess,
  unqualifyColumns,
} from '../transforms';
import {
  arrayConcatSql,
  arrayAppendSql,
  binaryFromFunction,
  buildFormattedTime,
  buildLike,
  buildReplaceWithOptionalReplacement,
  buildTimeToStrOrToChar,
  buildTrunc,
  dateDeltaSql,
  dateStrToDateSql,
  dateTruncToTime,
  Dialect, Dialects,
  groupConcatSql,
  ifSql,
  mapDatePart,
  NormalizationStrategy,
  renameFunc,
  strPositionSql,
  timeStrToTimeSql,
  timestampDiffSql,
  timestampTruncSql,
  varMapSql,
  buildDefaultDecimalType,
  noMakeIntervalSql,
  maxOrGreatest,
  minOrLeast,
  noTimestampSql,
  unitToStr,
  inlineArraySql,
} from './dialect';

const TIMESTAMP_TYPES: Partial<Record<DataTypeExprKind, string>> = {
  [DataTypeExprKind.TIMESTAMP]: 'TO_TIMESTAMP',
  [DataTypeExprKind.TIMESTAMPLTZ]: 'TO_TIMESTAMP_LTZ',
  [DataTypeExprKind.TIMESTAMPNTZ]: 'TO_TIMESTAMP_NTZ',
  [DataTypeExprKind.TIMESTAMPTZ]: 'TO_TIMESTAMP_TZ',
};

function buildStrtok (args: Expression[]): SplitPartExpr {
  // Add default delimiter (space) if missing - per Snowflake docs
  if (args.length === 1) {
    args.push(LiteralExpr.string(' '));
  }

  // Add default part_index (1) if missing
  if (args.length === 2) {
    args.push(LiteralExpr.number(1));
  }

  return SplitPartExpr.fromArgList(args);
}

/**
 * Normalizes APPROX_TOP_K arguments to match Snowflake semantics.
 *
 * Snowflake APPROX_TOP_K signature: APPROX_TOP_K(column [, k] [, counters])
 * - k defaults to 1 if omitted (per Snowflake documentation)
 * - counters is optional precision parameter
 */
function buildApproxTopK (args: Expression[]): ApproxTopKExpr {
  // Add default k=1 if only column is provided
  if (args.length === 1) {
    args.push(LiteralExpr.number(1));
  }

  return ApproxTopKExpr.fromArgList(args);
}

function buildDateFromParts (args: Expression[]): DateFromPartsExpr {
  return new DateFromPartsExpr({
    year: seqGet(args, 0),
    month: seqGet(args, 1),
    day: seqGet(args, 2),
    allowOverflow: true,
  });
}

function buildDatetime (
  name: string,
  kind: DataTypeExprKind,
  options: { safe?: boolean } = {},
) {
  const { safe = false } = options;

  return (args: Expression[]): Expression => {
    const value = seqGet(args, 0);
    const scaleOrFmt = seqGet(args, 1);

    const intValue = value instanceof IdentifierExpr && isInt(value.name);
    const intScaleOrFmt = scaleOrFmt instanceof LiteralExpr && scaleOrFmt.isNumber;

    if (value instanceof LiteralExpr || value instanceof NegExpr || (value && scaleOrFmt)) {
      // Converts calls like `TO_TIME('01:02:03')` into casts
      if (args.length === 1 && value instanceof LiteralExpr && value.isString && !intValue) {
        return safe
          ? new TryCastExpr({
            this: value,
            to: DataTypeExpr.build(kind),
            requiresString: true,
          })
          : new CastExpr({
            this: value,
            to: DataTypeExpr.build(kind),
          });
      }

      // Handles `TO_TIMESTAMP(str, fmt)` and `TO_TIMESTAMP(num, scale)` as special
      // cases so we can transpile them, since they're relatively common
      if (kind in TIMESTAMP_TYPES) {
        if (!safe && (intScaleOrFmt || (intValue && scaleOrFmt === undefined))) {
          const unixExpr = new UnixToTimeExpr({
            this: value,
            scale: scaleOrFmt,
          });
          unixExpr.setArgKey('targetType', DataTypeExpr.build(kind, { dialect: 'snowflake' }));
          return unixExpr;
        }
        if (scaleOrFmt && !intScaleOrFmt) {
          // Format string provided (e.g., 'YYYY-MM-DD'), use StrToTime
          const strtotimeExpr = buildFormattedTime(StrToTimeExpr, { dialect: 'snowflake' })(args);
          strtotimeExpr.setArgKey('safe', safe);
          strtotimeExpr.setArgKey('targetType', DataTypeExpr.build(kind, { dialect: 'snowflake' }));
          return strtotimeExpr;
        }
      }
    }

    // Handle DATE/TIME with format strings - allow intValue if a format string is provided
    const hasFormatString = scaleOrFmt && !intScaleOrFmt;
    if (
      (kind === DataTypeExprKind.DATE || kind === DataTypeExprKind.TIME)
      && (!intValue || hasFormatString)
    ) {
      const Klass = kind === DataTypeExprKind.DATE ? TsOrDsToDateExpr : TsOrDsToTimeExpr;
      const formattedExp = buildFormattedTime(Klass, { dialect: 'snowflake' })(args);
      formattedExp.setArgKey('safe', safe);
      return formattedExp;
    }

    return new AnonymousExpr({
      this: name,
      expressions: args,
    });
  };
}

function buildObjectConstruct (args: Expression[]): StarMapExpr | StructExpr {
  const expression = buildVarMap(args);

  if (expression instanceof StarMapExpr) {
    return expression;
  }

  return new StructExpr({
    expressions: [...expression.args.keys ?? []].map(
      (k, i) => new PropertyEqExpr({
        this: k as ExpressionValue,
        expression: seqGet(expression.args.values ?? [], i) as ExpressionValue,
      }),
    ),
  });
}

function buildDatediff (args: Expression[]): DateDiffExpr {
  return new DateDiffExpr({
    this: seqGet(args, 2),
    expression: seqGet(args, 1),
    unit: mapDatePart(seqGet(args, 0)),
    datePartBoundary: true,
  });
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function buildDateTimeAdd<T extends Expression> (ExprClass: new (args: any) => T) {
  return (args: Expression[]): T => {
    return new ExprClass({
      this: seqGet(args, 2),
      expression: seqGet(args, 1),
      unit: mapDatePart(seqGet(args, 0)),
    });
  };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function buildBitwise<T extends Expression> (ExprClass: new (args: any) => T, name: string) {
  return (args: Expression[]): T | AnonymousExpr => {
    if (args.length === 3) {
      // Special handling for bitwise operations with padside argument
      if (
        ExprClass === BitwiseAndExpr as unknown
        || ExprClass === BitwiseOrExpr as unknown
        || ExprClass === BitwiseXorExpr as unknown
      ) {
        return new ExprClass({
          this: seqGet(args, 0),
          expression: seqGet(args, 1),
          padside: seqGet(args, 2),
        }) as T;
      }
      return new AnonymousExpr({
        this: name,
        expressions: args,
      });
    }

    const result = binaryFromFunction(ExprClass)(args);

    // Snowflake specifies INT128 for bitwise shifts
    if (ExprClass === BitwiseLeftShiftExpr as unknown || ExprClass === BitwiseRightShiftExpr as unknown) {
      result.setArgKey('requiresInt128', true);
    }

    return result as T;
  };
}

// https://docs.snowflake.com/en/sql-reference/functions/div0
function buildIfFromDiv0 (args: Expression[]): IfExpr {
  const lhs = wrap(seqGet(args, 0), BinaryExpr);
  const rhs = wrap(seqGet(args, 1), BinaryExpr);

  const cond = new EqExpr({
    this: rhs,
    expression: LiteralExpr.number(0),
  }).and(
    new IsExpr({
      this: lhs,
      expression: null_(),
    }).not(),
  );
  const trueNode = LiteralExpr.number(0);
  const falseNode = new DivExpr({
    this: lhs,
    expression: rhs,
  });
  return new IfExpr({
    this: cond,
    true: trueNode,
    false: falseNode,
  });
}

// https://docs.snowflake.com/en/sql-reference/functions/div0null
function buildIfFromDiv0null (args: Expression[]): IfExpr {
  const lhs = wrap(seqGet(args, 0), BinaryExpr);
  const rhs = wrap(seqGet(args, 1), BinaryExpr);

  // Returns 0 when divisor is 0 OR NULL
  const cond = new EqExpr({
    this: rhs,
    expression: LiteralExpr.number(0),
  }).or(
    new IsExpr({
      this: rhs,
      expression: null_(),
    }),
  );
  const trueNode = LiteralExpr.number(0);
  const falseNode = new DivExpr({
    this: lhs,
    expression: rhs,
  });
  return new IfExpr({
    this: cond,
    true: trueNode,
    false: falseNode,
  });
}

// https://docs.snowflake.com/en/sql-reference/functions/zeroifnull
function buildIfFromZeroifnull (args: Expression[]): IfExpr {
  const cond = new IsExpr({
    this: seqGet(args, 0),
    expression: null_(),
  });
  return new IfExpr({
    this: cond,
    true: LiteralExpr.number(0),
    false: seqGet(args, 0),
  });
}

function buildSearch (args: Expression[]): SearchExpr {
  const kwargs: SearchExprArgs = {
    this: seqGet(args, 0),
    expression: seqGet(args, 1),
  };

  args.slice(2).forEach((arg) => {
    if (arg instanceof KwargExpr) {
      (kwargs as Record<string, unknown>)[arg.name] = arg;
    }
  });

  return new SearchExpr(kwargs);
}

// https://docs.snowflake.com/en/sql-reference/functions/nullifzero
function buildIfFromNullifzero (args: Expression[]): IfExpr {
  const cond = new EqExpr({
    this: seqGet(args, 0),
    expression: LiteralExpr.number(0),
  });
  return new IfExpr({
    this: cond,
    true: null_(),
    false: seqGet(args, 0),
  });
}

function regexpILikeSql (this: Generator, expression: RegexpILikeExpr): string {
  let flag = expression.text('flag');

  if (!flag.includes('i')) {
    flag += 'i';
  }

  return this.func(
    'REGEXP_LIKE',
    [
      expression.args.this,
      expression.args.expression,
      LiteralExpr.string(flag),
    ],
  );
}

function buildRegexpReplace (args: Expression[]): RegexpReplaceExpr {
  const regexpReplace = RegexpReplaceExpr.fromArgList(args);

  if (!regexpReplace.args.replacement) {
    regexpReplace.setArgKey('replacement', LiteralExpr.string(''));
  }

  return regexpReplace;
}

function showParser (str: string) {
  return function (this: Parser): ShowExpr {
    return (this as SnowflakeParser).parseShowSnowflake(str);
  };
}

function dateTruncToTimeWrapper (args: Expression[]): DateTruncExpr | TimestampTruncExpr {
  const trunc = dateTruncToTime(args);
  const unit = mapDatePart(trunc.args.unit);
  trunc.setArgKey('unit', unit);

  const isTimeInput = trunc.args.this?.isType([DataTypeExprKind.TIME, DataTypeExprKind.TIMETZ]);

  const needsTypePreservation =
    (trunc instanceof TimestampTruncExpr && (isDateUnit(unit) || isTimeInput))
    || (trunc instanceof DateTruncExpr && !isDateUnit(unit));

  if (needsTypePreservation) {
    trunc.setArgKey('inputTypePreserved', true);
  }
  return trunc;
}

/**
 * Snowflake doesn't allow columns referenced in UNPIVOT to be qualified,
 * so we need to unqualify them. Same goes for ANY ORDER BY <column>.
 */
function unqualifyPivotColumns (expression: Expression): Expression {
  if (expression instanceof PivotExpr) {
    if (expression.args.unpivot) {
      return unqualifyColumns(expression);
    } else {
      expression.args.fields?.forEach((field) => {
        const fieldExpr = seqGet(field?.args.expressions || [], 0);

        if (fieldExpr instanceof PivotAnyExpr) {
          const unqualifiedFieldExpr = unqualifyColumns(fieldExpr);
          field?.setArgKey('expressions', [unqualifiedFieldExpr], 0);
        }
      });
    }
  }

  return expression;
}

function flattenStructuredTypesUnlessIceberg (expression: Expression): Expression {
  if (!(expression instanceof CreateExpr)) return expression;

  const flattenStructuredType = (node: Expression): Expression => {
    if (node instanceof DataTypeExpr && DataTypeExpr.NESTED_TYPES.has(node.args.this?.toString() as DataTypeExprKind)) {
      node.setArgKey('expressions', undefined);
    }
    return node;
  };

  const props = expression.args.properties;
  const isIceberg = props?.find(IcebergPropertyExpr);

  if (expression.args.this instanceof SchemaExpr && !isIceberg) {
    for (const schemaExpression of expression.args.this.args.expressions || []) {
      if (schemaExpression instanceof ColumnDefExpr && schemaExpression.args.kind instanceof DataTypeExpr) {
        schemaExpression.args.kind.transform(flattenStructuredType, { copy: false });
      }
    }
  }

  return expression;
}

function unnestGenerateDateArray (unnest: UnnestExpr): void {
  const generateDateArray = unnest.args.expressions?.[0] as GenerateDateArrayExpr;
  const {
    start, end, step,
  } = generateDateArray.args;

  if (!start || !end || !(step instanceof IntervalExpr) || step.name !== '1') {
    return;
  }

  const unit = step.args.unit;
  if (!unit) return;
  const unnestAlias = unnest.args.alias;
  const sequenceValueName = narrowInstanceOf(unnestAlias, TableAliasExpr)?.args.columns?.[0] || 'value';

  const dateAdd = buildDateTimeAdd(DateAddExpr)([
    unit,
    new CastExpr({
      this: sequenceValueName,
      to: DataTypeExpr.build(DataTypeExprKind.INT),
    }),
    new CastExpr({
      this: start,
      to: DataTypeExpr.build(DataTypeExprKind.DATE),
    }),
  ]);

  const numberSequence = (SnowflakeParser.FUNCTIONS['ARRAY_GENERATE_RANGE'] as (args: Expression[]) => Expression)([
    LiteralExpr.number(0),
    buildDatediff([
      unit,
      start,
      end,
    ]).add(1),
  ]);

  unnest.setArgKey('expressions', [numberSequence]);

  const unnestParent = unnest.parent;
  if (unnestParent instanceof JoinExpr) {
    const select = unnestParent.parent;
    if (select instanceof SelectExpr) {
      const replaceColumnName = sequenceValueName instanceof Expression ? sequenceValueName.name : sequenceValueName.toString();
      const scope = buildScope(select);
      if (scope) {
        for (const column of scope.columns) {
          if (column.name.toLowerCase() === replaceColumnName.toLowerCase()) {
            column.replace(
              column.parent instanceof SelectExpr ? dateAdd.as(replaceColumnName) : dateAdd,
            );
          }
        }
      }

      const lateral = new LateralExpr({ this: unnestParent.args.this?.pop() });
      unnestParent.replace(new JoinExpr({ this: lateral }));
    }
  } else {
    unnest.replace(
      select(dateAdd.as(sequenceValueName as string | IdentifierExpr))
        .from(unnest.copy())
        .subquery(unnestAlias),
    );
  }
}

function transformGenerateDateArray (expression: Expression): Expression {
  if (expression instanceof SelectExpr) {
    for (const generateDateArray of expression.findAll(GenerateDateArrayExpr)) {
      const parent = generateDateArray.parent;

      if (!(parent instanceof UnnestExpr)) {
        const unnest = new UnnestExpr({ expressions: [generateDateArray.copy()] });
        generateDateArray.replace(
          select(new ArrayAggExpr({ this: new StarExpr({}) })).from(unnest)
            .subquery(),
        );
      }

      if (
        parent instanceof UnnestExpr
        && (parent.parent instanceof FromExpr || parent.parent instanceof JoinExpr)
        && parent.args.expressions?.length === 1
      ) {
        unnestGenerateDateArray(parent);
      }
    }
  }

  return expression;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function buildRegexpExtract<T extends Expression> (ExprClass: new (args: any) => T) {
  return (args: Expression[], { dialect }: { dialect: Dialect }): T => {
    return new ExprClass({
      this: seqGet(args, 0),
      expression: seqGet(args, 1),
      position: seqGet(args, 2),
      occurrence: seqGet(args, 3),
      parameters: seqGet(args, 4),
      group: seqGet(args, 5) || LiteralExpr.number(0),
      ...(ExprClass === RegexpExtractExpr as unknown
        ? { nullIfPosOverflow: dialect._constructor.REGEXP_EXTRACT_POSITION_OVERFLOW_RETURNS_NULL }
        : {}),
    });
  };
}

function regexpExtractSql (this: Generator, expression: RegexpExtractExpr | RegexpExtractAllExpr): string {
  let group = expression.args.group;

  if (group instanceof IdentifierExpr && group.name === '0') {
    group = undefined;
  }

  const parameters = expression.args.parameters || (group ? LiteralExpr.string('c') : undefined);
  const occurrence = expression.args.occurrence || (parameters ? LiteralExpr.number(1) : undefined);
  const position = expression.args.position || (occurrence ? LiteralExpr.number(1) : undefined);

  return this.func(
    expression instanceof RegexpExtractExpr ? 'REGEXP_SUBSTR' : 'REGEXP_EXTRACT_ALL',
    [
      expression.args.this,
      expression.args.expression,
      position,
      occurrence,
      parameters,
      group,
    ],
  );
}

function jsonExtractValueArraySql (
  this: Generator,
  expression: JsonValueArrayExpr | JsonExtractArrayExpr,
): string {
  const jsonExtract = new JsonExtractExpr({
    this: expression.args.this,
    expression: expression.args.expression,
  });
  const ident = toIdentifier('x');

  const thisNode: Expression =
    expression instanceof JsonValueArrayExpr
      ? new CastExpr({
        this: ident,
        to: DataTypeExpr.build(DataTypeExprKind.VARCHAR),
      })
      : new ParseJsonExpr({ this: `TO_JSON(${ident})` });

  const transformLambda = new LambdaExpr({
    expressions: [ident],
    this: thisNode,
  });

  return this.func('TRANSFORM', [jsonExtract, transformLambda]);
}

function qualifyUnnestedColumns (expression: Expression): Expression {
  if (!(expression instanceof SelectExpr)) return expression;

  const scope = buildScope(expression);
  if (!scope) return expression;

  const unnests = Array.from(scope.findAll(UnnestExpr));
  if (unnests.length === 0) return expression;

  const takenSourceNames = new Set(scope.sources.keys());
  const columnSource: Record<string, ExpressionValue<IdentifierExpr>> = {};
  const unnestToIdentifier = new Map<Expression, IdentifierExpr>();

  let unnestIdentifier: ExpressionValue<IdentifierExpr> | undefined;
  const origExpression = expression.copy();

  for (const unnest of unnests) {
    if (!(unnest.parent instanceof FromExpr || unnest.parent instanceof JoinExpr)) continue;

    const unnestColumns = new Set<string>();
    for (const unnestExpr of unnest.args.expressions || []) {
      if (!(unnestExpr instanceof ArrayExpr)) continue;

      for (const arrayExpr of unnestExpr.args.expressions || []) {
        if (
          arrayExpr instanceof StructExpr
          && 0 < (arrayExpr.args.expressions || []).length
          && arrayExpr.args.expressions?.every((e) => e instanceof PropertyEqExpr)
        ) {
          arrayExpr.args.expressions.forEach((structExpr) => {
            unnestColumns.add(
              structExpr.args.this instanceof Expression
                ? structExpr.args.this.name.toLowerCase()
                : structExpr.args.this?.toString().toLowerCase() ?? '',
            );
          });
          break;
        }
      }
      if (0 < unnestColumns.size) break;
    }

    const unnestAlias = unnest.args.alias;
    if (!unnestAlias) {
      const aliasName = findNewName(takenSourceNames, 'value');
      takenSourceNames.add(aliasName);

      const aliasedUnnest = alias(unnest, undefined, { table: [aliasName] });
      scope.replace(unnest, aliasedUnnest);
      unnestIdentifier = narrowInstanceOf(aliasedUnnest.args.alias, TableAliasExpr)?.args.columns?.[0];
    } else {
      const narrowedUnnestAlias = narrowInstanceOf(unnestAlias, TableAliasExpr);
      const aliasColumns = narrowedUnnestAlias?.args.columns || [];
      unnestIdentifier = narrowedUnnestAlias?.args.this || seqGet(aliasColumns, 0);
    }

    if (!(unnestIdentifier instanceof IdentifierExpr)) return origExpression;

    unnestToIdentifier.set(unnest, unnestIdentifier);
    unnestColumns.forEach((c) => {
      if (unnestIdentifier) columnSource[c.toLowerCase()] = unnestIdentifier;
    });
  }

  for (const column of scope.columns) {
    if (column.args.table) continue;
    const unnestIdentifierName = unnestIdentifier instanceof Expression ? unnestIdentifier.name : unnestIdentifier?.toString();

    let table = columnSource[column.name.toLowerCase()];
    if (
      unnestIdentifier
      && !table
      && scope.sources.size === 1
      && column.name.toLowerCase() !== unnestIdentifierName?.toLowerCase()
    ) {
      const unnestAncestor = column.findAncestor<UnnestExpr | SelectExpr>(UnnestExpr, SelectExpr);
      const ancestorIdentifier = unnestAncestor ? unnestToIdentifier.get(unnestAncestor) : undefined;
      if (
        unnestAncestor instanceof UnnestExpr
        && ancestorIdentifier
        && ancestorIdentifier.name.toLowerCase() === unnestIdentifierName?.toLowerCase()
      ) {
        continue;
      }
      table = unnestIdentifier;
    }

    if (table) column.setArgKey('table', table instanceof Expression ? table.copy() : table);
  }

  return expression;
}

/**
 * This transformation is used to facilitate transpilation of BigQuery `UNNEST` operations
 * to Snowflake. It should not affect roundtrip because `Unnest` nodes cannot be produced
 * by Snowflake's parser.
 */
function eliminateDotVariantLookup (expression: Expression): Expression {
  if (expression instanceof SelectExpr) {
    const unnestAliases = new Set<string>();

    for (const unnest of findAllInScope(expression, [UnnestExpr])) {
      const unnestAlias = unnest.args.alias;
      if (
        unnestAlias instanceof TableAliasExpr
        && !unnestAlias.args.this
        && unnestAlias.args.columns?.length === 1
      ) {
        unnestAliases.add(unnestAlias.args.columns[0] instanceof Expression ? unnestAlias.args.columns[0].name : unnestAlias.args.columns[0].toString());
      }
    }

    if (0 < unnestAliases.size) {
      for (const c of findAllInScope(expression, [ColumnExpr])) {
        if (c.args.table instanceof Expression && unnestAliases.has(c.args.table.name)) {
          const bracketLhs = c.args.table;
          const bracketRhs = LiteralExpr.string(c.name);
          const bracket = new BracketExpr({
            this: bracketLhs,
            expressions: [bracketRhs],
          });

          if (c.parent === expression) {
            // Retain column projection names by using aliases
            c.replace(alias(bracket, c.args.this instanceof Expression ? c.args.this.copy() as IdentifierExpr : c.args.this?.toString()));
          } else {
            c.replace(bracket);
          }
        }
      }
    }
  }

  return expression;
}

/**
 * Build TimestampFromParts with support for both syntaxes:
 * 1. TIMESTAMP_FROM_PARTS(year, month, day, hour, minute, second [, nanosecond] [, time_zone])
 * 2. TIMESTAMP_FROM_PARTS(date_expr, time_expr) - Snowflake specific
 */
function buildTimestampFromParts (args: Expression[]): FuncExpr {
  if (args.length === 2) {
    return new TimestampFromPartsExpr({
      this: seqGet(args, 0),
      expression: seqGet(args, 1),
    });
  }

  return TimestampFromPartsExpr.fromArgList(args);
}

/**
 * Build Round expression, unwrapping Snowflake's named parameters.
 * Maps EXPR => this, SCALE => decimals, ROUNDING_MODE => truncate.
 */
function buildRound (args: Expression[]): RoundExpr {
  const kwargMap: Record<string, string> = {
    EXPR: 'this',
    SCALE: 'decimals',
    ROUNDING_MODE: 'truncate',
  };
  const roundArgs: Record<string, unknown> = {};
  const positionalKeys = [
    'this',
    'decimals',
    'truncate',
  ];
  let positionalIdx = 0;

  for (const arg of args) {
    if (arg instanceof KwargExpr) {
      const key = arg.args.this?.name.toUpperCase();
      const roundKey = key !== undefined ? kwargMap[key] : undefined;
      if (roundKey) {
        roundArgs[roundKey] = arg.args.expression;
      }
    } else {
      if (positionalIdx < positionalKeys.length) {
        roundArgs[positionalKeys[positionalIdx]] = arg;
        positionalIdx++;
      }
    }
  }

  const expression = new RoundExpr(roundArgs);
  expression.setArgKey('castsNonIntegerDecimals', true);
  return expression;
}

/**
 * Build Generator expression, unwrapping Snowflake's named parameters.
 * Maps ROWCOUNT => rowcount, TIMELIMIT => time_limit.
 */
function buildGenerator (args: Expression[]): GeneratorExpr {
  const kwargMap: Record<string, string> = {
    ROWCOUNT: 'rowcount',
    TIMELIMIT: 'timeLimit',
  };
  const genArgs: Record<string, unknown> = {};

  for (const arg of args) {
    if (arg instanceof KwargExpr) {
      const key = arg.args.this?.name.toUpperCase();
      const genKey = key !== undefined ? kwargMap[key] : undefined;
      if (genKey) {
        genArgs[genKey] = arg.args.expression;
      }
    }
  }

  return new GeneratorExpr(genArgs);
}

function buildTryToNumber (args: Expression[]): ToNumberExpr {
  return new ToNumberExpr({
    this: seqGet(args, 0),
    format: seqGet(args, 1),
    precision: seqGet(args, 2),
    scale: seqGet(args, 3),
    safe: true,
  });
}

class SnowflakeJsonPathTokenizer extends JsonPathTokenizer {
  @cache
  static get SINGLE_TOKENS (): Record<string, TokenType> {
    const tokens: Record<string, TokenType> = { ...JsonPathTokenizer.SINGLE_TOKENS };
    delete tokens['$'];
    return tokens;
  }
}

class SnowflakeTokenizer extends Tokenizer {
  static STRING_ESCAPES = ['\\', '\''];

  static HEX_STRINGS: [string, string][] = [['x\'', '\''], ['X\'', '\'']];

  static RAW_STRINGS = ['$$'];

  static COMMENTS: TokenPair[] = [
    '--',
    '//',
    ['/*', '*/'],
  ];

  static NESTED_COMMENTS = false;

  @cache
  static get ORIGINAL_KEYWORDS (): Record<string, TokenType> {
    const keywords: Record<string, TokenType> = {
      ...Tokenizer.KEYWORDS,
      'BYTEINT': TokenType.INT,
      'FILE://': TokenType.URI_START,
      'FILE FORMAT': TokenType.FILE_FORMAT,
      'GET': TokenType.GET,
      'MATCH_CONDITION': TokenType.MATCH_CONDITION,
      'MATCH_RECOGNIZE': TokenType.MATCH_RECOGNIZE,
      'MINUS': TokenType.EXCEPT,
      'NCHAR VARYING': TokenType.VARCHAR,
      'PUT': TokenType.PUT,
      'REMOVE': TokenType.COMMAND,
      'RM': TokenType.COMMAND,
      'SAMPLE': TokenType.TABLE_SAMPLE,
      'SEMANTIC VIEW': TokenType.SEMANTIC_VIEW,
      'SQL_DOUBLE': TokenType.DOUBLE,
      'SQL_VARCHAR': TokenType.VARCHAR,
      'STAGE': TokenType.STAGE,
      'STORAGE INTEGRATION': TokenType.STORAGE_INTEGRATION,
      'STREAMLIT': TokenType.STREAMLIT,
      'TAG': TokenType.TAG,
      'TIMESTAMP_TZ': TokenType.TIMESTAMPTZ,
      'TOP': TokenType.TOP,
      'WAREHOUSE': TokenType.WAREHOUSE,
      'FLOAT': TokenType.DOUBLE,
    };
    delete keywords['/*+'];
    return keywords;
  }

  @cache
  static get SINGLE_TOKENS (): Record<string, TokenType> {
    return {
      ...Tokenizer.SINGLE_TOKENS,
      '$': TokenType.PARAMETER,
      '!': TokenType.EXCLAMATION,
    };
  }

  static VAR_SINGLE_TOKENS = new Set(['$']);

  @cache
  static get COMMANDS (): Set<TokenType> {
    return new Set(
      Array.from(Tokenizer.COMMANDS).filter((t) => t !== TokenType.SHOW),
    );
  }
}

class SnowflakeParser extends Parser {
  static IDENTIFY_PIVOT_STRINGS = true;
  static DEFAULT_SAMPLING_METHOD = 'BERNOULLI' as const;
  static COLON_IS_VARIANT_EXTRACT = true;
  static JSON_EXTRACT_REQUIRES_JSON_EXPRESSION = true;

  @cache
  static get ID_VAR_TOKENS (): Set<TokenType> {
    return new Set([
      ...Parser.ID_VAR_TOKENS,
      TokenType.EXCEPT,
      TokenType.MATCH_CONDITION,
    ]);
  }

  @cache
  static get TABLE_ALIAS_TOKENS (): Set<TokenType> {
    return (() => {
      const tokens = new Set([...Parser.TABLE_ALIAS_TOKENS, TokenType.WINDOW]);
      tokens.delete(TokenType.MATCH_CONDITION);
      return tokens;
    })();
  }

  @cache
  static get COLON_PLACEHOLDER_TOKENS (): Set<TokenType> {
    return new Set([...SnowflakeParser.ID_VAR_TOKENS, TokenType.NUMBER]);
  }

  @cache
  static get NO_PAREN_FUNCTIONS (): Partial<Record<TokenType, typeof Expression>> {
    return {
      ...Parser.NO_PAREN_FUNCTIONS,
      [TokenType.CURRENT_TIME]: LocaltimeExpr,
    };
  }

  @cache
  static get FUNCTIONS (): Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression> {
    return (() => {
      const functions: Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression> = {
        ...Parser.FUNCTIONS,
        ADD_MONTHS: (args: Expression[]) =>
          new AddMonthsExpr({
            this: seqGet(args, 0),
            expression: seqGet(args, 1),
            preserveEndOfMonth: true,
          }),
        APPROX_PERCENTILE: ApproxQuantileExpr.fromArgList,
        CURRENT_TIME: (args: Expression[]) => new LocaltimeExpr({ this: seqGet(args, 0) }),
        APPROX_TOP_K: buildApproxTopK,
        ARRAY_CONSTRUCT: (args: Expression[]) => new ArrayExpr({ expressions: args }),
        ARRAY_CONTAINS: (args: Expression[]) =>
          new ArrayContainsExpr({
            this: seqGet(args, 1),
            expression: seqGet(args, 0),
            ensureVariant: false,
          }),
        ARRAY_GENERATE_RANGE: (args: Expression[]) =>
          new GenerateSeriesExpr({
            start: seqGet(args, 0),
            end: new SubExpr({
              this: seqGet(args, 1),
              expression: LiteralExpr.number(1),
            }),
            step: seqGet(args, 2),
          }),
        ARRAY_SORT: SortArrayExpr.fromArgList,
        ARRAY_FLATTEN: FlattenExpr.fromArgList,
        BITAND: buildBitwise(BitwiseAndExpr, 'BITAND'),
        BIT_AND: buildBitwise(BitwiseAndExpr, 'BITAND'),
        BITNOT: (args: Expression[]) => new BitwiseNotExpr({ this: seqGet(args, 0) }),
        BIT_NOT: (args: Expression[]) => new BitwiseNotExpr({ this: seqGet(args, 0) }),
        BITXOR: buildBitwise(BitwiseXorExpr, 'BITXOR'),
        BIT_XOR: buildBitwise(BitwiseXorExpr, 'BITXOR'),
        BITOR: buildBitwise(BitwiseOrExpr, 'BITOR'),
        BIT_OR: buildBitwise(BitwiseOrExpr, 'BITOR'),
        BITSHIFTLEFT: buildBitwise(BitwiseLeftShiftExpr, 'BITSHIFTLEFT'),
        BIT_SHIFTLEFT: buildBitwise(BitwiseLeftShiftExpr, 'BIT_SHIFTLEFT'),
        BITSHIFTRIGHT: buildBitwise(BitwiseRightShiftExpr, 'BITSHIFTRIGHT'),
        BIT_SHIFTRIGHT: buildBitwise(BitwiseRightShiftExpr, 'BITSHIFTRIGHT'),
        BITANDAGG: BitwiseAndAggExpr.fromArgList,
        BITAND_AGG: BitwiseAndAggExpr.fromArgList,
        BIT_AND_AGG: BitwiseAndAggExpr.fromArgList,
        BIT_ANDAGG: BitwiseAndAggExpr.fromArgList,
        BITORAGG: BitwiseOrAggExpr.fromArgList,
        BITOR_AGG: BitwiseOrAggExpr.fromArgList,
        BIT_OR_AGG: BitwiseOrAggExpr.fromArgList,
        BIT_ORAGG: BitwiseOrAggExpr.fromArgList,
        BITXORAGG: BitwiseXorAggExpr.fromArgList,
        BITXOR_AGG: BitwiseXorAggExpr.fromArgList,
        BIT_XOR_AGG: BitwiseXorAggExpr.fromArgList,
        BIT_XORAGG: BitwiseXorAggExpr.fromArgList,
        BITMAP_OR_AGG: BitmapOrAggExpr.fromArgList,
        BOOLAND: (args: Expression[]) =>
          new BoolandExpr({
            this: seqGet(args, 0),
            expression: seqGet(args, 1),
            roundInput: true,
          }),
        BOOLOR: (args: Expression[]) =>
          new BoolorExpr({
            this: seqGet(args, 0),
            expression: seqGet(args, 1),
            roundInput: true,
          }),
        BOOLNOT: (args: Expression[]) => new BoolnotExpr({
          this: seqGet(args, 0),
          roundInput: true,
        }),
        BOOLXOR: (args: Expression[]) =>
          new XorExpr({
            this: seqGet(args, 0),
            expression: seqGet(args, 1),
            roundInput: true,
          }),
        CORR: (args: Expression[]) =>
          new CorrExpr({
            this: seqGet(args, 0),
            expression: seqGet(args, 1),
            nullOnZeroVariance: true,
          }),
        DATE: buildDatetime('DATE', DataTypeExprKind.DATE),
        DATEFROMPARTS: buildDateFromParts,
        DATE_FROM_PARTS: buildDateFromParts,
        DATE_TRUNC: dateTruncToTimeWrapper,
        DATEADD: buildDateTimeAdd(DateAddExpr),
        DATEDIFF: buildDatediff,
        DAYNAME: (args: Expression[]) => new DaynameExpr({
          this: seqGet(args, 0),
          abbreviated: true,
        }),
        DAYOFWEEKISO: DayOfWeekIsoExpr.fromArgList,
        DIV0: buildIfFromDiv0,
        DIV0NULL: buildIfFromDiv0null,
        EDITDISTANCE: (args: Expression[]) =>
          new LevenshteinExpr({
            this: seqGet(args, 0),
            expression: seqGet(args, 1),
            maxDist: seqGet(args, 2),
          }),
        FLATTEN: ExplodeExpr.fromArgList,
        GENERATOR: buildGenerator,
        GET: GetExtractExpr.fromArgList,
        GETDATE: CurrentTimestampExpr.fromArgList,
        GET_PATH: (args: Expression[], { dialect }: { dialect: Dialect }) =>
          new JsonExtractExpr({
            this: seqGet(args, 0),
            expression: dialect.toJsonPath(seqGet(args, 1)),
            requiresJson: true,
          }),
        GREATEST_IGNORE_NULLS: (args: Expression[]) =>
          new GreatestExpr({
            this: seqGet(args, 0),
            expressions: args.slice(1),
            ignoreNulls: true,
          }),
        LEAST_IGNORE_NULLS: (args: Expression[]) =>
          new LeastExpr({
            this: seqGet(args, 0),
            expressions: args.slice(1),
            ignoreNulls: true,
          }),
        HEX_DECODE_BINARY: UnhexExpr.fromArgList,
        IFF: IfExpr.fromArgList,
        MD5_HEX: Md5Expr.fromArgList,
        MD5_BINARY: Md5DigestExpr.fromArgList,
        MD5_NUMBER_LOWER64: Md5NumberLower64Expr.fromArgList,
        MD5_NUMBER_UPPER64: Md5NumberUpper64Expr.fromArgList,
        MONTHNAME: (args: Expression[]) => new MonthnameExpr({
          this: seqGet(args, 0),
          abbreviated: true,
        }),
        LAST_DAY: (args: Expression[]) =>
          new LastDayExpr({
            this: seqGet(args, 0),
            unit: mapDatePart(seqGet(args, 1)),
          }),
        LEN: (args: Expression[]) => new LengthExpr({
          this: seqGet(args, 0),
          binary: true,
        }),
        LENGTH: (args: Expression[]) => new LengthExpr({
          this: seqGet(args, 0),
          binary: true,
        }),
        LOCALTIMESTAMP: CurrentTimestampExpr.fromArgList,
        NULLIFZERO: buildIfFromNullifzero,
        OBJECT_CONSTRUCT: buildObjectConstruct,
        OBJECT_KEYS: JsonKeysExpr.fromArgList,
        OCTET_LENGTH: ByteLengthExpr.fromArgList,
        PARSE_URL: (args: Expression[]) =>
          new ParseUrlExpr({
            this: seqGet(args, 0),
            permissive: seqGet(args, 1),
          }),
        REGEXP_EXTRACT_ALL: buildRegexpExtract(RegexpExtractAllExpr),
        REGEXP_REPLACE: buildRegexpReplace,
        REGEXP_SUBSTR: buildRegexpExtract(RegexpExtractExpr),
        REGEXP_SUBSTR_ALL: buildRegexpExtract(RegexpExtractAllExpr),
        REPLACE: buildReplaceWithOptionalReplacement,
        RLIKE: RegexpLikeExpr.fromArgList,
        ROUND: buildRound,
        SHA1_BINARY: Sha1DigestExpr.fromArgList,
        SHA1_HEX: ShaExpr.fromArgList,
        SHA2_BINARY: Sha2DigestExpr.fromArgList,
        SHA2_HEX: Sha2Expr.fromArgList,
        SQUARE: (args: Expression[]) =>
          new PowExpr({
            this: seqGet(args, 0),
            expression: LiteralExpr.number(2),
          }),
        STDDEV_SAMP: StddevExpr.fromArgList,
        STRTOK: buildStrtok,
        SYSDATE: (args: Expression[]) =>
          new CurrentTimestampExpr({
            this: seqGet(args, 0),
            sysdate: true,
          }),
        TABLE: (args: Expression[]) => new TableFromRowsExpr({ this: seqGet(args, 0) }),
        TIME_ADD: buildDateTimeAdd(TimeAddExpr),
        TIMEDIFF: buildDatediff,
        TIME_FROM_PARTS: (args: Expression[]) =>
          new TimeFromPartsExpr({
            hour: seqGet(args, 0),
            min: seqGet(args, 1),
            sec: seqGet(args, 2),
            nano: seqGet(args, 3),
            overflow: true,
          }),
        TIMESTAMPADD: buildDateTimeAdd(DateAddExpr),
        TIMESTAMPDIFF: buildDatediff,
        TIMESTAMPFROMPARTS: buildTimestampFromParts,
        TIMESTAMP_FROM_PARTS: buildTimestampFromParts,
        TIMESTAMPNTZFROMPARTS: buildTimestampFromParts,
        TIMESTAMP_NTZ_FROM_PARTS: buildTimestampFromParts,
        TRUNC: (args: Expression[], { dialect }: { dialect: Dialect }) =>
          buildTrunc(args, {
            dialect,
            dateTruncRequiresPart: false,
          }),
        TRUNCATE: (args: Expression[], { dialect }: { dialect: Dialect }) =>
          buildTrunc(args, {
            dialect,
            dateTruncRequiresPart: false,
          }),
        TRY_DECRYPT: (args: Expression[]) =>
          new DecryptExpr({
            this: seqGet(args, 0),
            passphrase: seqGet(args, 1),
            aad: seqGet(args, 2),
            encryptionMethod: seqGet(args, 3),
            safe: true,
          }),
        TRY_DECRYPT_RAW: (args: Expression[]) =>
          new DecryptRawExpr({
            this: seqGet(args, 0),
            key: seqGet(args, 1),
            iv: seqGet(args, 2),
            aad: seqGet(args, 3),
            encryptionMethod: seqGet(args, 4),
            aead: seqGet(args, 5),
            safe: true,
          }),
        TRY_PARSE_JSON: (args: Expression[]) => new ParseJsonExpr({
          this: seqGet(args, 0),
          safe: true,
        }),
        TRY_TO_BINARY: (args: Expression[]) =>
          new ToBinaryExpr({
            this: seqGet(args, 0),
            format: seqGet(args, 1),
            safe: true,
          }),
        TRY_TO_BOOLEAN: (args: Expression[]) => new ToBooleanExpr({
          this: seqGet(args, 0),
          safe: true,
        }),
        TRY_TO_DATE: buildDatetime('TRY_TO_DATE', DataTypeExprKind.DATE, { safe: true }),
        TRY_TO_DECIMAL: buildTryToNumber,
        TRY_TO_NUMBER: buildTryToNumber,
        TRY_TO_NUMERIC: buildTryToNumber,
        TRY_TO_DOUBLE: (args: Expression[]) =>
          new ToDoubleExpr({
            this: seqGet(args, 0),
            format: seqGet(args, 1),
            safe: true,
          }),
        TRY_TO_FILE: (args: Expression[]) =>
          new ToFileExpr({
            this: seqGet(args, 0),
            path: seqGet(args, 1),
            safe: true,
          }),
        TRY_TO_TIME: buildDatetime('TRY_TO_TIME', DataTypeExprKind.TIME, { safe: true }),
        TRY_TO_TIMESTAMP: buildDatetime('TRY_TO_TIMESTAMP', DataTypeExprKind.TIMESTAMP, { safe: true }),
        TRY_TO_TIMESTAMP_LTZ: buildDatetime('TRY_TO_TIMESTAMP_LTZ', DataTypeExprKind.TIMESTAMPLTZ, { safe: true }),
        TRY_TO_TIMESTAMP_NTZ: buildDatetime('TRY_TO_TIMESTAMP_NTZ', DataTypeExprKind.TIMESTAMPNTZ, { safe: true }),
        TRY_TO_TIMESTAMP_TZ: buildDatetime('TRY_TO_TIMESTAMP_TZ', DataTypeExprKind.TIMESTAMPTZ, { safe: true }),
        TO_CHAR: buildTimeToStrOrToChar,
        TO_DATE: buildDatetime('TO_DATE', DataTypeExprKind.DATE),
        TO_DECIMAL: (args: Expression[]) =>
          new ToNumberExpr({
            this: seqGet(args, 0),
            format: seqGet(args, 1),
            precision: seqGet(args, 2),
            scale: seqGet(args, 3),
          }),
        TO_NUMBER: (args: Expression[]) =>
          new ToNumberExpr({
            this: seqGet(args, 0),
            format: seqGet(args, 1),
            precision: seqGet(args, 2),
            scale: seqGet(args, 3),
          }),
        TO_NUMERIC: (args: Expression[]) =>
          new ToNumberExpr({
            this: seqGet(args, 0),
            format: seqGet(args, 1),
            precision: seqGet(args, 2),
            scale: seqGet(args, 3),
          }),
        TO_TIME: buildDatetime('TO_TIME', DataTypeExprKind.TIME),
        TO_TIMESTAMP: buildDatetime('TO_TIMESTAMP', DataTypeExprKind.TIMESTAMP),
        TO_TIMESTAMP_LTZ: buildDatetime('TO_TIMESTAMP_LTZ', DataTypeExprKind.TIMESTAMPLTZ),
        TO_TIMESTAMP_NTZ: buildDatetime('TO_TIMESTAMP_NTZ', DataTypeExprKind.TIMESTAMPNTZ),
        TO_TIMESTAMP_TZ: buildDatetime('TO_TIMESTAMP_TZ', DataTypeExprKind.TIMESTAMPTZ),
        TO_VARCHAR: buildTimeToStrOrToChar,
        TO_JSON: JsonFormatExpr.fromArgList,
        VECTOR_COSINE_SIMILARITY: CosineDistanceExpr.fromArgList,
        VECTOR_INNER_PRODUCT: DotProductExpr.fromArgList,
        VECTOR_L1_DISTANCE: ManhattanDistanceExpr.fromArgList,
        VECTOR_L2_DISTANCE: EuclideanDistanceExpr.fromArgList,
        ZEROIFNULL: buildIfFromZeroifnull,
        LIKE: buildLike(LikeExpr),
        ILIKE: buildLike(ILikeExpr),
        SEARCH: buildSearch,
        SKEW: SkewnessExpr.fromArgList,
        SYSTIMESTAMP: CurrentTimestampExpr.fromArgList,
        WEEKISO: WeekOfYearExpr.fromArgList,
        WEEKOFYEAR: WeekExpr.fromArgList,
      };
      delete functions['PREDICT'];
      return functions;
    })();
  }

  @cache
  static get FUNCTION_PARSERS (): Partial<Record<string, (this: Parser) => Expression | undefined>> {
    return (() => {
      const parsers: Partial<Record<string, (this: Parser) => Expression | undefined>> = {
        ...Parser.FUNCTION_PARSERS,
        DATE_PART: function (this: Parser) {
          return (this as SnowflakeParser).parseDatePart();
        },
        DIRECTORY: function (this: Parser) {
          return (this as SnowflakeParser).parseDirectory();
        },
        OBJECT_CONSTRUCT_KEEP_NULL: function (this: Parser) {
          return (this as SnowflakeParser).parseJsonObject();
        },
        LISTAGG: function (this: Parser) {
          return this.parseStringAgg();
        },
        SEMANTIC_VIEW: function (this: Parser) {
          return (this as SnowflakeParser).parseSemanticView();
        },
      };
      delete parsers['TRIM'];
      return parsers;
    })();
  }

  @cache
  static get TIMESTAMPS (): Set<TokenType> {
    return new Set(
      Array.from(Parser.TIMESTAMPS).filter((t) => t !== TokenType.TIME),
    );
  }

  @cache
  static get ALTER_PARSERS (): Partial<Record<string, (this: Parser) => Expression | Expression[] | undefined>> {
    return {
      ...Parser.ALTER_PARSERS,
      SESSION: function (this: Parser) {
        return this.parseAlterSession();
      },
      UNSET: function (this: Parser) {
        return this.expression(SetExpr, {
          tag: this.matchTextSeq('TAG'),
          expressions: this.parseCsv(() => this.parseIdVar()),
          unset: true,
        });
      },
    };
  }

  @cache
  static get STATEMENT_PARSERS (): Partial<Record<TokenType, (this: Parser) => Expression | undefined>> {
    return {
      ...Parser.STATEMENT_PARSERS,
      [TokenType.GET]: function (this: Parser) {
        return (this as SnowflakeParser).parseGet();
      },
      [TokenType.PUT]: function (this: Parser) {
        return (this as SnowflakeParser).parsePut();
      },
      [TokenType.SHOW]: function (this: Parser) {
        return this.parseShow();
      },
    };
  }

  @cache
  static get PROPERTY_PARSERS (): Record<string, (this: Parser, ...args: unknown[]) => Expression | Expression[] | undefined> {
    return {
      ...Parser.PROPERTY_PARSERS,
      CREDENTIALS: function (this: Parser) {
        return (this as SnowflakeParser).parseCredentialsProperty();
      },
      FILE_FORMAT: function (this: Parser) {
        return (this as SnowflakeParser).parseFileFormatProperty();
      },
      LOCATION: function (this: Parser) {
        return (this as SnowflakeParser).parseLocationProperty();
      },
      TAG: function (this: Parser) {
        return (this as SnowflakeParser).parseTag();
      },
      USING: function (this: Parser) {
        return this.matchTextSeq('TEMPLATE')
          && this.expression(UsingTemplatePropertyExpr, {
            this: this.parseStatement(),
          });
      },
    };
  }

  static TYPE_CONVERTERS = {
    [DataTypeExprKind.DECIMAL]: buildDefaultDecimalType(38, 0),
  };

  static SHOW_PARSERS = {
    'DATABASES': showParser('DATABASES'),
    'TERSE DATABASES': showParser('DATABASES'),
    'SCHEMAS': showParser('SCHEMAS'),
    'TERSE SCHEMAS': showParser('SCHEMAS'),
    'OBJECTS': showParser('OBJECTS'),
    'TERSE OBJECTS': showParser('OBJECTS'),
    'TABLES': showParser('TABLES'),
    'TERSE TABLES': showParser('TABLES'),
    'VIEWS': showParser('VIEWS'),
    'TERSE VIEWS': showParser('VIEWS'),
    'PRIMARY KEYS': showParser('PRIMARY KEYS'),
    'TERSE PRIMARY KEYS': showParser('PRIMARY KEYS'),
    'IMPORTED KEYS': showParser('IMPORTED KEYS'),
    'TERSE IMPORTED KEYS': showParser('IMPORTED KEYS'),
    'UNIQUE KEYS': showParser('UNIQUE KEYS'),
    'TERSE UNIQUE KEYS': showParser('UNIQUE KEYS'),
    'SEQUENCES': showParser('SEQUENCES'),
    'TERSE SEQUENCES': showParser('SEQUENCES'),
    'STAGES': showParser('STAGES'),
    'COLUMNS': showParser('COLUMNS'),
    'USERS': showParser('USERS'),
    'TERSE USERS': showParser('USERS'),
    'FILE FORMATS': showParser('FILE FORMATS'),
    'FUNCTIONS': showParser('FUNCTIONS'),
    'PROCEDURES': showParser('PROCEDURES'),
    'WAREHOUSES': showParser('WAREHOUSES'),
  };

  @cache
  static get CONSTRAINT_PARSERS (): Partial<Record<string, (this: Parser, ...args: unknown[]) => Expression | Expression[] | undefined>> {
    return {
      ...Parser.CONSTRAINT_PARSERS,
      WITH: function (this: Parser) {
        return (this as SnowflakeParser).parseWithConstraint();
      },
      MASKING: function (this: Parser) {
        return (this as SnowflakeParser).parseWithConstraint();
      },
      PROJECTION: function (this: Parser) {
        return (this as SnowflakeParser).parseWithConstraint();
      },
      TAG: function (this: Parser) {
        return (this as SnowflakeParser).parseWithConstraint();
      },
    };
  }

  @cache
  static get STAGED_FILE_SINGLE_TOKENS (): Set<TokenType> {
    return new Set([
      TokenType.DOT,
      TokenType.MOD,
      TokenType.SLASH,
    ]);
  }

  static FLATTEN_COLUMNS = [
    'SEQ',
    'KEY',
    'PATH',
    'INDEX',
    'VALUE',
    'THIS',
  ];

  static SCHEMA_KINDS = new Set([
    'OBJECTS',
    'TABLES',
    'VIEWS',
    'SEQUENCES',
    'UNIQUE KEYS',
    'IMPORTED KEYS',
  ]);

  static NON_TABLE_CREATABLES = new Set<string>([
    'STORAGE INTEGRATION',
    'TAG',
    'WAREHOUSE',
    'STREAMLIT',
  ]);

  @cache
  static get LAMBDAS (): Partial<Record<TokenType, (this: Parser, expressions: Expression[]) => Expression>> {
    return {
      ...Parser.LAMBDAS,
      [TokenType.ARROW]: function (this: Parser, expressions: Expression[]) {
        return this.expression(LambdaExpr, {
          this: this.replaceLambda(this.parseAssignment(), expressions),
          expressions: expressions.map((e) => (e instanceof CastExpr ? e.args.this : e)),
        });
      },
    };
  }

  @cache
  static get COLUMN_OPERATORS (): Partial<Record<TokenType, undefined | ((this: Parser, this_?: Expression, to?: Expression) => Expression)>> {
    return {
      ...Parser.COLUMN_OPERATORS,
      [TokenType.EXCLAMATION]: function (this: Parser, thisNode?: Expression, attr?: Expression) {
        return this.expression(ModelAttributeExpr, {
          this: thisNode,
          expression: attr,
        });
      },
    };
  }

  parseDirectory (): DirectoryStageExpr {
    let table: ExpressionOrString | undefined = this.parseTableParts();

    if (table instanceof TableExpr) {
      table = table.args.this;
    }

    return this.expression(DirectoryStageExpr, { this: table });
  }

  parseUse (): UseExpr {
    if (this.matchTextSeq(['SECONDARY', 'ROLES'])) {
      const thisNode = this.matchTexts(['ALL', 'NONE']) && new VarExpr({ this: this.prev?.text.toUpperCase() });
      const roles = thisNode ? undefined : this.parseCsv(() => this.parseTable({ schema: false }));
      return this.expression(UseExpr, {
        kind: 'SECONDARY ROLES',
        this: thisNode || undefined,
        expressions: roles,
      });
    }

    return super.parseUse();
  }

  negateRange (thisNode?: Expression): Expression | undefined {
    if (!thisNode) {
      return thisNode;
    }

    const query = thisNode.getArgKey('query');
    if (thisNode instanceof InExpr && query instanceof QueryExpr) {
      /**
       * Snowflake treats `value NOT IN (subquery)` as `VALUE <> ALL (subquery)`, so
       * we do this conversion here to avoid parsing it into `NOT value IN (subquery)`
       * which can produce different results.
       */
      return this.expression(NeqExpr, {
        this: thisNode.args.this,
        expression: new AllExpr({ this: query.unnest() }),
      });
    }

    return this.expression(NotExpr, { this: thisNode });
  }

  parseTag (): TagsExpr {
    return this.expression(TagsExpr, {
      expressions: this.parseWrappedCsv(() => this.parseProperty()),
    });
  }

  parseWithConstraint (): Expression | undefined {
    if (this.prev?.tokenType !== TokenType.WITH) {
      this.retreat(this.index - 1);
    }

    if (this.matchTextSeq(['MASKING', 'POLICY'])) {
      const policy = this.parseColumn();
      return this.expression(MaskingPolicyColumnConstraintExpr, {
        this: policy instanceof ColumnExpr ? policy.toDot() : policy,
        expressions: this.match(TokenType.USING) && this.parseWrappedCsv(() => this.parseIdVar()),
      });
    }

    if (this.matchTextSeq(['PROJECTION', 'POLICY'])) {
      const policy = this.parseColumn();
      return this.expression(ProjectionPolicyColumnConstraintExpr, {
        this: policy instanceof ColumnExpr ? policy.toDot() : policy,
      });
    }

    if (this.match(TokenType.TAG)) {
      return this.parseTag();
    }

    return undefined;
  }

  parseWithProperty (): Expression | Expression[] | undefined {
    if (this.match(TokenType.TAG)) {
      return this.parseTag();
    }

    return super.parseWithProperty();
  }

  parseCreate (): CreateExpr | CommandExpr {
    const expression = super.parseCreate();
    if (
      expression instanceof CreateExpr
      && (this.constructor as typeof SnowflakeParser).NON_TABLE_CREATABLES.has(expression.args.kind ?? '')
    ) {
      // Replace the Table node with the enclosed Identifier
      const tableNode = expression.args.this;
      if (tableNode instanceof TableExpr) {
        tableNode.replace(tableNode.args.this);
      }
    }

    return expression;
  }

  parseDatePart (): Expression | undefined {
    const thisNode = this.parseVar() || this.parseType();

    if (!thisNode) {
      return undefined;
    }

    // Handle both syntaxes: DATE_PART(part, expr) and DATE_PART(part FROM expr)
    const expression = this.matchSet([TokenType.FROM, TokenType.COMMA]) && this.parseBitwise();

    return this.expression(ExtractExpr, {
      this: mapDatePart(thisNode, { dialect: this.dialect }),
      expression: expression || undefined,
    });
  }

  parseBracketKeyValue (options: { isMap?: boolean } = {}): Expression | undefined {
    const {
      isMap = false,
    } = options;

    if (isMap) {
      return this.parseSlice(this.parseString()) || this.parseAssignment();
    }

    return this.parseSlice(this.parseAlias(this.parseAssignment(), { explicit: true }));
  }

  parseLateral (): LateralExpr | undefined {
    const lateral = super.parseLateral();
    if (!lateral) {
      return lateral;
    }

    if (lateral.args.this instanceof ExplodeExpr) {
      const tableAlias = lateral.args.alias;
      const columns = (this.constructor as typeof SnowflakeParser).FLATTEN_COLUMNS.map((col) =>
        toIdentifier(col));

      if (tableAlias && !tableAlias.getArgKey('columns')) {
        tableAlias.setArgKey('columns', columns);
      } else if (!tableAlias) {
        alias(lateral, '_flattened', {
          table: columns,
          copy: false,
        });
      }
    }

    return lateral;
  }

  parseTableParts (options: { schema?: boolean;
    isDbReference?: boolean;
    wildcard?: boolean; } = {}): TableExpr {
    const {
      schema = false, isDbReference = false,
    } = options;

    let table: Expression | undefined;

    if (this.match(TokenType.STRING, { advance: false })) {
      table = this.parseString();
    } else if (this.matchTextSeq('@', { advance: false })) {
      table = this.parseLocationPath();
    }

    if (table) {
      let fileFormat: Expression | undefined;
      let pattern: Expression | undefined;

      const wrapped = this.match(TokenType.L_PAREN);
      while (this.curr && wrapped && !this.match(TokenType.R_PAREN)) {
        if (this.matchTextSeq(['FILE_FORMAT', '=>'])) {
          fileFormat = this.parseString() || super.parseTableParts({ isDbReference });
        } else if (this.matchTextSeq(['PATTERN', '=>'])) {
          pattern = this.parseString();
        } else {
          break;
        }

        this.match(TokenType.COMMA);
      }

      return this.expression(TableExpr, {
        this: table,
        format: fileFormat,
        pattern,
      });
    }

    return super.parseTableParts({
      schema,
      isDbReference,
    });
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
    let table = super.parseTable(options);

    if (table instanceof TableExpr && table.args.this instanceof TableFromRowsExpr) {
      const tableFromRows = table.args.this;
      for (const arg of TableFromRowsExpr.availableArgs) {
        if (arg !== 'this') {
          tableFromRows.setArgKey(arg, table.getArgKey(arg));
        }
      }

      table = tableFromRows;
    }

    return table;
  }

  parseIdVar (options: { anyToken?: boolean;
    tokens?: Set<TokenType>; } = {}): Expression | undefined {
    const {
      anyToken = true, tokens,
    } = options;

    if (this.matchTextSeq(['IDENTIFIER', '('])) {
      const identifier = super.parseIdVar({
        anyToken,
        tokens,
      }) || this.parseString();
      this.matchRParen();
      return this.expression(AnonymousExpr, {
        this: 'IDENTIFIER',
        expressions: [identifier],
      });
    }

    return super.parseIdVar({
      anyToken,
      tokens,
    });
  }

  parseShowSnowflake (thisNode: string): ShowExpr {
    let scope: Expression | undefined;
    let scopeKind: string | undefined;

    // will identify SHOW TERSE SCHEMAS but not SHOW TERSE PRIMARY KEYS
    // which is syntactically valid but has no effect on the output
    const terse = this.tokens[this.index - 2].text.toUpperCase() === 'TERSE';

    const history = this.matchTextSeq('HISTORY');

    const like = this.match(TokenType.LIKE) ? this.parseString() : undefined;

    if (this.match(TokenType.IN)) {
      if (this.matchTextSeq('ACCOUNT')) {
        scopeKind = 'ACCOUNT';
      } else if (this.matchTextSeq('CLASS')) {
        scopeKind = 'CLASS';
        scope = this.parseTableParts();
      } else if (this.matchTextSeq('APPLICATION')) {
        scopeKind = 'APPLICATION';
        if (this.matchTextSeq('PACKAGE')) {
          scopeKind += ' PACKAGE';
        }
        scope = this.parseTableParts();
      } else if (this.matchSet((this.constructor as typeof SnowflakeParser).DB_CREATABLES)) {
        scopeKind = this.prev?.text.toUpperCase();
        if (this.curr) {
          scope = this.parseTableParts();
        }
      } else if (this.curr) {
        scopeKind = (this.constructor as typeof SnowflakeParser).SCHEMA_KINDS.has(thisNode) ? 'SCHEMA' : 'TABLE';
        scope = this.parseTableParts();
      }
    }

    return this.expression(ShowExpr, {
      terse,
      this: thisNode,
      history,
      like,
      scope,
      scopeKind,
      startsWith: this.matchTextSeq(['STARTS', 'WITH']) ? this.parseString() : undefined,
      limit: this.parseLimit(),
      from: this.match(TokenType.FROM) ? this.parseString() : undefined,
      privileges: this.matchTextSeq(['WITH', 'PRIVILEGES'])
        ? this.parseCsv(() => this.parseVar({
          anyToken: true,
          upper: true,
        }))
        : undefined,
    });
  }

  parsePut (): PutExpr | CommandExpr {
    if (this.curr?.tokenType !== TokenType.STRING) {
      return this.parseAsCommand(this.prev);
    }

    return this.expression(PutExpr, {
      this: this.parseString(),
      target: this.parseLocationPath(),
      properties: this.parseProperties(),
    });
  }

  parseGet (): Expression | undefined {
    const start = this.prev;

    // If we detect GET( then we need to parse a function, not a statement
    if (this.match(TokenType.L_PAREN)) {
      this.retreat(this.index - 2);
      return this.parseExpression();
    }

    const target = this.parseLocationPath();

    // Parse as command if unquoted file path
    if (this.curr?.tokenType === TokenType.URI_START) {
      return this.parseAsCommand(start);
    }

    return this.expression(GetExpr, {
      this: this.parseString(),
      target,
      properties: this.parseProperties(),
    });
  }

  parseLocationProperty (): LocationPropertyExpr {
    this.match(TokenType.EQ);
    return this.expression(LocationPropertyExpr, { this: this.parseLocationPath() });
  }

  parseFileLocation (): Expression | undefined {
    // Parse either a subquery or a staged file
    return this.match(TokenType.L_PAREN, { advance: false })
      ? this.parseSelect({
        table: true,
        parseSubqueryAlias: false,
      })
      : this.parseTableParts();
  }

  parseLocationPath (): VarExpr {
    const start = this.curr;
    this.advanceAny({ ignoreReserved: true });

    // We avoid consuming a comma token because external tables like @foo and @bar
    // can be joined in a query with a comma separator, as well as closing paren
    // in case of subqueries
    while (
      this.isConnected()
      && !this.matchSet([
        TokenType.COMMA,
        TokenType.L_PAREN,
        TokenType.R_PAREN,
      ], { advance: false })
    ) {
      this.advanceAny({ ignoreReserved: true });
    }

    return new VarExpr({ this: this.findSql(start, this.prev) });
  }

  parseLambdaArg (): Expression | undefined {
    const thisNode = super.parseLambdaArg();

    if (!thisNode) {
      return thisNode;
    }

    const typ = this.parseTypes();

    if (typ) {
      return this.expression(CastExpr, {
        this: thisNode,
        to: typ,
      });
    }

    return thisNode;
  }

  parseForeignKey (): ForeignKeyExpr {
    // inlineFK, the REFERENCES columns are implied
    if (this.match(TokenType.REFERENCES, { advance: false })) {
      return this.expression(ForeignKeyExpr);
    }

    // outoflineFK, explicitly names the columns
    return super.parseForeignKey();
  }

  parseFileFormatProperty (): FileFormatPropertyExpr {
    this.match(TokenType.EQ);
    let expressions: Expression[];

    if (this.match(TokenType.L_PAREN, { advance: false })) {
      expressions = this.parseWrappedOptions();
    } else {
      expressions = [this.parseFormatName()];
    }

    return this.expression(FileFormatPropertyExpr, {
      expressions,
    });
  }

  parseCredentialsProperty (): CredentialsPropertyExpr {
    return this.expression(CredentialsPropertyExpr, {
      expressions: this.parseWrappedOptions(),
    });
  }

  parseSemanticView (): SemanticViewExpr {
    const kwargs: Record<string, unknown> = { this: this.parseTableParts() };

    while (this.curr && !this.match(TokenType.R_PAREN, { advance: false })) {
      if (this.matchTexts([
        'DIMENSIONS',
        'METRICS',
        'FACTS',
      ])) {
        const keyword = this.prev?.text.toLowerCase() ?? '';
        kwargs[keyword] = this.parseCsv(() => this.parseDisjunction());
      } else if (this.matchTextSeq('WHERE')) {
        kwargs['where'] = this.parseExpression();
      } else {
        this.raiseError('Expecting ) or encountered unexpected keyword');
        break;
      }
    }

    return this.expression(SemanticViewExpr, kwargs);
  }

  parseSet (options: {
    unset?: boolean;
    tag?: boolean;
  } = {}): SetExpr | CommandExpr {
    const {
      unset = false, tag = false,
    } = options;
    const setNode = super.parseSet({
      unset,
      tag,
    });

    if (setNode instanceof SetExpr) {
      for (const expr of setNode.args.expressions || []) {
        if (expr instanceof SetItemExpr) {
          expr.setArgKey('kind', 'VARIABLE');
        }
      }
    }
    return setNode;
  }

  parseWindow (thisNode?: Expression, options: { alias?: boolean } = {}): Expression | undefined {
    if (thisNode instanceof NthValueExpr) {
      if (this.matchTextSeq('FROM')) {
        if (this.matchTexts(['FIRST', 'LAST'])) {
          const fromFirst = this.prev?.text.toUpperCase() === 'FIRST';
          thisNode.setArgKey('fromFirst', fromFirst);
        }
      }
    }

    return super.parseWindow(thisNode, options);
  }
}

class SnowflakeGenerator extends Generator {
  static PARAMETER_TOKEN = '$';
  static MATCHED_BY_SOURCE = false;
  static SINGLE_STRING_INTERVAL = true;
  static JOIN_HINTS = false;
  static TABLE_HINTS = false;
  static QUERY_HINTS = false;
  static AGGREGATE_FILTER_SUPPORTED = false;
  static SUPPORTS_TABLE_COPY = false;
  static COLLATE_IS_FUNC = true;
  static LIMIT_ONLY_LITERALS = true;
  static JSON_KEY_VALUE_PAIR_SEP = ',';
  static INSERT_OVERWRITE = ' OVERWRITE INTO';
  static STRUCT_DELIMITER = ['(', ')'];
  static COPY_PARAMS_ARE_WRAPPED = false;
  static COPY_PARAMS_EQ_REQUIRED = true;
  static STAR_EXCEPT = 'EXCLUDE';
  static SUPPORTS_EXPLODING_PROJECTIONS = false;
  static ARRAY_CONCAT_IS_VAR_LEN = false;
  static SUPPORTS_CONVERT_TIMEZONE = true;
  static EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = false;
  static SUPPORTS_MEDIAN = true;
  static ARRAY_SIZE_NAME = 'ARRAY_SIZE';
  static SUPPORTS_DECODE_CASE = true;
  static IS_BOOL_ALLOWED = false;
  static DIRECTED_JOINS = true;

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (this: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (this: Generator, e: any) => string>([
      ...Generator.TRANSFORMS,
      [ApproxDistinctExpr, renameFunc('APPROX_COUNT_DISTINCT')],
      [ArgMaxExpr, renameFunc('MAX_BY')],
      [ArgMinExpr, renameFunc('MIN_BY')],
      [ArrayExpr, preprocess([inheritStructFieldNames])],
      [ArrayConcatExpr, arrayConcatSql('ARRAY_CAT')],
      [ArrayAppendExpr, arrayAppendSql('ARRAY_APPEND')],
      [ArrayPrependExpr, arrayAppendSql('ARRAY_PREPEND')],
      [
        ArrayContainsExpr,
        function (this: Generator, e: ArrayContainsExpr) {
          return this.func('ARRAY_CONTAINS', [
            e.args.ensureVariant === false
              ? e.args.expression
              : new CastExpr({
                this: e.args.expression,
                to: DataTypeExpr.build(DataTypeExprKind.VARIANT),
              }),
            e.args.this,
          ]);
        },
      ],
      [ArrayIntersectExpr, renameFunc('ARRAY_INTERSECTION')],
      [
        AtTimeZoneExpr,
        function (this: Generator, e: AtTimeZoneExpr) {
          return this.func('CONVERT_TIMEZONE', [e.args.zone, e.args.this]);
        },
      ],
      [BitwiseOrExpr, renameFunc('BITOR')],
      [BitwiseXorExpr, renameFunc('BITXOR')],
      [BitwiseAndExpr, renameFunc('BITAND')],
      [BitwiseAndAggExpr, renameFunc('BITANDAGG')],
      [BitwiseOrAggExpr, renameFunc('BITORAGG')],
      [BitwiseXorAggExpr, renameFunc('BITXORAGG')],
      [BitwiseNotExpr, renameFunc('BITNOT')],
      [BitwiseLeftShiftExpr, renameFunc('BITSHIFTLEFT')],
      [BitwiseRightShiftExpr, renameFunc('BITSHIFTRIGHT')],
      [CreateExpr, preprocess([flattenStructuredTypesUnlessIceberg])],
      [
        CurrentTimestampExpr,
        function (this: Generator, e: CurrentTimestampExpr) {
          return e.args.sysdate ? this.func('SYSDATE', []) : this.functionFallbackSql(e);
        },
      ],
      [
        LocaltimeExpr,
        function (this: Generator, e: LocaltimeExpr) {
          return e.args.this ? this.func('CURRENT_TIME', [e.args.this]) : 'CURRENT_TIME';
        },
      ],
      [
        LocaltimestampExpr,
        function (this: Generator, e: LocaltimestampExpr) {
          return e.args.this ? this.func('CURRENT_TIMESTAMP', [e.args.this]) : 'CURRENT_TIMESTAMP';
        },
      ],
      [DateAddExpr, dateDeltaSql('DATEADD')],
      [DateDiffExpr, dateDeltaSql('DATEDIFF')],
      [DatetimeAddExpr, dateDeltaSql('TIMESTAMPADD')],
      [DatetimeDiffExpr, timestampDiffSql],
      [DateStrToDateExpr, dateStrToDateSql],
      [
        DecryptExpr,
        function (this: Generator, e: DecryptExpr) {
          return this.func(`${e.args.safe ? 'TRY_' : ''}DECRYPT`, [
            e.args.this,
            e.args.passphrase,
            e.args.aad,
            e.args.encryptionMethod,
          ]);
        },
      ],
      [
        DecryptRawExpr,
        function (this: Generator, e: DecryptRawExpr) {
          return this.func(`${e.args.safe ? 'TRY_' : ''}DECRYPT_RAW`, [
            e.args.this,
            e.args.key,
            e.args.iv,
            e.args.aad,
            e.args.encryptionMethod,
            e.args.aead as Expression | undefined,
          ]);
        },
      ],
      [DayOfMonthExpr, renameFunc('DAYOFMONTH')],
      [DayOfWeekExpr, renameFunc('DAYOFWEEK')],
      [DayOfWeekIsoExpr, renameFunc('DAYOFWEEKISO')],
      [DayOfYearExpr, renameFunc('DAYOFYEAR')],
      [DotProductExpr, renameFunc('VECTOR_INNER_PRODUCT')],
      [ExplodeExpr, renameFunc('FLATTEN')],
      [
        ExtractExpr,
        function (this: Generator, e: ExtractExpr) {
          return this.func('DATE_PART', [mapDatePart(e.args.this, { dialect: this.dialect }), e.args.expression]);
        },
      ],
      [CosineDistanceExpr, renameFunc('VECTOR_COSINE_SIMILARITY')],
      [EuclideanDistanceExpr, renameFunc('VECTOR_L2_DISTANCE')],
      [
        FileFormatPropertyExpr,
        function (this: Generator, e: FileFormatPropertyExpr) {
          return `FILE_FORMAT=(${this.expressions(e, {
            key: 'expressions',
            sep: ' ',
          })})`;
        },
      ],
      [
        FromTimeZoneExpr,
        function (this: Generator, e: FromTimeZoneExpr) {
          return this.func('CONVERT_TIMEZONE', [
            e.args.zone,
            LiteralExpr.string('UTC'),
            e.args.this,
          ]);
        },
      ],
      [
        GenerateSeriesExpr,
        function (this: Generator, e: GenerateSeriesExpr) {
          return this.func('ARRAY_GENERATE_RANGE', [
            e.args.start,
            e.args.end?.add(1),
            e.args.step,
          ]);
        },
      ],
      [GetExtractExpr, renameFunc('GET')],
      [
        GroupConcatExpr,
        function (this: Generator, e: GroupConcatExpr) {
          return groupConcatSql.call(this, e, { sep: '' });
        },
      ],
      [IfExpr, ifSql('IFF', 'NULL')],
      [JsonExtractArrayExpr, jsonExtractValueArraySql],
      [
        JsonExtractScalarExpr,
        function (this: Generator, e: JsonExtractScalarExpr) {
          return this.func('JSON_EXTRACT_PATH_TEXT', [e.args.this, e.args.expression]);
        },
      ],
      [JsonKeysExpr, renameFunc('OBJECT_KEYS')],
      [
        JsonObjectExpr,
        function (this: Generator, e: JsonObjectExpr) {
          return this.func('OBJECT_CONSTRUCT_KEEP_NULL', e.args.expressions || []);
        },
      ],
      [JsonPathRootExpr, () => ''],
      [JsonValueArrayExpr, jsonExtractValueArraySql],
      [
        LevenshteinExpr,
        function (this: Generator, e: LevenshteinExpr) {
          unsupportedArgs.call(this, e, 'insCost', 'delCost', 'subCost');
          return renameFunc('EDITDISTANCE').call(this, e);
        },
      ],
      [
        LocationPropertyExpr,
        function (this: Generator, e: LocationPropertyExpr) {
          return `LOCATION=${this.sql(e, 'this')}`;
        },
      ],
      [LogicalAndExpr, renameFunc('BOOLAND_AGG')],
      [LogicalOrExpr, renameFunc('BOOLOR_AGG')],
      [
        MapExpr,
        function (this: Generator, e: MapExpr) {
          return varMapSql.call(this, e, 'OBJECT_CONSTRUCT');
        },
      ],
      [ManhattanDistanceExpr, renameFunc('VECTOR_L1_DISTANCE')],
      [MakeIntervalExpr, noMakeIntervalSql],
      [MaxExpr, maxOrGreatest],
      [MinExpr, minOrLeast],
      [
        ParseJsonExpr,
        function (this: Generator, e: ParseJsonExpr) {
          return this.func(`${e.args.safe ? 'TRY_' : ''}PARSE_JSON`, [e.args.this]);
        },
      ],
      [
        ToBinaryExpr,
        function (this: Generator, e: ToBinaryExpr) {
          return this.func(`${e.args.safe ? 'TRY_' : ''}TO_BINARY`, [e.args.this, e.args.format]);
        },
      ],
      [
        ToBooleanExpr,
        function (this: Generator, e: ToBooleanExpr) {
          return this.func(`${e.args.safe ? 'TRY_' : ''}TO_BOOLEAN`, [e.args.this]);
        },
      ],
      [
        ToDoubleExpr,
        function (this: Generator, e: ToDoubleExpr) {
          return this.func(`${e.args.safe ? 'TRY_' : ''}TO_DOUBLE`, [e.args.this, e.args.format]);
        },
      ],
      [
        ToFileExpr,
        function (this: Generator, e: ToFileExpr) {
          return this.func(`${e.args.safe ? 'TRY_' : ''}TO_FILE`, [e.args.this, e.args.path]);
        },
      ],
      [
        ToNumberExpr,
        function (this: Generator, e: ToNumberExpr) {
          return this.func(`${e.args.safe ? 'TRY_' : ''}TO_NUMBER`, [
            e.args.this,
            e.args.format,
            e.args.precision,
            e.args.scale,
          ]);
        },
      ],
      [JsonFormatExpr, renameFunc('TO_JSON')],
      [
        PartitionedByPropertyExpr,
        function (this: Generator, e: PartitionedByPropertyExpr) {
          return `PARTITION BY ${this.sql(e, 'this')}`;
        },
      ],
      [PercentileContExpr, preprocess([addWithinGroupForPercentiles])],
      [PercentileDiscExpr, preprocess([addWithinGroupForPercentiles])],
      [PivotExpr, preprocess([unqualifyPivotColumns])],
      [RegexpExtractExpr, regexpExtractSql],
      [RegexpExtractAllExpr, regexpExtractSql],
      [RegexpILikeExpr, regexpILikeSql],
      [RandExpr, renameFunc('RANDOM')],
      [
        SelectExpr,
        preprocess([
          eliminateWindowClause,
          eliminateDistinctOn,
          explodeProjectionToUnnest(),
          eliminateSemiAndAntiJoins,
          transformGenerateDateArray,
          qualifyUnnestedColumns,
          eliminateDotVariantLookup,
        ]),
      ],
      [ShaExpr, renameFunc('SHA1')],
      [Sha1DigestExpr, renameFunc('SHA1_BINARY')],
      [Md5DigestExpr, renameFunc('MD5_BINARY')],
      [Md5NumberLower64Expr, renameFunc('MD5_NUMBER_LOWER64')],
      [Md5NumberUpper64Expr, renameFunc('MD5_NUMBER_UPPER64')],
      [LowerHexExpr, renameFunc('TO_CHAR')],
      [SortArrayExpr, renameFunc('ARRAY_SORT')],
      [SkewnessExpr, renameFunc('SKEW')],
      [StarMapExpr, renameFunc('OBJECT_CONSTRUCT')],
      [StartsWithExpr, renameFunc('STARTSWITH')],
      [EndsWithExpr, renameFunc('ENDSWITH')],
      [
        StrPositionExpr,
        function (this: Generator, e: StrPositionExpr) {
          return strPositionSql.call(this, e, {
            funcName: 'CHARINDEX',
            supportsPosition: true,
          });
        },
      ],
      [
        StrToDateExpr,
        function (this: Generator, e: StrToDateExpr) {
          return this.func('DATE', [e.args.this, this.formatTime(e)]);
        },
      ],
      [StringToArrayExpr, renameFunc('STRTOK_TO_ARRAY')],
      [StuffExpr, renameFunc('INSERT')],
      [StPointExpr, renameFunc('ST_MAKEPOINT')],
      [TimeAddExpr, dateDeltaSql('TIMEADD')],
      [
        TimeSliceExpr,
        function (this: Generator, e: TimeSliceExpr) {
          return this.func('TIME_SLICE', [
            e.args.this,
            e.args.expression,
            unitToStr(e),
            e.args.kind,
          ]);
        },
      ],
      [TimestampExpr, noTimestampSql],
      [TimestampAddExpr, dateDeltaSql('TIMESTAMPADD')],
      [
        TimestampDiffExpr,
        function (this: Generator, e: TimestampDiffExpr) {
          return this.func('TIMESTAMPDIFF', [
            e.args.unit,
            e.args.expression,
            e.args.this,
          ]);
        },
      ],
      [TimestampTruncExpr, timestampTruncSql()],
      [TimeStrToTimeExpr, timeStrToTimeSql],
      [
        TimeToUnixExpr,
        function (this: Generator, e: TimeToUnixExpr) {
          return `EXTRACT(epoch_second FROM ${this.sql(e, 'this')})`;
        },
      ],
      [ToArrayExpr, renameFunc('TO_ARRAY')],
      [
        ToCharExpr,
        function (this: Generator, e: ToCharExpr) {
          return this.functionFallbackSql(e);
        },
      ],
      [TsOrDsAddExpr, dateDeltaSql('DATEADD', { cast: true })],
      [TsOrDsDiffExpr, dateDeltaSql('DATEDIFF')],
      [
        TsOrDsToDateExpr,
        function (this: Generator, e: TsOrDsToDateExpr) {
          return this.func(`${e.args.safe ? 'TRY_' : ''}TO_DATE`, [e.args.this, this.formatTime(e)]);
        },
      ],
      [
        TsOrDsToTimeExpr,
        function (this: Generator, e: TsOrDsToTimeExpr) {
          return this.func(`${e.args.safe ? 'TRY_' : ''}TO_TIME`, [e.args.this, this.formatTime(e)]);
        },
      ],
      [UnhexExpr, renameFunc('HEX_DECODE_BINARY')],
      [
        UnixToTimeExpr,
        function (this: Generator, e: UnixToTimeExpr) {
          return this.func('TO_TIMESTAMP', [e.args.this, e.args.scale]);
        },
      ],
      [UuidExpr, renameFunc('UUID_STRING')],
      [
        VarMapExpr,
        function (this: Generator, e: VarMapExpr) {
          return varMapSql.call(this, e, 'OBJECT_CONSTRUCT');
        },
      ],
      [BoolandExpr, renameFunc('BOOLAND')],
      [BoolorExpr, renameFunc('BOOLOR')],
      [WeekOfYearExpr, renameFunc('WEEKISO')],
      [YearOfWeekExpr, renameFunc('YEAROFWEEK')],
      [YearOfWeekIsoExpr, renameFunc('YEAROFWEEKISO')],
      [XorExpr, renameFunc('BOOLXOR')],
      [ByteLengthExpr, renameFunc('OCTET_LENGTH')],
      [FlattenExpr, renameFunc('ARRAY_FLATTEN')],
      [
        ArrayConcatAggExpr,
        function (this: Generator, e: ArrayConcatAggExpr) {
          return this.func('ARRAY_FLATTEN', [new ArrayAggExpr({ this: e.args.this })]);
        },
      ],
      [
        Sha2DigestExpr,
        function (this: Generator, e: Sha2DigestExpr) {
          return this.func('SHA2_BINARY', [e.args.this, e.args.length || LiteralExpr.number(256)]);
        },
      ],
    ]);
    return transforms;
  }

  nthvalueSql (expression: NthValueExpr): string {
    let result = this.func('NTH_VALUE', [expression.args.this, expression.args.offset]);
    const fromFirst = expression.args.fromFirst;

    if (fromFirst !== undefined) {
      result += fromFirst ? ' FROM FIRST' : ' FROM LAST';
    }

    return result;
  }

  @cache
  static get SUPPORTED_JSON_PATH_PARTS (): Set<typeof Expression> {
    return new Set([
      JsonPathKeyExpr,
      JsonPathRootExpr,
      JsonPathSubscriptExpr,
    ]);
  }

  @cache
  static get TYPE_MAPPING () {
    return {
      ...Generator.TYPE_MAPPING,
      [DataTypeExprKind.BIGDECIMAL]: 'DOUBLE',
      [DataTypeExprKind.NESTED]: 'OBJECT',
      [DataTypeExprKind.STRUCT]: 'OBJECT',
      [DataTypeExprKind.TEXT]: 'VARCHAR',
    };
  }

  static TOKEN_MAPPING: Partial<Record<TokenType, string>> = {
    [TokenType.AUTO_INCREMENT]: 'AUTOINCREMENT',
  };

  @cache
  static get PROPERTIES_LOCATION () {
    return new Map([
      ...Generator.PROPERTIES_LOCATION,
      [CredentialsPropertyExpr, PropertiesLocation.POST_WITH],
      [LocationPropertyExpr, PropertiesLocation.POST_WITH],
      [PartitionedByPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [SetPropertyExpr, PropertiesLocation.UNSUPPORTED],
      [VolatilePropertyExpr, PropertiesLocation.UNSUPPORTED],
    ]);
  }

  @cache
  static get UNSUPPORTED_VALUES_EXPRESSIONS (): Set<typeof Expression> {
    return new Set([
      MapExpr,
      StarMapExpr,
      StructExpr,
      VarMapExpr,
    ]);
  }

  @cache
  static get RESPECT_IGNORE_NULLS_UNSUPPORTED_EXPRESSIONS (): (typeof Expression)[] {
    return [ArrayAggExpr];
  }

  withProperties (properties: PropertiesExpr): string {
    return this.properties(properties, {
      wrapped: false,
      prefix: this.sep(''),
      sep: ' ',
    });
  }

  valuesSql (expression: ValuesExpr, options: { valuesAsTable?: boolean } = {}): string {
    let { valuesAsTable = true } = options;
    if (expression.find((this._constructor as typeof SnowflakeGenerator).UNSUPPORTED_VALUES_EXPRESSIONS)) {
      valuesAsTable = false;
    }

    return super.valuesSql(expression, { valuesAsTable });
  }

  dataTypeSql (expression: DataTypeExpr): string {
    // Check if this is a FLOAT type nested inside a VECTOR type
    // VECTOR only accepts FLOAT (not DOUBLE), INT, and STRING as element types
    if (expression.isType(DataTypeExprKind.DOUBLE)) {
      const parent = expression.parent;
      if (parent instanceof DataTypeExpr && parent.isType(DataTypeExprKind.VECTOR)) {
        return 'FLOAT';
      }
    }

    const expressions = expression.args.expressions;
    if (expressions && expression.isType(DataTypeExpr.STRUCT_TYPES)) {
      for (const fieldType of expressions) {
        // The correct syntax is OBJECT [ (<key> <value_type [NOT NULL] [, ...]) ]
        if (fieldType instanceof DataTypeExpr) {
          return 'OBJECT';
        }
        if (fieldType instanceof ColumnDefExpr && fieldType.args.this instanceof LiteralExpr && fieldType.args.this.isString) {
          fieldType.args.this.replace(toIdentifier(fieldType.name, { quoted: true }));
        }
      }
    }

    return super.dataTypeSql(expression);
  }

  toNumberSql (expression: ToNumberExpr): string {
    return this.func('TO_NUMBER', [
      expression.args.this,
      expression.args.format,
      expression.args.precision,
      expression.args.scale,
    ]);
  }

  timestampFromPartsSql (expression: TimestampFromPartsExpr): string {
    const milli = expression.args.milli;
    if (milli !== undefined) {
      const milliToNano = milli.pop().mul(LiteralExpr.number(1000000));
      expression.setArgKey('nano', milliToNano);
    }

    return renameFunc('TIMESTAMP_FROM_PARTS').call(this, expression);
  }

  castSql (expression: CastExpr, options: { safePrefix?: string } = {}): string {
    if (expression.isType(DataTypeExprKind.GEOGRAPHY)) {
      return this.func('TO_GEOGRAPHY', [expression.args.this]);
    }
    if (expression.isType(DataTypeExprKind.GEOMETRY)) {
      return this.func('TO_GEOMETRY', [expression.args.this]);
    }

    return super.castSql(expression, options);
  }

  tryCastSql (expression: TryCastExpr): string {
    let value = expression.args.this;

    if (value !== undefined && !value.type) {
      value = annotateTypes(value, { dialect: this.dialect });
    }

    // Snowflake requires that TRY_CAST's value be a string
    if (expression.args.requiresString || value?.isType(DataTypeExpr.TEXT_TYPES)) {
      return super.tryCastSql(expression);
    }

    return this.castSql(expression as CastExpr);
  }

  logSql (expression: LogExpr): string {
    if (!expression.args.expression) {
      return this.func('LN', [expression.args.this]);
    }

    return super.logSql(expression);
  }

  greatestSql (expression: GreatestExpr): string {
    const name = expression.args.ignoreNulls ? 'GREATEST_IGNORE_NULLS' : 'GREATEST';
    return this.func(name, [expression.args.this, ...(expression.args.expressions || [])]);
  }

  leastSql (expression: LeastExpr): string {
    const name = expression.args.ignoreNulls ? 'LEAST_IGNORE_NULLS' : 'LEAST';
    return this.func(name, [expression.args.this, ...(expression.args.expressions || [])]);
  }

  generatorSql (expression: GeneratorExpr): string {
    const args: Expression[] = [];
    const rowcount = expression.args.rowcount;
    const timeLimit = expression.args.timeLimit;

    if (rowcount) {
      args.push(new KwargExpr({
        this: new VarExpr({ this: 'ROWCOUNT' }),
        expression: rowcount,
      }));
    }
    if (timeLimit) {
      args.push(new KwargExpr({
        this: new VarExpr({ this: 'TIMELIMIT' }),
        expression: timeLimit,
      }));
    }

    return this.func('GENERATOR', args);
  }

  unnestSql (expression: UnnestExpr): string {
    const unnestAlias = expression.args.alias;
    const offset = expression.args.offset;

    const unnestAliasColumns = narrowInstanceOf(unnestAlias, TableAliasExpr)?.args.columns || [];
    const value = seqGet(unnestAliasColumns, 0) || toIdentifier('value');

    const columns = [
      toIdentifier('seq'),
      toIdentifier('key'),
      toIdentifier('path'),
      offset instanceof IdentifierExpr ? offset.pop() : toIdentifier('index'),
      value,
      toIdentifier('this'),
    ];

    let finalAlias = unnestAlias;
    if (finalAlias) {
      finalAlias.setArgKey('columns', columns);
    } else {
      finalAlias = new TableAliasExpr({
        this: toIdentifier('_u'),
        columns,
      });
    }

    let tableInput = this.sql(expression.args.expressions?.[0]);
    if (!tableInput.startsWith('INPUT =>')) {
      tableInput = `INPUT => ${tableInput}`;
    }

    const expressionParent = expression.parent;

    const explode = (expressionParent instanceof LateralExpr)
      ? `FLATTEN(${tableInput})`
      : `TABLE(FLATTEN(${tableInput}))`;

    const aliasSql = this.sql(finalAlias);
    const aliasPart = aliasSql ? ` AS ${aliasSql}` : '';

    const prefix = (expressionParent instanceof FromExpr || expressionParent instanceof JoinExpr || expressionParent instanceof LateralExpr)
      ? ''
      : `${this.sql(value)} FROM `;

    return `${prefix}${explode}${aliasPart}`;
  }

  showSql (expression: ShowExpr): string {
    const terse = expression.args.terse ? 'TERSE ' : '';
    const history = expression.args.history ? ' HISTORY' : '';

    let like = this.sql(expression, 'like');
    like = like ? ` LIKE ${like}` : '';

    let scope = this.sql(expression, 'scope');
    scope = scope ? ` ${scope}` : '';

    let scopeKind = this.sql(expression, 'scopeKind');
    if (scopeKind) {
      scopeKind = ` IN ${scopeKind}`;
    }

    let startsWith = this.sql(expression, 'startsWith');
    if (startsWith) {
      startsWith = ` STARTS WITH ${startsWith}`;
    }

    const limit = this.sql(expression, 'limit');

    let from = this.sql(expression, 'from');
    if (from) {
      from = ` FROM ${from}`;
    }

    let privileges = this.expressions(expression, {
      key: 'privileges',
      flat: true,
    });
    privileges = privileges ? ` WITH PRIVILEGES ${privileges}` : '';

    return `SHOW ${terse}${expression.name}${history}${like}${scopeKind}${scope}${startsWith}${limit}${from}${privileges}`;
  }

  describeSql (expression: DescribeExpr): string {
    const kindValue = expression.args.kind || 'TABLE';
    const kind = kindValue ? ` ${kindValue}` : '';
    const thisNode = ` ${this.sql(expression, 'this')}`;

    let expressions = this.expressions(expression, { flat: true });
    expressions = expressions ? ` ${expressions}` : '';

    return `DESCRIBE${kind}${thisNode}${expressions}`;
  }

  generatedAsIdentityColumnConstraintSql (expression: GeneratedAsIdentityColumnConstraintExpr): string {
    const start = expression.args.start ? ` START ${expression.args.start}` : '';
    const increment = expression.args.increment ? ` INCREMENT ${expression.args.increment}` : '';

    let orderClause = '';
    if (expression.args.order !== undefined) {
      orderClause = expression.args.order ? ' ORDER' : ' NOORDER';
    }

    return `AUTOINCREMENT${start}${increment}${orderClause}`;
  }

  clusterSql (expression: ClusterExpr): string {
    return `CLUSTER BY (${this.expressions(expression, { flat: true })})`;
  }

  structSql (expression: StructExpr): string {
    if (expression.args.expressions?.length === 1) {
      const arg = expression.args.expressions[0];
      if (arg.isStar || (arg instanceof ILikeExpr && narrowInstanceOf(arg.args.this, Expression)?.isStar)) {
        return `{${this.sql(expression.args.expressions[0])}}`;
      }
    }

    const keys: ExpressionValue[] = [];
    const values: ExpressionValue[] = [];

    expression.args.expressions?.forEach((e, i) => {
      if (e instanceof PropertyEqExpr) {
        if (e.args.this !== undefined) {
          keys.push(
            e.args.this instanceof IdentifierExpr
              ? LiteralExpr.string(e.name)
              : e.args.this,
          );
        }
        if (e.args.expression !== undefined) {
          values.push(e.args.expression);
        }
      } else {
        keys.push(LiteralExpr.string(`_${i}`));
        values.push(e);
      }
    });

    const args: ExpressionValue[] = [];
    for (let i = 0; i < keys.length; i++) {
      args.push(keys[i], values[i]);
    }

    return this.func('OBJECT_CONSTRUCT', args);
  }

  approxQuantileSql (expression: ApproxQuantileExpr): string {
    unsupportedArgs.call(this, expression, 'weight', 'accuracy');
    return this.func('APPROX_PERCENTILE', [expression.args.this, expression.args.quantile]);
  }

  alterSetSql (expression: AlterSetExpr): string {
    let exprs = this.expressions(expression, { flat: true });
    exprs = exprs ? ` ${exprs}` : '';

    let fileFormat = this.expressions(expression, {
      key: 'fileFormat',
      flat: true,
      sep: ' ',
    });
    fileFormat = fileFormat ? ` STAGE_FILE_FORMAT = (${fileFormat})` : '';

    let copyOptions = this.expressions(expression, {
      key: 'copyOptions',
      flat: true,
      sep: ' ',
    });
    copyOptions = copyOptions ? ` STAGE_COPY_OPTIONS = (${copyOptions})` : '';

    let tag = this.expressions(expression, {
      key: 'tag',
      flat: true,
    });
    tag = tag ? ` TAG ${tag}` : '';

    return `SET${exprs}${fileFormat}${copyOptions}${tag}`;
  }

  strToTimeSql (expression: StrToTimeExpr): string {
    const targetType = expression.args.targetType;
    let typeEnum: ExpressionValue | undefined;

    if (targetType instanceof DataTypeExpr) {
      typeEnum = targetType.args.this?.toString();
    } else if (expression.type) {
      typeEnum = expression.type instanceof Expression ? expression.type.args.this?.toString() : expression.type.toString();
    } else {
      typeEnum = DataTypeExprKind.TIMESTAMP;
    }

    const funcName = TIMESTAMP_TYPES[(typeEnum || '') as DataTypeExprKind] || 'TO_TIMESTAMP';

    return this.func(
      `${expression.args.safe ? 'TRY_' : ''}${funcName}`,
      [expression.args.this, this.formatTime(expression)],
    );
  }

  timestampSubSql (expression: TimestampSubExpr): string {
    return this.sql(
      new TimestampAddExpr({
        this: expression.args.this,
        expression: expression.args.expression?.mul(-1),
        unit: expression.args.unit,
      }),
    );
  }

  jsonExtractSql (expression: JsonExtractExpr): string {
    let thisNode = expression.args.this;

    if (
      !(thisNode instanceof ParseJsonExpr || thisNode instanceof JsonExtractExpr)
      && !expression.args.requiresJson
    ) {
      thisNode = new ParseJsonExpr({ this: thisNode });
    }

    return this.func('GET_PATH', [thisNode, expression.args.expression]);
  }

  timeToStrSql (expression: TimeToStrExpr): string {
    let thisNode = expression.args.this;
    if (thisNode instanceof LiteralExpr && thisNode.isString) {
      thisNode = new CastExpr({
        this: thisNode,
        to: DataTypeExpr.build(DataTypeExprKind.TIMESTAMP),
      });
    }

    return this.func('TO_CHAR', [thisNode, this.formatTime(expression)]);
  }

  dateSubSql (expression: DateSubExpr): string {
    const value = expression.args.expression;
    if (value) {
      value.replace(value.mul(-1));
    } else {
      this.unsupported('DateSub cannot be transpiled if the subtracted count is unknown');
    }

    return dateDeltaSql('DATEADD').call(this, expression);
  }

  selectSql (expression: SelectExpr): string {
    const limit = expression.args.limit;
    const offset = expression.args.offset;
    if (offset && !limit) {
      expression.limit(null_(), { copy: false });
    }
    return super.selectSql(expression);
  }

  createableSql (expression: CreateExpr, locations: Map<PropertiesLocation, Expression[]>): string {
    const isMaterialized = expression.find(MaterializedPropertyExpr);
    const copyGrantsProperty = expression.find(CopyGrantsPropertyExpr);

    if (expression.args.kind === CreateExprKind.VIEW && isMaterialized && copyGrantsProperty) {
      const postSchemaProperties = locations.get(PropertiesLocation.POST_SCHEMA) || [];
      const index = postSchemaProperties.indexOf(copyGrantsProperty);
      if (index !== -1) {
        postSchemaProperties.splice(index, 1);
      }

      const thisName = this.sql(expression.args.this, 'this');
      const copyGrants = this.sql(copyGrantsProperty);
      let thisSchema = this.schemaColumnsSql(expression.args.this as SchemaExpr);
      thisSchema = thisSchema ? `${this.sep()}${thisSchema}` : '';

      return `${thisName}${this.sep()}${copyGrants}${thisSchema}`;
    }

    return super.createableSql(expression, locations);
  }

  arrayAggSql (expression: ArrayAggExpr): string {
    const thisNode = expression.args.this;
    const order = thisNode instanceof OrderExpr ? thisNode : undefined;

    if (order) {
      expression.setArgKey('this', order.args.this?.pop());
    }

    let exprSql = super.arrayAggSql(expression);

    if (order) {
      exprSql = this.sql(new WithinGroupExpr({
        this: exprSql,
        expression: order,
      }));
    }

    return exprSql;
  }

  arraySql (expression: ArrayExpr): string {
    const firstExpr = seqGet(expression.args.expressions || [], 0);

    if (firstExpr instanceof SelectExpr) {
      if (firstExpr.text('kind').toUpperCase() === 'STRUCT') {
        const objectConstructArgs: Expression[] = [];

        firstExpr.args.expressions?.forEach((expr) => {
          const name = expr instanceof AliasExpr ? expr.args.this : expr;
          objectConstructArgs.push(LiteralExpr.string(expr.aliasOrName), name ?? null_());
        });

        const arrayAgg = new ArrayAggExpr({
          this: buildObjectConstruct(objectConstructArgs),
        });

        firstExpr.setArgKey('kind', undefined);
        firstExpr.setArgKey('expressions', [arrayAgg]);

        return this.sql(firstExpr.subquery());
      }
    }

    return inlineArraySql.call(this, expression);
  }

  currentDateSql (expression: CurrentDateExpr): string {
    const zone = this.sql(expression, 'this');
    if (!zone) {
      return super.currentDateSql(expression);
    }

    const expr = new CastExpr({
      this: new ConvertTimezoneExpr({
        targetTz: zone,
        timestamp: new CurrentTimestampExpr({}),
      }),
      to: DataTypeExpr.build(DataTypeExprKind.DATE),
    });
    return this.sql(expr);
  }

  dotSql (expression: DotExpr): string {
    let thisNode = expression.args.this;

    if (thisNode instanceof Expression && !thisNode?.type) {
      thisNode = annotateTypes(thisNode, { dialect: this.dialect });
    }

    if (!(thisNode instanceof DotExpr) && isType(thisNode, DataTypeExprKind.STRUCT)) {
      return `${this.sql(thisNode)}:${this.sql(expression, 'expression')}`;
    }

    return super.dotSql(expression);
  }

  modelAttributeSql (expression: ModelAttributeExpr): string {
    return `${this.sql(expression, 'this')}!${this.sql(expression, 'expression')}`;
  }

  formatSql (expression: FormatExpr): string {
    if (expression.name.toLowerCase() === '%s' && expression.args.expressions?.length === 1) {
      return this.func('TO_CHAR', [expression.args.expressions[0]]);
    }

    return this.functionFallbackSql(expression);
  }

  splitPartSql (expression: SplitPartExpr): string {
    if (!expression.args.delimiter) {
      expression.setArgKey('delimiter', LiteralExpr.string(' '));
    }

    if (!expression.args.partIndex) {
      expression.setArgKey('partIndex', LiteralExpr.number(1));
    }

    return renameFunc('SPLIT_PART').call(this, expression);
  }

  uniformSql (expression: UniformExpr): string {
    let gen = expression.args.gen;
    const seed = expression.args.seed;

    if (seed) {
      gen = new RandExpr({ this: seed });
    }

    if (!gen) {
      gen = new RandExpr();
    }

    return this.func('UNIFORM', [
      expression.args.this,
      expression.args.expression,
      gen,
    ]);
  }
}

export class Snowflake extends Dialect {
  static NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE;
  static NULL_ORDERING = 'nulls_are_large' as const;
  static TIME_FORMAT = '\'YYYY-MM-DD HH24:MI:SS\'';
  static SUPPORTS_USER_DEFINED_TYPES = false;
  static SUPPORTS_SEMI_ANTI_JOIN = false;
  static PREFER_CTE_ALIAS_COLUMN = true;
  static TABLESAMPLE_SIZE_IS_PERCENT = true;
  static COPY_PARAMS_ARE_CSV = false;
  static ARRAY_AGG_INCLUDES_NULLS = undefined;
  static ARRAY_FUNCS_PROPAGATES_NULLS = true;
  static ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN = false;
  static TRY_CAST_REQUIRES_STRING = true;
  static SUPPORTS_ALIAS_REFS_IN_JOIN_CONDITIONS = true;
  static LEAST_GREATEST_IGNORES_NULLS = false;

  static INITCAP_DEFAULT_DELIMITER_CHARS = ' \t\n\r\f\v!?@"^#$&~_,.:;+\\-*%/|\\[\\](){}<>';

  static INVERSE_TIME_MAPPING = {
    T: 'T', // Prevent 'T' from being mapped back to '"T"'
  };

  static TIME_MAPPING = {
    'YYYY': '%Y',
    'yyyy': '%Y',
    'YY': '%y',
    'yy': '%y',
    'MMMM': '%B',
    'mmmm': '%B',
    'MON': '%b',
    'mon': '%b',
    'MM': '%m',
    'mm': '%m',
    'DD': '%d',
    'dd': '%-d',
    'DY': '%a',
    'dy': '%w',
    'HH24': '%H',
    'hh24': '%H',
    'HH12': '%I',
    'hh12': '%I',
    'MI': '%M',
    'mi': '%M',
    'SS': '%S',
    'ss': '%S',
    'FF': '%f_nine',
    'ff': '%f_nine',
    'FF0': '%f_zero',
    'ff0': '%f_zero',
    'FF1': '%f_one',
    'ff1': '%f_one',
    'FF2': '%f_two',
    'ff2': '%f_two',
    'FF3': '%f_three',
    'ff3': '%f_three',
    'FF4': '%f_four',
    'ff4': '%f_four',
    'FF5': '%f_five',
    'ff5': '%f_five',
    'FF6': '%f',
    'ff6': '%f',
    'FF7': '%f_seven',
    'ff7': '%f_seven',
    'FF8': '%f_eight',
    'ff8': '%f_eight',
    'FF9': '%f_nine',
    'ff9': '%f_nine',
    'TZHTZM': '%z',
    'tzhtzm': '%z',
    'TZH:TZM': '%:z',
    'tzh:tzm': '%:z',
    'TZH': '%-z',
    'tzh': '%-z',
    '"T"': 'T',
    'AM': '%p',
    'am': '%p',
    'PM': '%p',
    'pm': '%p',
  };

  @cache
  static get DATE_PART_MAPPING (): Record<string, string> {
    return {
      ...Dialect.DATE_PART_MAPPING,
      ISOWEEK: 'WEEKISO',
      EPOCH_SECOND: 'EPOCH_SECOND',
      EPOCH_SECONDS: 'EPOCH_SECOND',
    };
  }

  static PSEUDOCOLUMNS = new Set(['LEVEL']);

  canQuote (identifier: IdentifierExpr, options: { identify?: string | boolean } = {}): boolean {
    const {
      identify = 'safe',
    } = options;
    // This disables quoting DUAL in SELECT ... FROM DUAL, because Snowflake treats an
    // unquoted DUAL keyword in a special way and does not map it to a user-defined table
    return (
      super.canQuote(identifier, { identify })
        && !(
          identifier.parent instanceof TableExpr
          && !identifier.args.quoted
          && identifier.name.toLowerCase() === 'dual'
        )
    );
  }

  static JsonPathTokenizer = SnowflakeJsonPathTokenizer;
  static Tokenizer = SnowflakeTokenizer;
  static Parser = SnowflakeParser;
  static Generator = SnowflakeGenerator;
}

Dialect.register(Dialects.SNOWFLAKE, Snowflake);
