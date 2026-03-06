import { cache } from '../port_internals';
import type {
  CastExpr, UnnestExpr,
  ArrayExpr,
  AlterSetExpr,
  Expression,
} from '../expressions';
import {
  AnyValueExpr,
  ApproxDistinctExpr,
  ArrayConcatExpr,
  ConcatExpr,
  ConcatWsExpr,
  CountExpr,
  CurrentTimestampExpr,
  DataTypeExpr,
  DataTypeExprKind,
  DateAddExpr,
  DateDiffExpr,
  DistinctExpr,
  DistKeyPropertyExpr,
  DistStylePropertyExpr,
  ExplodeExpr,
  FarmFingerprintExpr,
  FromBaseExpr,
  FromExpr,
  GeneratedAsIdentityColumnConstraintExpr,
  GetbitExpr,
  GroupConcatExpr,
  HexExpr,
  isType,
  JoinExpr,
  JsonExtractExpr,
  JsonExtractScalarExpr,
  LastDayExpr,
  LiteralExpr,
  ParseJsonExpr,
  PivotExpr,
  RegexpExtractExpr,
  RoundExpr, SelectExpr, Sha2DigestExpr, Sha2Expr, SortKeyPropertyExpr, StartsWithExpr, StringToArrayExpr, TableSampleExpr, TryCastExpr, TsOrDsAddExpr, TsOrDsDiffExpr, UnixToTimeExpr, var_,
} from '../expressions';
import type { Generator } from '../generator';
import { seqGet } from '../helper';
import type { Parser } from '../parser';
import { buildConvertTimezone } from '../parser';
import { TokenType } from '../tokens';
import {
  eliminateDistinctOn, eliminateSemiAndAntiJoins, eliminateWindowClause, preprocess, unnestGenerateDateArrayUsingRecursiveCte, unqualifyUnnest,
} from '../transforms';
import {
  arrayConcatSql,
  concatToDPipeSql,
  concatWsToDPipeSql,
  Dialect,
  Dialects,
  generatedAsIdentityColumnConstraintSql,
  jsonExtractSegments,
  mapDatePart, NormalizationStrategy,
  noTablesampleSql,
  renameFunc,
} from './dialect';
import { Postgres } from './postgres';
import { dateDeltaSql } from './presto';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function buildDateDelta<T extends Expression> (ExprClass: new (args: any) => T) {
  return (args: Expression[]): T => {
    const expr = new ExprClass({
      this: seqGet(args, 2),
      expression: seqGet(args, 1),
      unit: mapDatePart(seqGet(args, 0)),
    });

    if (ExprClass === TsOrDsAddExpr as unknown) {
      expr.setArgKey('returnType', DataTypeExpr.build('TIMESTAMP'));
    }

    return expr;
  };
}

class RedshiftParser extends Postgres.Parser {
  @cache
  static get FUNCTIONS () {
    return (() => {
      const functions: Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression> = {
        ...Postgres.Parser.FUNCTIONS,
        ADD_MONTHS: (args: Expression[]) =>
          new TsOrDsAddExpr({
            this: seqGet(args, 0),
            expression: seqGet(args, 1),
            unit: var_('month'),
            returnType: DataTypeExpr.build('TIMESTAMP'),
          }),
        CONVERT_TIMEZONE: (args: Expression[]) => buildConvertTimezone(args, { defaultSourceTz: 'UTC' }),
        DATEADD: buildDateDelta(TsOrDsAddExpr),
        DATE_ADD: buildDateDelta(TsOrDsAddExpr),
        DATEDIFF: buildDateDelta(TsOrDsDiffExpr),
        DATE_DIFF: buildDateDelta(TsOrDsDiffExpr),
        GETDATE: CurrentTimestampExpr.fromArgList,
        LISTAGG: GroupConcatExpr.fromArgList,
        REGEXP_SUBSTR: (args: Expression[]) =>
          new RegexpExtractExpr({
            this: seqGet(args, 0),
            expression: seqGet(args, 1),
            position: seqGet(args, 2),
            occurrence: seqGet(args, 3),
            parameters: seqGet(args, 4),
          }),
        SPLIT_TO_ARRAY: (args: Expression[]) =>
          new StringToArrayExpr({
            this: seqGet(args, 0),
            expression: seqGet(args, 1) || LiteralExpr.string(','),
          }),
        STRTOL: FromBaseExpr.fromArgList,
      };
      delete functions['GET_BIT'];
      return functions;
    })();
  }

  @cache
  static get NO_PAREN_FUNCTION_PARSERS () {
    return {
      ...Postgres.Parser.NO_PAREN_FUNCTION_PARSERS,
      APPROXIMATE: (self: Parser) => (self as RedshiftParser).parseApproximateCount(),
      SYSDATE: (self: Parser) => self.expression(CurrentTimestampExpr, { sysdate: true }),
    };
  }

  static SUPPORTS_IMPLICIT_UNNEST = true;

  parseTable (options: {
    schema?: boolean;
    joins?: boolean;
    aliasTokens?: Set<TokenType>;
    parseBracket?: boolean;
    isDbReference?: boolean;
    parsePartition?: boolean;
    consumePipe?: boolean;
  } = {}): Expression | undefined {
    const unpivot = this.match(TokenType.UNPIVOT);
    const table = super.parseTable(options);

    return unpivot
      ? this.expression(PivotExpr, {
        this: table,
        unpivot: true,
      })
      : table;
  }

  parseConvert (strict: boolean, options: {
    safe?: boolean;
  } = {}): Expression | undefined {
    const {
      safe,
    } = options;
    const to = this.parseTypes();
    this.match(TokenType.COMMA);
    const thisNode = this.parseBitwise();
    return this.expression(TryCastExpr, {
      this: thisNode,
      to: to,
      safe,
    });
  }

  parseApproximateCount (): ApproxDistinctExpr | undefined {
    const index = this.index - 1;
    const func = this.parseFunction();

    if (func instanceof CountExpr && func.args.this instanceof DistinctExpr) {
      return this.expression(ApproxDistinctExpr, {
        this: seqGet(func.args.this.args.expressions || [], 0),
      });
    }
    this.retreat(index);
    return undefined;
  }
}

class RedshiftTokenizer extends Postgres.Tokenizer {
  static BIT_STRINGS = [];
  static HEX_STRINGS = [];
  static STRING_ESCAPES = ['\\', '\''];

  static ORIGINAL_KEYWORDS = (() => {
    const keywords: Record<string, TokenType> = {
      ...Postgres.Tokenizer.KEYWORDS,
      '(+)': TokenType.JOIN_MARKER,
      'HLLSKETCH': TokenType.HLLSKETCH,
      'MINUS': TokenType.EXCEPT,
      'SUPER': TokenType.SUPER,
      'TOP': TokenType.TOP,
      'UNLOAD': TokenType.COMMAND,
      'VARBYTE': TokenType.VARBINARY,
      'BINARY VARYING': TokenType.VARBINARY,
    };
    delete keywords['VALUES'];
    return keywords;
  })();

  static SINGLE_TOKENS = (() => {
    const singleTokens: Record<string, TokenType> = { ...Postgres.Tokenizer.SINGLE_TOKENS };
    delete singleTokens['#'];
    return singleTokens;
  })();
}

class RedshiftGenerator extends Postgres.Generator {
  static LOCKING_READS_SUPPORTED = false;
  static QUERY_HINTS = false;
  static VALUES_AS_TABLE = false;
  static TZ_TO_WITH_TIME_ZONE = true;
  static NVL2_SUPPORTED = true;
  static LAST_DAY_SUPPORTS_DATE_PART = false;
  static CAN_IMPLEMENT_ARRAY_ANY = false;
  static MULTI_ARG_DISTINCT = true;
  static COPY_PARAMS_ARE_WRAPPED = false;
  static HEX_FUNC = 'TO_HEX';
  static PARSE_JSON_NAME = 'JSON_PARSE';
  static ARRAY_CONCAT_IS_VAR_LEN = false;
  static SUPPORTS_CONVERT_TIMEZONE = true;
  static EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = false;
  static SUPPORTS_MEDIAN = true;
  static ALTER_SET_TYPE = 'TYPE';
  static SUPPORTS_DECODE_CASE = true;
  static SUPPORTS_BETWEEN_FLAGS = false;
  static LIMIT_FETCH = 'LIMIT';

  static WITH_PROPERTIES_PREFIX = ' ';

  static TYPE_MAPPING = {
    ...Postgres.Generator.TYPE_MAPPING,
    [DataTypeExprKind.BINARY]: 'VARBYTE',
    [DataTypeExprKind.BLOB]: 'VARBYTE',
    [DataTypeExprKind.INT]: 'INTEGER',
    [DataTypeExprKind.TIMETZ]: 'TIME',
    [DataTypeExprKind.TIMESTAMPTZ]: 'TIMESTAMP',
    [DataTypeExprKind.VARBINARY]: 'VARBYTE',
    [DataTypeExprKind.ROWVERSION]: 'VARBYTE',
  };

  static ORIGINAL_TRANSFORMS = (() => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (self: Generator, e: any) => string>([
      ...Postgres.Generator.TRANSFORMS,
      [ArrayConcatExpr, arrayConcatSql('ARRAY_CONCAT')],
      [ConcatExpr, concatToDPipeSql],
      [ConcatWsExpr, concatWsToDPipeSql],
      [
        ApproxDistinctExpr,
        (self: Generator, e: ApproxDistinctExpr) =>
          `APPROXIMATE COUNT(DISTINCT ${self.sql(e, 'this')})`,
      ],
      [
        CurrentTimestampExpr,
        (self: Generator, e: CurrentTimestampExpr) =>
          e.args.sysdate ? 'SYSDATE' : 'GETDATE()',
      ],
      [DateAddExpr, dateDeltaSql('DATEADD')],
      [DateDiffExpr, dateDeltaSql('DATEDIFF')],
      [DistKeyPropertyExpr, (self: Generator, e: DistKeyPropertyExpr) => self.func('DISTKEY', [e.args.this])],
      [DistStylePropertyExpr, (self: Generator, e: DistStylePropertyExpr) => (self as RedshiftGenerator).nakedProperty(e)],
      [ExplodeExpr, (self: Generator, e: ExplodeExpr) => (self as RedshiftGenerator).explodeSql(e)],
      [FarmFingerprintExpr, renameFunc('FARMFINGERPRINT64')],
      [FromBaseExpr, renameFunc('STRTOL')],
      [GeneratedAsIdentityColumnConstraintExpr, generatedAsIdentityColumnConstraintSql],
      [JsonExtractExpr, jsonExtractSegments('JSON_EXTRACT_PATH_TEXT')],
      [JsonExtractScalarExpr, jsonExtractSegments('JSON_EXTRACT_PATH_TEXT')],
      [GroupConcatExpr, renameFunc('LISTAGG')],
      [
        HexExpr,
        (self: Generator, e: HexExpr) =>
          self.func('UPPER', [self.func('TO_HEX', [self.sql(e, 'this')])]),
      ],
      [RegexpExtractExpr, renameFunc('REGEXP_SUBSTR')],
      [
        SelectExpr,
        preprocess([
          eliminateWindowClause,
          eliminateDistinctOn,
          eliminateSemiAndAntiJoins,
          unqualifyUnnest,
          unnestGenerateDateArrayUsingRecursiveCte,
        ]),
      ],
      [
        SortKeyPropertyExpr,
        (self: Generator, e: SortKeyPropertyExpr) =>
          `${e.args.compound ? 'COMPOUND ' : ''}SORTKEY(${self.formatArgs([e.args.this])})`,
      ],
      [
        StartsWithExpr,
        (self: Generator, e: StartsWithExpr) =>
          `${self.sql(e.args.this)} LIKE ${self.sql(e.args.expression)} || '%'`,
      ],
      [StringToArrayExpr, renameFunc('SPLIT_TO_ARRAY')],
      [TableSampleExpr, noTablesampleSql],
      [TsOrDsAddExpr, dateDeltaSql('DATEADD')],
      [TsOrDsDiffExpr, dateDeltaSql('DATEDIFF')],
      [UnixToTimeExpr, (self: Generator, e: UnixToTimeExpr) => (self as RedshiftGenerator).unixToTimeSql(e)],
      [
        Sha2DigestExpr,
        (self: Generator, e: Sha2DigestExpr) =>
          self.func('SHA2', [e.args.this, e.args.length || LiteralExpr.number(256)]),
      ],
    ]);

    transforms.delete(PivotExpr);
    transforms.delete(ParseJsonExpr);
    transforms.delete(AnyValueExpr);
    transforms.delete(LastDayExpr);
    transforms.delete(Sha2Expr);
    transforms.delete(GetbitExpr);
    transforms.delete(RoundExpr);

    return transforms;
  })();

  static RESERVED_KEYWORDS = new Set([
    'aes128',
    'aes256',
    'all',
    'allowoverwrite',
    'analyse',
    'analyze',
    'and',
    'any',
    'array',
    'as',
    'asc',
    'authorization',
    'az64',
    'backup',
    'between',
    'binary',
    'blanksasnull',
    'both',
    'bytedict',
    'bzip2',
    'case',
    'cast',
    'check',
    'collate',
    'column',
    'constraint',
    'create',
    'credentials',
    'cross',
    'current_date',
    'current_time',
    'current_timestamp',
    'current_user',
    'current_user_id',
    'default',
    'deferrable',
    'deflate',
    'defrag',
    'delta',
    'delta32k',
    'desc',
    'disable',
    'distinct',
    'do',
    'else',
    'emptyasnull',
    'enable',
    'encode',
    'encrypt',
    'encryption',
    'end',
    'except',
    'explicit',
    'false',
    'for',
    'foreign',
    'freeze',
    'from',
    'full',
    'globaldict256',
    'globaldict64k',
    'grant',
    'group',
    'gzip',
    'having',
    'identity',
    'ignore',
    'ilike',
    'in',
    'initially',
    'inner',
    'intersect',
    'interval',
    'into',
    'is',
    'isnull',
    'join',
    'leading',
    'left',
    'like',
    'limit',
    'localtime',
    'localtimestamp',
    'lun',
    'luns',
    'lzo',
    'lzop',
    'minus',
    'mostly16',
    'mostly32',
    'mostly8',
    'natural',
    'new',
    'not',
    'notnull',
    'null',
    'nulls',
    'off',
    'offline',
    'offset',
    'oid',
    'old',
    'on',
    'only',
    'open',
    'or',
    'order',
    'outer',
    'overlaps',
    'parallel',
    'partition',
    'percent',
    'permissions',
    'pivot',
    'placing',
    'primary',
    'raw',
    'readratio',
    'recover',
    'references',
    'rejectlog',
    'resort',
    'respect',
    'restore',
    'right',
    'select',
    'session_user',
    'similar',
    'snapshot',
    'some',
    'sysdate',
    'system',
    'table',
    'tag',
    'tdes',
    'text255',
    'text32k',
    'then',
    'timestamp',
    'to',
    'top',
    'trailing',
    'true',
    'truncatecolumns',
    'type',
    'union',
    'unique',
    'unnest',
    'unpivot',
    'user',
    'using',
    'verbose',
    'wallet',
    'when',
    'where',
    'with',
    'without',
  ]);

  unnestSql (expression: UnnestExpr): string {
    const args = expression.args.expressions || [];
    const numArgs = args.length;

    if (numArgs !== 1) {
      this.unsupported(`Unsupported number of arguments in UNNEST: ${numArgs}`);
      return '';
    }

    if (expression.findAncestor(SelectExpr) && !expression.findAncestor<FromExpr | JoinExpr>(FromExpr, JoinExpr)) {
      this.unsupported('Unsupported UNNEST when not used in FROM/JOIN clauses');
      return '';
    }

    const arg = this.sql(seqGet(args, 0));

    const alias = this.expressions(expression.args.alias, {
      key: 'columns',
      flat: true,
    });
    return alias ? `${arg} AS ${alias}` : arg;
  }

  castSql (expression: CastExpr, options: { safePrefix?: string } = {}): string {
    const { safePrefix } = options;
    if (isType(expression.args.to, DataTypeExprKind.JSON)) {
      // Redshift doesn't support a JSON type, so casting to it is treated as a noop
      return this.sql(expression.args.this);
    }

    return super.castSql(expression, { safePrefix });
  }

  /**
   * Redshift converts the `TEXT` data type to `VARCHAR(255)` by default when people more generally mean
   * VARCHAR of max length which is `VARCHAR(max)` in Redshift. Therefore if we get a `TEXT` data type
   * without precision we convert it to `VARCHAR(max)` and if it does have precision then we just convert
   * `TEXT` to `VARCHAR`.
   */
  dataTypeSql (expression: DataTypeExpr): string {
    if (expression.isType(DataTypeExprKind.TEXT)) {
      expression.setArgKey('this', DataTypeExprKind.VARCHAR);
      const precision = expression.args.expressions;

      if (!precision || precision.length === 0) {
        expression.append('expressions', var_('MAX'));
      }
    }

    return super.dataTypeSql(expression);
  }

  alterSetSql (expression: AlterSetExpr): string {
    let exprs = this.expressions(expression, { flat: true });
    exprs = exprs ? ` TABLE PROPERTIES (${exprs})` : '';

    let location = this.sql(expression, 'location');
    location = location ? ` LOCATION ${location}` : '';

    let fileFormat = this.expressions(expression, {
      key: 'file_format',
      flat: true,
      sep: ' ',
    });
    fileFormat = fileFormat ? ` FILE FORMAT ${fileFormat}` : '';

    return `SET${exprs}${location}${fileFormat}`;
  }

  arraySql (expression: ArrayExpr): string {
    if (expression.args.bracketNotation) {
      return super.arraySql(expression);
    }

    return renameFunc('ARRAY')(this, expression);
  }

  explodeSql (_expression: ExplodeExpr): string {
    this.unsupported('Unsupported EXPLODE() function');
    return '';
  }

  unixToTimeSql (expression: UnixToTimeExpr): string {
    const scale = expression.args.scale;
    let thisSql = this.sql(expression.args.this);

    if (scale !== undefined && scale !== UnixToTimeExpr.SECONDS && (scale as LiteralExpr).isNumber) {
      thisSql = `(${thisSql} / POWER(10, ${(scale as LiteralExpr).args.this}))`;
    }

    return `(TIMESTAMP 'epoch' + ${thisSql} * INTERVAL '1 SECOND')`;
  }
}

export class Redshift extends Postgres {
  static NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE;

  static SUPPORTS_USER_DEFINED_TYPES = false;
  static INDEX_OFFSET = 0;
  static COPY_PARAMS_ARE_CSV = false;
  static HEX_LOWERCASE = true;
  static HAS_DISTINCT_ARRAY_CONSTRUCTORS = true;
  static COALESCE_COMPARISON_NON_STANDARD = true;
  static REGEXP_EXTRACT_POSITION_OVERFLOW_RETURNS_NULL = false;
  static ARRAY_FUNCS_PROPAGATES_NULLS = true;

  static TIME_FORMAT = '\'YYYY-MM-DD HH24:MI:SS\'';

  static TIME_MAPPING = {
    ...Postgres.TIME_MAPPING,
    MON: '%b',
    HH24: '%H',
    HH: '%I',
  };

  static Tokenizer = RedshiftTokenizer;
  static Parser = RedshiftParser;
  static Generator = RedshiftGenerator;
}

Dialect.register(Dialects.REDSHIFT, Redshift);
