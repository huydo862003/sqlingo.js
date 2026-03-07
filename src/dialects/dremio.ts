import { cache } from '../port_internals';
import {
  Generator,
} from '../generator';
import { Parser } from '../parser';
import type { TokenPair } from '../tokens';
import {
  Tokenizer, TokenType,
} from '../tokens';
import type { Expression } from '../expressions';
import {
  DateAddExpr,
  DateSubExpr,
  isType,
  TimeToStrExpr,
  DateExpr,
  GenerateSeriesExpr,
  BitwiseAndAggExpr,
  BitwiseOrAggExpr,
  RegexpLikeExpr,
  RepeatExpr,
  ExtractExpr,
  ToCharExpr,
  TryCastExpr,
  TsOrDsToDateExpr,
  CastExpr,
  AtTimeZoneExpr,
  CurrentTimestampExpr,
  DataTypeExpr,
  DataTypeExprKind,
  LiteralExpr,
  IntervalExpr,
  ConcatExpr,
} from '../expressions';
import { seqGet } from '../helper';
import type { DialectType } from './dialect';
import {
  buildFormattedTime,
  buildDateDelta,
  noTrycastSql,
  Dialect, Dialects,
  buildTimeToStrOrToChar,
  renameFunc,
} from './dialect';

type DateDeltaType = DateAddExpr | DateSubExpr;

function dateDeltaSql (name: string): (this: Generator, expression: DateDeltaType) => string {
  return function (this: Generator, expression: DateDeltaType): string {
    const unit = expression.text('unit').toUpperCase();

    // Fallback to default behavior if unit is missing or 'DAY'
    if (!unit || unit === 'DAY') {
      return this.func(name, [expression.args.this, expression.args.expression]);
    }

    const thisSql = this.sql(expression, 'this');
    const exprSql = this.sql(expression, 'expression');

    const intervalSql = `CAST(${exprSql} AS INTERVAL ${unit})`;
    return `${name}(${thisSql}, ${intervalSql})`;
  };
}

function toCharIsNumericHandler (args: Expression[], { dialect }: { dialect: DialectType }): TimeToStrExpr | ToCharExpr {
  const expression = buildTimeToStrOrToChar(args, { dialect });
  const fmt = seqGet(args, 1);

  if (
    fmt
    && expression instanceof ToCharExpr
    && fmt instanceof LiteralExpr
    && fmt.isString
    && fmt.name.includes('#')
  ) {
    // Only mark as numeric if format is a literal containing #
    expression.setArgKey('isNumeric', true);
  }

  return expression;
}

function buildDateDeltaWithCastInterval (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expressionClass: new (args: any) => DateDeltaType,
): (args: Expression[]) => Expression {
  const fallbackBuilder = buildDateDelta(expressionClass);

  return (args: Expression[]): Expression => {
    if (args.length === 2) {
      const [dateArg, intervalArg] = args;

      if (
        intervalArg instanceof CastExpr
        && intervalArg.args.to instanceof DataTypeExpr
        && intervalArg.args.to.args.this instanceof IntervalExpr
      ) {
        const interval = intervalArg.args.to.args.this;
        return new expressionClass({
          this: dateArg,
          expression: intervalArg.args.this,
          unit: interval.args.unit,
        });
      }

      return new expressionClass({
        this: dateArg,
        expression: intervalArg,
      });
    }

    return fallbackBuilder(args);
  };
}

function dateTypeHandler (args: Expression[], { dialect }: { dialect: DialectType }): Expression {
  const [
    year,
    month,
    day,
  ] = args;

  if (
    year instanceof LiteralExpr && year.isNumber
    && month instanceof LiteralExpr && month.isNumber
    && day instanceof LiteralExpr && day.isNumber
  ) {
    const y = parseInt(year.args.this ?? '0').toString()
      .padStart(4, '0');
    const m = parseInt(month.args.this ?? '0').toString()
      .padStart(2, '0');
    const d = parseInt(day.args.this ?? '0').toString()
      .padStart(2, '0');

    const dateStr = `${y}-${m}-${d}`;
    return new DateExpr({ this: LiteralExpr.string(dateStr) });
  }

  const resolvedDialect = Dialect.getOrRaise(dialect);

  return new CastExpr({
    this: new ConcatExpr({
      expressions: [
        year,
        LiteralExpr.string('-'),
        month,
        LiteralExpr.string('-'),
        day,
      ],
      coalesce: resolvedDialect._constructor.CONCAT_COALESCE,
    }),
    to: DataTypeExpr.build('DATE'),
  });
}

class DremioTokenizer extends Tokenizer {
  static COMMENTS = [
    '--',
    '//',
    ['/*', '*/'] as TokenPair,
  ];
};

class DremioParser extends Parser {
  static LOG_DEFAULTS_TO_LN = true;
  @cache
  static get NO_PAREN_FUNCTION_PARSERS (): Partial<Record<string, (this: Parser) => Expression | undefined>> {
    return {
      ...Parser.NO_PAREN_FUNCTION_PARSERS,
      CURRENT_DATE_UTC: function (this: Parser) {
        return (this as DremioParser).parseCurrentDateUtc();
      },
    };
  }

  @cache
  static get FUNCTIONS (): Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression> {
    return {
      ...Parser.FUNCTIONS,
      ARRAY_GENERATE_RANGE: GenerateSeriesExpr.fromArgList,
      BIT_AND: BitwiseAndAggExpr.fromArgList,
      BIT_OR: BitwiseOrAggExpr.fromArgList,
      DATE_ADD: buildDateDeltaWithCastInterval(DateAddExpr),
      DATE_FORMAT: buildFormattedTime(TimeToStrExpr, { dialect: 'dremio' }),
      DATE_SUB: buildDateDeltaWithCastInterval(DateSubExpr),
      REGEXP_MATCHES: RegexpLikeExpr.fromArgList,
      REPEATSTR: RepeatExpr.fromArgList,
      TO_CHAR: toCharIsNumericHandler,
      TO_DATE: buildFormattedTime(TsOrDsToDateExpr, { dialect: 'dremio' }),
      DATE_PART: ExtractExpr.fromArgList,
      DATETYPE: dateTypeHandler,
    };
  }

  parseCurrentDateUtc (): CastExpr {
    if (this.match(TokenType.L_PAREN)) {
      this.matchRParen();
    }

    return new CastExpr({
      this: new AtTimeZoneExpr({
        this: new CurrentTimestampExpr({}),
        zone: LiteralExpr.string('UTC'),
      }),
      to: DataTypeExpr.build('DATE'),
    });
  }
}

class DremioGenerator extends Generator {
  static NVL2_SUPPORTED = false;
  static SUPPORTS_CONVERT_TIMEZONE = true;
  static INTERVAL_ALLOWS_PLURAL_FORM = false;
  static JOIN_HINTS = false;
  static LIMIT_ONLY_LITERALS = true;
  static MULTI_ARG_DISTINCT = false;
  static SUPPORTS_BETWEEN_FLAGS = true;

  @cache
  static get TYPE_MAPPING () {
    return {
      ...Generator.TYPE_MAPPING,
      [DataTypeExprKind.SMALLINT]: 'INT',
      [DataTypeExprKind.TINYINT]: 'INT',
      [DataTypeExprKind.BINARY]: 'VARBINARY',
      [DataTypeExprKind.TEXT]: 'VARCHAR',
      [DataTypeExprKind.NCHAR]: 'VARCHAR',
      [DataTypeExprKind.CHAR]: 'VARCHAR',
      [DataTypeExprKind.TIMESTAMPNTZ]: 'TIMESTAMP',
      [DataTypeExprKind.DATETIME]: 'TIMESTAMP',
      [DataTypeExprKind.ARRAY]: 'LIST',
      [DataTypeExprKind.BIT]: 'BOOLEAN',
    };
  }

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (this: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (this: Generator, e: any) => string>([
      ...Generator.TRANSFORMS,
      [BitwiseAndAggExpr, renameFunc('BIT_AND')],
      [BitwiseOrAggExpr, renameFunc('BIT_OR')],
      [ToCharExpr, renameFunc('TO_CHAR')],
      [
        TimeToStrExpr,
        function (this: Generator, e) {
          return this.func('TO_CHAR', [e.args.this, this.formatTime(e)]);
        },
      ],
      [TryCastExpr, noTrycastSql],
      [DateAddExpr, dateDeltaSql('DATE_ADD')],
      [DateSubExpr, dateDeltaSql('DATE_SUB')],
      [GenerateSeriesExpr, renameFunc('ARRAY_GENERATE_RANGE')],
    ]);
    return transforms;
  }

  dataTypeSql (expression: DataTypeExpr): string {
    if (expression.isType([DataTypeExprKind.TIMESTAMPTZ, DataTypeExprKind.TIMESTAMPLTZ])) {
      this.unsupported('Dremio does not support time-zone-aware TIMESTAMP');
    }
    return super.dataTypeSql(expression);
  }

  castSql (expression: CastExpr, options: { safePrefix?: string } = {}): string {
    const { safePrefix } = options;

    // Match: CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS DATE)
    if (isType(expression.args.to, DataTypeExprKind.DATE)) {
      const atTimeZone = expression.args.this;

      if (
        atTimeZone instanceof AtTimeZoneExpr
        && atTimeZone.args.this instanceof CurrentTimestampExpr
        && atTimeZone.args.zone instanceof LiteralExpr
        && atTimeZone.text('zone').toUpperCase() === 'UTC'
      ) {
        return 'CURRENT_DATE_UTC';
      }
    }

    return super.castSql(expression, { safePrefix });
  }
}

export class Dremio extends Dialect {
  static SUPPORTS_USER_DEFINED_TYPES = false;
  static CONCAT_COALESCE = true;
  static TYPED_DIVISION = true;
  static SUPPORTS_SEMI_ANTI_JOIN = false;
  static NULL_ORDERING = 'nulls_are_last' as const;
  static SUPPORTS_VALUES_DEFAULT = false;

  static TIME_MAPPING = {
    YYYY: '%Y',
    yyyy: '%Y',
    YY: '%y',
    yy: '%y',
    MM: '%m',
    mm: '%m',
    MON: '%b',
    mon: '%b',
    MONTH: '%B',
    month: '%B',
    DDD: '%j',
    ddd: '%j',
    DD: '%d',
    dd: '%d',
    DY: '%a',
    dy: '%a',
    DAY: '%A',
    day: '%A',
    HH24: '%H',
    hh24: '%H',
    HH12: '%I',
    hh12: '%I',
    HH: '%I',
    hh: '%I',
    MI: '%M',
    mi: '%M',
    SS: '%S',
    ss: '%S',
    FFF: '%f',
    fff: '%f',
    AMPM: '%p',
    ampm: '%p',
    WW: '%W',
    ww: '%W',
    D: '%w',
    d: '%w',
    CC: '%C',
    cc: '%C',
    TZD: '%Z',
    tzd: '%Z',
    TZO: '%z',
    tzo: '%z',
  };

  static Tokenizer = DremioTokenizer;
  static Parser = DremioParser;
  static Generator = DremioGenerator;
}

Dialect.register(Dialects.DREMIO, Dremio);
