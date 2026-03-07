import { cache } from '../port_internals';
import type { Expression } from '../expressions';
import {
  ArrayContainsExpr,
  ArraySizeExpr,
  CastExpr,
  CreateExpr,
  CurrentTimestampExpr,
  DataTypeExpr,
  DataTypeExprKind,
  DateAddExpr,
  DateStrToDateExpr,
  DateSubExpr,
  DateToDiExpr,
  DiToDateExpr,
  IfExpr,
  ILikeExpr,
  IntervalExpr,
  LevenshteinExpr,
  PartitionedByPropertyExpr,
  PowExpr,
  PropertiesLocation,
  RegexpLikeExpr,
  SelectExpr,
  StrPositionExpr,
  StrToDateExpr,
  StrToTimeExpr,
  TimeStrToDateExpr,
  TimeStrToTimeExpr,
  TimeStrToUnixExpr,
  TimeToStrExpr,
  TimeToUnixExpr,
  ToCharExpr,
  TryCastExpr,
  TsOrDiToDiExpr,
  TsOrDsAddExpr,
  VarExpr,
  VolatilePropertyExpr,
} from '../expressions';
import {
  Generator, unsupportedArgs,
} from '../generator';
import { Parser } from '../parser';
import type { TokenType } from '../tokens';
import { Tokenizer } from '../tokens';
import {
  eliminateDistinctOn, eliminateSemiAndAntiJoins, moveSchemaColumnsToPartitionedBy, preprocess,
} from '../transforms';
import {
  buildFormattedTime,
  dateStrToDateSql,
  Dialect, Dialects,
  noTrycastSql,
  renameFunc,
  strPositionSql,
  timeStrToTimeSql,
} from './dialect';
import { dateAddSql } from './mysql';

class DrillTokenizer extends Tokenizer {
  static IDENTIFIERS = ['`'];
  static STRING_ESCAPES = ['\\'];
  @cache
  static get KEYWORDS (): Record<string, TokenType> {
    const keywords = { ...Tokenizer.KEYWORDS };
    delete keywords['/*+'];
    return keywords;
  }
}

class DrillParser extends Parser {
  static STRICT_CAST = false;
  static LOG_DEFAULTS_TO_LN = true;
  @cache
  static get FUNCTIONS (): Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression> {
    return {
      ...Parser.FUNCTIONS,
      REPEATED_COUNT: ArraySizeExpr.fromArgList,
      TO_TIMESTAMP: TimeStrToTimeExpr.fromArgList,
      TO_CHAR: buildFormattedTime(TimeToStrExpr, { dialect: 'drill' }),
      LEVENSHTEIN_DISTANCE: LevenshteinExpr.fromArgList,
    };
  }
}

class DrillGenerator extends Generator {
  static JOIN_HINTS = false;
  static TABLE_HINTS = false;
  static QUERY_HINTS = false;
  static NVL2_SUPPORTED = false;
  static LAST_DAY_SUPPORTS_DATE_PART = false;
  static SUPPORTS_CREATE_TABLE_LIKE = false;
  static ARRAY_SIZE_NAME = 'REPEATED_COUNT';

  @cache
  static get TYPE_MAPPING () {
    return {
      ...Generator.TYPE_MAPPING,
      [DataTypeExprKind.INT]: 'INTEGER',
      [DataTypeExprKind.SMALLINT]: 'INTEGER',
      [DataTypeExprKind.TINYINT]: 'INTEGER',
      [DataTypeExprKind.BINARY]: 'VARBINARY',
      [DataTypeExprKind.TEXT]: 'VARCHAR',
      [DataTypeExprKind.NCHAR]: 'VARCHAR',
      [DataTypeExprKind.TIMESTAMPLTZ]: 'TIMESTAMP',
      [DataTypeExprKind.TIMESTAMPTZ]: 'TIMESTAMP',
      [DataTypeExprKind.DATETIME]: 'TIMESTAMP',
    };
  }

  @cache
  static get PROPERTIES_LOCATION () {
    return new Map<typeof Expression, PropertiesLocation>([
      ...Generator.PROPERTIES_LOCATION,
      [PartitionedByPropertyExpr, PropertiesLocation.POST_SCHEMA],
      [VolatilePropertyExpr, PropertiesLocation.UNSUPPORTED],
    ]);
  }

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (this: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (this: Generator, e: any) => string>([
      ...Generator.TRANSFORMS,
      [CurrentTimestampExpr, () => 'CURRENT_TIMESTAMP'],
      [ArrayContainsExpr, renameFunc('REPEATED_CONTAINS')],
      [CreateExpr, preprocess([moveSchemaColumnsToPartitionedBy])],
      [DateAddExpr, dateAddSql('ADD')],
      [DateStrToDateExpr, dateStrToDateSql],
      [DateSubExpr, dateAddSql('SUB')],
      [
        DateToDiExpr,
        function (this: Generator, e) {
          return `CAST(TO_DATE(${this.sql(e, 'this')}, ${Drill.DATEINT_FORMAT}) AS INT)`;
        },
      ],
      [
        DiToDateExpr,
        function (this: Generator, e) {
          return `TO_DATE(CAST(${this.sql(e, 'this')} AS VARCHAR), ${Drill.DATEINT_FORMAT})`;
        },
      ],
      [
        IfExpr,
        function (this: Generator, e) {
          return `\`IF\`(${this.formatArgs([
            e.args.this,
            e.args.true,
            e.args.false,
          ])})`;
        },
      ],
      [
        ILikeExpr,
        function (this: Generator, e) {
          return this.binary(e, '`ILIKE`');
        },
      ],
      [
        LevenshteinExpr,
        function (this: Generator, e) {
          unsupportedArgs.call(this, e, 'insCost', 'delCost', 'subCost', 'maxDist');
          return renameFunc('LEVENSHTEIN_DISTANCE').call(this, e);
        },
      ],
      [
        PartitionedByPropertyExpr,
        function (this: Generator, e) {
          return `PARTITION BY ${this.sql(e, 'this')}`;
        },
      ],
      [RegexpLikeExpr, renameFunc('REGEXP_MATCHES')],
      [
        StrToDateExpr,
        function (this: Generator, e) {
          return (this as DrillGenerator).strToDate(e);
        },
      ],
      [PowExpr, renameFunc('POW')],
      [SelectExpr, preprocess([eliminateDistinctOn, eliminateSemiAndAntiJoins])],
      [StrPositionExpr, strPositionSql],
      [
        StrToTimeExpr,
        function (this: Generator, e) {
          return this.func('TO_TIMESTAMP', [e.args.this, this.formatTime(e)]);
        },
      ],
      [
        TimeStrToDateExpr,
        function (this: Generator, e) {
          return this.sql(new CastExpr({
            this: e.args.this,
            to: new DataTypeExpr({ this: DataTypeExprKind.DATE }),
          }));
        },
      ],
      [TimeStrToTimeExpr, timeStrToTimeSql],
      [TimeStrToUnixExpr, renameFunc('UNIX_TIMESTAMP')],
      [
        TimeToStrExpr,
        function (this: Generator, e) {
          return this.func('TO_CHAR', [e.args.this, this.formatTime(e)]);
        },
      ],
      [TimeToUnixExpr, renameFunc('UNIX_TIMESTAMP')],
      [
        ToCharExpr,
        function (this: Generator, e) {
          return this.functionFallbackSql(e);
        },
      ],
      [TryCastExpr, noTrycastSql],
      [
        TsOrDsAddExpr,
        function (this: Generator, e) {
          return `DATE_ADD(CAST(${this.sql(e, 'this')} AS DATE), ${this.sql(new IntervalExpr({
            this: e.args.expression,
            unit: new VarExpr({ this: 'DAY' }),
          }))})`;
        },
      ],
      [
        TsOrDiToDiExpr,
        function (this: Generator, e) {
          return `CAST(SUBSTR(REPLACE(CAST(${this.sql(e, 'this')} AS VARCHAR), '-', ''), 1, 8) AS INT)`;
        },
      ],
    ]);
    return transforms;
  }

  strToDate (expression: StrToDateExpr): string {
    const thisSql = this.sql(expression, 'this');
    const timeFormat = this.formatTime(expression);
    if (timeFormat === Drill.DATE_FORMAT) {
      return this.sql(new CastExpr({
        this: expression.args.this,
        to: new DataTypeExpr({ this: DataTypeExprKind.DATE }),
      }));
    }
    return this.func('TO_DATE', [thisSql, timeFormat]);
  }
}

export class Drill extends Dialect {
  static NORMALIZE_FUNCTIONS = false as const;
  static PRESERVE_ORIGINAL_NAMES = true;
  static NULL_ORDERING = 'nulls_are_last' as const;
  static DATE_FORMAT = '\'yyyy-MM-dd\'';
  static DATEINT_FORMAT = '\'yyyyMMdd\'';
  static TIME_FORMAT = '\'yyyy-MM-dd HH:mm:ss\'';
  static SUPPORTS_USER_DEFINED_TYPES = false;
  static SUPPORTS_SEMI_ANTI_JOIN = false;
  static TYPED_DIVISION = true;
  static CONCAT_COALESCE = true;

  static TIME_MAPPING = {
    'y': '%Y',
    'Y': '%Y',
    'YYYY': '%Y',
    'yyyy': '%Y',
    'YY': '%y',
    'yy': '%y',
    'MMMM': '%B',
    'MMM': '%b',
    'MM': '%m',
    'M': '%-m',
    'dd': '%d',
    'd': '%-d',
    'HH': '%H',
    'H': '%-H',
    'hh': '%I',
    'h': '%-I',
    'mm': '%M',
    'm': '%-M',
    'ss': '%S',
    's': '%-S',
    'SSSSSS': '%f',
    'a': '%p',
    'DD': '%j',
    'D': '%-j',
    'E': '%a',
    'EE': '%a',
    'EEE': '%a',
    'EEEE': '%A',
    '\'\'T\'\'': 'T',
  };

  static Tokenizer = DrillTokenizer;
  static Parser = DrillParser;
  static Generator = DrillGenerator;
}

Dialect.register(Dialects.DRILL, Drill);
