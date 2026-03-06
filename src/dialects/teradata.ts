import { cache } from '../port_internals';
import {
  Generator,
} from '../generator';
import { Parser } from '../parser';
import type { TokenPair } from '../tokens';
import {
  Tokenizer, TokenType,
} from '../tokens';
import type {
  Expression,
  DataTypeExpr,
  UpdateExpr,
  TryCastExpr,
  FuncExpr,
  JsonExtractExpr,
} from '../expressions';
import {
  Md5Expr,
  CurrentDateExpr,
  CurrentTimeExpr,
  CurrentTimestampExpr,
  LogExpr,
  ModExpr,
  SelectExpr,
  StrToTimeExpr,
  TableSampleExpr,
  TimeToStrExpr,
  DataTypeExprKind,
  ArgMaxExpr,
  ArgMinExpr,
  MaxExpr,
  MinExpr,
  PowExpr,
  RandExpr,
  StrPositionExpr,
  StrToDateExpr,
  ToCharExpr,
  ToNumberExpr,
  UseExpr,
  QuarterExpr,
  LockingStatementExpr,
  RangeNExpr,
  LiteralExpr,
  NegExpr,
  IntervalExpr,
  PropertiesLocation,
  OnCommitPropertyExpr,
  PartitionedByPropertyExpr,
  StabilityPropertyExpr,
  ExtractExpr,
  DateAddExpr,
  DateSubExpr,
  AnonymousExpr,
  TranslateCharactersExpr,
  VarExpr,
  cast,
  QueryBandExpr,
} from '../expressions';
import { seqGet } from '../helper';
import {
  eliminateDistinctOn, eliminateSemiAndAntiJoins, preprocess,
} from '../transforms';
import {
  noTablesampleSql,
  maxOrGreatest,
  minOrLeast,
  strPositionSql,
  toNumberWithNlsParam,
  Dialect, NormalizationStrategy, Dialects,
  renameFunc,
} from './dialect';

function dateAddSqlTd (
  kind: '+' | '-',
): (self: Generator, expression: DateAddExpr | DateSubExpr) => string {
  return function (self: Generator, expression: DateAddExpr | DateSubExpr): string {
    const thisSql = self.sql(expression, 'this');
    const unit = expression.args.unit;
    let value = expression.args.expression && self.simplifyUnlessLiteral(expression.args.expression);

    if (!(value instanceof LiteralExpr)) {
      self.unsupported('Cannot add non literal');
    }

    let kindToOp: string;
    if (value instanceof NegExpr) {
      kindToOp = kind === '+' ? '-' : '+';
      value = LiteralExpr.string((value.args.this as LiteralExpr).toValue());
    } else {
      kindToOp = kind;
      (value as LiteralExpr).setArgKey('isString', true);
    }

    return `${thisSql} ${kindToOp} ${self.sql(new IntervalExpr({
      this: value,
      unit,
    }))}`;
  };
}

export class TeradataTokenizer extends Tokenizer {
  static HEX_STRINGS: TokenPair[] = [
    ['X\'', '\''],
    ['x\'', '\''],
    ['0x', ''],
  ];

  @cache
  static get ORIGINAL_KEYWORDS (): Record<string, TokenType> {
    const keywords: Record<string, TokenType> = {
      ...Tokenizer.KEYWORDS,
      '**': TokenType.DSTAR,
      '^=': TokenType.NEQ,
      'BYTEINT': TokenType.SMALLINT,
      'COLLECT': TokenType.COMMAND,
      'DEL': TokenType.DELETE,
      'EQ': TokenType.EQ,
      'GE': TokenType.GTE,
      'GT': TokenType.GT,
      'HELP': TokenType.COMMAND,
      'INS': TokenType.INSERT,
      'LE': TokenType.LTE,
      'LOCKING': TokenType.LOCK,
      'LT': TokenType.LT,
      'MINUS': TokenType.EXCEPT,
      'MOD': TokenType.MOD,
      'NE': TokenType.NEQ,
      'NOT=': TokenType.NEQ,
      'SAMPLE': TokenType.TABLE_SAMPLE,
      'SEL': TokenType.SELECT,
      'ST_GEOMETRY': TokenType.GEOMETRY,
      'TOP': TokenType.TOP,
      'UPD': TokenType.UPDATE,
    };
    delete keywords['/*+'];
    return keywords;
  };

  @cache
  static get SINGLE_TOKENS (): Record<string, TokenType> {
    const tokens = { ...Tokenizer.SINGLE_TOKENS };
    delete tokens['%'];
    return tokens;
  }
}

export class TeradataParser extends Parser {
  static TABLESAMPLE_CSV = true;
  static VALUES_FOLLOWED_BY_PAREN = false;

  static CHARSET_TRANSLATORS = new Set([
    'GRAPHIC_TO_KANJISJIS',
    'GRAPHIC_TO_LATIN',
    'GRAPHIC_TO_UNICODE',
    'GRAPHIC_TO_UNICODE_PadSpace',
    'KANJI1_KanjiEBCDIC_TO_UNICODE',
    'KANJI1_KanjiEUC_TO_UNICODE',
    'KANJI1_KANJISJIS_TO_UNICODE',
    'KANJI1_SBC_TO_UNICODE',
    'KANJISJIS_TO_GRAPHIC',
    'KANJISJIS_TO_LATIN',
    'KANJISJIS_TO_UNICODE',
    'LATIN_TO_GRAPHIC',
    'LATIN_TO_KANJISJIS',
    'LATIN_TO_UNICODE',
    'LOCALE_TO_UNICODE',
    'UNICODE_TO_GRAPHIC',
    'UNICODE_TO_GRAPHIC_PadGraphic',
    'UNICODE_TO_GRAPHIC_VarGraphic',
    'UNICODE_TO_KANJI1_KanjiEBCDIC',
    'UNICODE_TO_KANJI1_KanjiEUC',
    'UNICODE_TO_KANJI1_KANJISJIS',
    'UNICODE_TO_KANJI1_SBC',
    'UNICODE_TO_KANJISJIS',
    'UNICODE_TO_LATIN',
    'UNICODE_TO_LOCALE',
    'UNICODE_TO_UNICODE_FoldSpace',
    'UNICODE_TO_UNICODE_Fullwidth',
    'UNICODE_TO_UNICODE_Halfwidth',
    'UNICODE_TO_UNICODE_NFC',
    'UNICODE_TO_UNICODE_NFD',
    'UNICODE_TO_UNICODE_NFKC',
    'UNICODE_TO_UNICODE_NFKD',
  ]);

  @cache
  static get FUNC_TOKENS (): Set<TokenType> {
    return (() => {
      const tokens = new Set(Parser.FUNC_TOKENS);
      tokens.delete(TokenType.REPLACE);
      return tokens;
    })();
  }

  @cache
  static get STATEMENT_PARSERS (): Record<TokenType | string, (self: Parser) => Expression | Expression[] | undefined> {
    return {
      ...Parser.STATEMENT_PARSERS,
      [TokenType.DATABASE]: (self: Parser) => self.expression(
        UseExpr,
        { this: self.parseTable({ schema: false }) },
      ),
      [TokenType.REPLACE]: (self: Parser) => self.parseCreate(),
      [TokenType.LOCK]: (self: Parser) => (self as TeradataParser).parseLockingStatement(),
    };
  }

  @cache
  static get SET_PARSERS (): Record<string, (self: Parser) => Expression | undefined> {
    return {
      ...Parser.SET_PARSERS,
      QUERY_BAND: (self: Parser) => (self as TeradataParser).parseQueryBand(),
    };
  }

  @cache
  static get FUNCTION_PARSERS (): Partial<Record<string, (self: Parser) => Expression | undefined>> {
    return {
      ...Parser.FUNCTION_PARSERS,
      TRYCAST: Parser.FUNCTION_PARSERS['TRY_CAST'],
      RANGE_N: (self: Parser) => (self as TeradataParser).parseRangeN(),
      TRANSLATE: (self: Parser) => (self as TeradataParser).parseTranslate(),
    };
  }

  @cache
  static get FUNCTIONS (): Record<string, (args: Expression[]) => Expression> {
    return {
      ...Parser.FUNCTIONS,
      DATE: (args: Expression[]) => new CurrentDateExpr({ expressions: args }),
      HASHMD5: (args: Expression[]) => new Md5Expr({ this: seqGet(args, 0) }),
      LOG: (args: Expression[]) => new LogExpr({
        this: seqGet(args, 1),
        expression: seqGet(args, 0),
      }),
      TIME: (args: Expression[]) => new CurrentTimeExpr({ expressions: args }),
    };
  }

  @cache
  static get EXPONENT (): Partial<Record<TokenType, typeof Expression>> {
    return {
      ...Parser.EXPONENT,
      [TokenType.DSTAR]: PowExpr,
    };
  }

  public parseLockingStatement (): LockingStatementExpr {
    const lockingProperty = this.parseLocking();
    const wrappedQuery = this.parseSelect();

    if (!wrappedQuery) {
      this.raiseError('Expected SELECT statement after LOCKING clause');
    }

    return this.expression(LockingStatementExpr, {
      this: lockingProperty,
      expression: wrappedQuery,
    });
  }

  public parseRangeN (): RangeNExpr {
    const thisExpr = this.parseIdVar();
    this.match(TokenType.BETWEEN);

    const expressions = this.parseCsv(() => this.parseAssignment());
    const each = this.matchTextSeq('EACH') && this.parseAssignment();

    return this.expression(RangeNExpr, {
      this: thisExpr,
      expressions,
      each: each || undefined,
    });
  }

  public parseTranslate (): TranslateCharactersExpr {
    const thisExpr = this.parseAssignment();
    this.match(TokenType.USING);
    this.matchTexts(TeradataParser.CHARSET_TRANSLATORS);

    return this.expression(TranslateCharactersExpr, {
      this: thisExpr,
      expression: this.prev?.text.toUpperCase(),
      withError: this.matchTextSeq(['WITH', 'ERROR'])
        ? this.expression(AnonymousExpr, {
          this: 'WITH ERROR',
          expressions: [],
        })
        : undefined,
    });
  }

  public parseQueryBand (): Expression | undefined {
    // Parse: SET QUERY_BAND = 'key=value;key2=value2;' FOR SESSION|TRANSACTION
  // Also supports: SET QUERY_BAND = 'key=value;' UPDATE FOR SESSION|TRANSACTION
  // Also supports: SET QUERY_BAND = NONE FOR SESSION|TRANSACTION
    this.match(TokenType.EQ);

    let queryBandString: Expression | undefined;

    // Handle both string literals and NONE keyword
    if (this.matchTextSeq('NONE')) {
      queryBandString = new VarExpr({ this: 'NONE' });
    } else {
      queryBandString = this.parseString();
    }

    const update = this.matchTextSeq('UPDATE');
    this.matchTextSeq('FOR');

    // Handle scope - can be SESSION, TRANSACTION, VOLATILE, or SESSION VOLATILE
    let scope: string | undefined;

    if (this.matchTextSeq(['SESSION', 'VOLATILE'])) {
      scope = 'SESSION VOLATILE';
    } else if (this.matchTexts(['SESSION', 'TRANSACTION'])) {
      scope = this.prev?.text.toUpperCase();
    } else {
      scope = undefined;
    }

    return this.expression(QueryBandExpr, {
      this: queryBandString,
      scope: scope,
      update: update,
    });
  }
}

export class TeradataGenerator extends Generator {
  static LIMIT_IS_TOP = true;
  static JOIN_HINTS = false;
  static TABLE_HINTS = false;
  static QUERY_HINTS = false;
  static TABLESAMPLE_KEYWORDS = 'SAMPLE';
  static LAST_DAY_SUPPORTS_DATE_PART = false;
  static CAN_IMPLEMENT_ARRAY_ANY = true;
  static TZ_TO_WITH_TIME_ZONE = true;
  static ARRAY_SIZE_NAME = 'CARDINALITY';
  static NVL2_SUPPORTED = false;
  static SUPPORTS_TO_NUMBER = false;
  static EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = false;
  static SUPPORTS_MEDIAN = false;

  @cache
  static get TYPE_MAPPING (): Map<DataTypeExprKind | string, string> {
    const mapping = new Map(Generator.TYPE_MAPPING);
    mapping.set(DataTypeExprKind.BOOLEAN, 'BYTEINT');
    mapping.set(DataTypeExprKind.DOUBLE, 'DOUBLE PRECISION');
    mapping.set(DataTypeExprKind.TEXT, 'CLOB');
    mapping.set(DataTypeExprKind.GEOMETRY, 'ST_GEOMETRY');
    mapping.set(DataTypeExprKind.TIMESTAMPTZ, 'TIMESTAMP');
    return mapping;
  };

  @cache
  static get PROPERTIES_LOCATION (): Map<typeof Expression, PropertiesLocation> {
    const m = new Map(Generator.PROPERTIES_LOCATION);
    m.set(OnCommitPropertyExpr, PropertiesLocation.POST_INDEX);
    m.set(PartitionedByPropertyExpr, PropertiesLocation.POST_EXPRESSION);
    m.set(StabilityPropertyExpr, PropertiesLocation.POST_CREATE);
    return m;
  };

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (self: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const m = new Map<typeof Expression, (self: Generator, e: any) => string>(Generator.TRANSFORMS);
    m.set(ArgMaxExpr, renameFunc('MAX_BY'));
    m.set(ArgMinExpr, renameFunc('MIN_BY'));
    m.set(CurrentDateExpr, () => 'DATE');
    m.set(CurrentTimeExpr, () => 'TIME');
    m.set(CurrentTimestampExpr, () => 'CURRENT_TIMESTAMP');
    m.set(MaxExpr, (self, e) => maxOrGreatest(self, e as MaxExpr));
    m.set(MinExpr, (self, e) => minOrLeast(self, e as MinExpr));
    m.set(Md5Expr, renameFunc('HASHMD5'));
    m.set(ModExpr, (self, e) => (self as TeradataGenerator).binary(e as ModExpr, 'MOD'));
    m.set(PowExpr, (self, e) => self.binary(e as PowExpr, '**'));
    m.set(RandExpr, (self, e) => self.func('RANDOM', [(e as RandExpr).args.lower, (e as RandExpr).args.upper]));
    m.set(SelectExpr, preprocess([eliminateDistinctOn, eliminateSemiAndAntiJoins]));
    m.set(StrPositionExpr, (self, e) => strPositionSql(self, e as StrPositionExpr, {
      funcName: 'INSTR',
      supportsPosition: true,
      supportsOccurrence: true,
    }));
    m.set(StrToDateExpr, (self, e) => `CAST(${self.sql(e, 'this')} AS DATE FORMAT ${self.formatTime(e as StrToDateExpr)})`);
    m.set(StrToTimeExpr, (self, e) => self.castSql(e as StrToTimeExpr));
    m.set(TableSampleExpr, noTablesampleSql);
    m.set(TimeToStrExpr, (self, e) => self.func('TO_CHAR', [(e as TimeToStrExpr).args.this, self.formatTime(e as TimeToStrExpr)]));
    m.set(ToCharExpr, (self, e) => self.functionFallbackSql(e as FuncExpr | JsonExtractExpr));
    m.set(ToNumberExpr, toNumberWithNlsParam);
    m.set(UseExpr, (self, e) => `DATABASE ${self.sql(e, 'this')}`);
    m.set(DateAddExpr, (self, e) => dateAddSqlTd('+')(self, e as DateAddExpr));
    m.set(DateSubExpr, (self, e) => dateAddSqlTd('-')(self, e as DateSubExpr));
    m.set(QuarterExpr, (self, e) => self.sql(new ExtractExpr({
      this: 'QUARTER',
      expression: (e as QuarterExpr).args.this,
    })));
    return m;
  };

  public currentTimestampSql (expression: CurrentTimestampExpr): string {
    const prefix = expression.args.this ? '(' : '';
    const suffix = expression.args.this ? ')' : '';
    return this.func('CURRENT_TIMESTAMP', [expression.args.this], {
      prefix,
      suffix,
    });
  }

  public castSql (expression: Expression, options: { safePrefix?: string } = {}): string {
    const { safePrefix } = options;
    const to = (expression as TryCastExpr).args.to;
    if (to && (to as DataTypeExpr).args.this === DataTypeExprKind.UNKNOWN && (expression as TryCastExpr).args.format) {
      (to as DataTypeExpr).pop();
    }
    return super.castSql(expression, { safePrefix });
  }

  public tryCastSql (expression: TryCastExpr): string {
    return this.castSql(expression, { safePrefix: 'TRY' });
  }

  public tableSampleSql (expression: TableSampleExpr, _options: { tablesampleKeyword?: string } = {}): string {
    return `${this.sql(expression, 'this')} SAMPLE ${this.expressions(expression)}`;
  }

  public partitionedByPropertySql (expression: PartitionedByPropertyExpr): string {
    return `PARTITION BY ${this.sql(expression, 'this')}`;
  }

  public updateSql (expression: UpdateExpr): string {
    const thisSql = this.sql(expression, 'this');
    const fromSql = this.sql(expression, 'from') ? ` ${this.sql(expression, 'from')}` : '';
    const setSql = this.expressions(expression, { flat: true });
    const whereSql = this.sql(expression, 'where');
    const sql = `UPDATE ${thisSql}${fromSql} SET ${setSql}${whereSql}`;
    return this.prependCtes(expression, sql);
  }

  public modSql (expression: ModExpr): string {
    return this.binary(expression, 'MOD');
  }

  public dataTypeSql (expression: DataTypeExpr): string {
    const typeSql = super.dataTypeSql(expression);
    const prefixSql = expression.args.prefix;
    return prefixSql ? `SYSUDTLIB.${typeSql}` : typeSql;
  }

  public rangeNSql (expression: RangeNExpr): string {
    const thisSql = this.sql(expression, 'this');
    const expressionsSql = this.expressions(expression);
    const eachSql = this.sql(expression, 'each');
    const eachStr = eachSql ? ` EACH ${eachSql}` : '';
    return `RANGE_N(${thisSql} BETWEEN ${expressionsSql}${eachStr})`;
  }

  public lockingStatementSql (expression: LockingStatementExpr): string {
    const lockingClause = this.sql(expression, 'this');
    const querySql = this.sql(expression, 'expression');
    return `${lockingClause} ${querySql}`;
  }

  public extractSql (expression: ExtractExpr): string {
    const thisSql = this.sql(expression, 'this');
    if (thisSql.toUpperCase() !== 'QUARTER') {
      return super.extractSql(expression);
    }

    const toChar = new AnonymousExpr({
      this: 'to_char',
      expressions: [expression.args.expression ?? '', LiteralExpr.string('Q')],
    });
    return this.sql(cast(toChar, DataTypeExprKind.INT));
  }

  public intervalSql (expression: IntervalExpr): string {
    const unit = expression.args.unit?.toString() ?? '';
    let multiplier = 0;

    if (unit.startsWith('WEEK')) {
      multiplier = 7;
    } else if (unit.startsWith('QUARTER')) {
      multiplier = 90;
    }

    if (multiplier) {
      const dayInterval = new IntervalExpr({
        this: expression.args.this,
        unit: new VarExpr({ this: 'DAY' }),
      });
      return `(${multiplier} * ${super.intervalSql(dayInterval)})`;
    }

    return super.intervalSql(expression);
  }

  public selectSql (expression: SelectExpr): string {
    const top = expression.args.limit;
    if (top && !expression.args.offset) {
      expression.setArgKey('limit', undefined);
      return `${super.selectSql(expression)} TOP ${this.sql(top)}`;
    }
    return super.selectSql(expression);
  }
}

export class Teradata extends Dialect {
  static NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE;
  static SUPPORTS_SEMI_ANTI_JOIN = false;
  static TYPED_DIVISION = true;
  static TIME_MAPPING = {
    YY: '%y',
    Y4: '%Y',
    YYYY: '%Y',
    M4: '%B',
    M3: '%b',
    M: '%-M',
    MI: '%M',
    MM: '%m',
    MMM: '%b',
    MMMM: '%B',
    D: '%-d',
    DD: '%d',
    D3: '%j',
    DDD: '%j',
    H: '%-H',
    HH: '%H',
    HH24: '%H',
    S: '%-S',
    SS: '%S',
    SSSSSS: '%f',
    E: '%a',
    EE: '%a',
    E3: '%a',
    E4: '%A',
    EEE: '%a',
    EEEE: '%A',
  };

  static Tokenizer = TeradataTokenizer;
  static Parser = TeradataParser;
  static Generator = TeradataGenerator;
}

Dialect.register(Dialects.TERADATA, Teradata);
