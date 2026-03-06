import { cache } from '../port_internals';
import {
  Generator,
  unsupportedArgs,
} from '../generator';
import {
  Parser, binaryRangeParser,
} from '../parser';
import {
  Tokenizer, TokenType,
} from '../tokens';
import type {
  Expression,
  DateAddExpr,
  CastExpr,
  TruncExpr,
  GenerateSeriesExpr,
  DateDiffExpr,
  GroupConcatExpr,
  LeastExpr,
  TransactionExpr,
  IsAsciiExpr,
  CurrentSchemaExpr,
  IgnoreNullsExpr,
  RespectNullsExpr,
  WindowSpecExpr,
  JsonExtractExpr,
} from '../expressions';
import {
  AnonymousExpr,
  TimeToStrExpr,
  TsOrDsToTimestampExpr,
  CreateExpr,
  SchemaExpr,
  ColumnDefExpr,
  PrimaryKeyExpr,
  PrimaryKeyColumnConstraintExpr,
  ColumnConstraintExpr,
  AutoIncrementColumnConstraintExpr,
  GeneratedAsIdentityColumnConstraintExpr,
  NotNullColumnConstraintExpr,
  MatchExpr,
  AttachExpr,
  DetachExpr,
  LevenshteinExpr,
  CurrentTimestampExpr,
  CurrentVersionExpr,
  DataTypeExprKind,
  AnyValueExpr,
  ChrExpr,
  ConcatExpr,
  CountIfExpr,
  CurrentDateExpr,
  CurrentTimeExpr,
  DateStrToDateExpr,
  IfExpr,
  ILikeExpr,
  LogicalOrExpr,
  LogicalAndExpr,
  PivotExpr,
  RandExpr,
  SelectExpr,
  StrPositionExpr,
  TableSampleExpr,
  TimeStrToTimeExpr,
  TryCastExpr,
  PropertiesLocation,
  LikePropertyExpr,
  TemporaryPropertyExpr,
  TableAliasExpr,
  DistinctExpr,
  OrderExpr,
  UniqueColumnConstraintExpr,
  IdentifierExpr,
  LiteralExpr,
  JsonPathKeyExpr,
  JsonPathRootExpr,
  JsonPathSubscriptExpr,
  JsonExtractScalarExpr,
  alias,
} from '../expressions';
import {
  eliminateDistinctOn, eliminateQualify, eliminateSemiAndAntiJoins, preprocess,
} from '../transforms';
import {
  anyValueToMaxSql,
  arrowJsonExtractSql,
  concatToDPipeSql,
  countIfToSum,
  noIlikeSql,
  noPivotSql,
  noTablesampleSql,
  noTrycastSql,
  strPositionSql,
  renameFunc,
  Dialect, NormalizationStrategy, Dialects,
} from './dialect';

function buildStrftime (args: Expression[]): AnonymousExpr | TimeToStrExpr {
  if (args.length === 1) {
    args.push(new CurrentTimestampExpr({}));
  }

  if (args.length === 2) {
    return new TimeToStrExpr({
      this: new TsOrDsToTimestampExpr({ this: args[1] }),
      format: args[0],
    });
  }

  return new AnonymousExpr({
    this: 'STRFTIME',
    expressions: args,
  });
}

function transformCreate (expression: Expression): Expression {
  /** Move primary key to a column and enforce auto_increment on primary keys. */
  const schema = (expression as CreateExpr).args.this;

  if (expression instanceof CreateExpr && schema instanceof SchemaExpr) {
    const defs: Record<string, ColumnDefExpr> = {};
    let primaryKey: PrimaryKeyExpr | undefined = undefined;

    for (const e of (schema.args.expressions || [])) {
      if (e instanceof ColumnDefExpr) {
        defs[e.name] = e;
      } else if (e instanceof PrimaryKeyExpr) {
        primaryKey = e;
      }
    }

    if (primaryKey && (primaryKey.args.expressions || []).length === 1) {
      const columnName = ((primaryKey.args.expressions || [])[0] as IdentifierExpr).name;
      const column = defs[columnName];

      if (!column.args.constraints) {
        column.setArgKey('constraints', []);
      }
      if (!column.args.constraints) {
        column.args.constraints = [];
      }
      column.args.constraints.push(
        new ColumnConstraintExpr({ kind: new PrimaryKeyColumnConstraintExpr() }),
      );

      schema.args.expressions = (schema.args.expressions || []).filter((e) => e !== primaryKey);
    } else {
      for (const column of Object.values(defs)) {
        let autoIncrement: ColumnConstraintExpr | undefined = undefined;

        for (const constraint of (column.args.constraints || [])) {
          const constraintExpr = constraint as ColumnConstraintExpr;
          if ((constraintExpr.args.kind as unknown) instanceof PrimaryKeyColumnConstraintExpr) {
            autoIncrement = undefined; // Reset if we hit a PK to stop processing this column
            break;
          }
          if ((constraintExpr.args.kind as unknown) instanceof AutoIncrementColumnConstraintExpr) {
            autoIncrement = constraintExpr;
          }
        }

        if (autoIncrement) {
          column.args.constraints = (column.args.constraints || []).filter((c) => c !== autoIncrement);
        }
      }
    }
  }

  return expression;
}

function generatedToAutoIncrement (expression: Expression): Expression {
  if (!(expression instanceof ColumnDefExpr)) {
    return expression;
  }

  const generated = expression.find(GeneratedAsIdentityColumnConstraintExpr);

  if (generated) {
    (generated.parent as ColumnConstraintExpr).pop();

    const notNull = expression.find(NotNullColumnConstraintExpr);
    if (notNull) {
      (notNull.parent as ColumnConstraintExpr).pop();
    }

    if (!expression.args.constraints) {
      expression.args.constraints = [];
    }
    expression.args.constraints.push(
      new ColumnConstraintExpr({ kind: new AutoIncrementColumnConstraintExpr() }),
    );
  }

  return expression;
}

class SQLiteTokenizer extends Tokenizer {
  static IDENTIFIERS: (string | [string, string])[] = [
    '"',
    ['[', ']'],
    '`',
  ];

  static HEX_STRINGS: [string, string][] = [
    ['x\'', '\''],
    ['X\'', '\''],
    ['0x', ''],
    ['0X', ''],
  ];

  static NESTED_COMMENTS = false;

  @cache
  static get ORIGINAL_KEYWORDS (): Record<string, TokenType> {
    const keywords: Record<string, TokenType> = {
      ...Tokenizer.KEYWORDS,
      'ATTACH': TokenType.ATTACH,
      'DETACH': TokenType.DETACH,
      'INDEXED BY': TokenType.INDEXED_BY,
      'MATCH': TokenType.MATCH,
    };
    delete keywords['/*+'];
    return keywords;
  }

  static COMMANDS = new Set([...Array.from(Tokenizer.COMMANDS), TokenType.REPLACE]);
}

class SQLiteParser extends Parser {
  static STRING_ALIASES = true;
  static ALTER_RENAME_REQUIRES_COLUMN = false;
  static JOINS_HAVE_EQUAL_PRECEDENCE = true;
  static ADD_JOIN_ON_TRUE = true;
  @cache
  static get FUNCTIONS (): Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression> {
    return {
      ...Parser.FUNCTIONS,
      EDITDIST3: LevenshteinExpr.fromArgList,
      STRFTIME: buildStrftime,
      DATETIME: (args: Expression[]) => new AnonymousExpr({
        this: 'DATETIME',
        expressions: args,
      }),
      TIME: (args: Expression[]) => new AnonymousExpr({
        this: 'TIME',
        expressions: args,
      }),
      SQLITE_VERSION: CurrentVersionExpr.fromArgList,
    };
  }

  @cache
  static get STATEMENT_PARSERS (): Partial<Record<TokenType, (self: Parser) => Expression | undefined>> {
    return {
      ...Parser.STATEMENT_PARSERS,
      [TokenType.ATTACH]: (self: Parser) => (self as SQLiteParser).parseAttachDetach(),
      [TokenType.DETACH]: (self: Parser) => (self as SQLiteParser).parseAttachDetach({ isAttach: false }),
    };
  }

  @cache
  static get RANGE_PARSERS (): Partial<Record<TokenType, (self: Parser, this_: Expression) => Expression | undefined>> {
    return {
      ...Parser.RANGE_PARSERS,
      [TokenType.MATCH]: binaryRangeParser(MatchExpr),
    };
  }

  parseUnique (): UniqueColumnConstraintExpr {
    // Do not consume more tokens if UNIQUE is used as a standalone constraint
    if (this._constructor.CONSTRAINT_PARSERS[this.curr?.text.toUpperCase() ?? '']) {
      return this.expression(UniqueColumnConstraintExpr);
    }

    return super.parseUnique();
  }

  parseAttachDetach (options: { isAttach?: boolean } = {}): AttachExpr | DetachExpr {
    const { isAttach = true } = options;
    this.match(TokenType.DATABASE);
    const thisNode = this.parseExpression();

    return isAttach
      ? this.expression(AttachExpr, { this: thisNode })
      : this.expression(DetachExpr, { this: thisNode });
  }
}

class SQLiteGenerator extends Generator {
  static JOIN_HINTS = false;
  static TABLE_HINTS = false;
  static QUERY_HINTS = false;
  static NVL2_SUPPORTED = false;
  static JSON_PATH_BRACKETED_KEY_SUPPORTED = false;
  static SUPPORTS_CREATE_TABLE_LIKE = false;
  static SUPPORTS_TABLE_ALIAS_COLUMNS = false;
  static SUPPORTS_TO_NUMBER = false;
  static SUPPORTS_WINDOW_EXCLUDE = true;
  static EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = false;
  static SUPPORTS_MEDIAN = false;
  static JSON_KEY_VALUE_PAIR_SEP = ',';
  static PARSE_JSON_NAME = undefined;

  @cache
  static get SUPPORTED_JSON_PATH_PARTS (): Set<typeof Expression> {
    return new Set([
      JsonPathKeyExpr,
      JsonPathRootExpr,
      JsonPathSubscriptExpr,
    ]);
  }

  @cache
  static get TYPE_MAPPING (): Map<DataTypeExprKind | string, string> {
    const mapping = new Map<DataTypeExprKind | string, string>([
      ...Generator.TYPE_MAPPING,
      [DataTypeExprKind.BOOLEAN, 'INTEGER'],
      [DataTypeExprKind.TINYINT, 'INTEGER'],
      [DataTypeExprKind.SMALLINT, 'INTEGER'],
      [DataTypeExprKind.INT, 'INTEGER'],
      [DataTypeExprKind.BIGINT, 'INTEGER'],
      [DataTypeExprKind.FLOAT, 'REAL'],
      [DataTypeExprKind.DOUBLE, 'REAL'],
      [DataTypeExprKind.DECIMAL, 'REAL'],
      [DataTypeExprKind.CHAR, 'TEXT'],
      [DataTypeExprKind.NCHAR, 'TEXT'],
      [DataTypeExprKind.VARCHAR, 'TEXT'],
      [DataTypeExprKind.NVARCHAR, 'TEXT'],
      [DataTypeExprKind.BINARY, 'BLOB'],
      [DataTypeExprKind.VARBINARY, 'BLOB'],
    ]);

    mapping.delete(DataTypeExprKind.BLOB);

    return mapping;
  }

  static TOKEN_MAPPING = {
    [TokenType.AUTO_INCREMENT]: 'AUTOINCREMENT',
  } as Record<TokenType, string>;

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (self: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (self: Generator, e: any) => string>([
      ...Generator.TRANSFORMS.entries(),
      [AnyValueExpr, anyValueToMaxSql],
      [ChrExpr, renameFunc('CHAR')],
      [ConcatExpr, concatToDPipeSql],
      [CountIfExpr, countIfToSum],
      [CreateExpr, preprocess([transformCreate])],
      [CurrentDateExpr, () => 'CURRENT_DATE'],
      [CurrentTimeExpr, () => 'CURRENT_TIME'],
      [CurrentTimestampExpr, () => 'CURRENT_TIMESTAMP'],
      [CurrentVersionExpr, () => 'SQLITE_VERSION()'],
      [ColumnDefExpr, preprocess([generatedToAutoIncrement])],
      [DateStrToDateExpr, (self: Generator, e: DateStrToDateExpr) => self.sql(e, 'this')],
      [IfExpr, renameFunc('IIF')],
      [ILikeExpr, noIlikeSql],
      [JsonExtractScalarExpr, (self: Generator, e: JsonExtractScalarExpr) => arrowJsonExtractSql(self, e)],
      [
        LevenshteinExpr,
        (self: Generator, e) => unsupportedArgs(
          'insCost',
          'delCost',
          'subCost',
          'maxDist',
        )(
          () => renameFunc('EDITDIST3')(self, e),
        )(e),
      ],
      [LogicalOrExpr, renameFunc('MAX')],
      [LogicalAndExpr, renameFunc('MIN')],
      [PivotExpr, noPivotSql],
      [RandExpr, renameFunc('RANDOM')],
      [
        SelectExpr,
        preprocess([
          eliminateDistinctOn,
          eliminateQualify,
          eliminateSemiAndAntiJoins,
        ]),
      ],
      [
        StrPositionExpr,
        (self: Generator, e: StrPositionExpr) =>
          strPositionSql(self, e, { funcName: 'INSTR' }),
      ],
      [TableSampleExpr, noTablesampleSql],
      [TimeStrToTimeExpr, (self: Generator, e: TimeStrToTimeExpr) => self.sql(e, 'this')],
      [
        TimeToStrExpr,
        (self: Generator, e: TimeToStrExpr) =>
          self.func('STRFTIME', [e.args.format, e.args.this]),
      ],
      [TryCastExpr, noTrycastSql],
      [TsOrDsToTimestampExpr, (self: Generator, e: TsOrDsToTimestampExpr) => self.sql(e, 'this')],
    ]);

    return transforms;
  }

  @cache
  static get PROPERTIES_LOCATION (): Map<typeof Expression, PropertiesLocation> {
    const locations = new Map<typeof Expression, PropertiesLocation>();

    // Initialize all base properties as UNSUPPORTED for SQLite
    for (const prop of Generator.PROPERTIES_LOCATION.keys()) {
      locations.set(prop, PropertiesLocation.UNSUPPORTED);
    }

    // Override specific supported properties
    locations.set(LikePropertyExpr, PropertiesLocation.POST_SCHEMA);
    locations.set(TemporaryPropertyExpr, PropertiesLocation.POST_CREATE);

    return locations;
  }

  static LIMIT_FETCH = 'LIMIT';

  jsonExtractSql (expression: JsonExtractExpr): string {
    if (expression.args.expressions && 0 < expression.args.expressions.length) {
      return this.functionFallbackSql(expression);
    }
    return arrowJsonExtractSql(this, expression);
  }

  dateAddSql (expression: DateAddExpr): string {
    const modifier = expression.args.expression;
    const modifierSql = modifier instanceof LiteralExpr && modifier.isString
      ? modifier.name
      : this.sql(modifier);

    const unit = expression.args.unit;
    const finalModifier = unit ? `'${modifierSql} ${unit.name}'` : `'${modifierSql}'`;

    return this.func('DATE', [expression.args.this, finalModifier]);
  }

  castSql (expression: CastExpr, options: { safePrefix?: string } = {}): string {
    if (expression.isType('date')) {
      return this.func('DATE', [expression.args.this]);
    }
    return super.castSql(expression, options);
  }

  @unsupportedArgs('decimals')
  truncSql (expression: TruncExpr): string {
    return this.func('TRUNC', [expression.args.this]);
  }

  generateSeriesSql (expression: GenerateSeriesExpr): string {
    const parent = expression.parent;
    const aliasExpr = parent?.args.alias;

    if (aliasExpr instanceof TableAliasExpr && aliasExpr.args.columns?.length) {
      const columnAlias = aliasExpr.args.columns[0];
      aliasExpr.setArgKey('columns', undefined);
      return this.sql(
        new SelectExpr({
          expressions: [alias('value', columnAlias)],
        })
          .from(expression)
          .subquery(),
      );
    }

    return this.functionFallbackSql(expression);
  }

  dateDiffSql (expression: DateDiffExpr): string {
    const unitNode = expression.args.unit;
    const unit = unitNode instanceof IdentifierExpr ? unitNode.name.toUpperCase() : 'DAY';

    let sql = `(JULIANDAY(${this.sql(expression, 'this')}) - JULIANDAY(${this.sql(expression, 'expression')}))`;

    const multipliers: Record<string, string> = {
      MONTH: ' / 30.0',
      YEAR: ' / 365.0',
      HOUR: ' * 24.0',
      MINUTE: ' * 1440.0',
      SECOND: ' * 86400.0',
      MILLISECOND: ' * 86400000.0',
      MICROSECOND: ' * 86400000000.0',
      NANOSECOND: ' * 8640000000000.0',
    };

    if (unit !== 'DAY') {
      const adjustment = multipliers[unit];
      if (adjustment) {
        sql = `${sql}${adjustment}`;
      } else {
        this.unsupported(`DATEDIFF unsupported for '${unit}'.`);
      }
    }

    return `CAST(${sql} AS INTEGER)`;
  }

  groupConcatSql (expression: GroupConcatExpr): string {
    let thisNode: Expression | undefined = expression.args.this;
    const distinct = expression.find(DistinctExpr);
    let distinctSql = '';

    if (distinct) {
      thisNode = distinct.args.expressions?.[0];
      distinctSql = 'DISTINCT ';
    }

    if (expression.args.this instanceof OrderExpr) {
      this.unsupported('SQLite GROUP_CONCAT doesn\'t support ORDER BY.');
      if (expression.args.this.args.this && !distinct) {
        thisNode = expression.args.this.args.this;
      }
    }

    const separator = expression.args.separator;
    return `GROUP_CONCAT(${distinctSql}${this.formatArgs([thisNode], { sep: separator })})`;
  }

  leastSql (expression: LeastExpr): string {
    if (expression.args.expressions && 0 < expression.args.expressions.length) {
      return renameFunc('MIN')(this, expression);
    }
    return this.sql(expression, 'this');
  }

  transactionSql (expression: TransactionExpr): string {
    const thisNode = expression.args.this;
    const thisPart = thisNode ? ` ${thisNode}` : '';
    return `BEGIN${thisPart} TRANSACTION`;
  }

  isAsciiSql (expression: IsAsciiExpr): string {
    return `(NOT ${this.sql(expression.args.this)} GLOB CAST(x'2a5b5e012d7f5d2a' AS TEXT))`;
  }

  @unsupportedArgs('this')
  currentSchemaSql (_expression: CurrentSchemaExpr): string {
    return '\'main\'';
  }

  ignoreNullsSql (expression: IgnoreNullsExpr): string {
    this.unsupported('SQLite does not support IGNORE NULLS.');
    return this.sql(expression.args.this);
  }

  respectNullsSql (expression: RespectNullsExpr): string {
    return this.sql(expression.args.this);
  }

  windowSpecSql (expression: WindowSpecExpr): string {
    if (
      expression.text('kind').toUpperCase() === 'RANGE'
      && expression.text('start').toUpperCase() === 'CURRENT ROW'
    ) {
      return 'RANGE CURRENT ROW';
    }

    return super.windowSpecSql(expression);
  }
}

export class SQLite extends Dialect {
  static NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE;
  static SUPPORTS_SEMI_ANTI_JOIN = false;
  static TYPED_DIVISION = true;
  static SAFE_DIVISION = true;
  static SAFE_TO_ELIMINATE_DOUBLE_NEGATION = false;

  static Tokenizer = SQLiteTokenizer;
  static Parser = SQLiteParser;
  static Generator = SQLiteGenerator;
}

Dialect.register(Dialects.SQLITE, SQLite);
