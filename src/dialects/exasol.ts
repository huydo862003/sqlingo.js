import { cache } from '../port_internals';
import {
  Generator,
  unsupportedArgs,
} from '../generator';
import { Parser } from '../parser';
import {
  Tokenizer, TokenType, type TokenPair,
} from '../tokens';
import {
  Expression,
  toIdentifier,
  SelectExpr,
  AliasExpr,
  IdentifierExpr,
  TableExpr,
  ColumnExpr,
  StarExpr,
  LiteralExpr,
  IfExpr,
  IsExpr,
  EqExpr,
  CastExpr,
  CollateExpr,
  IntervalExpr,
  NegExpr,
  BitwiseAndExpr,
  BitwiseOrExpr,
  BitwiseXorExpr,
  BitwiseNotExpr,
  BitwiseLeftShiftExpr,
  BitwiseRightShiftExpr,
  IntDivExpr,
  AllExpr,
  LevenshteinExpr,
  ShaExpr,
  Sha2Expr,
  Md5Expr,
  Md5DigestExpr,
  RegexpExtractExpr,
  RegexpReplaceExpr,
  VariancePopExpr,
  ApproxDistinctExpr,
  ToCharExpr,
  TsOrDsToDateExpr,
  TimeToStrExpr,
  TimeStrToTimeExpr,
  TimestampTruncExpr,
  StrToTimeExpr,
  CurrentUserExpr,
  AtTimeZoneExpr,
  StrPositionExpr,
  GroupConcatExpr,
  ModExpr,
  DateAddExpr,
  DateSubExpr,
  DateDiffExpr,
  TsOrDsAddExpr,
  TsOrDsDiffExpr,
  DateTruncExpr,
  DatetimeTruncExpr,
  DayOfWeekExpr,
  WeekOfYearExpr,
  QuarterExpr,
  LastDayExpr,
  DateExpr,
  TimestampExpr,
  CommentColumnConstraintExpr,
  SubstringIndexExpr,
  SystimestampExpr,
  ConvertTimezoneExpr,
  type RankExpr,
  type DataTypeExpr,
  DataTypeExprKind,
  type SelectExprArgs,
  null_,
} from '../expressions';
import { seqGet } from '../helper';
import {
  preprocess,
} from '../transforms';
import { buildScope } from '../optimizer/scope';
import {
  Dialect, Dialects, NormalizationStrategy,
  renameFunc,
  buildFormattedTime,
  buildTimeToStrOrToChar,
  buildDateDelta,
  binaryFromFunction,
  buildTrunc,
  groupConcatSql,
  strPositionSql,
  timeStrToTimeSql,
  noLastDaySql,
} from './dialect';

const DATE_UNITS = new Set([
  'DAY',
  'WEEK',
  'MONTH',
  'YEAR',
  'HOUR',
  'MINUTE',
  'SECOND',
]);

function sha2Sql (self: ExasolGenerator, expression: Sha2Expr): string {
  const length = expression.text('length');
  const funcName = length === '256' ? 'HASH_SHA256' : 'HASH_SHA512';
  return self.func(funcName, [expression.args.this]);
}

function dateDiffSql (self: ExasolGenerator, expression: DateDiffExpr | TsOrDsDiffExpr): string {
  const unit = expression.text('unit').toUpperCase() || 'DAY';

  if (!DATE_UNITS.has(unit)) {
    self.unsupported(`'${unit}' is not supported in Exasol.`);
    return self.functionFallbackSql(expression);
  }

  return self.func(`${unit}S_BETWEEN`, [expression.args.this, expression.args.expression]);
}

/**
 * https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/zeroifnull.htm
 */
function buildZeroIfNull (args: Expression[]): IfExpr {
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

/**
 * https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/nullifzero.htm
 */
function buildNullIfZero (args: Expression[]): IfExpr {
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

/**
 * https://docs.exasol.com/db/latest/sql/select.htm#:~:text=If%20you%20have,local.x%3E10
 */
function addLocalPrefixForAliases (expression: Expression): Expression {
  if (expression instanceof SelectExpr) {
    const aliases: Record<string, boolean> = {};
    expression.args.expressions?.forEach((sel) => {
      if (sel instanceof AliasExpr) {
        const aliasNode = sel.args.alias;
        if (aliasNode instanceof IdentifierExpr) {
          aliases[aliasNode.name] = aliasNode.args.quoted || false;
        }
      }
    });

    const table = expression.find(TableExpr);
    const tableIdent = table?.args.this;

    if (
      tableIdent instanceof IdentifierExpr
      && tableIdent.name.toUpperCase() === 'LOCAL'
      && !tableIdent.args.quoted
    ) {
      tableIdent.replace(toIdentifier(tableIdent.name.toUpperCase(), { quoted: true }));
    }

    const prefixLocal = (node: Expression, visibleAliases: Record<string, boolean>): Expression => {
      if (node instanceof ColumnExpr && !node.args.table) {
        const colName = (node.args.this as IdentifierExpr).name;
        if (colName in visibleAliases) {
          return new ColumnExpr({
            this: toIdentifier(colName, { quoted: visibleAliases[colName] }),
            table: toIdentifier('LOCAL', { quoted: false }),
          });
        }
      }
      return node;
    };

    [
      'where',
      'group',
      'having',
    ].forEach((key) => {
      const arg = expression.args[key as keyof SelectExprArgs];
      if (arg instanceof Expression) {
        expression.setArgKey(key, arg.transform((node) => prefixLocal(node, aliases)));
      }
    });

    const seenAliases: Record<string, boolean> = {};
    const newSelects: Expression[] = [];
    expression.args.expressions?.forEach((sel) => {
      if (sel instanceof AliasExpr) {
        const inner = sel.args.this?.transform((node) => prefixLocal(node, seenAliases));
        sel.setArgKey('this', inner);

        const aliasNode = sel.args.alias;
        if (aliasNode instanceof IdentifierExpr) {
          seenAliases[aliasNode.name] = aliasNode.args.quoted || false;
        }
        newSelects.push(sel);
      } else {
        newSelects.push(sel.transform((node: Expression) => prefixLocal(node, seenAliases)));
      }
    });
    expression.setArgKey('expressions', newSelects);
  }

  return expression;
}

type TruncableExpr = DateTruncExpr | TimestampTruncExpr | DatetimeTruncExpr;

function truncSql (self: ExasolGenerator, kind: string, expression: TruncableExpr): string {
  const unit = expression.text('unit');
  const node = expression.args.this instanceof CastExpr ? expression.args.this.args.this : expression.args.this;
  let exprSql = self.sql(node);

  if (node instanceof LiteralExpr && node.isString) {
    exprSql =
      kind === 'TIMESTAMP'
        ? `${kind} '${(node.args.this ?? '').replace('T', ' ')}'`
        : `DATE '${node.args.this ?? ''}'`;
  }
  return `DATE_TRUNC('${unit}', ${exprSql})`;
}

function dateTruncSql (self: ExasolGenerator, expression: DateTruncExpr): string {
  return truncSql(self, 'DATE', expression);
}

function timestampTruncSql (self: ExasolGenerator, expression: TruncableExpr): string {
  return truncSql(self, 'TIMESTAMP', expression);
}

function isCaseInsensitive (node: Expression): boolean {
  return node instanceof CollateExpr && node.text('expression').toUpperCase() === 'UTF8_LCASE';
}

function substringIndexSql (self: ExasolGenerator, expression: SubstringIndexExpr): string {
  const thisNode = expression.args.this;
  const delimiterNode = expression.args.delimiter;
  const delimiterExpr: Expression = delimiterNode instanceof Expression ? delimiterNode : LiteralExpr.number(delimiterNode ?? 0);
  const countNode = expression.args.count;
  const countSql = self.sql(expression, 'count');
  const num = countNode instanceof LiteralExpr && countNode.isNumber ? parseFloat(countNode.args.this ?? '0') : 0;

  const haystackSql = self.sql(thisNode);
  if (num === 0) {
    return self.func('SUBSTR', [
      haystackSql,
      '1',
      '0',
    ]);
  }

  const fromRight = num < 0;
  const direction = fromRight ? '-1' : '1';
  const occur = fromRight ? self.func('ABS', [countSql]) : countSql;

  const delimiterSql = self.sql(delimiterExpr);

  const position = self.func('INSTR', [
    thisNode instanceof Expression && isCaseInsensitive(thisNode) ? self.func('LOWER', [haystackSql]) : haystackSql,
    isCaseInsensitive(delimiterExpr) ? self.func('LOWER', [delimiterSql]) : delimiterSql,
    direction,
    occur,
  ]);
  const nullablePos = self.func('NULLIF', [position, '0']);

  if (fromRight) {
    const start = self.func('NVL', [`${nullablePos} + ${self.func('LENGTH', [delimiterSql])}`, direction]);
    return self.func('SUBSTR', [haystackSql, start]);
  }

  const length = self.func('NVL', [`${nullablePos} - 1`, self.func('LENGTH', [haystackSql])]);
  return self.func('SUBSTR', [
    haystackSql,
    direction,
    length,
  ]);
}

/**
 * Exasol doesn't support a bare * alongside other select items.
 * Rewrite: SELECT *, <other> FROM <Table> Into: SELECT T.*, <other> FROM <Table> AS T
 */
function qualifyUnscopedStar (expression: Expression): Expression {
  if (!(expression instanceof SelectExpr)) {
    return expression;
  }

  const selectExpressions = expression.args.expressions || [];

  const isBareStar = (expr: Expression): boolean => expr instanceof StarExpr && !expr.args.this;

  let hasOtherExpression = false;
  let bareStarExpr: Expression | null = null;
  for (const expr of selectExpressions) {
    const hasBareStar = isBareStar(expr);
    if (hasBareStar && bareStarExpr === null) {
      bareStarExpr = expr;
    } else if (!hasBareStar) {
      hasOtherExpression = true;
    }
    if (bareStarExpr && hasOtherExpression) break;
  }

  if (!(bareStarExpr && hasOtherExpression)) {
    return expression;
  }

  const scope = buildScope(expression);
  if (!scope || !scope.selectedSources || Object.keys(scope.selectedSources).length === 0) {
    return expression;
  }

  const tableIdentifiers: IdentifierExpr[] = [];
  for (const [sourceName, sourceEntries] of Object.entries(scope.selectedSources)) {
    const [sourceExpr] = sourceEntries as Expression[];
    const ident =
      sourceExpr instanceof TableExpr && sourceExpr.args.this instanceof IdentifierExpr
        ? (sourceExpr.args.this.copy() as IdentifierExpr)
        : toIdentifier(sourceName);
    tableIdentifiers.push(ident);
  }

  // bareStarExpr is guaranteed non-null here (early return above)
  const nonNullBareStar = bareStarExpr as Expression;
  const qualifiedStarColumns = tableIdentifiers.map(
    (ident) => new ColumnExpr({
      this: nonNullBareStar.copy(),
      table: ident,
    }),
  );

  const newSelectExpressions: Expression[] = [];
  for (const selectExpr of selectExpressions) {
    if (isBareStar(selectExpr)) {
      newSelectExpressions.push(...qualifiedStarColumns);
    } else {
      newSelectExpressions.push(selectExpr);
    }
  }

  expression.setArgKey('expressions', newSelectExpressions);
  return expression;
}

function addDateSql (self: ExasolGenerator, expression: DateAddExpr | DateSubExpr | TsOrDsAddExpr): string {
  const interval = expression.args.expression instanceof IntervalExpr ? expression.args.expression : null;

  const unit = (
    interval ? (interval.text('unit') || 'DAY') : (expression.text('unit') || 'DAY')
  ).toUpperCase();

  if (!DATE_UNITS.has(unit)) {
    self.unsupported(`'${unit}' is not supported in Exasol.`);
    return self.functionFallbackSql(expression);
  }

  if (!expression.args.expression) return self.functionFallbackSql(expression);
  let offsetExpr: Expression = expression.args.expression;
  if (interval) {
    if (!interval.args.this) return self.functionFallbackSql(expression);
    offsetExpr = interval.args.this;
  }

  if (expression instanceof DateSubExpr) {
    offsetExpr = new NegExpr({ this: offsetExpr });
  }

  return self.func(`ADD_${unit}S`, [expression.args.this, offsetExpr]);
}

class ExasolTokenizer extends Tokenizer {
  static IDENTIFIERS: TokenPair[] = ['"', ['[', ']']];

  @cache
  static get ORIGINAL_KEYWORDS (): Record<string, TokenType> {
    const keywords = {
      ...Tokenizer.KEYWORDS,
      'USER': TokenType.CURRENT_USER,
      'ENDIF': TokenType.END,
      'LONG VARCHAR': TokenType.TEXT,
      'SEPARATOR': TokenType.SEPARATOR,
      'SYSTIMESTAMP': TokenType.SYSTIMESTAMP,
    };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    delete (keywords as any)['DIV'];
    return keywords;
  }
}

class ExasolParser extends Parser {
  @cache
  static get FUNCTIONS (): Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression> {
    return (() => {
      const functions: Record<string, (args: Expression[]) => Expression> = {
        ...Parser.FUNCTIONS,
        BIT_AND: binaryFromFunction(BitwiseAndExpr),
        BIT_OR: binaryFromFunction(BitwiseOrExpr),
        BIT_XOR: binaryFromFunction(BitwiseXorExpr),
        BIT_NOT: (args: Expression[]) => new BitwiseNotExpr({ this: seqGet(args, 0) }),
        BIT_LSHIFT: binaryFromFunction(BitwiseLeftShiftExpr),
        BIT_RSHIFT: binaryFromFunction(BitwiseRightShiftExpr),
        DATE_TRUNC: (args: Expression[]) => new TimestampTruncExpr({
          this: seqGet(args, 1),
          unit: seqGet(args, 0),
        }),
        DIV: binaryFromFunction(IntDivExpr),
        EVERY: (args: Expression[]) => new AllExpr({ this: seqGet(args, 0) }),
        EDIT_DISTANCE: LevenshteinExpr.fromArgList,
        HASH_SHA: ShaExpr.fromArgList,
        HASH_SHA1: ShaExpr.fromArgList,
        HASH_MD5: Md5Expr.fromArgList,
        HASHTYPE_MD5: Md5DigestExpr.fromArgList,
        REGEXP_SUBSTR: RegexpExtractExpr.fromArgList,
        REGEXP_REPLACE: (args: Expression[]) => new RegexpReplaceExpr({
          this: seqGet(args, 0),
          expression: seqGet(args, 1),
          replacement: seqGet(args, 2),
          position: seqGet(args, 3),
          occurrence: seqGet(args, 4),
        }),
        HASH_SHA256: (args: Expression[]) => new Sha2Expr({
          this: seqGet(args, 0),
          length: LiteralExpr.number(256),
        }),
        HASH_SHA512: (args: Expression[]) => new Sha2Expr({
          this: seqGet(args, 0),
          length: LiteralExpr.number(512),
        }),
        TRUNC: (args: Expression[]) => buildTrunc(args, { dialect: Dialects.EXASOL }),
        TRUNCATE: (args: Expression[]) => buildTrunc(args, { dialect: Dialects.EXASOL }),
        VAR_POP: VariancePopExpr.fromArgList,
        APPROXIMATE_COUNT_DISTINCT: ApproxDistinctExpr.fromArgList,
        TO_CHAR: (args: Expression[]) => buildTimeToStrOrToChar(args, { dialect: Dialects.EXASOL }),
        TO_DATE: buildFormattedTime(TsOrDsToDateExpr, { dialect: Dialects.EXASOL }),
        CONVERT_TZ: (args: Expression[]) => new ConvertTimezoneExpr({
          timestamp: seqGet(args, 0)!,
          sourceTz: seqGet(args, 1),
          targetTz: seqGet(args, 2)!,
        }),
        NULLIFZERO: buildNullIfZero,
        ZEROIFNULL: buildZeroIfNull,
      };

      DATE_UNITS.forEach((unit) => {
        functions[`ADD_${unit}S`] = buildDateDelta(DateAddExpr, undefined, { defaultUnit: unit });
        functions[`${unit}S_BETWEEN`] = buildDateDelta(DateDiffExpr, undefined, { defaultUnit: unit });
      });

      return functions;
    })();
  }

  @cache
  static get CONSTRAINT_PARSERS (): Partial<Record<string, (self: Parser, ...args: unknown[]) => Expression | Expression[] | undefined>> {
    return {
      ...Parser.CONSTRAINT_PARSERS,
      COMMENT: (self: Parser) => {
        return self.expression(
          CommentColumnConstraintExpr,
          { this: (self as ExasolParser).match(TokenType.IS) && self.parseString() },
        );
      },
    };
  }

  @cache
  static get FUNC_TOKENS (): Set<TokenType> {
    return new Set([...Parser.FUNC_TOKENS, TokenType.SYSTIMESTAMP]);
  }

  @cache
  static get NO_PAREN_FUNCTIONS (): Partial<Record<TokenType, typeof Expression>> {
    return {
      ...Parser.NO_PAREN_FUNCTIONS,
      [TokenType.SYSTIMESTAMP]: SystimestampExpr,
    };
  }

  @cache
  static get FUNCTION_PARSERS (): Partial<Record<string, (self: Parser) => Expression | undefined>> {
    return {
      ...Parser.FUNCTION_PARSERS,
      ...Object.fromEntries(['GROUP_CONCAT', 'LISTAGG'].map((k) => [k, (self: Parser) => self.parseGroupConcat()])),
    };
  }

  static ODBC_DATETIME_LITERALS = {
    d: DateExpr,
    ts: TimestampExpr,
  };

  parseColumn (): Expression | undefined {
    const column = super.parseColumn();
    if (!(column instanceof ColumnExpr)) return column;

    const tableIdent = column.args.table;
    if (
      tableIdent instanceof IdentifierExpr
      && tableIdent.name.toUpperCase() === 'LOCAL'
      && !tableIdent.args.quoted
    ) {
      column.setArgKey('table', undefined);
    }
    return column;
  }
}

class ExasolGenerator extends Generator {
  static STRING_TYPE_MAPPING = {
    [DataTypeExprKind.BLOB]: 'VARCHAR',
    [DataTypeExprKind.LONGBLOB]: 'VARCHAR',
    [DataTypeExprKind.LONGTEXT]: 'VARCHAR',
    [DataTypeExprKind.MEDIUMBLOB]: 'VARCHAR',
    [DataTypeExprKind.MEDIUMTEXT]: 'VARCHAR',
    [DataTypeExprKind.TINYBLOB]: 'VARCHAR',
    [DataTypeExprKind.TINYTEXT]: 'VARCHAR',
    [DataTypeExprKind.TEXT]: 'LONG VARCHAR',
    [DataTypeExprKind.VARBINARY]: 'VARCHAR',
  };

  @cache
  static get TYPE_MAPPING () {
    return {
      ...Generator.TYPE_MAPPING,
      ...ExasolGenerator.STRING_TYPE_MAPPING,
      [DataTypeExprKind.TINYINT]: 'SMALLINT',
      [DataTypeExprKind.MEDIUMINT]: 'INT',
      [DataTypeExprKind.DECIMAL32]: 'DECIMAL',
      [DataTypeExprKind.DECIMAL64]: 'DECIMAL',
      [DataTypeExprKind.DECIMAL128]: 'DECIMAL',
      [DataTypeExprKind.DECIMAL256]: 'DECIMAL',
      [DataTypeExprKind.DATETIME]: 'TIMESTAMP',
      [DataTypeExprKind.TIMESTAMPTZ]: 'TIMESTAMP',
      [DataTypeExprKind.TIMESTAMPLTZ]: 'TIMESTAMP',
      [DataTypeExprKind.TIMESTAMPNTZ]: 'TIMESTAMP',
    };
  }

  dataTypeSql (expression: DataTypeExpr): string {
    if (expression.isType([DataTypeExprKind.TIMESTAMPLTZ])) {
      return 'TIMESTAMP WITH LOCAL TIME ZONE';
    }
    return super.dataTypeSql(expression);
  }

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (self: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (self: Generator, e: any) => string>([
      ...Generator.TRANSFORMS,
      [AllExpr, renameFunc('EVERY')],
      [BitwiseAndExpr, renameFunc('BIT_AND')],
      [BitwiseOrExpr, renameFunc('BIT_OR')],
      [BitwiseNotExpr, renameFunc('BIT_NOT')],
      [BitwiseLeftShiftExpr, renameFunc('BIT_LSHIFT')],
      [BitwiseRightShiftExpr, renameFunc('BIT_RSHIFT')],
      [BitwiseXorExpr, renameFunc('BIT_XOR')],
      [DateDiffExpr, (self: Generator, e: DateDiffExpr) => dateDiffSql(self as ExasolGenerator, e)],
      [DateAddExpr, (self: Generator, e: DateAddExpr) => addDateSql(self as ExasolGenerator, e)],
      [TsOrDsAddExpr, (self: Generator, e: TsOrDsAddExpr) => addDateSql(self as ExasolGenerator, e)],
      [DateSubExpr, (self: Generator, e: DateSubExpr) => addDateSql(self as ExasolGenerator, e)],
      [IntDivExpr, renameFunc('DIV')],
      [TsOrDsDiffExpr, (self: Generator, e: TsOrDsDiffExpr) => dateDiffSql(self as ExasolGenerator, e)],
      [DateTruncExpr, (self: Generator, e: DateTruncExpr) => dateTruncSql(self as ExasolGenerator, e)],
      [DayOfWeekExpr, (self: Generator, e: DayOfWeekExpr) => `CAST(TO_CHAR(${self.sql(e, 'this')}, 'D') AS INTEGER)`],
      [DatetimeTruncExpr, (self: Generator, e: DatetimeTruncExpr) => timestampTruncSql(self as ExasolGenerator, e)],
      [
        GroupConcatExpr,
        (self: Generator, e: GroupConcatExpr) => groupConcatSql(self, e, {
          funcName: 'LISTAGG',
          withinGroup: true,
        }),
      ],
      [LevenshteinExpr, (self: Generator, e: Expression) => unsupportedArgs('insCost', 'delCost', 'subCost', 'maxDist')((expr) => renameFunc('EDIT_DISTANCE')(self, expr))(e)],
      [ModExpr, renameFunc('MOD')],
      [RegexpExtractExpr, (self: Generator, e: Expression) => unsupportedArgs('parameters', 'group')((expr) => renameFunc('REGEXP_SUBSTR')(self, expr))(e)],
      [RegexpReplaceExpr, (self: Generator, e: Expression) => unsupportedArgs('modifiers')((expr) => renameFunc('REGEXP_REPLACE')(self, expr))(e)],
      [VariancePopExpr, renameFunc('VAR_POP')],
      [ApproxDistinctExpr, (self: Generator, e: Expression) => unsupportedArgs('accuracy')((expr) => renameFunc('APPROXIMATE_COUNT_DISTINCT')(self, expr))(e)],
      [ToCharExpr, (self: Generator, e: ToCharExpr) => self.func('TO_CHAR', [e.args.this, self.formatTime(e)!])],
      [TsOrDsToDateExpr, (self: Generator, e: TsOrDsToDateExpr) => self.func('TO_DATE', [e.args.this, self.formatTime(e)!])],
      [TimeToStrExpr, (self: Generator, e: TimeToStrExpr) => self.func('TO_CHAR', [e.args.this, self.formatTime(e)!])],
      [TimeStrToTimeExpr, (self: Generator, e: TimeStrToTimeExpr) => timeStrToTimeSql(self, e)],
      [TimestampTruncExpr, (self: Generator, e: TimestampTruncExpr) => timestampTruncSql(self as ExasolGenerator, e)],
      [StrToTimeExpr, (self: Generator, e: StrToTimeExpr) => self.func('TO_DATE', [e.args.this, self.formatTime(e)!])],
      [CurrentUserExpr, () => 'CURRENT_USER'],
      [
        AtTimeZoneExpr,
        (self: Generator, e: AtTimeZoneExpr) => self.func('CONVERT_TZ', [
          e.args.this,
          '\'UTC\'',
          e.args.zone,
        ]),
      ],
      [
        StrPositionExpr,
        (self: Generator, e: StrPositionExpr) => strPositionSql(self, e, {
          funcName: 'INSTR',
          supportsPosition: true,
          supportsOccurrence: true,
        }),
      ],
      [ShaExpr, renameFunc('HASH_SHA')],
      [Sha2Expr, (self: Generator, e: Sha2Expr) => sha2Sql(self as ExasolGenerator, e)],
      [Md5Expr, renameFunc('HASH_MD5')],
      [Md5DigestExpr, renameFunc('HASHTYPE_MD5')],
      [CommentColumnConstraintExpr, (self: Generator, e: CommentColumnConstraintExpr) => `COMMENT IS ${self.sql(e, 'this')}`],
      [SelectExpr, preprocess([qualifyUnscopedStar, addLocalPrefixForAliases])],
      [SubstringIndexExpr, (self: Generator, e: SubstringIndexExpr) => substringIndexSql(self as ExasolGenerator, e)],
      [WeekOfYearExpr, renameFunc('WEEK')],
      [DateExpr, renameFunc('TO_DATE')],
      [TimestampExpr, renameFunc('TO_TIMESTAMP')],
      [QuarterExpr, (self: Generator, e: QuarterExpr) => `CEIL(MONTH(TO_DATE(${self.sql(e, 'this')}))/3)`],
      [LastDayExpr, noLastDaySql],
    ]);
    return transforms;
  }

  convertTimezoneSql (expression: ConvertTimezoneExpr): string {
    return this.func('CONVERT_TZ', [
      expression.args.timestamp,
      expression.args.sourceTz,
      expression.args.targetTz,
    ]);
  }

  ifSql (expression: IfExpr): string {
    const thisSql = this.sql(expression, 'this');
    const trueSql = this.sql(expression, 'true');
    const falseSql = this.sql(expression, 'false');
    return `IF ${thisSql} THEN ${trueSql} ELSE ${falseSql} ENDIF`;
  }

  collateSql (expression: CollateExpr): string {
    return this.sql(expression.args.this);
  }

  rankSql (expression: RankExpr): string {
    if (expression.args.expressions && 0 < expression.args.expressions.length) {
      this.unsupported('Exasol does not support arguments in RANK');
    }
    return this.func('RANK', []);
  }
}

export class Exasol extends Dialect {
  static NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE;
  static SUPPORTS_USER_DEFINED_TYPES = false;
  static SUPPORTS_SEMI_ANTI_JOIN = false;
  static SUPPORTS_COLUMN_JOIN_MARKS = true;
  static NULL_ORDERING = 'nulls_are_last' as const;
  static CONCAT_COALESCE = true;
  static TIME_MAPPING = {
    CC: '%y',
    D: '%u',
    DAY: '%A',
    DD: '%d',
    DDD: '%j',
    DY: '%a',
    FF1: '%f',
    FF2: '%f',
    FF3: '%f',
    FF4: '%f',
    FF5: '%f',
    FF6: '%f',
    FF7: '%f',
    FF8: '%f',
    FF9: '%f',
    FX: '%c',
    HH: '%I',
    HH12: '%I',
    HH24: '%H',
    IW: '%V',
    MI: '%M',
    MM: '%m',
    MON: '%b',
    MONTH: '%B',
    Q: '',
    RR: '%y',
    RRRR: '%Y',
    SS: '%S',
    SSSSS: '%f',
    WW: '%W',
    W: '%W',
    YY: '%y',
    YYY: '%y',
    YYYY: '%Y',
  };

  static Tokenizer = ExasolTokenizer;
  static Parser = ExasolParser;
  static Generator = ExasolGenerator;
}

Dialect.register(Dialects.EXASOL, Exasol);
