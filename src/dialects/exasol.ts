import { cache } from '../port_internals';
import {
  Generator, unsupportedArgs,
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
  NullOrdering,
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

function sha2Sql (this: ExasolGenerator, expression: Sha2Expr): string {
  const length = expression.text('length');
  const funcName = length === '256' ? 'HASH_SHA256' : 'HASH_SHA512';
  return this.func(funcName, [expression.args.this]);
}

function dateDiffSql (this: ExasolGenerator, expression: DateDiffExpr | TsOrDsDiffExpr): string {
  const unit = expression.text('unit').toUpperCase() || 'DAY';

  if (!DATE_UNITS.has(unit)) {
    this.unsupported(`'${unit}' is not supported in Exasol.`);
    return this.functionFallbackSql(expression);
  }

  return this.func(`${unit}S_BETWEEN`, [expression.args.this, expression.args.expression]);
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

function truncSql (this: ExasolGenerator, kind: string, expression: TruncableExpr): string {
  const unit = expression.text('unit');
  const node = expression.args.this instanceof CastExpr ? expression.args.this.args.this : expression.args.this;
  let exprSql = this.sql(node);

  if (node instanceof LiteralExpr && node.isString) {
    exprSql =
      kind === 'TIMESTAMP'
        ? `${kind} '${(node.args.this ?? '').replace('T', ' ')}'`
        : `DATE '${node.args.this ?? ''}'`;
  }
  return `DATE_TRUNC('${unit}', ${exprSql})`;
}

function dateTruncSql (this: ExasolGenerator, expression: DateTruncExpr): string {
  return truncSql.call(this, 'DATE', expression);
}

function timestampTruncSql (this: ExasolGenerator, expression: TruncableExpr): string {
  return truncSql.call(this, 'TIMESTAMP', expression);
}

function isCaseInsensitive (node: Expression): boolean {
  return node instanceof CollateExpr && node.text('expression').toUpperCase() === 'UTF8_LCASE';
}

function substringIndexSql (this: ExasolGenerator, expression: SubstringIndexExpr): string {
  const thisNode = expression.args.this;
  const delimiterNode = expression.args.delimiter;
  const delimiterExpr: Expression = delimiterNode instanceof Expression ? delimiterNode : LiteralExpr.number(delimiterNode ?? 0);
  const countNode = expression.args.count;
  const countSql = this.sql(expression, 'count');
  const num = countNode instanceof LiteralExpr && countNode.isNumber ? parseFloat(countNode.args.this ?? '0') : 0;

  const haystackSql = this.sql(thisNode);
  if (num === 0) {
    return this.func('SUBSTR', [
      haystackSql,
      '1',
      '0',
    ]);
  }

  const fromRight = num < 0;
  const direction = fromRight ? '-1' : '1';
  const occur = fromRight ? this.func('ABS', [countSql]) : countSql;

  const delimiterSql = this.sql(delimiterExpr);

  const position = this.func('INSTR', [
    thisNode instanceof Expression && isCaseInsensitive(thisNode) ? this.func('LOWER', [haystackSql]) : haystackSql,
    isCaseInsensitive(delimiterExpr) ? this.func('LOWER', [delimiterSql]) : delimiterSql,
    direction,
    occur,
  ]);
  const nullablePos = this.func('NULLIF', [position, '0']);

  if (fromRight) {
    const start = this.func('NVL', [`${nullablePos} + ${this.func('LENGTH', [delimiterSql])}`, direction]);
    return this.func('SUBSTR', [haystackSql, start]);
  }

  const length = this.func('NVL', [`${nullablePos} - 1`, this.func('LENGTH', [haystackSql])]);
  return this.func('SUBSTR', [
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

function addDateSql (this: ExasolGenerator, expression: DateAddExpr | DateSubExpr | TsOrDsAddExpr): string {
  const interval = expression.args.expression instanceof IntervalExpr ? expression.args.expression : null;

  const unit = (
    interval ? (interval.text('unit') || 'DAY') : (expression.text('unit') || 'DAY')
  ).toUpperCase();

  if (!DATE_UNITS.has(unit)) {
    this.unsupported(`'${unit}' is not supported in Exasol.`);
    return this.functionFallbackSql(expression);
  }

  if (!expression.args.expression) return this.functionFallbackSql(expression);
  let offsetExpr: Expression = expression.args.expression;
  if (interval) {
    if (!interval.args.this) return this.functionFallbackSql(expression);
    offsetExpr = interval.args.this;
  }

  if (expression instanceof DateSubExpr) {
    offsetExpr = new NegExpr({ this: offsetExpr });
  }

  return this.func(`ADD_${unit}S`, [expression.args.this, offsetExpr]);
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
          timestamp: args[0],
          sourceTz: seqGet(args, 1),
          targetTz: args[2],
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
  static get CONSTRAINT_PARSERS (): Partial<Record<string, (this: Parser, ...args: unknown[]) => Expression | Expression[] | undefined>> {
    return {
      ...Parser.CONSTRAINT_PARSERS,
      COMMENT: function (this: Parser) {
        return this.expression(
          CommentColumnConstraintExpr,
          { this: (this as ExasolParser).match(TokenType.IS) && this.parseString() },
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
  static get FUNCTION_PARSERS (): Partial<Record<string, (this: Parser) => Expression | undefined>> {
    return {
      ...Parser.FUNCTION_PARSERS,
      ...Object.fromEntries(['GROUP_CONCAT', 'LISTAGG'].map((k) => [
        k,
        function (this: Parser) {
          return this.parseGroupConcat();
        },
      ])),
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
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (this: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (this: Generator, e: any) => string>([
      ...Generator.TRANSFORMS,
      [AllExpr, renameFunc('EVERY')],
      [BitwiseAndExpr, renameFunc('BIT_AND')],
      [BitwiseOrExpr, renameFunc('BIT_OR')],
      [BitwiseNotExpr, renameFunc('BIT_NOT')],
      [BitwiseLeftShiftExpr, renameFunc('BIT_LSHIFT')],
      [BitwiseRightShiftExpr, renameFunc('BIT_RSHIFT')],
      [BitwiseXorExpr, renameFunc('BIT_XOR')],
      [
        DateDiffExpr,
        function (this: Generator, e: DateDiffExpr) {
          return dateDiffSql.call(this as ExasolGenerator, e);
        },
      ],
      [
        DateAddExpr,
        function (this: Generator, e: DateAddExpr) {
          return addDateSql.call(this as ExasolGenerator, e);
        },
      ],
      [
        TsOrDsAddExpr,
        function (this: Generator, e: TsOrDsAddExpr) {
          return addDateSql.call(this as ExasolGenerator, e);
        },
      ],
      [
        DateSubExpr,
        function (this: Generator, e: DateSubExpr) {
          return addDateSql.call(this as ExasolGenerator, e);
        },
      ],
      [IntDivExpr, renameFunc('DIV')],
      [
        TsOrDsDiffExpr,
        function (this: Generator, e: TsOrDsDiffExpr) {
          return dateDiffSql.call(this as ExasolGenerator, e);
        },
      ],
      [
        DateTruncExpr,
        function (this: Generator, e: DateTruncExpr) {
          return dateTruncSql.call(this as ExasolGenerator, e);
        },
      ],
      [
        DayOfWeekExpr,
        function (this: Generator, e: DayOfWeekExpr) {
          return `CAST(TO_CHAR(${this.sql(e, 'this')}, 'D') AS INTEGER)`;
        },
      ],
      [
        DatetimeTruncExpr,
        function (this: Generator, e: DatetimeTruncExpr) {
          return timestampTruncSql.call(this as ExasolGenerator, e);
        },
      ],
      [
        GroupConcatExpr,
        function (this: Generator, e: GroupConcatExpr) {
          return groupConcatSql.call(this, e, {
            funcName: 'LISTAGG',
            withinGroup: true,
          });
        },
      ],
      [
        LevenshteinExpr,
        function (this: Generator, e: Expression) {
          unsupportedArgs.call(this, e, 'insCost', 'delCost', 'subCost', 'maxDist');
          return renameFunc('EDIT_DISTANCE').call(this, e);
        },
      ],
      [ModExpr, renameFunc('MOD')],
      [
        RegexpExtractExpr,
        function (this: Generator, e: Expression) {
          unsupportedArgs.call(this, e, 'parameters', 'group');
          return renameFunc('REGEXP_SUBSTR').call(this, e);
        },
      ],
      [
        RegexpReplaceExpr,
        function (this: Generator, e: Expression) {
          unsupportedArgs.call(this, e, 'modifiers');
          return renameFunc('REGEXP_REPLACE').call(this, e);
        },
      ],
      [VariancePopExpr, renameFunc('VAR_POP')],
      [
        ApproxDistinctExpr,
        function (this: Generator, e: Expression) {
          unsupportedArgs.call(this, e, 'accuracy');
          return renameFunc('APPROXIMATE_COUNT_DISTINCT').call(this, e);
        },
      ],
      [
        ToCharExpr,
        function (this: Generator, e: ToCharExpr) {
          return this.func('TO_CHAR', [e.args.this, this.formatTime(e)]);
        },
      ],
      [
        TsOrDsToDateExpr,
        function (this: Generator, e: TsOrDsToDateExpr) {
          return this.func('TO_DATE', [e.args.this, this.formatTime(e)]);
        },
      ],
      [
        TimeToStrExpr,
        function (this: Generator, e: TimeToStrExpr) {
          return this.func('TO_CHAR', [e.args.this, this.formatTime(e)]);
        },
      ],
      [
        TimeStrToTimeExpr,
        function (this: Generator, e: TimeStrToTimeExpr) {
          return timeStrToTimeSql.call(this, e);
        },
      ],
      [
        TimestampTruncExpr,
        function (this: Generator, e: TimestampTruncExpr) {
          return timestampTruncSql.call(this as ExasolGenerator, e);
        },
      ],
      [
        StrToTimeExpr,
        function (this: Generator, e: StrToTimeExpr) {
          return this.func('TO_DATE', [e.args.this, this.formatTime(e)]);
        },
      ],
      [CurrentUserExpr, () => 'CURRENT_USER'],
      [
        AtTimeZoneExpr,
        function (this: Generator, e: AtTimeZoneExpr) {
          return this.func('CONVERT_TZ', [
            e.args.this,
            '\'UTC\'',
            e.args.zone,
          ]);
        },
      ],
      [
        StrPositionExpr,
        function (this: Generator, e: StrPositionExpr) {
          return strPositionSql.call(this, e, {
            funcName: 'INSTR',
            supportsPosition: true,
            supportsOccurrence: true,
          });
        },
      ],
      [ShaExpr, renameFunc('HASH_SHA')],
      [
        Sha2Expr,
        function (this: Generator, e: Sha2Expr) {
          return sha2Sql.call(this as ExasolGenerator, e);
        },
      ],
      [Md5Expr, renameFunc('HASH_MD5')],
      [Md5DigestExpr, renameFunc('HASHTYPE_MD5')],
      [
        CommentColumnConstraintExpr,
        function (this: Generator, e: CommentColumnConstraintExpr) {
          return `COMMENT IS ${this.sql(e, 'this')}`;
        },
      ],
      [SelectExpr, preprocess([qualifyUnscopedStar, addLocalPrefixForAliases])],
      [
        SubstringIndexExpr,
        function (this: Generator, e: SubstringIndexExpr) {
          return substringIndexSql.call(this as ExasolGenerator, e);
        },
      ],
      [WeekOfYearExpr, renameFunc('WEEK')],
      [DateExpr, renameFunc('TO_DATE')],
      [TimestampExpr, renameFunc('TO_TIMESTAMP')],
      [
        QuarterExpr,
        function (this: Generator, e: QuarterExpr) {
          return `CEIL(MONTH(TO_DATE(${this.sql(e, 'this')}))/3)`;
        },
      ],
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
  static NULL_ORDERING = NullOrdering.NULLS_ARE_LAST;
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
