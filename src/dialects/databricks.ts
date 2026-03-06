import type {
  Generator,
} from '../generator';
import { Parser } from '../parser';
import { TokenType } from '../tokens';
import type {
  ColumnDefExpr,
  JsonPathExpr,
} from '../expressions';
import {
  JsonExtractScalarExpr,
  Expression,
  JsonPathRootExpr,
  GroupConcatExpr,
  JsonExtractExpr,
  DataTypeExpr,
  DataTypeExprKind,
  CurrentTimestampExpr,
  DateAddExpr,
  DateDiffExpr,
  TsOrDsToDateExpr,
  UniformExpr,
  CurrentDateExpr,
  TryCastExpr,
  CurrentVersionExpr,
  DatetimeAddExpr,
  DatetimeSubExpr,
  MulExpr,
  DatetimeTruncExpr,
  SelectExpr,
  ToCharExpr,
  CastExpr,
  CurrentCatalogExpr,
  GeneratedAsIdentityColumnConstraintExpr,
  RegexpLikeExpr,
  LiteralExpr,
} from '../expressions';
import { seqGet } from '../helper';
import { TypeAnnotator } from '../optimizer';
import { JsonPathTokenizer } from '../jsonpath';
import {
  anyToExists, eliminateDistinctOn, preprocess, unnestToExplode,
} from '../transforms';
import { narrowInstanceOf } from '../port_internals';
import { Spark } from './spark';
import {
  dateDeltaSql,
  buildDateDelta,
  buildFormattedTime,
  Dialect, Dialects,
  timestampTruncSql,
  groupConcatSql,
} from './dialect';

function jsonExtractSql (self: Generator, expression: JsonExtractExpr | JsonExtractScalarExpr): string {
  const thisSql = self.sql(expression, 'this');
  const exprSql = self.sql(expression, 'expression');
  return `${thisSql}:${exprSql}`;
}

class DatabricksJsonPathTokenizer extends JsonPathTokenizer {
  static IDENTIFIERS = ['`', '"'];
}

class DatabricksTokenizer extends Spark.Tokenizer {
  static KEYWORDS = {
    ...Spark.Tokenizer.KEYWORDS,
    VOID: TokenType.VOID,
  };
}

class DatabricksParser extends Spark.Parser {
  static LOG_DEFAULTS_TO_LN = true;
  static STRICT_CAST = true;
  static COLON_IS_VARIANT_EXTRACT = true;

  static #FUNCTIONS: undefined = undefined;
  static get FUNCTIONS () {
    return DatabricksParser.#FUNCTIONS ??= {
      ...Spark.Parser.FUNCTIONS,
      GETDATE: CurrentTimestampExpr.fromArgList,
      DATEADD: buildDateDelta(DateAddExpr),
      DATE_ADD: buildDateDelta(DateAddExpr),
      DATEDIFF: buildDateDelta(DateDiffExpr),
      DATE_DIFF: buildDateDelta(DateDiffExpr),
      NOW: CurrentTimestampExpr.fromArgList,
      TO_DATE: buildFormattedTime(TsOrDsToDateExpr, { dialect: Dialects.DATABRICKS }),
      UNIFORM: (args: Expression[]) => new UniformExpr({
        this: seqGet(args, 0),
        expression: seqGet(args, 1),
        seed: seqGet(args, 2),
      }),
    };
  }

  static #NO_PAREN_FUNCTION_PARSERS: undefined = undefined;
  static get NO_PAREN_FUNCTION_PARSERS () {
    return DatabricksParser.#NO_PAREN_FUNCTION_PARSERS ??= {
      ...Spark.Parser.NO_PAREN_FUNCTION_PARSERS,
      CURDATE: (self: Parser) => (self as DatabricksParser).parseCurdate(),
    };
  }

  static #FACTOR: undefined = undefined;
  static get FACTOR () {
    return DatabricksParser.#FACTOR ??= {
      ...Spark.Parser.FACTOR,
      [TokenType.COLON]: JsonExtractExpr,
    };
  }

  static #COLUMN_OPERATORS: undefined = undefined;
  static get COLUMN_OPERATORS () {
    return DatabricksParser.#COLUMN_OPERATORS ??= {
      ...Parser.COLUMN_OPERATORS,
      [TokenType.QDCOLON]: (self: Parser, thisNode?: Expression, to?: Expression) =>
        self.expression(TryCastExpr, {
          this: thisNode,
          to: to,
        }),
    };
  }

  parseCurdate (): CurrentDateExpr {
    if (this.match(TokenType.L_PAREN)) {
      this.matchRParen();
    }
    return this.expression(CurrentDateExpr);
  }
}

class DatabricksGenerator extends Spark.Generator {
  static TABLESAMPLE_SEED_KEYWORD = 'REPEATABLE';
  static COPY_PARAMS_ARE_WRAPPED = false;
  static COPY_PARAMS_EQ_REQUIRED = true;
  static JSON_PATH_SINGLE_QUOTE_ESCAPE = false;
  static QUOTE_JSON_PATH = false;
  static PARSE_JSON_NAME = 'PARSE_JSON' as const;

  static TYPE_MAPPING = {
    ...Spark.Generator.TYPE_MAPPING,
    [DataTypeExprKind.NULL]: 'VOID',
  };

  static ORIGINAL_TRANSFORMS = (() => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (self: Generator, e: any) => string>([
      ...Spark.Generator.TRANSFORMS,
      [CurrentVersionExpr, () => 'CURRENT_VERSION()'],
      [DateAddExpr, dateDeltaSql('DATEADD')],
      [DateDiffExpr, dateDeltaSql('DATEDIFF')],
      [
        DatetimeAddExpr,
        (self, e) => self.func('TIMESTAMPADD', [
          e.args.unit,
          e.args.expression,
          e.args.this,
        ]),
      ],
      [
        DatetimeSubExpr,
        (self, e) =>
          self.func('TIMESTAMPADD', [
            e.args.unit,
            new MulExpr({
              this: e.args.expression,
              expression: LiteralExpr.number(-1),
            }),
            e.args.this,
          ]),
      ],
      [DatetimeTruncExpr, timestampTruncSql()],
      [GroupConcatExpr, groupConcatSql],
      [
        SelectExpr,
        preprocess([
          eliminateDistinctOn,
          unnestToExplode,
          anyToExists,
        ]),
      ],
      [JsonExtractExpr, (self, e) => jsonExtractSql(self, e)],
      [JsonExtractScalarExpr, (self, e) => jsonExtractSql(self, e)],
      [JsonPathRootExpr, () => ''],
      [
        ToCharExpr,
        (self, e: ToCharExpr) =>
          e.args.isNumeric
            ? self.castSql(new CastExpr({
              this: e.args.this,
              to: new DataTypeExpr({ this: 'STRING' }),
            }))
            : self.functionFallbackSql(e),
      ],
      [CurrentCatalogExpr, () => 'CURRENT_CATALOG()'],
    ]);

    transforms.delete(RegexpLikeExpr);
    transforms.delete(TryCastExpr);

    return transforms;
  })();

  columnDefSql (expression: ColumnDefExpr, options: { sep?: string } = {}): string {
    const {
      sep = ' ',
    } = options;
    const constraint = expression.find(GeneratedAsIdentityColumnConstraintExpr);
    const kind = expression.kind;

    if (
      constraint
      && kind instanceof DataTypeExpr
      && DataTypeExpr.INTEGER_TYPES.has(kind.args.this as DataTypeExprKind)
    ) {
      // only BIGINT generated identity constraints are supported
      expression.setArgKey('kind', DataTypeExpr.build('bigint'));
    }

    return super.columnDefSql(expression, { sep });
  }

  generatedAsIdentityColumnConstraintSql (expression: GeneratedAsIdentityColumnConstraintExpr): string {
    expression.setArgKey('this', true); // trigger ALWAYS in super class
    return super.generatedAsIdentityColumnConstraintSql(expression);
  }

  jsonPathSql (expression: JsonPathExpr): string {
    expression.setArgKey('escape', undefined);
    return super.jsonPathSql(expression);
  }

  uniformSql (expression: UniformExpr): string {
    const gen = expression.args.gen;
    let seed: Expression | string | undefined = expression.args.seed;

    // From Snowflake UNIFORM(min, max, gen) as RANDOM(), RANDOM(seed), or constant value -> Extract seed
    if (gen) {
      seed = narrowInstanceOf(gen.args.this, Expression, 'string');
    }

    return this.func('UNIFORM', [
      expression.args.this,
      expression.args.expression,
      seed,
    ]);
  }
}

export class Databricks extends Spark {
  static SAFE_DIVISION = false;
  static COPY_PARAMS_ARE_CSV = false;

  static Tokenizer = DatabricksTokenizer;
  static JsonPathTokenizer = DatabricksJsonPathTokenizer;
  static Parser = DatabricksParser;
  static Generator = DatabricksGenerator;

  static COERCES_TO = (() => {
    const coersionMap = Object.fromEntries(Object.entries(TypeAnnotator.COERCES_TO));

    for (const textType of DataTypeExpr.TEXT_TYPES) {
      const types = new Set([
        ...(coersionMap.get(textType) || []),
        ...DataTypeExpr.NUMERIC_TYPES,
        ...DataTypeExpr.TEMPORAL_TYPES,
        DataTypeExprKind.BINARY,
        DataTypeExprKind.BOOLEAN,
        DataTypeExprKind.INTERVAL,
      ]);
      coersionMap.set(textType, types);
    }
    return coersionMap;
  })();
}

Dialect.register(Dialects.DATABRICKS, Databricks);
