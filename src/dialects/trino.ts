import { cache } from '../port_internals';
import type { Expression } from '../expressions';
import {
  JsonExtractExpr, JsonExtractQuoteExpr,
  CurrentVersionExpr,
  LocationPropertyExpr,
  ArraySumExpr,
  ArrayUniqueAggExpr,
  GroupConcatExpr,
  PropertiesLocation,
  MergeExpr,
  SelectExpr,
  TimeStrToTimeExpr,
  TrimExpr,
  JsonPathKeyExpr,
  JsonPathRootExpr,
  JsonPathSubscriptExpr,
} from '../expressions';
import type { Generator } from '../generator';
import type { Parser } from '../parser';
import { TokenType } from '../tokens';
import {
  eliminateDistinctOn, eliminateQualify, eliminateSemiAndAntiJoins, explodeProjectionToUnnest, preprocess,
} from '../transforms';
import {
  Dialect,
  Dialects,
  groupConcatSql, mergeWithoutTargetSql, renameFunc, timeStrToTimeSql, trimSql,
} from './dialect';
import {
  Presto, amendExplodedColumnTable,
} from './presto';

class TrinoTokenizer extends Presto.Tokenizer {
  static ORIGINAL_KEYWORDS: Record<string, TokenType> = {
    ...Presto.Tokenizer.KEYWORDS,
    REFRESH: TokenType.REFRESH,
  };
};

class TrinoParser extends Presto.Parser {
  @cache
  static get FUNCTIONS (): Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression> {
    return {
      ...Presto.Parser.FUNCTIONS,
      VERSION: (args: Expression[]) => CurrentVersionExpr.fromArgList(args),
    };
  }

  @cache
  static get FUNCTION_PARSERS (): Partial<Record<string, (self: Parser) => Expression | undefined>> {
    return {
      ...Presto.Parser.FUNCTION_PARSERS,
      TRIM: (self: Parser) => self.parseTrim(),
      JSON_QUERY: (self: Parser) => (self as TrinoParser).parseJsonQuery(),
      JSON_VALUE: (self: Parser) => self.parseJsonValue(),
      LISTAGG: (self: Parser) => self.parseStringAgg(),
    };
  }

  static JSON_QUERY_OPTIONS: Record<string, string[][]> = {
    WITH: [
      ['WRAPPER'],
      ['ARRAY', 'WRAPPER'],
      ['CONDITIONAL', 'WRAPPER'],
      [
        'CONDITIONAL',
        'ARRAY',
        'WRAPPED',
      ],
      ['UNCONDITIONAL', 'WRAPPER'],
      [
        'UNCONDITIONAL',
        'ARRAY',
        'WRAPPER',
      ],
    ],
    WITHOUT: [
      ['WRAPPER'],
      ['ARRAY', 'WRAPPER'],
      ['CONDITIONAL', 'WRAPPER'],
      [
        'CONDITIONAL',
        'ARRAY',
        'WRAPPED',
      ],
      ['UNCONDITIONAL', 'WRAPPER'],
      [
        'UNCONDITIONAL',
        'ARRAY',
        'WRAPPER',
      ],
    ],
  };

  public parseJsonQueryQuote (): JsonExtractQuoteExpr | undefined {
    if (
      !(this.matchTextSeq(['KEEP', 'QUOTES']) || this.matchTextSeq(['OMIT', 'QUOTES']))
    ) {
      return undefined;
    }

    // If matched, _matchTextSeq advanced the index by 2 tokens.
    // We look back to grab the KEEP or OMIT text.
    return this.expression(JsonExtractQuoteExpr, {
      option: this.tokens[this.index - 2].text.toUpperCase(),
      scalar: this.matchTextSeq([
        'ON',
        'SCALAR',
        'STRING',
      ]),
    });
  }

  public parseJsonQuery (): JsonExtractExpr {
    return this.expression(JsonExtractExpr, {
      this: this.parseBitwise(),
      expression: this.match(TokenType.COMMA) ? this.parseBitwise() : undefined,
      option: this.parseVarFromOptions(
        (this._constructor as typeof TrinoParser).JSON_QUERY_OPTIONS,
        { raiseUnmatched: false },
      ),
      jsonQuery: true,
      quote: this.parseJsonQueryQuote(),
      onCondition: this.parseOnCondition(),
    });
  }
}

class TrinoGenerator extends Presto.Generator {
  static EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE = true;

  @cache
  static get PROPERTIES_LOCATION (): Map<typeof Expression, PropertiesLocation> {
    return {
      ...Presto.Generator.PROPERTIES_LOCATION,
      [LocationPropertyExpr.name]: PropertiesLocation.POST_WITH,
    };
  }

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (self: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return new Map<typeof Expression, (self: Generator, e: any) => string>([
      ...Presto.Generator.TRANSFORMS,
      [
        ArraySumExpr,
        (self, e) =>
          `REDUCE(${self.sql(e, 'this')}, 0, (acc, x) -> acc + x, acc -> acc)`,
      ],
      [
        ArrayUniqueAggExpr,
        (self, e) =>
          `ARRAY_AGG(DISTINCT ${self.sql(e, 'this')})`,
      ],
      [CurrentVersionExpr, renameFunc('VERSION')],
      [
        GroupConcatExpr,
        (self, e) =>
          groupConcatSql(self, e, { onOverflow: true }),
      ],
      [
        LocationPropertyExpr,
        (self, e) =>
          self.propertySql(e),
      ],
      [MergeExpr, mergeWithoutTargetSql],
      [
        SelectExpr,
        preprocess([
          eliminateQualify,
          eliminateDistinctOn,
          explodeProjectionToUnnest(1),
          eliminateSemiAndAntiJoins,
          amendExplodedColumnTable,
        ]),
      ],
      [
        TimeStrToTimeExpr,
        (self, e) =>
          timeStrToTimeSql(self, e, { includePrecision: true }),
      ],
      [TrimExpr, trimSql],
    ]);
  }

  @cache
  static get SUPPORTED_JSON_PATH_PARTS (): Set<typeof Expression> {
    return new Set([
      JsonPathKeyExpr,
      JsonPathRootExpr,
      JsonPathSubscriptExpr,
    ]);
  }

  public jsonExtractSql (expression: JsonExtractExpr): string {
    if (!expression.args.jsonQuery) {
      return super.jsonExtractSql(expression);
    }

    const jsonPath = this.sql(expression, 'expression');
    const option = this.sql(expression, 'option');
    const optionStr = option ? ` ${option}` : '';

    const quote = this.sql(expression, 'quote');
    const quoteStr = quote ? ` ${quote}` : '';

    const onCondition = this.sql(expression, 'onCondition');
    const onConditionStr = onCondition ? ` ${onCondition}` : '';

    return this.func(
      'JSON_QUERY',
      [expression.args.this, jsonPath + optionStr + quoteStr + onConditionStr],
    );
  }
}

export class Trino extends Presto {
  static SUPPORTS_USER_DEFINED_TYPES = false;
  static LOG_BASE_FIRST = true;

  static Tokenizer = TrinoTokenizer;
  static Parser = TrinoParser;
  static Generator = TrinoGenerator;
}
Dialect.register(Dialects.TRINO, Trino);
