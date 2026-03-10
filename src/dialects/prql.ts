import { cache } from '../port_internals';
import type {
  Expression, OrderedExpr, QueryExpr, SelectExpr,
} from '../expressions';
import {
  AliasExpr,
  AndExpr, AvgExpr, ColumnExpr, EqExpr, FromExpr, func, IsExpr, NeqExpr, NotExpr, NullExpr, OrderExpr, OrExpr, select, SumExpr,
} from '../expressions';
import { seqGet } from '../helper';
import { Parser } from '../parser';
import {
  Tokenizer, TokenType,
} from '../tokens';
import {
  Dialect, Dialects,
} from './dialect';

function selectAll (table: Expression | undefined): SelectExpr | undefined {
  return table ? select('*').from(table, { copy: false }) : undefined;
}

class PRQLTokenizer extends Tokenizer {
  @cache
  static get IDENTIFIERS () {
    return ['`'] as const;
  }

  @cache
  static get QUOTES () {
    return ['\'', '"'] as const;
  }

  @cache
  static get SINGLE_TOKENS () {
    return {
      ...Tokenizer.SINGLE_TOKENS,
      '=': TokenType.ALIAS,
      '\'': TokenType.QUOTE,
      '"': TokenType.QUOTE,
      '`': TokenType.IDENTIFIER,
      '#': TokenType.COMMENT,
    };
  }

  @cache
  static get ORIGINAL_KEYWORDS () {
    return {
      ...Tokenizer.KEYWORDS,
    };
  }
}

class PRQLParser extends Parser {
  @cache
  static get CONJUNCTION (): Partial<Record<TokenType, typeof Expression>> {
    return {
      ...Parser.CONJUNCTION,
      [TokenType.DAMP]: AndExpr,
    };
  }

  @cache
  static get DISJUNCTION (): Partial<Record<TokenType, typeof Expression>> {
    return {
      ...Parser.DISJUNCTION,
      [TokenType.DPIPE]: OrExpr,
    };
  }

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get TRANSFORM_PARSERS (): Record<string, (this: Parser, query: any) => QueryExpr | undefined> {
    return {
      DERIVE: function (this: Parser, query) {
        return (this as PRQLParser).parseSelection(query);
      },
      SELECT: function (this: Parser, query) {
        return (this as PRQLParser).parseSelection(query, { append: false });
      },
      TAKE: function (this: Parser, query) {
        return (this as PRQLParser).parseTake(query);
      },
      FILTER: function (this: Parser, query) {
        return query.where((this as PRQLParser).parseDisjunction());
      },
      APPEND: function (this: Parser, query) {
        return query.union(selectAll((this as PRQLParser).parseTable()), {
          distinct: false,
          copy: false,
        });
      },
      REMOVE: function (this: Parser, query) {
        return query.except(selectAll((this as PRQLParser).parseTable()), {
          distinct: false,
          copy: false,
        });
      },
      INTERSECT: function (this: Parser, query) {
        return query.intersect(selectAll((this as PRQLParser).parseTable()), {
          distinct: false,
          copy: false,
        });
      },
      SORT: function (this: Parser, query) {
        return (this as PRQLParser).parseOrderBy(query);
      },
      AGGREGATE: function (this: Parser, query) {
        return (this as PRQLParser).parseSelection(query, {
          parseMethod: (this as PRQLParser).parseAggregate,
          append: false,
        });
      },
    };
  }

  @cache
  static get FUNCTIONS (): Record<string, (args: Expression[], options: { dialect: Dialect }) => Expression> {
    return {
      ...Parser.FUNCTIONS,
      AVERAGE: (args: unknown[]) => AvgExpr.fromArgList(args),
      SUM: (args: Expression[]) => func('COALESCE', new SumExpr({
        this: seqGet(args, 0),
        expression: 0,
      })),
    };
  }

  parseEquality (): Expression | undefined {
    const eq = this.parseTokens(() => this.parseComparison(), (this._constructor as typeof PRQLParser).EQUALITY);
    if (!(eq instanceof EqExpr || eq instanceof NeqExpr)) {
      return eq;
    }

    if (eq.args.expression instanceof NullExpr) {
      const isExp = new IsExpr({
        this: eq.args.this,
        expression: eq.args.expression,
      });
      return eq instanceof EqExpr ? isExp : new NotExpr({ this: isExp });
    }
    if (eq.args.this instanceof NullExpr) {
      const isExp = new IsExpr({
        this: eq.args.expression,
        expression: eq.args.this,
      });
      return eq instanceof EqExpr ? isExp : new NotExpr({ this: isExp });
    }
    return eq;
  }

  parseStatement (): Expression | undefined {
    const expression = this.parseExpression();
    return expression ? expression : this.parseQuery();
  }

  parseQuery (): QueryExpr | undefined {
    const from = this.parseFrom();

    if (!from) {
      return undefined;
    }

    let query: QueryExpr = select('*').from(from, { copy: false });

    while (this.matchTexts(Object.keys((this._constructor as typeof PRQLParser).TRANSFORM_PARSERS))) {
      const transform = (this._constructor as typeof PRQLParser).TRANSFORM_PARSERS[this.prev?.text.toUpperCase() ?? ''];
      const result = transform.call(this, query);
      if (result) {
        query = result;
      }
    }

    return query;
  }

  parseSelection (
    query: QueryExpr,
    options: {
      parseMethod?: () => Expression | undefined;
      append?: boolean;
    } = {},
  ): QueryExpr {
    const {
      parseMethod = () => this.parseExpression(), append = true,
    } = options;
    let selects: (Expression | undefined)[];

    if (this.match(TokenType.L_BRACE)) {
      selects = this.parseCsv(parseMethod);

      if (!this.match(TokenType.R_BRACE)) {
        this.raiseError('Expecting }');
      }
    } else {
      const expression = parseMethod();
      selects = expression ? [expression] : [];
    }

    const projections: Record<string, Expression> = {};
    (query as SelectExpr).args.expressions?.forEach((select) => {
      projections[select.aliasOrName] = select instanceof AliasExpr ? (select.args.this ?? select) : select;
    });

    const transformedSelects = selects.map((select) => {
      if (!select) return select;
      return select.transform((s) => {
        if (s instanceof ColumnExpr && projections[s.name]) {
          return projections[s.name].copy();
        }
        return s;
      }, { copy: false });
    }) as Expression[];

    return select(...transformedSelects, {
      append,
      copy: false,
    });
  }

  parseTake (query: QueryExpr): QueryExpr | undefined {
    const num = this.parseNumber();
    return num ? query.limit(num) : undefined;
  }

  parseOrdered (parseMethod?: () => Expression | undefined): OrderedExpr | undefined {
    const asc = this.match(TokenType.PLUS);
    const desc = this.match(TokenType.DASH) || (asc && false);
    const term = super.parseOrdered(parseMethod);
    if (term && desc) {
      term.setArgKey('desc', true);
      term.setArgKey('nullsFirst', false);
    }
    return term;
  }

  parseOrderBy (query: SelectExpr): QueryExpr {
    const lBrace = this.match(TokenType.L_BRACE);
    const expressions = this.parseCsv(() => this.parseOrdered());
    if (lBrace && !this.match(TokenType.R_BRACE)) {
      this.raiseError('Expecting }');
    }
    return query.orderBy(new OrderExpr({ expressions: expressions.filter(Boolean) }), {
      copy: false,
    });
  }

  parseAggregate (): Expression | undefined {
    let alias: string | undefined = undefined;
    if (this.next && this.next.tokenType === TokenType.ALIAS) {
      alias = this.parseIdVar({ anyToken: true })?.name;
      this.match(TokenType.ALIAS);
    }

    const name = this.curr?.text.toUpperCase() ?? '';
    const funcBuilder = this._constructor.FUNCTIONS[name];
    let func: Expression;

    if (funcBuilder) {
      this.advance();
      const args = this.parseColumn();
      func = funcBuilder(args ? [args] : [], { dialect: this.dialect });
    } else {
      this.raiseError(`Unsupported aggregation function ${name}`);
      return undefined;
    }

    if (alias) {
      return new AliasExpr({
        this: func,
        alias,
      });
    }
    return func;
  }

  parseExpression (): Expression | undefined {
    if (this.next && this.next.tokenType === TokenType.ALIAS) {
      const alias = this.parseIdVar({ anyToken: true })?.name;
      this.match(TokenType.ALIAS);
      const parsedExpr = this.parseAssignment();
      if (!parsedExpr) return undefined;
      return new AliasExpr({
        this: parsedExpr,
        alias,
      });
    }
    return this.parseAssignment();
  }

  parseTable (_options: {
    schema?: boolean;
    joins?: boolean;
    aliasTokens?: Set<TokenType>;
    parseBracket?: boolean;
    isDbReference?: boolean;
    parsePartition?: boolean;
    consumePipe?: boolean;
  } = {}): Expression | undefined {
    return this.parseTableParts();
  }

  parseFrom (options: {
    joins?: boolean;
    skipFromToken?: boolean;
  } = {}): FromExpr | undefined {
    const {
      joins = false, skipFromToken = false,
    } = options;
    if (!skipFromToken && !this.match(TokenType.FROM)) {
      return undefined;
    }

    return this.expression(FromExpr, {
      comments: this.prevComments,
      this: this.parseTable({ joins }),
    });
  }
};

export class PRQL extends Dialect {
  static DPIPE_IS_STRING_CONCAT = false;

  static Tokenizer = PRQLTokenizer;

  static Parser = PRQLParser;
}

Dialect.register(Dialects.PRQL, PRQL);
