import { cache } from '../port_internals';
import {
  Generator,
} from '../generator';
import { Parser } from '../parser';
import type { TokenPair } from '../tokens';
import { Tokenizer } from '../tokens';
import type { Expression } from '../expressions';
import {
  CoalesceExpr,
  CountExpr,
  DataTypeExprKind,
  DistinctExpr,
  IfExpr,
  SelectExpr,
  StrPositionExpr,
} from '../expressions';
import { seqGet } from '../helper';
import {
  eliminateDistinctOn, preprocess,
} from '../transforms';
import {
  renameFunc, strPositionSql,
  Dialect, NormalizationStrategy, Dialects,
} from './dialect';

export class TableauTokenizer extends Tokenizer {
  static IDENTIFIERS: TokenPair[] = [['[', ']']];
  static QUOTES = ['\'', '"'];
}

export class TableauParser extends Parser {
  static NO_PAREN_IF_COMMANDS = false;

  @cache
  static get FUNCTIONS (): Record<string, (args: Expression[]) => Expression> {
    return {
      ...Parser.FUNCTIONS,
      COUNTD: (args: Expression[]) => new CountExpr({ this: new DistinctExpr({ expressions: args }) }),
      FIND: (args: Expression[]) => StrPositionExpr.fromArgList(args),
      FINDNTH: (args: Expression[]) => new StrPositionExpr({
        this: seqGet(args, 0),
        substr: seqGet(args, 1),
        occurrence: seqGet(args, 2),
      }),
    };
  }
}

export class TableauGenerator extends Generator {
  static JOIN_HINTS = false;
  static TABLE_HINTS = false;
  static QUERY_HINTS = false;

  @cache
  static get TYPE_MAPPING (): Map<DataTypeExprKind | string, string> {
    const mapping = new Map(Generator.TYPE_MAPPING);
    mapping.set(DataTypeExprKind.BOOLEAN, 'BOOL');
    mapping.set(DataTypeExprKind.TINYINT, 'INT');
    mapping.set(DataTypeExprKind.SMALLINT, 'INT');
    mapping.set(DataTypeExprKind.INT, 'INT');
    mapping.set(DataTypeExprKind.BIGINT, 'INT');
    mapping.set(DataTypeExprKind.DOUBLE, 'FLOAT');
    mapping.set(DataTypeExprKind.FLOAT, 'FLOAT');
    mapping.set(DataTypeExprKind.VARCHAR, 'STR');
    mapping.set(DataTypeExprKind.TEXT, 'STR');
    return mapping;
  }

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (this: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const m = new Map<typeof Expression, (this: Generator, e: any) => string>(Generator.TRANSFORMS);
    m.set(CoalesceExpr, renameFunc('IFNULL'));

    m.set(IfExpr, function (this: Generator, e: IfExpr) {
      return `IF ${this.sql(e, 'this')} THEN ${this.sql(e, 'true')} ELSE ${this.sql(e, 'false')} END`;
    });
    m.set(SelectExpr, preprocess([eliminateDistinctOn]));
    return m;
  }

  public countSql (expression: CountExpr): string {
    const inner = expression.args.this;
    if (inner instanceof DistinctExpr) {
      return this.func('COUNTD', inner.args.expressions ?? []);
    }
    return this.func('COUNT', [inner]);
  }

  public strPositionSql (expression: StrPositionExpr): string {
    const hasOccurrence = expression.args.occurrence !== undefined;
    return strPositionSql.call(this, expression, {
      funcName: hasOccurrence ? 'FINDNTH' : 'FIND',
      supportsOccurrence: hasOccurrence,
    });
  }
}

export class Tableau extends Dialect {
  static LOG_BASE_FIRST = false;
  static NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE;

  static Tokenizer = TableauTokenizer;
  static Parser = TableauParser;
  static Generator = TableauGenerator;
}

Dialect.register(Dialects.TABLEAU, Tableau);
