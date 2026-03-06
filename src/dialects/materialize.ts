import type {
  DataTypeExpr, ListExpr,
} from '../expressions';
import {
  AutoIncrementColumnConstraintExpr,
  CreateExpr, DataTypeExprKind, Expression, GeneratedAsIdentityColumnConstraintExpr, KwargExpr, OnConflictExpr, PrimaryKeyColumnConstraintExpr, PropertyEqExpr, SelectExpr, StructExpr, ToMapExpr,
} from '../expressions';
import type { Generator } from '../generator';
import { seqGet } from '../helper';
import type { Parser } from '../parser';
import {
  cache, narrowInstanceOf,
} from '../port_internals';
import { TokenType } from '../tokens';
import {
  ctasWithTmpTablesToCreateTmpView, preprocess, removeUniqueConstraints,
} from '../transforms';
import {
  Dialect, Dialects,
} from './dialect';
import { Postgres } from './postgres';

class MaterializeParser extends Postgres.Parser {
  static NO_PAREN_FUNCTION_PARSERS = {
    ...Postgres.Parser.NO_PAREN_FUNCTION_PARSERS,
    MAP: (self: Parser) => (self as MaterializeParser).parseMap(),
  };

  static LAMBDAS = {
    ...Postgres.Parser.LAMBDAS,
    [TokenType.FARROW]: (self: Parser, expressions: Expression[]) =>
      self.expression(KwargExpr, {
        this: seqGet(expressions, 0),
        expression: (self as MaterializeParser).parseAssignment(),
      }),
  };

  parseLambdaArg (): Expression | undefined {
    return this.parseField();
  }

  parseMap (): ToMapExpr {
    if (this.match(TokenType.L_PAREN)) {
      const toMap = this.expression(ToMapExpr, { this: this.parseSelect() });
      this.matchRParen();
      return toMap;
    }

    if (!this.match(TokenType.L_BRACKET)) {
      this.raiseError('Expecting [');
    }

    const entries = this.parseWrappedCsv(() => (this as MaterializeParser).parseLambda()).map((e: Expression) =>
      new PropertyEqExpr({
        this: narrowInstanceOf(e.args.this, 'string', Expression),
        expression: narrowInstanceOf(e.args.expression, 'string', Expression),
      }));

    if (!this.match(TokenType.R_BRACKET)) {
      this.raiseError('Expecting ]');
    }

    return this.expression(ToMapExpr, {
      this: this.expression(StructExpr, { expressions: entries }),
    });
  }
}

class MaterializeGenerator extends Postgres.Generator {
  static SUPPORTS_CREATE_TABLE_LIKE = false;
  static SUPPORTS_BETWEEN_FLAGS = false;

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (self: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (self: Generator, e: any) => string>([
      ...Postgres.Generator.TRANSFORMS,
      [AutoIncrementColumnConstraintExpr, () => ''],
      [CreateExpr, preprocess([removeUniqueConstraints, ctasWithTmpTablesToCreateTmpView])],
      [GeneratedAsIdentityColumnConstraintExpr, () => ''],
      [OnConflictExpr, () => ''],
      [PrimaryKeyColumnConstraintExpr, () => ''],
    ]);
    transforms.delete(ToMapExpr);
    return transforms;
  }

  propertyEqSql (expression: PropertyEqExpr): string {
    return this.binary(expression, '=>');
  }

  dataTypeSql (expression: DataTypeExpr): string {
    if (expression.isType(DataTypeExprKind.LIST)) {
      if (expression.args.expressions && 0 < expression.args.expressions.length) {
        return `${this.expressions(expression, { flat: true })} LIST`;
      }
      return 'LIST';
    }

    if (expression.isType(DataTypeExprKind.MAP) && expression.args.expressions?.length === 2) {
      const [key, value] = expression.args.expressions;
      return `MAP[${this.sql(key)} => ${this.sql(value)}]`;
    }

    return super.dataTypeSql(expression);
  }

  listSql (expression: ListExpr): string {
    const firstExpr = seqGet(expression.args.expressions || [], 0);
    if (firstExpr instanceof SelectExpr) {
      return this.func('LIST', [firstExpr]);
    }

    return `${this.normalizeFunc('LIST')}[${this.expressions(expression, { flat: true })}]`;
  }

  toMapSql (expression: ToMapExpr): string {
    if (expression.args.this instanceof SelectExpr) {
      return this.func('MAP', [expression.args.this]);
    }
    return `${this.normalizeFunc('MAP')}[${expression.args.this instanceof Expression ? this.expressions(expression.args.this) : expression.args.this}]`;
  }
}

export class Materialize extends Postgres {
  static Parser = MaterializeParser;
  static Generator = MaterializeGenerator;
}

Dialect.register(Dialects.MATERIALIZE, Materialize);
