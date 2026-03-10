import type {
  DataTypeExpr, ListExpr,
} from '../expressions';
import {
  AutoIncrementColumnConstraintExpr,
  CreateExpr, DataTypeExprKind, Expression, GeneratedAsIdentityColumnConstraintExpr, KwargExpr, OnConflictExpr, PrimaryKeyColumnConstraintExpr, PropertyEqExpr, SelectExpr, StructExpr, ToMapExpr,
} from '../expressions';
import type { Generator } from '../generator';
import { seqGet } from '../helper';
import { Parser } from '../parser';
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
  @cache
  static get ID_VAR_TOKENS (): Set<TokenType> {
    return new Set([
      ...Parser.ID_VAR_TOKENS,
      TokenType.SESSION_USER,
      TokenType.CURRENT_CATALOG,
      TokenType.STRAIGHT_JOIN,
    ]);
  }

  // port from _Dialect metaclass logic
  @cache
  static get NO_PAREN_FUNCTIONS () {
    const noParenFunctions = { ...Postgres.Parser.NO_PAREN_FUNCTIONS };
    delete noParenFunctions[TokenType.LOCALTIME];
    delete noParenFunctions[TokenType.LOCALTIMESTAMP];
    return noParenFunctions;
  }

  @cache
  static get NO_PAREN_FUNCTION_PARSERS () {
    return {
      ...Postgres.Parser.NO_PAREN_FUNCTION_PARSERS,
      MAP: function (this: Parser) {
        return (this as MaterializeParser).parseMap();
      },
    };
  }

  @cache
  static get LAMBDAS () {
    return {
      ...Postgres.Parser.LAMBDAS,
      [TokenType.FARROW]: function (this: Parser, expressions: Expression[]) {
        return this.expression(KwargExpr, {
          this: seqGet(expressions, 0),
          expression: (this as MaterializeParser).parseAssignment(),
        });
      },
    };
  }

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

    const entries = this.parseCsv(() => (this as MaterializeParser).parseLambda()).map((e: Expression) =>
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

  // port from _Dialect metaclass logic
  @cache
  static get TABLE_ALIAS_TOKENS (): Set<TokenType> {
    return new Set([...Postgres.Parser.TABLE_ALIAS_TOKENS, TokenType.STRAIGHT_JOIN]);
  }
}
class MaterializeGenerator extends Postgres.Generator {
  // port from _Dialect metaclass logic
  @cache
  static get AFTER_HAVING_MODIFIER_TRANSFORMS () {
    const modifiers = new Map(super.AFTER_HAVING_MODIFIER_TRANSFORMS);
    [
      'cluster',
      'distribute',
      'sort',
    ].forEach((m) => modifiers.delete(m));
    return modifiers;
  }

  // port from _Dialect metaclass logic
  static SUPPORTS_DECODE_CASE = false;
  // port from _Dialect metaclass logic
  static TRY_SUPPORTED = false;
  // port from _Dialect metaclass logic
  static SUPPORTS_UESCAPE = false;
  static SUPPORTS_CREATE_TABLE_LIKE = false;
  static SUPPORTS_BETWEEN_FLAGS = false;

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (this: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (this: Generator, e: any) => string>([
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
  static DIALECT_NAME = Dialects.MATERIALIZE;
  static Parser = MaterializeParser;
  static Generator = MaterializeGenerator;
}

Dialect.register(Dialects.MATERIALIZE, Materialize);
