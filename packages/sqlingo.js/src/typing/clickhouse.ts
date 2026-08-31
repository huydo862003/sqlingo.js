import type {
  Expression,
} from '../expressions/expressions';
import {
  cache,
} from '../port_internals';
import {
  DataTypeExprKind,
} from '../expressions/types';
import {
  CountIfExpr,
} from '../expressions/expressions';
import type {
  ExpressionMetadata,
} from './dialect';
import {
  DialectTyping,
} from './dialect';

export class ClickHouseTyping {
  @cache
  static get EXPRESSION_METADATA (): ExpressionMetadata {
    const map: ExpressionMetadata = new Map(DialectTyping.EXPRESSION_METADATA);

    const extend = (types: (typeof Expression)[], data: Record<string, unknown>) => {
      for (const type of types) map.set(type, data);
    };

    extend([CountIfExpr], {
      returns: DataTypeExprKind.UBIGINT,
    });

    return map;
  }
}
