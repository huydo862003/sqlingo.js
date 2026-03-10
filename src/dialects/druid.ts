import {
  Generator,
} from '../generator';
import type { Expression } from '../expressions';
import {
  CurrentTimestampExpr,
  ModExpr,
  ArrayExpr,
  DataTypeExprKind,
} from '../expressions';
import { cache } from '../port_internals';
import {
  Dialect, Dialects,
  renameFunc,
} from './dialect';

export class DruidGenerator extends Generator {
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
  static readonly SELECT_KINDS: string[] = [];
  // port from _Dialect metaclass logic
  static TRY_SUPPORTED = false;
  // port from _Dialect metaclass logic
  static SUPPORTS_UESCAPE = false;

  @cache
  static get TYPE_MAPPING (): Map<DataTypeExprKind | string, string> {
    const mapping = new Map(Generator.TYPE_MAPPING);
    mapping.set(DataTypeExprKind.NCHAR, 'STRING');
    mapping.set(DataTypeExprKind.NVARCHAR, 'STRING');
    mapping.set(DataTypeExprKind.TEXT, 'STRING');
    mapping.set(DataTypeExprKind.UUID, 'STRING');
    return mapping;
  }

  @cache
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (this: Generator, e: Expression) => string> {
    const m = new Map<typeof Expression, (this: Generator, e: Expression) => string>(Generator.TRANSFORMS);
    m.set(CurrentTimestampExpr, () => 'CURRENT_TIMESTAMP');
    m.set(ModExpr, renameFunc('MOD'));
    m.set(ArrayExpr, function (this: Generator, e) {
      return `ARRAY[${this.expressions(e)}]`;
    });
    return m;
  };
}

export class Druid extends Dialect {
  static DIALECT_NAME = Dialects.DRUID;
  static Generator = DruidGenerator;
}

Dialect.register(Dialects.DRUID, Druid);
