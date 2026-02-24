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
import {
  Dialect, Dialects,
  renameFunc,
} from './dialect';

export class DruidGenerator extends Generator {
  public static TYPE_MAPPING: Map<DataTypeExprKind | string, string> = (() => {
    const mapping = new Map(Generator.TYPE_MAPPING);
    mapping.set(DataTypeExprKind.NCHAR, 'STRING');
    mapping.set(DataTypeExprKind.NVARCHAR, 'STRING');
    mapping.set(DataTypeExprKind.TEXT, 'STRING');
    mapping.set(DataTypeExprKind.UUID, 'STRING');
    return mapping;
  })();

  public static ORIGINAL_TRANSFORMS: Map<typeof Expression, (self: Generator, e: Expression) => string> = (() => {
    const m = new Map<typeof Expression, (self: Generator, e: Expression) => string>(Generator.TRANSFORMS);
    m.set(CurrentTimestampExpr, () => 'CURRENT_TIMESTAMP');
    m.set(ModExpr, renameFunc('MOD'));
    m.set(ArrayExpr, (self, e) => `ARRAY[${self.expressions(e)}]`);
    return m;
  })();
}

export class Druid extends Dialect {
  public static Generator = DruidGenerator;
}

Dialect.register(Dialects.DRUID, Druid);
