import type { Expression } from '../expressions';
import { HexStringExpr } from '../expressions';
import type { Generator } from '../generator';
import { cache } from '../port_internals';
import type { TokenPair } from '../tokens';
import {
  Dialect, Dialects,
} from './dialect';
import { Trino } from './trino';

class DuneTokenizer extends Trino.Tokenizer {
  @cache
  static get HEX_STRINGS (): TokenPair[] {
    return ['0x', ['X\'', '\'']];
  }
}

class DuneGenerator extends Trino.Generator {
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

  @cache
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static get ORIGINAL_TRANSFORMS (): Map<typeof Expression, (this: Generator, e: any) => string> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (this: Generator, e: any) => string>([
      ...Trino.Generator.TRANSFORMS,
      [
        HexStringExpr,
        function (this: Generator, e) {
          return `0x${e.args.this}`;
        },
      ],
    ]);
    return transforms;
  }
}

export class Dune extends Trino {
  static DIALECT_NAME = Dialects.DUNE;
  static Tokenizer = DuneTokenizer;
  static Generator = DuneGenerator;
}

Dialect.register(Dialects.DUNE, Dune);
