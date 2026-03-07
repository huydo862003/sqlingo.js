import type { Expression } from '../expressions';
import { HexStringExpr } from '../expressions';
import type { Generator } from '../generator';
import { cache } from '../port_internals';
import {
  Dialect, Dialects,
} from './dialect';
import { Trino } from './trino';

class DuneTokenizer extends Trino.Tokenizer {
  @cache
  static get HEX_STRINGS (): (string | [string, string])[] { return ['0x', ['X\'', '\'']]; }
}

class DuneGenerator extends Trino.Generator {
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
  static Tokenizer = DuneTokenizer;
  static Generator = DuneGenerator;
}

Dialect.register(Dialects.DUNE, Dune);
