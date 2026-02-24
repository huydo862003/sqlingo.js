import type { Expression } from '../expressions';
import { HexStringExpr } from '../expressions';
import type { Generator } from '../generator';
import {
  Dialect, Dialects,
} from './dialect';
import { Trino } from './trino';

class DuneTokenizer extends Trino.Tokenizer {
  static HEX_STRINGS: (string | [string, string])[] = ['0x', ['X\'', '\'']];
}

class DuneGenerator extends Trino.Generator {
  static ORIGINAL_TRANSFORMS = (() => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const transforms = new Map<typeof Expression, (self: Generator, e: any) => string>([...Trino.Generator.TRANSFORMS, [HexStringExpr, (self, e) => `0x${e.args.this}`]]);
    return transforms;
  })();
}

export class Dune extends Trino {
  static Tokenizer = DuneTokenizer;
  static Generator = DuneGenerator;
}

Dialect.register(Dialects.DUNE, Dune);
