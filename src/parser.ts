// https://github.com/tobymao/sqlglot/blob/main/sqlglot/parser.py

import type { Expression } from './expressions';
import {
  array,
  EscapeExpr,
  LikeExpr,
  StarMapExpr,
  VarMapExpr,
} from './expressions';
import { seqGet } from './helper';

export type OptionsType = Record<string, (string[] | string)[]>;

// Used to detect alphabetical characters and +/- in timestamp literals
export const TIME_ZONE_RE: RegExp = /:.*?[a-zA-Z+\-]/;

export function buildVarMap (args: Expression[]): StarMapExpr | VarMapExpr {
  if (args.length === 1 && args[0].isStar) {
    return new StarMapExpr({ this: args[0] });
  }

  const keys: Expression[] = [];
  const values: Expression[] = [];
  for (let i = 0; i < args.length; i += 2) {
    keys.push(args[i]);
    values.push(args[i + 1]);
  }

  return new VarMapExpr({
    keys: array(keys, { copy: false }),
    values: array(values, { copy: false }),
  });
}

export function buildLike (args: Expression[]): EscapeExpr | LikeExpr {
  if (args.length < 2) {
    throw new Error('buildLike only accept expression lists with at least 2 expressions');
  }
  const like = new LikeExpr({
    this: seqGet(args, 1)!,
    expression: seqGet(args, 0)!,
  });
  return 2 < args.length
    ? new EscapeExpr({
      this: like,
      expression: seqGet(args, 2)!,
    })
    : like;
}

export function binaryRangeParser (
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  exprType: new (args: any) => Expression,
  options: { reverseArgs?: boolean } = {},
): (parser: Parser, thisExpr: Expression | undefined) => Expression | undefined {
  const { reverseArgs = false } = options;

  return function parseBinaryRange (
    parser: Parser,
    thisExpr: Expression | undefined,
  ): Expression | undefined {
    let expression = parser._parseBitwise();
    let thisArg = thisExpr;

    if (reverseArgs) {
      [thisArg, expression] = [expression, thisArg];
    }

    return parser._parseEscape(
      parser.expression(exprType, {
        this: thisArg,
        expression,
      }),
    );
  };
}
