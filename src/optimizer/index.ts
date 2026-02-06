import type { Expression } from '../expressions';

export function optimize (expression: Expression): Expression {
  throw new Error(`optimize not implemented: ${expression}`);
}
