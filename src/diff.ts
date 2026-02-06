import type { Expression } from './expressions';

export function diff (source: Expression, target: Expression): unknown {
  throw new Error(`diff not implemented: ${source}, ${target}`);
}
