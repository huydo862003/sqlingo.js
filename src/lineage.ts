import type { Expression } from './expressions';

export function lineage (expression: Expression): unknown {
  throw new Error(`lineage not implemented: ${expression}`);
}
