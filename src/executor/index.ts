import type { Expression } from '../expressions';

export function execute (expression: Expression): unknown {
  throw new Error(`execute not implemented: ${expression}`);
}
