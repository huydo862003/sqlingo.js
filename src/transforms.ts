import type { Expression } from './expressions';

export function removeNullabilityFromSchema (expression: Expression): Expression {
  throw new Error(`removeNullabilityFromSchema not implemented: ${expression}`);
}

export function unnestToExplode (expression: Expression): Expression {
  throw new Error(`unnestToExplode not implemented: ${expression}`);
}
