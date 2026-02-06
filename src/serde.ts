import type { Expression } from './expressions';

export function dump (expression: Expression): string {
  throw new Error(`dump not implemented: ${expression}`);
}

export function load (data: string): Expression {
  throw new Error(`load not implemented: ${data}`);
}
