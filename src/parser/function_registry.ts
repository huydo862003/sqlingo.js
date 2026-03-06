import type { FuncExpr } from '../expressions';

export const FUNCTION_BY_NAME = new Map<string, typeof FuncExpr>();
export const ALL_FUNCTIONS = new Set<typeof FuncExpr>();

export function registerFunc (cls: typeof FuncExpr): void {
  ALL_FUNCTIONS.add(cls);
  const sqlNames = cls.sqlNames();
  for (const name of sqlNames) {
    FUNCTION_BY_NAME.set(name.toUpperCase(), cls);
  }
}
