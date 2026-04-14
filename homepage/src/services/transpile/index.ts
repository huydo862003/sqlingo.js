import '../dialects';
import {
  transpile as _transpile, parse, CommandExpr,
} from '@hdnax/sqlingo.js';

export function transpile (
  sql: string,
  opts: {read?: string;
    write?: string;
    pretty?: boolean;},
): string[] {
  const parsed = parse(sql, opts.read
    ? {
      read: opts.read,
    }
    : {});
  if (parsed.some((s) => s instanceof CommandExpr)) throw new Error('Unsupported SQL syntax');
  return _transpile(sql, opts);
}
