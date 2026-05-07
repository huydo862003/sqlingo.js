import '../dialects';
import {
  transpile as _transpile, parse, CommandExpr,
} from '@hdnax/sqlingo.js';

export function transpile (
  sql: string,
  options: {
    read?: string;
    write?: string;
    pretty?: boolean;
  },
): string[] {
  const parsed = parse(sql, options.read
    ? {
      read: options.read,
    }
    : {});
  if (parsed.some((stmt) => stmt instanceof CommandExpr)) throw new Error('Unsupported SQL syntax');
  return _transpile(sql, options);
}
