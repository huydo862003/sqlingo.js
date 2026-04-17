import {
  Expression, type ReferenceExpr,
} from '@hdnax/sqlingo.js';
import {
  DbmlEndpoint, DbmlRefAction,
} from '../types';

const ACTION_BY_TOKEN: Record<string, DbmlRefAction> = {
  CASCADE: DbmlRefAction.CASCADE,
  RESTRICT: DbmlRefAction.RESTRICT,
  'SET NULL': DbmlRefAction.SET_NULL,
  'SET DEFAULT': DbmlRefAction.SET_DEFAULT,
  'NO ACTION': DbmlRefAction.NO_ACTION,
};

function tokenize (s: string): string[] {
  return s.trim().toUpperCase()
    .split(/\s+/)
    .filter(Boolean);
}

function actionFromTokens (tokens: string[]): DbmlRefAction | undefined {
  const joined = tokens.join(' ');
  return ACTION_BY_TOKEN[joined];
}

export function parseActionExpr (e: Expression | undefined): DbmlRefAction | undefined {
  if (!e) return undefined;
  return actionFromTokens(tokenize(e.sql()));
}

/**
 * ReferenceExpr.options are pre-tokenized strings like "ON DELETE CASCADE" emitted by sqlingo.
 * Split into category + action tokens.
 */
export function referenceActions (
  ref: ReferenceExpr,
): {
  onDelete?: DbmlRefAction;
  onUpdate?: DbmlRefAction;
} {
  const out: {
    onDelete?: DbmlRefAction;
    onUpdate?: DbmlRefAction;
  } = {};
  for (
    const opt of ref.args.options ?? []
  ) {
    const raw = opt instanceof Expression ? opt.sql() : String(opt);
    const tokens = tokenize(raw);
    if (tokens.length < 3 || tokens[0] !== 'ON') continue;
    const category = tokens[1];
    const action = actionFromTokens(tokens.slice(2));
    if (!action) continue;
    if (category === 'DELETE') out.onDelete = action;
    else if (category === 'UPDATE') out.onUpdate = action;
  }
  return out;
}

export function endpoint (schemaName: string | undefined, table: string, columns: string[]): DbmlEndpoint {
  return new DbmlEndpoint({
    schema: schemaName,
    table,
    columns,
  });
}
