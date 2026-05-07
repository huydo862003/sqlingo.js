import {
  Expression, type ReferenceExpr,
} from '@hdnax/sqlingo.js';
import {
  DbmlEndpoint, DbmlReferenceAction,
} from '../types';

const ACTION_BY_TOKEN: Record<string, DbmlReferenceAction> = {
  CASCADE: DbmlReferenceAction.CASCADE,
  RESTRICT: DbmlReferenceAction.RESTRICT,
  'SET NULL': DbmlReferenceAction.SET_NULL,
  'SET DEFAULT': DbmlReferenceAction.SET_DEFAULT,
  'NO ACTION': DbmlReferenceAction.NO_ACTION,
};

function tokenize (text: string): string[] {
  return text.trim().toUpperCase()
    .split(/\s+/)
    .filter(Boolean);
}

function actionFromTokens (tokens: string[]): DbmlReferenceAction | undefined {
  const joined = tokens.join(' ');
  return ACTION_BY_TOKEN[joined];
}

export function parseActionExpr (expr: Expression | undefined): DbmlReferenceAction | undefined {
  if (!expr) return undefined;
  return actionFromTokens(tokenize(expr.sql()));
}

/**
 * ReferenceExpr.options are pre-tokenized strings like "ON DELETE CASCADE" emitted by sqlingo.
 * Split into category + action tokens
 */
export function referenceActions (
  ref: ReferenceExpr,
): {
  onDelete?: DbmlReferenceAction;
  onUpdate?: DbmlReferenceAction;
} {
  const out: {
    onDelete?: DbmlReferenceAction;
    onUpdate?: DbmlReferenceAction;
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
