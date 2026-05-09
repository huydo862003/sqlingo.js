import {
  Expression, type ReferenceExpr,
} from '@hdnax/sqlingo.js';
import {
  DbmlEndpoint, DbmlReferenceAction,
} from '../../types';

// Parse foreign key actions and build dbml model reference endpoints from sqlingo.js AST

const ACTION_BY_TOKEN: Record<string, DbmlReferenceAction> = {
  CASCADE: DbmlReferenceAction.CASCADE,
  RESTRICT: DbmlReferenceAction.RESTRICT,
  'SET NULL': DbmlReferenceAction.SET_NULL,
  'SET DEFAULT': DbmlReferenceAction.SET_DEFAULT,
  'NO ACTION': DbmlReferenceAction.NO_ACTION,
};

export function buildEndpoint (schemaName: string | undefined, table: string, columns: string[]): DbmlEndpoint {
  return new DbmlEndpoint({
    schema: schemaName,
    table,
    columns,
  });
}

/**
 * Extract ON DELETE / ON UPDATE actions from a ReferenceExpr's option list.
 * Options are strings like "ON DELETE CASCADE" emitted by sqlingo
 */
export function extractReferenceActions (
  ref: ReferenceExpr,
): {
  onDelete?: DbmlReferenceAction;
  onUpdate?: DbmlReferenceAction;
} {
  const out: {
    onDelete?: DbmlReferenceAction;
    onUpdate?: DbmlReferenceAction;
  } = {};

  for (const option of ref.args.options ?? []) {
    const raw = option instanceof Expression ? option.sql() : String(option);
    const words = splitWords(raw);

    if (words.length < 3 || words[0] !== 'ON') continue;
    const category = words[1];
    const action = resolveAction(words.slice(2));

    if (!action) continue;
    if (category === 'DELETE') out.onDelete = action;
    else if (category === 'UPDATE') out.onUpdate = action;
  }

  return out;
}

export function parseActionExpr (expr: Expression | undefined): DbmlReferenceAction | undefined {
  if (!expr) return undefined;

  return resolveAction(splitWords(expr.sql()));
}

function resolveAction (words: string[]): DbmlReferenceAction | undefined {
  return ACTION_BY_TOKEN[words.join(' ')];
}

function splitWords (text: string): string[] {
  return text.trim().toUpperCase()
    .split(/\s+/)
    .filter(Boolean);
}
