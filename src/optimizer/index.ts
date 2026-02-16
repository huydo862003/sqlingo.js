export {
  optimize, RULES,
} from './optimizer';

export {
  annotateTypes, TypeAnnotator,
} from './annotate_types';
export { canonicalize } from './canonicalize';
export { eliminateCtes } from './eliminate_ctes';
export {
  eliminateJoins, joinCondition,
} from './eliminate_joins';
export { eliminateSubqueries } from './eliminate_subqueries';
export { isolateTableSelects } from './isolate_table_selects';
export { mergeSubqueries } from './merge_subqueries';
export {
  normalize,
  normalized,
  normalizationDistance,
} from './normalize';
export { normalizeIdentifiers } from './normalize_identifiers';
export { pushdownPredicates } from './pushdown_predicates';
export { pushdownProjections } from './pushdown_projections';
export {
  simplify, flatten, Simplifier,
} from './simplify';
export { unnestSubqueries } from './unnest_subqueries';
export {
  optimizeJoins,
  reorderJoins,
  normalize as normalizeJoins,
  otherTableNames,
  isReorderable,
} from './optimize_joins';
export { qualify } from './qualify';
export {
  qualifyColumns,
  validateQualifyColumns,
  quoteIdentifiers,
  pushdownCteAliasColumns,
  qualifyOutputs,
} from './qualify_columns';
export { qualifyTables } from './qualify_tables';
export { Resolver } from './resolver';
export {
  Scope,
  ScopeType,
  traverseScope,
  buildScope,
  walkInScope,
  findInScope,
  findAllInScope,
} from './scope';
