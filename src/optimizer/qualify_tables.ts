// https://github.com/tobymao/sqlglot/blob/main/sqlglot/optimizer/qualify_tables.py

import {
  Expression,
  FromExpr,
  FuncExpr,
  IdentifierExpr,
  JoinExpr,
  parseIdentifier,
  QueryExpr,
  select,
  SubqueryExpr,
  TableAliasExpr,
  TableExpr,
  toIdentifier,
  ValuesExpr,
  WithExpr,
} from '../expressions';
import {
  Dialect, type DialectType,
} from '../dialects/dialect';
import {
  ensureIterable, nameSequence, seqGet,
} from '../helper';
import { normalizeIdentifiers } from './normalize_identifiers';
import {
  Scope, traverseScope,
} from './scope';

/**
 * Rewrite sqlglot AST to have fully qualified tables. Join constructs such as
 * (t1 JOIN t2) AS t will be expanded into (SELECT * FROM t1 AS t1, t2 AS t2) AS t.
 *
 * Examples:
 *     ```ts
 *     import { parseOne } from 'sqlglot';
 *     import { qualifyTables } from 'sqlglot/optimizer';
 *
 *     const expression = parseOne("SELECT 1 FROM tbl");
 *     qualifyTables(expression, { db: "db" }).sql();
 *     // 'SELECT 1 FROM db.tbl AS tbl'
 *
 *     const expression2 = parseOne("SELECT 1 FROM (t1 JOIN t2) AS t");
 *     qualifyTables(expression2).sql();
 *     // 'SELECT 1 FROM (SELECT * FROM t1 AS t1, t2 AS t2) AS t'
 *     ```
 *
 * @param expression - Expression to qualify
 * @param options - Qualification options
 * @param options.db - Database name
 * @param options.catalog - Catalog name
 * @param options.onQualify - Callback after a table has been qualified
 * @param options.dialect - The dialect to parse catalog and schema into
 * @param options.canonicalizeTableAliases - Whether to use canonical aliases (_0, _1, ...)
 *   for all sources instead of preserving table names. Defaults to false.
 * @returns The qualified expression
 */
export function qualifyTables<E extends Expression> (
  expression: E,
  options: {
    db?: string | IdentifierExpr;
    catalog?: string | IdentifierExpr;
    onQualify?: (table: TableExpr) => void;
    dialect?: DialectType;
    canonicalizeTableAliases?: boolean;
  } = {},
): E {
  const {
    db: dbArg,
    catalog: catalogArg,
    onQualify,
    dialect: dialectArg,
    canonicalizeTableAliases = false,
  } = options;

  const dialect = Dialect.getOrRaise(dialectArg);
  const nextAliasName = nameSequence('_');

  let db: IdentifierExpr | undefined;
  if (dbArg) {
    db = parseIdentifier(dbArg, { dialect });
    db.meta.isTable = true;
    db = normalizeIdentifiers(db, { dialect });
  }

  let catalog: IdentifierExpr | undefined;
  if (catalogArg) {
    catalog = parseIdentifier(catalogArg, { dialect });
    catalog.meta.isTable = true;
    catalog = normalizeIdentifiers(catalog, { dialect });
  }

  const qualify = (table: TableExpr): void => {
    if (table.args.this instanceof IdentifierExpr) {
      if (db && !table.args.db) {
        table.args.db = db.copy();
      }
      if (catalog && !table.args.catalog && table.args.db) {
        table.args.catalog = catalog.copy();
      }
    }
  };

  if ((db || catalog) && !(expression instanceof QueryExpr)) {
    const withClause = expression.getArgKey('with') as WithExpr | undefined;
    const with_ = withClause || new WithExpr({ expressions: [] });
    const cteNames = new Set(
      (with_.args.expressions || []).map((cte) => cte.aliasOrName),
    );

    for (const node of expression.walk({
      prune: (n) => n instanceof QueryExpr,
    })) {
      if (node instanceof TableExpr) {
        if (!cteNames.has(node.name)) {
          qualify(node);
        }
      }
    }
  }

  const setAlias = (
    expr: Expression,
    canonicalAliases: Map<string, string>,
    options: {
      targetAlias?: string;
      scope?: Scope;
      normalize?: boolean;
      columns?: (string | IdentifierExpr)[];
    } = {},
  ): void => {
    const {
      targetAlias, scope, normalize = false, columns,
    } = options;

    let alias = expr.getArgKey('alias') as TableAliasExpr | undefined;
    if (!alias) {
      alias = new TableAliasExpr({});
    }

    let newAliasName: string;
    if (canonicalizeTableAliases) {
      newAliasName = nextAliasName();
      canonicalAliases.set(alias.name || targetAlias || '', newAliasName);
    } else if (!alias.name) {
      newAliasName = targetAlias || nextAliasName();
      if (normalize && targetAlias) {
        newAliasName = normalizeIdentifiers(newAliasName, { dialect }).name;
      }
    } else {
      return;
    }

    alias.args.this = toIdentifier(newAliasName);

    if (columns) {
      alias.args.columns = columns.map((c) => toIdentifier(c));
    }

    expr.setArgKey('alias', alias);

    if (scope) {
      scope.renameSource('', newAliasName);
    }
  };

  for (const scope of traverseScope(expression)) {
    const localColumns = scope.localColumns;
    const canonicalAliases = new Map<string, string>();

    for (const query of scope.subqueries) {
      const subquery = query.parent;
      if (subquery && subquery instanceof SubqueryExpr) {
        subquery.unnest().replace(subquery);
      }
    }

    for (const derivedTable of scope.derivedTables) {
      const unnested = derivedTable.unnest();
      if (unnested instanceof TableExpr) {
        const joins = unnested.args.joins;
        unnested.args.joins = undefined;

        const derivedThis = derivedTable.args.this;
        if (derivedThis instanceof Expression) {
          derivedThis.replace(
            select('*').from(unnested.copy(), { copy: false }),
          );
          derivedThis.args.joins = joins;
        }
      }

      setAlias(derivedTable, canonicalAliases, { scope });

      const pivot = seqGet(derivedTable.getArgKey('pivots') as Expression[] || [], 0);
      if (pivot) {
        setAlias(pivot, canonicalAliases);
      }
    }

    const tableAliases = new Map<string, IdentifierExpr>();

    for (const [name, source] of scope.sources) {
      if (source instanceof TableExpr) {
        const isRealTableSource = Boolean(name);

        const pivots = source.args.pivots;
        const pivot = pivots ? seqGet(pivots, 0) : undefined;
        let sourceName = name;
        if (pivot) {
          sourceName = source.name;
        }

        const tableThis = source.args.this;
        const tableAlias = source.args.alias;
        let functionColumns: (string | IdentifierExpr)[] = [];

        if (tableThis && tableThis instanceof FuncExpr) {
          const func = tableThis as FuncExpr;
          const funcTypeName = func.constructor.name;

          if (!tableAlias) {
            const defaultCols = dialect._constructor.DEFAULT_FUNCTIONS_COLUMN_NAMES.get(funcTypeName);
            functionColumns = defaultCols ? Array.from(ensureIterable(defaultCols)) : [];
          } else if (tableAlias instanceof TableAliasExpr && tableAlias.args.columns?.length) {
            functionColumns = tableAlias.columns as IdentifierExpr[];
          } else if (dialect._constructor.DEFAULT_FUNCTIONS_COLUMN_NAMES.has(funcTypeName)) {
            functionColumns = Array.from(ensureIterable(source.aliasOrName));
            source.setArgKey('alias', undefined);
            sourceName = '';
          }
        }

        setAlias(source, canonicalAliases, {
          targetAlias: sourceName || source.name || undefined,
          normalize: true,
          columns: functionColumns,
        });

        const sourceFqn = source.parts.map((p) => p.name).join('.');
        const sourceAliasThis = source.args.alias?.args.this;
        if (sourceAliasThis instanceof IdentifierExpr) {
          tableAliases.set(sourceFqn, sourceAliasThis.copy());
        }

        if (pivot) {
          const targetAlias = pivot.getArgKey('unpivot') ? undefined : source.alias;
          setAlias(pivot, canonicalAliases, {
            targetAlias,
            normalize: true,
          });

          if (scope.sources.get(source.aliasOrName) instanceof Scope) {
            continue;
          }
        }

        if (isRealTableSource) {
          qualify(source);

          if (onQualify) {
            onQualify(source);
          }
        }
      } else if (source instanceof Scope && source.isUdtf) {
        const udtf = source.expression;
        setAlias(udtf, canonicalAliases);

        if (udtf instanceof ValuesExpr) {
          const tableAlias = udtf.getArgKey('alias');
          if (tableAlias instanceof TableAliasExpr && !tableAlias.args.columns?.length) {
            tableAlias.args.columns = dialect
              .generateValuesAliases(udtf)
              .map((i: IdentifierExpr) => normalizeIdentifiers(i, { dialect }));
          }
        }
      }
    }

    for (const table of scope.tables) {
      if (!table.alias && table.parent) {
        const parent = table.parent;
        if (parent instanceof FromExpr || parent instanceof JoinExpr) {
          setAlias(table, canonicalAliases, { targetAlias: table.name });
        }
      }
    }

    for (const column of localColumns) {
      const table = column.table;

      if (column.args.db) {
        const columnParts = column.parts.slice(0, -1).map((p) => p.name)
          .join('.');
        const tableAlias = tableAliases.get(columnParts);

        if (tableAlias) {
          column.args.table = undefined;
          column.args.db = undefined;
          column.args.catalog = undefined;
          column.args.table = tableAlias.copy();
        }
      } else if (0 < canonicalAliases.size && table) {
        const canonicalTable = canonicalAliases.get(table);
        if (canonicalTable && canonicalTable !== column.table) {
          column.args.table = toIdentifier(canonicalTable);
        }
      }
    }
  }

  return expression;
}
