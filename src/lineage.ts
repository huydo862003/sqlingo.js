import type {
  Expression, QueryExpr,
} from './expressions';
import {
  TableExpr, TagExpr,
  SelectExpr,
  StarExpr,
  SubqueryExpr,
  SetOperationExpr,
  ColumnExpr,
  UdtfExpr,
  PlaceholderExpr,
  UNWRAPPED_QUERIES,
  maybeParse,
  expand,
} from './expressions';
import { SqlglotError } from './errors';
import { normalizeIdentifiers } from './optimizer/normalize_identifiers';
import { qualify } from './optimizer/qualify';
import {
  buildScope,
  Scope, ScopeType, findAllInScope,
} from './optimizer/scope';
import type { DialectType } from './dialects/dialect';
import type { Schema } from './schema';
import { id } from './port_internals';

export interface NodeOptions {
  name: string;
  expression: Expression;
  source: Expression;
  downstream?: Node[];
  sourceName?: string;
  referenceNodeName?: string;
}

export class Node {
  public name: string;
  public expression: Expression;
  public source: Expression;
  public downstream: Node[];
  public sourceName: string;
  public referenceNodeName: string;

  constructor (options: NodeOptions) {
    this.name = options.name;
    this.expression = options.expression;
    this.source = options.source;
    this.downstream = options.downstream || [];
    this.sourceName = options.sourceName || '';
    this.referenceNodeName = options.referenceNodeName || '';
  }

  * walk (): Generator<Node> {
    yield this;
    for (const d of this.downstream) {
      yield* d.walk();
    }
  }

  toHtml (dialect?: DialectType, opts: Record<string, unknown> = {}): GraphHTML {
    const nodes: Record<string, unknown> = {};
    const edges: unknown[] = [];

    for (const node of this.walk()) {
      let label: string;
      let title: string;
      let group: number;

      if (node.expression instanceof TableExpr) {
        label = `FROM ${node.expression.args.this}`;
        title = `<pre>SELECT ${node.name} FROM ${node.expression.args.this}</pre>`;
        group = 1;
      } else {
        label = node.expression.sql({
          pretty: true,
          dialect,
        });

        const sourceSql = node.source.transform((n: Expression) => {
          return n === node.expression
            ? new TagExpr({
              this: n,
              prefix: '<b>',
              postfix: '</b>',
            })
            : n;
        }, { copy: false }).sql({
          pretty: true,
          dialect,
        });

        title = `<pre>${sourceSql}</pre>`;
        group = 0;
      }

      const nodeId = id(node);

      nodes[nodeId] = {
        id: nodeId,
        label,
        title,
        group,
      };

      for (const d of node.downstream) {
        edges.push({
          from: nodeId,
          to: id(d),
        });
      }
    }

    return new GraphHTML(nodes, edges, opts);
  }
}

export interface LineageOptions {
  schema?: Record<string, unknown> | Schema;
  sources?: Record<string, string | QueryExpr>;
  dialect?: DialectType;
  scope?: Scope;
  trimSelects?: boolean;
  copy?: boolean;
  [key: string]: unknown;
}

/**
 * Build the lineage graph for a column of a SQL query.
 *
 * @param column The column to build the lineage for.
 * @param sql The SQL string or expression.
 * @param options Additional options like schema, dialect, scope, etc.
 * @returns A lineage node.
 */
export function lineage (
  column: string | ColumnExpr,
  sql: string | Expression,
  options: LineageOptions = {},
): Node {
  const {
    schema,
    sources,
    dialect,
    scope: providedScope,
    trimSelects = true,
    copy = true,
    ...kwargs
  } = options;

  let expression = maybeParse(sql, {
    copy,
    dialect,
  });
  const normalizedColumn = normalizeIdentifiers(column, { dialect }).name;

  if (sources) {
    const parsedSources: Record<string, QueryExpr> = {};
    for (const [k, v] of Object.entries(sources)) {
      parsedSources[k] = maybeParse(v, {
        copy,
        dialect,
      }) as QueryExpr;
    }

    expression = expand(expression, parsedSources, {
      dialect,
      copy,
    });
  }

  let scope = providedScope;

  if (!scope) {
    expression = qualify(expression, {
      dialect,
      schema,
      validateQualifyColumns: false,
      identify: false,
      ...kwargs,
    });

    scope = buildScope(expression);
  }

  if (!scope) {
    throw new SqlglotError('Cannot build lineage, sql must be SELECT');
  }

  // Assuming scope.expression is a Select expression containing `selects`
  const hasColumn = scope.expression.selects.some(
    (select) => select.aliasOrName === normalizedColumn,
  );

  if (!hasColumn) {
    throw new SqlglotError(`Cannot find column '${normalizedColumn}' in query.`);
  }

  return toNode(normalizedColumn, scope, dialect, { trimSelects });
}

export interface ToNodeOptions {
  scopeName?: string;
  upstream?: Node;
  sourceName?: string;
  referenceNodeName?: string;
  trimSelects?: boolean;
}

export function toNode (
  column: string | number,
  scope: Scope,
  dialect: DialectType | undefined,
  options: ToNodeOptions = {},
): Node {
  let {
    // eslint-disable-next-line prefer-const
    scopeName = undefined,
    upstream = undefined,
    // eslint-disable-next-line prefer-const
    sourceName = undefined,
    // eslint-disable-next-line prefer-const
    referenceNodeName = undefined,
    // eslint-disable-next-line prefer-const
    trimSelects = true,
  } = options;

  // Find the specific select clause that is the source of the column we want.
  // This can either be a specific, named select or a generic `*` clause.
  let select: Expression;
  if (typeof column === 'number') {
    select = scope.expression.selects[column];
  } else {
    const foundSelect = scope.expression.selects.find(
      (s) => s.aliasOrName === column,
    );
    select = foundSelect || (scope.expression.isStar ? new StarExpr({}) : scope.expression);
  }

  if (scope.expression instanceof SubqueryExpr) {
    for (const source of scope.subqueryScopes) {
      return toNode(column, source, dialect, {
        upstream,
        sourceName,
        referenceNodeName,
        trimSelects,
      });
    }
  }

  if (scope.expression instanceof SetOperationExpr) {
    const name = scope.expression.constructor.name.toUpperCase().replace('EXPR', '');
    upstream = upstream || new Node({
      name,
      source: scope.expression,
      expression: select,
    });

    let index: number;
    if (typeof column === 'number') {
      index = column;
    } else {
      index = scope.expression.selects.findIndex(
        (s) => s.aliasOrName === column || s.isStar,
      );
    }

    if (index === -1) {
      throw new Error(`Could not find ${column} in ${scope.expression}`);
    }

    for (const s of scope.unionScopes) {
      toNode(index, s, dialect, {
        upstream,
        sourceName,
        referenceNodeName,
        trimSelects,
      });
    }

    return upstream;
  }

  let source: Expression;
  if (trimSelects && scope.expression instanceof SelectExpr) {
    // For better ergonomics in our node labels, replace the full select with
    // a version that has only the column we care about.
    source = scope.expression.select(select, { append: false }) as Expression;
  } else {
    source = scope.expression;
  }

  // Create the node for this step in the lineage chain, and attach it to the previous one.
  const node = new Node({
    name: scopeName ? `${scopeName}.${column}` : String(column),
    source,
    expression: select,
    sourceName: sourceName || '',
    referenceNodeName: referenceNodeName || '',
  });

  if (upstream) {
    upstream.downstream.push(node);
  }

  const subqueryScopes = new WeakMap<Expression, Scope>();
  for (const subqueryScope of scope.subqueryScopes) {
    subqueryScopes.set(subqueryScope.expression, subqueryScope);
  }

  for (const subquery of findAllInScope<QueryExpr>(select, UNWRAPPED_QUERIES)) {
    const subqueryScope = subqueryScopes.get(subquery);
    if (!subqueryScope) {
      console.warn(`Unknown subquery scope: ${subquery.sql({ dialect })}`);
      continue;
    }

    for (const name of subquery.namedSelects || []) {
      toNode(name, subqueryScope, dialect, {
        upstream: node,
        trimSelects,
      });
    }
  }

  // if the select is a star add all scope sources as downstreams
  if (select instanceof StarExpr) {
    for (const sourceVal of Array.from(scope.sources.values())) {
      let sourceExpr;
      if (sourceVal instanceof Scope) {
        sourceExpr = sourceVal.expression;
      } else {
        sourceExpr = sourceVal;
      }
      node.downstream.push(
        new Node({
          name: select.sql({ comments: false }),
          source: sourceExpr,
          expression: sourceExpr,
        }),
      );
    }
  }

  // Find all columns that went into creating this one to list their lineage nodes.
  const sourceColumnsSet = new Set<ColumnExpr>(
    findAllInScope(select, [ColumnExpr]),
  );

  let derivedTables: Expression[];
  // If the source is a Udtf find columns used in the Udtf to generate the table
  if (source instanceof UdtfExpr) {
    const udtfCols = source.findAll(ColumnExpr);
    for (const c of udtfCols) sourceColumnsSet.add(c);

    derivedTables = Array.from(scope.sources.values())
      .filter((s): s is Scope => s instanceof Scope && s.isDerivedTable)
      .map((s) => s.expression.parent)
      .filter((p): p is Expression => p !== undefined);
  } else {
    derivedTables = scope.derivedTables;
  }

  const sourceNames = new Map<string, string>();
  for (const dt of derivedTables) {
    if (dt.comments && dt.comments[0]?.startsWith('source: ')) {
      sourceNames.set(dt.alias, dt.comments[0].split(' ')[1]);
    }
  }

  const pivots = scope.pivots;
  const pivot = pivots.length === 1 && !pivots[0].unpivot ? pivots[0] : undefined;
  const pivotColumnMapping = new Map<string, ColumnExpr[]>();

  if (pivot) {
    const pivotColumns = pivot.args.columns ?? [];
    const pivotAggsCount = pivot.args.expressions?.length ?? 0;

    pivot.args.expressions?.forEach((agg, i: number) => {
      const aggCols = Array.from(agg.findAll(ColumnExpr)) as ColumnExpr[];
      for (let colIndex = i; colIndex < (pivotColumns?.length || 0); colIndex += pivotAggsCount) {
        pivotColumnMapping.set(pivotColumns[colIndex].name, aggCols);
      }
    });
  }

  for (const c of Array.from(sourceColumnsSet)) {
    const table = c.table;
    let sourceScopeOrExpr: Expression | Scope | undefined = scope.sources.get(table);

    if (sourceScopeOrExpr instanceof Scope) {
      let refNodeName: string | undefined = undefined;
      if (sourceScopeOrExpr.scopeType === ScopeType.DERIVED_TABLE && !sourceNames.has(table)) {
        refNodeName = table;
      } else if (sourceScopeOrExpr.scopeType === ScopeType.CTE) {
        const selectedNode = scope.selectedSources[table]?.[0];
        refNodeName = selectedNode ? selectedNode.name : undefined;
      }

      toNode(c.name, sourceScopeOrExpr, dialect, {
        scopeName: table,
        upstream: node,
        sourceName: sourceNames.get(table) || sourceName,
        referenceNodeName: refNodeName,
        trimSelects,
      });
    } else if (pivot && pivot.aliasOrName === c.table) {
      const downstreamColumns: ColumnExpr[] = [];
      const columnName = c.name;

      if (pivot.args.columns?.some((pc) => pc.name === columnName)) {
        downstreamColumns.push(...(pivotColumnMapping.get(columnName) || []));
      } else {
        // Adapt column to be from the implicit pivoted source
        downstreamColumns.push(new ColumnExpr({
          this: c.args.this,
          table: pivot.parent?.aliasOrName,
        }));
      }

      for (const downstreamColumn of downstreamColumns) {
        const dsTable = downstreamColumn.table;
        let dsSource: Expression | Scope | undefined = scope.sources.get(dsTable);

        if (dsSource instanceof Scope) {
          toNode(downstreamColumn.name, dsSource, dialect, {
            scopeName: dsTable,
            upstream: node,
            sourceName: sourceNames.get(dsTable) || sourceName,
            referenceNodeName,
            trimSelects,
          });
        } else {
          dsSource = dsSource || new PlaceholderExpr({});
          node.downstream.push(
            new Node({
              name: downstreamColumn.sql({ comments: false }),
              source: dsSource,
              expression: dsSource,
            }),
          );
        }
      }
    } else {
      sourceScopeOrExpr = sourceScopeOrExpr || new PlaceholderExpr({});
      node.downstream.push(
        new Node({
          name: c.sql({ comments: false }),
          source: sourceScopeOrExpr,
          expression: sourceScopeOrExpr,
        }),
      );
    }
  }

  return node;
}

/**
 * Node to HTML generator using vis.js.
 *
 * https://visjs.github.io/vis-network/docs/network/
 */
export class GraphHTML {
  public imports: boolean;
  public options: Record<string, unknown>;
  public nodes: Record<string, unknown>;
  public edges: unknown[];

  constructor (
    nodes: Record<string, unknown>,
    edges: unknown[],
    options: {
      imports?: boolean;
      [index: string]: unknown;
    } = {},
  ) {
    const { imports = true } = options;

    this.imports = imports;
    this.nodes = nodes;
    this.edges = edges;

    this.options = {
      height: '500px',
      width: '100%',
      layout: {
        hierarchical: {
          enabled: true,
          nodeSpacing: 200,
          sortMethod: 'directed',
        },
      },
      interaction: {
        dragNodes: false,
        selectable: false,
      },
      physics: {
        enabled: false,
      },
      edges: {
        arrows: 'to',
      },
      nodes: {
        font: '20px monaco',
        shape: 'box',
        widthConstraint: {
          maximum: 300,
        },
      },
      ...(options || {}),
    };
  }

  toString (): string {
    const nodesJson = JSON.stringify(Object.values(this.nodes));
    const edgesJson = JSON.stringify(this.edges);
    const optionsJson = JSON.stringify(this.options);

    const importsHtml = this.imports
      ? `<script type="text/javascript" src="https://unpkg.com/vis-data@latest/peer/umd/vis-data.min.js"></script>
  <script type="text/javascript" src="https://unpkg.com/vis-network@latest/peer/umd/vis-network.min.js"></script>
  <link rel="stylesheet" type="text/css" href="https://unpkg.com/vis-network/styles/vis-network.min.css" />`
      : '';

    return `<div>
  <div id="sqlglot-lineage"></div>
  ${importsHtml}
  <script type="text/javascript">
    var nodes = new vis.DataSet(${nodesJson});
    nodes.forEach(row => {
      row["title"] = new DOMParser().parseFromString(row["title"], "text/html").body.childNodes[0];
    });

    new vis.Network(
        document.getElementById("sqlglot-lineage"),
        {
            nodes: nodes,
            edges: new vis.DataSet(${edgesJson})
        },
        ${optionsJson}
    );
  </script>
</div>`;
  }

  toHtml (): string {
    return this.toString();
  }
}
