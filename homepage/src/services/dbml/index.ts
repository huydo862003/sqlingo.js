import '../dialects'; // register all dialects as side effect
import {
  parse,
  CommandExpr,
  CreateExpr,
  InsertExpr,
  SchemaExpr,
  TupleExpr,
  ColumnDefExpr,
  DataTypeExpr,
  ColumnConstraintExpr,
  PrimaryKeyColumnConstraintExpr,
  NotNullColumnConstraintExpr,
  UniqueColumnConstraintExpr,
  DefaultColumnConstraintExpr,
  AutoIncrementColumnConstraintExpr,
  GeneratedAsIdentityColumnConstraintExpr,
  ForeignKeyExpr,
  PrimaryKeyExpr,
  ReferenceExpr,
  Expression,
  CreateExprKind,
  DataTypeExprKind,
} from '@hdnax/sqlingo.js';

interface DbmlRef {
  fromTable: string;
  fromCol: string;
  toTable: string;
  toCol: string;
}

interface DbmlColumn {
  name: string;
  type: string;
  pk: boolean;
  notNull: boolean;
  unique: boolean;
  increment: boolean;
  default: string | null;
  ref: DbmlRef | null;
}

interface DbmlTable {
  name: string;
  columns: DbmlColumn[];
}

interface DbmlRecord {
  tableName: string;
  columns: string[];
  rows: string[][];
}

interface DbmlSchema {
  tables: DbmlTable[];
  refs: DbmlRef[];
  records: DbmlRecord[];
}

interface ConversionResult {
  schema: DbmlSchema;
  dbml: string;
}

function mapDataType (dtype: DataTypeExpr): string {
  const kind = dtype.args.this;
  const exprs = dtype.args.expressions ?? [
  ];

  const params = 0 < exprs.length
    ? `(${exprs.map((e) => (e instanceof Expression ? e.name || e.sql() : String(e))).join(', ')})`
    : '';

  switch (kind) {
    case DataTypeExprKind.INT:
    case DataTypeExprKind.INT128:
    case DataTypeExprKind.INT256:
      return 'int';
    case DataTypeExprKind.BIGINT:
    case DataTypeExprKind.BIGSERIAL:
      return 'bigint';
    case DataTypeExprKind.SMALLINT:
      return 'smallint';
    case DataTypeExprKind.TINYINT:
      return 'tinyint';
    case DataTypeExprKind.MEDIUMINT:
      return 'mediumint';
    case DataTypeExprKind.FLOAT:
      return 'float';
    case DataTypeExprKind.DOUBLE:
      return 'double';
    case DataTypeExprKind.DECIMAL:
    case DataTypeExprKind.BIGDECIMAL:
      return `decimal${params}`;
    case DataTypeExprKind.BOOLEAN:
      return 'boolean';
    case DataTypeExprKind.CHAR:
      return `char${params}`;
    case DataTypeExprKind.NCHAR:
      return `nchar${params}`;
    case DataTypeExprKind.VARCHAR:
      return `varchar${params}`;
    case DataTypeExprKind.NVARCHAR:
      return `nvarchar${params}`;
    case DataTypeExprKind.TEXT:
    case DataTypeExprKind.LONGTEXT:
    case DataTypeExprKind.MEDIUMTEXT:
    case DataTypeExprKind.TINYTEXT:
      return 'text';
    case DataTypeExprKind.BLOB:
    case DataTypeExprKind.LONGBLOB:
    case DataTypeExprKind.MEDIUMBLOB:
    case DataTypeExprKind.TINYBLOB:
      return 'blob';
    case DataTypeExprKind.DATE:
      return 'date';
    case DataTypeExprKind.DATETIME:
    case DataTypeExprKind.DATETIME2:
      return `datetime${params}`;
    case DataTypeExprKind.TIMESTAMP:
    case DataTypeExprKind.TIMESTAMPTZ:
    case DataTypeExprKind.TIMESTAMPLTZ:
    case DataTypeExprKind.TIMESTAMPNTZ:
      return `timestamp${params}`;
    case DataTypeExprKind.TIME:
    case DataTypeExprKind.TIMETZ:
      return `time${params}`;
    case DataTypeExprKind.JSON:
      return 'json';
    case DataTypeExprKind.JSONB:
      return 'jsonb';
    case DataTypeExprKind.UUID:
      return 'uuid';
    case DataTypeExprKind.SERIAL:
      return 'serial';
    case DataTypeExprKind.SMALLSERIAL:
      return 'smallserial';
    case DataTypeExprKind.BINARY:
    case DataTypeExprKind.BIT:
      return `binary${params}`;
    case DataTypeExprKind.ENUM:
      return `enum${params}`;
    default:
      if (typeof kind === 'string') return kind;
      return 'unknown';
  }
}

function nodeText (e: Expression): string {
  return e.name || e.sql();
}

export function convertSqlToDbml (sql: string, dialect?: string): ConversionResult {
  const parsed = parse(sql, dialect
    ? {
      read: dialect,
    }
    : {});
  const bad = parsed.find((s) => s instanceof CommandExpr);
  if (bad) throw new Error('Unsupported SQL syntax');
  const schema: DbmlSchema = {
    tables: [
    ],
    refs: [
    ],
    records: [
    ],
  };

  for (const stmt of parsed) {
    if (stmt instanceof InsertExpr) {
      const tableExpr = stmt.args.this;
      if (!(tableExpr instanceof SchemaExpr)) continue;
      const tableName = tableExpr.args.this ? nodeText(tableExpr.args.this) : null;
      if (!tableName) continue;
      const cols = (tableExpr.args.expressions ?? [
      ]).map((e) =>
        e instanceof Expression ? nodeText(e) : String(e));
      const valuesExpr = stmt.args.expression;
      if (!valuesExpr) continue;
      const rows: string[][] = [
      ];
      for (const tuple of valuesExpr.args.expressions ?? [
      ]) {
        if (!(tuple instanceof TupleExpr)) continue;
        rows.push((tuple.args.expressions ?? [
        ]).map((v) =>
          v instanceof Expression ? v.sql() : String(v)));
      }
      if (!rows.length) continue;
      const existing = schema.records.find((r) => r.tableName === tableName);
      if (existing) {
        existing.rows.push(...rows);
      } else {
        schema.records.push({
          tableName,
          columns: cols,
          rows,
        });
      }
      continue;
    }

    if (!(stmt instanceof CreateExpr)) continue;
    if (stmt.kind !== CreateExprKind.TABLE) continue;

    const schemaNode = stmt.args.this;
    if (!(schemaNode instanceof SchemaExpr)) continue;

    const tableName = schemaNode.args.this ? nodeText(schemaNode.args.this) : '?';
    const expressions = schemaNode.args.expressions ?? [
    ];

    const columns: DbmlColumn[] = [
    ];
    const tablePkCols = new Set<string>();
    const tableRefs: DbmlRef[] = [
    ];

    for (const expr of expressions) {
      if (expr instanceof ColumnDefExpr) {
        const name = expr.args.this ? nodeText(expr.args.this) : '?';
        const typeExpr = expr.args.kind;
        const typeStr = typeExpr instanceof DataTypeExpr
          ? mapDataType(typeExpr)
          : typeof typeExpr === 'string'
            ? typeExpr
            : '?';

        let pk = false;
        let notNull = false;
        let unique = false;
        let increment = false;
        let defaultVal: string | null = null;
        let colRef: DbmlRef | null = null;

        for (const constraintExpr of expr.constraints) {
          if (!(constraintExpr instanceof ColumnConstraintExpr)) continue;
          const kind = constraintExpr.args.kind;
          if (!kind) continue;

          if (kind instanceof PrimaryKeyColumnConstraintExpr) {
            pk = true;
            notNull = true;
          } else if (kind instanceof NotNullColumnConstraintExpr) {
            notNull = !kind.args.allowNull;
          } else if (kind instanceof UniqueColumnConstraintExpr) {
            unique = true;
          } else if (kind instanceof AutoIncrementColumnConstraintExpr) {
            increment = true;
          } else if (kind instanceof GeneratedAsIdentityColumnConstraintExpr) {
            increment = true;
          } else if (kind instanceof DefaultColumnConstraintExpr) {
            const thisExpr = kind.args.this;
            defaultVal = thisExpr instanceof Expression ? nodeText(thisExpr) : null;
          } else if (kind instanceof ReferenceExpr) {
            const refTable = kind.args.this ? nodeText(kind.args.this) : null;
            const refCols = kind.args.expressions ?? [
            ];
            if (refTable) {
              colRef = {
                fromTable: tableName,
                fromCol: name,
                toTable: refTable,
                toCol: refCols[0] instanceof Expression ? nodeText(refCols[0]) : name,
              };
            }
          }
        }

        columns.push({
          name,
          type: typeStr,
          pk,
          notNull,
          unique,
          increment,
          default: defaultVal,
          ref: colRef,
        });
      } else if (expr instanceof PrimaryKeyExpr) {
        for (const col of expr.args.expressions ?? [
        ]) {
          if (col instanceof Expression) tablePkCols.add(nodeText(col));
        }
      } else if (expr instanceof ForeignKeyExpr) {
        const fkCols = expr.args.expressions ?? [
        ];
        const ref = expr.args.reference;
        if (ref instanceof ReferenceExpr) {
          const refTable = ref.args.this ? nodeText(ref.args.this) : null;
          const refCols = ref.args.expressions ?? [
          ];
          if (refTable && 0 < fkCols.length && fkCols[0] instanceof Expression) {
            tableRefs.push({
              fromTable: tableName,
              fromCol: nodeText(fkCols[0]),
              toTable: refTable,
              toCol: refCols[0] instanceof Expression ? nodeText(refCols[0]) : nodeText(fkCols[0]),
            });
          }
        }
      }
    }

    for (const col of columns) {
      if (tablePkCols.has(col.name)) {
        col.pk = true;
        col.notNull = true;
      }
    }

    schema.tables.push({
      name: tableName,
      columns,
    });
    schema.refs.push(...tableRefs);
  }

  return {
    schema,
    dbml: renderDbml(schema),
  };
}

function renderDbml (schema: DbmlSchema): string {
  const lines: string[] = [
  ];

  for (const table of schema.tables) {
    lines.push(`Table ${table.name} {`);
    for (const col of table.columns) {
      const settings: string[] = [
      ];
      if (col.pk) settings.push('pk');
      if (col.increment) settings.push('increment');
      if (col.notNull && !col.pk) settings.push('not null');
      if (col.unique && !col.pk) settings.push('unique');
      if (col.default !== null) settings.push(`default: \`${col.default}\``);
      if (col.ref) {
        settings.push(`ref: > ${col.ref.toTable}.${col.ref.toCol}`);
      }
      const settingsStr = 0 < settings.length ? ` [${settings.join(', ')}]` : '';
      lines.push(`  ${col.name} ${col.type}${settingsStr}`);
    }
    lines.push('}');
    lines.push('');
  }

  for (const ref of schema.refs) {
    lines.push(`Ref: ${ref.fromTable}.${ref.fromCol} > ${ref.toTable}.${ref.toCol}`);
  }

  if (schema.refs.length && schema.records.length) lines.push('');

  for (const rec of schema.records) {
    const colList = rec.columns.length ? `(${rec.columns.join(', ')})` : '';
    lines.push(`records ${rec.tableName}${colList} {`);
    for (const row of rec.rows) {
      lines.push(`  ${row.join(', ')}`);
    }
    lines.push('}');
    lines.push('');
  }

  return lines.join('\n').trimEnd();
}
