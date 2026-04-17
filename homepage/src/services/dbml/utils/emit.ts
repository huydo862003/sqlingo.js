import {
  DEFAULT_SCHEMA_NAME,
} from '../constants';
import type {
  DbmlColumnType,
  DbmlEndpoint,
  DbmlSchema,
} from '../types';

function emitEndpoint (e: DbmlEndpoint): string {
  const t = e.schema ? `${e.schema}.${e.table}` : e.table;
  return `${t}.${e.columns.length === 1 ? e.columns[0] : `(${e.columns.join(', ')})`}`;
}

function emitType (t: DbmlColumnType): string {
  const base = t.schema ? `${t.schema}.${t.name}` : t.name;
  const args = t.args?.length ? `(${t.args.join(', ')})` : '';
  let arr = '';
  if (t.array === true) arr = '[]';
  else if (Array.isArray(t.array)) arr = t.array.map((n) => `[${n ?? ''}]`).join('');
  return `${base}${args}${arr}`;
}

export function schemaToDbml (schema: DbmlSchema): string {
  const lines: string[] = [];

  if (schema.project) {
    lines.push(`Project ${schema.project.name ?? 'project'} {`);
    if (schema.project.databaseType) lines.push(`  database_type: '${schema.project.databaseType}'`);
    if (schema.project.note) lines.push(`  note: '${schema.project.note}'`);
    for (const [
      k,
      v,
    ] of Object.entries(schema.project.custom ?? {})) {
      lines.push(`  ${k}: '${v}'`);
    }
    lines.push('}');
    lines.push('');
  }

  for (const en of schema.enums) {
    const qname = en.schema ? `${en.schema}.${en.name}` : en.name;
    lines.push(`Enum ${qname} {`);
    for (const v of en.values) {
      const note = v.note ? ` [note: '${v.note}']` : '';
      lines.push(`  ${v.name}${note}`);
    }
    lines.push('}');
    lines.push('');
  }

  for (const table of schema.tables) {
    const qname = table.schema ? `${table.schema}.${table.name}` : table.name;
    const header = table.alias ? ` as ${table.alias}` : '';
    const settings: string[] = [];
    if (table.headerColor) settings.push(`headercolor: ${table.headerColor}`);
    if (table.note) settings.push(`note: '${table.note}'`);
    const settingsStr = settings.length ? ` [${settings.join(', ')}]` : '';
    lines.push(`Table ${qname}${header}${settingsStr} {`);
    for (const col of table.columns) {
      const s: string[] = [];
      if (col.pk) s.push('pk');
      if (col.increment) s.push('increment');
      if (col.notNull && !col.pk) s.push('not null');
      if (col.unique && !col.pk) s.push('unique');
      if (col.default !== undefined) s.push(`default: \`${col.default}\``);
      if (col.note) s.push(`note: '${col.note}'`);
      if (col.check) s.push(`check: \`${col.check.expression}\``);
      for (const r of col.ref ?? []) s.push(`ref: ${r.relation} ${emitEndpoint(r.target)}`);
      const st = s.length ? ` [${s.join(', ')}]` : '';
      lines.push(`  ${col.name} ${emitType(col.type)}${st}`);
    }
    if (table.indexes?.length) {
      lines.push('');
      lines.push('  indexes {');
      for (const idx of table.indexes) {
        const cols = idx.columns.map((c) => c.isExpression ? `\`${c.expression}\`` : c.expression);
        const colsStr = 1 < cols.length ? `(${cols.join(', ')})` : cols[0];
        const s: string[] = [];
        if (idx.pk) s.push('pk');
        if (idx.unique) s.push('unique');
        if (idx.name) s.push(`name: '${idx.name}'`);
        if (idx.type) s.push(`type: ${idx.type}`);
        if (idx.note) s.push(`note: '${idx.note}'`);
        const st = s.length ? ` [${s.join(', ')}]` : '';
        lines.push(`    ${colsStr}${st}`);
      }
      lines.push('  }');
    }
    if (table.checks?.length) {
      lines.push('');
      lines.push('  Checks {');
      for (const c of table.checks) {
        const s = c.name ? ` [name: '${c.name}']` : '';
        lines.push(`    \`${c.expression}\`${s}`);
      }
      lines.push('  }');
    }
    lines.push('}');
    lines.push('');
  }

  for (const ref of schema.refs) {
    const s: string[] = [];
    if (ref.onDelete) s.push(`delete: ${ref.onDelete}`);
    if (ref.onUpdate) s.push(`update: ${ref.onUpdate}`);
    const st = s.length ? ` [${s.join(', ')}]` : '';
    const nm = ref.name ? ` ${ref.name}` : '';
    lines.push(`Ref${nm}: ${emitEndpoint(ref.source)} ${ref.relation} ${emitEndpoint(ref.target)}${st}`);
  }
  if (schema.refs.length) lines.push('');

  for (const g of schema.tableGroups) {
    lines.push(`TableGroup ${g.name} {`);
    for (const t of g.tables) {
      lines.push(`  ${t.schema === DEFAULT_SCHEMA_NAME ? t.table : `${t.schema}.${t.table}`}`);
    }
    lines.push('}');
    lines.push('');
  }

  for (const sn of schema.stickyNotes) {
    lines.push(`Note ${sn.name} {`);
    lines.push(`  '''${sn.content}'''`);
    lines.push('}');
    lines.push('');
  }

  for (const rec of schema.records) {
    const qname = rec.schema ? `${rec.schema}.${rec.tableName}` : rec.tableName;
    const colList = rec.columns.length ? `(${rec.columns.join(', ')})` : '';
    lines.push(`records ${qname}${colList} {`);
    for (const row of rec.rows) {
      lines.push(`  ${row.join(', ')}`);
    }
    lines.push('}');
    lines.push('');
  }

  return lines.join('\n').trimEnd();
}
