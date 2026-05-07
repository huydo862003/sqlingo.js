import {
  DEFAULT_SCHEMA_NAME,
} from '../constants';
import type {
  DbmlColumnType,
  DbmlEndpoint,
  DbmlSchema,
} from '../types';

function emitEndpoint (ep: DbmlEndpoint): string {
  const table = ep.schema ? `${ep.schema}.${ep.table}` : ep.table;
  return `${table}.${ep.columns.length === 1 ? ep.columns[0] : `(${ep.columns.join(', ')})`}`;
}

function emitType (type: DbmlColumnType): string {
  const base = type.schema ? `${type.schema}.${type.name}` : type.name;
  const arguments_ = type.args?.length ? `(${type.args.join(', ')})` : '';
  let array = '';
  if (type.array === true) array = '[]';
  else if (Array.isArray(type.array)) array = type.array.map((n) => `[${n ?? ''}]`).join('');
  return `${base}${arguments_}${array}`;
}

export function schemaToDbml (schema: DbmlSchema): string {
  const lines: string[] = [];

  if (schema.project) {
    lines.push(`Project ${schema.project.name ?? 'project'} {`);
    if (schema.project.databaseType) lines.push(`  database_type: '${schema.project.databaseType}'`);
    if (schema.project.note) lines.push(`  note: '${schema.project.note}'`);
    for (const [
      key,
      value,
    ] of Object.entries(schema.project.custom ?? {})) {
      lines.push(`  ${key}: '${value}'`);
    }
    lines.push('}');
    lines.push('');
  }

  for (const en of schema.enums) {
    const qname = en.schema ? `${en.schema}.${en.name}` : en.name;
    lines.push(`Enum ${qname} {`);
    for (const value of en.values) {
      const note = value.note ? ` [note: '${value.note}']` : '';
      lines.push(`  ${value.name}${note}`);
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
    const settingsString = settings.length ? ` [${settings.join(', ')}]` : '';
    lines.push(`Table ${qname}${header}${settingsString} {`);
    for (const col of table.columns) {
      const settings: string[] = [];
      if (col.pk) settings.push('pk');
      if (col.increment) settings.push('increment');
      if (col.notNull && !col.pk) settings.push('not null');
      if (col.unique && !col.pk) settings.push('unique');
      if (col.default !== undefined) settings.push(`default: \`${col.default}\``);
      if (col.note) settings.push(`note: '${col.note}'`);
      if (col.check) settings.push(`check: \`${col.check.expression}\``);
      for (const ref of col.ref ?? []) settings.push(`ref: ${ref.relation} ${emitEndpoint(ref.target)}`);
      const st = settings.length ? ` [${settings.join(', ')}]` : '';
      lines.push(`  ${col.name} ${emitType(col.type)}${st}`);
    }
    if (table.indexes?.length) {
      lines.push('');
      lines.push('  indexes {');
      for (const index of table.indexes) {
        const cols = index.columns.map((col) => col.isExpression ? `\`${col.expression}\`` : col.expression);
        const colsString = 1 < cols.length ? `(${cols.join(', ')})` : cols[0];
        const indexSettings: string[] = [];
        if (index.pk) indexSettings.push('pk');
        if (index.unique) indexSettings.push('unique');
        if (index.name) indexSettings.push(`name: '${index.name}'`);
        if (index.type) indexSettings.push(`type: ${index.type}`);
        if (index.note) indexSettings.push(`note: '${index.note}'`);
        const st = indexSettings.length ? ` [${indexSettings.join(', ')}]` : '';
        lines.push(`    ${colsString}${st}`);
      }
      lines.push('  }');
    }
    if (table.checks?.length) {
      lines.push('');
      lines.push('  Checks {');
      for (const check of table.checks) {
        const checkSettings = check.name ? ` [name: '${check.name}']` : '';
        lines.push(`    \`${check.expression}\`${checkSettings}`);
      }
      lines.push('  }');
    }
    lines.push('}');
    lines.push('');
  }

  for (const ref of schema.refs) {
    const refSettings: string[] = [];
    if (ref.onDelete) refSettings.push(`delete: ${ref.onDelete}`);
    if (ref.onUpdate) refSettings.push(`update: ${ref.onUpdate}`);
    const st = refSettings.length ? ` [${refSettings.join(', ')}]` : '';
    const nm = ref.name ? ` ${ref.name}` : '';
    lines.push(`Ref${nm}: ${emitEndpoint(ref.source)} ${ref.relation} ${emitEndpoint(ref.target)}${st}`);
  }
  if (schema.refs.length) lines.push('');

  for (const group of schema.tableGroups) {
    lines.push(`TableGroup ${group.name} {`);
    for (const tg of group.tables) {
      lines.push(`  ${tg.schema === DEFAULT_SCHEMA_NAME ? tg.table : `${tg.schema}.${tg.table}`}`);
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
