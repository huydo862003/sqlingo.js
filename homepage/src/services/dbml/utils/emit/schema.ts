import {
  DEFAULT_SCHEMA_NAME,
} from '../../constants';
import type {
  DbmlColumnType,
  DbmlEndpoint,
  DbmlSchema,
} from '../../types';

// Serialize dbml model schema to DBML text

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

  // Generate tables
  for (const table of schema.tables) {
    const qualifiedName = table.schema ? `${table.schema}.${table.name}` : table.name;

    const header = table.alias ? ` as ${table.alias}` : '';
    const settings: string[] = [];
    if (table.headerColor) settings.push(`headercolor: ${table.headerColor}`);
    if (table.note) settings.push(`note: '${table.note}'`);
    const settingsString = settings.length ? ` [${settings.join(', ')}]` : '';
    lines.push(`Table ${qualifiedName}${header}${settingsString} {`);

    // Generate columns
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
    // Generate indexes
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
    // Generate table checks
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

  // Generate ref elements
  for (const ref of schema.refs) {
    const refSettings: string[] = [];
    if (ref.onDelete) refSettings.push(`delete: ${ref.onDelete}`);
    if (ref.onUpdate) refSettings.push(`update: ${ref.onUpdate}`);
    const st = refSettings.length ? ` [${refSettings.join(', ')}]` : '';
    const nm = ref.name ? ` ${ref.name}` : '';
    lines.push(`Ref${nm}: ${emitEndpoint(ref.source)} ${ref.relation} ${emitEndpoint(ref.target)}${st}`);
  }
  if (schema.refs.length) lines.push('');

  // Generate table groups
  for (const group of schema.tableGroups) {
    lines.push(`TableGroup ${group.name} {`);
    for (const tg of group.tables) {
      lines.push(`  ${tg.schema === DEFAULT_SCHEMA_NAME ? tg.table : `${tg.schema}.${tg.table}`}`);
    }
    lines.push('}');
    lines.push('');
  }

  // Generate sticky notes
  for (const stickyNote of schema.stickyNotes) {
    lines.push(`Note ${stickyNote.name} {`);
    lines.push(`  '''${stickyNote.content}'''`);
    lines.push('}');
    lines.push('');
  }

  // Generate records
  for (const records of schema.records) {
    const qualifiedName = records.schema ? `${records.schema}.${records.tableName}` : records.tableName;
    const colList = records.columns.length ? `(${records.columns.join(', ')})` : '';
    lines.push(`records ${qualifiedName}${colList} {`);
    for (const row of records.rows) {
      lines.push(`  ${row.join(', ')}`);
    }
    lines.push('}');
    lines.push('');
  }

  return lines.join('\n').trimEnd();
}

function emitEndpoint (endpoint: DbmlEndpoint): string {
  const table = endpoint.schema ? `${endpoint.schema}.${endpoint.table}` : endpoint.table;
  return `${table}.${endpoint.columns.length === 1 ? endpoint.columns[0] : `(${endpoint.columns.join(', ')})`}`;
}

function emitType (type: DbmlColumnType): string {
  const base = type.schema ? `${type.schema}.${type.name}` : type.name;
  const arguments_ = type.args?.length ? `(${type.args.join(', ')})` : '';
  let array = '';
  if (type.array === true) array = '[]';
  else if (Array.isArray(type.array)) array = type.array.map((n) => `[${n ?? ''}]`).join('');
  return `${base}${arguments_}${array}`;
}
