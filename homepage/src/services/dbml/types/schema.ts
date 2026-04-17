import {
  SchemaElement,
} from './element';
import {
  DbmlKind,
} from './kind';
import type {
  DbmlTable,
} from './table';
import type {
  DbmlRef,
} from './ref';
import type {
  DbmlEnum,
} from './enum';
import type {
  DbmlTableGroup,
} from './tableGroup';
import type {
  DbmlStickyNote,
} from './stickyNote';
import type {
  DbmlProject,
} from './project';
import type {
  DbmlRecord,
} from './record';

export class DbmlSchema extends SchemaElement {
  readonly kind = DbmlKind.SCHEMA;
  project?: DbmlProject;
  tables: DbmlTable[];
  refs: DbmlRef[];
  enums: DbmlEnum[];
  tableGroups: DbmlTableGroup[];
  stickyNotes: DbmlStickyNote[];
  records: DbmlRecord[];

  constructor (args: {
    project?: DbmlProject;
    tables?: DbmlTable[];
    refs?: DbmlRef[];
    enums?: DbmlEnum[];
    tableGroups?: DbmlTableGroup[];
    stickyNotes?: DbmlStickyNote[];
    records?: DbmlRecord[];
  } = {}) {
    super();
    this.project = args.project;
    this.tables = args.tables ?? [];
    this.refs = args.refs ?? [];
    this.enums = args.enums ?? [];
    this.tableGroups = args.tableGroups ?? [];
    this.stickyNotes = args.stickyNotes ?? [];
    this.records = args.records ?? [];
  }

  intern (): string {
    return `${this.kind}`;
  }
}
