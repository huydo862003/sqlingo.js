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
  DbmlReference,
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
  refs: DbmlReference[];
  enums: DbmlEnum[];
  tableGroups: DbmlTableGroup[];
  stickyNotes: DbmlStickyNote[];
  records: DbmlRecord[];

  constructor (arguments_: {
    project?: DbmlProject;
    tables?: DbmlTable[];
    refs?: DbmlReference[];
    enums?: DbmlEnum[];
    tableGroups?: DbmlTableGroup[];
    stickyNotes?: DbmlStickyNote[];
    records?: DbmlRecord[];
  } = {}) {
    super();
    this.project = arguments_.project;
    this.tables = arguments_.tables ?? [];
    this.refs = arguments_.refs ?? [];
    this.enums = arguments_.enums ?? [];
    this.tableGroups = arguments_.tableGroups ?? [];
    this.stickyNotes = arguments_.stickyNotes ?? [];
    this.records = arguments_.records ?? [];
  }

  intern (): string {
    return `${this.kind}`;
  }
}
