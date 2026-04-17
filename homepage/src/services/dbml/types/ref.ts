import {
  DEFAULT_SCHEMA_NAME,
} from '../constants';
import {
  SchemaElement,
} from './element';
import {
  DbmlKind,
} from './kind';

export enum DbmlRelation {
  MANY_TO_ONE = '>',
  ONE_TO_MANY = '<',
  ONE_TO_ONE = '-',
  MANY_TO_MANY = '<>',
}

export enum DbmlRefAction {
  CASCADE = 'cascade',
  RESTRICT = 'restrict',
  SET_NULL = 'set null',
  SET_DEFAULT = 'set default',
  NO_ACTION = 'no action',
}

export class DbmlEndpoint extends SchemaElement {
  readonly kind = DbmlKind.ENDPOINT;
  schema?: string;
  table: string;
  columns: string[];

  constructor (args: {
    schema?: string;
    table: string;
    columns: string[];
  }) {
    super();
    this.schema = args.schema;
    this.table = args.table;
    this.columns = args.columns;
  }

  intern (): string {
    const s = this.schema ?? DEFAULT_SCHEMA_NAME;
    return `${this.kind}:${s}.${this.table}(${this.columns.join(',')})`;
  }
}

export class DbmlRef extends SchemaElement {
  readonly kind = DbmlKind.REF;
  name?: string;
  relation: DbmlRelation;
  source: DbmlEndpoint;
  target: DbmlEndpoint;
  onDelete?: DbmlRefAction;
  onUpdate?: DbmlRefAction;
  note?: string;

  constructor (args: {
    name?: string;
    relation: DbmlRelation;
    source: DbmlEndpoint;
    target: DbmlEndpoint;
    onDelete?: DbmlRefAction;
    onUpdate?: DbmlRefAction;
    note?: string;
  }) {
    super();
    this.name = args.name;
    this.relation = args.relation;
    this.source = args.source;
    this.target = args.target;
    this.onDelete = args.onDelete;
    this.onUpdate = args.onUpdate;
    this.note = args.note;
  }

  intern (): string {
    return `${this.kind}:${this.source.intern()}${this.relation}${this.target.intern()}`;
  }
}

export class DbmlInlineRef extends SchemaElement {
  readonly kind = DbmlKind.INLINE_REF;
  relation: DbmlRelation;
  target: DbmlEndpoint;
  onDelete?: DbmlRefAction;
  onUpdate?: DbmlRefAction;

  constructor (args: {
    relation: DbmlRelation;
    target: DbmlEndpoint;
    onDelete?: DbmlRefAction;
    onUpdate?: DbmlRefAction;
  }) {
    super();
    this.relation = args.relation;
    this.target = args.target;
    this.onDelete = args.onDelete;
    this.onUpdate = args.onUpdate;
  }

  intern (): string {
    return `${this.kind}:${this.relation}${this.target.intern()}`;
  }
}
