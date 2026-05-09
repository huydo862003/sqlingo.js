import {
  DEFAULT_SCHEMA_NAME,
} from '../constants';
import {
  SchemaElement,
} from './element';
import {
  DbmlKind,
} from './kind';

export enum DbmlReferenceAction {
  CASCADE = 'cascade',
  RESTRICT = 'restrict',
  SET_NULL = 'set null',
  SET_DEFAULT = 'set default',
  NO_ACTION = 'no action',
}

export enum DbmlRelation {
  MANY_TO_ONE = '>',
  ONE_TO_MANY = '<',
  ONE_TO_ONE = '-',
  MANY_TO_MANY = '<>',
}

export class DbmlEndpoint extends SchemaElement {
  readonly kind = DbmlKind.ENDPOINT;
  schema?: string;
  table: string;
  columns: string[];

  constructor (arguments_: {
    schema?: string;
    table: string;
    columns: string[];
  }) {
    super();
    this.schema = arguments_.schema;
    this.table = arguments_.table;
    this.columns = arguments_.columns;
  }

  intern (): string {
    const schemaName = this.schema ?? DEFAULT_SCHEMA_NAME;

    return `${this.kind}:${schemaName}.${this.table}(${this.columns.join(',')})`;
  }
}

export class DbmlInlineReference extends SchemaElement {
  readonly kind = DbmlKind.INLINE_REF;
  relation: DbmlRelation;
  target: DbmlEndpoint;
  onDelete?: DbmlReferenceAction;
  onUpdate?: DbmlReferenceAction;

  constructor (arguments_: {
    relation: DbmlRelation;
    target: DbmlEndpoint;
    onDelete?: DbmlReferenceAction;
    onUpdate?: DbmlReferenceAction;
  }) {
    super();
    this.relation = arguments_.relation;
    this.target = arguments_.target;
    this.onDelete = arguments_.onDelete;
    this.onUpdate = arguments_.onUpdate;
  }

  intern (): string {
    return `${this.kind}:${this.relation}${this.target.intern()}`;
  }
}

export class DbmlReference extends SchemaElement {
  readonly kind = DbmlKind.REF;
  name?: string;
  relation: DbmlRelation;
  source: DbmlEndpoint;
  target: DbmlEndpoint;
  onDelete?: DbmlReferenceAction;
  onUpdate?: DbmlReferenceAction;
  note?: string;

  constructor (arguments_: {
    name?: string;
    relation: DbmlRelation;
    source: DbmlEndpoint;
    target: DbmlEndpoint;
    onDelete?: DbmlReferenceAction;
    onUpdate?: DbmlReferenceAction;
    note?: string;
  }) {
    super();
    this.name = arguments_.name;
    this.relation = arguments_.relation;
    this.source = arguments_.source;
    this.target = arguments_.target;
    this.onDelete = arguments_.onDelete;
    this.onUpdate = arguments_.onUpdate;
    this.note = arguments_.note;
  }

  intern (): string {
    return `${this.kind}:${this.source.intern()}${this.relation}${this.target.intern()}`;
  }
}
