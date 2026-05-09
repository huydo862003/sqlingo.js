import {
  SchemaElement,
} from './element';
import {
  DbmlKind,
} from './kind';

export enum DbmlIndexType {
  BTREE = 'btree',
  HASH = 'hash',
  GIN = 'gin',
  GIST = 'gist',
}

export class DbmlIndexColumn extends SchemaElement {
  readonly kind = DbmlKind.INDEX_COLUMN;
  expression: string;
  isExpression?: boolean;

  constructor (arguments_: {
    expression: string;
    isExpression?: boolean;
  }) {
    super();
    this.expression = arguments_.expression;
    this.isExpression = arguments_.isExpression;
  }

  intern (): string {
    return `${this.kind}:${this.isExpression ? 'e' : 'c'}:${this.expression}`;
  }
}

export class DbmlIndex extends SchemaElement {
  readonly kind = DbmlKind.INDEX;
  name?: string;
  columns: DbmlIndexColumn[];
  unique?: boolean;
  pk?: boolean;
  type?: DbmlIndexType | string;
  note?: string;

  constructor (arguments_: {
    name?: string;
    columns: DbmlIndexColumn[];
    unique?: boolean;
    pk?: boolean;
    type?: DbmlIndexType | string;
    note?: string;
  }) {
    super();
    this.name = arguments_.name;
    this.columns = arguments_.columns;
    this.unique = arguments_.unique;
    this.pk = arguments_.pk;
    this.type = arguments_.type;
    this.note = arguments_.note;
  }

  intern (): string {
    const columns = this.columns.map((column) => column.intern()).join('|');

    return `${this.kind}:${this.name ?? ''}:${columns}`;
  }
}
