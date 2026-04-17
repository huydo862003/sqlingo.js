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

  constructor (args: {
    expression: string;
    isExpression?: boolean;
  }) {
    super();
    this.expression = args.expression;
    this.isExpression = args.isExpression;
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

  constructor (args: {
    name?: string;
    columns: DbmlIndexColumn[];
    unique?: boolean;
    pk?: boolean;
    type?: DbmlIndexType | string;
    note?: string;
  }) {
    super();
    this.name = args.name;
    this.columns = args.columns;
    this.unique = args.unique;
    this.pk = args.pk;
    this.type = args.type;
    this.note = args.note;
  }

  intern (): string {
    const cols = this.columns.map((c) => c.intern()).join('|');
    return `${this.kind}:${this.name ?? ''}:${cols}`;
  }
}
