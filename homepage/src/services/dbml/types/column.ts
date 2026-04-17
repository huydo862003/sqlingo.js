import type {
  DbmlCheck,
} from './check';
import {
  SchemaElement,
} from './element';
import {
  DbmlKind,
} from './kind';
import type {
  DbmlInlineRef,
} from './ref';

export class DbmlColumnType extends SchemaElement {
  readonly kind = DbmlKind.COLUMN_TYPE;
  schema?: string;
  name: string;
  args?: string[];
  array?: boolean | (number | undefined)[];

  constructor (args: {
    schema?: string;
    name: string;
    args?: string[];
    array?: boolean | (number | undefined)[];
  }) {
    super();
    this.schema = args.schema;
    this.name = args.name;
    this.args = args.args;
    this.array = args.array;
  }

  intern (): string {
    const base = this.schema ? `${this.schema}.${this.name}` : this.name;
    const a = this.args?.length ? `(${this.args.join(',')})` : '';
    let arr = '';
    if (this.array === true) arr = '[]';
    else if (Array.isArray(this.array)) arr = this.array.map((n) => `[${n ?? ''}]`).join('');
    return `${this.kind}:${base}${a}${arr}`;
  }
}

export class DbmlColumn extends SchemaElement {
  readonly kind = DbmlKind.COLUMN;
  name: string;
  type: DbmlColumnType;
  pk?: boolean;
  notNull?: boolean;
  unique?: boolean;
  increment?: boolean;
  default?: string;
  note?: string;
  check?: DbmlCheck;
  ref?: DbmlInlineRef[];

  constructor (args: {
    name: string;
    type: DbmlColumnType;
    pk?: boolean;
    notNull?: boolean;
    unique?: boolean;
    increment?: boolean;
    default?: string;
    note?: string;
    check?: DbmlCheck;
    ref?: DbmlInlineRef[];
  }) {
    super();
    this.name = args.name;
    this.type = args.type;
    this.pk = args.pk;
    this.notNull = args.notNull;
    this.unique = args.unique;
    this.increment = args.increment;
    this.default = args.default;
    this.note = args.note;
    this.check = args.check;
    this.ref = args.ref;
  }

  intern (): string {
    return `${this.kind}:${this.name}:${this.type.intern()}`;
  }
}
