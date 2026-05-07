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
  DbmlInlineReference,
} from './ref';

export class DbmlColumnType extends SchemaElement {
  readonly kind = DbmlKind.COLUMN_TYPE;
  schema?: string;
  name: string;
  args?: string[];
  array?: boolean | (number | undefined)[];

  constructor (arguments_: {
    schema?: string;
    name: string;
    args?: string[];
    array?: boolean | (number | undefined)[];
  }) {
    super();
    this.schema = arguments_.schema;
    this.name = arguments_.name;
    this.args = arguments_.args;
    this.array = arguments_.array;
  }

  intern (): string {
    const base = this.schema ? `${this.schema}.${this.name}` : this.name;
    const argumentsString = this.args?.length ? `(${this.args.join(',')})` : '';
    let array = '';
    if (this.array === true) array = '[]';
    else if (Array.isArray(this.array)) array = this.array.map((n) => `[${n ?? ''}]`).join('');
    return `${this.kind}:${base}${argumentsString}${array}`;
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
  ref?: DbmlInlineReference[];

  constructor (arguments_: {
    name: string;
    type: DbmlColumnType;
    pk?: boolean;
    notNull?: boolean;
    unique?: boolean;
    increment?: boolean;
    default?: string;
    note?: string;
    check?: DbmlCheck;
    ref?: DbmlInlineReference[];
  }) {
    super();
    this.name = arguments_.name;
    this.type = arguments_.type;
    this.pk = arguments_.pk;
    this.notNull = arguments_.notNull;
    this.unique = arguments_.unique;
    this.increment = arguments_.increment;
    this.default = arguments_.default;
    this.note = arguments_.note;
    this.check = arguments_.check;
    this.ref = arguments_.ref;
  }

  intern (): string {
    return `${this.kind}:${this.name}:${this.type.intern()}`;
  }
}
