import {
  SchemaElement,
} from './element';
import {
  DbmlKind,
} from './kind';

export class DbmlProject extends SchemaElement {
  readonly kind = DbmlKind.PROJECT;
  name?: string;
  databaseType?: string;
  note?: string;
  custom?: Record<string, string>;

  constructor (arguments_: {
    name?: string;
    databaseType?: string;
    note?: string;
    custom?: Record<string, string>;
  }) {
    super();
    this.name = arguments_.name;
    this.databaseType = arguments_.databaseType;
    this.note = arguments_.note;
    this.custom = arguments_.custom;
  }

  intern (): string {
    return `${this.kind}:${this.name ?? ''}`;
  }
}
