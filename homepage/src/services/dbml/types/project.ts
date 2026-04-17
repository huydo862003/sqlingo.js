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

  constructor (args: {
    name?: string;
    databaseType?: string;
    note?: string;
    custom?: Record<string, string>;
  }) {
    super();
    this.name = args.name;
    this.databaseType = args.databaseType;
    this.note = args.note;
    this.custom = args.custom;
  }

  intern (): string {
    return `${this.kind}:${this.name ?? ''}`;
  }
}
