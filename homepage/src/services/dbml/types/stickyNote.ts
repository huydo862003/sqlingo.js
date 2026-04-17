import {
  SchemaElement,
} from './element';
import {
  DbmlKind,
} from './kind';

export class DbmlStickyNote extends SchemaElement {
  readonly kind = DbmlKind.STICKY_NOTE;
  name: string;
  content: string;

  constructor (args: {
    name: string;
    content: string;
  }) {
    super();
    this.name = args.name;
    this.content = args.content;
  }

  intern (): string {
    return `${this.kind}:${this.name}`;
  }
}
