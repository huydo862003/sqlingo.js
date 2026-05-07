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

  constructor (arguments_: {
    name: string;
    content: string;
  }) {
    super();
    this.name = arguments_.name;
    this.content = arguments_.content;
  }

  intern (): string {
    return `${this.kind}:${this.name}`;
  }
}
