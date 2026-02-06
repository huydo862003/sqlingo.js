import type { Dialects } from './dialects';

export interface Token {
  type: string;
  text: string;
  line: number;
  col: number;
}

export enum TokenType {
  // Placeholder
}

export class Tokenizer {
  tokenize (_sql: string): Token[] {
    throw new Error('Tokenizer not implemented');
  }
}

export function tokenize (sql: string, dialect?: Dialects): Token[] {
  throw new Error(`tokenize not implemented: ${sql}, ${dialect}`);
}
