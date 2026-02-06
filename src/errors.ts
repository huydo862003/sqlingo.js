export class SqlglotError extends Error {
  constructor (message: string) {
    super(message);
    this.name = 'SqlglotError';
  }
}

export class ParseError extends SqlglotError {
  constructor (message: string) {
    super(message);
    this.name = 'ParseError';
  }
}

export class TokenError extends SqlglotError {
  constructor (message: string) {
    super(message);
    this.name = 'TokenError';
  }
}

export class UnsupportedError extends SqlglotError {
  constructor (message: string) {
    super(message);
    this.name = 'UnsupportedError';
  }
}

export enum ErrorLevel {
  IGNORE = 'IGNORE',
  WARN = 'WARN',
  RAISE = 'RAISE',
  IMMEDIATE = 'IMMEDIATE',
}
