// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/errors.py

const ANSI_UNDERLINE = '\x1b[4m';
const ANSI_RESET = '\x1b[0m';
const ERROR_MESSAGE_CONTEXT_DEFAULT = 100;

export const enum ErrorLevel {
  IGNORE,
  WARN,
  RAISE,
  IMMEDIATE,
}

export interface ErrorDetail {
  description?: string;
  line?: number;
  col?: number;
  startContext?: string;
  highlight?: string;
  endContext?: string;
  intoExpression?: string;
}

export class SqlglotError extends Error {
  constructor (message: string) {
    super(message);
    this.name = 'SqlglotError';
    Error.captureStackTrace(this, this.constructor);
  }
}

export class UnsupportedError extends SqlglotError {
  constructor (message: string) {
    super(message);
    this.name = 'UnsupportedError';
    Error.captureStackTrace(this, this.constructor);
  }
}

export class ParseError extends SqlglotError {
  errors: ErrorDetail[];

  constructor (message: string, errors?: ErrorDetail[]) {
    super(message);
    this.name = 'ParseError';
    this.errors = errors || [];
    Error.captureStackTrace(this, this.constructor);
  }

  static new (
    message: string,
    description?: string,
    line?: number,
    col?: number,
    startContext?: string,
    highlight?: string,
    endContext?: string,
    intoExpression?: string,
  ): ParseError {
    return new ParseError(message, [
      {
        description,
        line,
        col,
        startContext,
        highlight,
        endContext,
        intoExpression,
      },
    ]);
  }
}

export class TokenError extends SqlglotError {
  constructor (message: string) {
    super(message);
    this.name = 'TokenError';
    Error.captureStackTrace(this, this.constructor);
  }
}

export class OptimizeError extends SqlglotError {
  constructor (message: string) {
    super(message);
    this.name = 'OptimizeError';
    Error.captureStackTrace(this, this.constructor);
  }
}

export class SchemaError extends SqlglotError {
  constructor (message: string) {
    super(message);
    this.name = 'SchemaError';
    Error.captureStackTrace(this, this.constructor);
  }
}

export class ExecuteError extends SqlglotError {
  constructor (message: string) {
    super(message);
    this.name = 'ExecuteError';
    Error.captureStackTrace(this, this.constructor);
  }
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/errors.py#L90-L150
export function highlightSql (
  sql: string,
  positions: [number, number][],
  contextLength: number = ERROR_MESSAGE_CONTEXT_DEFAULT,
): [string, string, string, string] {
  if (positions.length === 0) {
    throw new Error('positions must contain at least one [start, end] tuple');
  }

  let startContext = '';
  let endContext = '';
  let firstHighlightStart = 0;
  const formattedParts: string[] = [];
  let previousPartEnd = 0;
  const sortedPositions = [...positions].sort((a, b) => a[0] - b[0]);

  if (sortedPositions[0][0] > 0) {
    firstHighlightStart = sortedPositions[0][0];
    startContext = sql.slice(
      Math.max(0, firstHighlightStart - contextLength),
      firstHighlightStart,
    );
    formattedParts.push(startContext);
    previousPartEnd = firstHighlightStart;
  }

  for (const [start, end] of sortedPositions) {
    const highlightStart = Math.max(start, previousPartEnd);
    const highlightEnd = end + 1;
    if (highlightStart >= highlightEnd) {
      continue;
    }
    if (highlightStart > previousPartEnd) {
      formattedParts.push(sql.slice(previousPartEnd, highlightStart));
    }
    formattedParts.push(
      `${ANSI_UNDERLINE}${sql.slice(highlightStart, highlightEnd)}${ANSI_RESET}`,
    );
    previousPartEnd = highlightEnd;
  }

  if (previousPartEnd < sql.length) {
    endContext = sql.slice(previousPartEnd, previousPartEnd + contextLength);
    formattedParts.push(endContext);
  }

  const formattedSql = formattedParts.join('');
  const highlight = sql.slice(firstHighlightStart, previousPartEnd);

  return [
    formattedSql,
    startContext,
    highlight,
    endContext,
  ];
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/errors.py#L153-L158
export function concatMessages (errors: unknown[], maximum: number): string {
  const msg = errors.slice(0, maximum).map((e) => String(e));
  const remaining = errors.length - maximum;
  if (remaining > 0) {
    msg.push(`... and ${remaining} more`);
  }
  return msg.join('\n\n');
}

// https://github.com/tobymao/sqlglot/blob/264e95f04d95f2cd7bcf255ee7ae160db36882a7/sqlglot/errors.py#L161-L162
export function mergeErrors (errors: ParseError[]): ErrorDetail[] {
  return errors.flatMap((error) => error.errors);
}
