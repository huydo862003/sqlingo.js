import {
  expect, vi,
} from 'vitest';
import {
  parseOne, ErrorLevel, UnsupportedError, type Expression,
} from '../../../src/index';
import type { DialectType } from '../../../src/dialects/dialect';

export { UnsupportedError };
export type { DialectType };

export interface ValidateAllOptions {
  read?: Record<string, string>;
  write?: Record<string, string | typeof UnsupportedError>;
  pretty?: boolean;
  identify?: boolean;
}

export class Validator {
  dialect: DialectType | undefined = undefined;

  parseOne (sql: string, opts: Record<string, unknown> = {}): Expression {
    return parseOne(sql, {
      read: this.dialect,
      ...opts,
    });
  }

  validateIdentity (
    sql: string,
    writeSql?: string,
    options: {
      pretty?: boolean;
      identify?: boolean;
      checkCommandWarning?: boolean;
    } = {},
  ): Expression {
    const {
      checkCommandWarning, ...rest
    } = options;

    if (checkCommandWarning) {
      const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
      try {
        const expression = this.parseOne(sql);
        const warned = warnSpy.mock.calls.some(
          (args) => String(args[0]).includes('contains unsupported syntax'),
        );
        expect(warned).toBe(true);
        expect(expression.sql({
          dialect: this.dialect,
          ...rest,
        })).toBe(writeSql ?? sql);
        return expression;
      } finally {
        warnSpy.mockRestore();
      }
    }

    const expression = this.parseOne(sql);
    expect(expression.sql({
      dialect: this.dialect,
      ...rest,
    })).toBe(writeSql ?? sql);
    return expression;
  }

  validateAll (
    sql: string,
    {
      read = {}, write = {}, pretty = false, identify = false,
    }: ValidateAllOptions = {},
  ): void {
    const expression = this.parseOne(sql);

    for (const [readDialect, readSql] of Object.entries(read)) {
      const parsed = parseOne(readSql, { read: readDialect || undefined });
      const generated = parsed.sql({
        dialect: this.dialect,
        unsupportedLevel: ErrorLevel.IGNORE,
        pretty,
        identify,
      });
      expect(generated).toBe(sql);
    }

    for (const [writeDialect, writeSql] of Object.entries(write)) {
      if (writeSql === UnsupportedError) {
        expect(() => expression.sql({
          dialect: writeDialect || undefined,
          unsupportedLevel: ErrorLevel.RAISE,
        })).toThrow(UnsupportedError);
      } else {
        const generated = expression.sql({
          dialect: writeDialect || undefined,
          unsupportedLevel: ErrorLevel.IGNORE,
          pretty,
          identify,
        });
        expect(generated).toBe(writeSql);
      }
    }
  }
}
