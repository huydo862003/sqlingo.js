import { describe, it, expect } from 'vitest';
import { version, parse, transpile } from './index';

describe('sqlglot.js', () => {
  it('should export version', () => {
    expect(version).toBe('0.1.0');
  });

  it('should have parse function', () => {
    expect(typeof parse).toBe('function');
  });

  it('should have transpile function', () => {
    expect(typeof transpile).toBe('function');
  });

  it('parse should throw not implemented error', () => {
    expect(() => parse('SELECT * FROM table')).toThrow('Not implemented yet');
  });

  it('transpile should throw not implemented error', () => {
    expect(() => transpile('SELECT * FROM table', 'mysql', 'postgres')).toThrow('Not implemented yet');
  });
});
