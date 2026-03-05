import {
  describe, test, expect,
} from 'vitest';
import { highlightSql } from '../src/errors';

const ANSI_UNDERLINE = '\x1b[4m';
const ANSI_RESET = '\x1b[0m';

class TestErrors {
  testHighlightSqlSingleCharacter () {
    const sql = 'SELECT a FROM t';
    const {
      formattedSql, startContext, highlight, endContext,
    } = highlightSql({
      sql,
      positions: [[7, 7]],
    });

    expect(startContext).toBe('SELECT ');
    expect(highlight).toBe('a');
    expect(endContext).toBe(' FROM t');
    expect(formattedSql).toBe(`SELECT ${ANSI_UNDERLINE}a${ANSI_RESET} FROM t`);
  }

  testHighlightSqlMultiCharacter () {
    const sql = 'SELECT foo FROM table';
    const {
      formattedSql, startContext, highlight, endContext,
    } = highlightSql({
      sql,
      positions: [[7, 9]],
    });

    expect(startContext).toBe('SELECT ');
    expect(highlight).toBe('foo');
    expect(endContext).toBe(' FROM table');
    expect(formattedSql).toBe(`SELECT ${ANSI_UNDERLINE}foo${ANSI_RESET} FROM table`);
  }

  testHighlightSqlMultipleHighlights () {
    const sql = 'SELECT a, b, c FROM table';
    const {
      formattedSql, startContext, highlight, endContext,
    } = highlightSql({
      sql,
      positions: [[7, 7], [10, 10]],
    });

    expect(startContext).toBe('SELECT ');
    expect(highlight).toBe('a, b');
    expect(endContext).toBe(', c FROM table');
    expect(formattedSql).toBe(
      `SELECT ${ANSI_UNDERLINE}a${ANSI_RESET}, ${ANSI_UNDERLINE}b${ANSI_RESET}, c FROM table`,
    );
  }

  testHighlightSqlAtEnd () {
    const sql = 'SELECT a FROM t';
    const {
      formattedSql, startContext, highlight, endContext,
    } = highlightSql({
      sql,
      positions: [[14, 14]],
    });

    expect(startContext).toBe('SELECT a FROM ');
    expect(highlight).toBe('t');
    expect(endContext).toBe('');
    expect(formattedSql).toBe(`SELECT a FROM ${ANSI_UNDERLINE}t${ANSI_RESET}`);
  }

  testHighlightSqlEntireString () {
    const sql = 'SELECT a';
    const {
      formattedSql, startContext, highlight, endContext,
    } = highlightSql({
      sql,
      positions: [[0, 7]],
    });

    expect(startContext).toBe('');
    expect(highlight).toBe('SELECT a');
    expect(endContext).toBe('');
    expect(formattedSql).toBe(`${ANSI_UNDERLINE}SELECT a${ANSI_RESET}`);
  }

  testHighlightSqlAdjacentHighlights () {
    const sql = 'SELECT ab FROM t';
    const {
      formattedSql, startContext, highlight, endContext,
    } = highlightSql({
      sql,
      positions: [[7, 7], [8, 8]],
    });

    expect(startContext).toBe('SELECT ');
    expect(highlight).toBe('ab');
    expect(endContext).toBe(' FROM t');
    expect(formattedSql).toBe(
      `SELECT ${ANSI_UNDERLINE}a${ANSI_RESET}${ANSI_UNDERLINE}b${ANSI_RESET} FROM t`,
    );
  }

  testHighlightSqlSmallContextLength () {
    const sql = 'SELECT a, b, c FROM table WHERE x = 1';
    const {
      formattedSql, startContext, highlight, endContext,
    } = highlightSql({
      sql,
      positions: [[7, 7], [10, 10]],
      contextLength: 5,
    });

    expect(startContext).toBe('LECT ');
    expect(highlight).toBe('a, b');
    expect(endContext).toBe(', c F');
    expect(formattedSql).toBe(
      `LECT ${ANSI_UNDERLINE}a${ANSI_RESET}, ${ANSI_UNDERLINE}b${ANSI_RESET}, c F`,
    );
  }

  testHighlightSqlEmptyPositions () {
    const sql = 'SELECT a FROM t';
    expect(() => highlightSql({
      sql,
      positions: [],
    })).toThrow();
  }

  testHighlightSqlPartialOverlap () {
    const sql = 'SELECT foo FROM table';
    const {
      formattedSql, startContext, highlight, endContext,
    } = highlightSql({
      sql,
      positions: [[7, 9], [8, 10]],
    });

    expect(startContext).toBe('SELECT ');
    expect(highlight).toBe('foo ');
    expect(endContext).toBe('FROM table');
    expect(formattedSql).toBe(
      `SELECT ${ANSI_UNDERLINE}foo${ANSI_RESET}${ANSI_UNDERLINE} ${ANSI_RESET}FROM table`,
    );
  }

  testHighlightSqlFullOverlap () {
    const sql = 'SELECT foobar FROM table';
    const {
      formattedSql, startContext, highlight, endContext,
    } = highlightSql({
      sql,
      positions: [[7, 12], [9, 11]],
    });

    expect(startContext).toBe('SELECT ');
    expect(highlight).toBe('foobar');
    expect(endContext).toBe(' FROM table');
    expect(formattedSql).toBe(`SELECT ${ANSI_UNDERLINE}foobar${ANSI_RESET} FROM table`);
  }

  testHighlightSqlIdenticalPositions () {
    const sql = 'SELECT a FROM t';
    const {
      formattedSql, startContext, highlight, endContext,
    } = highlightSql({
      sql,
      positions: [[7, 7], [7, 7]],
    });

    expect(startContext).toBe('SELECT ');
    expect(highlight).toBe('a');
    expect(endContext).toBe(' FROM t');
    expect(formattedSql).toBe(`SELECT ${ANSI_UNDERLINE}a${ANSI_RESET} FROM t`);
  }

  testHighlightSqlReversedPositions () {
    const sql = 'SELECT a, b FROM table';
    const {
      formattedSql, startContext, highlight, endContext,
    } = highlightSql({
      sql,
      positions: [[10, 10], [7, 7]],
    });

    expect(startContext).toBe('SELECT ');
    expect(highlight).toBe('a, b');
    expect(endContext).toBe(' FROM table');
    expect(formattedSql).toBe(
      `SELECT ${ANSI_UNDERLINE}a${ANSI_RESET}, ${ANSI_UNDERLINE}b${ANSI_RESET} FROM table`,
    );
  }

  testHighlightSqlZeroContextLength () {
    const sql = 'SELECT a, b FROM table';
    const {
      formattedSql, startContext, highlight, endContext,
    } = highlightSql({
      sql,
      positions: [[7, 7]],
      contextLength: 0,
    });

    expect(startContext).toBe('');
    expect(endContext).toBe('');
    expect(highlight).toBe('a');
    expect(formattedSql).toBe(`${ANSI_UNDERLINE}a${ANSI_RESET}`);
  }
}

const t = new TestErrors();
describe('TestErrors', () => {
  test('testHighlightSqlSingleCharacter', () => t.testHighlightSqlSingleCharacter());
  test('testHighlightSqlMultiCharacter', () => t.testHighlightSqlMultiCharacter());
  test('testHighlightSqlMultipleHighlights', () => t.testHighlightSqlMultipleHighlights());
  test('testHighlightSqlAtEnd', () => t.testHighlightSqlAtEnd());
  test('testHighlightSqlEntireString', () => t.testHighlightSqlEntireString());
  test('testHighlightSqlAdjacentHighlights', () => t.testHighlightSqlAdjacentHighlights());
  test('testHighlightSqlSmallContextLength', () => t.testHighlightSqlSmallContextLength());
  test('testHighlightSqlEmptyPositions', () => t.testHighlightSqlEmptyPositions());
  test('testHighlightSqlPartialOverlap', () => t.testHighlightSqlPartialOverlap());
  test('testHighlightSqlFullOverlap', () => t.testHighlightSqlFullOverlap());
  test('testHighlightSqlIdenticalPositions', () => t.testHighlightSqlIdenticalPositions());
  test('testHighlightSqlReversedPositions', () => t.testHighlightSqlReversedPositions());
  test('testHighlightSqlZeroContextLength', () => t.testHighlightSqlZeroContextLength());
});
