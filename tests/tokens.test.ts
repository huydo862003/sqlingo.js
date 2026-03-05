import {
  describe, test, expect,
} from 'vitest';
import {
  Tokenizer, TokenType,
} from '../src/tokens';
import { BigQueryTokenizer } from '../src/dialects/bigquery';
import { TokenError } from '../src/errors';

class TestTokens {
  testSpaceKeywords () {
    const cases: [string, number][] = [
      ['group bys', 2],
      [' group bys', 2],
      [' group bys ', 2],
      ['group by)', 2],
      ['group bys)', 3],
      ['group \r', 1],
    ];

    for (const [string, length] of cases) {
      const tokens = new Tokenizer().tokenize(string);
      expect(tokens[0].text.toUpperCase()).toContain('GROUP');
      expect(tokens.length).toBe(length);
    }
  }

  testCommentAttachment () {
    const tokenizer = new Tokenizer();
    const sqlComment: [string, string[]][] = [
      ['/*comment*/ foo', ['comment']],
      ['/*comment*/ foo --test', ['comment', 'test']],
      ['--comment\nfoo --test', ['comment', 'test']],
      ['foo --comment', ['comment']],
      ['foo', []],
      ['foo /*comment 1*/ /*comment 2*/', ['comment 1', 'comment 2']],
      ['foo\n-- comment', [' comment']],
      ['1 /*/2 */', ['/2 ']],
      ['1\n/*comment*/;', ['comment']],
    ];

    for (const [sql, comment] of sqlComment) {
      expect(tokenizer.tokenize(sql)[0].comments).toEqual(comment);
    }
  }

  testTokenLineCol () {
    const tokens = new Tokenizer().tokenize(
      'SELECT /*\nline break\n*/\n\'x\n y\',\nx',
    );

    expect(tokens[0].line).toBe(1);
    expect(tokens[0].col).toBe(6);
    expect(tokens[1].line).toBe(5);
    expect(tokens[1].col).toBe(3);
    expect(tokens[2].line).toBe(5);
    expect(tokens[2].col).toBe(4);
    expect(tokens[3].line).toBe(6);
    expect(tokens[3].col).toBe(1);

    const tokens2 = new Tokenizer().tokenize('SELECT .');
    expect(tokens2[1].line).toBe(1);
    expect(tokens2[1].col).toBe(8);

    expect(new Tokenizer().tokenize('\'\'\'abc\'')[0].start).toBe(0);
    expect(new Tokenizer().tokenize('\'\'\'abc\'')[0].end).toBe(6);
    expect(new Tokenizer().tokenize('\'abc\'')[0].start).toBe(0);

    const tokens3 = new Tokenizer().tokenize('SELECT\r\n  1,\r\n  2');
    expect(tokens3[0].line).toBe(1);
    expect(tokens3[0].col).toBe(6);
    expect(tokens3[1].line).toBe(2);
    expect(tokens3[1].col).toBe(3);
    expect(tokens3[2].line).toBe(2);
    expect(tokens3[2].col).toBe(4);
    expect(tokens3[3].line).toBe(3);
    expect(tokens3[3].col).toBe(3);

    const tokens4 = new Tokenizer().tokenize('  SELECT\n    100');
    expect(tokens4[0].line).toBe(1);
    expect(tokens4[0].col).toBe(8);
    expect(tokens4[1].line).toBe(2);
    expect(tokens4[1].col).toBe(7);
  }

  testCrlf () {
    const tokens = new Tokenizer().tokenize('SELECT a\r\nFROM b');
    const pairs = tokens.map((token) => [token.tokenType, token.text]);

    expect(pairs).toEqual([
      [TokenType.SELECT, 'SELECT'],
      [TokenType.VAR, 'a'],
      [TokenType.FROM, 'FROM'],
      [TokenType.VAR, 'b'],
    ]);

    for (const simpleQuery of ['SELECT 1\r\n', '\r\nSELECT 1']) {
      const t = new Tokenizer().tokenize(simpleQuery);
      const p = t.map((token) => [token.tokenType, token.text]);
      expect(p).toEqual([[TokenType.SELECT, 'SELECT'], [TokenType.NUMBER, '1']]);
    }
  }

  testCommand () {
    const tokens = new Tokenizer().tokenize('SHOW;');
    expect(tokens[0].tokenType).toBe(TokenType.SHOW);
    expect(tokens[1].tokenType).toBe(TokenType.SEMICOLON);

    const tokens2 = new Tokenizer().tokenize('EXECUTE');
    expect(tokens2[0].tokenType).toBe(TokenType.EXECUTE);
    expect(tokens2.length).toBe(1);

    const tokens3 = new Tokenizer().tokenize('FETCH;SHOW;');
    expect(tokens3[0].tokenType).toBe(TokenType.FETCH);
    expect(tokens3[1].tokenType).toBe(TokenType.SEMICOLON);
    expect(tokens3[2].tokenType).toBe(TokenType.SHOW);
    expect(tokens3[3].tokenType).toBe(TokenType.SEMICOLON);
  }

  testErrorMsg () {
    expect(() => new Tokenizer().tokenize('select /*')).toThrow(TokenError);
    expect(() => new Tokenizer().tokenize('select /*')).toThrow('Error tokenizing \'select /\'');
  }

  testJinja () {
    const tokenizer = new BigQueryTokenizer();

    const tokens = tokenizer.tokenize(`
            SELECT
               {{ x }},
               {{- x -}},
               {# it's a comment #}
               {% for x in y -%}
               a {{+ b }}
               {% endfor %};
        `);

    const pairs = tokens.map((token) => [token.tokenType, token.text]);

    expect(pairs).toEqual([
      [TokenType.SELECT, 'SELECT'],
      [TokenType.L_BRACE, '{'],
      [TokenType.L_BRACE, '{'],
      [TokenType.VAR, 'x'],
      [TokenType.R_BRACE, '}'],
      [TokenType.R_BRACE, '}'],
      [TokenType.COMMA, ','],
      [TokenType.BLOCK_START, '{{-'],
      [TokenType.VAR, 'x'],
      [TokenType.BLOCK_END, '-}}'],
      [TokenType.COMMA, ','],
      [TokenType.BLOCK_START, '{%'],
      [TokenType.FOR, 'for'],
      [TokenType.VAR, 'x'],
      [TokenType.IN, 'in'],
      [TokenType.VAR, 'y'],
      [TokenType.BLOCK_END, '-%}'],
      [TokenType.VAR, 'a'],
      [TokenType.BLOCK_START, '{{+'],
      [TokenType.VAR, 'b'],
      [TokenType.R_BRACE, '}'],
      [TokenType.R_BRACE, '}'],
      [TokenType.BLOCK_START, '{%'],
      [TokenType.VAR, 'endfor'],
      [TokenType.BLOCK_END, '%}'],
      [TokenType.SEMICOLON, ';'],
    ]);

    const tokens2 = tokenizer.tokenize('\'{{ var(\'x\') }}\'');
    const pairs2 = tokens2.map((token) => [token.tokenType, token.text]);
    expect(pairs2).toEqual([
      [TokenType.STRING, '{{ var('],
      [TokenType.VAR, 'x'],
      [TokenType.STRING, ') }}'],
    ]);
  }

  testPartialTokenList () {
    const tokenizer = new Tokenizer();

    try {
      tokenizer.tokenize('foo \'bar');
    } catch (e) {
      expect(e).toBeInstanceOf(TokenError);
      expect(String(e)).toContain('Error tokenizing \'foo \'ba\'');
    }

    const partialTokens = tokenizer.tokens;
    expect(partialTokens.length).toBe(1);
    expect(partialTokens[0].tokenType).toBe(TokenType.VAR);
    expect(partialTokens[0].text).toBe('foo');
  }
}

const t = new TestTokens();
describe('TestTokens', () => {
  test('testSpaceKeywords', () => t.testSpaceKeywords());
  test('testCommentAttachment', () => t.testCommentAttachment());
  test('testTokenLineCol', () => t.testTokenLineCol());
  test('crlf', () => t.testCrlf());
  test('command', () => t.testCommand());
  test('testErrorMsg', () => t.testErrorMsg());
  test('jinja', () => t.testJinja());
  test('testPartialTokenList', () => t.testPartialTokenList());
});
