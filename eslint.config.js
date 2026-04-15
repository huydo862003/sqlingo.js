import stylistic from '@stylistic/eslint-plugin';
import ts from 'typescript-eslint';
import vue from 'eslint-plugin-vue';
import vueParser from 'vue-eslint-parser';

const stylisticRules = {
  // quotes
  '@stylistic/quotes': ['error', 'single'],

  // statements
  '@stylistic/semi': ['error', 'always'],
  '@stylistic/eol-last': ['error', 'always'],
  '@stylistic/no-trailing-spaces': 'error',
  '@stylistic/no-multiple-empty-lines': ['error', { max: 1, maxEOF: 0 }],
  '@stylistic/indent': ['error', 2, { flatTernaryExpressions: true }],
  '@stylistic/max-len': ['error', {
    code: 200,
    tabWidth: 2,
    ignoreUrls: true,
    ignoreStrings: true,
    ignoreTemplateLiterals: true,
    ignoreRegExpLiterals: true,
    ignoreComments: true,
  }],

  // expressions
  '@stylistic/arrow-parens': ['error', 'always'],
  '@stylistic/arrow-spacing': ['error', { before: true, after: true }],
  '@stylistic/comma-dangle': ['error', 'always-multiline'],
  '@stylistic/comma-spacing': ['error', { before: false, after: true }],
  '@stylistic/key-spacing': ['error', { beforeColon: false, afterColon: true }],
  '@stylistic/space-before-function-paren': ['error', 'always'],
  '@stylistic/function-call-spacing': ['error', 'never'],
  '@stylistic/function-call-argument-newline': ['error', 'consistent'],
  '@stylistic/function-paren-newline': ['error', 'multiline-arguments'],
  '@stylistic/operator-linebreak': ['error', 'before', { overrides: { '=': 'after', '+=': 'after', '-=': 'after' } }],
  '@stylistic/object-curly-spacing': ['error', 'never'],
  '@stylistic/object-curly-newline': ['error', {
    ObjectExpression: { multiline: true, minProperties: 1 },
    ObjectPattern: { multiline: true, minProperties: 1 },
    ImportDeclaration: { multiline: true, minProperties: 1 },
    ExportDeclaration: { multiline: true, minProperties: 1 },
  }],
  '@stylistic/object-property-newline': ['error', { allowAllPropertiesOnSameLine: false }],
  '@stylistic/array-bracket-spacing': ['error', 'never'],
  '@stylistic/array-bracket-newline': ['error', 'always'],
  '@stylistic/array-element-newline': ['error', 'always'],

  // block types
  '@stylistic/brace-style': ['error', '1tbs'],
  '@stylistic/space-before-blocks': ['error', 'always'],
  '@stylistic/block-spacing': ['error', 'always'],
  '@stylistic/member-delimiter-style': ['error', {
    multiline: { delimiter: 'semi', requireLast: true },
    singleline: { delimiter: 'semi', requireLast: false },
  }],
  '@stylistic/multiline-ternary': ['error', 'always-multiline'],
  '@stylistic/newline-per-chained-call': ['error', { ignoreChainWithDepth: 2 }],
};

export default [
  {
    ignores: [
      'upstream/**',
      'node_modules/**',
      'dist/**',
      'doc/**',
      'homepage/dist/**',
      'homepage/node_modules/**',
    ],
  },
  {
    files: ['src/**/*.ts', 'src/**/*.js'],
    plugins: {
      '@stylistic': stylistic,
      '@typescript-eslint': ts.plugin,
    },
    languageOptions: {
      parser: ts.parser,
    },
    rules: {
      ...stylisticRules,
      '@typescript-eslint/consistent-type-imports': 'error',
      '@typescript-eslint/no-unused-vars': ['error', {
        varsIgnorePattern: '^_',
        argsIgnorePattern: '^_',
        caughtErrorsIgnorePattern: '^_',
      }],
      '@typescript-eslint/no-non-null-assertion': 'warn',
      '@typescript-eslint/no-use-before-define': 'off',
      'no-use-before-define': 'off',
      '@typescript-eslint/no-extraneous-class': 'off',
      '@typescript-eslint/no-useless-constructor': 'off',
      '@typescript-eslint/no-dynamic-delete': 'off',
      '@typescript-eslint/no-unused-expressions': 'off',
      '@typescript-eslint/unified-signatures': 'off',
    },
  },
  {
    files: ['homepage/src/**/*.ts', 'homepage/vite.config.ts'],
    plugins: {
      '@stylistic': stylistic,
      '@typescript-eslint': ts.plugin,
    },
    languageOptions: {
      parser: ts.parser,
    },
    rules: {
      ...stylisticRules,
      '@typescript-eslint/consistent-type-imports': 'error',
      '@typescript-eslint/no-unused-vars': ['error', {
        varsIgnorePattern: '^_',
        argsIgnorePattern: '^_',
        caughtErrorsIgnorePattern: '^_',
      }],
      '@typescript-eslint/no-non-null-assertion': 'warn',
    },
  },
  {
    files: ['homepage/src/**/*.vue'],
    plugins: {
      '@stylistic': stylistic,
      '@typescript-eslint': ts.plugin,
      vue,
    },
    languageOptions: {
      parser: vueParser,
      parserOptions: {
        parser: ts.parser,
        extraFileExtensions: ['.vue'],
      },
    },
    rules: {
      ...stylisticRules,
      ...vue.configs['flat/recommended'].reduce((acc, c) => ({ ...acc, ...c.rules }), {}),
      'vue/comment-directive': 'off',
      '@typescript-eslint/consistent-type-imports': 'error',
      '@typescript-eslint/no-non-null-assertion': 'warn',
    },
  },
];
