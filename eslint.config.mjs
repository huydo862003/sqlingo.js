import {
  baseConfig, tailwindConfig, vueConfig,
} from '@hdnax/nuclint';

export default [
  ...baseConfig,
  ...vueConfig.map((config) => ({
    files: ['./homepage/**/*.vue'],
    ...config,
  })),
  ...tailwindConfig('./homepage/src/style.css').map((config) => ({
    files: ['./homepage/**/*.vue'],
    ...config,
  })),
  {
    ignores: [
      '**/upstream/**',
      '**/doc/**',
      './homepage/api-reference/**',
    ],
  },
  {
    files: ['packages/sqlingo.js/tests/**/*.ts'],
    rules: {
      'id-length': 'off',
      'no-restricted-imports': 'off',
      'custom/no-import-alias': 'off',
      'unicorn/prevent-abbreviations': 'off',
    },
  },
  {
    files: ['packages/sqlingo.js/src/**/*.ts'],
    rules: {
      '@typescript-eslint/no-extraneous-class': 'off',
      '@typescript-eslint/no-useless-constructor': 'off',
      '@typescript-eslint/no-dynamic-delete': 'off',
      '@typescript-eslint/no-unused-expressions': 'off',
      '@typescript-eslint/unified-signatures': 'off',
      '@typescript-eslint/no-this-alias': 'off',
      '@typescript-eslint/no-unsafe-function-type': 'off',
      '@typescript-eslint/no-empty-object-type': 'off',
      '@typescript-eslint/no-restricted-types': 'off',
      'prefer-const': 'off',
      'import/no-cycle': 'off',
      'import/no-deprecated': 'off',
      'import/no-unused-modules': 'off',
      'import/no-namespace': 'off',
      'n/prefer-node-protocol': 'off',
      'unicorn/prevent-abbreviations': 'off',
      'unicorn/name-replacements': 'off',
      'id-length': 'off',
      'custom/no-import-alias': 'off',
      'perfectionist/sort-modules': 'off',
    },
  },
];
