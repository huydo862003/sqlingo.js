import nuclint from '@hdnax/nuclint';

export default [
  ...nuclint,
  {
    // Override for this project - we're not using Vue
    files: ['**/*.ts', '**/*.js'],
    rules: {
      // Add any project-specific rule overrides here
      '@typescript-eslint/no-non-null-assertion': 'warn',
      '@typescript-eslint/no-use-before-define': 'off',
      'no-use-before-define': 'off',
    },
  },
  {
    // Additional ignores specific to this project
    ignores: ['upstream/**'],
  },
];
