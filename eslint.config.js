import nuclint from '@hdnax/nuclint';

export default [
  ...nuclint,
  {
    // Override for this project - we're not using Vue
    files: ['**/*.ts', '**/*.js'],
    rules: {
      // Add any project-specific rule overrides here
    },
  },
  {
    // Additional ignores specific to this project
    ignores: ['sqlglot/**'],
  },
];
