# sqlglot.js

![Status](https://img.shields.io/badge/status-active-brightblue)
![License](https://img.shields.io/badge/license-MIT-green)
![TypeScript](https://img.shields.io/badge/TypeScript-5.9-blue)
![Module](https://img.shields.io/badge/module-ESM%20%2B%20CJS-yellow)
![SQLGlot](https://img.shields.io/badge/SQLGlot-264e95f-orange)
![Node](https://img.shields.io/badge/node-%3E%3D18-brightgreen)

A JavaScript port of [SQLGlot](https://github.com/tobymao/sqlglot), a SQL parser, transpiler, optimizer, and engine.

## Development Setup

### Prerequisites

Make sure these are installed on your machine:
- `node`@^18
- `pnpm`@^10.26.1

### Step 1: Configuring npm Profile

Create or edit `~/.npmrc`:

```bash
# Replace YOUR_GITHUB_TOKEN with your actual token
echo "//npm.pkg.github.com/:_authToken=YOUR_GITHUB_TOKEN" >> ~/.npmrc
```

### Step 2: Installing Dependencies

#### Option A: With Nix (Recommended)

If you have [nix](https://nixos.org/) with flakes enabled:
  
```bash
nix develop
pnpm install
```

#### Option B: Without Nix

Install node and pnpm manually, then run:

```bash
pnpm install
```

### Step 3: Verify Setup

```bash
pnpm typecheck  # Typecheck the project
pnpm build      # Build the project
```

### Available Scripts

```bash
pnpm test          # Run tests
pnpm test:ui       # Run tests with UI
pnpm test:coverage # Run tests with coverage
pnpm build         # Build the project
pnpm dev           # Build in watch mode
pnpm typecheck     # Type check without emitting
pnpm lint          # Lint the code
pnpm lint:fix      # Lint and auto-fix issues
pnpm docs          # Generate documentation
```

## License

sqlglot.js is licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Attribution

This project is based on [SQLGlot](https://github.com/tobymao/sqlglot) by Toby Mao, which is also licensed under the MIT License. The original SQLGlot source code is included as a submodule in this repository.

See [COPYRIGHT_NOTICE](COPYRIGHT_NOTICE) for full copyright information.
