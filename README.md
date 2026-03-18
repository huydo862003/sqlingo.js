# sqlingo.js

![Status](https://img.shields.io/badge/status-active-brightblue)
![Status](https://img.shields.io/badge/status-alpha)
![License](https://img.shields.io/badge/license-MIT-green)
![SQLGlot](https://img.shields.io/badge/SQLGlot-264e95f-orange)

A JavaScript port of [SQLGlot](https://github.com/tobymao/sqlglot), a SQL parser, transpiler, optimizer, and engine.

Supports Typescript & CJS/ESM.

Bundled size is around 1MB minified, gzipped.

Notice: There's currently an alternative [polyglot](https://github.com/tobilg/polyglot) library here. Check it out!

## Goals (& Non-goals)

The main goal is that sqlingo.js should be a close mirror to SQLGlot. This way, it can quickly catch up with SQLGlot bug fixes and new releases.

Another goal is to stay true to Typescript convention (check [CONVENTION.md](./CONVENTION.md)).

Currently, these are non-goals:
- Optimized performance.
- Optimized bundle size.
- Compatibility with SQLGlot (but it should be trivial to make the two compatible)

## Backstory

I'm currenly maintaining [@dbml/core](https://github.com/holistics/dbml), a library that supports converting between DBML and SQL. Under the hood it uses ANTLR for parsing, and honestly it's been a mess:
- `@dbml/core` is **33MB**, which is quite insane to be honest. It actually broke our CI with OOM errors.
- We can't add more dialects without making the bundle even larger.
- The parser is feature-incomplete and spits out user-unfriendly error messages like `No viable alternative at...`.
- After all that, we only support **5 dialects**.

At a hackathon, I was poking around [Dagster](https://dagster.io/) and stumbled upon SQLGlot. I thought it was amazing that there was a library like this. SQLGlot seems to be trusted by a lots of tools in the Python ecosystems. 

Since then, I was looking for an alternative in Javascript, because I want to run it on the browser. Sadly, at the time, there was none that I knew of.

I tried running SQLGlot through [Pyodide](https://pyodide.org/) as a hack, but the runtime is way too heavy to ship anywhere that matters.

Therefore, I decided to port it. At 2 weeks into my porting process, [polyglot](https://github.com/tobilg/polyglot) was announced (LOL!). However, I didn't want to waste my effort & also wanted full control - so I just continued anyways.

## Development Setup

### Prerequisites

Make sure these are installed on your machine:
- [`node`](https://nodejs.org/)@^18 - [Installation Guide](https://nodejs.org/en/download/package-manager)
- [`pnpm`](https://pnpm.io/)@^10.26.1 - [Installation Guide](https://pnpm.io/installation)

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

### Mirror Guide

Check [CONVENTION.md](./CONVENTION.md).

I have compiled our convention and lots of pitfalls there. You can use the knowledge there to allow easier debugging.

## License

sqlingo.js is licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Attribution

This project is based on [SQLGlot](https://github.com/tobymao/sqlglot) by Toby Mao, which is also licensed under the MIT License. The original SQLGlot source code is included as a submodule in this repository.

See [COPYRIGHT_NOTICE](COPYRIGHT_NOTICE) for full copyright information.
