# sqlingo.js API Reference

A SQL parser, transpiler, and optimizer supporting 32 dialects.

## Core

- [parse / parseOne / transpile](core/parsing) - Parse and transpile SQL
- [optimize](core/optimize) - Optimize SQL queries
- [Dialect](core/dialect) - Dialect registration and configuration

## Expressions

- [Expression](expressions/expression) - Base expression class
- [Builder helpers](expressions/builders) - `select()`, `column()`, `condition()`, etc.

## Dialects

- [Available dialects](dialects/index) - All 32 supported dialects
