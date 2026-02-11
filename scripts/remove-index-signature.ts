#!/usr/bin/env tsx

import * as fs from 'fs';
import * as path from 'path';

const EXPRESSIONS_FILE = path.join(process.cwd(), 'src/expressions.ts');

function removeIndexSignatures () {
  console.log('Reading file:', EXPRESSIONS_FILE);
  const content = fs.readFileSync(EXPRESSIONS_FILE, 'utf-8');

  // Remove [key: string]: unknown; with various whitespace patterns
  const modified = content
    // Match inline: { [key: string]: unknown } & -> just &
    .replace(/\{\s*\[key: string\]: unknown\s*\}\s*&/g, '')
    // Match with semicolon and newline
    .replace(/\s*\[key: string\]: unknown;\s*\n/g, '\n')
    // Match without trailing newline
    .replace(/\s*\[key: string\]: unknown;/g, '')
    // Match with just spaces
    .replace(/\[key: string\]: unknown;\s*/g, '');

  const changes = content !== modified;

  if (!changes) {
    console.log('No index signatures found to remove.');
    return;
  }

  fs.writeFileSync(EXPRESSIONS_FILE, modified);
  console.log('Successfully removed [key: string]: unknown from Args types');
}

removeIndexSignatures();
