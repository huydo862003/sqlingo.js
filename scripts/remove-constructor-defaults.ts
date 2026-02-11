#!/usr/bin/env tsx

import * as fs from 'fs';
import * as path from 'path';

const EXPRESSIONS_FILE = path.join(process.cwd(), 'src/expressions.ts');

function removeConstructorDefaults () {
  console.log('Reading file:', EXPRESSIONS_FILE);
  const content = fs.readFileSync(EXPRESSIONS_FILE, 'utf-8');

  // Replace constructor (args: ...Args = {}) with constructor (args: ...Args)
  const modified = content.replace(
    /constructor\s*\(\s*args:\s*(\w+Args)\s*=\s*\{\}\s*\)/g,
    'constructor (args: $1)',
  );

  const changes = content !== modified;

  if (!changes) {
    console.log('No constructor defaults found to remove.');
    return;
  }

  // Count how many were changed
  const originalMatches = content.match(/constructor\s*\(\s*args:\s*\w+Args\s*=\s*\{\}\s*\)/g);
  const count = originalMatches ? originalMatches.length : 0;

  fs.writeFileSync(EXPRESSIONS_FILE, modified);
  console.log(`Successfully removed default args = {} from ${count} constructor(s)`);
}

removeConstructorDefaults();
