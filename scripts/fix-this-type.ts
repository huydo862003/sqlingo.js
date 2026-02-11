#!/usr/bin/env tsx

import * as fs from 'fs';
import * as path from 'path';

const EXPRESSIONS_FILE = path.join(process.cwd(), 'src/expressions.ts');

function fixThisType () {
  console.log('Reading file:', EXPRESSIONS_FILE);
  const content = fs.readFileSync(EXPRESSIONS_FILE, 'utf-8');

  // Count occurrences before replacement
  const beforeMatches = content.match(/this\??\s*:\s*Expression\[\]/g);
  const countBefore = beforeMatches ? beforeMatches.length : 0;

  // Replace this: Expression[] with this: Expression
  // Replace this?: Expression[] with this?: Expression
  let modified = content
    .replace(/(\s+)this\?:\s*Expression\[\]/g, '$1this?: Expression')
    .replace(/(\s+)this:\s*Expression\[\]/g, '$1this: Expression');

  const changes = content !== modified;

  if (!changes) {
    console.log('No incorrect "this: Expression[]" or "this?: Expression[]" declarations found.');
    return;
  }

  fs.writeFileSync(EXPRESSIONS_FILE, modified);
  console.log(`Successfully fixed ${countBefore} "this" declaration(s) from Expression[] to Expression`);
}

fixThisType();
