#!/usr/bin/env tsx

import * as fs from 'fs';
import * as path from 'path';

const EXPRESSIONS_FILE = path.join(process.cwd(), 'src/expressions.ts');

function removeGetterAssertions () {
  console.log('Reading file:', EXPRESSIONS_FILE);
  const content = fs.readFileSync(EXPRESSIONS_FILE, 'utf-8');

  // Pattern to match getters with $ and remove 'as Type' from return statements
  // Matches: return this.args.something as SomeType;
  // Replaces with: return this.args.something;

  const lines = content.split('\n');
  const newLines: string[] = [];
  let inDollarGetter = false;
  let modificationsCount = 0;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    // Check if we're entering a $ getter
    if (line.trim().startsWith('get $')) {
      inDollarGetter = true;
      newLines.push(line);
      continue;
    }

    // Check if we're exiting the getter
    if (inDollarGetter && line.trim() === '}') {
      inDollarGetter = false;
      newLines.push(line);
      continue;
    }

    // If we're inside a $ getter and this is a return statement
    if (inDollarGetter && line.includes('return this.args.') && line.includes(' as ')) {
      // Remove the 'as Type' part
      const modified = line.replace(/\s+as\s+[^;]+;/, ';');
      if (modified !== line) {
        modificationsCount++;
      }
      newLines.push(modified);
    }
    else {
      newLines.push(line);
    }
  }

  if (modificationsCount === 0) {
    console.log('No type assertions found to remove.');
    return;
  }

  fs.writeFileSync(EXPRESSIONS_FILE, newLines.join('\n'));
  console.log(`Successfully removed type assertions from ${modificationsCount} getter(s)`);
}

removeGetterAssertions();
