#!/usr/bin/env tsx

import * as fs from 'fs';
import * as path from 'path';

const EXPRESSIONS_FILE = path.join(process.cwd(), 'src/expressions.ts');

function removeAllGetters () {
  console.log('Reading file:', EXPRESSIONS_FILE);
  const content = fs.readFileSync(EXPRESSIONS_FILE, 'utf-8');
  const lines = content.split('\n');
  const newLines: string[] = [];

  let i = 0;
  let gettersRemoved = 0;

  while (i < lines.length) {
    const line = lines[i];

    // Check if this line starts a getter with $
    if (line.trim().startsWith('get $')) {
      // Skip this getter (consume lines until we find the closing brace)
      let braceCount = 0;
      let started = false;

      while (i < lines.length) {
        const currentLine = lines[i];

        for (const char of currentLine) {
          if (char === '{') {
            braceCount++;
            started = true;
          } else if (char === '}') {
            braceCount--;
          }
        }

        i++;

        if (started && braceCount === 0) {
          // Skip any empty lines after the getter
          while (i < lines.length && lines[i].trim() === '') {
            i++;
          }
          gettersRemoved++;
          break;
        }
      }
    } else {
      newLines.push(line);
      i++;
    }
  }

  if (gettersRemoved === 0) {
    console.log('No getters found to remove.');
    return;
  }

  fs.writeFileSync(EXPRESSIONS_FILE, newLines.join('\n'));
  console.log(`Successfully removed ${gettersRemoved} getters`);
}

removeAllGetters();
