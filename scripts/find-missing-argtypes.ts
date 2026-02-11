#!/usr/bin/env tsx

import * as fs from 'fs';
import * as path from 'path';

const EXPRESSIONS_FILE = path.join(process.cwd(), 'src/expressions.ts');

function findMissingArgTypes () {
  const content = fs.readFileSync(EXPRESSIONS_FILE, 'utf-8');
  const lines = content.split('\n');

  const classesWithoutArgTypes: Array<{ line: number; className: string; }> = [];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    // Check if this line declares a class that extends something
    const classMatch = line.match(/^export class (\w+) extends/);
    if (classMatch) {
      const className = classMatch[1];

      // Look ahead up to 10 lines to see if there's a static argTypes
      let hasArgTypes = false;
      for (let j = i + 1; j < Math.min(i + 10, lines.length); j++) {
        if (lines[j].includes('static argTypes')) {
          hasArgTypes = true;
          break;
        }
        // Stop if we hit another class or type definition
        if (lines[j].match(/^export (class|type|interface)/)) {
          break;
        }
      }

      if (!hasArgTypes) {
        classesWithoutArgTypes.push({ line: i + 1, className });
      }
    }
  }

  console.log(`Found ${classesWithoutArgTypes.length} classes without argTypes:\n`);
  classesWithoutArgTypes.forEach(({ line, className }) => {
    console.log(`  Line ${line}: ${className}`);
  });
}

findMissingArgTypes();
