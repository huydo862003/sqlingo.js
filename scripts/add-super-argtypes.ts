#!/usr/bin/env tsx

import * as fs from 'fs';
import * as path from 'path';

const EXPRESSIONS_FILE = path.join(process.cwd(), 'src/expressions.ts');

function addSuperArgTypes () {
  console.log('Reading file:', EXPRESSIONS_FILE);
  const content = fs.readFileSync(EXPRESSIONS_FILE, 'utf-8');

  // Step 1: Add ...super.argTypes to existing non-empty argTypes
  let modified = content.replace(
    /(static argTypes: Record<string, boolean> = \{\n)(\s+)([a-zA-Z_$][\w$]*:)/g,
    (match, declaration, indent, firstProp) => {
      const matchIndex = content.indexOf(match);
      const afterMatch = content.substring(
        matchIndex + match.length - firstProp.length,
      );
      if (afterMatch.startsWith('...super.argTypes')) {
        return match;
      }
      return `${declaration}${indent}...super.argTypes,\n${indent}${firstProp}`;
    },
  );

  const step1Changes = (modified.match(/\.\.\.super\.argTypes,/g) || []).length;

  // Step 2: Add ...super.argTypes to empty argTypes = {}
  modified = modified.replace(
    /(static argTypes: Record<string, boolean> = )\{\}( satisfies)/g,
    (_match, prefix, suffix) => {
      return `${prefix}{\n    ...super.argTypes,\n  }${suffix}`;
    },
  );

  const step2Changes = (modified.match(/\{\n    \.\.\.super\.argTypes,\n  \}/g) || []).length;

  // Step 3: Add missing argTypes declarations for classes that don't have them
  const newLines = modified.split('\n');
  const additions: Array<{
    lineIndex: number;
    content: string;
  }> = [];

  for (let i = 0; i < newLines.length; i++) {
    const line = newLines[i];
    const classMatch = line.match(/^export class (\w+) extends (\w+)/);

    if (classMatch) {
      // Look ahead to see if there's already a static argTypes
      let hasArgTypes = false;
      let insertionPoint = -1;
      let argsTypeName = '';

      for (let j = i + 1; j < Math.min(i + 15, newLines.length); j++) {
        if (newLines[j].includes('static argTypes')) {
          hasArgTypes = true;
          break;
        }

        // Find the declare args line to get the type name
        const declareMatch = newLines[j].match(/declare args: (\w+);/);
        if (declareMatch && insertionPoint === -1) {
          argsTypeName = declareMatch[1];
          insertionPoint = j;
        }

        // Stop if we hit another class or export
        if (newLines[j].match(/^export (class|type|interface|const|function)/)) {
          break;
        }
      }

      if (!hasArgTypes && 0 < insertionPoint && argsTypeName) {
        // Find the indentation of the declare line
        const indent = newLines[insertionPoint].match(/^(\s*)/)?.[1] || '  ';

        additions.push({
          lineIndex: insertionPoint,
          content: `${indent}static argTypes: Record<string, boolean> = {
${indent}  ...super.argTypes,
${indent}} satisfies RequiredMap<${argsTypeName}>;
`,
        });
      }
    }
  }

  // Apply additions in reverse order to maintain line indices
  additions.reverse();
  let finalContent = modified;
  let step3Changes = 0;

  for (const addition of additions) {
    const linesArray = finalContent.split('\n');
    linesArray.splice(addition.lineIndex, 0, addition.content.trimEnd());
    finalContent = linesArray.join('\n');
    step3Changes++;
  }

  const totalChanges = content !== finalContent;

  if (!totalChanges) {
    console.log('No changes needed - all classes already have proper argTypes.');
    return;
  }

  fs.writeFileSync(EXPRESSIONS_FILE, finalContent);
  console.log('✅ Successfully updated expressions.ts:');
  console.log(`   - Added ...super.argTypes to ${step1Changes} non-empty argTypes`);
  console.log(`   - Added ...super.argTypes to ${step2Changes} empty argTypes`);
  console.log(`   - Added missing argTypes to ${step3Changes} classes`);
  console.log(`   - Total: ${step1Changes + step2Changes + step3Changes} modifications`);
}

addSuperArgTypes();
