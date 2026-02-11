#!/usr/bin/env tsx

import * as fs from 'fs';
import * as path from 'path';

const EXPRESSIONS_FILE = path.join(process.cwd(), 'src/expressions.ts');
const MAX_LINE_LENGTH = 100;

function wrapJSDocLine (line: string, indent: string): string[] {
  const match = line.match(/^(\s*\*\s*)(.+)$/);
  if (!match) return [line];

  const [
    , prefix,
    content,
  ] = match;
  const availableLength = MAX_LINE_LENGTH - prefix.length;

  if (content.length <= availableLength) {
    return [line];
  }

  const words = content.split(' ');
  const lines: string[] = [];
  let currentLine = '';

  for (const word of words) {
    const testLine = currentLine
      ? `${currentLine} ${word}`
      : word;
    if (testLine.length <= availableLength) {
      currentLine = testLine;
    } else {
      if (currentLine) {
        lines.push(`${prefix}${currentLine}`);
      }
      currentLine = word;
    }
  }

  if (currentLine) {
    lines.push(`${prefix}${currentLine}`);
  }

  return lines;
}

function wrapArgTypesLine (line: string): string {
  // Pattern: static argTypes: Record<string, boolean> = {} satisfies RequiredMap<VeryLongNameArgs>;
  const match = line.match(/^(\s*)(static argTypes: Record<string, boolean> = \{\} satisfies RequiredMap<)(.+)(>;)$/);
  if (!match) return line;

  const [
    , indent,
    prefix,
    argsType,
    suffix,
  ] = match;
  const fullLine = `${indent}${prefix}${argsType}${suffix}`;

  if (fullLine.length <= MAX_LINE_LENGTH) {
    return line;
  }

  // Break into multiple lines
  return `${indent}${prefix}\n${indent}  ${argsType}\n${indent}${suffix}`;
}

function fixLongLines () {
  console.log('Reading file:', EXPRESSIONS_FILE);
  const content = fs.readFileSync(EXPRESSIONS_FILE, 'utf-8');
  const lines = content.split('\n');
  const newLines: string[] = [];
  let modified = false;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    if (line.length <= MAX_LINE_LENGTH) {
      newLines.push(line);
      continue;
    }

    // Check if it's a JSDoc comment line
    if (line.trim().startsWith('*') && !line.trim().startsWith('*/')) {
      const indent = line.match(/^(\s*)/)?.[1] || '';
      const wrapped = wrapJSDocLine(line, indent);
      if (1 < wrapped.length) {
        console.log(`Wrapping JSDoc line ${i + 1}: ${line.substring(0, 50)}...`);
        newLines.push(...wrapped);
        modified = true;
        continue;
      }
    }

    // Check if it's a static argTypes line
    if (line.includes('static argTypes') && line.includes('satisfies RequiredMap')) {
      const wrapped = wrapArgTypesLine(line);
      if (wrapped !== line) {
        console.log(`Wrapping argTypes line ${i + 1}`);
        newLines.push(wrapped);
        modified = true;
        continue;
      }
    }

    // If we couldn't fix it, keep the original line
    newLines.push(line);
  }

  if (!modified) {
    console.log('No modifications needed!');
    return;
  }

  const newContent = newLines.join('\n');
  fs.writeFileSync(EXPRESSIONS_FILE, newContent);
  console.log(`\nModifications applied to: ${EXPRESSIONS_FILE}`);
}

fixLongLines();
