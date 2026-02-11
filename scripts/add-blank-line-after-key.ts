#!/usr/bin/env tsx

import * as fs from 'fs';
import * as path from 'path';
import * as ts from 'typescript';

const EXPRESSIONS_FILE = path.join(process.cwd(), 'src/expressions.ts');

interface BlankLinePosition {
  position: number;
  className: string;
  description: string;
}

function main () {
  console.log('Reading file:', EXPRESSIONS_FILE);
  const sourceText = fs.readFileSync(EXPRESSIONS_FILE, 'utf-8');
  const sourceFile = ts.createSourceFile(
    EXPRESSIONS_FILE,
    sourceText,
    ts.ScriptTarget.Latest,
    true,
  );

  const positions: BlankLinePosition[] = [];

  // Process each class
  ts.forEachChild(sourceFile, (node) => {
    if (!ts.isClassDeclaration(node) || !node.name) return;

    const className = node.name.text;
    if (!className.endsWith('Expr')) return;

    let keyMember: ts.PropertyDeclaration | null = null;
    let argTypesMember: ts.PropertyDeclaration | null = null;

    // Find key and argTypes members
    for (const member of node.members) {
      if (ts.isPropertyDeclaration(member) && member.name && ts.isIdentifier(member.name)) {
        const name = member.name.text;

        if (name === 'key') {
          keyMember = member;
        } else if (
          name === 'argTypes'
          && member.modifiers?.some((m) => m.kind === ts.SyntaxKind.StaticKeyword)
        ) {
          argTypesMember = member;
        }
      }
    }

    // If both exist and argTypes comes right after key, check for blank line
    if (keyMember && argTypesMember) {
      const keyEnd = keyMember.getEnd();
      const argTypesStart = argTypesMember.getFullStart();

      // Get the text between key and argTypes
      const textBetween = sourceText.substring(keyEnd, argTypesStart);

      // Count the number of newlines in the text between
      const newlineCount = (textBetween.match(/\n/g) || []).length;

      // We want exactly 2 newlines (one for the line end, one for the blank line)
      // If there's only 1 newline, we need to add one more
      if (newlineCount === 1) {
        // Find the position right after the key's line (after the first newline)
        const keyEndLine = sourceText.substring(keyEnd);
        const firstNewline = keyEndLine.indexOf('\n');

        if (firstNewline !== -1) {
          const insertPos = keyEnd + firstNewline + 1;

          positions.push({
            position: insertPos,
            className,
            description: `Add blank line after key in ${className}`,
          });

          console.log(`Processing: ${className}`);
          console.log('  - Will add blank line after key property');
        }
      }
    }
  });

  if (positions.length === 0) {
    console.log('\nNo modifications needed!');
    return;
  }

  console.log(`\n\nTotal modifications: ${positions.length}`);

  // Sort by position (descending) to apply from end to start
  positions.sort((a, b) => b.position - a.position);

  let newText = sourceText;
  for (const pos of positions) {
    console.log(`Applying: ${pos.description} at position ${pos.position}`);
    newText = newText.slice(0, pos.position) + '\n' + newText.slice(pos.position);
  }

  // Write back to the file
  fs.writeFileSync(EXPRESSIONS_FILE, newText);
  console.log(`\nModifications applied to: ${EXPRESSIONS_FILE}`);
}

main();
