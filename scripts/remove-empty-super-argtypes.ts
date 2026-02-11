#!/usr/bin/env tsx

import * as fs from 'fs';
import * as path from 'path';
import { Project, SyntaxKind } from 'ts-morph';

const PYTHON_FILE = path.join(process.cwd(), 'upstream/sqlglot/sqlglot/expressions.py');
const EXPRESSIONS_FILE = path.join(process.cwd(), 'src/expressions.ts');

function findEmptyArgTypesInPython (): Set<string> {
  console.log('Scanning Python file:', PYTHON_FILE);
  const content = fs.readFileSync(PYTHON_FILE, 'utf-8');
  const lines = content.split('\n');

  const emptyArgTypeClasses = new Set<string>();

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    const classMatch = line.match(/^class (\w+)\(/);
    if (classMatch) {
      const className = classMatch[1];
      let foundArgTypes = false;
      let hasPass = false;

      for (let j = i + 1; j < Math.min(i + 30, lines.length); j++) {
        const nextLine = lines[j].trim();

        if (nextLine.match(/^arg_types\s*[:=]\s*\{\s*\}/)) {
          emptyArgTypeClasses.add(className);
          foundArgTypes = true;
          break;
        }

        if (nextLine.match(/^arg_types\s*[:=]\s*\{/) && !nextLine.includes('{}')) {
          foundArgTypes = true;
          break;
        }

        if (nextLine === 'pass') {
          hasPass = true;
        }

        if (nextLine.match(/^(class|def) /) && j > i + 1) {
          break;
        }
      }

      if (hasPass && !foundArgTypes) {
        console.log(`  - Keeping ${className} (only has 'pass')`);
      }
    }
  }

  return emptyArgTypeClasses;
}

function removeEmptySuperArgTypes () {
  const emptyClasses = findEmptyArgTypesInPython();

  console.log(`\nFound ${emptyClasses.size} classes with empty arg_types = {} in Python`);
  if (emptyClasses.size > 0) {
    console.log('Examples:', Array.from(emptyClasses).slice(0, 10).join(', '));
  }

  console.log('\nProcessing TypeScript file:', EXPRESSIONS_FILE);

  const project = new Project({
    tsConfigFilePath: path.join(process.cwd(), 'tsconfig.json'),
  });

  const sourceFile = project.addSourceFileAtPath(EXPRESSIONS_FILE);
  const changes: string[] = [];

  const classes = sourceFile.getClasses();

  for (const classDecl of classes) {
    const className = classDecl.getName();
    if (!className) continue;

    const pyClassName = className.replace(/Expr$/, '');

    if (emptyClasses.has(pyClassName)) {
      const argTypesProperty = classDecl.getStaticProperty('argTypes');

      if (argTypesProperty) {
        let initializer = argTypesProperty.getInitializer();

        // Handle `satisfies` expression
        if (initializer && initializer.getKind() === SyntaxKind.SatisfiesExpression) {
          initializer = initializer.asKindOrThrow(SyntaxKind.SatisfiesExpression).getExpression();
        }

        if (initializer && initializer.getKind() === SyntaxKind.ObjectLiteralExpression) {
          const objectLiteral = initializer.asKindOrThrow(SyntaxKind.ObjectLiteralExpression);
          const properties = objectLiteral.getProperties();

          // Only process if there are properties (not already empty)
          if (0 < properties.length) {
            // Remove all properties
            for (const prop of properties) {
              prop.remove();
            }
            changes.push(pyClassName);
            console.log(`  ✓ Cleared argTypes for ${className}`);
          }
        }
      }
    }
  }

  if (changes.length === 0) {
    console.log('\nNo changes needed - all matching classes already have empty argTypes.');
    return;
  }

  sourceFile.saveSync();

  console.log(`\n✅ Successfully removed properties from argTypes in ${changes.length} classes:`);
  if (10 < changes.length) {
    console.log('   ', changes.slice(0, 10).join(', '));
    console.log(`    ... and ${changes.length - 10} more`);
  } else {
    console.log('   ', changes.join(', '));
  }
}

removeEmptySuperArgTypes();
