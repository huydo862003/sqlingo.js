#!/usr/bin/env tsx

import * as fs from 'fs';
import * as path from 'path';
import * as ts from 'typescript';

const EXPRESSIONS_FILE = path.join(process.cwd(), 'src/expressions.ts');

interface ClassInfo {
  className: string;
  baseClassName: string;
  classNode: ts.ClassDeclaration;
  hasArgsType: boolean;
  hasArgTypes: boolean;
  hasDeclareArgs: boolean;
  hasConstructor: boolean;
  argsTypeName: string;
  classStartPos: number;
  classKeyPos: number | null;
  constructorPos: number | null;
}

function extractClassInfo (sourceFile: ts.SourceFile): ClassInfo[] {
  const classes: ClassInfo[] = [];
  const existingArgsTypes = new Set<string>();

  // First pass: collect all existing Args type declarations
  ts.forEachChild(sourceFile, (node) => {
    if (ts.isTypeAliasDeclaration(node) && node.name.text.endsWith('Args')) {
      existingArgsTypes.add(node.name.text);
    }
  });

  // Second pass: analyze class declarations
  ts.forEachChild(sourceFile, (node) => {
    if (ts.isClassDeclaration(node) && node.name) {
      const className = node.name.text;

      // Only process classes that end with "Expr"
      if (!className.endsWith('Expr')) {
        return;
      }

      // Get base class name
      let baseClassName = 'Expression';
      if (node.heritageClauses) {
        for (const clause of node.heritageClauses) {
          if (clause.token === ts.SyntaxKind.ExtendsKeyword) {
            const type = clause.types[0];
            if (ts.isIdentifier(type.expression)) {
              baseClassName = type.expression.text;
            }
          }
        }
      }

      const argsTypeName = `${className}Args`;
      const hasArgsType = existingArgsTypes.has(argsTypeName);

      // Check for static argTypes
      let hasArgTypes = false;
      let hasDeclareArgs = false;
      let hasConstructor = false;
      let classKeyPos: number | null = null;
      let constructorPos: number | null = null;

      for (const member of node.members) {
        // Check for static argTypes
        if (
          ts.isPropertyDeclaration(member)
          && member.name
          && ts.isIdentifier(member.name)
          && member.name.text === 'argTypes'
          && member.modifiers?.some((m) => m.kind === ts.SyntaxKind.StaticKeyword)
        ) {
          hasArgTypes = true;
        }

        // Check for declare args
        if (
          ts.isPropertyDeclaration(member)
          && member.name
          && ts.isIdentifier(member.name)
          && member.name.text === 'args'
          && member.modifiers?.some((m) => m.kind === ts.SyntaxKind.DeclareKeyword)
        ) {
          hasDeclareArgs = true;
        }

        // Check for constructor
        if (ts.isConstructorDeclaration(member)) {
          hasConstructor = true;
          constructorPos = member.getStart(sourceFile);
        }

        // Find 'key' property to determine insertion point
        if (
          ts.isPropertyDeclaration(member)
          && member.name
          && ts.isIdentifier(member.name)
          && member.name.text === 'key'
        ) {
          classKeyPos = member.getEnd();
        }
      }

      classes.push({
        className,
        baseClassName,
        classNode: node,
        hasArgsType,
        hasArgTypes,
        hasDeclareArgs,
        hasConstructor,
        argsTypeName,
        classStartPos: node.getStart(sourceFile),
        classKeyPos,
        constructorPos,
      });
    }
  });

  return classes;
}

function generateArgsType (className: string, baseClassName: string): string {
  const baseArgsType = baseClassName === 'Expression'
    ? 'BaseExpressionArgs'
    : `${baseClassName}Args`;
  return `export type ${className}Args = BaseExpressionArgs;\n`;
}

function generateStaticArgTypes (argsTypeName: string, indent: string = '  '): string {
  return `${indent}static argTypes: Record<string, boolean> = {} satisfies RequiredMap<${argsTypeName}>;\n`;
}

function generateDeclareArgs (argsTypeName: string, indent: string = '  '): string {
  return `${indent}declare args: ${argsTypeName};\n`;
}

function generateConstructor (argsTypeName: string, indent: string = '  '): string {
  return `${indent}constructor (args: ${argsTypeName} = {}) {\n${indent}  super(args);\n${indent}}\n`;
}

function getIndentation (sourceFile: ts.SourceFile, node: ts.Node): string {
  const fullStart = node.getFullStart();
  const start = node.getStart(sourceFile);
  const leadingText = sourceFile.text.substring(fullStart, start);
  const lines = leadingText.split('\n');
  const lastLine = lines[lines.length - 1];
  return lastLine.match(/^(\s*)/)?.[1] || '  ';
}

async function transformFile () {
  console.log('Reading file:', EXPRESSIONS_FILE);
  const sourceText = fs.readFileSync(EXPRESSIONS_FILE, 'utf-8');
  const sourceFile = ts.createSourceFile(
    EXPRESSIONS_FILE,
    sourceText,
    ts.ScriptTarget.Latest,
    true,
  );

  const classes = extractClassInfo(sourceFile);

  console.log(`Found ${classes.length} expression classes`);

  // Collect all transformations to apply
  interface Modification {
    position: number;
    text: string;
    description: string;
    order: number; // Lower order = applied first (when positions are equal)
  }

  const modifications: Modification[] = [];

  for (const classInfo of classes) {
    const needsProcessing =
      !classInfo.hasArgsType
      || !classInfo.hasArgTypes
      || !classInfo.hasDeclareArgs
      || !classInfo.hasConstructor;

    if (!needsProcessing) {
      continue;
    }

    console.log(`\nProcessing: ${classInfo.className}`);

    const indent = getIndentation(sourceFile, classInfo.classNode.members[0] || classInfo.classNode);

    // Add Args type if missing (before class)
    if (!classInfo.hasArgsType) {
      const argsTypeText = generateArgsType(classInfo.className, classInfo.baseClassName);
      modifications.push({
        position: classInfo.classStartPos,
        text: argsTypeText,
        description: `Add ${classInfo.argsTypeName} type before ${classInfo.className}`,
        order: 0,
      });
      console.log(`  - Will add ${classInfo.argsTypeName} type`);
    }

    // Find the right insertion point for class members
    // We want to insert after the 'key' property if it exists
    let insertionPoint: number;
    let needsLeadingNewline = false;

    if (classInfo.classKeyPos !== null) {
      // Insert after the key property (after semicolon and newline)
      insertionPoint = classInfo.classKeyPos;
      // Find the end of the line
      const textAfterKey = sourceText.substring(insertionPoint);
      const nextNewline = textAfterKey.indexOf('\n');
      if (nextNewline !== -1) {
        insertionPoint += nextNewline + 1;
      }
    } else if (0 < classInfo.classNode.members.length) {
      // Insert before the first member
      const firstMember = classInfo.classNode.members[0];
      insertionPoint = firstMember.getFullStart();
      needsLeadingNewline = true;
    } else {
      // Empty class body - insert after opening brace
      const openBrace = sourceFile.text.indexOf('{', classInfo.classNode.getStart(sourceFile));
      insertionPoint = openBrace + 1;
      needsLeadingNewline = true;
    }

    // Build up the text to insert (we'll insert all missing members together)
    let membersToInsert = '';
    const order = 1;

    // Add static argTypes if missing
    if (!classInfo.hasArgTypes) {
      membersToInsert += '\n' + generateStaticArgTypes(classInfo.argsTypeName, indent);
      console.log('  - Will add static argTypes');
    }

    // Add declare args if missing
    if (!classInfo.hasDeclareArgs) {
      membersToInsert += generateDeclareArgs(classInfo.argsTypeName, indent);
      console.log('  - Will add declare args');
    }

    // Add constructor if missing
    if (!classInfo.hasConstructor) {
      membersToInsert += generateConstructor(classInfo.argsTypeName, indent);
      console.log('  - Will add constructor');
    }

    if (membersToInsert) {
      modifications.push({
        position: insertionPoint,
        text: membersToInsert,
        description: `Add missing members to ${classInfo.className}`,
        order,
      });
    }
  }

  if (modifications.length === 0) {
    console.log('\nNo modifications needed!');
    return;
  }

  console.log(`\n\nTotal modifications to apply: ${modifications.length}`);
  console.log('\nWARNING: This is a dry run. Review the changes carefully before applying.');
  console.log('Modifications will be applied in reverse order to maintain positions.\n');

  // Sort modifications by position (descending), then by order (descending)
  modifications.sort((a, b) => {
    if (b.position !== a.position) {
      return b.position - a.position;
    }
    return b.order - a.order;
  });

  let newText = sourceText;
  for (const mod of modifications) {
    console.log(`Applying: ${mod.description} at position ${mod.position}`);
    newText = newText.slice(0, mod.position) + mod.text + newText.slice(mod.position);
  }

  // Write to a new file for review
  const outputFile = EXPRESSIONS_FILE.replace('.ts', '.modified.ts');
  fs.writeFileSync(outputFile, newText);
  console.log(`\nWrote modified file to: ${outputFile}`);
  console.log('Please review the changes before applying them to the original file.');
}

transformFile().catch((error) => {
  console.error('Error:', error);
  process.exit(1);
});
