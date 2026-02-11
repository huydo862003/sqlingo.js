#!/usr/bin/env tsx

import * as fs from 'fs';
import * as path from 'path';
import * as ts from 'typescript';

const EXPRESSIONS_FILE = path.join(process.cwd(), 'src/expressions.ts');

interface MemberInfo {
  hasKey: boolean;
  hasStaticArgTypes: boolean;
  hasDeclareArgs: boolean;
  hasConstructor: boolean;
  keyMember?: ts.PropertyDeclaration;
  argTypesMember?: ts.PropertyDeclaration;
  declareArgsMember?: ts.PropertyDeclaration;
  constructorMember?: ts.ConstructorDeclaration;
}

function analyzeclassMembers (classNode: ts.ClassDeclaration): MemberInfo {
  const info: MemberInfo = {
    hasKey: false,
    hasStaticArgTypes: false,
    hasDeclareArgs: false,
    hasConstructor: false,
  };

  for (const member of classNode.members) {
    if (ts.isPropertyDeclaration(member) && member.name && ts.isIdentifier(member.name)) {
      const name = member.name.text;

      if (name === 'key') {
        info.hasKey = true;
        info.keyMember = member;
      } else if (name === 'argTypes' && member.modifiers?.some((m) => m.kind === ts.SyntaxKind.StaticKeyword)) {
        info.hasStaticArgTypes = true;
        info.argTypesMember = member;
      } else if (name === 'args' && member.modifiers?.some((m) => m.kind === ts.SyntaxKind.DeclareKeyword)) {
        info.hasDeclareArgs = true;
        info.declareArgsMember = member;
      }
    } else if (ts.isConstructorDeclaration(member)) {
      info.hasConstructor = true;
      info.constructorMember = member;
    }
  }

  return info;
}

interface Modification {
  position: number;
  text: string;
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

  // Collect existing Args types
  const existingArgsTypes = new Set<string>();
  ts.forEachChild(sourceFile, (node) => {
    if (ts.isTypeAliasDeclaration(node) && node.name.text.endsWith('Args')) {
      existingArgsTypes.add(node.name.text);
    }
  });

  const modifications: Modification[] = [];

  // Process each class
  ts.forEachChild(sourceFile, (node) => {
    if (!ts.isClassDeclaration(node) || !node.name) return;

    const className = node.name.text;
    if (!className.endsWith('Expr')) return;

    const argsTypeName = `${className}Args`;
    const hasArgsType = existingArgsTypes.has(argsTypeName);
    const memberInfo = analyzeclassMembers(node);

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

    const needsProcessing =
      !hasArgsType
      || !memberInfo.hasStaticArgTypes
      || !memberInfo.hasDeclareArgs
      || !memberInfo.hasConstructor;

    if (!needsProcessing) return;

    console.log(`\nProcessing: ${className}`);

    // Add Args type before class if missing
    if (!hasArgsType) {
      const classStart = node.getFullStart();
      const leadingTrivia = sourceFile.text.substring(classStart, node.getStart(sourceFile));
      let insertPos = node.getStart(sourceFile);

      // If there's an export keyword, insert right before it
      if (leadingTrivia.includes('export')) {
        insertPos = classStart + leadingTrivia.indexOf('export');
      }

      const argsTypeText = `export type ${argsTypeName} = BaseExpressionArgs;\n`;
      modifications.push({
        position: insertPos,
        text: argsTypeText,
        description: `Add ${argsTypeName} type`,
      });
      console.log(`  - Will add ${argsTypeName} type`);
    }

    // Now handle class members
    // Strategy: Find the last position where we can insert new members
    // This should be after key (if exists), or after static argTypes (if exists), or after declare args (if exists)

    // Find the rightmost existing member in our sequence
    let insertAfterMember: ts.Node | null = null;

    if (memberInfo.constructorMember) {
      // Don't insert after constructor - we'll handle missing constructor separately
    }

    if (memberInfo.declareArgsMember) {
      insertAfterMember = memberInfo.declareArgsMember;
    } else if (memberInfo.argTypesMember) {
      insertAfterMember = memberInfo.argTypesMember;
    } else if (memberInfo.keyMember) {
      insertAfterMember = memberInfo.keyMember;
    }

    // Build the text to insert for missing members
    let textToInsert = '';

    if (!memberInfo.hasStaticArgTypes) {
      textToInsert += `  static argTypes: Record<string, boolean> = {} satisfies RequiredMap<${argsTypeName}>;\n`;
      console.log('  - Will add static argTypes');
    }

    if (!memberInfo.hasDeclareArgs) {
      textToInsert += `  declare args: ${argsTypeName};\n`;
      console.log('  - Will add declare args');
    }

    if (!memberInfo.hasConstructor) {
      textToInsert += `  constructor (args: ${argsTypeName} = {}) {\n    super(args);\n  }\n`;
      console.log('  - Will add constructor');
    }

    if (textToInsert) {
      let insertPos: number;

      if (insertAfterMember) {
        // Insert after this member
        insertPos = insertAfterMember.getEnd();
        // Find the newline after this member
        const textAfter = sourceText.substring(insertPos);
        const newlineMatch = textAfter.match(/^[^\n]*\n/);
        if (newlineMatch) {
          insertPos += newlineMatch[0].length;
        }
      } else {
        // No key member - insert at start of class body
        const openBrace = sourceFile.text.indexOf('{', node.getStart(sourceFile));
        insertPos = openBrace + 1;
        textToInsert = '\n' + textToInsert;
      }

      modifications.push({
        position: insertPos,
        text: textToInsert,
        description: `Add missing members to ${className}`,
      });
    }
  });

  if (modifications.length === 0) {
    console.log('\nNo modifications needed!');
    return;
  }

  console.log(`\n\nTotal modifications: ${modifications.length}`);

  // Sort by position (descending) to apply from end to start
  modifications.sort((a, b) => b.position - a.position);

  let newText = sourceText;
  for (const mod of modifications) {
    console.log(`Applying: ${mod.description} at position ${mod.position}`);
    newText = newText.slice(0, mod.position) + mod.text + newText.slice(mod.position);
  }

  const outputFile = EXPRESSIONS_FILE.replace('.ts', '.modified.ts');
  fs.writeFileSync(outputFile, newText);
  console.log(`\nWrote modified file to: ${outputFile}`);
  console.log('Review the changes, then rename to expressions.ts if they look good.');
}

main();
