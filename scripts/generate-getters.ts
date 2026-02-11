#!/usr/bin/env tsx

import * as fs from 'fs';
import * as path from 'path';
import * as ts from 'typescript';

const EXPRESSIONS_FILE = path.join(process.cwd(), 'src/expressions.ts');

// Properties that should not get getters (none - generate for all)
const SKIP_GETTERS = new Set<string>();

// Properties that are typically arrays
const ARRAY_PROPS = new Set([
  'expressions',
  'options',
  'selects',
  'columns',
  'values',
  'indexes',
  'properties',
  'privileges',
  'principals',
]);

interface ClassAnalysis {
  className: string;
  argsTypeName: string;
  argTypesProps: Map<string, boolean>; // prop name -> isRequired
  existingGetters: Set<string>;
  existingArgsTypeProps: Set<string>;
  argsTypeNode?: ts.TypeAliasDeclaration;
  classNode: ts.ClassDeclaration;
  lastMemberPos: number;
}

function inferPropertyType (propName: string): string {
  // Explicit array properties
  if (ARRAY_PROPS.has(propName)) {
    return 'Expression[]';
  }

  // Known singular properties that end with 's'
  const singularProps = new Set([
    'this',
    'alias',
    'as',
    'class',
    'status',
  ]);
  if (singularProps.has(propName)) {
    return 'Expression';
  }

  // Properties ending in 's' are likely arrays, but check exceptions
  if (propName.endsWith('s') && !propName.endsWith('ss')) {
    return 'Expression[]';
  }

  // Check for specific known types
  if (propName === 'kind' || propName.includes('Kind')) return 'string';
  if (propName.includes('scalar') || propName.includes('boolean')) return 'boolean';

  return 'Expression';
}

function parseArgTypesObject (node: ts.ObjectLiteralExpression): Map<string, boolean> {
  const props = new Map<string, boolean>();

  for (const prop of node.properties) {
    if (ts.isPropertyAssignment(prop) && ts.isIdentifier(prop.name)) {
      const propName = prop.name.text;
      const isRequired = prop.initializer.kind === ts.SyntaxKind.TrueKeyword;
      props.set(propName, isRequired);
    }
  }

  return props;
}

function analyzeClass (
  sourceFile: ts.SourceFile,
  classNode: ts.ClassDeclaration,
  argsTypes: Map<string, ts.TypeAliasDeclaration>,
): ClassAnalysis | null {
  if (!classNode.name) return null;

  const className = classNode.name.text;
  if (!className.endsWith('Expr')) return null;

  const argsTypeName = `${className}Args`;
  const analysis: ClassAnalysis = {
    className,
    argsTypeName,
    argTypesProps: new Map(),
    existingGetters: new Set(),
    existingArgsTypeProps: new Set(),
    argsTypeNode: argsTypes.get(argsTypeName),
    classNode,
    lastMemberPos: 0,
  };

  // Analyze class members
  for (const member of classNode.members) {
    analysis.lastMemberPos = Math.max(analysis.lastMemberPos, member.getEnd());

    // Find static argTypes
    if (
      ts.isPropertyDeclaration(member)
      && member.name
      && ts.isIdentifier(member.name)
      && member.name.text === 'argTypes'
      && member.modifiers?.some((m) => m.kind === ts.SyntaxKind.StaticKeyword)
    ) {
      // Parse the argTypes object - handle both direct object literals and wrapped in satisfies
      let argTypesObj: ts.ObjectLiteralExpression | undefined;

      if (member.initializer) {
        if (ts.isObjectLiteralExpression(member.initializer)) {
          argTypesObj = member.initializer;
        } else if (ts.isAsExpression(member.initializer) || ts.isSatisfiesExpression(member.initializer)) {
          // Handle: {} satisfies RequiredMap<...> or {} as Record<...>
          const expr = member.initializer as any;
          if (expr.expression && ts.isObjectLiteralExpression(expr.expression)) {
            argTypesObj = expr.expression;
          }
        }
      }

      if (argTypesObj) {
        analysis.argTypesProps = parseArgTypesObject(argTypesObj);
      }
    }

    // Find existing getters
    if (ts.isGetAccessorDeclaration(member) && member.name && ts.isIdentifier(member.name)) {
      const getterName = member.name.text;
      if (getterName.startsWith('$')) {
        const propName = getterName.slice(1);
        analysis.existingGetters.add(propName);
      }
    }
  }

  // Analyze Args type
  if (analysis.argsTypeNode) {
    const typeNode = analysis.argsTypeNode.type;

    if (ts.isIntersectionTypeNode(typeNode)) {
      // Look for the object type (not the BaseExpressionArgs part)
      for (const type of typeNode.types) {
        if (ts.isTypeLiteralNode(type)) {
          for (const member of type.members) {
            if (ts.isPropertySignature(member) && member.name && ts.isIdentifier(member.name)) {
              analysis.existingArgsTypeProps.add(member.name.text);
            }
          }
        }
      }
    } else if (ts.isTypeLiteralNode(typeNode)) {
      for (const member of typeNode.members) {
        if (ts.isPropertySignature(member) && member.name && ts.isIdentifier(member.name)) {
          analysis.existingArgsTypeProps.add(member.name.text);
        }
      }
    }
  }

  return analysis;
}

function generateGetter (propName: string, isRequired: boolean): string {
  const type = inferPropertyType(propName);
  const returnType = isRequired
    ? type
    : `${type} | undefined`;

  if (type.endsWith('[]')) {
    // Array type
    return `  get $${propName} (): ${returnType} {\n    return this.args.${propName} as ${returnType};\n  }\n`;
  } else {
    return `  get $${propName} (): ${returnType} {\n    return this.args.${propName} as ${returnType};\n  }\n`;
  }
}

function updateArgsType (
  sourceText: string,
  argsTypeNode: ts.TypeAliasDeclaration,
  missingProps: Map<string, boolean>,
): string {
  const typeNode = argsTypeNode.type;

  // Find the object literal type node
  let objectTypeNode: ts.TypeLiteralNode | null = null;
  let insertBeforePos: number;

  if (ts.isIntersectionTypeNode(typeNode)) {
    for (const type of typeNode.types) {
      if (ts.isTypeLiteralNode(type)) {
        objectTypeNode = type;
        break;
      }
    }
  } else if (ts.isTypeLiteralNode(typeNode)) {
    objectTypeNode = typeNode;
  }

  if (!objectTypeNode) {
    // If no object type exists, we need to create one
    // This means the Args type is just BaseExpressionArgs
    // We need to transform: type FooArgs = BaseExpressionArgs;
    // into: type FooArgs = { prop?: Type; } & BaseExpressionArgs;

    const props = Array.from(missingProps.entries())
      .map(([name, isRequired]) => {
        const type = inferPropertyType(name);
        const optional = isRequired
          ? ''
          : '?';
        return `  ${name}${optional}: ${type};`;
      })
      .join('\n');

    const typeStart = argsTypeNode.type.getStart();
    const typeEnd = argsTypeNode.type.getEnd();
    const originalType = sourceText.substring(typeStart, typeEnd);

    const newType = `{\n${props}\n  [key: string]: unknown;\n} & ${originalType}`;

    return sourceText.slice(0, typeStart) + newType + sourceText.slice(typeEnd);
  }

  // Find insertion point (before the index signature or closing brace)
  insertBeforePos = objectTypeNode.members[objectTypeNode.members.length - 1]?.getEnd() || objectTypeNode.getStart() + 1;

  // Check if there's an index signature
  const hasIndexSignature = objectTypeNode.members.some((m) => ts.isIndexSignatureDeclaration(m));

  if (hasIndexSignature) {
    // Insert before the index signature
    const indexSig = objectTypeNode.members.find((m) => ts.isIndexSignatureDeclaration(m));
    if (indexSig) {
      insertBeforePos = indexSig.getFullStart();
    }
  }

  // Generate new properties
  const newProps = Array.from(missingProps.entries())
    .map(([name, isRequired]) => {
      const type = inferPropertyType(name);
      const optional = isRequired
        ? ''
        : '?';
      return `  ${name}${optional}: ${type};\n`;
    })
    .join('');

  return sourceText.slice(0, insertBeforePos) + newProps + sourceText.slice(insertBeforePos);
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

  // Collect all Args type declarations
  const argsTypes = new Map<string, ts.TypeAliasDeclaration>();
  ts.forEachChild(sourceFile, (node) => {
    if (ts.isTypeAliasDeclaration(node) && node.name.text.endsWith('Args')) {
      argsTypes.set(node.name.text, node);
    }
  });

  const analyses: ClassAnalysis[] = [];

  // Analyze all classes
  ts.forEachChild(sourceFile, (node) => {
    if (ts.isClassDeclaration(node)) {
      const analysis = analyzeClass(sourceFile, node, argsTypes);
      if (analysis && 0 < analysis.argTypesProps.size) {
        analyses.push(analysis);
      }
    }
  });

  console.log(`Found ${analyses.length} classes with argTypes\n`);

  // Generate modifications
  let modifiedText = sourceText;
  let totalGettersAdded = 0;
  let totalPropsAdded = 0;

  // Sort by position (descending) to apply from end to start
  analyses.sort((a, b) => b.lastMemberPos - a.lastMemberPos);

  for (const analysis of analyses) {
    const missingGetters: string[] = [];
    const missingArgsProps = new Map<string, boolean>();

    for (const [propName, isRequired] of analysis.argTypesProps) {
      // Check if getter exists
      if (!analysis.existingGetters.has(propName)) {
        missingGetters.push(generateGetter(propName, isRequired));
      }

      // Check if property exists in Args type
      if (!analysis.existingArgsTypeProps.has(propName)) {
        missingArgsProps.set(propName, isRequired);
      }
    }

    if (missingGetters.length === 0 && missingArgsProps.size === 0) continue;

    console.log(`\nProcessing: ${analysis.className}`);

    // Add getters to class
    if (0 < missingGetters.length) {
      const gettersText = '\n' + missingGetters.join('\n');
      modifiedText = modifiedText.slice(0, analysis.lastMemberPos)
        + gettersText
        + modifiedText.slice(analysis.lastMemberPos);

      console.log(`  - Added ${missingGetters.length} getter(s)`);
      totalGettersAdded += missingGetters.length;
    }

    // Update Args type
    if (0 < missingArgsProps.size && analysis.argsTypeNode) {
      modifiedText = updateArgsType(modifiedText, analysis.argsTypeNode, missingArgsProps);
      console.log(`  - Added ${missingArgsProps.size} property(ies) to ${analysis.argsTypeName}`);
      totalPropsAdded += missingArgsProps.size;

      // Re-parse to update positions
      const newSourceFile = ts.createSourceFile(
        EXPRESSIONS_FILE,
        modifiedText,
        ts.ScriptTarget.Latest,
        true,
      );

      // Update positions for remaining analyses
      for (const remaining of analyses) {
        if (remaining === analysis) continue;

        ts.forEachChild(newSourceFile, (node) => {
          if (ts.isClassDeclaration(node) && node.name?.text === remaining.className) {
            remaining.lastMemberPos = node.members[node.members.length - 1]?.getEnd() || node.getEnd();
          }
          if (ts.isTypeAliasDeclaration(node) && node.name.text === remaining.argsTypeName) {
            remaining.argsTypeNode = node;
          }
        });
      }
    }
  }

  if (totalGettersAdded === 0 && totalPropsAdded === 0) {
    console.log('\nNo modifications needed!');
    return;
  }

  console.log('\n\nSummary:');
  console.log(`  - Total getters added: ${totalGettersAdded}`);
  console.log(`  - Total properties added to Args types: ${totalPropsAdded}`);

  fs.writeFileSync(EXPRESSIONS_FILE, modifiedText);
  console.log(`\nModifications applied to: ${EXPRESSIONS_FILE}`);
}

main();
