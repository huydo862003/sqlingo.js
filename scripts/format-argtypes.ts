#!/usr/bin/env tsx

import { Project, SyntaxKind } from 'ts-morph';
import * as path from 'path';

const EXPRESSIONS_FILE = path.join(process.cwd(), 'src/expressions.ts');

function formatArgTypes () {
  console.log('Processing TypeScript file:', EXPRESSIONS_FILE);

  const project = new Project({
    tsConfigFilePath: path.join(process.cwd(), 'tsconfig.json'),
  });

  const sourceFile = project.addSourceFileAtPath(EXPRESSIONS_FILE);
  let changeCount = 0;

  // Process all classes
  const classes = sourceFile.getClasses();

  for (const classDecl of classes) {
    const className = classDecl.getName();
    if (!className) continue;

    // Find the static argTypes property
    const argTypesProperty = classDecl.getStaticProperty('argTypes');

    if (argTypesProperty) {
      const fullText = argTypesProperty.getFullText();

      // Check if already properly formatted (multiline)
      if (fullText.includes('= {\n') && fullText.includes('\n  }')) {
        continue; // Already formatted
      }

      let initializer = argTypesProperty.getInitializer();
      if (!initializer) continue;

      // Extract satisfies type from the text
      const propText = argTypesProperty.getText();
      const satisfiesMatch = propText.match(/satisfies\s+RequiredMap<(\w+)>/);
      const satisfiesType = satisfiesMatch ? satisfiesMatch[1] : undefined;

      // Handle satisfies expression to get the object
      if (initializer.getKind() === SyntaxKind.SatisfiesExpression) {
        const satisfiesExpr = initializer.asKindOrThrow(SyntaxKind.SatisfiesExpression);
        initializer = satisfiesExpr.getExpression();
      }

      if (initializer.getKind() === SyntaxKind.ObjectLiteralExpression) {
        const objectLiteral = initializer.asKindOrThrow(SyntaxKind.ObjectLiteralExpression);
        const properties = objectLiteral.getProperties();

        // Build formatted string - no leading spaces, ts-morph handles indentation
        let formattedProps: string;

        if (properties.length === 0) {
          // Empty object - multiline format
          formattedProps = '{\n}';
        } else {
          // Has properties - 2 additional spaces for properties
          const propStrings = properties.map((prop) => `  ${prop.getText()}`);
          formattedProps = `{\n${propStrings.join(',\n')},\n}`;
        }

        // Build full replacement
        const satisfiesPart = satisfiesType ? ` satisfies RequiredMap<${satisfiesType}>` : '';
        const newText = `static argTypes: Record<string, boolean> = ${formattedProps}${satisfiesPart};`;

        argTypesProperty.replaceWithText(newText);

        changeCount++;
        console.log(`  ✓ Formatted argTypes for ${className}`);
      }
    }
  }

  if (changeCount === 0) {
    console.log('\nNo formatting changes needed.');
    return;
  }

  sourceFile.saveSync();

  console.log(`\n✅ Successfully formatted ${changeCount} argTypes declarations`);
}

formatArgTypes();
