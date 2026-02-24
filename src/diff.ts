import { Dialect } from './dialects';
import {
  AliasExpr, AnonymousExpr, BooleanExpr, ColumnExpr, DataTypeExpr, Expression, IdentifierExpr, JoinExpr, LambdaExpr, LiteralExpr, TableExpr, WindowExpr,
} from './expressions';
import type { Generator as SqlGenerator } from './generator';

/**
 * Indicates that a new node has been inserted
 */
export class Insert {
  constructor (public readonly expression: Expression) {}
}

/**
 * Indicates that an existing node has been removed
 */
export class Remove {
  constructor (public readonly expression: Expression) {}
}

/**
 * Indicates that an existing node's position within the tree has changed
 */
export class Move {
  constructor (
    public readonly source: Expression,
    public readonly target: Expression,
  ) {}
}

/**
 * Indicates that an existing node has been updated
 */
export class Update {
  constructor (
    public readonly source: Expression,
    public readonly target: Expression,
  ) {}
}

/**
 * Indicates that an existing node hasn't been changed
 */
export class Keep {
  constructor (
    public readonly source: Expression,
    public readonly target: Expression,
  ) {}
}

/**
 * Union type representing any possible tree edit operation.
 */
export type Edit = Insert | Remove | Move | Update | Keep;

type MatchingPair = [Expression, Expression];

export function diff (
  source: Expression,
  target: Expression,
  options: {
    matchings?: MatchingPair[];
    deltaOnly?: boolean;
    [key: string]: unknown;
  } = {},
): Edit[] {
  let {
    // eslint-disable-next-line prefer-const
    matchings = [], deltaOnly = false, ...kwargs
  } = options;

  function computeNodeMappings (
    oldNodes: Expression[],
    newNodes: Expression[],
  ): Map<Expression, Expression> {
    const nodeMapping = new Map<Expression, Expression>();
    // Zip reversed arrays
    for (let i = 0; i < oldNodes.length; i++) {
      const oldNode = oldNodes[oldNodes.length - 1 - i];
      const newNode = newNodes[newNodes.length - 1 - i];
      newNode.computeHash();
      nodeMapping.set(oldNode, newNode);
    }
    return nodeMapping;
  }

  const sourceNodes = Array.from(source.walk());
  const targetNodes = Array.from(target.walk());

  const sourceSet = new Set(sourceNodes);
  const targetSet = new Set(targetNodes);

  const copy = (
    sourceNodes.length !== sourceSet.size
    || targetNodes.length !== targetSet.size
    || sourceNodes.some((n) => targetSet.has(n))
  );

  const sourceCopy = copy ? source.copy() : source;
  const targetCopy = copy ? target.copy() : target;

  try {
    if (copy && 0 < matchings.length) {
      const sourceMapping = computeNodeMappings(sourceNodes, Array.from(sourceCopy.walk()));
      const targetMapping = computeNodeMappings(targetNodes, Array.from(targetCopy.walk()));
      matchings = matchings.map(([s, t]) => [sourceMapping.get(s)!, targetMapping.get(t)!]);
    } else {
      [...sourceNodes, ...targetNodes].reverse().forEach((node) => {
        node.computeHash();
      });
    }

    return new ChangeDistiller(kwargs).diff(
      sourceCopy,
      targetCopy,
      {
        deltaOnly,
        matchings,
      },
    );
  } finally {
    if (!copy) {
      [...sourceNodes, ...targetNodes].forEach((node) => {
        node.resetHash();
      });
    }
  }
}

const UPDATABLE_EXPRESSION_TYPES = [
  AliasExpr,
  BooleanExpr,
  ColumnExpr,
  DataTypeExpr,
  LambdaExpr,
  LiteralExpr,
  TableExpr,
  WindowExpr,
];

const IGNORED_LEAF_EXPRESSION_TYPES = [IdentifierExpr];

class ChangeDistiller {
  private f: number;
  private t: number;
  private sqlGenerator: SqlGenerator;
  private bigramHistoCache = new Map<Expression, Map<string, number>>();
  private unmatchedSourceNodes = new Set<Expression>();
  private unmatchedTargetNodes = new Set<Expression>();
  private source?: Expression;
  private sourceIndex = new Map<Expression, Expression>();
  private target?: Expression;
  private targetIndex = new Map<Expression, Expression>();

  constructor (options: {
    f?: number;
    t?: number;
    dialect?: string;
  } = {}) {
    const {
      f = 0.6,
      t = 0.6,
      dialect,
    } = options;

    this.f = f;
    this.t = t;
    this.sqlGenerator = Dialect.getOrRaise(dialect).generator({ comments: false });
  }

  diff (
    source: Expression,
    target: Expression,
    options: {
      deltaOnly?: boolean;
      matchings?: MatchingPair[];
    } = {},
  ): Edit[] {
    const {
      deltaOnly = false,
      matchings = [],
    } = options;

    const preMatchedNodes = new Map<Expression, Expression>(matchings);

    this.source = source;
    this.target = source;

    for (const n of source.bfs()) {
      if (!IGNORED_LEAF_EXPRESSION_TYPES.some((type) => n instanceof type)) {
        this.sourceIndex.set(n, n);
        if (!preMatchedNodes.has(n)) this.unmatchedSourceNodes.add(n);
      }
    }

    for (const n of target.bfs()) {
      if (!IGNORED_LEAF_EXPRESSION_TYPES.some((type) => n instanceof type)) {
        this.targetIndex.set(n, n);
        // Check if n is a target in preMatchedNodes
        let isPreMatched = false;
        for (const targetMatch of preMatchedNodes.values()) {
          if (targetMatch === n) {
            isPreMatched = true;
            break;
          }
        }
        if (!isPreMatched) this.unmatchedTargetNodes.add(n);
      }
    }

    this.bigramHistoCache.clear();

    const computedMatchings = this.computeMatchingSet();
    preMatchedNodes.forEach((v, k) => computedMatchings.set(k, v));

    return this.generateEditScript(computedMatchings, { deltaOnly });
  }

  private generateEditScript (
    matchings: Map<Expression, Expression>,
    options: { deltaOnly: boolean },
  ): Edit[] {
    const { deltaOnly } = options;
    const editScript: Edit[] = [];

    this.unmatchedSourceNodes.forEach((node) => editScript.push(new Remove(node)));
    this.unmatchedTargetNodes.forEach((node) => editScript.push(new Insert(node)));

    matchings.forEach((targetNode, sourceNode) => {
      const identicalNodes = sourceNode.equals(targetNode);

      if (!UPDATABLE_EXPRESSION_TYPES.some((type) => sourceNode instanceof type) || identicalNodes) {
        if (identicalNodes) {
          const sParent = sourceNode.parent;
          const tParent = targetNode.parent;

          if (
            (!sParent && tParent)
            || (sParent && !tParent)
            || (sParent && tParent && matchings.get(sParent) !== tParent)
          ) {
            editScript.push(new Move(sourceNode, targetNode));
          }
        } else {
          editScript.push(...this.generateMoveEdits(sourceNode, targetNode, matchings));
        }

        // Check non-expression leaves (attributes)
        if (JSON.stringify(getNonExpressionLeaves(sourceNode)) === JSON.stringify(getNonExpressionLeaves(targetNode))) {
          editScript.push(new Update(sourceNode, targetNode));
        } else if (!deltaOnly) {
          editScript.push(new Keep(sourceNode, targetNode));
        }
      } else {
        editScript.push(new Update(sourceNode, targetNode));
      }
    });

    return editScript;
  }

  private generateMoveEdits (
    source: Expression,
    target: Expression,
    matchings: Map<Expression, Expression>,
  ): Move[] {
    const sourceArgs = expressionOnlyArgs(source);
    const targetArgs = expressionOnlyArgs(target);

    // LCS expects a comparator that returns true if source element matches target element
    const lcsResult = lcs(
      sourceArgs,
      targetArgs,
      (l, r) => matchings.get(l) === r,
    );
    const argsLcs = new Set(lcsResult);

    const moveEdits: Move[] = [];
    for (const a of sourceArgs) {
      // If the node is matched but its position in the arg list changed (not in LCS), it's a Move
      if (!argsLcs.has(a) && !this.unmatchedSourceNodes.has(a)) {
        const targetNode = matchings.get(a)!;
        moveEdits.push(new Move(this.sourceIndex.get(a)!, this.targetIndex.get(targetNode)!));
      }
    }

    return moveEdits;
  }

  private computeMatchingSet (): Map<Expression, Expression> {
    const leavesMatchingSet = this.computeLeafMatchingSet();
    const matchingSet = new Map<Expression, Expression>(leavesMatchingSet);

    // Maintain BFS order for unmatched nodes
    const orderedUnmatchedSource: Expression[] = [];
    for (const n of this.source?.bfs() ?? []) {
      if (this.unmatchedSourceNodes.has(n)) orderedUnmatchedSource.push(n);
    }

    const orderedUnmatchedTarget: Expression[] = [];
    for (const n of this.target?.bfs() ?? []) {
      if (this.unmatchedTargetNodes.has(n)) orderedUnmatchedTarget.push(n);
    };

    for (const sourceNode of orderedUnmatchedSource) {
      for (const targetNode of orderedUnmatchedTarget) {
        if (isSameType(sourceNode, targetNode)) {
          const sourceLeafIds = new Set(getExpressionLeaves(sourceNode));
          const targetLeafIds = new Set(getExpressionLeaves(targetNode));

          const maxLeavesNum = Math.max(sourceLeafIds.size, targetLeafIds.size);
          let leafSimilarityScore = 0.0;

          if (0 < maxLeavesNum) {
            let commonLeavesNum = 0;
            for (const [sLeaf, tLeaf] of leavesMatchingSet) {
              if (sourceLeafIds.has(sLeaf) && targetLeafIds.has(tLeaf)) {
                commonLeavesNum++;
              }
            }
            leafSimilarityScore = commonLeavesNum / maxLeavesNum;
          }

          const adjustedT = 4 < Math.min(sourceLeafIds.size, targetLeafIds.size) ? this.t : 0.4;

          if (
            0.8 <= leafSimilarityScore
            || (adjustedT <= leafSimilarityScore && this.f <= this.diceCoefficient(sourceNode, targetNode))
          ) {
            matchingSet.set(sourceNode, targetNode);
            this.unmatchedSourceNodes.delete(sourceNode);
            this.unmatchedTargetNodes.delete(targetNode);
            orderedUnmatchedTarget.splice(orderedUnmatchedTarget.indexOf(targetNode), 1);
            break;
          }
        }
      }
    }

    return matchingSet;
  }

  private diceCoefficient (source: Expression, target: Expression): number {
    const sourceHisto = this.getBigramHisto(source);
    const targetHisto = this.getBigramHisto(target);

    let sourceTotal = 0;
    sourceHisto.forEach((v) => sourceTotal += v);
    let targetTotal = 0;
    targetHisto.forEach((v) => targetTotal += v);

    const totalGrams = sourceTotal + targetTotal;
    if (totalGrams === 0) return source.equals(target) ? 1.0 : 0.0;

    let overlapLen = 0;
    sourceHisto.forEach((count, gram) => {
      if (targetHisto.has(gram)) {
        overlapLen += Math.min(count, targetHisto.get(gram)!);
      }
    });

    return (2 * overlapLen) / totalGrams;
  }

  private computeLeafMatchingSet (): Map<Expression, Expression> {
    const candidateMatchings: Array<{
      similarity: number;
      parentSimilarity: number;
      index: number;
      sourceLeaf: Expression;
      targetLeaf: Expression;
    }> = [];

    const sourceExpressionLeaves = this.source ? getExpressionLeaves(this.source) : [];
    const targetExpressionLeaves = this.target ? getExpressionLeaves(this.target) : [];

    for (const sourceLeaf of sourceExpressionLeaves) {
      for (const targetLeaf of targetExpressionLeaves) {
        if (isSameType(sourceLeaf, targetLeaf)) {
          const similarityScore = this.diceCoefficient(sourceLeaf, targetLeaf);

          if (this.f <= similarityScore) {
            candidateMatchings.push({
              similarity: -similarityScore, // Negative for min-priority behavior
              parentSimilarity: -parentSimilarityScore(sourceLeaf, targetLeaf),
              index: candidateMatchings.length,
              sourceLeaf,
              targetLeaf,
            });
          }
        }
      }
    }

    // Sort to simulate heappop (highest similarity first)
    candidateMatchings.sort((a, b) => {
      if (a.similarity !== b.similarity) return a.similarity - b.similarity;
      if (a.parentSimilarity !== b.parentSimilarity) return a.parentSimilarity - b.parentSimilarity;
      return a.index - b.index;
    });

    const matchingSet = new Map<Expression, Expression>();

    for (const candidate of candidateMatchings) {
      const {
        sourceLeaf, targetLeaf,
      } = candidate;

      if (
        this.unmatchedSourceNodes.has(sourceLeaf)
        && this.unmatchedTargetNodes.has(targetLeaf)
      ) {
        matchingSet.set(sourceLeaf, targetLeaf);
        this.unmatchedSourceNodes.delete(sourceLeaf);
        this.unmatchedTargetNodes.delete(targetLeaf);
      }
    }

    return matchingSet;
  }

  private getBigramHisto (expression: Expression): Map<string, number> {
    if (this.bigramHistoCache.has(expression)) return this.bigramHistoCache.get(expression)!;

    const str = this.sqlGenerator.generate(expression);
    const histo = new Map<string, number>();
    for (let i = 0; i < str.length - 1; i++) {
      const gram = str.substring(i, i + 2);
      histo.set(gram, (histo.get(gram) || 0) + 1);
    }
    this.bigramHistoCache.set(expression, histo);
    return histo;
  }
}

/**
 * Recursively finds all leaf nodes in an expression tree.
 */
export function* getExpressionLeaves (expression: Expression): Generator<Expression> {
  let hasChildExprs = false;

  for (const node of expression.iterExpressions()) {
    if (!IGNORED_LEAF_EXPRESSION_TYPES.some((type) => node instanceof type)) {
      hasChildExprs = true;
      yield* getExpressionLeaves(node);
    }
  }

  if (!hasChildExprs) {
    yield expression;
  }
}

/**
 * Yields non-expression attributes (metadata) of a node for Update detection.
 */
export function* getNonExpressionLeaves (expression: Expression): Generator<[string, unknown]> {
  for (const [arg, value] of Object.entries(expression.args)) {
    if (
      value === null
      || value === undefined
      || value instanceof Expression
      || (Array.isArray(value) && value[0] instanceof Expression)
    ) {
      continue;
    }

    yield [arg, value];
  }
}

/**
 * Determines if two nodes are of the same semantic type.
 */
export function isSameType (source: Expression, target: Expression): boolean {
  if (source.constructor === target.constructor) {
    if (source instanceof JoinExpr && target instanceof JoinExpr) {
      return source.args.side === target.args.side;
    }

    if (source instanceof AnonymousExpr && target instanceof AnonymousExpr) {
      return source.args.this === target.args.this;
    }

    return true;
  }

  return false;
}

/**
 * Calculates a similarity score based on the depth of matching parent types.
 */
export function parentSimilarityScore (
  source: Expression | undefined,
  target: Expression | undefined,
): number {
  if (!source || !target || source.constructor !== target.constructor) {
    return 0;
  }

  return 1 + parentSimilarityScore(source.parent, target.parent);
}

/**
 * Yields only child expressions, filtering out ignored leaf types (like Identifiers).
 */
export function* expressionOnlyArgs (expression: Expression): Generator<Expression> {
  for (const arg of expression.iterExpressions()) {
    if (!IGNORED_LEAF_EXPRESSION_TYPES.some((type) => arg instanceof type)) {
      yield arg;
    }
  }
}

/**
 * Calculates the longest common subsequence between two sequences.
 * * This is used to detect "Move" edits by determining which nodes
 * maintained their relative order.
 */
export function lcs<T> (
  _seqA: Iterable<T>,
  _seqB: Iterable<T>,
  equal: (a: T, b: T) => boolean,
): T[] {
  const seqA = [..._seqA];
  const seqB = [..._seqB];
  const lenA = seqA.length;
  const lenB = seqB.length;

  // Initialize a 2D array for DP
  const lcsResult: T[][][] = Array.from({ length: lenA + 1 }, () =>
    Array.from({ length: lenB + 1 }, () => []));

  for (let i = 1; i <= lenA; i++) {
    for (let j = 1; j <= lenB; j++) {
      if (equal(seqA[i - 1], seqB[j - 1])) {
        // If equal, take the diagonal result and add current element
        lcsResult[i][j] = [...lcsResult[i - 1][j - 1], seqA[i - 1]];
      } else {
        // Otherwise, take the maximum of top or left
        const top = lcsResult[i - 1][j];
        const left = lcsResult[i][j - 1];
        lcsResult[i][j] = left.length < top.length ? top : left;
      }
    }
  }

  return lcsResult[lenA][lenB];
}
