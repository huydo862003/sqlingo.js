import { UnsupportedError } from './errors';
import type {
  ExpressionOrString,
  ExpressionValue,
} from './expressions';
import {
  alias,
  AliasesExpr,
  AliasExpr,
  AndExpr,
  AnonymousExpr,
  AnyExpr,
  ApproxQuantileExpr,
  ArrayExpr,
  ArraySizeExpr,
  BinaryExpr,
  cast,
  CastExpr,
  CoalesceExpr,
  column,
  ColumnDefExpr,
  ColumnExpr,
  CreateExpr,
  CreateExprKind,
  CteExpr,
  DataTypeExpr,
  DataTypeExprKind,
  DataTypeParamExpr,
  EqExpr,
  ExistsExpr,
  ExplodeExpr,
  ExplodeOuterExpr,
  Expression,
  FromExpr,
  FuncExpr,
  GenerateDateArrayExpr,
  GenerateSeriesExpr,
  GreatestExpr,
  GtExpr,
  IdentifierExpr,
  IfExpr,
  ILikeExpr,
  InlineExpr,
  IntervalExpr,
  JoinExpr,
  JoinExprKind,
  LambdaExpr,
  LateralExpr,
  LikeExpr,
  LiteralExpr,
  LteExpr,
  OrderedExpr,
  OrderExpr,
  ParenExpr,
  PartitionedByPropertyExpr,
  PERCENTILES,
  PosexplodeExpr,
  PropertyEqExpr,
  QualifyExpr,
  QueryExpr,
  RowNumberExpr,
  SchemaExpr,
  select,
  SelectExpr,
  SetOperationExpr,
  StarExpr,
  StructExpr,
  SubExpr,
  SubqueryPredicateExpr,
  TableAliasExpr,
  TableExpr,
  TemporaryPropertyExpr,
  toIdentifier,
  TryCastExpr,
  TupleExpr,
  union,
  UniqueColumnConstraintExpr,
  UnnestExpr,
  WhereExpr,
  WindowExpr,
  WithExpr,
  WithinGroupExpr,
} from './expressions';
import type { Generator } from './generator';
import {
  findNewName, nameSequence,
} from './helper';
import { ensureBools as canonicalizeEnsureBools } from './optimizer/canonicalize';
import {
  findAllInScope, normalize, Scope, traverseScope,
} from './optimizer';
import {
  assertIsInstanceOf, enumFromString, filterInstanceOf, isInstanceOf, narrowInstanceOf,
} from './port_internals';

/**
 * Creates a new transform by chaining a sequence of transformations.
 */
export function preprocess (
  transforms: ((e: Expression) => Expression)[],
  generator?: (self: Generator, e: Expression) => string,
): (self: Generator, expression: Expression) => string {
  return (self: Generator, expression: Expression): string => {
    let currentExpr = expression;
    const originalType = currentExpr.constructor;

    try {
      for (const transform of transforms) {
        currentExpr = transform(currentExpr);
      }
    } catch (e) {
      if (e instanceof UnsupportedError) {
        self.unsupported(e.message);
      } else {
        throw e;
      }
    }

    if (generator) {
      return generator(self, currentExpr);
    }

    const sqlHandler = self[`${currentExpr._constructor.key}Sql` as keyof typeof self];
    if (sqlHandler instanceof Function) {
      return (sqlHandler as (e: Expression) => string).call(self, currentExpr);
    }

    const transformHandler = self._constructor.TRANSFORMS.get(currentExpr._constructor);
    if (transformHandler) {
      if (originalType === currentExpr.constructor) {
        if (currentExpr instanceof FuncExpr) {
          return self.functionFallbackSql(currentExpr);
        }
        throw new Error(`Expression type ${currentExpr.constructor.name} requires a _sql method to be transformed.`);
      }
      return transformHandler(self, currentExpr);
    }

    throw new Error(`Unsupported expression type ${currentExpr.constructor.name}.`);
  };
}

export function unnestGenerateDateArrayUsingRecursiveCte (expression: Expression): Expression {
  if (expression instanceof SelectExpr) {
    let count = 0;
    const recursiveCtes: Expression[] = [];

    for (const unnest of expression.findAll(UnnestExpr)) {
      const parent = unnest.parent;
      if (
        !(parent instanceof FromExpr || parent instanceof JoinExpr)
        || unnest.args.expressions?.length !== 1
        || !(unnest.args.expressions[0] instanceof GenerateDateArrayExpr)
      ) {
        continue;
      }

      const genDateArray = unnest.args.expressions[0];
      const start = genDateArray.args.start;
      const end = genDateArray.args.end;
      const step = genDateArray.args.step;

      if (!start || !end || !(step instanceof IntervalExpr)) {
        continue;
      }

      const aliasExpr = unnest.args.alias;
      const rawColumnName = aliasExpr instanceof TableAliasExpr ? aliasExpr.columns[0] : undefined;
      const columnName: string | IdentifierExpr = isInstanceOf(rawColumnName, IdentifierExpr) ? rawColumnName : isInstanceOf(rawColumnName, Expression) ? rawColumnName.name : 'date_value';

      const startCast = cast(start, 'date');
      const dateAdd = new AnonymousExpr({
        this: 'date_add',
        expressions: [
          columnName,
          LiteralExpr.number(step.name),
          step.args.unit || '',
        ],
      });
      const castDateAdd = cast(dateAdd, 'date');

      const cteName = `_generated_dates${count ? `_${count}` : ''}`;

      const baseQuery = select(new AliasExpr({
        this: startCast,
        alias: columnName,
      }));
      const recursiveQuery = select(castDateAdd)
        .from(cteName)
        .where(new LteExpr({
          this: castDateAdd,
          expression: cast(end, 'date'),
        }));

      const cteQuery = baseQuery.union(recursiveQuery, { distinct: false });
      unnest.replace(
        select(columnName)
          .from(cteName)
          .subquery(cteName),
      );

      recursiveCtes.push(
        alias(
          new CteExpr({ this: cteQuery }),
          cteName,
          {
            table: [columnName],
          },
        ),
      );
      count++;
    }

    if (0 < recursiveCtes.length) {
      const withExpr = narrowInstanceOf(expression.args.with, WithExpr) ?? new WithExpr({
        expressions: [],
      });
      withExpr.setArgKey('recursive', true);
      withExpr.setArgKey('expressions', [...recursiveCtes, ...(withExpr.args.expressions ?? [])]);
      expression.setArgKey('with', withExpr);
    }
  }
  return expression;
}

export function unnestGenerateSeries (expression: Expression): Expression {
  const thisArg = expression.args.this;
  if (expression instanceof TableExpr && thisArg instanceof GenerateSeriesExpr) {
    const unnest = new UnnestExpr({ expressions: [thisArg] });
    if (expression.args.alias) {
      return alias(
        unnest,
        '_u',
        {
          table: [
            expression.args.alias instanceof TableAliasExpr
              ? (
                isInstanceOf(expression.args.alias.args.this, IdentifierExpr)
                  ? expression.args.alias.args.this
                  : typeof expression.args.alias.args.this === 'string' ? expression.args.alias.args.this : '')
              : (typeof expression.args.alias === 'string' ? expression.args.alias : ''),
          ],
        },
      );
    }
    return unnest;
  }
  return expression;
}

export function eliminateDistinctOn (expression: Expression): Expression {
  if (
    expression instanceof SelectExpr
    && expression.args.distinct
    && expression.args.distinct.getArgKey('on') instanceof TupleExpr
  ) {
    const rowNumberAlias = findNewName(expression.namedSelects, '_row_number');
    const distinctCols = (expression.args.distinct.getArgKey('on') as TupleExpr).args.expressions;

    const window = new WindowExpr({
      this: new RowNumberExpr({}),
      partitionBy: distinctCols,
    });

    const order = expression.args.order;
    if (order) {
      window.setArgKey('order', order.pop());
    } else {
      window.setArgKey('order', new OrderExpr({ expressions: distinctCols?.map((c) => c instanceof Expression ? c.copy() : c) ?? [] }));
    }

    expression.select(alias(window, rowNumberAlias), { copy: false });

    let newSelects: (ExpressionOrString | undefined)[] = [];
    const takenNames = [rowNumberAlias];

    for (const select of expression.selects.slice(0, -1)) {
      if (select.isStar) {
        newSelects = [new StarExpr({})];
        break;
      }
      let current: ExpressionValue | undefined = select;
      if (!(current instanceof AliasExpr)) {
        const aliasExpr = findNewName(takenNames, current.outputName || '_col');
        const quoted = current instanceof ColumnExpr ? !!current.getArgKey('quoted') : undefined;
        current = current.replace(alias(
          current,
          aliasExpr,
          { quoted },
        ));
      }
      takenNames.push(current.outputName);
      newSelects.push(current.getArgKey('alias') as Expression | string | undefined);
    }

    return select(newSelects)
      .from(expression.subquery('_t', { copy: false }))
      .where(new ColumnExpr({ this: rowNumberAlias }).eq(1));
  }
  return expression;
}

export function eliminateQualify (expression: Expression): Expression {
  if (expression instanceof SelectExpr && expression.args.qualify) {
    const taken = expression.namedSelects;
    for (const select of expression.selects) {
      if (!select.aliasOrName) {
        const alias = findNewName(taken, '_c');
        select.replace(new AliasExpr({
          this: select,
          alias,
        }));
        taken.push(alias);
      }
    }

    const selectAliasOrName = (s: Expression) => {
      const alias = s.aliasOrName;
      const id = s.getArgKey('alias') || s.args.this;
      if (id instanceof IdentifierExpr) {
        return column(
          { col: alias },
          { quoted: id.args.quoted },
        );
      }
      return alias;
    };

    const outerSelects = select(expression.selects.map(selectAliasOrName));
    let qualifyFilters = expression.args.qualify.pop().args.this;

    const expressionByAlias: Record<string, Expression | undefined> = {};
    for (const s of expression.selects) {
      if (s instanceof AliasExpr) expressionByAlias[s.alias] = s.args.this;
    }

    const candidates = isInstanceOf(qualifyFilters, Expression) ? qualifyFilters.findAll<WindowExpr | ColumnExpr>(expression.isStar ? [WindowExpr] : [WindowExpr, ColumnExpr]) : [];
    for (const candidate of candidates) {
      if (candidate instanceof WindowExpr) {
        for (const col of candidate.findAll(ColumnExpr)) {
          const expr = expressionByAlias[col.name];
          if (expr) col.replace(expr);
        }
        const aliasExpr = findNewName(expression.namedSelects, '_w');
        expression.select(alias(
          candidate,
          aliasExpr,
        ), { copy: false });
        const col = new ColumnExpr({ this: aliasExpr });

        if (candidate.parent instanceof QualifyExpr) {
          qualifyFilters = col;
        } else {
          candidate.replace(col);
        }
      } else if (!expression.namedSelects.includes(candidate.name)) {
        expression.select(candidate.copy(), { copy: false });
      }
    }

    return outerSelects.from(expression.subquery('_t', { copy: false })).where(isInstanceOf(qualifyFilters, Expression) ? qualifyFilters : typeof qualifyFilters === 'string' ? qualifyFilters : undefined);
  }
  return expression;
}

export function removePrecisionParameterizedTypes (expression: Expression): Expression {
  /**
   * Some dialects only allow the precision for parameterized types to be defined in the DDL and not in
   * other expressions. This transforms removes the precision from parameterized types in expressions.
   */
  for (const node of expression.findAll(DataTypeExpr)) {
    node.setArgKey(
      'expressions',
      node.args.expressions?.filter((e) => !(e instanceof DataTypeParamExpr)),
    );
  }

  return expression;
}

/**
 * Remove references to unnest table aliases, added by the optimizer's qualify_columns step.
 */
export function unqualifyUnnest (expression: Expression): Expression {
  if (expression instanceof SelectExpr) {
    const unnestAliases = new Set(
      findAllInScope(expression, [UnnestExpr])
        .filter((unnest) => unnest.parent instanceof FromExpr || unnest.parent instanceof JoinExpr)
        .map((unnest) => unnest.alias),
    );

    if (0 < unnestAliases.size) {
      for (const column of expression.findAll(ColumnExpr)) {
        const leftmostPart = column.parts[0];
        if (
          leftmostPart
          && leftmostPart.argKey !== 'this'
          && unnestAliases.has(leftmostPart.args.this as string)
        ) {
          leftmostPart.pop();
        }
      }
    }
  }

  return expression;
}

/**
 * Convert cross join unnest into lateral view explode.
 */
export function unnestToExplode (
  expression: Expression,
  options: {
    unnestUsingArraysZip?: boolean;
  } = {},
): Expression {
  const {
    unnestUsingArraysZip = true,
  } = options;
  function unnestZipExprs (
    u: UnnestExpr,
    unnestExprs: ExpressionValue[],
    options: { hasMultiExpr: boolean },
  ): ExpressionValue[] {
    const { hasMultiExpr } = options;
    if (hasMultiExpr) {
      if (!unnestUsingArraysZip) {
        throw new UnsupportedError('Cannot transpile UNNEST with multiple input arrays');
      }

      // Use INLINE(ARRAYS_ZIP(...)) for multiple expressions
      const zipExprs = [
        new AnonymousExpr({
          this: 'ARRAYS_ZIP',
          expressions: unnestExprs,
        }),
      ];
      u.setArgKey('expressions', zipExprs);
      return zipExprs;
    }
    return unnestExprs;
  }

  function getUdtfType (
    u: UnnestExpr,
    options: {
      hasMultiExpr?: boolean;
    } = {},
  ): typeof FuncExpr {
    const { hasMultiExpr } = options;
    if (u.args.offset) {
      return PosexplodeExpr;
    }
    return hasMultiExpr ? InlineExpr : ExplodeExpr;
  }

  if (expression instanceof SelectExpr) {
    const from = expression.args.from;

    if (from && from.args.this instanceof UnnestExpr) {
      const unnest = from.args.this;
      const alias = unnest.args.alias;
      const exprs = unnest.args.expressions;
      const hasMultiExpr = 1 < (exprs?.length ?? 0);
      const [thisArg] = unnestZipExprs(unnest, exprs ?? [], { hasMultiExpr });

      const columns: IdentifierExpr[] = isInstanceOf(alias, TableAliasExpr) ? filterInstanceOf(alias.columns, IdentifierExpr) : [];
      const offset = unnest.args.offset;
      if (offset) {
        columns.unshift(
          offset instanceof IdentifierExpr
            ? offset
            : new IdentifierExpr({
              this: 'pos',
              quoted: false,
            }),
        );
      }

      const UdtfClass = getUdtfType(unnest, { hasMultiExpr });
      unnest.replace(
        new TableExpr({
          this: new UdtfClass({ this: thisArg }),
          alias: isInstanceOf(alias, TableAliasExpr)
            ? new TableAliasExpr({
              this: isInstanceOf(alias.args.this, Expression) ? alias.args.this : typeof alias.args.this === 'string' ? alias.args.this : undefined,
              columns,
            })
            : undefined,
        }),
      );
    }

    const joins = expression.args.joins || [];
    for (const join of [...joins]) {
      const joinExpr = join.args.this;
      const isLateral = joinExpr instanceof LateralExpr;
      const unnest = isLateral ? joinExpr.args.this : joinExpr;

      if (unnest instanceof UnnestExpr) {
        const alias = isLateral ? joinExpr.args.alias : unnest.args.alias;
        const exprs = unnest.args.expressions;
        const hasMultiExpr = 1 < (exprs?.length ?? 0);
        const zippedExprs = unnestZipExprs(unnest, exprs ?? [], { hasMultiExpr });

        joins.splice(joins.indexOf(join), 1);

        const aliasAsTableAlias = isInstanceOf(alias, TableAliasExpr) ? alias : undefined;
        const aliasCols: IdentifierExpr[] = aliasAsTableAlias ? filterInstanceOf(aliasAsTableAlias.columns, IdentifierExpr) : [];

        if (!hasMultiExpr && aliasCols.length !== 1 && aliasCols.length !== 2) {
          throw new UnsupportedError(
            'CROSS JOIN UNNEST to LATERAL VIEW EXPLODE transformation requires explicit column aliases',
          );
        }

        const offset = unnest.args.offset;
        if (offset) {
          aliasCols.unshift(
            offset instanceof IdentifierExpr
              ? offset
              : new IdentifierExpr({
                this: 'pos',
                quoted: false,
              }),
          );
        }

        const UdtfClass = getUdtfType(unnest, { hasMultiExpr });
        for (const e of zippedExprs) {
          expression.append(
            'laterals',
            new LateralExpr({
              this: new UdtfClass({ this: e }),
              view: true,
              alias: new TableAliasExpr({
                this: aliasAsTableAlias ? (isInstanceOf(aliasAsTableAlias.args.this, Expression) ? aliasAsTableAlias.args.this : typeof aliasAsTableAlias.args.this === 'string' ? aliasAsTableAlias.args.this : undefined) : undefined,
                columns: aliasCols,
              }),
            }),
          );
        }
      }
    }
  }

  return expression;
}

export function moveCtesToTopLevel<T extends SelectExpr> (expression: T): T {
  let topLevelWith = expression.args.with;
  for (const innerWith of expression.findAll(WithExpr)) {
    if (innerWith.parent === expression) continue;

    if (!topLevelWith) {
      topLevelWith = innerWith.pop();
      expression.setArgKey('with', topLevelWith);
    } else {
      if (innerWith.recursive) topLevelWith.setArgKey('recursive', true);
      const parentCte = innerWith.findAncestor(CteExpr);
      innerWith.pop();

      const innerExprs = innerWith.args.expressions;
      assertIsInstanceOf(topLevelWith, WithExpr);
      if (parentCte) {
        const index = topLevelWith.args.expressions?.indexOf(parentCte) ?? -1;
        topLevelWith.args.expressions?.splice(index, 0, ...filterInstanceOf(innerExprs ?? [], Expression));
      } else {
        topLevelWith.setArgKey('expressions', [...topLevelWith.args.expressions ?? [], ...filterInstanceOf(innerExprs ?? [], Expression)]);
      }
    }
  }
  return expression;
}

export function structKvToAlias (expression: Expression): Expression {
  if (expression instanceof StructExpr) {
    expression.setArgKey(
      'expressions',
      expression.args.expressions?.map((e) =>
        e instanceof PropertyEqExpr
          ? alias(
            (() => {
              const v = e.args.expression;
              return (v instanceof Expression || typeof v === 'string') ? v : '';
            })(),
            isInstanceOf(e.args.this, IdentifierExpr) ? e.args.this : typeof e.args.this === 'string' ? e.args.this : undefined,
          )
          : e),
    );
  }
  return expression;
}

export function anyToExists (expression: Expression): Expression {
  if (expression instanceof SelectExpr) {
    for (const anyExpr of expression.findAll(AnyExpr)) {
      const thisArg = anyExpr.args.this;
      if (thisArg instanceof QueryExpr || anyExpr.parent instanceof LikeExpr || anyExpr.parent instanceof ILikeExpr) {
        continue;
      }

      const binop = anyExpr.parent;
      if (binop instanceof BinaryExpr) {
        const lambdaArg = new IdentifierExpr({
          this: 'x',
          quoted: false,
        });
        anyExpr.replace(lambdaArg);
        const lambdaExpr = new LambdaExpr({
          this: binop.copy(),
          expressions: [lambdaArg],
        });
        binop.replace(new ExistsExpr({
          this: thisArg?.unnest(),
          expression: lambdaExpr,
        }));
      }
    }
  }
  return expression;
}

/**
 * Convert explode/posexplode projections into unnests.
 */
export function explodeProjectionToUnnest (
  indexOffset: number = 0,
): (expression: Expression) => Expression {
  return (expression: Expression): Expression => {
    if (expression instanceof SelectExpr) {
      const takenSelectNames = [...expression.namedSelects];
      const scope = new Scope({ expression });
      const takenSourceNames = [...scope.references].map(([name]) => name);

      const newName = (names: string[], name: string): string => {
        const uniqueName = findNewName(names, name);
        names.push(uniqueName);
        return uniqueName;
      };

      const arrays: Expression[] = [];
      const seriesAlias = newName(takenSelectNames, 'pos');
      const unnestSourceBase = newName(takenSourceNames, '_u');

      const series = alias(
        new UnnestExpr({
          expressions: [new GenerateSeriesExpr({ start: LiteralExpr.number(indexOffset) })],
        }),
        unnestSourceBase,
        { table: [seriesAlias] },
      );

      for (const select of [...expression.selects]) {
        const explode = select.find(ExplodeExpr);

        if (explode) {
          let posAlias;
          let explodeAlias;
          let aliasExpr: Expression;

          if (select instanceof AliasExpr) {
            explodeAlias = select.alias;
            aliasExpr = select;
          } else if (select instanceof AliasesExpr) {
            posAlias = select.aliases[0];
            explodeAlias = select.aliases[1];
            aliasExpr = select.replace(new AliasExpr({
              this: select.args.this,
              alias: '',
            }));
          } else {
            aliasExpr = select.replace(new AliasExpr({
              this: select,
              alias: '',
            }));
            const foundExplode = aliasExpr.find(ExplodeExpr);
            if (foundExplode) {
              // Re-assign explode pointer to the one in the new alias tree
            }
          }

          const isPosexplode = explode instanceof PosexplodeExpr;
          let explodeArg = explode.args.this;

          if (explode instanceof ExplodeOuterExpr) {
            const bracket = explodeArg instanceof Expression ? explodeArg.args.expressions?.[0] : undefined;
            if (bracket instanceof Expression) {
              bracket.setArgKey('safe', true);
              bracket.setArgKey('offset', true);

              explodeArg = new AnonymousExpr({
                this: 'IF',
                expressions: [
                  new EqExpr({
                    this: new AnonymousExpr({
                      this: 'ARRAY_SIZE',
                      expressions: explodeArg !== undefined ? [new CoalesceExpr({ expressions: [explodeArg, new ArrayExpr({ expressions: [] })] })] : undefined,
                    }),
                    expression: LiteralExpr.number(0),
                  }),
                  new ArrayExpr({ expressions: [bracket.copy()] }),
                  ...(explodeArg !== undefined ? [explodeArg] : []),
                ],
              });
            }
          }

          if (explodeArg instanceof ColumnExpr) {
            takenSelectNames.push(explodeArg.outputName);
          }

          const unnestSourceAlias = newName(takenSourceNames, '_u');

          if (!explodeAlias) explodeAlias = newName(takenSelectNames, 'col');
          if (isPosexplode && !posAlias) posAlias = newName(takenSelectNames, 'pos');
          if (!posAlias) posAlias = newName(takenSelectNames, 'pos');

          aliasExpr.setArgKey('alias', new IdentifierExpr({
            this: explodeAlias,
            quoted: false,
          }));

          const seriesTableAlias = series.args.alias;
          const column = new IfExpr({
            this: new ColumnExpr({
              this: seriesAlias,
              table: seriesTableAlias,
            }).eq(
              new ColumnExpr({
                this: posAlias,
                table: unnestSourceAlias,
              }),
            ),
            true: new ColumnExpr({
              this: explodeAlias,
              table: unnestSourceAlias,
            }),
          });

          explode.replace(column);

          if (isPosexplode) {
            const exprs = expression.args.expressions;
            const index = exprs?.indexOf(aliasExpr) || -1;
            exprs?.splice(index + 1, 0, alias(
              new IfExpr({
                this: new ColumnExpr({
                  this: seriesAlias,
                  table: seriesTableAlias,
                }).eq(
                  new ColumnExpr({
                    this: posAlias,
                    table: unnestSourceAlias,
                  }),
                ),
                true: new ColumnExpr({
                  this: posAlias,
                  table: unnestSourceAlias,
                }),
              }),
              isInstanceOf(posAlias, IdentifierExpr) ? posAlias : typeof posAlias === 'string' ? posAlias : undefined,
            ));
          }

          if (arrays.length === 0) {
            if (expression.args.from) {
              expression.join(series, { joinType: JoinExprKind.CROSS });
            } else {
              expression.from(series);
            }
          }

          const size = new ArraySizeExpr({ this: (explodeArg instanceof Expression ? explodeArg.copy() : explodeArg) });
          arrays.push(size);

          expression.join(
            alias(
              new UnnestExpr({
                expressions: explodeArg !== undefined ? [explodeArg instanceof Expression ? explodeArg.copy() : explodeArg] : undefined,
                offset: new IdentifierExpr({
                  this: posAlias,
                  quoted: false,
                }),
              }),
              unnestSourceAlias,
              { table: [isInstanceOf(explodeAlias, IdentifierExpr) ? explodeAlias : typeof explodeAlias === 'string' ? explodeAlias : ''] },
            ),
            { joinType: JoinExprKind.CROSS },
          );

          let limit: SubExpr | ArraySizeExpr = size;
          if (indexOffset !== 1) {
            limit = new SubExpr({
              this: size,
              expression: LiteralExpr.number(1),
            });
          }

          expression.where(
            new ColumnExpr({
              this: seriesAlias,
              table: seriesTableAlias,
            })
              .eq(new ColumnExpr({
                this: posAlias,
                table: unnestSourceAlias,
              }))
              .or(
                new AndExpr({
                  this: new GtExpr({
                    this: new ColumnExpr({
                      this: seriesAlias,
                      table: seriesTableAlias,
                    }),
                    expression: limit,
                  }),
                  expression: new EqExpr({
                    this: new ColumnExpr({
                      this: posAlias,
                      table: unnestSourceAlias,
                    }),
                    expression: limit,
                  }),
                }),
              ),
          );
        }
      }

      if (0 < arrays.length) {
        let end: Expression = new GreatestExpr({
          this: arrays[0],
          expressions: arrays.slice(1),
          ignoreNulls: false,
        });
        if (indexOffset !== 1) {
          end = new SubExpr({
            this: end,
            expression: LiteralExpr.number(1 - indexOffset),
          });
        }
        narrowInstanceOf(narrowInstanceOf(series.args.this, UnnestExpr)?.args.expressions?.[0], Expression)?.setArgKey('end', end);
      }
    }
    return expression;
  };
}

/**
 * Transforms percentiles by adding a WITHIN GROUP clause to them.
 */
export function addWithinGroupForPercentiles (expression: Expression): Expression {
  if (
    PERCENTILES.some((cls) => expression instanceof cls)
    && !(expression.parent instanceof WithinGroupExpr)
    && expression.args.expression
  ) {
    const percentileExpr = expression as InstanceType<(typeof PERCENTILES)[number]>;
    const column = percentileExpr.args.this?.pop();
    percentileExpr.setArgKey('this', percentileExpr.args.expression?.pop());
    const order = new OrderExpr({
      expressions: [
        new OrderedExpr({
          this: column,
          nullsFirst: false,
        }),
      ],
    });
    return new WithinGroupExpr({
      this: percentileExpr,
      expression: order,
    });
  }
  return expression;
}

/**
 * Transforms percentiles by getting rid of their corresponding WITHIN GROUP clause.
 */
export function removeWithinGroupForPercentiles (expression: Expression): Expression {
  if (
    expression instanceof WithinGroupExpr
    && PERCENTILES.some((cls) => expression.args.this instanceof cls)
    && expression.args.expression instanceof OrderExpr
  ) {
    const quantile = narrowInstanceOf(expression.args.this, Expression)?.args.this;
    const ordered = expression.find(OrderedExpr);
    const inputValue = ordered ? ordered.args.this : undefined;
    return expression.replace(new ApproxQuantileExpr({
      this: inputValue,
      quantile,
    }));
  }
  return expression;
}

export function addRecursiveCteColumnNames (expression: Expression): Expression {
  /**
   * Uses projection output names in recursive CTE definitions to define the CTEs' columns.
   */
  if (expression instanceof WithExpr && expression.recursive) {
    const nextName = nameSequence('_c_');

    for (const cte of expression.args.expressions ?? []) {
      assertIsInstanceOf(cte, CteExpr);
      const cteAlias = isInstanceOf(cte.args.alias, TableAliasExpr) ? cte.args.alias : undefined;
      if (!cteAlias?.columns.length) {
        let query: Expression | undefined = cte.args.this;
        if (query instanceof SetOperationExpr) {
          query = query.args.this;
        }
        assertIsInstanceOf(query, QueryExpr);

        cteAlias?.setArgKey(
          'columns',
          query.selects.map((s) =>
            toIdentifier(s.aliasOrName || nextName())),
        );
      }
    }
  }

  return expression;
}

/**
 * Replace 'epoch' in casts by the equivalent date literal.
 */
export function epochCastToTs (expression: Expression): Expression {
  if (
    (expression instanceof CastExpr || expression instanceof TryCastExpr)
    && expression.name.toLowerCase() === 'epoch'
    && DataTypeExpr.TEMPORAL_TYPES.has(
      enumFromString(
        DataTypeExprKind,
        (expression.args.to instanceof Expression
          ? expression.args.to.args.this?.toString()
          : expression.args.to?.toString()) ?? '',
      ) ?? '',
    )
  ) {
    narrowInstanceOf(expression.args.this, Expression)?.replace(LiteralExpr.string('1970-01-01 00:00:00'));
  }

  return expression;
}

/**
 * Convert SEMI and ANTI joins into equivalent forms that use EXIST instead.
 */
export function eliminateSemiAndAntiJoins (expression: Expression): Expression {
  if (expression instanceof SelectExpr) {
    const joins = expression.args.joins || [];
    for (const join of [...joins]) {
      assertIsInstanceOf(join, JoinExpr);
      const on = join.args.on;
      if (on && (join.kind === JoinExprKind.SEMI || join.kind === JoinExprKind.ANTI)) {
        const subquery = select('1').from(join.args.this)
          .where(on);
        let exists: Expression = new ExistsExpr({ this: subquery });

        if (join.kind === JoinExprKind.ANTI) {
          exists = (exists as ExistsExpr).not();
        }

        join.pop();
        expression.where(exists);
      }
    }
  }

  return expression;
}

/**
 * Converts a query with a FULL OUTER join to a union of identical queries that
 * use LEFT/RIGHT OUTER joins instead.
 */
export function eliminateFullOuterJoin (expression: Expression): Expression {
  if (expression instanceof SelectExpr) {
    const joins = expression.args.joins || [];
    const joinExprs = filterInstanceOf(joins, JoinExpr);
    const fullOuterJoins = joinExprs
      .map((join, index) => ({
        index,
        join,
      }))
      .filter(({ join }) => join.side === JoinExprKind.FULL);

    if (fullOuterJoins.length === 1) {
      const expressionCopy = expression.copy();
      expression.setArgKey('limit', undefined);

      const {
        index, join: fullOuterJoin,
      } = fullOuterJoins[0];
      const fromName = expression.args.from?.aliasOrName;
      const joinName = fullOuterJoin.aliasOrName;

      const joinConditions = fullOuterJoin.args.on || new AndExpr({
        expressions: (fullOuterJoin.args.using || []).map((col: Expression) =>
          new EqExpr({
            this: new ColumnExpr({
              this: col,
              table: fromName,
            }),
            expression: new ColumnExpr({
              this: col,
              table: joinName,
            }),
          })),
      });

      fullOuterJoin.setArgKey('side', JoinExprKind.LEFT);

      const antiJoinClause = select('1')
        .from(expression.args.from)
        .where(joinConditions);

      const rightSideJoin = expressionCopy.args.joins?.[index];
      rightSideJoin?.setArgKey('side', JoinExprKind.RIGHT);

      const antiJoinCheck = new ExistsExpr({ this: antiJoinClause }).not();
      expressionCopy.where(antiJoinCheck);

      expressionCopy.setArgKey('with', undefined); // remove CTEs from RIGHT side
      expression.setArgKey('order', undefined); // remove order by from LEFT side

      return union([expression, expressionCopy], { distinct: false }) ?? expression;
    }
  }

  return expression;
}

/**
 * Converts numeric values used in conditions into explicit boolean expressions.
 */
export function ensureBools (expression: Expression): Expression {
  const _ensureBool = (node: Expression): void => {
    if (
      node.isNumber
      || (!(node instanceof SubqueryPredicateExpr)
        && node.isType([DataTypeExprKind.UNKNOWN, ...DataTypeExpr.NUMERIC_TYPES]))
      || (node instanceof ColumnExpr && !node.type)
    ) {
      node.replace(node.neq(0));
    }
  };

  for (const node of expression.walk()) {
    canonicalizeEnsureBools(node, _ensureBool);
  }

  return expression;
}

export function unqualifyColumns (expression: Expression): Expression {
  for (const column of expression.findAll(ColumnExpr)) {
    // Remove table, db, catalog parts, keeping only the column name
    const parts = column.parts;
    for (let i = 0; i < parts.length - 1; i++) {
      parts[i].pop();
    }
  }

  return expression;
}

export function removeUniqueConstraints (expression: Expression): Expression {
  if (!(expression instanceof CreateExpr)) return expression;

  for (const constraint of expression.findAll(UniqueColumnConstraintExpr)) {
    if (constraint.parent) {
      constraint.parent.pop();
    }
  }

  return expression;
}

export function ctasWithTmpTablesToCreateTmpView (
  expression: Expression,
  tmpStorageProvider: (e: Expression) => Expression = (e) => e,
): Expression {
  if (!(expression instanceof CreateExpr)) return expression;

  const properties = expression.args.properties;
  const temporary = properties?.args.expressions?.some(
    (prop) => prop instanceof TemporaryPropertyExpr,
  );

  // CTAS with temp tables map to CREATE TEMPORARY VIEW
  if (expression.kind === CreateExprKind.TABLE && temporary) {
    if (expression.args.expression) {
      return new CreateExpr({
        kind: CreateExprKind.TEMPORARY_VIEW,
        this: expression.args.this,
        expression: expression.args.expression,
      });
    }
    return tmpStorageProvider(expression);
  }

  return expression;
}

/**
 * In Hive, moves columns from the schema to PARTITIONED BY if they match.
 */
export function moveSchemaColumnsToPartitionedBy (expression: Expression): Expression {
  if (!(expression instanceof CreateExpr)) return expression;

  const hasSchema = expression.args.this instanceof SchemaExpr;
  const isPartitionable = expression.kind === CreateExprKind.TABLE || expression.kind === CreateExprKind.VIEW;

  if (hasSchema && isPartitionable) {
    const prop = expression.find(PartitionedByPropertyExpr);
    if (prop && prop.args.this && !(prop.args.this instanceof SchemaExpr)) {
      const schema = expression.args.this;
      assertIsInstanceOf(schema, SchemaExpr);
      const columns = new Set(prop.args.this.args.expressions?.map((v) => v instanceof Expression ? v.name.toUpperCase() : v.toString()));

      const partitions = schema.args.expressions?.filter((col) =>
        columns.has(col.name.toUpperCase()));

      schema.setArgKey(
        'expressions',
        schema.args.expressions?.filter((e) => !partitions?.includes(e)),
      );

      prop.replace(new PartitionedByPropertyExpr({
        this: new SchemaExpr({ expressions: filterInstanceOf(partitions ?? [], Expression) }),
      }));

      expression.setArgKey('this', schema);
    }
  }

  return expression;
}

/**
 * Spark 3: Move PARTITIONED BY columns back into the schema columns.
 */
export function movePartitionedByToSchemaColumns (expression: Expression): Expression {
  if (!(expression instanceof CreateExpr)) return expression;

  const prop = expression.find(PartitionedByPropertyExpr);
  if (
    prop
    && prop.args.this instanceof SchemaExpr
    && prop.args.this.args.expressions?.every((e) => e instanceof ColumnDefExpr && e.kind)
  ) {
    const propThis = new TupleExpr({
      expressions: prop.args.this.args.expressions.map((e) => {
        const t = e.args.this;
        if (t instanceof Expression) {
          return toIdentifier(isInstanceOf(t, IdentifierExpr) ? t : t.name);
        }
        return toIdentifier(t?.toString() ?? '');
      }),
    });

    const schema = expression.args.this as SchemaExpr;
    for (const e of prop.args.this.args.expressions) {
      schema.append('expressions', e);
    }
    prop.setArgKey('this', propThis);
  }

  return expression;
}

/**
 * Converts Oracle (+) join marks into explicit LEFT JOIN syntax.
 */
export function eliminateJoinMarks (expression: Expression): Expression {
  for (const scope of [...traverseScope(expression)].reverse()) {
    const query = scope.expression;
    if (!(query instanceof SelectExpr)) continue;

    const where = query.args.where;
    const joins = query.args.joins || [];

    if (!where || ![...where.findAll(ColumnExpr)].some((c) => c.args.joinMark)) {
      continue;
    }

    if (scope.isCorrelatedSubquery) {
      throw new Error('Correlated queries with join marks are not supported');
    }

    assertIsInstanceOf(where.args.this, Expression);
    const normalizedWhere = normalize(where.args.this);
    const joinsOns: Record<string, Expression[]> = {};

    const conditions = normalizedWhere instanceof AndExpr ? normalizedWhere.flatten() : [normalizedWhere];

    for (const cond of conditions) {
      const joinCols = [...cond.findAll(ColumnExpr)].filter((col) => col.args.joinMark);
      const leftJoinTable = new Set(joinCols.map((col) => col.table));

      if (leftJoinTable.size === 0) continue;
      if (1 < leftJoinTable.size) {
        throw new Error('Cannot combine JOIN predicates from different tables');
      }

      for (const col of joinCols) {
        col.setArgKey('joinMark', false);
      }

      const tableName = Array.from(leftJoinTable)[0]!;
      if (!joinsOns[tableName]) joinsOns[tableName] = [];
      joinsOns[tableName].push(cond);
    }

    const oldJoins: Record<string, JoinExpr> = {};
    for (const j of joins) {
      assertIsInstanceOf(j, JoinExpr);
      oldJoins[j.aliasOrName] = j;
    }

    const newJoins: Record<string, JoinExpr> = {};
    const queryFrom = query.args.from;

    for (const [table, predicates] of Object.entries(joinsOns)) {
      const joinTarget = (oldJoins[table] || queryFrom).args.this?.copy();
      if (joinTarget?.aliasOrName) {
        newJoins[joinTarget.aliasOrName] = new JoinExpr({
          this: joinTarget,
          on: new AndExpr({ expressions: predicates }),
          kind: JoinExprKind.LEFT,
        });
      }

      for (const p of predicates) {
        const current = p;
        while (current.parent instanceof ParenExpr) {
          current.parent.replace(current);
        }
        const parent = current.parent;
        current.pop();
        if (parent instanceof BinaryExpr) {
          parent.replace(parent.left === undefined ? parent.args.expression : parent.args.this);
        } else if (parent instanceof WhereExpr) {
          parent.pop();
        }
      }
    }

    if (queryFrom?.aliasOrName && queryFrom.aliasOrName in newJoins) {
      const onlyOldKeys = Object.keys(oldJoins).filter((k) => !newJoins[k]);
      if (onlyOldKeys.length === 0) throw new Error('Cannot determine new FROM clause');

      const newFromName = onlyOldKeys[0];
      query.setArgKey('from', new FromExpr({ this: oldJoins[newFromName].args.this }));
    }

    if (0 < Object.keys(newJoins).length) {
      for (const [n, j] of Object.entries(oldJoins)) {
        if (!newJoins[n] && n !== query.args.from?.name) {
          if (!j.kind) j.setArgKey('kind', JoinExprKind.CROSS);
          newJoins[n] = j;
        }
      }
      query.setArgKey('joins', Object.values(newJoins));
    }
  }

  return expression;
}

/**
 * Eliminates the WINDOW query clause by inlining each named window.
 */
export function eliminateWindowClause (expression: Expression): Expression {
  if (expression instanceof SelectExpr && expression.args.windows) {
    const windows = expression.args.windows;
    expression.setArgKey('windows', undefined);

    const windowMap: Record<string, Expression> = {};

    const inlineInheritedWindow = (window: Expression): void => {
      const alias = window.alias?.toLowerCase();
      if (!alias) return;
      const inherited = windowMap[alias];
      if (!inherited) return;

      window.setArgKey('alias', undefined);
      for (const key of [
        'partitionBy',
        'order',
        'spec',
      ]) {
        const arg = inherited.getArgKey(key);
        if (arg instanceof Expression) window.setArgKey(key, arg.copy());
      }
    };

    for (const window of windows) {
      inlineInheritedWindow(window);
      windowMap[window.name.toLowerCase()] = window;
    }

    for (const window of findAllInScope(expression, [WindowExpr])) {
      inlineInheritedWindow(window);
    }
  }

  return expression;
}

/**
 * Inherit field names from the first struct in an array for BigQuery.
 */
export function inheritStructFieldNames (expression: Expression): Expression {
  if (
    expression instanceof ArrayExpr
    && expression.args.structNameInheritance
    && expression.args.expressions?.[0] instanceof StructExpr
  ) {
    const firstItem = expression.args.expressions[0] as StructExpr;
    if (firstItem.args.expressions?.every((f) => f instanceof PropertyEqExpr)) {
      const fieldNames = firstItem.args.expressions.map((f) => f.args.this);

      for (const struct of expression.args.expressions.slice(1)) {
        if (!(struct instanceof StructExpr) || struct.args.expressions?.length !== fieldNames.length) {
          continue;
        }

        const newExpressions = struct.args.expressions.map((expr, i) => {
          if (!(expr instanceof PropertyEqExpr)) {
            const thisFieldNameRaw = fieldNames[i];
            const thisFieldName = thisFieldNameRaw instanceof Expression ? thisFieldNameRaw.args.this : thisFieldNameRaw;
            const propEq = new PropertyEqExpr({
              this: new IdentifierExpr({
                this: thisFieldName instanceof Expression ? thisFieldName.copy() : thisFieldName?.toString() || '',
                quoted: false,
              }),
              expression: expr,
            });
            propEq.type = expr.type;
            return propEq;
          }
          return expr;
        });

        struct.setArgKey('expressions', newExpressions);
      }
    }
  }

  return expression;
}
