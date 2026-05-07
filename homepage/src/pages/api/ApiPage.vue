<template>
  <MainLayout :breadcrumb="navBreadcrumb">
    <div
      v-if="!data"
      class="flex flex-1 overflow-hidden items-center justify-center gui-neutral-fg-muted text-md"
    >
      Loading API data...
    </div>

    <div
      v-else
      class="flex flex-1 overflow-hidden max-sm:flex-col max-sm:overflow-visible"
    >
      <button
        class="
          hidden max-sm:flex items-center
          gap-xs w-full px-md py-sm
          text-sm font-medium gui-neutral-fg gui-neutral-bg-subtle border-none border-b gui-neutral-border-subtle cursor-pointer hover:gui-neutral-bg-hover
        "
        @click="sidebarOpen = !sidebarOpen"
      >
        <GIcon
          :name="sidebarOpen ? GIconName.ChevronUp : GIconName.ChevronDown"
          :size="12"
        />
        {{ sidebarOpen ? 'Hide API list' : 'Show API list' }}
      </button>

      <aside
        class="
          w-[260px] shrink-0 border-r
          gui-neutral-border-subtle gui-neutral-bg-subtle overflow-y-auto max-sm:hidden max-sm:w-full max-sm:max-h-[50vh] max-sm:border-r-0 max-sm:border-b max-sm:gui-neutral-border-subtle
        "
        :class="{ 'max-sm:block': sidebarOpen }"
      >
        <div class="py-sm">
          <div class="px-sm mb-sm">
            <GTextInput
              v-model="query"
              class="w-full border gui-neutral-border"
              placeholder="Search..."
              autocomplete="off"
            />
          </div>
          <div
            v-for="group in navGroups"
            :key="group.label"
            class="mb-sm"
          >
            <div class="px-sm py-xs text-sm font-semibold uppercase tracking-wider gui-neutral-fg-muted">
              {{ group.label }}
            </div>
            <ul class="list-none m-0 p-0">
              <li
                v-for="item in group.items"
                :key="item.id"
              >
                <button
                  class="flex items-center gap-xs w-full ml-2 px-sm py-1 text-sm font-mono bg-transparent border-none cursor-pointer text-left"
                  :class="selected?.id === item.id ? 'gui-primary-bg-hover gui-primary-fg' : 'gui-neutral-fg hover:gui-neutral-bg-hover'"
                  @click="select(item)"
                >
                  <span
                    class="w-[6px] h-[6px] rounded-full shrink-0"
                    :style="kindDotStyle(kindSlug(item.kind))"
                  />
                  <span class="pl-1">{{ item.name }}</span>
                </button>
              </li>
            </ul>
          </div>
        </div>
      </aside>

      <main
        ref="mainElement"
        class="flex-1 min-w-0 overflow-y-auto"
      >
        <div
          v-if="selected"
          class="p-lg max-w-[800px]"
        >
          <div class="flex items-center gap-sm mb-xs">
            <span
              class="inline-block px-xs py-[2px] text-sm font-semibold rounded-sm lowercase"
              :style="kindChipStyle(kindSlug(selected.kind))"
            >{{ kindLabel(selected.kind) }}</span>
            <h1 class="text-xl font-mono font-bold gui-neutral-fg">
              {{ selected.name }}
            </h1>
          </div>

          <div
            v-if="sourceUrl(selected)"
            class="mb-sm text-sm gui-neutral-fg-muted"
          >
            <a
              :href="sourceUrl(selected)!"
              class="gui-info-fg no-underline hover:underline"
              target="_blank"
              rel="noopener"
            >{{ sourceFile(selected) }}</a>
          </div>

          <div
            v-if="comment(selected)"
            class="prose text-md gui-neutral-fg mb-md leading-3"
            v-html="comment(selected)"
          />

          <template v-if="selected.kind === ReflectionKind.Function">
            <div
              v-for="sig in (selected.signatures ?? [])"
              :key="sig.id"
              class="mt-lg"
            >
              <div class="font-mono text-sm p-sm gui-neutral-bg-subtle border gui-neutral-border-subtle rounded-sm overflow-x-auto whitespace-pre-wrap break-words gui-neutral-fg">
                <span class="gui-info-fg">function</span>
                <span class="font-semibold"> {{ selected.name }}</span>(<span
                  v-for="(p, i) in (sig.parameters ?? [])"
                  :key="p.id"
                ><span class="gui-warning-fg">{{ p.name }}</span><span
                  v-if="p.flags?.isOptional"
                  class="gui-info-fg"
                >?</span>: <span class="gui-success-fg">{{ typeStr(p.type) }}</span><span
                  v-if="i < (sig.parameters?.length ?? 0) - 1"
                >, </span></span>): <span class="gui-success-fg">{{ typeStr(sig.type) }}</span>
              </div>
              <div
                v-if="comment(sig)"
                class="prose text-sm gui-neutral-fg-muted mt-xs leading-3"
                v-html="comment(sig)"
              />
              <table
                v-if="sig.parameters?.length"
                class="w-full border-collapse text-sm mt-sm"
              >
                <thead>
                  <tr>
                    <th class="text-left font-semibold gui-neutral-fg-muted px-sm py-xs border-b gui-neutral-border">
                      Parameter
                    </th>
                    <th class="text-left font-semibold gui-neutral-fg-muted px-sm py-xs border-b gui-neutral-border">
                      Type
                    </th>
                    <th class="text-left font-semibold gui-neutral-fg-muted px-sm py-xs border-b gui-neutral-border">
                      Description
                    </th>
                  </tr>
                </thead>
                <tbody>
                  <tr
                    v-for="p in sig.parameters"
                    :key="p.id"
                  >
                    <td class="px-sm py-xs border-b gui-neutral-border-subtle align-top">
                      <code class="font-mono gui-warning-fg">{{ p.name }}{{ p.flags?.isOptional ? '?' : '' }}</code>
                    </td>
                    <td class="px-sm py-xs border-b gui-neutral-border-subtle align-top">
                      <code class="font-mono text-sm gui-success-fg">{{ typeStr(p.type) }}</code>
                    </td>
                    <td
                      class="prose gui-neutral-fg-muted leading-3 px-sm py-xs border-b gui-neutral-border-subtle align-top pdesc"
                      v-html="comment(p)"
                    />
                  </tr>
                </tbody>
              </table>
            </div>
          </template>

          <template v-if="selected.children?.length && selected.kind !== ReflectionKind.Enum">
            <div class="mt-lg">
              <h2 class="text-md font-bold gui-neutral-fg mb-sm pb-xs border-b gui-neutral-border-subtle">
                Members
              </h2>
              <div
                v-for="member in visibleMembers(selected)"
                :key="member.id"
                class="py-sm border-b gui-neutral-border-subtle last:border-b-0"
              >
                <div class="flex items-center gap-xs">
                  <span
                    class="w-[6px] h-[6px] rounded-full shrink-0"
                    :style="kindDotStyle(kindSlug(member.kind))"
                  />
                  <code class="font-mono text-md font-semibold gui-neutral-fg">{{ member.name }}</code>
                  <span
                    v-if="member.flags?.isStatic"
                    class="text-xs px-1 py-[1px] rounded-sm gui-neutral-bg-hover gui-neutral-fg-muted"
                  >static</span>
                  <span
                    v-if="member.flags?.isOptional"
                    class="text-xs px-1 py-[1px] rounded-sm gui-neutral-bg-hover gui-neutral-fg-muted"
                  >optional</span>
                  <span
                    v-if="member.flags?.isReadonly"
                    class="text-xs px-1 py-[1px] rounded-sm gui-neutral-bg-hover gui-neutral-fg-muted"
                  >readonly</span>
                  <a
                    v-if="sourceUrl(member)"
                    :href="sourceUrl(member)!"
                    target="_blank"
                    rel="noopener"
                    class="ml-auto text-sm gui-neutral-fg-muted no-underline hover:underline"
                  >{{ sourceLine(member) }}</a>
                </div>
                <div
                  v-if="memberType(member)"
                  class="mt-[2px]"
                >
                  <code class="font-mono text-sm gui-success-fg">{{ memberType(member) }}</code>
                </div>
                <div
                  v-if="comment(member)"
                  class="prose text-sm gui-neutral-fg-muted mt-[2px] leading-3"
                  v-html="comment(member)"
                />
              </div>
            </div>
          </template>

          <template v-if="selected.kind === ReflectionKind.Enum && selected.children?.length">
            <div class="mt-lg">
              <h2 class="text-md font-bold gui-neutral-fg mb-sm pb-xs border-b gui-neutral-border-subtle">
                Members
              </h2>
              <table class="w-full border-collapse text-sm mt-sm">
                <thead>
                  <tr>
                    <th class="text-left font-semibold gui-neutral-fg-muted px-sm py-xs border-b gui-neutral-border">
                      Name
                    </th>
                    <th class="text-left font-semibold gui-neutral-fg-muted px-sm py-xs border-b gui-neutral-border">
                      Value
                    </th>
                  </tr>
                </thead>
                <tbody>
                  <tr
                    v-for="m in selected.children"
                    :key="m.id"
                  >
                    <td class="px-sm py-xs border-b gui-neutral-border-subtle align-top">
                      <code class="font-mono gui-warning-fg">{{ m.name }}</code>
                    </td>
                    <td class="px-sm py-xs border-b gui-neutral-border-subtle align-top">
                      <code class="font-mono text-sm gui-success-fg">{{ m.type?.value !== undefined ? JSON.stringify(m.type.value) : '' }}</code>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </template>
        </div>

        <div
          v-else
          class="p-lg max-w-[800px] min-w-0"
        >
          <div class="mb-lg">
            <h1 class="text-2xl mb-5 font-mono font-bold gui-neutral-fg">
              @hdnax/sqlingo.js
              <span
                v-if="data?.packageVersion"
                class="text-md font-normal gui-neutral-fg-muted"
              >v{{ data.packageVersion }}</span>
            </h1>
            <div class="mb-6 flex gap-sm mt-sm">
              <a
                href="https://www.npmjs.com/package/@hdnax/sqlingo.js"
                target="_blank"
                rel="noopener"
              >
                <img
                  src="https://img.shields.io/npm/v/@hdnax/sqlingo.js"
                  alt="npm version"
                  class="h-5"
                >
              </a>
              <img
                src="https://img.shields.io/badge/license-MIT-green"
                alt="License: MIT"
                class="h-5"
              >
              <img
                src="https://img.shields.io/badge/SQLGlot-v28.10.0-blue"
                alt="SQLGlot"
                class="h-5"
              >
            </div>
          </div>

          <div class="prose text-md gui-neutral-fg leading-3">
            <p>
              A JavaScript/TypeScript port of <a
                href="https://github.com/tobymao/sqlglot"
                target="_blank"
                rel="noopener"
              >SQLGlot</a>, which is a comprehensive SQL parser, transpiler, optimizer, and engine.
            </p>
            <p>
              This package allows you to parse, transpile, optimize, and execute SQL across <strong>33+ dialects</strong> in JavaScript, with no other setup.
            </p>
            <p>Supports TypeScript &amp; CJS/ESM. Works in Node.js and the browser.</p>

            <ul class="links-list flex gap-md list-none p-0">
              <li><a href="https://github.com/huydo862003/sqlingo.js">GitHub</a></li>
              <li><a href="https://github.com/huydo862003/sqlingo.js/issues">Issues</a></li>
              <li><a href="https://github.com/huydo862003/sqlingo.js/blob/master/CHANGELOG.md">Changelog</a></li>
            </ul>

            <h2>Features</h2>
            <ul class="list-disc">
              <li>33+ SQL dialects: Postgres, MySQL, BigQuery, Snowflake, DuckDB, ClickHouse, Redshift, Athena, Spark, and many more</li>
              <li>Full SQLGlot feature set: parsing, transpilation, optimization, column lineage, SQL diffing, and execution</li>
              <li>Pure JavaScript: no need for WASM or native dependencies</li>
              <li>TypeScript-first: full type definitions included</li>
            </ul>

            <h2>Installation</h2>
            <GCodeBlock
              id="install"
              :code="CODE_INSTALL"
              :language="GCodeLanguage.Bash"
              :highlight-theme="GHighlightTheme.AtomOne"
              :show-header="false"
              class="mb-3"
            />
            <p>Peer dependency: <a href="https://www.npmjs.com/package/luxon">luxon</a> (^3.7.2) is required for date/time operations.</p>

            <h2>Quick Start</h2>
            <p>This example demonstrates transpiling a query from Spark to Postgres and then optimizing it.</p>
            <GCodeBlock
              id="quickstart"
              :language="GCodeLanguage.Typescript"
              :highlight-theme="GHighlightTheme.AtomOne"
              :code="CODE_QUICKSTART"
            />

            <h2>Core Usage</h2>
            <h3>Parsing</h3>
            <p>Parse SQL strings into expression trees (AST).</p>
            <GCodeBlock
              id="parsing"
              :language="GCodeLanguage.Typescript"
              :highlight-theme="GHighlightTheme.AtomOne"
              :code="CODE_PARSING"
            />

            <h3>Transpiling</h3>
            <p>Convert SQL between different dialects.</p>
            <GCodeBlock
              id="transpiling"
              :language="GCodeLanguage.Typescript"
              :highlight-theme="GHighlightTheme.AtomOne"
              :code="CODE_TRANSPILING"
            />

            <h3>Tokenizing</h3>
            <p>Extract tokens from a SQL string for lower-level analysis.</p>
            <GCodeBlock
              id="tokenizing"
              :language="GCodeLanguage.Typescript"
              :highlight-theme="GHighlightTheme.AtomOne"
              :code="CODE_TOKENIZING"
            />

            <h2>SQL Builder</h2>
            <p>Build queries programmatically using a fluent API.</p>
            <GCodeBlock
              id="builder"
              :language="GCodeLanguage.Typescript"
              :highlight-theme="GHighlightTheme.AtomOne"
              :code="CODE_BUILDER"
            />

            <h2>Optimization &amp; Analysis</h2>
            <h3>Optimization</h3>
            <p>Simplify and normalize queries based on schema information.</p>
            <GCodeBlock
              id="optimization"
              :language="GCodeLanguage.Typescript"
              :highlight-theme="GHighlightTheme.AtomOne"
              :code="CODE_OPTIMIZATION"
            />

            <h3>Column Lineage</h3>
            <p>Trace the origin of columns through subqueries and joins.</p>
            <GCodeBlock
              id="lineage"
              :language="GCodeLanguage.Typescript"
              :highlight-theme="GHighlightTheme.AtomOne"
              :code="CODE_LINEAGE"
            />

            <h2>Registering a Custom Dialect</h2>
            <p>You can extend the library by registering custom dialects or overriding existing behavior.</p>
            <GCodeBlock
              id="custom-dialect"
              :language="GCodeLanguage.Typescript"
              :highlight-theme="GHighlightTheme.AtomOne"
              :code="CODE_CUSTOM_DIALECT"
            />

            <h2>Supported Dialects</h2>
            <p class="gui-neutral-fg-muted">
              Athena, BigQuery, ClickHouse,
              Databricks, Doris, Dremio, Drill,
              Druid, DuckDB, Dune, Exasol, Fabric, Hive,
              Materialize, MySQL, Oracle, Postgres, Presto, PRQL, Redshift, RisingWave, SingleStore, Snowflake, Solr, Spark, Spark2, SQLite, StarRocks, Tableau, Teradata, Trino, TSQL
            </p>

            <h2>SQLGlot Compatibility</h2>
            <p>This package tracks <a href="https://github.com/tobymao/sqlglot">SQLGlot</a> v28.10.0 (commit <code>264e95f</code>). The API surface mirrors SQLGlot's Python API, adapted to TypeScript conventions. See <a href="https://github.com/huydo862003/sqlingo.js/blob/master/CONVENTION.md">CONVENTION.md</a> for details.</p>

            <h2>License</h2>
            <p>MIT. See <a href="https://github.com/huydo862003/sqlingo.js/blob/master/LICENSE">LICENSE</a>.</p>
            <p>Based on <a href="https://github.com/tobymao/sqlglot">SQLGlot</a> by Toby Mao (MIT). See <a href="https://github.com/huydo862003/sqlingo.js/blob/master/COPYRIGHT_NOTICE">COPYRIGHT_NOTICE</a>.</p>
          </div>
        </div>
      </main>
    </div>
  </MainLayout>
</template>

<script setup lang="ts">
import {
  ref, computed, useTemplateRef,
} from 'vue';
import {
  marked,
} from 'marked';
import MainLayout from '@/layout/main/MainLayout.vue';
import {
  GTextInput, GIcon, GIconName,
  GCodeBlock, GCodeLanguage, GHighlightTheme,
} from '@hdnax/genuix';
import {
  ReflectionKind,
} from '@/types/typedoc';
import {
  useSeoMeta,
} from '@unhead/vue';
import {
  kindDotStyle, kindChipStyle,
} from './kinds';

interface TypeInfo {
  type: string;
  name?: string;
  value?: unknown;
  elementType?: TypeInfo;
  typeArguments?: TypeInfo[];
  types?: TypeInfo[];
}

interface ReflectionFlags {
  isStatic?: boolean;
  isOptional?: boolean;
  isPrivate?: boolean;
  isProtected?: boolean;
  isReadonly?: boolean;
}

interface ReflectionNode {
  id: number;
  name: string;
  kind: number;
  packageVersion?: string;
  flags: ReflectionFlags;
  comment?: {
    summary: Array<{
      kind: string;
      text: string;
    }>;
  };
  children?: ReflectionNode[];
  signatures?: ReflectionNode[];
  parameters?: ReflectionNode[];
  sources?: Array<{
    fileName: string;
    line: number;
    url?: string;
  }>;
  type?: TypeInfo;
}

const base = import.meta.env.BASE_URL;

const data = ref<ReflectionNode | null>(null);
const mainElement = useTemplateRef<HTMLElement>('mainEl');

const navBreadcrumb = computed(() => {
  const crumbs: Array<{
    label: string;
    href?: string;
  }> = [
    {
      label: 'API reference',
      href: `${base}api-reference/`,
    },
  ];
  if (selected.value) crumbs.push({
    label: selected.value.name,
  });
  return crumbs;
});

const selected = ref<ReflectionNode | null>(null);
const query = ref('');
const sidebarOpen = ref(false);

useSeoMeta({
  title: () => selected.value ? `${selected.value.name} | API Reference | sqlingo.js` : 'API Reference: JavaScript SQL Parser Documentation | sqlingo.js',
  ogTitle: () => selected.value ? `${selected.value.name} | API Reference | sqlingo.js` : 'API Reference: JavaScript SQL Parser Documentation | sqlingo.js',
  description: () => selected.value
    ? `Documentation for ${selected.value.name} in sqlingo.js, the SQLGlot port for JavaScript/TypeScript.`
    : 'Full API documentation for sqlingo.js, including SQL parsing, transpiling, and optimization classes and functions.',
  ogDescription: () => selected.value
    ? `Documentation for ${selected.value.name} in sqlingo.js, the SQLGlot port for JavaScript/TypeScript.`
    : 'Full API documentation for sqlingo.js, including SQL parsing, transpiling, and optimization classes and functions.',
});

const CODE_INSTALL = 'npm install @hdnax/sqlingo.js';

const CODE_QUICKSTART = `import { transpile, parseOne, optimize, MappingSchema } from "@hdnax/sqlingo.js";
// Note: You must explicitly import the dialect to register it
import "@hdnax/sqlingo.js/postgres";
import "@hdnax/sqlingo.js/spark";

// Transpile between dialects
const [pgSql] = transpile("SELECT APPROX_COUNT_DISTINCT(x) FROM table", {
  read: "spark",
  write: "postgres",
});
console.log(pgSql);
// Output: SELECT COUNT(DISTINCT x) FROM "table"

// Optimize an expression
const sql = "SELECT a, b FROM t WHERE a + 1 = 2";
const schema = new MappingSchema({ t: { a: "int", b: "int" } });

const optimized = optimize(parseOne(sql), { schema });
console.log(optimized.sql());
// Output: SELECT t.a AS a, t.b AS b FROM t AS t WHERE t.a = 1`;

const CODE_PARSING = `import { parse, parseOne } from "@hdnax/sqlingo.js";

// Parse multiple statements
const expressions = parse("SELECT 1; SELECT 2");

// Parse a single statement
const expr = parseOne("SELECT a, b FROM t WHERE a > 1");`;

const CODE_TRANSPILING = `import { transpile, Dialects } from "@hdnax/sqlingo.js";
import "@hdnax/sqlingo.js/duckdb";
import "@hdnax/sqlingo.js/hive";

const [result] = transpile("SELECT EPOCH_MS(1618088028295)", {
  read: Dialects.Duckdb,
  write: Dialects.Hive,
});
// Output: "SELECT FROM_UNIXTIME(1618088028295 / POW(10, 3))"`;

const CODE_TOKENIZING = `import { tokenize, Dialects } from "@hdnax/sqlingo.js";
import "@hdnax/sqlingo.js/postgres";

const tokens = tokenize("SELECT 1", { dialect: Dialects.Postgres });`;

const CODE_BUILDER = `import { select, column, condition, Dialects } from "@hdnax/sqlingo.js";
import "@hdnax/sqlingo.js/mysql";

const query = select("a", "b")
  .from("t")
  .where(condition("a > 1"))
  .limit(10);

console.log(query.sql({ dialect: Dialects.Mysql }));
// Output: SELECT a, b FROM t WHERE a > 1 LIMIT 10`;

const CODE_OPTIMIZATION = `import { optimize, MappingSchema } from "@hdnax/sqlingo.js";

const schema = new MappingSchema({
  // define your schema
});

const optimized = optimize(parseOne("SELECT * FROM t"), { schema });`;

const CODE_LINEAGE = `import { lineage } from "@hdnax/sqlingo.js";

const node = lineage("b", "SELECT a AS b FROM (SELECT x AS a FROM y)");
console.log(node.source.name);
// Output: "y"`;

const CODE_CUSTOM_DIALECT = `import { Dialect, Generator, transpile } from "@hdnax/sqlingo.js";

class MyDialect extends Dialect {
  static DIALECT_NAME = "my_dialect";

  static Generator = class extends Generator {
    // Override how specific expressions are generated
  };
}

// Register for use in transpile/parse
Dialect.register("my_dialect", MyDialect);

const [result] = transpile("SELECT 1", { write: "my_dialect" });`;

function itemSlug (item: ReflectionNode): string {
  return `${kindLabel(item.kind).toLowerCase()}-${item.name}`;
}

function selectBySlug (slug: string): boolean {
  const item = topLevel.value.find((n) => itemSlug(n) === slug);
  if (item) {
    selected.value = item;
    mainElement.value?.scrollTo(0, 0);
    return true;
  }
  return false;
}

import('virtual:typedoc').then((mod) => {
  data.value = mod.default as ReflectionNode | null;
  // after data loads, check URL hash
  const hash = window.location.hash.slice(1);
  if (hash) selectBySlug(hash);
});

const topLevel = computed<ReflectionNode[]>(() => data.value?.children ?? []);

const GROUP_ORDER: Array<[string, number[]]> = [
  [
    'Classes',
    [ReflectionKind.Class],
  ],
  [
    'Functions',
    [ReflectionKind.Function],
  ],
  [
    'Interfaces',
    [ReflectionKind.Interface],
  ],
  [
    'Enumerations',
    [ReflectionKind.Enum],
  ],
  [
    'Type Aliases',
    [ReflectionKind.TypeAlias],
  ],
  [
    'Variables',
    [ReflectionKind.Variable],
  ],
];

const navGroups = computed(() => {
  const q = query.value.toLowerCase();
  return GROUP_ORDER.flatMap(([
    label,
    kinds,
  ]) => {
    const items = topLevel.value.filter(
      (n) => kinds.includes(n.kind) && (!q || n.name.toLowerCase().includes(q)),
    );
    return items.length
      ? [
        {
          label,
          items,
        },
      ]
      : [];
  });
});

function select (item: ReflectionNode) {
  selected.value = item;
  mainElement.value?.scrollTo(0, 0);
  window.location.hash = itemSlug(item);
}

function kindLabel (kind: number): string {
  return Object.entries(ReflectionKind).find(([
    , v,
  ]) => v === kind)?.[0] ?? '?';
}

function kindSlug (kind: number): string {
  return kindLabel(kind).toLowerCase();
}

function comment (node: ReflectionNode): string {
  const text = (node.comment?.summary ?? []).map((p) => p.text).join('')
    .trim();
  return marked(text) as string;
}

function sourceUrl (node: ReflectionNode): string | null {
  return node.sources?.[0]?.url ?? null;
}

function sourceFile (node: ReflectionNode): string {
  const s = node.sources?.[0];
  return s ? `${s.fileName}:${s.line}` : '';
}

function sourceLine (node: ReflectionNode): string {
  const s = node.sources?.[0];
  return s ? `L${s.line}` : '';
}

function typeStr (t: TypeInfo | undefined): string {
  if (!t) return '';
  switch (t.type) {
    case 'intrinsic':
    case 'reference':
      return t.typeArguments?.length
        ? `${t.name}<${t.typeArguments.map(typeStr).join(', ')}>`
        : (t.name ?? '?');
    case 'literal': return JSON.stringify(t.value);
    case 'array': return `${typeStr(t.elementType)}[]`;
    case 'union': return (t.types ?? []).map(typeStr).join(' | ');
    case 'intersection': return (t.types ?? []).map(typeStr).join(' & ');
    default: return t.name ?? t.type ?? '?';
  }
}

function memberType (node: ReflectionNode): string {
  if (node.type) return typeStr(node.type);
  if (node.signatures?.[0]?.type) return typeStr(node.signatures[0].type);
  return '';
}

const HIDE_KINDS = new Set<number>([ReflectionKind.Constructor]);

function visibleMembers (node: ReflectionNode): ReflectionNode[] {
  return (node.children ?? []).filter(
    (m) => !m.flags.isPrivate && !m.flags.isProtected && !HIDE_KINDS.has(m.kind),
  );
}
</script>

<style>
.prose h2 {
  font-size: var(--text-lg);
  font-weight: 700;
  margin-top: var(--spacing-lg);
  margin-bottom: var(--spacing-sm);
  padding-bottom: var(--spacing-xs);
  border-bottom: 1px solid var(--gui-neutral-border-subtle);
}
.prose h3 {
  font-size: var(--text-md);
  font-weight: 700;
  margin-top: var(--spacing-md);
  margin-bottom: var(--spacing-xs);
}
.prose p { margin-bottom: var(--spacing-sm); }
.prose ul {
  margin: var(--spacing-sm) 0;
  padding-left: var(--spacing-lg);
}
.prose li { margin-bottom: var(--spacing-xs); }
.prose a { color: var(--gui-info-fg); text-decoration: none; }
.prose a:hover { text-decoration: underline; }
.prose code {
  font-family: var(--font-mono);
  font-size: 0.9em;
  padding: 2px var(--spacing-xs);
  border-radius: var(--radius-sm);
  background: var(--gui-neutral-bg-hover);
  border: 1px solid var(--gui-neutral-border-subtle);
}

.pdesc p {
  margin-bottom: 0;
}
</style>
