<template>
  <MainLayout :breadcrumb="navBreadcrumb">
    <div
      v-if="!data"
      class="gui-neutral-fg-muted text-md flex flex-1 items-center justify-center overflow-hidden"
    >
      Loading API data...
    </div>

    <div
      v-else
      class="flex flex-1 overflow-hidden max-sm:flex-col max-sm:overflow-visible"
    >
      <button
        type="button"
        class="
          gap-xs px-md py-sm
          gui-neutral-fg gui-neutral-bg-subtle gui-neutral-border-subtle hover:gui-neutral-bg-hover
          hidden w-full cursor-pointer items-center border-b border-none text-sm font-medium max-sm:flex
        "
        @click="toggleSidebar"
      >
        <GIcon
          :name="sidebarOpen ? GIconName.ChevronUp : GIconName.ChevronDown"
          :size="12"
        />
        {{ sidebarOpen ? 'Hide API list' : 'Show API list' }}
      </button>

      <aside
        class="
          gui-neutral-border-subtle gui-neutral-bg-subtle max-sm:gui-neutral-border-subtle
          w-[260px] shrink-0 overflow-y-auto border-r max-sm:hidden max-sm:max-h-[50vh] max-sm:w-full max-sm:border-r-0 max-sm:border-b
        "
        :class="{
          'max-sm:block': sidebarOpen,
        }"
      >
        <div class="py-sm">
          <div class="px-sm mb-sm">
            <GTextInput
              v-model="query"
              class="gui-neutral-border w-full border"
              placeholder="Search..."
              autocomplete="off"
            />
          </div>
          <div
            v-for="group in navGroups"
            :key="group.label"
            class="mb-sm"
          >
            <div class="px-sm py-xs gui-neutral-fg-muted text-sm font-semibold tracking-wider uppercase">
              {{ group.label }}
            </div>
            <ul class="m-0 list-none p-0">
              <li
                v-for="item in group.items"
                :key="item.id"
              >
                <button
                  type="button"
                  class="gap-xs px-sm ml-2 flex w-full cursor-pointer items-center border-none bg-transparent py-1 text-left font-mono text-sm"
                  :class="selected?.id === item.id ? 'gui-primary-bg-hover gui-primary-fg' : 'gui-neutral-fg hover:gui-neutral-bg-hover'"
                  @click="() => select(item)"
                >
                  <span
                    class="size-[6px] shrink-0 rounded-full"
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
        class="min-w-0 flex-1 overflow-y-auto"
      >
        <div
          v-if="selected"
          class="p-lg max-w-[800px]"
        >
          <div class="gap-sm mb-xs flex items-center">
            <span
              class="px-xs inline-block rounded-sm py-[2px] text-sm font-semibold lowercase"
              :style="kindChipStyle(kindSlug(selected.kind))"
            >{{ kindLabel(selected.kind) }}</span>
            <h1 class="gui-neutral-fg font-mono text-xl font-bold">
              {{ selected.name }}
            </h1>
          </div>

          <div
            v-if="sourceUrl(selected)"
            class="mb-sm gui-neutral-fg-muted text-sm"
          >
            <a
              :href="sourceUrl(selected)!"
              class="gui-info-fg no-underline hover:underline"
              target="_blank"
              rel="noopener noreferrer"
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
              <div class="p-sm gui-neutral-bg-subtle gui-neutral-border-subtle gui-neutral-fg overflow-x-auto rounded-sm border font-mono text-sm wrap-break-word whitespace-pre-wrap">
                <span class="gui-info-fg">function</span>
                <span class="font-semibold"> {{ selected.name }}</span>(<span
                  v-for="(p, i) in (sig.parameters ?? [])"
                  :key="p.id"
                ><span class="gui-warning-fg">{{ p.name }}</span><span
                  v-if="p.flags?.isOptional"
                  class="gui-info-fg"
                >?</span>: <span class="gui-success-fg">{{ typeString(p.type) }}</span><span
                  v-if="i < (sig.parameters?.length ?? 0) - 1"
                >, </span></span>): <span class="gui-success-fg">{{ typeString(sig.type) }}</span>
              </div>
              <div
                v-if="comment(sig)"
                class="prose gui-neutral-fg-muted mt-xs text-sm/3"
                v-html="comment(sig)"
              />
              <table
                v-if="sig.parameters?.length"
                class="mt-sm w-full border-collapse text-sm"
              >
                <thead>
                  <tr>
                    <th class="gui-neutral-fg-muted px-sm py-xs gui-neutral-border border-b text-left font-semibold">
                      Parameter
                    </th>
                    <th class="gui-neutral-fg-muted px-sm py-xs gui-neutral-border border-b text-left font-semibold">
                      Type
                    </th>
                    <th class="gui-neutral-fg-muted px-sm py-xs gui-neutral-border border-b text-left font-semibold">
                      Description
                    </th>
                  </tr>
                </thead>
                <tbody>
                  <tr
                    v-for="p in sig.parameters"
                    :key="p.id"
                  >
                    <td class="px-sm py-xs gui-neutral-border-subtle border-b align-top">
                      <code class="gui-warning-fg font-mono">{{ p.name }}{{ p.flags?.isOptional ? '?' : '' }}</code>
                    </td>
                    <td class="px-sm py-xs gui-neutral-border-subtle border-b align-top">
                      <code class="gui-success-fg font-mono text-sm">{{ typeString(p.type) }}</code>
                    </td>
                    <td
                      class="prose gui-neutral-fg-muted px-sm py-xs gui-neutral-border-subtle pdesc border-b align-top leading-3"
                      v-html="comment(p)"
                    />
                  </tr>
                </tbody>
              </table>
            </div>
          </template>

          <template v-if="selected.children?.length && selected.kind !== ReflectionKind.Enum">
            <div class="mt-lg">
              <h2 class="text-md gui-neutral-fg mb-sm pb-xs gui-neutral-border-subtle border-b font-bold">
                Members
              </h2>
              <div
                v-for="member in visibleMembers(selected)"
                :key="member.id"
                class="py-sm gui-neutral-border-subtle border-b last:border-b-0"
              >
                <div class="gap-xs flex items-center">
                  <span
                    class="size-[6px] shrink-0 rounded-full"
                    :style="kindDotStyle(kindSlug(member.kind))"
                  />
                  <code class="text-md gui-neutral-fg font-mono font-semibold">{{ member.name }}</code>
                  <span
                    v-if="member.flags?.isStatic"
                    class="gui-neutral-bg-hover gui-neutral-fg-muted rounded-sm px-1 py-px text-xs"
                  >static</span>
                  <span
                    v-if="member.flags?.isOptional"
                    class="gui-neutral-bg-hover gui-neutral-fg-muted rounded-sm px-1 py-px text-xs"
                  >optional</span>
                  <span
                    v-if="member.flags?.isReadonly"
                    class="gui-neutral-bg-hover gui-neutral-fg-muted rounded-sm px-1 py-px text-xs"
                  >readonly</span>
                  <a
                    v-if="sourceUrl(member)"
                    :href="sourceUrl(member)!"
                    target="_blank"
                    rel="noopener noreferrer"
                    class="gui-neutral-fg-muted ml-auto text-sm no-underline hover:underline"
                  >{{ sourceLine(member) }}</a>
                </div>
                <div
                  v-if="memberType(member)"
                  class="mt-[2px]"
                >
                  <code class="gui-success-fg font-mono text-sm">{{ memberType(member) }}</code>
                </div>
                <div
                  v-if="comment(member)"
                  class="prose gui-neutral-fg-muted mt-[2px] text-sm/3"
                  v-html="comment(member)"
                />
              </div>
            </div>
          </template>

          <template v-if="selected.kind === ReflectionKind.Enum && selected.children?.length">
            <div class="mt-lg">
              <h2 class="text-md gui-neutral-fg mb-sm pb-xs gui-neutral-border-subtle border-b font-bold">
                Members
              </h2>
              <table class="mt-sm w-full border-collapse text-sm">
                <thead>
                  <tr>
                    <th class="gui-neutral-fg-muted px-sm py-xs gui-neutral-border border-b text-left font-semibold">
                      Name
                    </th>
                    <th class="gui-neutral-fg-muted px-sm py-xs gui-neutral-border border-b text-left font-semibold">
                      Value
                    </th>
                  </tr>
                </thead>
                <tbody>
                  <tr
                    v-for="m in selected.children"
                    :key="m.id"
                  >
                    <td class="px-sm py-xs gui-neutral-border-subtle border-b align-top">
                      <code class="gui-warning-fg font-mono">{{ m.name }}</code>
                    </td>
                    <td class="px-sm py-xs gui-neutral-border-subtle border-b align-top">
                      <code class="gui-success-fg font-mono text-sm">{{ m.type?.value === undefined ? '' : JSON.stringify(m.type.value) }}</code>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </template>
        </div>

        <div
          v-else
          class="p-lg min-w-0"
        >
          <div class="mb-lg">
            <h1 class="gui-neutral-fg mb-5 font-mono text-2xl font-bold">
              @hdnax/sqlingo.js
              <span
                v-if="data?.packageVersion"
                class="text-md gui-neutral-fg-muted font-normal"
              >v{{ data.packageVersion }}</span>
            </h1>
            <div class="gap-sm mt-sm mb-6 flex">
              <a
                href="https://www.npmjs.com/package/@hdnax/sqlingo.js"
                target="_blank"
                rel="noopener noreferrer"
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
                rel="noopener noreferrer"
              >SQLGlot</a>, which is a comprehensive SQL parser, transpiler, optimizer, and engine.
            </p>
            <p>
              This package allows you to parse, transpile, optimize, and execute SQL across <strong>33+ dialects</strong> in JavaScript, with no other setup.
            </p>
            <p>Supports TypeScript &amp; CJS/ESM. Works in Node.js and the browser.</p>

            <ul class="links-list gap-md flex list-none p-0">
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
import {
  GTextInput, GIcon, GIconName,
  GCodeBlock, GCodeLanguage, GHighlightTheme,
} from '@hdnax/genuix';
import {
  useSeoMeta,
} from '@unhead/vue';
import {
  kindDotStyle, kindChipStyle,
} from './kinds';
import {
  ReflectionKind,
} from '@/types/typedoc';
import MainLayout from '@/layout/main/MainLayout.vue';

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

interface TypeInfo {
  type: string;
  name?: string;
  value?: unknown;
  elementType?: TypeInfo;
  typeArguments?: TypeInfo[];
  types?: TypeInfo[];
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

function toggleSidebar () {
  sidebarOpen.value = !sidebarOpen.value;
}

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

import('virtual:typedoc').then((module_) => {
  data.value = module_.default as ReflectionNode | null;
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
  const search = query.value.toLowerCase();

  return GROUP_ORDER.flatMap(([
    label,
    kinds,
  ]) => {
    const items = topLevel.value.filter(
      (n) => kinds.includes(n.kind) && (!search || n.name.toLowerCase().includes(search)),
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

function comment (node: ReflectionNode): string {
  const text = (node.comment?.summary ?? []).map((part) => part.text).join('')
    .trim();

  return marked(text) as string;
}

function kindLabel (kind: number): string {
  return Object.entries(ReflectionKind).find(([
    , value,
  ]) => value === kind)?.[0] ?? '?';
}

function kindSlug (kind: number): string {
  return kindLabel(kind).toLowerCase();
}

function memberType (node: ReflectionNode): string {
  if (node.type) return typeString(node.type);
  if (node.signatures?.[0]?.type) return typeString(node.signatures[0].type);

  return '';
}

function select (item: ReflectionNode) {
  selected.value = item;
  mainElement.value?.scrollTo(0, 0);
  window.location.hash = itemSlug(item);
}

function sourceFile (node: ReflectionNode): string {
  const source = node.sources?.[0];

  return source ? `${source.fileName}:${source.line}` : '';
}

function sourceLine (node: ReflectionNode): string {
  const source = node.sources?.[0];

  return source ? `L${source.line}` : '';
}

function sourceUrl (node: ReflectionNode): string | null {
  return node.sources?.[0]?.url ?? null;
}

function typeString (typeInfo: TypeInfo | undefined): string {
  if (!typeInfo) return '';
  switch (typeInfo.type) {
  case 'intrinsic':
  case 'reference':
    return typeInfo.typeArguments?.length
      ? `${typeInfo.name}<${typeInfo.typeArguments.map(typeString).join(', ')}>`
      : (typeInfo.name ?? '?');
  case 'literal': return JSON.stringify(typeInfo.value);
  case 'array': return `${typeString(typeInfo.elementType)}[]`;
  case 'union': return (typeInfo.types ?? []).map(typeString).join(' | ');
  case 'intersection': return (typeInfo.types ?? []).map(typeString).join(' & ');
  default: return typeInfo.name ?? typeInfo.type ?? '?';
  }
}

const HIDE_KINDS = new Set<number>([ReflectionKind.Constructor]);

function visibleMembers (node: ReflectionNode): ReflectionNode[] {
  return (node.children ?? []).filter(
    (member) => !member.flags.isPrivate && !member.flags.isProtected && !HIDE_KINDS.has(member.kind),
  );
}
</script>

<!-- eslint-disable vue/enforce-style-attribute -->
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
<!-- eslint-enable vue/enforce-style-attribute -->
