<template>
  <div class="app">
    <NavBar :breadcrumb="navBreadcrumb" />

    <div
      v-if="!data"
      class="loading"
    >
      Loading API data...
    </div>

    <div
      v-else
      class="layout"
    >
      <!-- mobile search bar (hidden on desktop) -->
      <div class="mobile-search-bar">
        <div class="mobile-search-wrap">
          <input
            v-model="query"
            class="search"
            type="search"
            placeholder="Search API..."
            autocomplete="off"
            @focus="mobileSearchFocused = true"
            @blur="onMobileSearchBlur"
          >
          <div
            v-if="mobileSearchFocused && mobileResults.length"
            class="mobile-results"
          >
            <button
              v-for="item in mobileResults"
              :key="item.id"
              class="mobile-result-item"
              @mousedown.prevent="selectMobile(item)"
            >
              <span
                class="kind-dot"
                :class="`dot-${kindSlug(item.kind)}`"
              />
              <span class="mobile-result-name">{{ item.name }}</span>
              <span class="mobile-result-kind">{{ kindLabel(item.kind) }}</span>
            </button>
          </div>
        </div>
      </div>

      <!-- sidebar -->
      <aside class="sidebar">
        <div class="sidebar-inner">
          <div class="search-wrap">
            <input
              v-model="query"
              class="search"
              type="search"
              placeholder="Search..."
              autocomplete="off"
            >
          </div>

          <div class="nav-groups">
            <div
              v-for="group in navGroups"
              :key="group.label"
              class="nav-group"
            >
              <div class="nav-group-label">
                {{ group.label }}
              </div>
              <ul class="nav-list">
                <li
                  v-for="item in group.items"
                  :key="item.id"
                >
                  <button
                    class="nav-item"
                    :class="{ active: selected?.id === item.id }"
                    @click="select(item)"
                  >
                    <span
                      class="kind-dot"
                      :class="`dot-${kindSlug(item.kind)}`"
                    />
                    {{ item.name }}
                  </button>
                </li>
              </ul>
            </div>
          </div>
        </div>
      </aside>

      <!-- main content -->
      <main
        ref="mainEl"
        class="content"
      >
        <div
          v-if="selected"
          class="content-inner"
        >
          <!-- title -->
          <div class="entry-title">
            <span
              class="kind-chip"
              :class="`chip-${kindSlug(selected.kind)}`"
            >{{ kindLabel(selected.kind) }}</span>
            <h1 class="entry-name">
              {{ selected.name }}
            </h1>
          </div>

          <!-- source -->
          <div
            v-if="sourceUrl(selected)"
            class="entry-source"
          >
            <a
              :href="sourceUrl(selected)!"
              target="_blank"
              rel="noopener"
            >{{ sourceFile(selected) }}</a>
          </div>

          <!-- description -->
          <div
            v-if="comment(selected)"
            class="entry-desc"
          >
            {{ comment(selected) }}
          </div>

          <!-- function signatures -->
          <template v-if="selected.kind === ReflectionKind.Function">
            <div
              v-for="sig in (selected.signatures ?? [])"
              :key="sig.id"
              class="section"
            >
              <div class="sig-code">
                <span class="kw">function</span>
                <span class="sig-name"> {{ selected.name }}</span>(<span
                  v-for="(p, i) in (sig.parameters ?? [])"
                  :key="p.id"
                ><span class="param-name">{{ p.name }}</span><span
                  v-if="p.flags?.isOptional"
                  class="kw"
                >?</span>: <span class="type-ref">{{ typeStr(p.type) }}</span><span
                  v-if="i < (sig.parameters?.length ?? 0) - 1"
                >, </span></span>): <span class="type-ref">{{ typeStr(sig.type) }}</span>
              </div>
              <p
                v-if="comment(sig)"
                class="sig-comment"
              >
                {{ comment(sig) }}
              </p>
              <table
                v-if="sig.parameters?.length"
                class="param-table"
              >
                <thead>
                  <tr>
                    <th>Parameter</th>
                    <th>Type</th>
                    <th>Description</th>
                  </tr>
                </thead>
                <tbody>
                  <tr
                    v-for="p in sig.parameters"
                    :key="p.id"
                  >
                    <td><code class="pname">{{ p.name }}{{ p.flags?.isOptional ? '?' : '' }}</code></td>
                    <td><code class="ptype">{{ typeStr(p.type) }}</code></td>
                    <td class="pdesc">
                      {{ comment(p) }}
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </template>

          <!-- class / interface members -->
          <template v-if="selected.children?.length && selected.kind !== ReflectionKind.Enum">
            <div class="section">
              <h2 class="section-title">
                Members
              </h2>
              <div
                v-for="member in visibleMembers(selected)"
                :key="member.id"
                class="member"
              >
                <div class="member-title">
                  <span
                    class="kind-dot"
                    :class="`dot-${kindSlug(member.kind)}`"
                  />
                  <code class="member-name">{{ member.name }}</code>
                  <span
                    v-if="member.flags?.isStatic"
                    class="flag"
                  >static</span>
                  <span
                    v-if="member.flags?.isOptional"
                    class="flag"
                  >optional</span>
                  <span
                    v-if="member.flags?.isReadonly"
                    class="flag"
                  >readonly</span>
                  <a
                    v-if="sourceUrl(member)"
                    :href="sourceUrl(member)!"
                    target="_blank"
                    rel="noopener"
                    class="member-src"
                  >{{ sourceLine(member) }}</a>
                </div>
                <div
                  v-if="memberType(member)"
                  class="member-type"
                >
                  <code class="ptype">{{ memberType(member) }}</code>
                </div>
                <p
                  v-if="comment(member)"
                  class="member-comment"
                >
                  {{ comment(member) }}
                </p>
              </div>
            </div>
          </template>

          <!-- enum members -->
          <template v-if="selected.kind === ReflectionKind.Enum && selected.children?.length">
            <div class="section">
              <h2 class="section-title">
                Members
              </h2>
              <table class="param-table">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Value</th>
                  </tr>
                </thead>
                <tbody>
                  <tr
                    v-for="m in selected.children"
                    :key="m.id"
                  >
                    <td><code class="pname">{{ m.name }}</code></td>
                    <td><code class="ptype">{{ m.type?.value !== undefined ? JSON.stringify(m.type.value) : '' }}</code></td>
                  </tr>
                </tbody>
              </table>
            </div>
          </template>
        </div>

        <div
          v-else
          class="content-landing"
        >
          <div class="landing-header">
            <h1 class="landing-name">
              @hdnax/sqlingo.js
              <span
                v-if="data?.packageVersion"
                class="landing-version"
              >v{{ data.packageVersion }}</span>
            </h1>
            <div class="landing-badges">
              <a
                href="https://www.npmjs.com/package/@hdnax/sqlingo.js"
                target="_blank"
                rel="noopener"
              >
                <img
                  src="https://img.shields.io/npm/v/@hdnax/sqlingo.js"
                  alt="npm version"
                >
              </a>
              <img
                src="https://img.shields.io/badge/license-MIT-green"
                alt="License: MIT"
              >
              <img
                src="https://img.shields.io/badge/SQLGlot-v28.10.0-blue"
                alt="SQLGlot"
              >
            </div>
          </div>

          <div class="prose">
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
            <p>Supports TypeScript & CJS/ESM. Works in Node.js and the browser.</p>

            <ul class="links-list">
              <li><a href="https://github.com/huydo862003/sqlingo.js">GitHub</a></li>
              <li><a href="https://github.com/huydo862003/sqlingo.js/issues">Issues</a></li>
              <li><a href="https://github.com/huydo862003/sqlingo.js/blob/master/CHANGELOG.md">Changelog</a></li>
            </ul>

            <h2>Features</h2>
            <ul>
              <li>33+ SQL dialects: Postgres, MySQL, BigQuery, Snowflake, DuckDB, ClickHouse, Redshift, Athena, Spark, and many more</li>
              <li>Full SQLGlot feature set: parsing, transpilation, optimization, column lineage, SQL diffing, and execution</li>
              <li>Pure JavaScript: no need for WASM or native dependencies</li>
              <li>TypeScript-first: full type definitions included</li>
            </ul>

            <h2>Installation</h2>
            <pre><code class="language-bash">npm install @hdnax/sqlingo.js</code></pre>
            <p>Peer dependency: <a href="https://www.npmjs.com/package/luxon">luxon</a> (^3.7.2) is required for date/time operations.</p>

            <h2>Quick Start</h2>
            <p>This example demonstrates transpiling a query from Spark to Postgres and then optimizing it.</p>
            <pre><code class="language-ts">import { transpile, parseOne, optimize, MappingSchema } from "@hdnax/sqlingo.js";
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
// Output: SELECT t.a AS a, t.b AS b FROM t AS t WHERE t.a = 1</code></pre>

            <h2>Core Usage</h2>
            <h3>Parsing</h3>
            <p>Parse SQL strings into expression trees (AST).</p>
            <pre><code class="language-ts">import { parse, parseOne } from "@hdnax/sqlingo.js";

// Parse multiple statements
const expressions = parse("SELECT 1; SELECT 2");

// Parse a single statement
const expr = parseOne("SELECT a, b FROM t WHERE a > 1");</code></pre>

            <h3>Transpiling</h3>
            <p>Convert SQL between different dialects.</p>
            <pre><code class="language-ts">import { transpile, Dialects } from "@hdnax/sqlingo.js";
import "@hdnax/sqlingo.js/duckdb";
import "@hdnax/sqlingo.js/hive";

const [result] = transpile("SELECT EPOCH_MS(1618088028295)", {
  read: Dialects.Duckdb,
  write: Dialects.Hive,
});
// Output: "SELECT FROM_UNIXTIME(1618088028295 / POW(10, 3))"</code></pre>

            <h3>Tokenizing</h3>
            <p>Extract tokens from a SQL string for lower-level analysis.</p>
            <pre><code class="language-ts">import { tokenize, Dialects } from "@hdnax/sqlingo.js";
import "@hdnax/sqlingo.js/postgres";

const tokens = tokenize("SELECT 1", { dialect: Dialects.Postgres });</code></pre>

            <h2>SQL Builder</h2>
            <p>Build queries programmatically using a fluent API.</p>
            <pre><code class="language-ts">import { select, column, condition, Dialects } from "@hdnax/sqlingo.js";
import "@hdnax/sqlingo.js/mysql";

const query = select("a", "b")
  .from("t")
  .where(condition("a > 1"))
  .limit(10);

console.log(query.sql({ dialect: Dialects.Mysql }));
// Output: SELECT a, b FROM t WHERE a > 1 LIMIT 10</code></pre>

            <h2>Optimization &amp; Analysis</h2>
            <h3>Optimization</h3>
            <p>Simplify and normalize queries based on schema information.</p>
            <pre><code class="language-ts">import { optimize, MappingSchema } from "@hdnax/sqlingo.js";

const schema = new MappingSchema({
  // define your schema
});

const optimized = optimize(parseOne("SELECT * FROM t"), { schema });</code></pre>

            <h3>Column Lineage</h3>
            <p>Trace the origin of columns through subqueries and joins.</p>
            <pre><code class="language-ts">import { lineage } from "@hdnax/sqlingo.js";

const node = lineage("b", "SELECT a AS b FROM (SELECT x AS a FROM y)");
console.log(node.source.name);
// Output: "y"</code></pre>

            <h2>Registering a Custom Dialect</h2>
            <p>You can extend the library by registering custom dialects or overriding existing behavior.</p>
            <pre><code class="language-ts">import { Dialect, Generator, transpile } from "@hdnax/sqlingo.js";

class MyDialect extends Dialect {
  static DIALECT_NAME = "my_dialect";

  static Generator = class extends Generator {
    // Override how specific expressions are generated
  };
}

// Register for use in transpile/parse
Dialect.register("my_dialect", MyDialect);

const [result] = transpile("SELECT 1", { write: "my_dialect" });</code></pre>

            <h2>Supported Dialects</h2>
            <p class="dialects-list">
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
  </div>
</template>

<script setup lang="ts">
import {
  ref, computed, watch, useTemplateRef,
} from 'vue';
import NavBar from '../components/NavBar.vue';
import {
  ReflectionKind,
} from '../types/reflectionKind';
import {
  useSeo,
} from '../composables/useSeo';

useSeo(() => ({
  title: selected.value ? `${selected.value.name} | API Reference` : 'API Reference: JavaScript SQL Parser Documentation',
  description: selected.value
    ? `Documentation for ${selected.value.name} in sqlingo.js, the SQLGlot port for JavaScript/TypeScript.`
    : 'Full API documentation for sqlingo.js, including SQL parsing, transpiling, and optimization classes and functions.',
}));

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
const mainEl = useTemplateRef<HTMLElement>('mainEl');

const navBreadcrumb = computed(() => {
  const crumbs: Array<{label: string;
    href?: string;}> = [
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

const query = ref('');
const selected = ref<ReflectionNode | null>(null);

function itemSlug (item: ReflectionNode): string {
  return `${kindLabel(item.kind).toLowerCase()}-${item.name}`;
}

function selectBySlug (slug: string): boolean {
  const item = topLevel.value.find((n) => itemSlug(n) === slug);
  if (item) {
    selected.value = item;
    mainEl.value?.scrollTo(0, 0);
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

watch(navGroups, (groups) => {
  const all = groups.flatMap((g) => g.items);
  // keep selection valid when search narrows results
  if (selected.value && !all.find((i) => i.id === selected.value?.id)) {
    selected.value = null;
  }
}, {
  immediate: true,
});

function select (item: ReflectionNode) {
  selected.value = item;
  mainEl.value?.scrollTo(0, 0);
  window.location.hash = itemSlug(item);
}

const mobileSearchFocused = ref(false);

const mobileResults = computed(() => {
  const q = query.value.toLowerCase();
  if (!q) return topLevel.value.slice(0, 20);
  return topLevel.value.filter((n) => n.name.toLowerCase().includes(q)).slice(0, 20);
});

function selectMobile (item: ReflectionNode) {
  select(item);
  query.value = '';
  mobileSearchFocused.value = false;
}

function onMobileSearchBlur () {
  // small delay so mousedown on result fires first
  setTimeout(() => {
    mobileSearchFocused.value = false;
  }, 100);
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
  return (node.comment?.summary ?? []).map((p) => p.text).join('')
    .trim();
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

<style scoped>
@reference "../style.css";

.app {
  height: 100vh;
  overflow: hidden;
  @apply flex flex-col;
}

.loading {
  @apply flex items-center justify-center h-screen text-sm text-fg-muted;
}

.layout {
  display: grid;
  grid-template-columns: 260px 1fr;
  grid-template-rows: 1fr;
  flex: 1;
  height: calc(100vh - 3rem);
  overflow: hidden;
}

.mobile-search-bar {
  display: none;
}

/* sidebar */
.sidebar {
  @apply border-r border-border overflow-y-auto;
  background: var(--color-bg-subtle);
}

@media (max-width: 640px) {
  .layout {
    grid-template-columns: 1fr;
    grid-template-rows: auto 1fr;
    height: calc(100vh - 3rem);
  }

  .mobile-search-bar {
    display: flex;
    @apply px-3 py-2 border-b border-border;
    background: var(--color-bg-subtle);
  }

  .sidebar {
    display: none;
  }

  .content {
    grid-column: 1;
  }
}

.mobile-search-wrap {
  @apply relative w-full;
}

.mobile-results {
  position: absolute;
  top: calc(100% + 4px);
  left: 0;
  right: 0;
  @apply border border-border rounded-[var(--radius-md)] flex flex-col py-1 z-50;
  background: #1a1a24;
  box-shadow: 0 8px 24px rgba(0,0,0,0.5);
  max-height: 60vh;
  overflow-y: auto;
}

.mobile-result-item {
  @apply flex items-center gap-2 px-3 py-2 text-xs text-left w-full border-0 cursor-pointer;
  background: transparent;
  font-family: var(--font-mono);
  color: #9090a0;
  transition: background 0.1s, color 0.1s;
}
.mobile-result-item:hover {
  background: rgba(255,255,255,0.07);
  color: #e8e8ec;
}

.mobile-result-name {
  @apply text-fg flex-1 truncate;
}

.mobile-result-kind {
  @apply text-[10px] text-fg-faint shrink-0;
}

.sidebar-inner {
  @apply py-3;
}

.search-wrap {
  @apply px-3 pb-3;
}

.search {
  @apply w-full px-3 py-1.5 text-xs rounded-[var(--radius-md)] border border-border bg-bg text-fg;
  outline: none;
}
.search:focus { border-color: var(--color-accent); }

.nav-groups {
  @apply flex flex-col;
}

.nav-group {
  @apply mb-2;
}

.nav-group-label {
  @apply px-4 py-1 text-[10px] font-bold uppercase tracking-widest text-fg-faint;
}

.nav-list {
  @apply list-none m-0 p-0;
}

.nav-item {
  @apply flex items-center gap-2 w-full text-left px-4 py-1 text-xs text-fg-muted cursor-pointer;
  background: transparent;
  border: none;
  font-family: var(--font-mono);
  transition: color 0.1s, background 0.1s;
}
.nav-item:hover { @apply text-fg bg-bg; }
.nav-item.active {
  @apply text-fg font-bold;
  background: color-mix(in srgb, var(--color-accent) 8%, transparent);
  box-shadow: inset 2px 0 0 var(--color-accent);
}

/* kind dots */
.kind-dot {
  @apply w-2 h-2 rounded-full shrink-0;
}
.dot-class      { background: #3b82f6; }
.dot-function   { background: #f59e0b; }
.dot-interface  { background: #10b981; }
.dot-enum       { background: #8b5cf6; }
.dot-typealias  { background: #6366f1; }
.dot-variable   { background: #ef4444; }
.dot-property   { background: #64748b; }
.dot-method     { background: #f59e0b; }
.dot-accessor   { background: #06b6d4; }

/* kind chips */
.kind-chip {
  @apply text-xs font-bold px-2 py-0.5 rounded;
  font-family: var(--font-mono);
}
.chip-class     { background: #1e3a5f; color: #93c5fd; }
.chip-function  { background: #3d2600; color: #fcd34d; }
.chip-interface { background: #063b24; color: #6ee7b7; }
.chip-enum      { background: #2d1b5e; color: #c4b5fd; }
.chip-typealias { background: #1e2060; color: #a5b4fc; }
.chip-variable  { background: #3d0a0a; color: #fca5a5; }

/* content */
.content {
  @apply overflow-y-auto;
}

.content-inner {
  @apply px-10 py-8 max-w-4xl;
}

.content-empty {
  @apply flex items-center justify-center h-full text-sm text-fg-faint;
}

.content-landing {
  @apply px-10 py-12 max-w-4xl;
}

.landing-header {
  @apply mb-10;
}

.landing-name {
  @apply text-3xl font-black mb-2 flex items-baseline gap-3;
}

.landing-version {
  @apply text-base font-normal text-fg-muted;
}

.landing-badges {
  @apply flex gap-2 flex-wrap mt-4;
}

.prose h2 {
  @apply text-base font-bold text-fg-muted mt-10 mb-4 uppercase tracking-widest;
}

.prose h3 {
  @apply text-sm font-bold text-fg mt-6 mb-2;
}

.prose p {
  @apply text-sm text-fg-muted leading-relaxed mb-4;
}

.prose ul {
  @apply list-disc list-inside text-sm text-fg-muted mb-4 pl-2;
}

.prose li {
  @apply mb-1.5;
}

.prose code {
  @apply text-xs bg-bg-subtle px-1.5 py-0.5 rounded border border-border font-mono text-accent;
}

.prose pre {
  @apply bg-bg-subtle border border-border rounded-[var(--radius-md)] p-4 mb-6 overflow-x-auto;
}

.prose pre code {
  @apply p-0 bg-transparent border-0 text-fg text-xs;
}

.links-list {
  @apply list-none! p-0!;
}

.links-list a {
  @apply text-accent hover:underline;
}

.dialects-list {
  @apply text-xs! text-fg-faint! leading-relaxed!;
}

.prose a {
  @apply text-accent hover:underline;
}

/* breadcrumb */
.breadcrumb {
  @apply flex items-center gap-1.5 text-xs text-fg-faint mb-4 font-mono;
}
.breadcrumb-pkg { @apply text-fg-faint; }
.breadcrumb-sep { @apply text-fg-faint; }

/* title */
.entry-title {
  @apply flex items-center gap-3 mb-2;
}

.entry-name {
  @apply text-2xl font-black text-fg font-mono m-0;
}

.entry-source {
  @apply text-sm text-fg-faint font-mono mb-4;
}
.entry-source a { @apply text-fg-faint; }
.entry-source a:hover { @apply text-fg; }

.entry-desc {
  @apply text-base text-fg-muted leading-relaxed mb-6 border-l-2 border-border pl-4;
}

/* sections */
.section {
  @apply mb-8 border-t border-border pt-6;
}

.section-title {
  @apply text-sm font-bold text-fg-muted uppercase tracking-wider mb-4 m-0;
}

/* signature */
.sig-code {
  @apply text-base font-mono bg-bg-subtle border border-border rounded-[var(--radius-md)] px-4 py-3 mb-3 overflow-x-auto;
}
.kw { @apply text-fg-muted; }
.sig-name { @apply text-fg font-bold; }
.param-name { @apply text-fg; }
.type-ref { color: #60a5fa; }

.sig-comment {
  @apply text-base text-fg-muted mb-3;
}

/* param table */
.param-table {
  @apply w-full text-sm border border-border rounded-[var(--radius-md)];
  border-collapse: collapse;
}
.param-table th {
  @apply text-left px-4 py-2 text-fg-faint font-medium border-b border-border bg-bg-subtle;
}
.param-table td {
  @apply px-4 py-2 border-b border-border align-top;
}
.param-table tr:last-child td { @apply border-b-0; }
.pname { @apply font-mono text-fg; }
.ptype { @apply font-mono; color: #60a5fa; }
.pdesc { @apply text-fg-muted; }

/* members */
.member {
  @apply py-4 border-b border-border-muted last:border-b-0;
}

.member-title {
  @apply flex items-center gap-2 flex-wrap mb-1;
}

.member-name {
  @apply text-base font-mono text-fg;
}

.flag {
  @apply text-[10px] text-fg-faint border border-border-muted rounded px-1.5 py-0.5;
}

.member-src {
  @apply text-[10px] text-fg-faint font-mono ml-auto;
}
.member-src:hover { @apply text-fg; }

.member-type {
  @apply mb-1;
}

.member-comment {
  @apply text-sm text-fg-muted m-0 leading-relaxed;
}
</style>
