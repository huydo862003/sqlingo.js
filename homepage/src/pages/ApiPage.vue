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
      <button
        class="sidebar-toggle"
        @click="sidebarOpen = !sidebarOpen"
      >
        <GIcon
          :name="sidebarOpen ? GIconName.ChevronUp : GIconName.ChevronDown"
          :size="12"
        />
        {{ sidebarOpen ? 'Hide API list' : 'Show API list' }}
      </button>

      <aside
        class="sidebar"
        :class="{ 'sidebar-open': sidebarOpen }"
      >
        <div class="sidebar-inner">
          <div class="search-wrap">
            <GTextInput
              v-model="query"
              placeholder="Search..."
              autocomplete="off"
            />
          </div>
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
      </aside>

      <main
        ref="mainEl"
        class="content"
      >
        <div
          v-if="selected"
          class="content-inner"
        >
          <div class="entry-title">
            <span
              class="kind-chip"
              :class="`chip-${kindSlug(selected.kind)}`"
            >{{ kindLabel(selected.kind) }}</span>
            <h1 class="entry-name">
              {{ selected.name }}
            </h1>
          </div>

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

          <div
            v-if="comment(selected)"
            class="entry-desc"
          >
            {{ comment(selected) }}
          </div>

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
            <ul class="list-disc">
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
  ref, computed, useTemplateRef,
} from 'vue';
import NavBar from '../components/NavBar.vue';
import {
  GTextInput, GIcon, GIconName,
} from '@hdnax/genuix';
import {
  ReflectionKind,
} from '../types/reflectionKind';
import {
  useSeo,
} from '../composables/useSeo';

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

useSeo(() => ({
  title: selected.value ? `${selected.value.name} | API Reference` : 'API Reference: JavaScript SQL Parser Documentation',
  description: selected.value
    ? `Documentation for ${selected.value.name} in sqlingo.js, the SQLGlot port for JavaScript/TypeScript.`
    : 'Full API documentation for sqlingo.js, including SQL parsing, transpiling, and optimization classes and functions.',
}));

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

function select (item: ReflectionNode) {
  selected.value = item;
  mainEl.value?.scrollTo(0, 0);
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
@reference '../style.css';

.app {
  display: flex;
  flex-direction: column;
  min-height: 100vh;
}

.loading {
  display: flex;
  align-items: center;
  justify-content: center;
  flex: 1;
  color: var(--gui-neutral-fg-muted);
  font-size: var(--text-md);
}

.layout {
  display: flex;
  flex: 1;
  overflow: hidden;
}

.sidebar {
  width: 260px;
  flex-shrink: 0;
  border-right: 1px solid var(--gui-neutral-border-subtle);
  background: var(--gui-neutral-bg-subtle);
  overflow-y: auto;
  height: calc(100vh - 48px);
  position: sticky;
  top: 48px;
}

.sidebar-inner {
  padding: var(--spacing-sm) 0;
}

.search-wrap {
  padding: 0 var(--spacing-sm);
  margin-bottom: var(--spacing-sm);
}

.nav-group {
  margin-bottom: var(--spacing-sm);
}

.nav-group-label {
  padding: var(--spacing-xs) var(--spacing-sm);
  font-size: var(--text-sm);
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--gui-neutral-fg-muted);
}

.nav-list {
  list-style: none;
  margin: 0;
  padding: 0;
}

.nav-item {
  display: flex;
  align-items: center;
  gap: var(--spacing-xs);
  width: 100%;
  padding: 4px var(--spacing-sm);
  font-size: var(--text-sm);
  font-family: var(--font-mono);
  color: var(--gui-neutral-fg);
  background: transparent;
  border: none;
  cursor: pointer;
  text-align: left;
}

.nav-item:hover {
  background: var(--gui-neutral-bg-hover);
}

.nav-item.active {
  background: var(--gui-primary-bg-hover);
  color: var(--gui-primary-fg);
}

.kind-dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  flex-shrink: 0;
}

.dot-class { background: var(--gui-info-solid); }
.dot-function { background: var(--gui-success-solid); }
.dot-interface { background: var(--gui-warning-solid); }
.dot-enum { background: var(--gui-notice-solid); }
.dot-typealias { background: var(--gui-primary-solid); }
.dot-variable { background: var(--gui-danger-solid); }
.dot-property { background: var(--gui-neutral-solid); }
.dot-method { background: var(--gui-success-solid); }
.dot-constructor { background: var(--gui-neutral-solid); }
.dot-enumvalue { background: var(--gui-notice-solid); }

.content {
  flex: 1;
  min-width: 0;
  overflow-y: auto;
  height: calc(100vh - 48px);
  position: sticky;
  top: 48px;
}

.content-inner {
  padding: var(--spacing-lg);
  max-width: 800px;
}

.entry-title {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  margin-bottom: var(--spacing-xs);
}

.entry-name {
  font-size: var(--text-xl);
  font-family: var(--font-mono);
  font-weight: 700;
  color: var(--gui-neutral-fg);
}

.kind-chip {
  display: inline-block;
  padding: 2px var(--spacing-xs);
  font-size: var(--text-sm);
  font-weight: 600;
  border-radius: var(--radius-sm);
  text-transform: lowercase;
}

.chip-class { background: var(--gui-info-bg-hover); color: var(--gui-info-fg); }
.chip-function { background: var(--gui-success-bg-hover); color: var(--gui-success-fg); }
.chip-interface { background: var(--gui-warning-bg-hover); color: var(--gui-warning-fg); }
.chip-enum { background: var(--gui-notice-bg-hover); color: var(--gui-notice-fg); }
.chip-typealias { background: var(--gui-primary-bg-hover); color: var(--gui-primary-fg); }
.chip-variable { background: var(--gui-danger-bg-hover); color: var(--gui-danger-fg); }

.entry-source {
  margin-bottom: var(--spacing-sm);
  font-size: var(--text-sm);
  color: var(--gui-neutral-fg-muted);
}

.entry-source a {
  color: var(--gui-info-fg);
  text-decoration: none;
}

.entry-source a:hover {
  text-decoration: underline;
}

.entry-desc {
  font-size: var(--text-md);
  color: var(--gui-neutral-fg);
  margin-bottom: var(--spacing-md);
  line-height: var(--leading-3);
}

.section {
  margin-top: var(--spacing-lg);
}

.section-title {
  font-size: var(--text-md);
  font-weight: 700;
  color: var(--gui-neutral-fg);
  margin-bottom: var(--spacing-sm);
  padding-bottom: var(--spacing-xs);
  border-bottom: 1px solid var(--gui-neutral-border-subtle);
}

.sig-code {
  font-family: var(--font-mono);
  font-size: var(--text-sm);
  padding: var(--spacing-sm);
  background: var(--gui-neutral-bg-subtle);
  border: 1px solid var(--gui-neutral-border-subtle);
  border-radius: var(--radius-sm);
  overflow-x: auto;
  white-space: pre-wrap;
  word-break: break-word;
  color: var(--gui-neutral-fg);
}

.kw { color: var(--gui-info-fg); }
.sig-name { font-weight: 600; }
.param-name { color: var(--gui-warning-fg); }
.type-ref { color: var(--gui-success-fg); }

.sig-comment {
  font-size: var(--text-sm);
  color: var(--gui-neutral-fg-muted);
  margin-top: var(--spacing-xs);
  line-height: var(--leading-3);
}

.param-table {
  width: 100%;
  border-collapse: collapse;
  font-size: var(--text-sm);
  margin-top: var(--spacing-sm);
}

.param-table th {
  text-align: left;
  font-weight: 600;
  color: var(--gui-neutral-fg-muted);
  padding: var(--spacing-xs) var(--spacing-sm);
  border-bottom: 1px solid var(--gui-neutral-border);
}

.param-table td {
  padding: var(--spacing-xs) var(--spacing-sm);
  border-bottom: 1px solid var(--gui-neutral-border-subtle);
  vertical-align: top;
}

.pname {
  font-family: var(--font-mono);
  color: var(--gui-warning-fg);
}

.ptype {
  font-family: var(--font-mono);
  font-size: var(--text-sm);
  color: var(--gui-success-fg);
}

.pdesc {
  color: var(--gui-neutral-fg-muted);
  line-height: var(--leading-3);
}

.member {
  padding: var(--spacing-sm) 0;
  border-bottom: 1px solid var(--gui-neutral-border-subtle);
}

.member:last-child {
  border-bottom: none;
}

.member-title {
  display: flex;
  align-items: center;
  gap: var(--spacing-xs);
}

.member-name {
  font-family: var(--font-mono);
  font-size: var(--text-md);
  font-weight: 600;
  color: var(--gui-neutral-fg);
}

.member-src {
  margin-left: auto;
  font-size: var(--text-sm);
  color: var(--gui-neutral-fg-muted);
  text-decoration: none;
}

.member-src:hover {
  text-decoration: underline;
}

.flag {
  font-size: 10px;
  padding: 1px 4px;
  border-radius: var(--radius-sm);
  background: var(--gui-neutral-bg-hover);
  color: var(--gui-neutral-fg-muted);
}

.member-type {
  margin-top: 2px;
}

.member-comment {
  font-size: var(--text-sm);
  color: var(--gui-neutral-fg-muted);
  margin-top: 2px;
  line-height: var(--leading-3);
}

.content-landing {
  padding: var(--spacing-lg);
  max-width: 800px;
  min-width: 0;
}

.landing-header {
  margin-bottom: var(--spacing-lg);
}

.landing-name {
  font-size: var(--text-2xl);
  font-family: var(--font-mono);
  font-weight: 700;
  color: var(--gui-neutral-fg);
}

.landing-version {
  font-size: var(--text-md);
  font-weight: 400;
  color: var(--gui-neutral-fg-muted);
}

.landing-badges {
  display: flex;
  gap: var(--spacing-sm);
  margin-top: var(--spacing-sm);
}

.landing-badges img {
  height: 20px;
}

.prose {
  font-size: var(--text-md);
  color: var(--gui-neutral-fg);
  line-height: var(--leading-3);
}

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

.prose p {
  margin-bottom: var(--spacing-sm);
}

.prose ul {
  margin: var(--spacing-sm) 0;
  padding-left: var(--spacing-lg);
}

.prose li {
  margin-bottom: var(--spacing-xs);
}

.prose a {
  color: var(--gui-info-fg);
  text-decoration: none;
}

.prose a:hover {
  text-decoration: underline;
}

.prose code {
  font-family: var(--font-mono);
  font-size: 0.9em;
  padding: 2px var(--spacing-xs);
  border-radius: var(--radius-sm);
  background: var(--gui-neutral-bg-hover);
  border: 1px solid var(--gui-neutral-border-subtle);
}

.prose pre {
  margin: var(--spacing-sm) 0;
  border-radius: var(--radius-md);
  border: 1px solid var(--gui-neutral-border-subtle);
  overflow-x: auto;
  max-width: 100%;
}

.prose pre code {
  display: block;
  padding: var(--spacing-sm) var(--spacing-md);
  font-size: var(--text-sm);
  line-height: var(--leading-3);
  background: var(--gui-neutral-bg-subtle);
  border: none;
  white-space: pre;
  border-radius: var(--radius-md);
}

.links-list {
  display: flex;
  gap: var(--spacing-md);
  list-style: none;
  padding: 0;
}

.dialects-list {
  color: var(--gui-neutral-fg-muted);
}

.sidebar-toggle {
  display: none;
}

@media (max-width: 768px) {
  .layout {
    flex-direction: column;
    overflow: visible;
  }

  .sidebar-toggle {
    display: flex;
    align-items: center;
    gap: var(--spacing-xs);
    width: 100%;
    padding: var(--spacing-sm) var(--spacing-md);
    font-size: var(--text-sm);
    font-weight: var(--font-weight-medium);
    color: var(--gui-neutral-fg);
    background: var(--gui-neutral-bg-subtle);
    border: none;
    border-bottom: 1px solid var(--gui-neutral-border-subtle);
    cursor: pointer;
  }

  .sidebar-toggle:hover {
    background: var(--gui-neutral-bg-hover);
  }

  .sidebar {
    display: none;
    width: 100%;
    height: auto;
    max-height: 50vh;
    position: static;
    border-right: none;
    border-bottom: 1px solid var(--gui-neutral-border-subtle);
    overflow-y: auto;
  }

  .sidebar.sidebar-open {
    display: block;
  }

  .search-wrap :deep(input) {
    width: 100%;
    border: 1px solid var(--gui-neutral-border);
  }

  .content {
    height: auto;
    position: static;
  }
}
</style>
