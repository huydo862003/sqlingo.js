<template>
  <MainLayout>
    <main class="mx-auto max-w-[1080px] px-7">
      <!-- Hero -->
      <div class="grid items-start gap-16 py-20 lg:grid-cols-[1fr_380px]">
        <div>
          <div
            class="gui-primary-fg text-2xs mb-5 font-mono tracking-widest uppercase"
          >
            SQL parser / transpiler / optimizer
          </div>
          <h1 class="text-5xl/tight font-bold tracking-tight text-balance">
            A TypeScript port of <span class="gui-primary-fg">SQLGlot</span>.
          </h1>
          <p
            class="gui-neutral-fg-muted mt-5 max-w-[33em] leading-relaxed text-pretty"
          >
            Parse, transpile, and optimize SQL across 33+ dialects, in the
            browser or in Node.js.
          </p>

          <div class="mt-8 flex flex-wrap items-center gap-3">
            <div
              class="gui-primary-border-subtle flex min-w-0 flex-1 items-center gap-3 rounded-[10px] border bg-white px-4 py-3 shadow-xs"
            >
              <span class="min-w-0 truncate overflow-hidden font-mono text-sm">
                <span class="gui-primary-fg-muted">$</span> npm install
                @hdnax/sqlingo.js
              </span>
              <div class="flex-1" />
              <button
                type="button"
                class="gui-primary-border-subtle gui-primary-bg-subtle gui-neutral-fg-muted text-2xs shrink-0 cursor-pointer rounded-md border px-3 py-1 font-mono font-medium"
                @click="copyInstall"
              >
                {{ installCopied ? "copied" : "copy" }}
              </button>
            </div>
            <a
              href="/sqlingo.js/playground/"
              class="gui-primary-solid gui-primary-fg flex min-h-12 items-center justify-center rounded-[10px] px-5 py-3.5 text-sm font-semibold no-underline"
            >
              Try the playground
            </a>
          </div>
        </div>

        <!-- Stats card -->
        <div
          class="gui-primary-border-subtle hidden rounded-xl border bg-white px-5 py-1 lg:block"
        >
          <div
            v-for="(stat, i) in stats"
            :key="stat.label"
            class="gui-primary-border-subtle flex items-baseline justify-between py-4"
            :class="i < stats.length - 1 ? 'border-b' : ''"
          >
            <span class="gui-neutral-fg-muted text-sm">{{ stat.label }}</span>
            <span class="font-mono text-sm font-semibold">{{
              stat.value
            }}</span>
          </div>
        </div>
      </div>

      <!-- Code demo -->
      <GTab
        class="gui-primary-border-subtle mb-20 overflow-hidden rounded-[14px] border bg-white shadow-xs lg:mt-14"
      >
        <GTabPanel
          v-for="(demo, name) in demos"
          :key="name"
          :name="name.toUpperCase()"
        >
          <div
            class="gui-primary-fg text-2xs px-5 pt-4 pb-2 font-mono font-semibold tracking-widest uppercase"
          >
            Example
          </div>
          <GCodeBlock
            :id="`demo-${name}`"
            :code="demo.code"
            :language="GCodeLanguage.Typescript"
            :highlight-theme="GHighlightTheme.AtomOne"
            show-line-numbers
            hide-header
            class="border-none bg-white py-2"
          />
          <div class="border-t border-(--color-primary-3)">
            <div
              class="gui-primary-bg-subtle gui-primary-fg text-2xs px-5 pt-4 pb-2 font-mono font-semibold tracking-widest uppercase"
            >
              Returns
            </div>
            <GCodeBlock
              :id="`demo-result-${name}`"
              :code="demo.result"
              :language="GCodeLanguage.Typescript"
              :highlight-theme="GHighlightTheme.AtomOne"
              show-line-numbers
              hide-header
              class="border-none bg-white py-2"
            />
            <p
              class="gui-neutral-fg-muted gui-primary-bg-subtle mt-4 px-5 pb-4 text-sm/relaxed"
            >
              {{ demo.note }}
            </p>
          </div>
        </GTabPanel>
      </GTab>

      <!-- Dialects -->
      <div class="mt-20 flex flex-wrap items-baseline gap-4">
        <h2 class="text-xl font-semibold tracking-tight">
          Supported dialects
        </h2>
        <span class="gui-primary-fg-muted font-mono text-xs">{{ filteredDialects.length }} of 33</span>
        <div class="flex-1" />
        <input
          v-model="dialectQuery"
          placeholder="Filter dialects..."
          class="gui-primary-border-subtle gui-neutral-fg w-56 rounded-[9px] border bg-white px-3.5 py-2.5 font-mono text-xs outline-none"
        >
      </div>
      <div
        class="mt-5 grid grid-cols-[repeat(auto-fill,minmax(150px,1fr))] gap-2"
      >
        <div
          v-for="d in filteredDialects"
          :key="d"
          class="gui-primary-border-subtle gui-neutral-fg cursor-default rounded-lg border bg-white px-3 py-2.5 font-mono text-xs"
        >
          {{ d }}
        </div>
      </div>

      <!-- Cards -->
      <div class="mt-20 grid gap-4 lg:grid-cols-2">
        <a
          href="./api-reference/"
          class="gui-primary-border-subtle gui-neutral-fg block rounded-[14px] border bg-white p-7 no-underline"
        >
          <div
            class="gui-primary-fg text-2xs font-mono tracking-widest uppercase"
          >
            API reference
          </div>
          <div class="mt-3 text-lg font-semibold tracking-tight">
            Every class and function, typed
          </div>
          <div class="gui-neutral-fg-muted mt-2 text-sm/relaxed">
            Full TypeScript types generated from source, with examples for each
            expression node.
          </div>
          <div class="gui-primary-fg mt-4 text-sm font-semibold">
            Browse the docs
          </div>
        </a>
        <a
          href="/sqlingo.js/playground/"
          class="gui-primary-border-subtle gui-neutral-fg block rounded-[14px] border bg-white p-7 no-underline"
        >
          <div
            class="gui-primary-fg text-2xs font-mono tracking-widest uppercase"
          >
            Playground
          </div>
          <div class="mt-3 text-lg font-semibold tracking-tight">
            Transpile SQL in the browser
          </div>
          <div class="gui-neutral-fg-muted mt-2 text-sm/relaxed">
            Paste a query, pick two dialects, and watch it convert. Also does
            SQL to DBML.
          </div>
          <div class="gui-primary-fg mt-4 text-sm font-semibold">
            Open the playground
          </div>
        </a>
      </div>

      <!-- Why this exists -->
      <div
        class="gui-primary-border-subtle mt-24 grid gap-14 border-t pt-14 lg:grid-cols-[220px_1fr]"
      >
        <div>
          <div
            class="gui-primary-fg-muted text-2xs sticky top-24 font-mono tracking-widest uppercase"
          >
            Why this exists
          </div>
        </div>
        <div class="gui-neutral-fg max-w-[34em]">
          <p class="m-0 leading-loose">
            I maintain
            <a
              href="https://github.com/holistics/dbml"
              target="_blank"
              rel="noopener noreferrer"
              class="gui-primary-fg"
            >@dbml/core</a>
            at work, a library that converts between DBML and SQL. Under the
            hood it uses ANTLR, and honestly it has been a mess.
          </p>

          <div
            class="gui-primary-border-subtle my-8 grid overflow-hidden rounded-xl border lg:grid-cols-2"
          >
            <div
              class="gui-primary-border-subtle border-b bg-white p-6 lg:border-r lg:border-b-0"
            >
              <div
                class="gui-primary-fg-muted text-2xs font-mono tracking-widest uppercase"
              >
                @dbml/core, ANTLR
              </div>
              <div
                class="gui-danger-fg mt-2.5 font-mono text-2xl font-semibold"
              >
                33 MB / 5 dialects
              </div>
              <div class="gui-neutral-fg-muted mt-2 text-sm/relaxed">
                Bundle broke CI with OOM errors. Errors read
                <em>"No viable alternative at..."</em>.
              </div>
            </div>
            <div class="bg-white p-6">
              <div
                class="gui-primary-fg-muted text-2xs font-mono tracking-widest uppercase"
              >
                sqlingo.js
              </div>
              <div
                class="gui-success-fg mt-2.5 font-mono text-2xl font-semibold"
              >
                33+ dialects
              </div>
              <div class="gui-neutral-fg-muted mt-2 text-sm/relaxed">
                A hand port of SQLGlot's parser. Pure TypeScript, no grammar
                runtime.
              </div>
            </div>
          </div>

          <p class="mb-5 leading-loose">
            At a hackathon I stumbled on
            <a
              href="https://github.com/tobymao/sqlglot"
              target="_blank"
              rel="noopener noreferrer"
              class="gui-primary-fg"
            >SQLGlot</a>. It was amazing that a library like this existed. Too bad it was
            in Python. I tried Pyodide as a hack, but the runtime is too heavy
            to ship anywhere that matters.
          </p>
          <p class="mb-5 leading-loose">
            So I started porting it to JavaScript. Two weeks in,
            <a
              href="https://github.com/tobilg/polyglot"
              target="_blank"
              rel="noopener noreferrer"
              class="gui-primary-fg"
            >polyglot</a>
            was announced (LoL, if only it were sooner). I kept going anyway: I
            wanted full control over the implementation and a way to stay in
            sync with upstream.
          </p>
          <p class="m-0 leading-loose">
            sqlingo.js is a close mirror of SQLGlot, file for file. That's the
            whole trick: catching up with upstream is a diff, not a rewrite.
          </p>
        </div>
      </div>

      <div class="h-24" />

      <component
        :is="scriptTag"
        type="application/ld+json"
      >
        {{ JSON.stringify({
          "@context": "https://schema.org",
          "@type": "SoftwareApplication",
          "name": "sqlingo.js",
          "description": "sqlingo.js is a JavaScript/TypeScript port of SQLGlot, a SQL parser, transpiler, and optimizer supporting 33+ dialects.",
          "applicationCategory": "DeveloperApplication",
          "operatingSystem": "All",
          "license": "https://opensource.org/licenses/MIT",
          "softwareVersion": SQLINGO_VERSION,
          "url": "https://huydo862003.github.io/sqlingo.js/",
          "author": {
            "@type": "Person",
            "name": "Huy Do",
          },
        }) }}
      </component>
    </main>
  </MainLayout>
</template>

<script setup lang="ts">
import {
  ref, computed,
} from 'vue';
import {
  GCodeBlock,
  GCodeLanguage,
  GHighlightTheme,
  GTab,
  GTabPanel,
} from '@hdnax/genuix';
import {
  useSeoMeta,
} from '@unhead/vue';
import MainLayout from '@/layout/main/MainLayout.vue';
import {
  SQLINGO_VERSION, SQLGLOT_VERSION,
} from '@/constants';

const scriptTag = 'script';

useSeoMeta({
  title: 'Home | sqlingo.js',
  ogTitle: 'Home | sqlingo.js',
  description:
    'sqlingo.js is the JavaScript/TypeScript port of SQLGlot. It is a SQL parser, transpiler, and optimizer supporting 33+ dialects including BigQuery, Snowflake, and Postgres.',
  ogDescription:
    'sqlingo.js is the JavaScript/TypeScript port of SQLGlot. It is a SQL parser, transpiler, and optimizer supporting 33+ dialects including BigQuery, Snowflake, and Postgres.',
});

const installCopied = ref(false);

function copyInstall () {
  navigator.clipboard?.writeText('npm install @hdnax/sqlingo.js');
  installCopied.value = true;
  setTimeout(() => {
    installCopied.value = false;
  }, 1400);
}

const stats = [
  {
    label: 'Dialects',
    value: '33+',
  },
  {
    label: 'Tracks SQLGlot',
    value: `v${SQLGLOT_VERSION}`,
  },
  {
    label: 'Runtime',
    value: 'Browser + Node',
  },
  {
    label: 'License',
    value: 'MIT',
  },
];

const demos: Record<
  string,
  {
    code: string;
    result: string;
    note: string;
  }
> = {
  parse: {
    code: `import { parse } from "@hdnax/sqlingo.js";

const [ast] = parse(
  "SELECT a, b FROM t WHERE a > 1",
  { read: "mysql" },
);`,
    result: `Select(
  expressions=[Column(a), Column(b)],
  from=From(this=Table(t)),
  where=Where(this=GT(...)),
)`,
    note: 'A full expression tree, mirroring SQLGlot\'s node classes one for one.',
  },
  transpile: {
    code: `import { transpile } from "@hdnax/sqlingo.js";

const [sql] = transpile(
  "SELECT DATE_SUB(d, INTERVAL 1 DAY) FROM t",
  { read: "mysql", write: "postgres" },
);`,
    result: 'SELECT d - INTERVAL \'1 DAY\' FROM t',
    note: 'Dialect quirks (quoting, date math, casts) are rewritten for the target.',
  },
  optimize: {
    code: `import { optimize } from "@hdnax/sqlingo.js";

const sql = optimize(
  "SELECT * FROM t WHERE 1 = 1 AND x > 2",
  { schema: { t: { x: "INT" } } },
);`,
    result: 'SELECT t.x AS x FROM t AS t WHERE t.x > 2',
    note: 'Qualifies columns, expands stars, and folds constant predicates.',
  },
};

const ALL_DIALECTS = [
  'Athena',
  'BigQuery',
  'ClickHouse',
  'Databricks',
  'Doris',
  'Dremio',
  'Drill',
  'Druid',
  'DuckDB',
  'Dune',
  'Exasol',
  'Fabric',
  'Hive',
  'Materialize',
  'MySQL',
  'Oracle',
  'Postgres',
  'Presto',
  'PRQL',
  'Redshift',
  'RisingWave',
  'SingleStore',
  'Snowflake',
  'Solr',
  'Spark',
  'Spark2',
  'SQLite',
  'StarRocks',
  'Tableau',
  'Teradata',
  'Trino',
  'TSQL',
];

const dialectQuery = ref('');
const filteredDialects = computed(() => {
  const query = dialectQuery.value.trim().toLowerCase();

  if (!query) return ALL_DIALECTS;

  return ALL_DIALECTS.filter((dialect) =>
    dialect.toLowerCase().includes(query));
});
</script>
