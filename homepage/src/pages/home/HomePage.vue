<template>
  <MainLayout>
    <main
      id="hero"
      class="gui-neutral-bg"
    >
      <GHero class="p-5">
        <template #icon>
          <GLogo
            src="/sqlingo.js/icon.svg"
            alt="sqlingo.js logo"
            :size="GLogoSize.Lg"
            class="rounded-md bg-white"
          />
        </template>
        <template #title>
          <h1 class="text-3xl font-extrabold">
            sqlingo.js: A Typescript/Javascript port of SQLGlot
          </h1>
        </template>
        <template #description>
          <p class="gui-neutral-fg-muted my-2 text-sm lg:mx-56">
            A JavaScript/TypeScript SQL parser, transpiler, and optimizer ported from <a
              href="https://github.com/tobymao/sqlglot"
              target="_blank"
              rel="noopener noreferrer"
            >SQLGlot</a>.
            Parse, transpile, optimize, and run SQL across 33+ dialects in the browser or Node.js.
          </p>
        </template>
        <template #footer>
          <div class="flex justify-center">
            <GCodeBlock
              id="installation-code-block"
              code="npm install @hdnax/sqlingo.js"
              class="w-96"
              :language="GCodeLanguage.Bash"
              :highlight-theme="GHighlightTheme.AtomOne"
              :show-header="false"
            />
          </div>
          <div class="mt-4 flex justify-center gap-3">
            <GBadge
              label="npm"
              :color="GPillColor.Orange"
              :size="GBadgeSize.Md"
              :badge-style="GBadgeStyle.Flat"
              value="v0.2.2"
              href="https://www.npmjs.com/package/@hdnax/sqlingo.js"
            />
            <GBadge
              label="license"
              :color="GPillColor.Green"
              :size="GBadgeSize.Md"
              :badge-style="GBadgeStyle.Flat"
              value="MIT"
            />
            <GBadge
              label="SQLGlot"
              :color="GPillColor.Blue"
              :size="GBadgeSize.Md"
              :badge-style="GBadgeStyle.Flat"
              value="v28.10.1"
            />
          </div>
        </template>
      </GHero>

      <section
        id="quickstart"
        class="gui-neutral-border gui-neutral-bg-subtle border-y p-10 py-10 lg:px-48"
      >
        <div>
          <h2 class="gui-neutral-fg-muted text-md mb-2 font-medium uppercase">
            Interactive SQL Examples
          </h2>
          <SqlCodeExample />
        </div>
      </section>

      <section
        id="features"
        class="gui-neutral-fg-muted my-10 px-10 lg:px-48"
      >
        <div>
          <h2 class="mb-4 font-medium uppercase">
            SQL Parser Features
          </h2>
          <div class="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <div
              v-for="f in features"
              :key="f.title"
              class="gui-neutral-border flex flex-col gap-2 rounded-md border p-5"
            >
              <div>
                <GIcon
                  :name="f.icon"
                  class="gui-neutral-bg-subtle gui-neutral-border size-5 rounded-md border p-2"
                />
              </div>
              <h3 class="gui-neutral-fg font-bold">
                {{ f.title }}
              </h3>
              <p class="text-sm">
                {{ f.desc }}
              </p>
            </div>
          </div>
        </div>
      </section>

      <section
        id="dialects"
        class="gui-neutral-border gui-neutral-bg-subtle border-y p-10 lg:px-48"
      >
        <div>
          <h2 class="text-md my-4 font-medium uppercase">
            33+ Supported SQL Dialects
          </h2>
          <div class="flex flex-wrap gap-3">
            <GPill
              v-for="dialect in dialects"
              :key="dialect"
              :prominence="GPillProminence.Primary"
              :size="GPillSize.Md"
            >
              {{ dialect }}
            </GPill>
          </div>
        </div>
      </section>

      <section class="gui-neutral-fg-muted my-10 px-10 lg:px-48">
        <div class="flex items-center justify-between gap-2">
          <div>
            <h2 class="text-md mb-4 font-medium uppercase">
              API reference
            </h2>
            <p class="text-md">
              Full TypeScript types, all classes and functions documented.
            </p>
          </div>
          <GButton
            :prominence="GButtonProminence.Secondary"
            class="h-10 w-48"
            @click="goToApiReference"
          >
            Browse the docs
          </GButton>
        </div>
      </section>

      <section
        id="backstory"
        class="gui-neutral-border gui-neutral-bg-subtle border-y p-10 lg:px-48"
      >
        <div>
          <h2 class="text-md gui-neutral-fg-muted mb-4 font-medium uppercase">
            Why this exists
          </h2>
          <div class="gui-neutral-fg text-md gui-neutral-border flex flex-col gap-2 border-l-3 pl-5">
            <p>
              I maintain <a
                href="https://github.com/holistics/dbml"
                target="_blank"
                rel="noopener noreferrer"
              >@dbml/core</a> at work, a library that supports converting between DBML and SQL.
              Under the hood it uses ANTLR for parsing, and honestly it has been a mess:
              the bundle weighs <strong>33 MB</strong> (which actually broke our CI with OOM errors),
              we can't add more dialects without ballooning it further, and after all that
              we only support <strong>5 dialects</strong>, with error messages like <em>"No viable alternative at..."</em>.
            </p>
            <p>
              At a hackathon, I stumbled upon <a
                href="https://github.com/tobymao/sqlglot"
                target="_blank"
                rel="noopener noreferrer"
              >SQLGlot</a>. It was amazing that a library like this existed. Too bad it was in Python.
            </p>
            <p>
              I tried running it through Pyodide as a hack, but the runtime is too heavy to ship anywhere that matters.
              So I started porting it to JavaScript. Two weeks in,
              <a
                href="https://github.com/tobilg/polyglot"
                target="_blank"
                rel="noopener noreferrer"
              >polyglot</a> was announced (LoL! If only it were sooner). I kept doing anyways though, because I wanted full control over the implementation and staying in sync with upstream.
            </p>
            <p>
              sqlingo.js is a close mirror of SQLGlot. This way, I can easily catch up with SQLGlot updates as needed.
            </p>
          </div>
        </div>
      </section>

      <section
        id="goals"
        class="gui-neutral-fg-muted my-10 px-10 lg:px-48"
      >
        <div>
          <h2 class="text-md gui-neutral-fg-muted mb-4 font-medium uppercase">
            Design goals
          </h2>
          <div class="text-sm">
            <div
              v-for="g in goals"
              :key="g.title"
              class="my-3 flex gap-3"
            >
              <GPill
                :prominence="g.isNon ? GProminence.Primary : GProminence.Secondary"
                :size="GPillSize.Sm"
              >
                {{ g.isNon ? 'non-goal' : 'goal' }}
              </GPill>
              <div>
                <h3 class="mb-1 font-medium">
                  {{ g.title }}
                </h3>
                <p>
                  {{ g.desc }}
                </p>
              </div>
            </div>
          </div>
        </div>
      </section>

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
          "softwareVersion": "0.2.2",
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
  GButton, GButtonProminence,
  GHero,
  GIcon, GIconName,
  GPill, GProminence,
  GCodeBlock,
  GCodeLanguage,
  GHighlightTheme,
  GBadge,
  GPillColor,
  GBadgeSize,
  GBadgeStyle,
  GLogo,
  GLogoSize,
  GPillProminence,
  GPillSize,
} from '@hdnax/genuix';
import {
  useRouter,
} from 'vue-router';
import {
  useSeoMeta,
} from '@unhead/vue';
import SqlCodeExample from './SqlCodeExample.vue';
import MainLayout from '@/layout/main/MainLayout.vue';

const router = useRouter();
const scriptTag = 'script';

function goToApiReference () {
  router.push('/api-reference/');
}

useSeoMeta({
  title: 'Home | sqlingo.js',
  ogTitle: 'Home | sqlingo.js',
  description: 'sqlingo.js is the JavaScript/TypeScript port of SQLGlot. It is a SQL parser, transpiler, and optimizer supporting 33+ dialects including BigQuery, Snowflake, and Postgres.',
  ogDescription: 'sqlingo.js is the JavaScript/TypeScript port of SQLGlot. It is a SQL parser, transpiler, and optimizer supporting 33+ dialects including BigQuery, Snowflake, and Postgres.',
});

const features = [
  {
    title: 'SQL Parser',
    icon: GIconName.TreeStructure,
    desc: 'Turn any SQL string into a clean AST. A robust JavaScript SQL parser that works across 33+ dialects with real error messages.',
  },
  {
    title: 'SQL Transpiler',
    icon: GIconName.ArrowsLeftRight,
    desc: 'Convert SQL between dialects with our TypeScript SQL transpiler. DuckDB to Hive, Snowflake to BigQuery, and more.',
  },
  {
    title: 'SQL Optimizer',
    icon: GIconName.MagicWand,
    desc: 'Predicate pushdown, subquery elimination, and column qualification using the full SQLGlot optimizer ported to TypeScript.',
  },
  {
    title: 'SQL Engine',
    icon: GIconName.Play,
    desc: 'Run SQL in-process. A JavaScript SQL engine useful for tests, sandboxes, and in-browser query execution.',
  },
];

const goals = [
  {
    title: 'Close mirror to SQLGlot',
    desc: 'When SQLGlot fixes a bug or adds a dialect, sqlingo.js ports it. Staying in sync is the main priority.',
    isNon: false,
  },
  {
    title: 'TypeScript conventions',
    desc: 'Idiomatic TypeScript throughout, not a mechanical transliteration of Python.',
    isNon: false,
  },
  {
    title: 'Optimized performance',
    desc: 'The port is faithful first. Performance can come later.',
    isNon: true,
  },
  {
    title: 'Optimized bundle size',
    desc: 'Same reason. Tree-shaking helps, but this is not a priority today.',
    isNon: true,
  },
  {
    title: 'SQLGlot API compatibility',
    desc: 'The two libraries share logic, not an interface contract. Making them compatible would be trivial, but it is not a goal.',
    isNon: true,
  },
];

const dialects = [
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
</script>
