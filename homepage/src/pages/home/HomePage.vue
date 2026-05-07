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
            class="bg-white rounded-md"
          />
        </template>
        <template #title>
          <h1 class="font-extrabold text-3xl">
            sqlingo.js: A Typescript/Javascript port of SQLGlot
          </h1>
        </template>
        <template #description>
          <p class="my-2 text-sm gui-neutral-fg-muted">
            A JavaScript/TypeScript SQL parser, transpiler, and optimizer ported from <a
              href="https://github.com/tobymao/sqlglot"
              target="_blank"
              rel="noopener"
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
          <div class="mt-4 flex gap-3 justify-center">
            <GBadge
              label="npm"
              :color="GPillColor.Orange"
              :size="GBadgeSize.Md"
              :badge-style="GBadgeStyle.Flat"
              value="v0.1.7"
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
              value="v28.10.0"
            />
          </div>
        </template>
      </GHero>

      <section
        id="quickstart"
        class="gui-neutral-border border gui-neutral-bg-subtle p-5"
      >
        <div>
          <h2 class="font-medium uppercase gui-neutral-fg-muted text-sm mb-2">
            Interactive SQL Examples
          </h2>
          <SqlCodeExample />
        </div>
      </section>

      <section
        id="features"
        class="mt-5 p-5 gui-neutral-fg-muted"
      >
        <div>
          <h2 class="font-medium uppercase text-sm mb-4">
            SQL Parser Features
          </h2>
          <div class="grid gap-3 sm:grid-cols-2 lg:grid-cols-3 text-md">
            <div
              v-for="f in features"
              :key="f.title"
              class="flex flex-col gap-2 border rounded-md gui-neutral-border p-5"
            >
              <div>
                <GIcon
                  :name="f.icon"
                  class="gui-neutral-bg-subtle size-5 rounded-md p-2 border gui-neutral-border"
                />
              </div>
              <h3 class="font-bold gui-neutral-fg">
                {{ f.title }}
              </h3>
              <p class="text-sm leading-1">
                {{ f.desc }}
              </p>
            </div>
          </div>
        </div>
      </section>

      <section
        id="dialects"
        class="gui-neutral-border border gui-neutral-bg-subtle p-5"
      >
        <div>
          <h2 class="font-medium text-md uppercase mb-4">
            33+ Supported SQL Dialects
          </h2>
          <div class="flex flex-wrap gap-2">
            <GPill
              v-for="dialect in dialects"
              :key="dialect"
              :prominence="GPillProminence.Primary"
              :size="GPillSize.Sm"
            >
              {{ dialect }}
            </GPill>
          </div>
        </div>
      </section>

      <section class="mt-5 p-5 gui-neutral-fg-muted">
        <div class="flex gap-2 items-center">
          <div>
            <h2 class="font-medium text-md uppercase mb-4">
              API reference
            </h2>
            <p class="text-md">
              Full TypeScript types, all classes and functions documented.
            </p>
          </div>
          <GButton
            :prominence="GButtonProminence.Secondary"
            class="h-10 w-48"
            @click="$router.push('/api-reference/')"
          >
            Browse the docs
          </GButton>
        </div>
      </section>

      <section
        id="backstory"
        class="gui-neutral-border border gui-neutral-bg-subtle p-5"
      >
        <div>
          <h2 class="font-medium text-md uppercase mb-4 gui-neutral-fg-muted">
            Why this exists
          </h2>
          <div class="gui-neutral-fg text-md flex flex-col gap-2 border-l-3 gui-neutral-border pl-5">
            <p>
              I maintain <a
                href="https://github.com/holistics/dbml"
                target="_blank"
                rel="noopener"
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
                rel="noopener"
              >SQLGlot</a>. It was amazing that a library like this existed. Too bad it was in Python.
            </p>
            <p>
              I tried running it through Pyodide as a hack, but the runtime is too heavy to ship anywhere that matters.
              So I started porting it to JavaScript. Two weeks in,
              <a
                href="https://github.com/tobilg/polyglot"
                target="_blank"
                rel="noopener"
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
        class="mt-5 p-5 gui-neutral-fg-muted"
      >
        <div>
          <h2 class="font-medium text-md uppercase mb-4 gui-neutral-fg-muted">
            Design goals
          </h2>
          <div class="text-sm">
            <div
              v-for="g in goals"
              :key="g.title"
              class="flex gap-3 my-3"
            >
              <GPill
                :prominence="g.isNon ? GProminence.Primary : GProminence.Secondary"
                :size="GPillSize.Sm"
              >
                {{ g.isNon ? 'non-goal' : 'goal' }}
              </GPill>
              <div>
                <h3 class="font-medium mb-1">
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
        :is="'script'"
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
          "softwareVersion": "0.0.0",
          "url": "https://huydo862003.github.io/sqlingo.js/",
          "author": {
            "@type": "Person",
            "name": "Huy Do"
          }
        }) }}
      </component>

      <footer class="mt-5 gui-neutral-bg-subtle text-sm py-5 px-3 border-t gui-neutral-border gui-neutral-fg-muted">
        <div class="flex justify-between items-end">
          <div class="w-96">
            <span>
              sqlingo.js is licensed under the
              <a
                href="https://github.com/huydo862003/sqlingo.js/blob/master/COPYRIGHT_NOTICE"
                target="_blank"
                rel="noopener"
              >MIT License</a>.
            </span>
            <span>
              Based on <a
                href="https://github.com/tobymao/sqlglot"
                target="_blank"
                rel="noopener"
              >SQLGlot</a> by Toby Mao, also MIT.
            </span>
          </div>
          <a
            href="https://github.com/huydo862003/sqlingo.js/issues"
            target="_blank"
            rel="noopener"
          >Report an issue</a>
        </div>
      </footer>
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
import SqlCodeExample from './SqlCodeExample.vue';
import MainLayout from '@/layout/main/MainLayout.vue';
import {
  useSeo,
} from '@/composables/useSeo';

useSeo({
  title: 'Home',
  description: 'sqlingo.js is the JavaScript/TypeScript port of SQLGlot. It is a SQL parser, transpiler, and optimizer supporting 33+ dialects including BigQuery, Snowflake, and Postgres.',
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
