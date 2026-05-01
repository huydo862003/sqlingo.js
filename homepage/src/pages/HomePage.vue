<template>
  <NavBar />
  <main>
    <GHero class="hero">
      <template #icon>
        <img
          src="/icon.svg"
          alt="sqlingo.js logo"
          class="hero-icon"
        >
      </template>
      <template #title>
        <h1 class="hero-title">
          sqlingo.js: A Typescript/Javascript port of SQLGlot
        </h1>
      </template>
      <template #description>
        <p class="hero-description">
          A JavaScript/TypeScript SQL parser, transpiler, and optimizer ported from <a
            href="https://github.com/tobymao/sqlglot"
            target="_blank"
            rel="noopener"
          >SQLGlot</a>.
          Parse, transpile, optimize, and run SQL across 33+ dialects in the browser or Node.js.
        </p>
      </template>
      <template #footer>
        <div class="install-row">
          <div class="install-box">
            <code>npm install @hdnax/sqlingo.js</code>
            <GButton
              :prominence="ButtonProminence.Secondary"
              :size="ButtonSize.Sm"
              @click="copy"
            >
              <GIcon
                :name="GIconName.Copy"
                :size="12"
              />
              {{ copied ? 'Copied!' : 'Copy' }}
            </GButton>
          </div>
        </div>
        <div class="badges">
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
            alt="Licensed under MIT"
          >
          <img
            src="https://img.shields.io/badge/SQLGlot-v28.10.0-blue"
            alt="Ported from SQLGlot v28.10.0"
          >
        </div>
      </template>
    </GHero>

    <section
      id="quickstart"
      class="section section-alt"
    >
      <div class="page">
        <h2>Interactive SQL Examples</h2>
        <CodeTabs />
      </div>
    </section>

    <section
      id="features"
      class="section"
    >
      <div class="page">
        <h2>SQL Parser Features</h2>
        <div class="features">
          <div
            v-for="f in features"
            :key="f.title"
            class="feature-card"
          >
            <div class="feature-icon">
              <GIcon
                :name="f.icon"
                :size="20"
              />
            </div>
            <h3>{{ f.title }}</h3>
            <p>{{ f.desc }}</p>
          </div>
        </div>
      </div>
    </section>

    <section
      id="dialects"
      class="section section-alt"
    >
      <div class="page">
        <h2>33+ Supported SQL Dialects</h2>
        <div class="dialects">
          <GPill
            v-for="dialect in dialects"
            :key="dialect"
          >
            {{ dialect }}
          </GPill>
        </div>
      </div>
    </section>

    <section class="api-cta">
      <div class="page api-cta-inner">
        <div class="api-cta-text">
          <h2>API reference</h2>
          <p>Full TypeScript types, all classes and functions documented.</p>
        </div>
        <GButton
          :prominence="ButtonProminence.Primary"
          :size="ButtonSize.Md"
          class="api-cta-button"
          @click="$router.push('/api-reference/')"
        >
          Browse the docs
        </GButton>
      </div>
    </section>

    <section
      id="backstory"
      class="section section-alt"
    >
      <div class="page">
        <h2>Why this exists</h2>
        <div class="prose">
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
      class="section"
    >
      <div class="page">
        <h2>Design goals</h2>
        <div class="goals">
          <div
            v-for="g in goals"
            :key="g.title"
            class="goal-row"
          >
            <GPill
              :prominence="g.isNon ? Prominence.Secondary : Prominence.Primary"
              class="goal-pill"
            >
              {{ g.isNon ? 'non-goal' : 'goal' }}
            </GPill>
            <div>
              <strong>{{ g.title }}</strong>
              <p class="goal-desc">
                {{ g.desc }}
              </p>
            </div>
          </div>
        </div>
      </div>
    </section>

    <!-- Structured Data for SEO -->
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

    <footer>
      <div class="page footer-inner">
        <div class="footer-attribution">
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
</template>

<script setup lang="ts">
import {
  ref,
} from 'vue';
import {
  GButton, ButtonProminence, ButtonSize,
  GHero,
  GIcon, GIconName,
  GPill, Prominence,
} from '@hdnax/genuix';
import CodeTabs from '@/components/CodeTabs.vue';
import NavBar from '@/components/NavBar.vue';
import {
  useSeo,
} from '@/composables/useSeo';

useSeo({
  title: 'Home',
  description: 'sqlingo.js is the JavaScript/TypeScript port of SQLGlot. It is a SQL parser, transpiler, and optimizer supporting 33+ dialects including BigQuery, Snowflake, and Postgres.',
});

const copied = ref(false);

function copy () {
  navigator.clipboard.writeText('npm install @hdnax/sqlingo.js').then(() => {
    copied.value = true;
    setTimeout(() => {
      copied.value = false;
    }, 2000);
  });
}

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

<style scoped>
@reference "../style.css";

.page {
  max-width: 52rem;
  margin: 0 auto;
  padding: 0 var(--spacing-lg);
}

.page-wide {
  max-width: 72rem;
}

.section-sub {
  font-size: var(--text-sm);
  color: var(--gui-neutral-fg-muted);
  margin-bottom: var(--spacing-lg);
  margin-top: calc(-1 * var(--spacing-md));
}

.hero {
  padding: var(--spacing-3xl) var(--spacing-lg);
}

.hero-icon {
  width: 5rem;
  height: 5rem;
  border-radius: var(--radius-lg);
  background: #fff;
  border: 1px solid var(--gui-neutral-border);
  box-shadow: var(--shadow-sm);
}

.hero-title {
  font-weight: 900;
  letter-spacing: -0.02em;
  color: var(--gui-neutral-fg);
  font-size: clamp(2rem, 6vw, 3rem);
}

.hero-description {
  font-size: var(--text-sm);
  color: var(--gui-neutral-fg-muted);
  max-width: 32rem;
  line-height: 1.6;
}

.install-row {
  display: flex;
  justify-content: center;
  margin-bottom: var(--spacing-lg);
}

.install-box {
  display: flex;
  align-items: center;
  gap: var(--spacing-sm);
  padding: var(--spacing-sm) var(--spacing-md);
  border: 1px solid var(--gui-neutral-border);
  border-radius: var(--radius-md);
  background: var(--gui-neutral-bg-subtle);
}

.install-box code {
  font-size: var(--text-xs);
  color: var(--gui-neutral-fg);
}

.badges {
  display: flex;
  justify-content: center;
  gap: var(--spacing-sm);
  flex-wrap: wrap;
}

.section {
  padding-top: var(--spacing-2xl);
  padding-bottom: var(--spacing-2xl);
}

.section-alt {
  background: var(--gui-neutral-bg-subtle);
  border-top: 1px solid var(--gui-neutral-border);
  border-bottom: 1px solid var(--gui-neutral-border);
}

.section h2,
.api-cta-text h2 {
  font-size: var(--text-sm);
  font-weight: var(--font-weight-bold);
  color: var(--gui-neutral-fg-muted);
  margin-bottom: var(--spacing-lg);
  text-transform: uppercase;
  letter-spacing: 0.1em;
}

.features {
  display: grid;
  gap: var(--spacing-sm);
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
}

.feature-card {
  padding: var(--spacing-md);
  border: 1px solid var(--gui-neutral-border);
  border-radius: var(--radius-md);
}

.feature-icon {
  width: var(--size-5);
  height: var(--size-5);
  border-radius: var(--radius-md);
  display: flex;
  align-items: center;
  justify-content: center;
  margin-bottom: var(--spacing-sm);
  color: var(--gui-primary-fg-muted);
  background: var(--gui-primary-bg-hover);
}

.feature-card h3 {
  font-size: var(--text-sm);
  font-weight: var(--font-weight-bold);
  color: var(--gui-neutral-fg);
  margin-bottom: var(--spacing-xs);
}

.feature-card p {
  font-size: var(--text-xs);
  color: var(--gui-neutral-fg-muted);
  line-height: 1.6;
}

.api-cta {
  padding-top: var(--spacing-xl);
  padding-bottom: var(--spacing-xl);
  border-top: 1px solid var(--gui-neutral-border);
  border-bottom: 1px solid var(--gui-neutral-border);
}

.api-cta-inner {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-lg);
  flex-wrap: wrap;
}

.api-cta-text h2 {
  margin-bottom: var(--spacing-xs);
}

.api-cta-text p {
  font-size: var(--text-sm);
  color: var(--gui-neutral-fg-muted);
  margin: 0;
}

.api-cta-button {
  flex-shrink: 0;
}

.goals {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-sm);
}

.goal-row {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-sm);
  font-size: var(--text-sm);
  color: var(--gui-neutral-fg);
}

.goal-pill {
  flex-shrink: 0;
  margin-top: 2px;
}

.goal-desc {
  color: var(--gui-neutral-fg-muted);
  font-size: var(--text-xs);
  margin-top: 2px;
  margin-bottom: 0;
}

.dialects {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-xs);
}

.prose {
  font-size: var(--text-sm);
  color: var(--gui-neutral-fg);
  line-height: 1.6;
  border-left: 2px solid var(--gui-primary-solid);
  padding-left: var(--spacing-lg);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-sm);
}

.prose p {
  margin: 0;
}

footer {
  padding-top: var(--spacing-lg);
  padding-bottom: var(--spacing-lg);
  border-top: 1px solid var(--gui-neutral-border);
  background: var(--color-neutral-1);
}

.footer-inner {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: var(--spacing-md);
  font-size: var(--text-xs);
  flex-wrap: wrap;
  color: var(--gui-neutral-fg-muted);
}

.footer-attribution {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-xs);
}

footer a {
  color: var(--gui-neutral-fg-muted);
}

footer a:hover {
  color: var(--gui-neutral-fg);
}
</style>
