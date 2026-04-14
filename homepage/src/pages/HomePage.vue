<template>
  <main>
    <section class="hero">
      <div class="page">
        <img
          src="/icon.svg"
          alt=""
          class="hero-icon"
        >
        <h1 class="hero-title">
          sqlingo.js
        </h1>
        <p class="hero-sub">
          A JavaScript port of <a
            href="https://github.com/tobymao/sqlglot"
            target="_blank"
            rel="noopener"
          >SQLGlot</a>.
          Parse, transpile, optimize, and run SQL across 33+ dialects, in the browser or Node.
        </p>
        <div class="install-row">
          <div class="install-box">
            <code>npm install @hdnax/sqlingo.js</code>
            <button
              class="copy-btn"
              @click="copy"
            >
              {{ copied ? 'Copied!' : 'Copy' }}
            </button>
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
            alt="MIT"
          >
          <img
            src="https://img.shields.io/badge/SQLGlot-v28.10.0-blue"
            alt="SQLGlot v28.10.0"
          >
        </div>
      </div>
    </section>

    <section
      id="quickstart"
      class="section section-alt"
    >
      <div class="page">
        <h2>Try it</h2>
        <CodeTabs />
      </div>
    </section>

    <section
      id="features"
      class="section"
    >
      <div class="page">
        <h2>What it can do</h2>
        <div class="features">
          <div
            v-for="f in features"
            :key="f.title"
            class="feature-card"
          >
            <div class="feature-icon">
              <component
                :is="f.icon"
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
        <h2>Dialects</h2>
        <div class="dialects">
          <span
            v-for="d in dialects"
            :key="d"
            class="chip"
          >{{ d }}</span>
        </div>
      </div>
    </section>

    <section class="api-cta">
      <div class="page api-cta-inner">
        <div class="api-cta-text">
          <h2>API reference</h2>
          <p>Full TypeScript types, all classes and functions documented.</p>
        </div>
        <a
          href="./api-reference/"
          class="btn btn-primary"
        >Browse the docs</a>
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
            >SQLGlot</a>. It was amazing that a library like this existed.
            33+ dialects, a clean AST, good errors. Trusted by a lot of tools in the Python ecosystem.
            Exactly what I needed, except it is Python.
          </p>
          <p>
            I tried running it through Pyodide as a hack, but the runtime is too heavy to ship anywhere that matters.
            So I started porting it to JavaScript. Two weeks in,
            <a
              href="https://github.com/tobilg/polyglot"
              target="_blank"
              rel="noopener"
            >polyglot</a> was announced. I kept going anyway, I wanted full control and to stay in sync with upstream.
          </p>
          <p>
            sqlingo.js is a close mirror of SQLGlot. When SQLGlot fixes a bug or adds a dialect, I port it.
            It is not perfect, but it has been useful enough that I wanted to share it.
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
            <span
              class="goal-label"
              :class="g.isNon ? 'goal-label-non' : 'goal-label-yes'"
            >{{ g.isNon ? 'non-goal' : 'goal' }}</span>
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
  PhTreeStructure, PhArrowsLeftRight, PhMagicWand, PhPlay,
} from '@phosphor-icons/vue';
import CodeTabs from '../components/CodeTabs.vue';

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
    title: 'Parse',
    icon: PhTreeStructure,
    desc: 'Turn any SQL string into an AST. Works across dialects, gives you real error messages.',
  },
  {
    title: 'Transpile',
    icon: PhArrowsLeftRight,
    desc: 'Convert SQL between dialects. DuckDB to Hive, Snowflake to BigQuery, whatever you need.',
  },
  {
    title: 'Optimize',
    icon: PhMagicWand,
    desc: 'Predicate pushdown, subquery elimination, column qualification, the full SQLGlot optimizer.',
  },
  {
    title: 'Execute',
    icon: PhPlay,
    desc: 'Run SQL in-process. Useful for tests, sandboxes, and in-browser query engines.',
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
  padding: 0 1.5rem;
}

.page-wide {
  max-width: 72rem;
}

.section-sub {
  @apply text-sm text-fg-muted mb-6 mt-[-1rem];
}

.hero {
  @apply py-20 text-center border-b border-border;
}

.hero-icon {
  @apply w-20 h-20 mb-6 mx-auto block rounded-[var(--radius-lg)];
  background: #fff;
  border: 1px solid var(--color-border);
  box-shadow: 0 2px 8px rgba(0,0,0,0.08);
}

.hero-title {
  @apply font-black tracking-tight text-fg mb-4;
  font-size: clamp(2rem, 6vw, 3rem);
}

.hero-sub {
  @apply text-sm text-fg-muted mb-8 max-w-lg mx-auto leading-relaxed;
}

.install-row {
  @apply flex justify-center mb-5;
}

.install-box {
  @apply flex items-center gap-3 px-4 py-3 border border-border rounded-[var(--radius-md)] bg-bg-subtle;
}

.install-box code {
  @apply text-xs text-accent;
}

.copy-btn {
  @apply text-xs px-2.5 py-1 rounded-[var(--radius-sm)] cursor-pointer text-accent border border-accent/30;
  background: color-mix(in srgb, var(--color-accent) 8%, transparent);
  transition: background 0.15s;
}
.copy-btn:hover {
  background: color-mix(in srgb, var(--color-accent) 15%, transparent);
}

.badges {
  @apply flex justify-center gap-2 flex-wrap;
}

.section {
  @apply py-14;
}

.section-alt {
  @apply bg-bg-subtle border-y border-border;
}

.section h2, .api-cta-text h2 {
  @apply text-base font-bold text-fg-muted mb-6 uppercase tracking-widest;
}

.features {
  @apply grid gap-3;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
}

.feature-card {
  @apply p-4 border border-border rounded-[var(--radius-md)];
}

.feature-icon {
  @apply w-8 h-8 rounded-[var(--radius-md)] flex items-center justify-center mb-3 text-accent;
  background: color-mix(in srgb, var(--color-accent) 10%, transparent);
}

.feature-card h3 {
  @apply text-sm font-bold text-fg mb-1.5;
}

.feature-card p {
  @apply text-xs text-fg-muted leading-relaxed;
}

.api-cta {
  @apply py-10 border-y border-border;
}

.api-cta-inner {
  @apply flex items-center justify-between gap-6 flex-wrap;
}

.api-cta-text h2 {
  @apply mb-1;
}

.api-cta-text p {
  @apply text-sm text-fg-muted m-0;
}

.goals {
  @apply flex flex-col gap-3;
}

.goal-row {
  @apply flex items-start gap-3 text-sm text-fg;
}

.goal-label {
  @apply text-[10px] font-bold px-2 py-0.5 rounded-full shrink-0 mt-0.5;
}

.goal-label-yes {
  background: color-mix(in srgb, var(--color-accent) 12%, transparent);
  color: var(--color-accent);
}

.goal-label-non {
  @apply border border-border text-fg-faint;
}

.goal-desc {
  @apply text-fg-muted text-xs mt-0.5 m-0;
}

.dialects {
  @apply flex flex-wrap gap-1.5;
}

.chip {
  @apply px-2.5 py-1 text-xs rounded-[var(--radius-sm)] border border-border text-fg-muted;
}

.prose {
  @apply text-sm text-fg leading-relaxed border-l-2 border-accent pl-5;
  display: flex;
  flex-direction: column;
  gap: 0.875rem;
}
.prose p { @apply m-0; }

.btn {
  @apply px-5 py-2 rounded-[var(--radius-md)] text-sm font-semibold no-underline shrink-0;
  transition: opacity 0.15s;
}
.btn:hover { opacity: 0.8; @apply no-underline; }

.btn-primary {
  background: var(--color-accent);
  color: #fff;
}

footer {
  @apply py-5 border-t border-border;
  background: #111118;
}

.footer-inner {
  @apply flex justify-between items-start gap-4 text-xs flex-wrap;
  color: #707080;
}

.footer-attribution {
  @apply flex flex-col gap-1;
}

footer a {
  color: #a0a0b4;
}
footer a:hover {
  color: #e8e8ec;
}
</style>
