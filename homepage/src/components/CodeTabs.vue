<template>
  <div class="code-tabs">
    <div class="tab-bar">
      <button
        v-for="tab in tabs"
        :key="tab.id"
        class="tab-btn"
        :class="{ active: active === tab.id }"
        @click="active = tab.id"
      >
        {{ tab.label }}
      </button>
    </div>
    <div class="code-body">
      <pre><code
        class="language-typescript"
        v-html="currentTab.html"
      /></pre>
    </div>
  </div>
</template>

<script setup lang="ts">
import {
  ref, computed,
} from 'vue';
import hljs from 'highlight.js/lib/core';
import typescript from 'highlight.js/lib/languages/typescript';
import 'highlight.js/styles/github-dark.css';

hljs.registerLanguage('typescript', typescript);

function highlight (code: string): string {
  return hljs.highlight(code, {
    language: 'typescript',
  }).value;
}

const tabs = [
  {
    id: 'parse',
    label: 'Parse',
    html: highlight(`import { parse } from "@hdnax/sqlingo.js";

const [ast] = parse(
  "SELECT a, b FROM t WHERE a > 1",
  { read: "mysql" },
);
// => "SELECT a, b FROM t WHERE a > 1"`),
  },
  {
    id: 'transpile',
    label: 'Transpile',
    html: highlight(`import { transpile } from "@hdnax/sqlingo.js";

const [result] = transpile("SELECT EPOCH_MS(1618088028295)", {
  read: "duckdb",
  write: "hive",
});
// => "SELECT FROM_UNIXTIME(1618088028295 / POW(10, 3))"`),
  },
  {
    id: 'optimize',
    label: 'Optimize',
    html: highlight(`import { optimize } from "@hdnax/sqlingo.js";

const result = optimize(
  "SELECT a FROM (SELECT a, b FROM t) sub WHERE sub.a > 1",
  { dialect: "duckdb" },
);
// => "SELECT t.a FROM t WHERE t.a > 1"`),
  },
];

const active = ref('parse');
const currentTab = computed(() => tabs.find((t) => t.id === active.value) ?? tabs[0]);
</script>

<style scoped>
@reference "../style.css";

.code-tabs {
  @apply border border-border rounded-[var(--radius-lg)] overflow-hidden;
}

.tab-bar {
  @apply flex border-b border-border bg-bg-subtle;
}

.tab-btn {
  @apply px-4 py-2.5 text-xs font-semibold text-fg-muted cursor-pointer border-b-2 border-transparent;
  background: none;
  border-top: none;
  border-left: none;
  border-right: none;
  transition: color 0.15s, border-color 0.15s;
}
.tab-btn:hover { @apply text-fg; }
.tab-btn.active {
  @apply text-accent;
  border-bottom-color: var(--color-accent);
}

.code-body { @apply bg-bg-subtle; }

pre {
  margin: 0;
  padding: 1.25rem 1.5rem;
  overflow-x: auto;
}
</style>
