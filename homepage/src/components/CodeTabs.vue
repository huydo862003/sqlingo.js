<template>
  <div class="code-tabs">
    <div class="tab-bar">
      <GButton
        v-for="tab in tabs"
        :key="tab.id"
        :prominence="active === tab.id ? ButtonProminence.Primary : ButtonProminence.Ghost"
        :size="ButtonSize.Sm"
        @click="active = tab.id"
      >
        {{ tab.label }}
      </GButton>
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
import {
  GButton, ButtonProminence, ButtonSize,
} from '@hdnax/genuix';
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
  border: 1px solid var(--gui-neutral-border);
  border-radius: var(--radius-lg);
  overflow: hidden;
}

.tab-bar {
  display: flex;
  border-bottom: 1px solid var(--gui-neutral-border);
  background: var(--gui-neutral-bg-subtle);
  padding: var(--spacing-xs);
  gap: var(--spacing-xs);
}

.code-body {
  background: var(--gui-neutral-bg-subtle);
}

pre {
  margin: 0;
  padding: var(--spacing-lg);
  overflow-x: auto;
}
</style>
