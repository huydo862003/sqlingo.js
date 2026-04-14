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
      <pre
        v-for="tab in tabs"
        v-show="active === tab.id"
        :key="tab.id"
      ><code
ref="codeEls"
             class="language-typescript"
      >{{ tab.code }}</code></pre>
    </div>
  </div>
</template>

<script setup lang="ts">
import {
  onMounted,
  ref,
  useTemplateRef,
} from 'vue';
import hljs from 'highlight.js/lib/core';
import typescript from 'highlight.js/lib/languages/typescript';
import 'highlight.js/styles/github-dark.css';

hljs.registerLanguage('typescript', typescript);

const tabs = [
  {
    id: 'parse',
    label: 'Parse',
    code: `import { parse } from "@hdnax/sqlingo.js";

const [ast] = parse("SELECT a, b FROM t WHERE a > 1");
console.log(ast.toString());
// => "SELECT a, b FROM t WHERE a > 1"`,
  },
  {
    id: 'transpile',
    label: 'Transpile',
    code: `import { transpile } from "@hdnax/sqlingo.js";

const [result] = transpile("SELECT EPOCH_MS(1618088028295)", {
  read: "duckdb",
  write: "hive",
});
// => "SELECT FROM_UNIXTIME(1618088028295 / POW(10, 3))"`,
  },
  {
    id: 'optimize',
    label: 'Optimize',
    code: `import { optimize } from "@hdnax/sqlingo.js";

const result = optimize(
  "SELECT a FROM (SELECT a, b FROM t) sub WHERE sub.a > 1",
  { dialect: "duckdb" },
);
// => "SELECT t.a FROM t WHERE t.a > 1"`,
  },
];

const active = ref('parse');
const codeEls = useTemplateRef<HTMLElement[]>('codeEls');

onMounted(() => {
  codeEls.value?.forEach((el) => hljs.highlightElement(el));
});
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
