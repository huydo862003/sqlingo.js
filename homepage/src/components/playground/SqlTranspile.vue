<template>
  <div class="sql-transpile">
    <div class="panels">
      <div class="panel">
        <div class="panel-header">
          <div class="panel-header-left">
            <span class="panel-label">Input SQL</span>
            <DialectSelect v-model="fromDialect" />
          </div>
        </div>
        <MonacoEditor v-model="sqlInput" />
      </div>

      <div class="panel">
        <div class="panel-header">
          <div class="panel-header-left">
            <span class="panel-label">Output SQL</span>
            <DialectSelect v-model="toDialect" />
          </div>
          <button
            class="copy-btn"
            :disabled="!sqlOutput"
            @click="copyOutput"
          >
            {{ copied ? 'Copied!' : 'Copy' }}
          </button>
        </div>
        <div :class="{ 'output-error': error }">
          <MonacoEditor
            :model-value="sqlOutput || error"
            :read-only="true"
          />
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import {
  ref, computed, watch, onMounted,
} from 'vue';
import {
  transpile,
} from '@/services/transpile';
import MonacoEditor from './MonacoEditor.vue';
import DialectSelect from './DialectSelect.vue';
import {
  usePlaygroundStore,
} from '@/stores/playground';

const store = usePlaygroundStore();
const fromDialect = computed({
  get: () => store.transpileFrom,
  set: (v) => {
    store.transpileFrom = v; store.persist();
  },
});
const toDialect = computed({
  get: () => store.transpileTo,
  set: (v) => {
    store.transpileTo = v; store.persist();
  },
});
const sqlInput = computed({
  get: () => store.transpileInput,
  set: (v) => {
    store.transpileInput = v; store.persist();
  },
});

const sqlOutput = ref('');
const error = ref('');
const copied = ref(false);

function stripAnsi (s: string): string {
  return s.replace(/\x1b\[[0-9;]*m/g, '');
}

function convert () {
  if (!sqlInput.value.trim()) {
    sqlOutput.value = '';
    error.value = '';
    return;
  }
  try {
    const results = transpile(sqlInput.value, {
      read: fromDialect.value,
      write: toDialect.value,
      pretty: true,
    });
    sqlOutput.value = results.join('\n');
    error.value = '';
  } catch (e) {
    sqlOutput.value = '';
    error.value = stripAnsi(e instanceof Error ? e.message : String(e));
  }
}

function copyOutput () {
  navigator.clipboard.writeText(sqlOutput.value).then(() => {
    copied.value = true;
    setTimeout(() => {
      copied.value = false;
    }, 2000);
  });
}

watch([
  sqlInput,
  fromDialect,
  toDialect,
], convert);
onMounted(convert);
</script>

<style scoped>
@reference '../../style.css';
@import './playground.css';

.sql-transpile {
  @apply w-full;
}

.output-error :deep(.monaco-wrap) {
  @apply outline-1 rounded;
  outline-color: rgb(239 68 68 / 0.4);
}
</style>
