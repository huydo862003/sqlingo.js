<template>
  <div class="sql-to-dbml">
    <div class="panels">
      <div class="panel">
        <div class="panel-header">
          <div class="panel-header-left">
            <span class="panel-label">SQL</span>
            <DialectSelect
              v-model="dialect"
              :allow-auto="true"
            />
          </div>
        </div>
        <MonacoEditor v-model="sqlInput" />
      </div>

      <div class="panel">
        <div class="panel-header">
          <span class="panel-label">DBML</span>
          <button
            class="copy-btn"
            :disabled="!dbmlOutput"
            @click="copyDbml"
          >
            {{ copied ? 'Copied!' : 'Copy' }}
          </button>
        </div>
        <div :class="{ 'output-error': error }">
          <MonacoEditor
            :model-value="dbmlOutput || error"
            :read-only="true"
            :language="dbmlOutput ? 'dbml' : 'plaintext'"
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
  convertSqlToDbml,
} from '../services/dbml';
import MonacoEditor from './MonacoEditor.vue';
import DialectSelect from './DialectSelect.vue';
import {
  usePlaygroundStore,
} from '../stores/playground';

const store = usePlaygroundStore();
const sqlInput = computed({
  get: () => store.dbmlInput,
  set: (v) => {
    store.dbmlInput = v; store.persist();
  },
});
const dialect = computed({
  get: () => store.dbmlDialect,
  set: (v) => {
    store.dbmlDialect = v; store.persist();
  },
});
const dbmlOutput = ref('');
const error = ref('');
const copied = ref(false);

function stripAnsi (s: string): string {
  return s.replace(/\x1b\[[0-9;]*m/g, '');
}

function convert () {
  if (!sqlInput.value.trim()) {
    dbmlOutput.value = '';
    error.value = '';
    return;
  }
  try {
    const result = convertSqlToDbml(sqlInput.value, dialect.value || undefined);
    dbmlOutput.value = result.dbml || '';
    error.value = result.dbml ? '' : 'No CREATE TABLE statements found.';
  } catch (e) {
    dbmlOutput.value = '';
    error.value = stripAnsi(e instanceof Error ? e.message : String(e));
  }
}

function copyDbml () {
  navigator.clipboard.writeText(dbmlOutput.value).then(() => {
    copied.value = true;
    setTimeout(() => {
      copied.value = false;
    }, 2000);
  });
}

watch([
  sqlInput,
  dialect,
], convert);
onMounted(convert);
</script>

<style scoped>
@reference "../style.css";
@import './playground.css';

.sql-to-dbml {
  @apply w-full;
}

.output-error :deep(.monaco-wrap) {
  @apply outline outline-1 rounded;
  outline-color: rgb(239 68 68 / 0.4);
}
</style>
