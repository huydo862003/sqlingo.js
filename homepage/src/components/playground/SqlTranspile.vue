<template>
  <div class="sql-transpile">
    <div class="panels">
      <div class="panel">
        <div class="panel-header">
          <div class="panel-header-left">
            <span class="panel-label">Input SQL</span>
            <GSelect
              v-model="fromDialect"
              :size="SelectSize.Xs"
              placeholder="Dialect"
            >
              <GSelectOption
                v-for="d in DIALECTS"
                :key="d.value"
                :value="d.value"
                :label="d.label"
              />
            </GSelect>
          </div>
        </div>
        <MonacoEditor v-model="sqlInput" />
      </div>

      <div class="panel">
        <div class="panel-header">
          <div class="panel-header-left">
            <span class="panel-label">Output SQL</span>
            <GSelect
              v-model="toDialect"
              :size="SelectSize.Xs"
              placeholder="Dialect"
            >
              <GSelectOption
                v-for="d in DIALECTS"
                :key="d.value"
                :value="d.value"
                :label="d.label"
              />
            </GSelect>
          </div>
          <GButton
            :prominence="ButtonProminence.Secondary"
            :size="ButtonSize.Sm"
            :disabled="!sqlOutput"
            @click="copyOutput"
          >
            <GIcon
              :name="GIconName.Copy"
              :size="12"
            />
            {{ copied ? 'Copied!' : 'Copy' }}
          </GButton>
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
  GSelect, GSelectOption, SelectSize,
  GButton, ButtonProminence, ButtonSize,
  GIcon, GIconName,
} from '@hdnax/genuix';
import {
  transpile,
} from '@/services/transpile';
import {
  DIALECTS,
} from '@/services/dialects';
import MonacoEditor from './MonacoEditor.vue';
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
  width: 100%;
}

.output-error :deep(.monaco-wrap) {
  outline: 1px solid var(--gui-danger-border);
  border-radius: var(--radius-md);
}
</style>
