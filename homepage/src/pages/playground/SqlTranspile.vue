<template>
  <div class="w-full">
    <div class="flex flex-col gap-sm">
      <div class="flex flex-col rounded-md border gui-neutral-border overflow-hidden">
        <div class="flex items-center justify-between p-sm border-b gui-neutral-border gui-neutral-bg-subtle">
          <div class="flex items-center gap-sm">
            <span class="text-sm font-medium uppercase tracking-wide gui-neutral-fg-muted">Input SQL</span>
            <GSelect
              v-model="fromDialect"
              :size="GSelectSize.Xs"
              :variant="GSelectVariant.Box"
              class="w-40"
              placeholder="Dialect"
              close-on-select
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

      <div class="flex flex-col rounded-md border gui-neutral-border overflow-hidden">
        <div class="flex items-center justify-between p-sm border-b gui-neutral-border gui-neutral-bg-subtle">
          <div class="flex items-center gap-sm">
            <span class="text-sm font-medium uppercase tracking-wide gui-neutral-fg-muted">Output SQL</span>
            <GSelect
              v-model="toDialect"
              :size="GSelectSize.Xs"
              :variant="GSelectVariant.Box"
              class="w-40"
              placeholder="Dialect"
              close-on-select
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
            :prominence="GButtonProminence.Secondary"
            :size="GButtonSize.Xs"
            :disabled="!sqlOutput"
            class="flex gap-2 p-3"
            @click="copyOutput"
          >
            <GIcon
              :name="GIconName.Copy"
              :size="12"
            />
            {{ copied ? 'Copied!' : 'Copy' }}
          </GButton>
        </div>
        <div :class="error ? 'outline-1 outline-(--gui-danger-border) rounded-md' : ''">
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
  GSelect, GSelectOption, GSelectSize, GSelectVariant,
  GButton, GButtonProminence, GButtonSize,
  GIcon, GIconName,
} from '@hdnax/genuix';
import MonacoEditor from './MonacoEditor.vue';
import {
  transpile,
} from '@/services/transpile';
import {
  DIALECTS,
} from '@/services/dialects';
import {
  usePlaygroundStore,
} from '@/stores/playground';

const store = usePlaygroundStore();
const fromDialect = computed({
  get: () => store.transpileFrom,
  set: (value) => {
    store.transpileFrom = value; store.persist();
  },
});
const toDialect = computed({
  get: () => store.transpileTo,
  set: (value) => {
    store.transpileTo = value; store.persist();
  },
});
const sqlInput = computed({
  get: () => store.transpileInput,
  set: (value) => {
    store.transpileInput = value; store.persist();
  },
});

const sqlOutput = ref('');
const error = ref('');
const copied = ref(false);

function stripAnsi (text: string): string {
  return text.replace(/\x1b\[[0-9;]*m/g, '');
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
  } catch (error_) {
    sqlOutput.value = '';
    error.value = stripAnsi(error_ instanceof Error ? error_.message : String(error_));
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
