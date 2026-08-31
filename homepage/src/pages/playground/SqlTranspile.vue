<template>
  <div class="w-full">
    <div class="flex flex-col gap-4 md:flex-row">
      <div class="gui-primary-border-subtle flex min-w-0 flex-1 flex-col overflow-hidden rounded-xl border shadow-xs">
        <div class="gui-primary-border-subtle gui-primary-bg-subtle flex items-center justify-between border-b px-4 py-3">
          <div class="gap-sm flex items-center">
            <span class="gui-primary-fg text-2xs font-mono font-semibold tracking-widest uppercase">Input SQL</span>
            <GSelect
              v-model="fromDialect"
              :size="GSelectSize.Xs"
              :variant="GSelectVariant.Box"
              class="w-40 border-(--gui-primary-border-strong)"
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

      <div class="gui-primary-border-subtle flex min-w-0 flex-1 flex-col overflow-hidden rounded-xl border shadow-xs">
        <div class="gui-primary-border-subtle gui-primary-bg-subtle flex items-center justify-between border-b px-4 py-3">
          <div class="gap-sm flex items-center">
            <span class="gui-primary-fg text-2xs font-mono font-semibold tracking-widest uppercase">Output SQL</span>
            <GSelect
              v-model="toDialect"
              :size="GSelectSize.Xs"
              :variant="GSelectVariant.Box"
              class="w-40 border-(--gui-primary-border-strong)"
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
            class="flex gap-2 border-(--gui-primary-border-strong) bg-(--gui-primary-bg-subtle) p-3"
            @click="copyOutput"
          >
            <GIcon
              :name="GIconName.Copy"
              :size="12"
            />
            {{ copied ? 'Copied!' : 'Copy' }}
          </GButton>
        </div>
        <div :class="error ? 'rounded-md outline-1 outline-(--gui-danger-border)' : ''">
          <MonacoEditor
            :model-value="sqlOutput || error"
            read-only
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

function stripAnsi (text: string): string {
  return text.replace(/\x1b\[[0-9;]*m/g, '');
}

watch([
  sqlInput,
  fromDialect,
  toDialect,
], convert);
onMounted(convert);
</script>
