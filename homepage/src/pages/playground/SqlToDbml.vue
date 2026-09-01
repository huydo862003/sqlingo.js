<template>
  <div class="w-full">
    <div class="grid grid-cols-1 gap-4 lg:grid-cols-2">
      <div class="gui-primary-border-subtle flex min-w-0 flex-col overflow-hidden rounded-xl border shadow-xs">
        <div class="gui-primary-border-subtle gui-primary-bg-subtle flex items-center justify-between border-b px-4 py-3">
          <div class="gap-sm flex items-center">
            <span class="gui-primary-fg text-2xs font-mono font-semibold tracking-widest uppercase">SQL</span>
            <GSelect
              v-model="dialect"
              :size="GSelectSize.Xs"
              :variant="GSelectVariant.Box"
              class="w-40 border-(--gui-primary-border-strong)"
              placeholder="auto"
              close-on-select
            >
              <GSelectOption
                value=""
                label="auto"
              />
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

      <div class="gui-primary-border-subtle flex min-w-0 flex-col overflow-hidden rounded-xl border shadow-xs">
        <div class="gui-primary-border-subtle gui-primary-bg-subtle flex items-center justify-between border-b px-4 py-3">
          <span class="gui-primary-fg text-2xs font-mono font-semibold tracking-widest uppercase">DBML</span>
          <GButton
            :prominence="GButtonProminence.Secondary"
            :size="GButtonSize.Xs"
            :disabled="!dbmlOutput"
            class="flex gap-2 border-(--gui-primary-border-strong) bg-(--gui-primary-bg-subtle) p-3"
            @click="copyDbml"
          >
            <GIcon
              :name="GIconName.Copy"
            />
            <span>
              {{ copied ? 'Copied!' : 'Copy' }}
            </span>
          </GButton>
        </div>
        <div :class="error ? 'rounded-md outline-1 outline-(--gui-danger-border)' : ''">
          <MonacoEditor
            :model-value="dbmlOutput || error"
            read-only
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
  GSelect, GSelectOption, GSelectSize, GSelectVariant,
  GButton, GButtonProminence, GButtonSize,
  GIcon, GIconName,
} from '@hdnax/genuix';
import MonacoEditor from './MonacoEditor.vue';
import {
  sqlToDbml,
} from '@/services/dbml';
import {
  DIALECTS,
} from '@/services/dialects';
import {
  usePlaygroundStore,
} from '@/stores/playground';

const store = usePlaygroundStore();
const sqlInput = computed({
  get: () => store.dbmlInput,
  set: (value) => {
    store.dbmlInput = value; store.persist();
  },
});
const dialect = computed({
  get: () => store.dbmlDialect,
  set: (value) => {
    store.dbmlDialect = value; store.persist();
  },
});
const dbmlOutput = ref('');
const error = ref('');
const copied = ref(false);

function convert () {
  if (!sqlInput.value.trim()) {
    dbmlOutput.value = '';
    error.value = '';

    return;
  }
  try {
    const result = sqlToDbml(sqlInput.value, dialect.value || undefined);

    dbmlOutput.value = result.dbml || '';
    error.value = result.dbml ? '' : 'No CREATE TABLE statements found.';
  } catch (error_) {
    dbmlOutput.value = '';
    error.value = stripAnsi(error_ instanceof Error ? error_.message : String(error_));
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

function stripAnsi (text: string): string {
  return text.replace(/\x1b\[[0-9;]*m/g, '');
}

watch([
  sqlInput,
  dialect,
], convert);

onMounted(convert);
</script>
