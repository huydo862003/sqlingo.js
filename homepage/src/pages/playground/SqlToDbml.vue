<template>
  <div class="w-full">
    <div class="flex flex-col gap-sm">
      <div class="flex flex-col rounded-md border gui-neutral-border overflow-hidden">
        <div class="flex items-center justify-between p-sm border-b gui-neutral-border gui-neutral-bg-subtle">
          <div class="flex items-center gap-sm">
            <span class="text-sm font-medium uppercase tracking-wide gui-neutral-fg-muted">SQL</span>
            <GSelect
              v-model="dialect"
              :size="GSelectSize.Xs"
              :variant="GSelectVariant.Box"
              class="w-40"
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

      <div class="flex flex-col rounded-md border gui-neutral-border overflow-hidden">
        <div class="flex items-center justify-between p-sm border-b gui-neutral-border gui-neutral-bg-subtle">
          <span class="text-sm font-medium uppercase tracking-wide gui-neutral-fg-muted">DBML</span>
          <GButton
            :prominence="GButtonProminence.Secondary"
            :size="GButtonSize.Xs"
            :disabled="!dbmlOutput"
            class="flex gap-2 p-3"
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
        <div :class="error ? 'outline-1 outline-(--gui-danger-border) rounded-md' : ''">
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
  GSelect, GSelectOption, GSelectSize, GSelectVariant,
  GButton, GButtonProminence, GButtonSize,
  GIcon, GIconName,
} from '@hdnax/genuix';
import {
  sqlToDbml,
} from '@/services/dbml';
import {
  DIALECTS,
} from '@/services/dialects';
import MonacoEditor from './MonacoEditor.vue';
import {
  usePlaygroundStore,
} from '@/stores/playground';

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
    const result = sqlToDbml(sqlInput.value, dialect.value || undefined);
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
