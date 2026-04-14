<template>
  <div
    ref="triggerEl"
    class="ds-wrap"
  >
    <button
      class="ds-trigger"
      @click.stop="toggle"
    >
      <span>{{ currentLabel }}</span>
      <PhCaretDown
        :size="9"
        :class="['ds-caret', { open }]"
      />
    </button>
    <Teleport to="body">
      <div
        v-if="open"
        ref="dropdownEl"
        class="ds-dropdown"
        :style="dropdownStyle"
      >
        <button
          v-if="allowAuto"
          class="ds-option"
          :class="{ active: modelValue === '' }"
          @click="select('')"
        >
          auto
        </button>
        <button
          v-for="d in DIALECTS"
          :key="d.value"
          class="ds-option"
          :class="{ active: modelValue === d.value }"
          @click="select(d.value)"
        >
          {{ d.label }}
        </button>
      </div>
    </Teleport>
  </div>
</template>

<script setup lang="ts">
import {
  ref, computed, onMounted, onUnmounted, nextTick,
} from 'vue';
import {
  PhCaretDown,
} from '@phosphor-icons/vue';
import {
  DIALECTS,
} from '../services/dialects';

// Module-level: only one dropdown can be open at a time
const activeInstance = ref<symbol | null>(null);
const instanceId = Symbol();

const props = withDefaults(defineProps<{
  modelValue: string;
  allowAuto?: boolean;
}>(), {
  allowAuto: false,
});

const emit = defineEmits<{
  'update:modelValue': [string];
}>();

const open = computed({
  get: () => activeInstance.value === instanceId,
  set: (v) => {
    activeInstance.value = v ? instanceId : null;
  },
});
const triggerEl = ref<HTMLElement | null>(null);
const dropdownEl = ref<HTMLElement | null>(null);
const dropdownStyle = ref<Record<string, string>>({});
const currentLabel = computed(() => {
  if (props.modelValue === '') return 'auto';
  return DIALECTS.find((d) => d.value === props.modelValue)?.label ?? props.modelValue;
});

function updatePos () {
  if (!triggerEl.value) return;
  const r = triggerEl.value.getBoundingClientRect();
  dropdownStyle.value = {
    top: `${r.bottom + 4}px`,
    right: `${window.innerWidth - r.right}px`,
    minWidth: `${Math.max(r.width, 120)}px`,
  };
}

async function toggle () {
  open.value = !open.value;
  if (open.value) {
    await nextTick();
    updatePos();
  }
}

function select (val: string) {
  emit('update:modelValue', val);
  open.value = false;
}

function onDocClick (e: MouseEvent) {
  if (!open.value) return;
  const t = e.target as Node;
  if (!triggerEl.value?.contains(t) && !dropdownEl.value?.contains(t)) {
    open.value = false;
  }
}

function onKeyDown (e: KeyboardEvent) {
  if (e.key === 'Escape' && open.value) open.value = false;
}

function onScroll () {
  if (open.value) open.value = false;
}

function onResize () {
  if (open.value) open.value = false;
}

onMounted(() => {
  document.addEventListener('click', onDocClick);
  document.addEventListener('keydown', onKeyDown);
  window.addEventListener('scroll', onScroll, {
    passive: true,
  });
  window.addEventListener('resize', onResize, {
    passive: true,
  });
});
onUnmounted(() => {
  document.removeEventListener('click', onDocClick);
  document.removeEventListener('keydown', onKeyDown);
  window.removeEventListener('scroll', onScroll);
  window.removeEventListener('resize', onResize);
});
</script>

<!-- unscoped: teleported dropdown lives on <body> -->
<style>
.ds-dropdown {
  position: fixed;
  background: var(--color-bg-subtle);
  border: 1px solid var(--color-border);
  border-radius: var(--radius-md);
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.5);
  z-index: 9999;
  overflow-y: auto;
  max-height: 280px;
  display: flex;
  flex-direction: column;
}

.ds-option {
  padding: 0.375rem 0.75rem;
  font-size: 0.75rem;
  text-align: left;
  cursor: pointer;
  background: none;
  border: none;
  color: var(--color-fg-muted);
  white-space: nowrap;
}

.ds-option:hover {
  background: color-mix(in srgb, var(--color-fg) 6%, transparent);
  color: var(--color-fg);
}

.ds-option.active {
  color: var(--color-accent);
}
</style>

<style scoped>
.ds-wrap {
  position: relative;
  display: inline-block;
}

.ds-trigger {
  display: flex;
  align-items: center;
  gap: 0.25rem;
  font-size: 0.75rem;
  color: var(--color-fg);
  background: transparent;
  border: 1px solid var(--color-border);
  border-radius: var(--radius-sm);
  padding: 0.25rem 0.5rem;
  cursor: pointer;
  white-space: nowrap;
  transition: border-color 0.15s;
}

.ds-trigger:hover {
  border-color: var(--color-accent);
}

.ds-caret {
  transition: transform 0.15s;
  color: var(--color-fg-muted);
}

.ds-caret.open {
  transform: rotate(180deg);
}
</style>
