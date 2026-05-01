<template>
  <div
    ref="containerEl"
    class="monaco-wrap"
  />
</template>

<script setup lang="ts">
import {
  ref, onMounted, onBeforeUnmount, watch,
} from 'vue';
import * as monaco from 'monaco-editor';
import EditorWorker from 'monaco-editor/esm/vs/editor/editor.worker?worker';
import {
  dbmlMonarchTokensProvider,
} from '@dbml/parse';

self.MonacoEnvironment = {
  getWorker: () => new EditorWorker(),
};

monaco.languages.register({
  id: 'dbml',
});
monaco.languages.setMonarchTokensProvider('dbml', dbmlMonarchTokensProvider as monaco.languages.IMonarchLanguage);

function resolveToken (token: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(token).trim();
}

function registerEditorTheme () {
  monaco.editor.defineTheme('sqlingo-dark', {
    base: 'vs-dark',
    inherit: true,
    rules: [],
    colors: {
      'editor.background': resolveToken('--color-neutral-1'),
      'editor.lineHighlightBackground': resolveToken('--color-neutral-2'),
      'editorLineNumber.foreground': resolveToken('--color-neutral-7'),
      'editorLineNumber.activeForeground': resolveToken('--color-neutral-9'),
    },
  });
}

registerEditorTheme();

const {
  language = 'sql',
  readOnly = false,
  modelValue,
} = defineProps<{
  modelValue: string;
  language?: string;
  readOnly?: boolean;
}>();

const emit = defineEmits<{
  'update:modelValue': [value: string];
}>();

const containerEl = ref<HTMLElement | null>(null);
let editor: monaco.editor.IStandaloneCodeEditor | null = null;

onMounted(() => {
  if (!containerEl.value) return;
  editor = monaco.editor.create(containerEl.value, {
    value: modelValue,
    language,
    theme: 'sqlingo-dark',
    minimap: {
      enabled: false,
    },
    scrollBeyondLastLine: false,
    readOnly,
    fontSize: 14,
    fontFamily: 'var(--font-mono)',
    lineNumbers: 'on',
    folding: false,
    guides: {
      indentation: false,
    },
    overviewRulerLanes: 0,
    lineDecorationsWidth: 8,
    lineNumbersMinChars: 3,
    padding: {
      top: 12,
      bottom: 12,
    },
    automaticLayout: true,
    fixedOverflowWidgets: true,
    scrollbar: {
      verticalScrollbarSize: 4,
      horizontalScrollbarSize: 4,
    },
    renderLineHighlight: 'none',
  });
  editor.onDidChangeModelContent(() => {
    emit('update:modelValue', editor!.getValue());
  });
});

watch(() => modelValue, (newVal) => {
  if (editor && editor.getValue() !== newVal) editor.setValue(newVal);
});

watch(() => language, (newLang) => {
  const model = editor?.getModel();
  if (model) monaco.editor.setModelLanguage(model, newLang);
});

onBeforeUnmount(() => {
  editor?.dispose();
});
</script>

<style scoped>
.monaco-wrap {
  height: 16rem;
  width: 100%;
}
</style>
