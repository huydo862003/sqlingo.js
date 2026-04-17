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

monaco.editor.defineTheme('sqlingo-dark', {
  base: 'vs-dark',
  inherit: true,
  rules: [
  ],
  colors: {
    'editor.background': '#0d0d14',
    'editor.lineHighlightBackground': '#13131e',
    'editorLineNumber.foreground': '#4a4a6a',
    'editorLineNumber.activeForeground': '#8080a8',
  },
});

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
    fontFamily: '"JetBrains Mono", monospace',
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
@reference '../../style.css';

.monaco-wrap {
  @apply h-64 w-full;
}
</style>
