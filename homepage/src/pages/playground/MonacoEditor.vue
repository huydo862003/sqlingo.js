<template>
  <div
    ref="containerElement"
    class="h-64 w-full"
  />
</template>

<script setup lang="ts">
import {
  onMounted, onBeforeUnmount, watch,
  useTemplateRef,
} from 'vue';
// eslint-disable-next-line import/no-namespace
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
  return getComputedStyle(document.documentElement).getPropertyValue(token)
    .trim();
}

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

const content = defineModel<string | undefined>();

const {
  language = 'sql',
  readOnly = false,
} = defineProps<{
  language?: string;
  readOnly?: boolean;
}>();

const containerElement = useTemplateRef('containerElement');
let editor: monaco.editor.IStandaloneCodeEditor | null = null;

onMounted(() => {
  if (!containerElement.value) return;
  editor = monaco.editor.create(containerElement.value, {
    value: content.value,
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
    content.value = editor?.getValue();
  });
});

watch(content, (newValue) => {
  if (editor && editor.getValue() !== newValue) editor.setValue(newValue ?? '');
});

watch(() => language, (newLang) => {
  const model = editor?.getModel();
  if (model) monaco.editor.setModelLanguage(model, newLang);
});

onBeforeUnmount(() => {
  editor?.dispose();
});
</script>
