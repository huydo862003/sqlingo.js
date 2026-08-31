<template>
  <div
    ref="containerElement"
    class="h-112 w-full"
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

const content = defineModel<string | undefined>();

const {
  language = 'sql',
  readOnly = false,
} = defineProps<{
  /** Language mode for syntax highlighting */
  language?: string;
  /** Whether the editor is read-only */
  readOnly?: boolean;
}>();

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

monaco.editor.defineTheme('sqlingo-light', {
  base: 'vs',
  inherit: true,
  rules: [],
  colors: {
    'editor.background': '#FFFFFF',
    'editor.lineHighlightBackground': resolveToken('--color-primary-2'),
    'editorLineNumber.foreground': resolveToken('--color-primary-7'),
    'editorLineNumber.activeForeground': resolveToken('--color-primary-9'),
    'editorGutter.background': '#FFFFFF',
    'editor.lineHighlightBorder': '#00000000',
  },
});

const containerElement = useTemplateRef('containerElement');
let editor: monaco.editor.IStandaloneCodeEditor | null = null;

onMounted(() => {
  if (!containerElement.value) return;
  editor = monaco.editor.create(containerElement.value, {
    value: content.value,
    language,
    theme: 'sqlingo-light',
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

watch(() => language, (newLanguage) => {
  const model = editor?.getModel();

  if (model) monaco.editor.setModelLanguage(model, newLanguage);
});

onBeforeUnmount(() => {
  editor?.dispose();
});
</script>
