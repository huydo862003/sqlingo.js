/// <reference types="vite/client" />

declare module '*.vue' {
  import type {
    DefineComponent,
  } from 'vue';

  const component: DefineComponent;

  export default component;
}

interface ReflectionNode {
  kind: number;
  children?: ReflectionNode[];
}

declare module 'virtual:typedoc' {
  const data: ReflectionNode | null;

  export default data;
}
