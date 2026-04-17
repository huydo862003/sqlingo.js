import {
  existsSync,
  readFileSync,
  watch,
} from 'node:fs';
import path, {
  resolve,
} from 'node:path';
import tailwindcss from '@tailwindcss/vite';
import vue from '@vitejs/plugin-vue';
import {
  defineConfig,
} from 'vite';
import type {
  Plugin,
  ViteDevServer,
} from 'vite';

const __dirname = new URL('.', import.meta.url).pathname;
const apiJsonPath = new URL('../doc/api/api.json', import.meta.url).pathname;

function typedocVirtual (): Plugin {
  const VIRTUAL_ID = 'virtual:typedoc';
  const RESOLVED_ID = '\0' + VIRTUAL_ID;

  return {
    name: 'typedoc-virtual',
    resolveId (id) {
      if (id === VIRTUAL_ID) return RESOLVED_ID;
    },
    load (id) {
      if (id !== RESOLVED_ID) return;
      if (!existsSync(apiJsonPath)) return 'export default null';
      return `export default ${readFileSync(apiJsonPath, 'utf-8')}`;
    },
    configureServer (server: ViteDevServer) {
      watch(apiJsonPath, () => {
        const mod = server.moduleGraph.getModuleById(RESOLVED_ID);
        if (mod) server.moduleGraph.invalidateModule(mod);
        server.ws.send({
          type: 'full-reload',
        });
      });
    },
  };
}

export default defineConfig({
  base: '/sqlingo.js/',
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src/'),
    },
  },
  build: {
    outDir: '../doc',
    emptyOutDir: false,
    rollupOptions: {
      input: {
        main: resolve(__dirname, 'index.html'),
        apiRef: resolve(__dirname, 'api-reference/index.html'),
        playground: resolve(__dirname, 'playground/index.html'),
      },
      output: {
        manualChunks: {
          vue: [
            'vue',
          ],
          hljs: [
            'highlight.js/lib/core',
            'highlight.js/lib/languages/typescript',
          ],
        },
      },
    },
  },
  plugins: [
    vue(),
    tailwindcss(),
    typedocVirtual(),
  ],
});
