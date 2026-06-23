import path, {
  resolve,
} from 'node:path';
import tailwindcss from '@tailwindcss/vite';
import vue from '@vitejs/plugin-vue';
import {
  defineConfig,
} from 'vite';

const __dirname = new URL('.', import.meta.url).pathname;

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
        manualChunks (id) {
          if (id.includes('node_modules/vue/')) return 'vue';
          if (id.includes('highlight.js/')) return 'hljs';
        },
      },
    },
  },
  plugins: [
    vue(),
    tailwindcss(),
  ],
});
