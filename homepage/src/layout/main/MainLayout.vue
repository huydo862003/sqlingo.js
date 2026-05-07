<template>
  <div class="flex flex-col min-h-screen overflow-hidden">
    <NavBar :breadcrumb="breadcrumb" />
    <slot />
    <footer class="gui-neutral-bg-subtle text-sm p-5 border-t gui-neutral-border gui-neutral-fg-muted">
      <div class="flex justify-between items-end">
        <div class="w-96">
          <span>
            sqlingo.js is licensed under the
            <a
              href="https://github.com/huydo862003/sqlingo.js/blob/master/COPYRIGHT_NOTICE"
              target="_blank"
              rel="noopener"
            >MIT License</a>.
          </span>
          <span>
            Based on <a
              href="https://github.com/tobymao/sqlglot"
              target="_blank"
              rel="noopener"
            >SQLGlot</a> by Toby Mao, also MIT.
          </span>
        </div>
        <a
          href="https://github.com/huydo862003/sqlingo.js/issues"
          target="_blank"
          rel="noopener"
        >Report an issue</a>
      </div>
    </footer>
  </div>
</template>

<script setup lang="ts">
import NavBar from './NavBar.vue';

interface Crumb {
  label: string;
  href?: string;
}

const {
  breadcrumb = [],
} = defineProps<{
  breadcrumb?: Crumb[];
}>();
</script>
