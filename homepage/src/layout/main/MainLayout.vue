<template>
  <div class="flex min-h-screen flex-col overflow-hidden">
    <NavBar :breadcrumb="breadcrumb" />
    <slot />
    <footer class="gui-neutral-bg-subtle gui-neutral-border gui-neutral-fg-muted border-t p-5 text-sm">
      <div class="flex items-end justify-between">
        <div class="w-96">
          <span>
            sqlingo.js is licensed under the
            <a
              href="https://github.com/huydo862003/sqlingo.js/blob/master/COPYRIGHT_NOTICE"
              target="_blank"
              rel="noopener noreferrer"
            >MIT License</a>.
          </span>
          <span>
            Based on <a
              href="https://github.com/tobymao/sqlglot"
              target="_blank"
              rel="noopener noreferrer"
            >SQLGlot</a> by Toby Mao, also MIT.
          </span>
        </div>
        <a
          href="https://github.com/huydo862003/sqlingo.js/issues"
          target="_blank"
          rel="noopener noreferrer"
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
  /** Navigation breadcrumb items to display */
  breadcrumb?: Crumb[];
}>();
</script>
