<template>
  <div class="flex min-h-screen flex-col overflow-hidden">
    <NavBar :breadcrumb="breadcrumb" />
    <div class="flex-1">
      <slot />
    </div>
    <footer class="gui-primary-border-subtle gui-primary-bg-hover shrink-0 border-t">
      <div class="flex flex-wrap items-center gap-3 px-7 py-6">
        <span class="gui-neutral-fg-muted text-sm">
          sqlingo.js is
          <a
            href="https://github.com/huydo862003/sqlingo.js/blob/master/COPYRIGHT_NOTICE"
            target="_blank"
            rel="noopener noreferrer"
            class="gui-primary-fg"
          >MIT licensed</a>.
          Based on
          <a
            href="https://github.com/tobymao/sqlglot"
            target="_blank"
            rel="noopener noreferrer"
            class="gui-primary-fg"
          >SQLGlot</a> by Toby Mao, also MIT.
        </span>
        <div class="flex-1" />
        <a
          href="https://github.com/huydo862003/sqlingo.js/issues"
          target="_blank"
          rel="noopener noreferrer"
          class="gui-primary-fg text-sm"
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
  /** Navigation breadcrumb items */
  breadcrumb?: Crumb[];
}>();
</script>
