<template>
  <nav class="gui-primary-border-subtle gui-primary-bg sticky top-0 z-20 border-b backdrop-blur-[10px]">
    <div class="mx-auto flex h-14 max-w-[1080px] items-center gap-7 px-7">
      <RouterLink
        to="/"
        class="gui-neutral-fg flex items-center gap-2.5 no-underline"
      >
        <img
          :src="`${base}icon.svg`"
          alt="sqlingo.js"
          class="size-8 rounded-md"
        >
        <span class="gui-primary-fg text-sm font-semibold tracking-tight">sqlingo.js</span>
      </RouterLink>

      <GBreadcrumb v-if="breadcrumb.length">
        <GBreadcrumbItem
          v-for="crumb in breadcrumb"
          :key="crumb.label"
          as="a"
          :href="crumb.href"
        >
          {{ crumb.label }}
        </GBreadcrumbItem>
      </GBreadcrumb>

      <div class="flex-1" />

      <div class="flex items-center gap-1">
        <RouterLink
          to="/"
          class="gui-neutral-fg-muted rounded-[7px] px-3 py-1.5 text-sm font-medium no-underline"
        >
          Home
        </RouterLink>
        <RouterLink
          to="/playground/"
          class="gui-neutral-fg-muted rounded-[7px] px-3 py-1.5 text-sm font-medium no-underline"
        >
          Playground
        </RouterLink>
        <a
          href="./api-reference/"
          class="gui-neutral-fg-muted hidden rounded-[7px] px-3 py-1.5 text-sm font-medium no-underline sm:block"
        >
          API reference
        </a>
        <a
          href="https://github.com/huydo862003/sqlingo.js"
          target="_blank"
          rel="noopener noreferrer"
          class="gui-neutral-fg-muted hidden rounded-[7px] px-3 py-1.5 text-sm font-medium no-underline sm:block"
        >
          GitHub
        </a>
      </div>
    </div>
  </nav>
</template>

<script setup lang="ts">
import {
  RouterLink,
} from 'vue-router';
import {
  GBreadcrumb, GBreadcrumbItem,
} from '@hdnax/genuix';
const {
  breadcrumb = [],
} = defineProps<{
  /** Navigation breadcrumb items */
  breadcrumb?: Crumb[];
}>();

const base = import.meta.env.BASE_URL;

interface Crumb {
  label: string;
  href?: string;
}
</script>
