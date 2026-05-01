<template>
  <GNavbar class="nav">
    <template #left>
      <GNavBrand
        :as="RouterLink"
        to="/"
      >
        <img
          src="/icon.svg"
          alt="sqlingo.js logo"
          class="logo-icon"
        >
        <span>sqlingo.js</span>
      </GNavBrand>
      <GBreadcrumb
        v-if="breadcrumb.length"
        :items="breadcrumb"
        class="breadcrumb"
      />
    </template>
    <template #right>
      <GNavLink
        :as="RouterLink"
        to="/"
        :icon="GIconName.Home"
        label="Home"
      />
      <GNavLink
        :as="RouterLink"
        to="/api-reference/"
        :icon="GIconName.BookOpen"
        label="API reference"
      />
      <GNavLink
        :as="RouterLink"
        to="/playground/"
        :icon="GIconName.PlayCircle"
        label="Playground"
      />
      <GNavLink
        href="https://github.com/huydo862003/sqlingo.js"
        :icon="GIconName.GithubLogo"
        label="GitHub"
        target="_blank"
        rel="noopener"
      />
    </template>
  </GNavbar>
</template>

<script setup lang="ts">
import {
  RouterLink,
} from 'vue-router';
import {
  GNavbar, GNavBrand, GNavLink,
  GBreadcrumb,
  GIconName,
} from '@hdnax/genuix';

interface Crumb {
  label: string;
  href?: string;
}

const {
  breadcrumb = [],
} = defineProps<{ breadcrumb?: Crumb[] }>();
</script>

<style scoped>
@reference "../style.css";

.nav {
  position: sticky;
  top: 0;
  z-index: 50;
}

.logo-icon {
  height: var(--size-5);
  width: var(--size-5);
  border-radius: var(--radius-md);
  background: var(--gui-neutral-bg);
  border: 1px solid var(--gui-neutral-border-subtle);
  box-shadow: var(--shadow-xs);
}

.breadcrumb {
  font-size: var(--text-xs);
}
</style>
