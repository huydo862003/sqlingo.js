<template>
  <GNavbar class="nav">
    <template #left>
      <GNavBrand
        :as="RouterLink"
        to="/"
      >
        <GLogo
          src="/sqlingo.js/icon.svg"
          alt="sqlingo.js logo"
          :size="GLogoSize.Xs"
          class="rounded-sm bg-white"
        />
        <span>sqlingo.js</span>
      </GNavBrand>
      <GBreadcrumb
        v-if="breadcrumb.length"
        class="breadcrumb"
      >
        <GBreadcrumbItem
          v-for="crumb in breadcrumb"
          :key="crumb.label"
          as="a"
          :href="crumb.href"
        >
          {{ crumb.label }}
        </GBreadcrumbItem>
      </GBreadcrumb>
    </template>
    <template #right>
      <GNavLink
        :as="RouterLink"
        to="/"
        :icon="GIconName.Home"
        label="Home"
        class="text-sm"
      />
      <GNavLink
        as="a"
        href="./api-reference/"
        :icon="GIconName.BookOpen"
        label="API reference"
        class="text-sm"
      />
      <GNavLink
        :as="RouterLink"
        to="/playground/"
        :icon="GIconName.PlayCircle"
        label="Playground"
        class="text-sm"
      />
      <GNavLink
        href="https://github.com/huydo862003/sqlingo.js"
        :icon="GIconName.GithubLogo"
        label="GitHub"
        target="_blank"
        rel="noopener noreferrer"
        class="text-sm"
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
  GBreadcrumb, GBreadcrumbItem,
  GIconName,
  GLogo,
  GLogoSize,
} from '@hdnax/genuix';

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
