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
          class="bg-white rounded-sm"
        />
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
  breadcrumb?: Crumb[];
}>();
</script>
