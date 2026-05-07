<template>
  <div class="page-layout">
    <NavBar :breadcrumb="breadcrumb" />
    <slot />
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

<style scoped>
.page-layout {
  display: flex;
  flex-direction: column;
  min-height: 100vh;
  overflow: hidden;
}
</style>
