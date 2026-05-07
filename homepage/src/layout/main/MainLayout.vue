<template>
  <div class="flex flex-col min-h-screen overflow-hidden">
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
