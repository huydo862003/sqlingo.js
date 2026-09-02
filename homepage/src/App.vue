<template>
  <RouterView
    v-slot="{
      Component,
    }"
  >
    <Transition
      name="fade"
      mode="out-in"
    >
      <component :is="Component" />
    </Transition>
  </RouterView>
</template>

<script setup lang="ts">
import {
  RouterView,
} from 'vue-router';
</script>

<style scoped>
:global(.fade-enter-active),
:global(.fade-leave-active) {
  transition: opacity 0.08s ease;
}

:global(.fade-enter-from),
:global(.fade-leave-to) {
  opacity: 0;
}
</style>
