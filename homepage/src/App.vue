<template>
  <RouterView v-slot="{ Component }">
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

<style>
.fade-enter-active,
.fade-leave-active {
  transition: opacity 0.08s ease;
}

.fade-enter-from,
.fade-leave-to {
  opacity: 0;
}
</style>
