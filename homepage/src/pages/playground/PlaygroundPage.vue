<template>
  <MainLayout>
    <main class="px-7">
      <div class="mt-8 mb-6">
        <h1 class="m-0 text-3xl font-bold tracking-tight">
          Playground
        </h1>
        <p class="gui-neutral-fg-muted mt-2 text-sm">
          Everything runs locally in your browser. Nothing is sent anywhere.
        </p>
        <div class="gui-primary-border mt-4 inline-flex rounded-[10px] border bg-(--color-primary-3) p-1">
          <button
            type="button"
            class="cursor-pointer rounded-[7px] border-none px-5 py-2.5 text-sm font-semibold"
            :class="tab === Tab.Transpile ? 'gui-primary-fg bg-white shadow-xs' : 'gui-primary-fg-muted bg-transparent'"
            @click="onSelectTranspile"
          >
            Transpile
          </button>
          <button
            type="button"
            class="cursor-pointer rounded-[7px] border-none px-5 py-2.5 text-sm font-semibold"
            :class="tab === Tab.Dbml ? 'gui-primary-fg bg-white shadow-xs' : 'gui-primary-fg-muted bg-transparent'"
            @click="onSelectDbml"
          >
            SQL to DBML
          </button>
        </div>
      </div>

      <div v-if="tab === Tab.Transpile">
        <SqlTranspile />
      </div>
      <div v-else>
        <p class="gui-neutral-fg-muted mb-3 text-sm">
          Paste CREATE TABLE SQL and get a
          <a
            href="https://dbml.dbdiagram.io/docs/"
            target="_blank"
            rel="noopener noreferrer"
            class="gui-primary-fg no-underline hover:underline"
          >DBML</a> schema back.
        </p>
        <SqlToDbml />
      </div>

      <div class="mt-8" />
    </main>
  </MainLayout>
</template>

<script setup lang="ts">
import {
  computed,
  ref,
} from 'vue';
import {
  useSeoMeta,
} from '@unhead/vue';
import SqlToDbml from './SqlToDbml.vue';
import SqlTranspile from './SqlTranspile.vue';
import MainLayout from '@/layout/main/MainLayout.vue';
import {
  Tab,
  usePlaygroundStore,
} from '@/stores/playground';

useSeoMeta({
  title: 'Playground: SQL Transpiler & SQL to DBML | sqlingo.js',
  ogTitle: 'Playground: SQL Transpiler & SQL to DBML | sqlingo.js',
  description: 'Try sqlingo.js in your browser. Convert between SQL dialects and DBML.',
  ogDescription: 'Try sqlingo.js in your browser. Convert between SQL dialects and DBML.',
});

const store = usePlaygroundStore();
const tab = ref(store.tab);

function onSelectDbml () {
  selectTab(Tab.Dbml);
}

function onSelectTranspile () {
  selectTab(Tab.Transpile);
}

function selectTab (mode: Tab) {
  tab.value = mode;
  store.tab = mode;
  store.persist();
}
</script>
