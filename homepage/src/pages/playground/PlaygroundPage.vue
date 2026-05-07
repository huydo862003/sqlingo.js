<template>
  <MainLayout :breadcrumb="navBreadcrumb">
    <main class="p-lg">
      <div>
        <h1 class="text-xl font-bold gui-neutral-fg mb-sm">
          sqlingo.js Playground
        </h1>
        <GTab
          :default="tab"
          @select="onTabSelect"
        >
          <GTabPanel
            :name="Tab.Transpile"
            label="Transpile"
            :icon="GIconName.ArrowsLeftRight"
            class="p-3"
          >
            <p class="text-sm gui-neutral-fg-muted mb-sm leading-3">
              Transpile between SQL dialects.
            </p>
            <SqlTranspile />
          </GTabPanel>
          <GTabPanel
            :name="Tab.Dbml"
            label="SQL to DBML"
            :icon="GIconName.Database"
            class="p-3"
          >
            <p class="text-sm gui-neutral-fg-muted mb-sm leading-3">
              Paste CREATE TABLE SQL and get a <a
                href="https://dbml.dbdiagram.io/docs/"
                target="_blank"
                rel="noopener"
                class="gui-info-fg no-underline hover:underline"
              >DBML</a> schema back.
            </p>
            <SqlToDbml />
          </GTabPanel>
        </GTab>
      </div>
    </main>
  </MainLayout>
</template>

<script setup lang="ts">
import {
  computed,
} from 'vue';
import {
  GTab,
  GTabPanel,
  GIconName,
} from '@hdnax/genuix';
import MainLayout from '@/layout/main/MainLayout.vue';
import SqlToDbml from './SqlToDbml.vue';
import SqlTranspile from './SqlTranspile.vue';
import {
  Tab,
  usePlaygroundStore,
} from '@/stores/playground';
import {
  useSeoMeta,
} from '@unhead/vue';

useSeoMeta({
  title: 'Playground: SQL Transpiler & SQL to DBML | sqlingo.js',
  ogTitle: 'Playground: SQL Transpiler & SQL to DBML | sqlingo.js',
  description: 'Try sqlingo.js in your browser. Convert between SQL dialects and DBML.',
  ogDescription: 'Try sqlingo.js in your browser. Convert between SQL dialects and DBML.',
});

const base = import.meta.env.BASE_URL;

const navBreadcrumb = computed(() => {
  const crumbs: Array<{
    label: string;
    href?: string;
  }> = [
    {
      label: 'Playground',
      href: `${base}playground/`,
    },
  ];
  return crumbs;
});

const store = usePlaygroundStore();
const tab = store.tab;

function onTabSelect (name: string) {
  store.tab = name as Tab;
  store.persist();
}
</script>
