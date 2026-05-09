<template>
  <MainLayout :breadcrumb="navBreadcrumb">
    <main class="p-lg">
      <div>
        <h1 class="gui-neutral-fg mb-sm text-xl font-bold">
          sqlingo.js Playground
        </h1>
        <GTab
          :default="tab"
          @select="onTabSelect"
        >
          <template
            #tab-trigger="{
              tab: panelTab,
            }"
          >
            <GIcon
              v-if="panelTab.icon"
              :name="panelTab.icon"
            />
            <span class="text-sm">{{ panelTab.label }}</span>
          </template>
          <GTabPanel
            :name="Tab.Transpile"
            label="Transpile"
            :icon="GIconName.ArrowsLeftRight"
            class="p-3"
          >
            <p class="gui-neutral-fg-muted mb-sm text-sm/3">
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
            <p class="gui-neutral-fg-muted mb-sm text-sm/3">
              Paste CREATE TABLE SQL and get a <a
                href="https://dbml.dbdiagram.io/docs/"
                target="_blank"
                rel="noopener noreferrer"
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
  GIcon,
  GIconName,
} from '@hdnax/genuix';
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
