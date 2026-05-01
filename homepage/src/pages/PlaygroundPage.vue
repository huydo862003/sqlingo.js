<template>
  <NavBar :breadcrumb="navBreadcrumb" />

  <main class="main">
    <div>
      <h1 class="title">
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
          <p class="section-sub">
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
          <p class="section-sub">
            Paste CREATE TABLE SQL and get a <a
              href="https://dbml.dbdiagram.io/docs/"
              target="_blank"
              rel="noopener"
            >DBML</a> schema back.
          </p>
          <SqlToDbml />
        </GTabPanel>
      </GTab>
    </div>
  </main>
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
import NavBar from '@/components/NavBar.vue';
import SqlToDbml from '@/components/playground/SqlToDbml.vue';
import SqlTranspile from '@/components/playground/SqlTranspile.vue';
import {
  Tab,
  usePlaygroundStore,
} from '@/stores/playground';
import {
  useSeo,
} from '@/composables/useSeo';

useSeo({
  title: 'Playground: SQL Transpiler & SQL to DBML',
  description: 'Try sqlingo.js in your browser. Convert between SQL dialects and DBML.',
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

<style scoped>
@reference '../style.css';

.main {
  padding: var(--spacing-lg);
}

.title {
  font-size: var(--text-xl);
  font-weight: 700;
  color: var(--gui-neutral-fg);
  margin-bottom: var(--spacing-sm);
}

.section-sub {
  font-size: var(--text-sm);
  color: var(--gui-neutral-fg-muted);
  margin-bottom: var(--spacing-sm);
  line-height: var(--leading-3);
}

.section-sub a {
  color: var(--gui-info-fg);
  text-decoration: none;
}

.section-sub a:hover {
  text-decoration: underline;
}
</style>
