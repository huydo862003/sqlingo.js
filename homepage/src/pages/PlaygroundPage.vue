<template>
  <NavBar :breadcrumb="navBreadcrumb" />

  <main class="main">
    <div class="page page-wide">
      <h1 class="title">
        sqlingo.js Playground
      </h1>
      <div class="tabs">
        <GButton
          :prominence="tab === Tab.Transpile ? ButtonProminence.Primary : ButtonProminence.Ghost"
          :size="ButtonSize.Sm"
          @click="tab = Tab.Transpile"
        >
          <GIcon
            :name="GIconName.ArrowsLeftRight"
            :size="13"
          />
          Transpile
        </GButton>
        <GButton
          :prominence="tab === Tab.Dbml ? ButtonProminence.Primary : ButtonProminence.Ghost"
          :size="ButtonSize.Sm"
          @click="tab = Tab.Dbml"
        >
          <GIcon
            :name="GIconName.Database"
            :size="13"
          />
          SQL
          <GIcon
            :name="GIconName.ArrowRight"
            :size="11"
          />
          DBML
        </GButton>
      </div>
      <div v-show="tab === Tab.Transpile">
        <p class="section-sub">
          Transpile between SQL dialects.
        </p>
        <SqlTranspile />
      </div>
      <div v-show="tab === Tab.Dbml">
        <p class="section-sub">
          Paste CREATE TABLE SQL and get a <a
            href="https://dbml.dbdiagram.io/docs/"
            target="_blank"
            rel="noopener"
          >DBML</a> schema back.
        </p>
        <SqlToDbml />
      </div>
    </div>
  </main>
</template>

<script setup lang="ts">
import {
  computed,
} from 'vue';
import {
  GButton, ButtonProminence, ButtonSize,
  GIcon, GIconName,
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
const tab = computed({
  get: () => store.tab,
  set: (v) => {
    store.tab = v;
    store.persist();
  },
});
</script>

<style scoped>
@reference "../style.css";

.page {
  max-width: 52rem;
  margin: 0 auto;
  padding: 0 var(--spacing-lg);
}

.page-wide {
  max-width: 72rem;
}

.main {
  padding-top: var(--spacing-xl);
  padding-bottom: var(--spacing-3xl);
  padding-left: var(--spacing-md);
  padding-right: var(--spacing-md);
}

.title {
  font-size: var(--text-2xl);
  font-weight: var(--font-weight-bold);
  margin-bottom: var(--spacing-lg);
  color: var(--gui-neutral-fg);
}

.section-sub {
  font-size: var(--text-sm);
  color: var(--gui-neutral-fg-muted);
  margin-bottom: var(--spacing-md);
}

.tabs {
  display: flex;
  gap: var(--spacing-xs);
  margin-bottom: var(--spacing-lg);
  border-bottom: 1px solid var(--gui-neutral-border);
  padding-bottom: var(--spacing-sm);
}
</style>
