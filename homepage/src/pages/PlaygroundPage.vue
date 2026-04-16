<template>
  <NavBar :breadcrumb="navBreadcrumb" />

  <main class="main">
    <div class="page page-wide">
      <h1 class="title">
        sqlingo.js Playground
      </h1>
      <div class="tabs">
        <button
          class="tab"
          :class="{ active: tab === Tab.Transpile }"
          @click="tab = Tab.Transpile"
        >
          <PhArrowsLeftRight :size="13" />
          Transpile
        </button>
        <button
          class="tab"
          :class="{ active: tab === Tab.Dbml }"
          @click="tab = Tab.Dbml"
        >
          <PhDatabase :size="13" />
          SQL <PhArrowRight :size="11" /> DBML
        </button>
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
          >DBML</a> schema back. (Currently has a bug :))
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
  PhArrowsLeftRight, PhArrowRight, PhDatabase,
} from '@phosphor-icons/vue';
import NavBar from '../components/NavBar.vue';
import SqlToDbml from '../components/SqlToDbml.vue';
import SqlTranspile from '../components/SqlTranspile.vue';
import {
  usePlaygroundStore,
} from '../stores/playground';
import {
  useSeo,
} from '../composables/useSeo';

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

enum Tab {
  Transpile = 'transpile',
  Dbml = 'dbml',
}

const store = usePlaygroundStore();
const tab = computed({
  get: () => store.tab as Tab,
  set: (v) => {
    store.tab = v; store.persist();
  },
});
</script>

<style scoped>
@reference "../style.css";

.page {
  max-width: 52rem;
  margin: 0 auto;
  padding: 0 1.5rem;
}

.page-wide {
  max-width: 72rem;
}

.main {
  @apply pt-10 pb-24 px-4;
}

.title {
  @apply text-2xl font-bold mb-6;
}

.section-sub {
  @apply text-sm text-fg-muted mb-4;
}

.tabs {
  @apply flex gap-1 mb-5 border-b border-border;
}

.tab {
  @apply flex items-center gap-1 px-4 py-2 text-xs font-semibold text-fg-muted cursor-pointer border-b-2 border-transparent -mb-px;
  background: none;
  border-top: none;
  border-left: none;
  border-right: none;
  transition: color 0.15s, border-color 0.15s;
}
.tab:hover { @apply text-fg; }
.tab.active {
  @apply text-accent;
  border-bottom-color: var(--color-accent);
}
</style>
