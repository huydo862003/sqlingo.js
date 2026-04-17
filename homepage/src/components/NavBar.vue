<template>
  <nav class="nav">
    <div class="inner">
      <RouterLink
        to="/"
        class="logo"
      >
        <img
          src="/icon.svg"
          alt="sqlingo.js logo"
          class="logo-icon"
        >
        <span class="logo-text">sqlingo.js</span>
      </RouterLink>
      <div
        v-if="breadcrumb.length"
        class="breadcrumb"
      >
        <!-- first crumb: hidden on small screens when fully collapsed -->
        <span
          class="crumb crumb-first"
        >
          <span class="crumb-sep">/</span>
          <a
            v-if="breadcrumb[0].href"
            :href="breadcrumb[0].href"
            class="crumb-link"
          >{{ breadcrumb[0].label }}</a>
          <span
            v-else
            class="crumb-text"
          >{{ breadcrumb[0].label }}</span>
        </span>

        <!-- collapsed middle crumbs (or all crumbs on small screens) -->
        <span
          v-if="collapsedCrumbs.length || breadcrumb.length > 1"
          class="crumb crumb-ellipsis-wrap"
          :class="{ 'crumb-mobile-only': !collapsedCrumbs.length }"
        >
          <span class="crumb-sep">/</span>
          <button
            ref="ellipsisBtn"
            class="crumb-ellipsis"
            @click="dropdownOpen = !dropdownOpen"
          >
            &hellip;
          </button>
          <div
            v-if="dropdownOpen"
            class="crumb-dropdown"
          >
            <!-- desktop: middle crumbs only -->
            <a
              v-for="(crumb, i) in collapsedCrumbs"
              :key="'d-' + i"
              :href="crumb.href ?? '#'"
              class="crumb-dropdown-item hidden md:flex"
            >{{ crumb.label }}</a>
            <!-- mobile: all except last -->
            <a
              v-for="(crumb, i) in breadcrumb.slice(0, -1)"
              :key="'m-' + i"
              :href="crumb.href ?? '#'"
              class="crumb-dropdown-item flex md:hidden"
            >{{ crumb.label }}</a>
          </div>
        </span>

        <!-- last crumb always visible (only if more than 1 crumb total) -->
        <span
          v-if="breadcrumb.length > 1"
          class="crumb crumb-last"
        >
          <span class="crumb-sep">/</span>
          <a
            v-if="breadcrumb[breadcrumb.length - 1].href"
            :href="breadcrumb[breadcrumb.length - 1].href"
            class="crumb-link"
          >{{ breadcrumb[breadcrumb.length - 1].label }}</a>
          <span
            v-else
            class="crumb-text"
          >{{ breadcrumb[breadcrumb.length - 1].label }}</span>
        </span>
      </div>
      <div class="links">
        <RouterLink
          to="/"
          class="link"
          data-tooltip="Home"
        >
          <PhHouse :size="16" />
          <span class="link-label">Home</span>
        </RouterLink>
        <RouterLink
          to="/api-reference/"
          class="link"
          data-tooltip="API reference"
        >
          <PhBookOpen :size="16" />
          <span class="link-label">API reference</span>
        </RouterLink>
        <RouterLink
          to="/playground/"
          class="link"
          data-tooltip="Playground"
        >
          <PhPlayCircle :size="16" />
          <span class="link-label">Playground</span>
        </RouterLink>
        <a
          href="https://github.com/huydo862003/sqlingo.js"
          class="link"
          target="_blank"
          rel="noopener"
          data-tooltip="GitHub"
        >
          <PhGithubLogo :size="16" />
          <span class="link-label">GitHub</span>
        </a>
      </div>
    </div>
  </nav>
</template>

<script setup lang="ts">
import {
  computed, ref, onMounted, onUnmounted,
} from 'vue';
import {
  PhHouse, PhBookOpen, PhPlayCircle, PhGithubLogo,
} from '@phosphor-icons/vue';

interface Crumb {label: string;
  href?: string;}

const {
  breadcrumb = [],
} = defineProps<{breadcrumb?: Crumb[]}>();

const dropdownOpen = ref(false);
const ellipsisBtn = ref<HTMLElement | null>(null);
// middle crumbs (everything except first and last), shown in dropdown on desktop
const collapsedCrumbs = computed(() =>
  2 < breadcrumb.length
    ? breadcrumb.slice(1, -1)
    : []);

function onDocClick (e: MouseEvent) {
  if (ellipsisBtn.value && !ellipsisBtn.value.contains(e.target as Node)) {
    dropdownOpen.value = false;
  }
}

onMounted(() => document.addEventListener('click', onDocClick, true));
onUnmounted(() => document.removeEventListener('click', onDocClick, true));
</script>

<style scoped>
@reference "../style.css";

.nav {
  @apply sticky top-0 z-50 border-b border-border;
  background: #111118;
}

.inner {
  max-width: 64rem;
  margin: 0 auto;
  padding: 0 1.5rem;
  height: 3rem;
  @apply flex items-center;
}

.logo {
  @apply flex items-center gap-2 no-underline;
}
.logo:hover { @apply no-underline opacity-80; }

.logo-icon {
  @apply h-8 w-8 rounded-[var(--radius-md)];
  background: #fff;
  border: 1px solid rgba(255,255,255,0.15);
  box-shadow: 0 1px 4px rgba(0,0,0,0.3);
}

.logo-text {
  @apply text-sm font-bold;
  color: #e8e8ec;
}

.breadcrumb {
  @apply flex items-center ml-4;
}

/* hide the ... button on desktop when there are no real middle crumbs */
.crumb-mobile-only {
  display: none;
}
@media (max-width: 1024px) {
  .crumb-mobile-only {
    display: flex;
  }
}

.crumb {
  @apply flex items-center;
}

.crumb-ellipsis-wrap {
  position: relative;
}

.crumb-sep {
  @apply mx-1.5 text-xs;
  color: #404050;
}

.crumb-link, .crumb-text {
  @apply text-xs no-underline;
  color: #9090a0;
}
.crumb-link:hover { color: #e8e8ec; @apply no-underline; }
.crumb-text { color: #c8c8d8; }

.crumb-ellipsis {
  @apply text-xs px-1.5 py-0.5 rounded cursor-pointer border-0;
  background: rgba(255,255,255,0.07);
  color: #9090a0;
  line-height: 1;
  transition: background 0.15s, color 0.15s;
}
.crumb-ellipsis:hover {
  background: rgba(255,255,255,0.13);
  color: #e8e8ec;
}

.crumb-dropdown {
  position: absolute;
  top: calc(100% + 6px);
  left: 0;
  min-width: 10rem;
  @apply rounded-[var(--radius-md)] border border-border flex flex-col py-1 z-50;
  background: #1a1a24;
  box-shadow: 0 4px 16px rgba(0,0,0,0.4);
}

.crumb-dropdown-item {
  @apply px-3 py-1.5 text-xs no-underline;
  color: #9090a0;
  transition: background 0.1s, color 0.1s;
}
.crumb-dropdown-item:hover {
  background: rgba(255,255,255,0.07);
  color: #e8e8ec;
  @apply no-underline;
}

.links {
  @apply ml-auto flex items-center gap-1;
}

.link {
  @apply flex items-center gap-1.5 px-3 py-1.5 rounded-[var(--radius-md)] text-xs no-underline;
  color: #9090a0;
  transition: color 0.15s, background 0.15s;
  position: relative;
}
.link:hover {
  color: #e8e8ec;
  background: rgba(255,255,255,0.07);
  @apply no-underline;
}

.badge {
  @apply text-[10px] font-bold px-1.5 py-0.5 rounded-full;
  background: rgba(255,255,255,0.1);
  color: #9090a0;
}

/* collapsed nav on small screens */
@media (max-width: 1024px) {

  /* breadcrumb: hide first crumb and last crumb text, show only ... + last */
  .crumb-first { display: none; }
  /* the ... button is always shown on mobile (crumb-mobile-only hidden on desktop) */

  .link {
    @apply px-2 py-1.5;
  }
  .link-label { display: none; }
  .badge { display: none; }

  /* tooltip */
  .link::after {
    content: attr(data-tooltip);
    position: absolute;
    top: calc(100% + 6px);
    left: 50%;
    transform: translateX(-50%);
    white-space: nowrap;
    @apply text-xs px-2 py-1 rounded-[var(--radius-sm)] border border-border;
    background: #1a1a24;
    color: #c8c8d8;
    box-shadow: 0 4px 12px rgba(0,0,0,0.4);
    pointer-events: none;
    opacity: 0;
    transition: opacity 0.15s;
    z-index: 60;
  }
  .link:hover::after {
    opacity: 1;
  }
}
</style>
