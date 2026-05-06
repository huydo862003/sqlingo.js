import {
  createRouter, createWebHistory,
} from 'vue-router';
import HomePage from './pages/home/HomePage.vue';

export const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      component: HomePage,
    },
    {
      path: '/playground/',
      component: () => import('./pages/playground/PlaygroundPage.vue'),
    },
    {
      path: '/api-reference/',
      component: () => import('./pages/api/ApiPage.vue'),
    },
    {
      path: '/:pathMatch(.*)*',
      redirect: '/',
    },
  ],
});
