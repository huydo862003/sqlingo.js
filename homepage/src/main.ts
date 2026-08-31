import {
  createApp,
} from 'vue';
import {
  createPinia,
} from 'pinia';
import {
  createHead,
} from '@unhead/vue/client';
import App from './App.vue';
import {
  router,
} from './router';
import './style.css';

const app = createApp(App);

app.use(createPinia());
app.use(createHead());
app.use(router);

app.mount('#app');

// Force light theme
document.documentElement.classList.remove('dark');
