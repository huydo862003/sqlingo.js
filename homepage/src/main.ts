import {
  createApp,
} from 'vue';
import {
  createPinia,
} from 'pinia';
import {
  Dropdown, Menu, Tooltip, vClosePopper, vTooltip,
} from 'floating-vue';
import 'floating-vue/style.css';
import '@hdnax/genuix/style.css';
import App from './App.vue';
import {
  router,
} from './router';
import './style.css';

const app = createApp(App);
app.use(createPinia());
app.use(router);

app.directive('tooltip', vTooltip);
app.directive('close-popper', vClosePopper);
app.component('VDropdown', Dropdown);
app.directive('VTooltip', Tooltip);
app.directive('VMenu', Menu);

app.mount('#app');
