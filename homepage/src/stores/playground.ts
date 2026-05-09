import {
  defineStore,
} from 'pinia';
import {
  ref,
} from 'vue';

const STORAGE_KEY = 'pg:state';

const DEFAULT_TRANSPILE_INPUT = `SELECT
  users.id,
  users.name,
  COUNT(posts.id) AS post_count
FROM users
LEFT JOIN posts ON posts.user_id = users.id
WHERE users.created_at >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
GROUP BY users.id, users.name
HAVING COUNT(posts.id) > 0
ORDER BY post_count DESC
LIMIT 10;`;

const DEFAULT_DBML_INPUT = `CREATE TABLE users (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(100) NOT NULL,
  email VARCHAR(255) NOT NULL UNIQUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE posts (
  id INT PRIMARY KEY AUTO_INCREMENT,
  user_id INT NOT NULL,
  title VARCHAR(255) NOT NULL,
  body TEXT,
  FOREIGN KEY (user_id) REFERENCES users(id)
);`;

export enum Tab {
  Transpile = 'transpile',
  Dbml = 'dbml',
}

interface PersistedState {
  tab: Tab;
  transpileFrom: string;
  transpileTo: string;
  transpileInput: string;
  dbmlDialect: string;
  dbmlInput: string;
}

function loadState (): PersistedState {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);

    if (raw) return JSON.parse(raw) as PersistedState;
  } catch {}

  return {
    tab: Tab.Transpile,
    transpileFrom: 'mysql',
    transpileTo: 'postgres',
    transpileInput: DEFAULT_TRANSPILE_INPUT,
    dbmlDialect: '',
    dbmlInput: DEFAULT_DBML_INPUT,
  };
}

export const usePlaygroundStore = defineStore('playground', () => {
  const saved = loadState();

  const tab = ref(saved.tab);
  const transpileFrom = ref(saved.transpileFrom);
  const transpileTo = ref(saved.transpileTo);
  const transpileInput = ref(saved.transpileInput);
  const dbmlDialect = ref(saved.dbmlDialect);
  const dbmlInput = ref(saved.dbmlInput);

  function persist () {
    const state: PersistedState = {
      tab: tab.value,
      transpileFrom: transpileFrom.value,
      transpileTo: transpileTo.value,
      transpileInput: transpileInput.value,
      dbmlDialect: dbmlDialect.value,
      dbmlInput: dbmlInput.value,
    };

    localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  }

  return {
    tab,
    transpileFrom,
    transpileTo,
    transpileInput,
    dbmlDialect,
    dbmlInput,
    persist,
  };
});
