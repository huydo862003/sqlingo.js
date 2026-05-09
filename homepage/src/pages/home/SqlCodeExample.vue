<template>
  <GTab>
    <GTabPanel name="Parse">
      <GCodeBlock
        id="example-parse"
        code="
import { parse } from &quot;@hdnax/sqlingo.js&quot;;
const [ast] = parse(
  &quot;SELECT a, b FROM t WHERE a > 1&quot;,
  { read: &quot;mysql&quot; },
);
// => &quot;SELECT a, b FROM t WHERE a > 1&quot;
"
        :language="GCodeLanguage.Typescript"
        :highlight-theme="GHighlightTheme.AtomOne"
        show-line-numbers
        :show-header="false"
        class="border-none"
      />
    </GTabPanel>
    <GTabPanel name="Transpile">
      <GCodeBlock
        id="example-transpile"
        code="
import { transpile } from &quot;@hdnax/sqlingo.js&quot;;
const [result] = transpile(&quot;SELECT EPOCH_MS(1618088028295)&quot;, {
  read: &quot;duckdb&quot;,
  write: &quot;hive&quot;,
});
// => &quot;SELECT FROM_UNIXTIME(1618088028295 / POW(10, 3))&quot;
"
        :language="GCodeLanguage.Typescript"
        :highlight-theme="GHighlightTheme.AtomOne"
        show-line-numbers
        :show-header="false"
        class="border-none"
      />
    </GTabPanel>
    <GTabPanel name="Optimize">
      <GCodeBlock
        id="example-optimize"
        code="
import { optimize } from &quot;@hdnax/sqlingo.js&quot;;
const result = optimize(
  &quot;SELECT a FROM (SELECT a, b FROM t) sub WHERE sub.a > 1&quot;,
  { dialect: &quot;duckdb&quot; },
);
// => &quot;SELECT t.a FROM t WHERE t.a > 1&quot;
"
        :language="GCodeLanguage.Typescript"
        :highlight-theme="GHighlightTheme.AtomOne"
        show-line-numbers
        :show-header="false"
        class="border-none"
      />
    </GTabPanel>
  </GTab>
</template>

<script setup lang="ts">
import {
  GTab,
  GTabPanel,
  GCodeBlock,
  GCodeLanguage,
  GHighlightTheme,
} from '@hdnax/genuix';
</script>
