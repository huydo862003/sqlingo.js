import {
  watchEffect,
} from 'vue';

export interface SeoOptions {
  title?: string;
  description?: string;
}

export function useSeo (options: SeoOptions | (() => SeoOptions)) {
  watchEffect(() => {
    const {
      title, description,
    } = typeof options === 'function' ? options() : options;

    const fullTitle = title ? `${title} | sqlingo.js` : 'sqlingo.js: SQLGlot Port for JavaScript & TypeScript';
    document.title = fullTitle;

    const defaultDesc = 'sqlingo.js is a JavaScript/TypeScript port of SQLGlot, a SQL parser, transpiler, and optimizer supporting 33+ dialects. Handle complex SQL in the browser or Node.js.';
    const desc = description || defaultDesc;

    // Update meta description
    let metaDesc = document.querySelector('meta[name="description"]');
    if (!metaDesc) {
      metaDesc = document.createElement('meta');
      metaDesc.setAttribute('name', 'description');
      document.head.appendChild(metaDesc);
    }
    metaDesc.setAttribute('content', desc);

    // Update OG tags for social sharing
    updateMetaTag('og:title', fullTitle);
    updateMetaTag('og:description', desc);
    updateMetaTag('twitter:title', fullTitle);
    updateMetaTag('twitter:description', desc);
  });
}

function updateMetaTag (name: string, content: string) {
  let el = document.querySelector(`meta[property="${name}"], meta[name="${name}"]`);
  if (!el) {
    el = document.createElement('meta');
    if (name.startsWith('og:')) {
      el.setAttribute('property', name);
    } else {
      el.setAttribute('name', name);
    }
    document.head.appendChild(el);
  }
  el.setAttribute('content', content);
}
