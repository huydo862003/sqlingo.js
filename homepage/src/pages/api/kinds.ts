const DOT_COLORS: Record<string, string> = {
  class: 'var(--gui-info-solid)',
  function: 'var(--gui-success-solid)',
  interface: 'var(--gui-warning-solid)',
  enum: 'var(--gui-notice-solid)',
  typealias: 'var(--gui-primary-solid)',
  variable: 'var(--gui-danger-solid)',
  property: 'var(--gui-neutral-solid)',
  method: 'var(--gui-success-solid)',
  constructor: 'var(--gui-neutral-solid)',
  enumvalue: 'var(--gui-notice-solid)',
};

const CHIP_COLORS: Record<string, { bg: string;
  color: string; }> = {
  class: {
    bg: 'var(--gui-info-bg-hover)',
    color: 'var(--gui-info-fg)',
  },
  function: {
    bg: 'var(--gui-success-bg-hover)',
    color: 'var(--gui-success-fg)',
  },
  interface: {
    bg: 'var(--gui-warning-bg-hover)',
    color: 'var(--gui-warning-fg)',
  },
  enum: {
    bg: 'var(--gui-notice-bg-hover)',
    color: 'var(--gui-notice-fg)',
  },
  typealias: {
    bg: 'var(--gui-primary-bg-hover)',
    color: 'var(--gui-primary-fg)',
  },
  variable: {
    bg: 'var(--gui-danger-bg-hover)',
    color: 'var(--gui-danger-fg)',
  },
};

export function kindDotStyle (slug: string) {
  return {
    background: DOT_COLORS[slug] ?? 'var(--gui-neutral-solid)',
  };
}

export function kindChipStyle (slug: string) {
  const c = CHIP_COLORS[slug];
  return c
    ? {
      background: c.bg,
      color: c.color,
    }
    : {};
}
