export type Theme = "light" | "dark";

export const THEME_STORAGE_KEY = "kafkalab:theme:v1";

export function parseThemePreference(raw: string | null): Theme {
  try {
    const preference = raw ? JSON.parse(raw) : null;
    if (preference?.version === 1 && (preference.theme === "light" || preference.theme === "dark")) {
      return preference.theme;
    }
  } catch {
    // An invalid preference should never prevent the app from opening.
  }
  return "light";
}

// This synchronous head script applies the preference before the page is painted.
// Keep its whitelist in sync with parseThemePreference; never execute stored values.
export const THEME_BOOTSTRAP = `(() => {
  let theme = "light";
  try {
    const preference = JSON.parse(localStorage.getItem(${JSON.stringify(THEME_STORAGE_KEY)}));
    if (preference?.version === 1 && (preference.theme === "light" || preference.theme === "dark")) {
      theme = preference.theme;
    }
  } catch {}
  document.documentElement.dataset.theme = theme;
})();`;
