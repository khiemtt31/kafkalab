"use client";

import { parseThemePreference, THEME_STORAGE_KEY, type Theme } from "./theme";

function readPreference(): Theme {
  if (typeof window === "undefined") return "light";
  try {
    return parseThemePreference(window.localStorage.getItem(THEME_STORAGE_KEY));
  } catch {
    return "light";
  }
}

let currentTheme = readPreference();
const listeners = new Set<() => void>();

function applyTheme(theme: Theme) {
  currentTheme = theme;
  document.documentElement.dataset.theme = theme;
  listeners.forEach((listener) => listener());
}

function handleStorage(event: StorageEvent) {
  // Read the current value so that queued events cannot restore an older choice.
  if (event.key === THEME_STORAGE_KEY || event.key === null) {
    applyTheme(readPreference());
  }
}

export function subscribeToTheme(listener: () => void) {
  listeners.add(listener);
  if (listeners.size === 1) window.addEventListener("storage", handleStorage);
  return () => {
    listeners.delete(listener);
    if (listeners.size === 0) window.removeEventListener("storage", handleStorage);
  };
}

export const getTheme = () => currentTheme;
export const getServerTheme = (): Theme => "light";

export function toggleTheme() {
  const theme = currentTheme === "light" ? "dark" : "light";
  applyTheme(theme);
  try {
    window.localStorage.setItem(THEME_STORAGE_KEY, JSON.stringify({ version: 1, theme }));
  } catch {
    // The toggle remains usable for this session when browser storage is blocked.
  }
}
