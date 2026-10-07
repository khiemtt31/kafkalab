"use client";

import { useSyncExternalStore } from "react";
import { getServerTheme, getTheme, subscribeToTheme, toggleTheme } from "../theme/theme-store";
import { LabIcon } from "./LabIcon";

export function ThemeToggle() {
  const theme = useSyncExternalStore(subscribeToTheme, getTheme, getServerTheme);
  const label = theme === "dark" ? "Switch to light theme" : "Switch to dark theme";
  return (
    <button type="button" className="kl-icon-button kl-theme-toggle" onClick={toggleTheme} aria-label={label} aria-pressed={theme === "dark"} title={label}>
      <LabIcon name={theme === "dark" ? "sun" : "moon"} />
    </button>
  );
}
