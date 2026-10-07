"use client";

import { create } from "zustand";
import { experiments, type ExperimentId } from "./experiments";

export const LEARNING_STORAGE_KEY = "kafkalab:learning:v1";
type Entry = { prediction?: number; completed: boolean };
type Progress = Partial<Record<ExperimentId, Entry>>;
type LearningState = { progress: Progress; hydrated: boolean; notice?: string;
  hydrate: () => void; predict: (id: ExperimentId, choice: number) => void;
  complete: (id: ExperimentId) => void; reset: () => void };

export function validateLearningProgress(value: unknown): Progress {
  if (!value || typeof value !== "object" || !("version" in value) || value.version !== 1 || !("progress" in value)
    || !value.progress || typeof value.progress !== "object") return {};
  const progress: Progress = {};
  for (const lesson of experiments) {
    const entry = (value.progress as Record<string, unknown>)[lesson.id];
    if (!entry || typeof entry !== "object" || !("prediction" in entry) || !("completed" in entry)
      || typeof entry.prediction !== "number" || !Number.isInteger(entry.prediction)
      || entry.prediction < 0 || entry.prediction >= lesson.choices.length || typeof entry.completed !== "boolean") continue;
    progress[lesson.id] = { prediction: entry.prediction, completed: entry.completed };
  }
  return progress;
}

function save(progress: Progress): string | undefined {
  try { window.localStorage.setItem(LEARNING_STORAGE_KEY, JSON.stringify({ version: 1, progress })); }
  catch { return "Learning progress is kept for this session; browser storage is unavailable."; }
}

export const useLearningProgress = create<LearningState>((set, get) => ({
  progress: {}, hydrated: false,
  hydrate: () => {
    if (get().hydrated) return;
    try {
      const raw = window.localStorage.getItem(LEARNING_STORAGE_KEY);
      set({ progress: raw ? validateLearningProgress(JSON.parse(raw)) : {}, hydrated: true });
    } catch { set({ hydrated: true, notice: "Saved learning progress could not be restored. You can still run every experiment." }); }
  },
  predict: (id, choice) => {
    const lesson = experiments.find((candidate) => candidate.id === id);
    if (!lesson || !Number.isInteger(choice) || choice < 0 || choice >= lesson.choices.length) return;
    const progress = { ...get().progress, [id]: { prediction: choice, completed: get().progress[id]?.completed ?? false } };
    set({ progress, notice: save(progress) });
  },
  complete: (id) => {
    const entry = get().progress[id];
    if (entry?.prediction === undefined) return;
    const progress = { ...get().progress, [id]: { ...entry, completed: true } };
    set({ progress, notice: save(progress) });
  },
  reset: () => { set({ progress: {}, notice: save({}) }); },
}));
