"use client";

import { useMemo } from "react";

/** Keep large simulations from turning history arrays into large React trees. */
export function useBoundedHistory<T>(values: T[], limit = 100): T[] {
  return useMemo(() => values.slice(Math.max(0, values.length - limit)), [values, limit]);
}
