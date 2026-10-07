import type { SVGProps } from "react";

export type LabIconName = "flow" | "sliders" | "book" | "play" | "pause" | "reset" | "send" | "plus" | "arrow" | "close" | "layers" | "worker" | "check" | "activity" | "chevron" | "code" | "sun" | "moon";

const paths: Record<LabIconName, React.ReactNode> = {
  flow: <><rect x="3" y="8" width="5" height="8" rx="1.5" /><rect x="16" y="3" width="5" height="6" rx="1.5" /><rect x="16" y="15" width="5" height="6" rx="1.5" /><path d="M8 12h4m0 0V6h4m-4 6v6h4" /></>,
  sliders: <><path d="M4 7h16M4 17h16" /><circle cx="8" cy="7" r="3" fill="currentColor" stroke="none" /><circle cx="16" cy="17" r="3" fill="currentColor" stroke="none" /></>,
  book: <><path d="M12 5v15M12 5c-3-2-6-2-9-1v14c3-1 6-1 9 2 3-3 6-3 9-2V4c-3-1-6-1-9 1Z" /></>,
  play: <path d="m8 5 11 7-11 7V5Z" fill="currentColor" stroke="none" />,
  pause: <><path d="M8 5v14M16 5v14" strokeWidth="3" /></>,
  reset: <><path d="M3 10a9 9 0 1 1 1 8M3 4v6h6" /></>,
  send: <><path d="m21 3-7 18-4-7-7-4 18-7Z" /><path d="m10 14 5-5" /></>,
  plus: <path d="M12 5v14M5 12h14" />,
  arrow: <path d="M4 12h16m-6-6 6 6-6 6" />,
  close: <path d="m6 6 12 12M18 6 6 18" />,
  layers: <><path d="m12 3 10 5-10 5L2 8l10-5ZM2 12l10 5 10-5M2 16l10 5 10-5" /></>,
  worker: <><rect x="5" y="5" width="14" height="14" rx="3" /><path d="M9 9h6v6H9V9ZM9 2v3m6-3v3M9 19v3m6-3v3M2 9h3m-3 6h3m14-6h3m-3 6h3" /></>,
  check: <path d="m5 12 4 4L19 6" />,
  activity: <path d="M2 12h5l3-8 4 16 3-8h5" />,
  chevron: <path d="m9 5 7 7-7 7" />,
  code: <><path d="m7 6-5 6 5 6m10-12 5 6-5 6m-4-16-2 20" /></>,
  sun: <><circle cx="12" cy="12" r="4" /><path d="M12 2v2m0 16v2M2 12h2m16 0h2M5 5l1.5 1.5m11 11L19 19M5 19l1.5-1.5m11-11L19 5" /></>,
  moon: <path d="M20.5 14A9 9 0 0 1 10 3.5 9 9 0 1 0 20.5 14Z" />,
};

export function LabIcon({ name, ...props }: SVGProps<SVGSVGElement> & { name: LabIconName }) {
  return <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" {...props}>{paths[name]}</svg>;
}
