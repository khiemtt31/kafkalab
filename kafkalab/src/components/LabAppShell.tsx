"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import type { ReactNode } from "react";
import { LabIcon } from "./LabIcon";
import { AppFooter } from "./AppFooter";
import { ThemeToggle } from "./ThemeToggle";
import { persistKafkaWorkspace, useKafkaStore } from "../store/kafka-store";
import { useEffect, useState } from "react";

const destinations = [
  { href: "/", label: "Flow", icon: "flow" },
  { href: "/guide", label: "Guide", icon: "book" },
  { href: "/practice", label: "Practice", icon: "check" },
] as const;

export function LabAppShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const hydrated = useKafkaStore((state) => state.hydrated);
  const storageNotice = useKafkaStore((state) => state.storageNotice);
  const hydrate = useKafkaStore((state) => state.actions.hydrate);
  const [noticeDismissed, setNoticeDismissed] = useState(false);

  useEffect(() => {
    let pending: ReturnType<typeof setTimeout> | undefined;
    const flush = () => {
      if (pending) clearTimeout(pending);
      pending = undefined;
      persistKafkaWorkspace();
    };
    const unsubscribe = useKafkaStore.subscribe((state) => {
      if (!state.hydrated) return;
      if (pending) clearTimeout(pending);
      pending = setTimeout(flush, 300);
    });
    const handleVisibilityChange = () => { if (document.visibilityState === "hidden") flush(); };
    window.addEventListener("pagehide", flush);
    document.addEventListener("visibilitychange", handleVisibilityChange);
    hydrate?.();
    return () => {
      unsubscribe();
      if (pending) clearTimeout(pending);
      window.removeEventListener("pagehide", flush);
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, [hydrate]);
  return (
    <div className="kl-app">
      <a className="kl-skip" href="#main-content">Skip to content</a>
      <header className="kl-header">
        <Link href="/" className="kl-brand" aria-label="Kafka Lab home"><span className="kl-brand-mark"><LabIcon name="flow" /></span><span>kafka<span className="kl-brand-light">lab</span><span className="kl-brand-dot">.</span></span></Link>
        <nav className="kl-main-nav" aria-label="Main navigation">
          {destinations.map((destination) => <Link key={destination.href} href={destination.href} aria-current={pathname === destination.href || (destination.href === "/practice" && pathname.startsWith("/practice/")) ? "page" : undefined}><LabIcon name={destination.icon} /><span>{destination.label}</span></Link>)}
        </nav>
        <ThemeToggle />
      </header>
      {storageNotice && !noticeDismissed ? <div className="kl-storage-notice" role="status"><span>{storageNotice}</span><button type="button" aria-label="Dismiss storage notice" onClick={() => setNoticeDismissed(true)}>Dismiss</button></div> : null}
      {hydrated ? children : <main id="main-content" className="kl-app-loading" aria-live="polite">Restoring your Kafka Lab…</main>}
      <AppFooter />
    </div>
  );
}
