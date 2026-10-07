import type { Metadata } from "next";
import "./globals.css";
import "../components/lab.css";
import "../components/process-canvas.css";
import "../components/workspace.css";
import "../theme/theme.css";
import { LabAppShell } from "../components/LabAppShell";
import { THEME_BOOTSTRAP } from "../theme/theme";

export const metadata: Metadata = {
  title: "Kafka Visual Learning Lab",
  description: "Explore ordered Kafka partition logs, record delivery, processing failures, replication, and consumer checkpoints in a visual learning lab with interview practice.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en" data-theme="light" suppressHydrationWarning>
      <head><script dangerouslySetInnerHTML={{ __html: THEME_BOOTSTRAP }} /></head>
      <body><LabAppShell>{children}</LabAppShell></body>
    </html>
  );
}
