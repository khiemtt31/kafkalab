import type { AnchorHTMLAttributes, ReactNode } from "react";
import { cleanup, render, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { LabAppShell } from "@/components/LabAppShell";

const fixtures = vi.hoisted(() => ({
  pathname: "/",
  profile: { appName: "Kafka Lab", creatorName: "", creatorBio: "", creatorLinks: [] as Array<{ label: string; href: string }> },
  hydrate: vi.fn(),
}));

vi.mock("next/navigation", () => ({ usePathname: () => fixtures.pathname }));
vi.mock("next/link", () => ({
  default: ({ href, children, ...props }: AnchorHTMLAttributes<HTMLAnchorElement> & { href: string; children: ReactNode }) => <a href={href} {...props}>{children}</a>,
}));
vi.mock("@/config/site-profile", () => ({ siteProfile: fixtures.profile }));
vi.mock("@/store/kafka-store", () => ({
  persistKafkaWorkspace: vi.fn(),
  useKafkaStore: Object.assign(
    (select: (state: unknown) => unknown) => select({ hydrated: true, storageNotice: null, actions: { hydrate: fixtures.hydrate } }),
    { subscribe: () => () => {} },
  ),
}));

afterEach(() => {
  cleanup();
  fixtures.profile.creatorName = "";
  fixtures.profile.creatorBio = "";
  fixtures.profile.creatorLinks = [];
});

describe("shared application footer", () => {
  it.each(["/", "/guide", "/practice", "/practice/easy", "/practice/middle", "/practice/hard", "/practice/extreme-hard", "/practice/most-common", "/practice/trick", "/practice/situation"])("keeps footer navigation and copyright available on %s", (pathname) => {
    fixtures.pathname = pathname;
    render(<LabAppShell><main id="main-content">Route content</main></LabAppShell>);
    const footer = screen.getByRole("contentinfo", { name: "Application and creator information" });
    expect(within(footer).getByText("Creator details pending")).toBeInTheDocument();
    expect(within(footer).getByText(/© \d{4} Kafka Lab\./)).toBeInTheDocument();
    expect(within(footer).getByRole("link", { name: "Learning guide" })).toHaveAttribute("href", "/guide");
    expect(within(footer).getByRole("link", { name: "Interview practice" })).toHaveAttribute("href", "/practice");
    const docs = within(footer).getByRole("link", { name: /Apache Kafka documentation/ });
    expect(docs).toHaveAttribute("href", "https://kafka.apache.org/43/");
    expect(docs).toHaveAttribute("rel", "noopener noreferrer");
    expect(screen.getByText("Route content")).toBeInTheDocument();
  });

  it("keeps the header navigation focused on Flow, Guide, and Practice", () => {
    render(<LabAppShell><main id="main-content">Route content</main></LabAppShell>);
    const navigation = screen.getByRole("navigation", { name: "Main navigation" });
    expect(within(navigation).getAllByRole("link").map((link) => link.textContent)).toEqual(["Flow", "Guide", "Practice"]);
    expect(within(navigation).queryByText("Customize")).not.toBeInTheDocument();
  });

  it("renders supplied creator details and public links without a pending placeholder", () => {
    fixtures.profile.creatorName = "Project creator";
    fixtures.profile.creatorBio = "Builds visual learning tools.";
    fixtures.profile.creatorLinks = [{ label: "Portfolio", href: "https://example.org/creator" }];
    render(<LabAppShell><main id="main-content">Route content</main></LabAppShell>);
    const footer = screen.getByRole("contentinfo", { name: "Application and creator information" });
    expect(within(footer).queryByText("Creator details pending")).not.toBeInTheDocument();
    expect(within(footer).getByText("Project creator")).toBeInTheDocument();
    expect(within(footer).getByText("Builds visual learning tools.")).toBeInTheDocument();
    expect(within(footer).getByRole("link", { name: "Portfolio" })).toHaveAttribute("href", "https://example.org/creator");
  });
});
