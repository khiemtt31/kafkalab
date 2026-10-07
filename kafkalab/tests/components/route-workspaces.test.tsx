import type { AnchorHTMLAttributes, ReactNode } from "react";
import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { GuidePage } from "@/components/GuidePage";
import { useKafkaStore } from "@/store/kafka-store";

vi.mock("next/link", () => ({
  default: ({ href, children, ...props }: AnchorHTMLAttributes<HTMLAnchorElement> & { href: string; children: ReactNode }) => (
    <a href={href} {...props}>{children}</a>
  ),
}));

describe("route workspace interactions with the real simulation store", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    useKafkaStore.getState().actions.reset();
  });

  afterEach(() => {
    cleanup();
    useKafkaStore.getState().actions.reset();
    vi.useRealTimers();
  });

  it("guides one record through separate send, read, and manual save steps", () => {
    useKafkaStore.getState().actions.produce({ key: null, value: "previous-work" });
    render(<GuidePage />);
    expect(screen.getByRole("button", { name: "Read record" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Save progress" })).toBeDisabled();

    fireEvent.click(screen.getByRole("button", { name: "Send example" }));
    let snapshot = useKafkaStore.getState().snapshot;
    expect(snapshot.messageCount).toBe(1);
    expect(snapshot.partitions.flatMap((partition) => partition.messages).map((message) => message.key)).toEqual(["order-7F3K9"]);
    expect(snapshot.partitions.flatMap((partition) => partition.messages).map((message) => message.value)).toEqual(["order-created"]);
    expect(snapshot.consumers[0].autoCommit).toBe(false);
    expect(snapshot.totalLag).toBe(1);
    expect(screen.getByRole("button", { name: "Read record" })).toBeEnabled();

    fireEvent.click(screen.getByRole("button", { name: "Read record" }));
    act(() => { vi.advanceTimersByTime(1000); });
    snapshot = useKafkaStore.getState().snapshot;
    expect(snapshot.messageCount).toBe(1);
    expect(snapshot.producerRunning).toBe(false);
    expect(snapshot.running).toBe(false);
    expect(snapshot.partitions.reduce((sum, partition) => sum + (partition.currentOffset ?? 0), 0)).toBe(1);
    expect(snapshot.partitions.reduce((sum, partition) => sum + (partition.committedOffset ?? 0), 0)).toBe(0);
    expect(snapshot.totalLag).toBe(1);
    expect(screen.getByRole("button", { name: "Record read" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Save progress" })).toBeEnabled();

    fireEvent.click(screen.getByRole("button", { name: "Save progress" }));
    snapshot = useKafkaStore.getState().snapshot;
    expect(snapshot.messageCount).toBe(1);
    expect(snapshot.totalLag).toBe(0);
    expect(snapshot.partitions.reduce((sum, partition) => sum + (partition.committedOffset ?? 0), 0)).toBe(1);
    expect(screen.getByRole("button", { name: "Progress saved" })).toBeDisabled();
    expect(screen.getByRole("status")).toHaveTextContent("You sent, read, and committed your first record");
    expect(screen.getByRole("link", { name: "See it on the canvas" })).toHaveAttribute("href", "/");

    fireEvent.click(screen.getByRole("button", { name: "Start fresh" }));
    expect(useKafkaStore.getState().snapshot.messageCount).toBe(1);
    expect(useKafkaStore.getState().snapshot.totalLag).toBe(1);
    expect(screen.getByRole("button", { name: "Read record" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Save progress" })).toBeDisabled();
  });

  it("does not mistake the reference workload for a completed beginner exercise", () => {
    useKafkaStore.getState().actions.loadReferenceFlow?.();
    render(<GuidePage />);
    expect(screen.getByRole("button", { name: "Send example" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Read record" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Save progress" })).toBeDisabled();
    expect(screen.getByRole("status")).toHaveTextContent("Send the example to begin");
    fireEvent.click(screen.getByRole("button", { name: "Send example" }));
    expect(useKafkaStore.getState().snapshot.messageCount).toBe(1);
    expect(screen.getByRole("button", { name: "Read record" })).toBeEnabled();
  });

  it("keeps production learning sections collapsed until opened", () => {
    render(<GuidePage />);
    const beforeRevision = useKafkaStore.getState().snapshot.resetRevision;

    for (const title of ["Kafka concepts", "Interview Q&A", "Common traps", "Real project situations"]) {
      const summary = screen.getByText(title, { selector: "summary" });
      expect(summary.closest("details")).not.toHaveAttribute("open");
    }

    fireEvent.click(screen.getByText("Kafka concepts", { selector: "summary" }));
    expect(screen.getByText("Why is ordering scoped to a partition?")).toBeVisible();
    expect(useKafkaStore.getState().snapshot.resetRevision).toBe(beforeRevision);
  });

});
