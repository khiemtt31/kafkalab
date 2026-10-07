import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { PracticeQuiz } from "@/components/PracticeQuiz";
import { easyQuestions } from "@/data/kafka-quiz/easy";
import { useKafkaStore } from "@/store/kafka-store";

describe("Kafka Practice quiz", () => {
  beforeEach(() => {
    useKafkaStore.getState().actions.resetPracticeCategory?.("easy");
  });

  afterEach(() => {
    cleanup();
    useKafkaStore.getState().actions.resetPracticeCategory?.("easy");
    vi.restoreAllMocks();
  });

  it("records an answer, explains it, and restores the saved question selection", () => {
    render(<PracticeQuiz category="easy" questions={easyQuestions} />);
    expect(screen.getByText("0 / 50 answered")).toBeTruthy();
    const question = easyQuestions[0];
    const correct = question.options.find((option) => option.id === question.correctOptionId);
    expect(correct).toBeDefined();
    fireEvent.click(screen.getByRole("radio", { name: new RegExp(correct!.text) }));
    expect(screen.getByRole("status").textContent).toContain("Correct");
    expect(screen.getByText(question.explanation)).toBeTruthy();
    expect(screen.getByText("1 / 50 answered")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    expect(screen.getByText("Question 2 of 50")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Previous" }));
    expect(useKafkaStore.getState().practiceProgress.currentQuestion.easy).toBe(0);
    expect(useKafkaStore.getState().practiceProgress.answers.easy?.[question.id]).toBe(question.correctOptionId);
  });

  it("resets just the active question set", () => {
    const actions = useKafkaStore.getState().actions;
    const initialCount = useKafkaStore.getState().snapshot.messageCount;
    actions.produce({ key: "order-7F3K9", value: "order-created" });
    const simulationBeforeReset = useKafkaStore.getState().simulation;
    actions.answerPracticeQuestion?.("easy", easyQuestions[0].id, "a");
    vi.stubGlobal("confirm", vi.fn(() => true));
    render(<PracticeQuiz category="easy" questions={easyQuestions} />);
    fireEvent.click(screen.getByRole("button", { name: "Reset this set" }));
    expect(useKafkaStore.getState().practiceProgress.answers.easy).toBeUndefined();
    expect(useKafkaStore.getState().snapshot.messageCount).toBe(initialCount + 1);
    expect(useKafkaStore.getState().simulation).toEqual(simulationBeforeReset);
  });
});
