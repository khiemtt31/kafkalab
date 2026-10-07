import { describe, expect, it } from "vitest";
import { easyQuestions } from "@/data/kafka-quiz/easy";
import { middleQuestions } from "@/data/kafka-quiz/middle";
import { hardQuestions } from "@/data/kafka-quiz/hard";
import { extremeHardQuestions } from "@/data/kafka-quiz/extreme-hard";
import { mostCommonQuestions } from "@/data/kafka-quiz/most-common";
import { trickQuestions } from "@/data/kafka-quiz/trick";
import { situationQuestions } from "@/data/kafka-quiz/situation";

const banks = { easy: easyQuestions, middle: middleQuestions, hard: hardQuestions, "extreme-hard": extremeHardQuestions, "most-common": mostCommonQuestions, trick: trickQuestions, situation: situationQuestions };

describe.each(Object.entries(banks))("%s Kafka question bank", (_category, questions) => {
  it("contains exactly 50 unique, answerable four-option questions", () => {
    expect(questions).toHaveLength(50);
    expect(new Set(questions.map((question) => question.id)).size).toBe(50);
    expect(new Set(questions.map((question) => question.question.trim().toLowerCase())).size).toBe(50);
    const answerDistribution = questions.reduce((counts, question) => { counts[question.correctOptionId] += 1; return counts; }, { a: 0, b: 0, c: 0, d: 0 });
    expect(Object.values(answerDistribution).every((count) => count >= 5 && count <= 20)).toBe(true);
    for (const question of questions) {
      expect(question.id).toBeTruthy();
      expect(question.concept).toBeTruthy();
      expect(question.explanation).toBeTruthy();
      expect(question.options).toHaveLength(4);
      expect(new Set(question.options.map((option) => option.id)).size).toBe(4);
      expect(question.options.some((option) => option.id === question.correctOptionId)).toBe(true);
    }
  });
});
