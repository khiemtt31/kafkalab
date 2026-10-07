export type QuizOptionId = "a" | "b" | "c" | "d";

export type QuizOption = {
  id: QuizOptionId;
  text: string;
};

export type QuizDifficulty = "easy" | "middle" | "hard" | "extreme-hard" | "most-common" | "trick" | "situation";
export type QuizCategory = QuizDifficulty;

export type QuizQuestion = {
  id: string;
  concept: string;
  question: string;
  options: readonly [QuizOption, QuizOption, QuizOption, QuizOption];
  correctOptionId: QuizOptionId;
  explanation: string;
  difficulty: QuizDifficulty;
};

export function makeQuestion(
  id: string,
  concept: string,
  question: string,
  optionText: readonly [string, string, string, string],
  correctOptionId: QuizOptionId,
  explanation: string,
  difficulty: QuizDifficulty,
): QuizQuestion {
  const correctSourceIndex = "abcd".indexOf(correctOptionId);
  let hash = 2166136261;
  for (const character of id) hash = Math.imul(hash ^ character.charCodeAt(0), 16777619);
  const correctTargetIndex = (hash >>> 0) % 4;
  const shuffled: Array<QuizOption | undefined> = Array.from({ length: 4 });
  shuffled[correctTargetIndex] = { id: ("abcd"[correctTargetIndex] ?? "a") as QuizOptionId, text: optionText[correctSourceIndex] };
  const distractorIndexes = [0, 1, 2, 3].filter((index) => index !== correctSourceIndex);
  let distractorIndex = 0;
  for (let optionIndex = 0; optionIndex < 4; optionIndex += 1) {
    if (optionIndex === correctTargetIndex) continue;
    const textIndex = distractorIndexes[distractorIndex++];
    shuffled[optionIndex] = { id: ("abcd"[optionIndex] ?? "a") as QuizOptionId, text: optionText[textIndex] };
  }
  const options: QuizQuestion["options"] = [shuffled[0]!, shuffled[1]!, shuffled[2]!, shuffled[3]!];
  const shuffledCorrectOptionId = ("abcd"[correctTargetIndex] ?? "a") as QuizOptionId;
  return {
    id,
    concept,
    question,
    options,
    correctOptionId: shuffledCorrectOptionId,
    explanation,
    difficulty,
  };
}
