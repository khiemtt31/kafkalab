import { notFound } from "next/navigation";
import { PracticeQuiz } from "@/components/PracticeQuiz";
import type { QuizDifficulty as QuizCategory } from "@/data/kafka-quiz/types";

const loaders = {
  easy: () => import("@/data/kafka-quiz/easy").then((module) => module.easyQuestions),
  middle: () => import("@/data/kafka-quiz/middle").then((module) => module.middleQuestions),
  hard: () => import("@/data/kafka-quiz/hard").then((module) => module.hardQuestions),
  "extreme-hard": () => import("@/data/kafka-quiz/extreme-hard").then((module) => module.extremeHardQuestions),
  "most-common": () => import("@/data/kafka-quiz/most-common").then((module) => module.mostCommonQuestions),
  trick: () => import("@/data/kafka-quiz/trick").then((module) => module.trickQuestions),
  situation: () => import("@/data/kafka-quiz/situation").then((module) => module.situationQuestions),
} satisfies Record<QuizCategory, () => Promise<readonly import("@/data/kafka-quiz/types").QuizQuestion[]>>;

export function generateStaticParams() {
  return Object.keys(loaders).map((category) => ({ category }));
}

export default async function PracticeCategoryPage({ params }: { params: Promise<{ category: string }> }) {
  const { category: rawCategory } = await params;
  if (!(rawCategory in loaders)) notFound();
  const category = rawCategory as QuizCategory;
  const questions = await loaders[category]();
  return <PracticeQuiz category={category} questions={questions} />;
}
