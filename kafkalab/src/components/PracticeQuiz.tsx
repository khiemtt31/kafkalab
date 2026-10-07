"use client";

import Link from "next/link";
import { useEffect } from "react";
import { useKafkaStore } from "@/store/kafka-store";
import type { PracticeCategory } from "@/hooks/useKafkaLab";
import type { QuizQuestion } from "@/data/kafka-quiz/types";

const titles: Record<PracticeCategory, string> = {
  easy: "Easy", middle: "Middle", hard: "Hard", "extreme-hard": "Extreme hard", "most-common": "Most common", trick: "Trick questions", situation: "Situations",
};

export function PracticeQuiz({ category, questions }: { category: PracticeCategory; questions: readonly QuizQuestion[] }) {
  const progress = useKafkaStore((state) => state.practiceProgress);
  const actions = useKafkaStore((state) => state.actions);
  const index = Math.max(0, Math.min(questions.length - 1, progress.currentQuestion[category] ?? 0));
  const savedIndex = progress.currentQuestion[category] ?? 0;
  const question = questions[index];
  const answers = progress.answers[category] ?? {};
  const selectedOption = question ? answers[question.id] : undefined;
  const answeredCount = Object.keys(answers).filter((id) => questions.some((candidate) => candidate.id === id)).length;
  const correct = selectedOption === question?.correctOptionId;

  useEffect(() => { actions.selectPracticeQuestion?.(category, savedIndex); }, [actions, category, savedIndex]);

  if (questions.length !== 50 || !question) return <main id="main-content" className="kl-practice-page"><h1>Question set unavailable</h1><p>This set must contain 50 questions.</p><Link href="/practice">Back to Practice</Link></main>;

  return (
    <main id="main-content" className="kl-practice-page kl-quiz-page">
      <Link className="kl-practice-back" href="/practice">← All practice sets</Link>
      <header className="kl-practice-heading"><p className="kl-overline">PRACTICE SET</p><h1>{titles[category]} Kafka questions</h1><p>One question at a time. Choose an answer to see why it is right or wrong.</p></header>
      <div className="kl-quiz-progress-row"><span>Question {index + 1} of 50</span><span>{answeredCount} / 50 answered</span></div>
      <div className="kl-quiz-progress" role="progressbar" aria-label="Questions answered" aria-valuemin={0} aria-valuemax={50} aria-valuenow={answeredCount}><i style={{ width: `${answeredCount * 2}%` }} /></div>
      <article className="kl-quiz-card" aria-labelledby="quiz-question">
        <div className="kl-quiz-topic"><span>{question.concept}</span><span>{question.difficulty.replaceAll("-", " ")}</span></div>
        <h2 id="quiz-question">{question.question}</h2>
        <fieldset className="kl-quiz-options"><legend className="kl-sr-only">Choose one answer</legend>{question.options.map((option) => {
          const isCorrect = option.id === question.correctOptionId;
          const isSelected = option.id === selectedOption;
          const outcome = selectedOption ? isCorrect ? "correct" : isSelected ? "incorrect" : "" : "";
          return <label className={`kl-quiz-option ${outcome ? `is-${outcome}` : ""}`.trim()} key={option.id}>
            <input type="radio" name={question.id} value={option.id} checked={isSelected} onChange={() => actions.answerPracticeQuestion?.(category, question.id, option.id)} />
            <span className="kl-quiz-option__key">{option.id.toUpperCase()}</span><span>{option.text}</span>{outcome === "correct" ? <span className="kl-quiz-option__state">Correct</span> : outcome === "incorrect" ? <span className="kl-quiz-option__state">Your answer</span> : null}
          </label>;
        })}</fieldset>
        {selectedOption ? <div className={`kl-quiz-feedback ${correct ? "is-correct" : "is-incorrect"}`} role="status"><strong>{correct ? "Correct" : "Not quite"}</strong><p>{question.explanation}</p></div> : <p className="kl-quiz-prompt">Select an answer to reveal the explanation.</p>}
        <nav className="kl-quiz-controls" aria-label="Question navigation"><button className="kl-button kl-button--white" type="button" onClick={() => actions.selectPracticeQuestion?.(category, index - 1)} disabled={index === 0}>Previous</button><div className="kl-quiz-dots" aria-label="Question picker">{[0, 9, 19, 29, 39, 49].map((target) => <button key={target} type="button" aria-label={`Go to question ${target + 1}`} aria-current={target === index ? "step" : undefined} onClick={() => actions.selectPracticeQuestion?.(category, target)}>{target + 1}</button>)}</div><button className="kl-button kl-button--red" type="button" onClick={() => actions.selectPracticeQuestion?.(category, index + 1)} disabled={index === 49}>Next</button></nav>
      </article>
      <div className="kl-quiz-footer"><p>Answers and your current question are saved in this browser.</p><button type="button" className="kl-text-button" onClick={() => { if (window.confirm(`Reset your ${titles[category]} question progress?`)) actions.resetPracticeCategory?.(category); }}>Reset this set</button></div>
    </main>
  );
}
