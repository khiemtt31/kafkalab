"use client";

import Link from "next/link";
import { useKafkaStore } from "@/store/kafka-store";
import type { PracticeCategory } from "@/hooks/useKafkaLab";
import { useEffect } from "react";

const categories: Array<{ id: PracticeCategory; title: string; description: string }> = [
  { id: "easy", title: "Easy", description: "Build the core vocabulary and mental model." },
  { id: "middle", title: "Middle", description: "Apply partitions, offsets, and group behavior." },
  { id: "hard", title: "Hard", description: "Reason about reliability and operations." },
  { id: "extreme-hard", title: "Extreme hard", description: "Work through advanced design tradeoffs." },
  { id: "most-common", title: "Most common", description: "Practice questions interviewers ask often." },
  { id: "trick", title: "Trick questions", description: "Spot assumptions hidden in familiar terms." },
  { id: "situation", title: "Situations", description: "Diagnose realistic production symptoms." },
];

export function PracticeHub() {
  const progress = useKafkaStore((state) => state.practiceProgress);
  const selectCategory = useKafkaStore((state) => state.actions.selectPracticeCategory);
  useEffect(() => { selectCategory?.(progress.selectedCategory ?? "easy"); }, [progress.selectedCategory, selectCategory]);

  return (
    <main id="main-content" className="kl-practice-page">
      <header className="kl-practice-heading"><p className="kl-overline">RECALL AND APPLY</p><h1>Kafka practice</h1><p>Choose a set, answer one question at a time, and keep your progress between visits.</p></header>
      <section className="kl-practice-grid" aria-label="Practice question sets">
        {categories.map((category) => {
          const answered = Object.keys(progress.answers[category.id] ?? {}).length;
          return <Link className="kl-practice-category" href={`/practice/${category.id}`} key={category.id} onClick={() => selectCategory?.(category.id)}>
            <span className="kl-practice-category__top"><strong>{category.title}</strong><span>{answered} / 50</span></span>
            <p>{category.description}</p>
            <span className="kl-practice-progress" aria-label={`${answered} of 50 answered`}><i style={{ width: `${Math.min(answered, 50) * 2}%` }} /></span>
            <span className="kl-practice-category__link">Open set <span aria-hidden="true">→</span></span>
          </Link>;
        })}
      </section>
    </main>
  );
}
