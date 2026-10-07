"use client";

import { KafkaVisualLab } from "@/components/KafkaVisualLab";
import { useKafkaLabStoreAdapter } from "@/store/kafka-store";

export default function HomePage() {
  const store = useKafkaLabStoreAdapter();
  return <KafkaVisualLab store={store} />;
}
