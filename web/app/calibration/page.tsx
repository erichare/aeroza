import type { Metadata } from "next";

import { CalibrationPanel } from "@/components/CalibrationPanel";

export const metadata: Metadata = {
  title: "Calibration",
  description:
    "Sample-weighted MAE / bias / RMSE per (algorithm × forecast horizon) — " +
    "the public face of Aeroza's calibration moat.",
};

export default function CalibrationPage() {
  return (
    <main className="mx-auto flex w-full max-w-[1400px] flex-col gap-6 px-6 py-8">
      <header className="flex flex-col gap-2">
        <span className="rounded-full border border-accent/40 bg-accent/10 self-start px-2.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.2em] text-accent">
          Public moat · §3.3
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-text">
          Calibration
        </h1>
        <p className="max-w-3xl text-sm leading-relaxed text-muted">
          Continuous verification: every nowcast is scored against the matching
          observation grid as soon as the truth lands. Aggregates are
          sample-weighted by cell count, so a verification covering 1M cells
          contributes 1M times to the means. Watch a real algorithm overtake
          the persistence baseline in real time.
        </p>
      </header>

      <CalibrationPanel />
    </main>
  );
}
