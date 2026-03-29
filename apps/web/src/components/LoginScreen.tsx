import { useState } from "react";

import { SkimmableText } from "./SkimmableText";

interface LoginScreenProps {
  onSubmit: (email: string, password: string) => Promise<void>;
  error?: string | null;
  busy: boolean;
}

export function LoginScreen({ onSubmit, error, busy }: LoginScreenProps) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");

  return (
    <div className="min-h-screen overflow-hidden bg-[var(--paper)] text-[var(--ink)]">
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_left,_rgba(154,52,18,0.15),_transparent_35%),radial-gradient(circle_at_bottom_right,_rgba(14,77,100,0.18),_transparent_30%),linear-gradient(135deg,_rgba(17,19,18,0.05),_transparent_40%)]" />
      <div className="relative mx-auto flex min-h-screen max-w-7xl flex-col justify-center gap-10 px-6 py-12 lg:flex-row lg:items-center lg:px-10">
        <section className="max-w-2xl space-y-6">
          <p className="font-mono text-xs uppercase tracking-[0.38em] text-[var(--muted)]">Research Center / Editorial Briefing System</p>
          <h1 className="max-w-xl font-display text-6xl leading-[0.9] text-[var(--ink)] sm:text-7xl">
            Every morning should start with signal, not feed sludge.
          </h1>
          <SkimmableText className="max-w-xl text-lg leading-8 text-[var(--muted)]">
            Research Center ingests papers, newsletters, and selected feeds, deduplicates repetitive coverage,
            ranks what actually matters, and turns it into a calm briefing surface built for iPad review.
          </SkimmableText>
          <div className="grid max-w-2xl gap-3 sm:grid-cols-3">
            {[
              "Top 3 briefing with editorial framing",
              "Near-duplicate clustering with source mentions",
              "One-tap Zotero export and follow-up prompts",
            ].map((line) => (
              <div key={line} className="editorial-panel">
                <p className="font-mono text-[11px] uppercase tracking-[0.26em] text-[var(--muted)]">v1 capability</p>
                <SkimmableText className="mt-3 text-sm leading-6 text-[var(--ink)]">{line}</SkimmableText>
              </div>
            ))}
          </div>
        </section>

        <section className="editorial-panel w-full max-w-md border-[var(--ink)]/10 bg-[rgba(255,255,255,0.55)]">
          <div className="mb-6 flex items-center justify-between">
            <div>
              <p className="font-mono text-xs uppercase tracking-[0.26em] text-[var(--muted)]">Managed Login</p>
              <h2 className="mt-2 font-display text-3xl text-[var(--ink)]">Open the cockpit</h2>
            </div>
            <div className="h-16 w-16 rounded-full border border-[var(--ink)]/15 bg-[radial-gradient(circle_at_30%_30%,_rgba(154,52,18,0.75),_rgba(17,19,18,0.95))]" />
          </div>
          <form
            className="space-y-4"
            onSubmit={async (event) => {
              event.preventDefault();
              await onSubmit(email, password);
            }}
          >
            <label className="block">
              <span className="field-label">Email</span>
              <input className="field-input" value={email} onChange={(event) => setEmail(event.target.value)} />
            </label>
            <label className="block">
              <span className="field-label">Password</span>
              <input
                className="field-input"
                type="password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
              />
            </label>
            {error ? <p className="text-sm text-[var(--danger)]">{error}</p> : null}
            <button className="primary-button w-full justify-center" disabled={busy} type="submit">
              {busy ? "Opening…" : "Sign in"}
            </button>
          </form>
        </section>
      </div>
    </div>
  );
}
