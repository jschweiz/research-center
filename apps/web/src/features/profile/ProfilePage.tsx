import { FormEvent, useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { api } from "../../api/client";
import type { DataMode } from "../../api/types";
import { SkimmableText } from "../../components/SkimmableText";

function listToCsv(values: string[]) {
  return values.join(", ");
}

function csvToList(value: string) {
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

export function ProfilePage() {
  const queryClient = useQueryClient();
  const profileQuery = useQuery({ queryKey: ["profile"], queryFn: api.getProfile });
  const [favoriteTopics, setFavoriteTopics] = useState("");
  const [favoriteAuthors, setFavoriteAuthors] = useState("");
  const [favoriteSources, setFavoriteSources] = useState("");
  const [ignoredTopics, setIgnoredTopics] = useState("");
  const [digestTime, setDigestTime] = useState("07:00");
  const [timezone, setTimezone] = useState("Europe/Zurich");
  const [dataMode, setDataMode] = useState<DataMode>("seed");
  const [summaryDepth, setSummaryDepth] = useState("balanced");
  const [weights, setWeights] = useState<Record<string, string>>({
    relevance: "0.3",
    novelty: "0.15",
    source_quality: "0.15",
    author_match: "0.1",
    topic_match: "0.15",
    zotero_affinity: "0.15",
  });

  const saveProfile = useMutation({
    mutationFn: (payload: Record<string, unknown>) => api.updateProfile(payload),
    onSuccess: async (profile) => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["profile"] }),
        queryClient.invalidateQueries({ queryKey: ["briefs"] }),
        queryClient.invalidateQueries({ queryKey: ["items"] }),
        queryClient.invalidateQueries({ queryKey: ["item"] }),
      ]);
      setFavoriteTopics(listToCsv(profile.favorite_topics));
      setFavoriteAuthors(listToCsv(profile.favorite_authors));
      setFavoriteSources(listToCsv(profile.favorite_sources));
      setIgnoredTopics(listToCsv(profile.ignored_topics));
      setDigestTime(profile.digest_time.slice(0, 5));
      setTimezone(profile.timezone);
      setDataMode(profile.data_mode);
      setSummaryDepth(profile.summary_depth);
      setWeights(
        Object.fromEntries(
          Object.entries(profile.ranking_weights).map(([key, value]) => [key, String(value)]),
        ),
      );
    },
  });

  useEffect(() => {
    if (profileQuery.data) {
      setFavoriteTopics(listToCsv(profileQuery.data.favorite_topics));
      setFavoriteAuthors(listToCsv(profileQuery.data.favorite_authors));
      setFavoriteSources(listToCsv(profileQuery.data.favorite_sources));
      setIgnoredTopics(listToCsv(profileQuery.data.ignored_topics));
      setDigestTime(profileQuery.data.digest_time.slice(0, 5));
      setTimezone(profileQuery.data.timezone);
      setDataMode(profileQuery.data.data_mode);
      setSummaryDepth(profileQuery.data.summary_depth);
      setWeights(
        Object.fromEntries(
          Object.entries(profileQuery.data.ranking_weights).map(([key, value]) => [key, String(value)]),
        ),
      );
    }
  }, [profileQuery.data]);

  if (profileQuery.isLoading) {
    return <div className="page-loading">Loading profile…</div>;
  }

  if (profileQuery.error || !profileQuery.data) {
    return <div className="page-empty">Profile settings are unavailable right now. Refresh or check the backend.</div>;
  }

  const onSubmit = (event: FormEvent) => {
    event.preventDefault();
    saveProfile.mutate({
      favorite_topics: csvToList(favoriteTopics),
      favorite_authors: csvToList(favoriteAuthors),
      favorite_sources: csvToList(favoriteSources),
      ignored_topics: csvToList(ignoredTopics),
      digest_time: `${digestTime}:00`,
      timezone,
      data_mode: dataMode,
      summary_depth: summaryDepth,
      ranking_weights: Object.fromEntries(
        Object.entries(weights).map(([key, value]) => [key, Number(value)]),
      ),
    });
  };

  return (
    <div className="space-y-6 pb-10">
      <section className="editorial-panel">
        <p className="section-kicker">Taste model</p>
        <h3 className="section-title">Interest profile</h3>
        <SkimmableText className="mt-4 max-w-3xl text-base leading-7 text-[var(--muted)]">
          Keep the ranking legible. Tell the system what to amplify, what to ignore, and how aggressively the brief should compress.
        </SkimmableText>
      </section>

      <div className="page-breakout grid gap-6 xl:grid-cols-[minmax(0,1.2fr)_380px]">
        <form className="editorial-panel space-y-5" onSubmit={onSubmit}>
          <div>
            <p className="section-kicker">Profile controls</p>
            <SkimmableText className="mt-4 max-w-3xl text-base leading-7 text-[var(--muted)]">
              Adjust interests, digest timing, live-vs-seed mode, and the scoring weights that shape each morning brief.
            </SkimmableText>
          </div>
          <label>
            <span className="field-label">Favorite topics</span>
            <textarea className="field-input min-h-28" value={favoriteTopics} onChange={(event) => setFavoriteTopics(event.target.value)} />
          </label>
          <div className="grid gap-4 sm:grid-cols-2">
            <label>
              <span className="field-label">Favorite authors</span>
              <textarea className="field-input min-h-28" value={favoriteAuthors} onChange={(event) => setFavoriteAuthors(event.target.value)} />
            </label>
            <label>
              <span className="field-label">Favorite sources</span>
              <textarea className="field-input min-h-28" value={favoriteSources} onChange={(event) => setFavoriteSources(event.target.value)} />
            </label>
          </div>
          <label>
            <span className="field-label">Ignored topics</span>
            <textarea className="field-input min-h-28" value={ignoredTopics} onChange={(event) => setIgnoredTopics(event.target.value)} />
          </label>

          <div className="grid gap-4 sm:grid-cols-2">
            <label>
              <span className="field-label">Digest time</span>
              <input className="field-input" type="time" value={digestTime} onChange={(event) => setDigestTime(event.target.value)} />
            </label>
            <label>
              <span className="field-label">Timezone</span>
              <input
                className="field-input"
                list="timezone-options"
                onChange={(event) => setTimezone(event.target.value)}
                placeholder="Europe/Zurich"
                value={timezone}
              />
              <datalist id="timezone-options">
                {["Europe/Zurich", "Europe/London", "America/New_York", "America/Los_Angeles", "Asia/Tokyo"].map((zone) => (
                  <option key={zone} value={zone} />
                ))}
              </datalist>
            </label>
          </div>

          <div className="grid gap-4 sm:grid-cols-2">
            <label>
              <span className="field-label">Data mode</span>
              <select className="field-input" value={dataMode} onChange={(event) => setDataMode(event.target.value as DataMode)}>
                <option value="seed">Seed data</option>
                <option value="live">Live data</option>
              </select>
            </label>
            <label>
              <span className="field-label">Summary depth</span>
              <select className="field-input" value={summaryDepth} onChange={(event) => setSummaryDepth(event.target.value)}>
                <option value="compact">Compact</option>
                <option value="balanced">Balanced</option>
                <option value="deep">Deep</option>
              </select>
            </label>
          </div>

          <div className="space-y-3">
            <p className="field-label">Ranking weights</p>
            <div className="grid gap-3 sm:grid-cols-2">
              {Object.entries(weights).map(([key, value]) => (
                <label key={key}>
                  <span className="field-label capitalize">{key.replaceAll("_", " ")}</span>
                  <input
                    className="field-input"
                    onChange={(event) => setWeights({ ...weights, [key]: event.target.value })}
                    value={value}
                  />
                </label>
              ))}
            </div>
          </div>

          {saveProfile.isError ? (
            <div className="rounded-2xl border border-[var(--danger)]/20 bg-[rgba(255,255,255,0.56)] px-4 py-3 text-sm leading-6 text-[var(--danger)]">
              {saveProfile.error.message}
            </div>
          ) : null}
          {saveProfile.isSuccess ? (
            <div className="rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)] px-4 py-3 text-sm leading-6 text-[var(--muted-strong)]">
              Profile saved.
            </div>
          ) : null}
          <button className="primary-button" disabled={saveProfile.isPending} type="submit">
            {saveProfile.isPending ? "Saving profile..." : "Save profile"}
          </button>
        </form>

        <aside className="space-y-4">
          <section className="editorial-panel-dark">
            <p className="section-kicker text-[rgba(236,228,211,0.68)]">Current posture</p>
            <h4 className="mt-3 font-display text-3xl">What the system will bias toward</h4>
            <p className="mt-4 font-mono text-[11px] uppercase tracking-[0.22em] text-[rgba(236,228,211,0.68)]">
              {dataMode === "seed" ? "Seed data mode" : "Live data mode"}
            </p>
            <ul className="mt-5 space-y-3 text-sm leading-7 text-[rgba(236,228,211,0.92)]">
              {csvToList(favoriteTopics).slice(0, 6).map((topic) => (
                <li key={topic}>{topic}</li>
              ))}
            </ul>
          </section>
          <section className="editorial-panel">
            <p className="section-kicker">Data split</p>
            <SkimmableText className="mt-4 text-sm leading-7 text-[var(--muted)]">
              Seed mode keeps the demo records visible. Live mode shows ingested feeds and manual imports only, so you can inspect real extraction output without losing the demo setup.
            </SkimmableText>
          </section>
          <section className="editorial-panel">
            <p className="section-kicker">Ignored lane</p>
            <SkimmableText className="mt-4 text-sm leading-7 text-[var(--muted)]">
              Ignored topics lower ranking scores and keep repetitive clusters out of the brief. The goal is not perfect filtering. It is cleaner mornings.
            </SkimmableText>
          </section>
        </aside>
      </div>
    </div>
  );
}
