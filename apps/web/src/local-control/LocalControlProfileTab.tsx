import { type FormEvent, type ReactNode, useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import type { DataMode, Profile } from "../api/types";
import { SkimmableText } from "../components/SkimmableText";
import { LocalControlError, localControlClient } from "./client";

const RANKING_WEIGHT_HELP: Record<string, string> = {
  relevance: "Primary relevance signal. Uses the AI enrichment score when available, then falls back to heuristics.",
  novelty: "Keeps repetitive cluster members from dominating the shortlist.",
  source_quality: "Carries source priority into the final score.",
  author_match: "Rewards items from authors you explicitly follow.",
  topic_match: "Rewards titles, body text, and sources that match your stated interests.",
  zotero_affinity: "Rewards overlap with material already close to your Zotero library.",
};

type RankingThresholdForm = {
  [Key in keyof Profile["ranking_thresholds"]]: string;
};

type BriefSectionForm = {
  [Key in keyof Profile["brief_sections"]]: string;
};

type AudioBriefSettingsForm = {
  [Key in keyof Profile["audio_brief_settings"]]: string;
};

type PromptGuidanceKey = keyof Profile["prompt_guidance"];
type BriefSectionKey = keyof Profile["brief_sections"];

const DEFAULT_RANKING_THRESHOLDS: RankingThresholdForm = {
  must_read_min: "0.72",
  worth_a_skim_min: "0.45",
};

const DEFAULT_BRIEF_SECTIONS: BriefSectionForm = {
  editorial_shortlist_count: "3",
  headlines_count: "4",
  side_signals_count: "3",
  remaining_reads_count: "5",
  papers_count: "5",
  follow_up_questions_count: "5",
};

const DEFAULT_AUDIO_BRIEF_SETTINGS: AudioBriefSettingsForm = {
  target_duration_minutes: "5",
  max_items_per_section: "3",
};

const DEFAULT_PROMPT_GUIDANCE: Profile["prompt_guidance"] = {
  enrichment: "",
  editorial_note: "",
  audio_brief: "",
};

const RANKING_THRESHOLD_HELP: Record<keyof Profile["ranking_thresholds"], string> = {
  must_read_min: "Scores at or above this cut line land in the must-read lane.",
  worth_a_skim_min: "Scores below must-read but above this cut line become worth-a-skim items.",
};

const BRIEF_SECTION_META: Array<{
  key: BriefSectionKey;
  label: string;
  hint: string;
  min: number;
  max: number;
}> = [
  {
    key: "editorial_shortlist_count",
    label: "Editorial shortlist",
    hint: "Lead items carried into the top section and passed into the editorial note prompt.",
    min: 1,
    max: 12,
  },
  {
    key: "headlines_count",
    label: "Headlines",
    hint: "Fast-moving updates kept in the main headlines lane of the written brief.",
    min: 0,
    max: 20,
  },
  {
    key: "side_signals_count",
    label: "Side signals",
    hint: "Background signals that stay visible without taking over the lead section.",
    min: 0,
    max: 12,
  },
  {
    key: "remaining_reads_count",
    label: "Remaining reads",
    hint: "Longer reads that remain in the edition for later, lower-priority follow-through.",
    min: 0,
    max: 20,
  },
  {
    key: "papers_count",
    label: "Papers table",
    hint: "Research papers surfaced in the dedicated paper table block.",
    min: 0,
    max: 20,
  },
  {
    key: "follow_up_questions_count",
    label: "Follow-up questions",
    hint: "Heuristic follow-up prompts suggested at the bottom of the brief.",
    min: 0,
    max: 12,
  },
];

const AUDIO_BRIEF_FIELD_META: Array<{
  key: keyof Profile["audio_brief_settings"];
  label: string;
  hint: string;
  min: number;
  max: number;
}> = [
  {
    key: "target_duration_minutes",
    label: "Target duration (minutes)",
    hint: "Passed directly into the audio script prompt as the target spoken runtime.",
    min: 1,
    max: 30,
  },
  {
    key: "max_items_per_section",
    label: "Max items per section",
    hint: "Caps how many written brief entries from each section make it into the spoken shortlist.",
    min: 1,
    max: 10,
  },
];

const PROMPT_GUIDANCE_META: Array<{
  key: PromptGuidanceKey;
  label: string;
  placeholder: string;
  hint: string;
}> = [
  {
    key: "enrichment",
    label: "Enrichment guidance",
    placeholder: "Prefer practical tags, be skeptical about hype, emphasize tooling relevance.",
    hint: "Appended to the batch enrichment prompt that assigns relevance reasons, tags, and extracted authors.",
  },
  {
    key: "editorial_note",
    label: "Editorial note guidance",
    placeholder: "Keep the note dry and connective, emphasize implications over recap.",
    hint: "Appended to the editorial note prompt that writes the top paragraph of the daily brief.",
  },
  {
    key: "audio_brief",
    label: "Audio brief guidance",
    placeholder: "Sound more analytical than chatty, avoid podcast filler, foreground uncertainty.",
    hint: "Appended to the audio script prompt that turns the written digest into spoken narration.",
  },
];

function listToCsv(values: string[]) {
  return values.join(", ");
}

function csvToList(value: string) {
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function labelFromKey(value: string) {
  return value
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function readErrorMessage(error: unknown) {
  return error instanceof Error && error.message ? error.message : "Request failed.";
}

function PipelineCard({
  kicker,
  title,
  description,
  usedBy,
  children,
}: {
  kicker: string;
  title: string;
  description: string;
  usedBy: string[];
  children: ReactNode;
}) {
  return (
    <section className="editorial-panel space-y-5">
      <div className="space-y-4">
        <div>
          <p className="section-kicker">{kicker}</p>
          <h3 className="section-title">{title}</h3>
          <SkimmableText className="mt-4 max-w-3xl text-base leading-7 text-[var(--muted)]">{description}</SkimmableText>
        </div>
        <div className="flex flex-wrap gap-2">
          {usedBy.map((label) => (
            <span
              className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.46)] px-3 py-1.5 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--muted)]"
              key={label}
            >
              {label}
            </span>
          ))}
        </div>
      </div>
      {children}
    </section>
  );
}

function FieldHint({ children }: { children: ReactNode }) {
  return <p className="mt-2 text-sm leading-6 text-[var(--muted)]">{children}</p>;
}

function describeProfileRequestError(error: unknown) {
  if (error instanceof LocalControlError) {
    return error.message;
  }
  if (error instanceof Error && error.message) {
    return error.message;
  }
  return "Local profile settings are unavailable right now.";
}

export function LocalControlProfileTab() {
  const queryClient = useQueryClient();
  const profileQuery = useQuery({
    queryKey: ["local-control", "profile"],
    queryFn: localControlClient.getProfile,
    retry: false,
  });
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
  const [rankingThresholds, setRankingThresholds] = useState<RankingThresholdForm>(DEFAULT_RANKING_THRESHOLDS);
  const [briefSections, setBriefSections] = useState<BriefSectionForm>(DEFAULT_BRIEF_SECTIONS);
  const [audioBriefSettings, setAudioBriefSettings] = useState<AudioBriefSettingsForm>(DEFAULT_AUDIO_BRIEF_SETTINGS);
  const [promptGuidance, setPromptGuidance] = useState<Profile["prompt_guidance"]>(DEFAULT_PROMPT_GUIDANCE);

  const syncFormFromProfile = (profile: Profile) => {
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
    setRankingThresholds({
      must_read_min: String(profile.ranking_thresholds.must_read_min),
      worth_a_skim_min: String(profile.ranking_thresholds.worth_a_skim_min),
    });
    setBriefSections({
      editorial_shortlist_count: String(profile.brief_sections.editorial_shortlist_count),
      headlines_count: String(profile.brief_sections.headlines_count),
      side_signals_count: String(profile.brief_sections.side_signals_count),
      remaining_reads_count: String(profile.brief_sections.remaining_reads_count),
      papers_count: String(profile.brief_sections.papers_count),
      follow_up_questions_count: String(profile.brief_sections.follow_up_questions_count),
    });
    setAudioBriefSettings({
      target_duration_minutes: String(profile.audio_brief_settings.target_duration_minutes),
      max_items_per_section: String(profile.audio_brief_settings.max_items_per_section),
    });
    setPromptGuidance({
      enrichment: profile.prompt_guidance.enrichment,
      editorial_note: profile.prompt_guidance.editorial_note,
      audio_brief: profile.prompt_guidance.audio_brief,
    });
  };

  const saveProfile = useMutation({
    mutationFn: (payload: Record<string, unknown>) => localControlClient.updateProfile(payload),
    onSuccess: async (profile) => {
      await queryClient.invalidateQueries({ queryKey: ["local-control"] });
      syncFormFromProfile(profile);
    },
  });

  useEffect(() => {
    if (profileQuery.data) {
      syncFormFromProfile(profileQuery.data);
    }
  }, [profileQuery.data]);

  if (profileQuery.isLoading) {
    return <div className="page-loading">Loading profile…</div>;
  }

  if (profileQuery.error || !profileQuery.data) {
    return (
      <div className="page-empty">
        {profileQuery.error
          ? describeProfileRequestError(profileQuery.error)
          : "Local profile settings are unavailable right now. Refresh or check the Mac connection."}
      </div>
    );
  }

  const activeTopics = csvToList(favoriteTopics);
  const activeAuthors = csvToList(favoriteAuthors);
  const activeSources = csvToList(favoriteSources);
  const suppressedTopics = csvToList(ignoredTopics);
  const strongestWeight = Object.entries(weights)
    .map(([key, value]) => [key, Number(value)] as const)
    .filter((entry) => Number.isFinite(entry[1]))
    .sort((left, right) => right[1] - left[1])[0];
  const activeGuidanceCount = Object.values(promptGuidance).filter((value) => value.trim()).length;
  const briefItemCount =
    Number(briefSections.editorial_shortlist_count)
    + Number(briefSections.headlines_count)
    + Number(briefSections.side_signals_count)
    + Number(briefSections.remaining_reads_count)
    + Number(briefSections.papers_count);
  const thresholdWarning =
    Number(rankingThresholds.must_read_min) <= Number(rankingThresholds.worth_a_skim_min)
      ? "Must-read should stay above worth-a-skim or the bucket logic becomes ambiguous."
      : null;

  const onSubmit = (event: FormEvent) => {
    event.preventDefault();
    saveProfile.mutate({
      favorite_topics: activeTopics,
      favorite_authors: activeAuthors,
      favorite_sources: activeSources,
      ignored_topics: suppressedTopics,
      digest_time: `${digestTime}:00`,
      timezone,
      data_mode: dataMode,
      summary_depth: summaryDepth,
      ranking_weights: Object.fromEntries(
        Object.entries(weights).map(([key, value]) => [key, Number(value)]),
      ),
      ranking_thresholds: {
        must_read_min: Number(rankingThresholds.must_read_min),
        worth_a_skim_min: Number(rankingThresholds.worth_a_skim_min),
      },
      brief_sections: {
        editorial_shortlist_count: Number(briefSections.editorial_shortlist_count),
        headlines_count: Number(briefSections.headlines_count),
        side_signals_count: Number(briefSections.side_signals_count),
        remaining_reads_count: Number(briefSections.remaining_reads_count),
        papers_count: Number(briefSections.papers_count),
        follow_up_questions_count: Number(briefSections.follow_up_questions_count),
      },
      audio_brief_settings: {
        target_duration_minutes: Number(audioBriefSettings.target_duration_minutes),
        max_items_per_section: Number(audioBriefSettings.max_items_per_section),
      },
      prompt_guidance: promptGuidance,
    });
  };

  return (
    <div className="space-y-6">
      <section className="editorial-panel">
        <p className="section-kicker">Local configuration</p>
        <h3 className="section-title">Profile</h3>
        <SkimmableText className="mt-4 max-w-3xl text-base leading-7 text-[var(--muted)]">
          The local-control surface writes back to the same profile record the Mac uses for ranking, briefing, audio generation, and prompt steering. The sections below are grouped by where they take effect so the operator model stays legible.
        </SkimmableText>
      </section>

      <div className="grid gap-6 2xl:grid-cols-[minmax(0,1fr)_340px]">
        <form className="min-w-0 space-y-6" onSubmit={onSubmit}>
          <PipelineCard
            description="This gate decides which database-backed records remain visible in the app after ingestion. It acts before ranking and before the item list is assembled."
            kicker="Pipeline 01"
            title="Data visibility"
            usedBy={["Inbox", "Items API", "Item detail"]}
          >
            <div className="grid gap-4 lg:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]">
              <label>
                <span className="field-label">Data mode</span>
                <select className="field-input" onChange={(event) => setDataMode(event.target.value as DataMode)} value={dataMode}>
                  <option value="seed">Seed data</option>
                  <option value="live">Live data</option>
                </select>
                <FieldHint>
                  Seed keeps demo records in view. Live filters the system to ingested feeds and manual imports only.
                </FieldHint>
              </label>
              <div className="rounded-[1.4rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.42)] p-4">
                <p className="field-label">How it is applied</p>
                <SkimmableText className="mt-3 text-sm leading-7 text-[var(--muted)]">
                  This setting is a visibility gate, not a preference weight. It decides which stored records the app is allowed to show before any scoring or briefing logic gets a chance to act on them.
                </SkimmableText>
              </div>
            </div>
          </PipelineCard>

          <PipelineCard
            description="These signals express taste. They feed the item-enrichment prompt and they are also reused by the ranking heuristics, so changes here affect both AI annotation and later scoring."
            kicker="Pipeline 02"
            title="Interest signals"
            usedBy={["Enrichment prompt", "Topic matching", "Ignore penalties"]}
          >
            <label>
              <span className="field-label">Favorite topics</span>
              <textarea
                className="field-input min-h-28"
                onChange={(event) => setFavoriteTopics(event.target.value)}
                placeholder="agents, evals, inference, reasoning"
                value={favoriteTopics}
              />
              <FieldHint>Matched against item text during ranking and injected into the enrichment prompt as positive interest context.</FieldHint>
            </label>

            <div className="grid gap-4 sm:grid-cols-2">
              <label>
                <span className="field-label">Favorite authors</span>
                <textarea
                  className="field-input min-h-28"
                  onChange={(event) => setFavoriteAuthors(event.target.value)}
                  placeholder="Author names, comma separated"
                  value={favoriteAuthors}
                />
                <FieldHint>Boosts author-match scoring and tells enrichment which names should read as familiar or high-interest.</FieldHint>
              </label>
              <label>
                <span className="field-label">Favorite sources</span>
                <textarea
                  className="field-input min-h-28"
                  onChange={(event) => setFavoriteSources(event.target.value)}
                  placeholder="arXiv, Anthropic, OpenAI, Latent Space"
                  value={favoriteSources}
                />
                <FieldHint>Boosts source preference in ranking and gives the enricher a publication-level prior.</FieldHint>
              </label>
            </div>

            <label>
              <span className="field-label">Ignored topics</span>
              <textarea
                className="field-input min-h-28"
                onChange={(event) => setIgnoredTopics(event.target.value)}
                placeholder="topics to suppress or down-rank"
                value={ignoredTopics}
              />
              <FieldHint>Passed into enrichment as negative preference context and subtracted during score assembly when matched.</FieldHint>
            </label>
          </PipelineCard>

          <PipelineCard
            description="After enrichment lands, the ranking service combines the component scores with these weights and then applies the bucket thresholds. This is the part that decides must-read versus skim versus archive."
            kicker="Pipeline 03"
            title="Ranking model"
            usedBy={["Inbox ordering", "Bucket assignment", "Reason trace"]}
          >
            <div className="space-y-5">
              <div>
                <p className="field-label">Component weights</p>
                <div className="mt-3 grid gap-3 sm:grid-cols-2">
                  {Object.entries(weights).map(([key, value]) => (
                    <label
                      className="rounded-[1.45rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.44)] p-4"
                      key={key}
                    >
                      <span className="field-label">{labelFromKey(key)}</span>
                      <input
                        className="field-input"
                        inputMode="decimal"
                        onChange={(event) => setWeights({ ...weights, [key]: event.target.value })}
                        value={value}
                      />
                      <FieldHint>{RANKING_WEIGHT_HELP[key] ?? "Applied directly during score assembly."}</FieldHint>
                    </label>
                  ))}
                </div>
              </div>

              <div>
                <p className="field-label">Bucket thresholds</p>
                <div className="mt-3 grid gap-4 sm:grid-cols-2">
                  {Object.entries(rankingThresholds).map(([key, value]) => (
                    <label key={key}>
                      <span className="field-label">{labelFromKey(key)}</span>
                      <input
                        className="field-input"
                        inputMode="decimal"
                        max="1"
                        min="0"
                        onChange={(event) =>
                          setRankingThresholds({
                            ...rankingThresholds,
                            [key]: event.target.value,
                          })
                        }
                        step="0.01"
                        type="number"
                        value={value}
                      />
                      <FieldHint>{RANKING_THRESHOLD_HELP[key as keyof Profile["ranking_thresholds"]]}</FieldHint>
                    </label>
                  ))}
                </div>
                {thresholdWarning ? (
                  <div className="mt-4 rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)] px-4 py-3 text-sm leading-6 text-[var(--muted-strong)]">
                    {thresholdWarning}
                  </div>
                ) : null}
              </div>
            </div>
          </PipelineCard>

          <PipelineCard
            description="These controls determine which local day counts as the current edition for this profile and when the next daily brief is considered due."
            kicker="Pipeline 04"
            title="Brief schedule"
            usedBy={["Edition date", "Daily scheduler", "Vault runtime"]}
          >
            <div className="grid gap-4 sm:grid-cols-2">
              <label>
                <span className="field-label">Digest time</span>
                <input className="field-input" onChange={(event) => setDigestTime(event.target.value)} type="time" value={digestTime} />
                <FieldHint>The scheduler compares the current local time against this clock time before deciding the day’s brief is due.</FieldHint>
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
                <FieldHint>This timezone is used to interpret the digest clock and to resolve the profile’s current local date.</FieldHint>
              </label>
            </div>
          </PipelineCard>

          <PipelineCard
            description="These controls shape the written brief itself: how dense the narration prompts should be, how many items land in each section, and how many follow-up questions get proposed."
            kicker="Pipeline 05"
            title="Brief drafting"
            usedBy={["Digest builder", "Editorial note prompt", "Follow-up generator"]}
          >
            <label>
              <span className="field-label">Summary depth</span>
              <select className="field-input" onChange={(event) => setSummaryDepth(event.target.value)} value={summaryDepth}>
                <option value="compact">Compact</option>
                <option value="balanced">Balanced</option>
                <option value="deep">Deep</option>
              </select>
              <FieldHint>
                Passed into the editorial note and audio brief prompts as the preferred briefing depth so the generated narration can skew tighter or more expansive.
              </FieldHint>
            </label>

            <div>
              <p className="field-label">Section sizing</p>
              <div className="mt-3 grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
                {BRIEF_SECTION_META.map(({ key, label, hint, min, max }) => (
                  <label key={key}>
                    <span className="field-label">{label}</span>
                    <input
                      className="field-input"
                      max={String(max)}
                      min={String(min)}
                      onChange={(event) =>
                        setBriefSections({
                          ...briefSections,
                          [key]: event.target.value,
                        })
                      }
                      step="1"
                      type="number"
                      value={briefSections[key]}
                    />
                    <FieldHint>{hint}</FieldHint>
                  </label>
                ))}
              </div>
            </div>
          </PipelineCard>

          <PipelineCard
            description="These settings tune the spoken brief: how long it should aim to run and how aggressively the written sections are condensed before script generation."
            kicker="Pipeline 06"
            title="Audio brief"
            usedBy={["Audio shortlist", "Audio script prompt", "TTS generation"]}
          >
            <div className="grid gap-4 sm:grid-cols-2">
              {AUDIO_BRIEF_FIELD_META.map(({ key, label, hint, min, max }) => (
                <label key={key}>
                  <span className="field-label">{label}</span>
                  <input
                    className="field-input"
                    max={String(max)}
                    min={String(min)}
                    onChange={(event) =>
                      setAudioBriefSettings({
                        ...audioBriefSettings,
                        [key]: event.target.value,
                      })
                    }
                    step="1"
                    type="number"
                    value={audioBriefSettings[key]}
                  />
                  <FieldHint>{hint}</FieldHint>
                </label>
              ))}
            </div>
          </PipelineCard>

          <PipelineCard
            description="These are short operator notes appended to the built-in prompts. Use them to steer tone, emphasis, or vocabulary without replacing the underlying instructions."
            kicker="Pipeline 07"
            title="Prompt guidance"
            usedBy={["Enrichment prompt", "Editorial note prompt", "Audio brief prompt"]}
          >
            <div className="space-y-4">
              {PROMPT_GUIDANCE_META.map(({ key, label, placeholder, hint }) => (
                <label key={key}>
                  <span className="field-label">{label}</span>
                  <textarea
                    className="field-input min-h-28"
                    onChange={(event) =>
                      setPromptGuidance({
                        ...promptGuidance,
                        [key]: event.target.value,
                      })
                    }
                    placeholder={placeholder}
                    value={promptGuidance[key]}
                  />
                  <FieldHint>{hint}</FieldHint>
                </label>
              ))}
            </div>
          </PipelineCard>

          <section className="editorial-panel space-y-4">
            <div>
              <p className="section-kicker">Save profile</p>
              <SkimmableText className="mt-4 max-w-3xl text-sm leading-7 text-[var(--muted)]">
                All seven sections write back to the shared local profile record so the downstream pipelines stay aligned while still being configured separately.
              </SkimmableText>
            </div>
            {saveProfile.isError ? (
              <div className="rounded-2xl border border-[var(--danger)]/20 bg-[rgba(255,255,255,0.56)] px-4 py-3 text-sm leading-6 text-[var(--danger)]">
                {readErrorMessage(saveProfile.error)}
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
          </section>
        </form>

        <aside className="min-w-0 space-y-4">
          <section className="editorial-panel-dark">
            <p className="section-kicker text-[rgba(236,228,211,0.68)]">Current posture</p>
            <h4 className="mt-3 font-display text-3xl leading-tight">How the local system is wired right now</h4>
            <div className="mt-5 space-y-4">
              <div className="rounded-[1.5rem] border border-white/10 bg-white/6 p-4">
                <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-[rgba(236,228,211,0.68)]">Visible data</p>
                <p className="mt-3 text-sm leading-6 text-[rgba(236,228,211,0.92)]">
                  {dataMode === "seed" ? "Seed mode keeps demo records in scope." : "Live mode hides demo data and keeps only real ingest/manual items in scope."}
                </p>
              </div>
              <div className="rounded-[1.5rem] border border-white/10 bg-white/6 p-4">
                <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-[rgba(236,228,211,0.68)]">Ranking cut lines</p>
                <p className="mt-3 text-sm leading-6 text-[rgba(236,228,211,0.92)]">
                  Must-read at {rankingThresholds.must_read_min}, worth-a-skim at {rankingThresholds.worth_a_skim_min}
                </p>
              </div>
              <div className="rounded-[1.5rem] border border-white/10 bg-white/6 p-4">
                <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-[rgba(236,228,211,0.68)]">Strongest score weight</p>
                <p className="mt-3 text-sm leading-6 text-[rgba(236,228,211,0.92)]">
                  {strongestWeight ? `${labelFromKey(strongestWeight[0])} · ${strongestWeight[1].toFixed(2)}` : "No ranking weights configured"}
                </p>
              </div>
              <div className="rounded-[1.5rem] border border-white/10 bg-white/6 p-4">
                <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-[rgba(236,228,211,0.68)]">Written brief mix</p>
                <p className="mt-3 text-sm leading-6 text-[rgba(236,228,211,0.92)]">
                  {briefItemCount} surfaced items, {briefSections.follow_up_questions_count} follow-up prompts, {summaryDepth} depth.
                </p>
              </div>
              <div className="rounded-[1.5rem] border border-white/10 bg-white/6 p-4">
                <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-[rgba(236,228,211,0.68)]">Audio brief shape</p>
                <p className="mt-3 text-sm leading-6 text-[rgba(236,228,211,0.92)]">
                  {audioBriefSettings.target_duration_minutes} minute target, up to {audioBriefSettings.max_items_per_section} items per section.
                </p>
              </div>
              <div className="rounded-[1.5rem] border border-white/10 bg-white/6 p-4">
                <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-[rgba(236,228,211,0.68)]">Daily brief clock</p>
                <p className="mt-3 text-sm leading-6 text-[rgba(236,228,211,0.92)]">
                  {digestTime} in {timezone}
                </p>
              </div>
            </div>
          </section>

          <section className="editorial-panel">
            <p className="section-kicker">Pipeline map</p>
            <div className="mt-4 space-y-4">
              <div>
                <p className="text-sm font-semibold leading-6 text-[var(--ink)]">Visibility</p>
                <p className="text-sm leading-6 text-[var(--muted)]">Filters which stored records the local app is allowed to show before any downstream logic runs.</p>
              </div>
              <div>
                <p className="text-sm font-semibold leading-6 text-[var(--ink)]">Interest signals</p>
                <p className="text-sm leading-6 text-[var(--muted)]">Feeds both the enrichment prompt and the topic or ignore matches used by ranking.</p>
              </div>
              <div>
                <p className="text-sm font-semibold leading-6 text-[var(--ink)]">Ranking model</p>
                <p className="text-sm leading-6 text-[var(--muted)]">Assembles the final score and applies bucket thresholds for must-read, skim, and archive.</p>
              </div>
              <div>
                <p className="text-sm font-semibold leading-6 text-[var(--ink)]">Brief schedule</p>
                <p className="text-sm leading-6 text-[var(--muted)]">Controls when the profile’s local edition date turns over and when a new brief becomes due.</p>
              </div>
              <div>
                <p className="text-sm font-semibold leading-6 text-[var(--ink)]">Brief drafting</p>
                <p className="text-sm leading-6 text-[var(--muted)]">Shapes the written digest by setting section sizes, follow-up count, and preferred briefing depth.</p>
              </div>
              <div>
                <p className="text-sm font-semibold leading-6 text-[var(--ink)]">Audio brief</p>
                <p className="text-sm leading-6 text-[var(--muted)]">Shrinks the written brief into a spoken shortlist and passes target runtime into the audio prompt.</p>
              </div>
              <div>
                <p className="text-sm font-semibold leading-6 text-[var(--ink)]">Prompt guidance</p>
                <p className="text-sm leading-6 text-[var(--muted)]">Adds short operator notes to enrichment, editorial, and audio prompts without replacing the core templates.</p>
              </div>
            </div>
          </section>

          <section className="editorial-panel">
            <p className="section-kicker">Active signals</p>
            <div className="mt-4 space-y-4 text-sm leading-6 text-[var(--muted)]">
              <div>
                <p className="font-semibold text-[var(--ink)]">Topics in play</p>
                <p>{activeTopics.length ? activeTopics.slice(0, 6).join(", ") : "No favorite topics configured yet."}</p>
              </div>
              <div>
                <p className="font-semibold text-[var(--ink)]">Authors tracked</p>
                <p>{activeAuthors.length ? activeAuthors.slice(0, 4).join(", ") : "No author preferences configured yet."}</p>
              </div>
              <div>
                <p className="font-semibold text-[var(--ink)]">Sources favored</p>
                <p>{activeSources.length ? activeSources.slice(0, 4).join(", ") : "No source preferences configured yet."}</p>
              </div>
              <div>
                <p className="font-semibold text-[var(--ink)]">Suppressed topics</p>
                <p>{suppressedTopics.length ? suppressedTopics.slice(0, 4).join(", ") : "No ignored topics configured yet."}</p>
              </div>
              <div>
                <p className="font-semibold text-[var(--ink)]">Prompt nudges</p>
                <p>{activeGuidanceCount ? `${activeGuidanceCount} prompt guidance block${activeGuidanceCount === 1 ? "" : "s"} active.` : "No prompt guidance configured yet."}</p>
              </div>
            </div>
          </section>
        </aside>
      </div>
    </div>
  );
}
