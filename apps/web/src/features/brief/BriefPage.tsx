import { useEffect, useMemo, useState } from "react";
import { ArrowUpRight, FileText, Lightbulb, LoaderCircle, Orbit, Sparkles } from "lucide-react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useOutletContext, useSearchParams } from "react-router-dom";

import { ApiError, api } from "../../api/client";
import type { BriefAvailability, BriefPeriodType, Digest, DigestEntry, ItemListEntry, PaperTableEntry } from "../../api/types";
import { AudioBriefPlayer } from "../../components/AudioBriefPlayer";
import { ImportantButton } from "../../components/ImportantButton";
import { ItemCard } from "../../components/ItemCard";
import { SkimmableText } from "../../components/SkimmableText";
import type { AppShellOutletContext } from "../../layout/AppShell";

const contentTypeLabel: Record<DigestEntry["item"]["content_type"], string> = {
  article: "Article",
  news: "News",
  paper: "Paper",
  newsletter: "Newsletter",
  post: "Post",
  thread: "Thread",
  signal: "Signal",
};

type SelectedPeriod =
  | { periodType: "day"; value: string }
  | { periodType: "week"; value: string };

const HEADLINE_SENTENCE_VERB_PATTERN =
  /\b(?:is|are|was|were|be|been|being|has|have|had|can|could|will|would|should|may|might|must|do|does|did|announces?|launch(?:es|ed)?|release(?:s|d)?|introduce(?:s|d)?|show(?:s|ed)?|focus(?:es|ed)?|expand(?:s|ed)?|improve(?:s|d)?|cut(?:s)?|raise(?:s|d)?|keep(?:s)?|make(?:s|made)?|argue(?:s|d)?|say(?:s|said)?|report(?:s|ed)?|reveal(?:s|ed)?|target(?:s|ed)?|open(?:s|ed)|build(?:s|ing|built)?|ship(?:s|ped)?|add(?:s|ed)?)\b/i;

function normalizeInlineText(text: string | null | undefined) {
  const normalized = text?.replace(/\s+/g, " ").trim();
  return normalized || null;
}

function stripTerminalPunctuation(text: string) {
  return text.replace(/[.!?]+$/, "").trim();
}

function toSingleSentence(text: string | null | undefined) {
  const normalized = normalizeInlineText(text);
  if (!normalized) return null;

  const sentenceMatch = normalized.match(/^.+?[.!?](?=\s|$)/);
  const sentence = sentenceMatch?.[0] ?? normalized;
  return /[.!?]$/.test(sentence) ? sentence : `${sentence}.`;
}

function looksLikeCompactHeadlineText(text: string) {
  const normalized = stripTerminalPunctuation(text);
  if (!normalized) return false;
  if (/[.!?]/.test(normalized)) return false;
  if (HEADLINE_SENTENCE_VERB_PATTERN.test(normalized)) return false;

  const phraseCount = normalized
    .split(/[,;]+/)
    .map((segment) => segment.trim())
    .filter(Boolean).length;
  const wordCount = normalized.split(/\s+/).filter(Boolean).length;

  return phraseCount >= 2 || wordCount <= 4;
}

function joinPhraseList(parts: string[]) {
  if (parts.length === 1) return parts[0];
  if (parts.length === 2) return `${parts[0]} and ${parts[1]}`;
  return `${parts.slice(0, -1).join(", ")}, and ${parts[parts.length - 1]}`;
}

function expandCompactHeadlineText(text: string | null | undefined) {
  const normalized = normalizeInlineText(text);
  if (!normalized || !looksLikeCompactHeadlineText(normalized)) return null;

  const parts = normalized
    .split(/[,;]+/)
    .map((segment) => stripTerminalPunctuation(segment.trim()))
    .filter(Boolean);
  const subject = joinPhraseList(parts.length ? parts : [stripTerminalPunctuation(normalized)]);

  return `This update centers on ${subject}.`;
}

function getHeadlineSummary(entry: DigestEntry) {
  const shortSummary = normalizeInlineText(entry.item.short_summary);
  if (shortSummary && !looksLikeCompactHeadlineText(shortSummary)) {
    return toSingleSentence(shortSummary) ?? "Open the source directly for the full context.";
  }

  const note = normalizeInlineText(entry.note);
  if (note && !looksLikeCompactHeadlineText(note)) {
    return toSingleSentence(note) ?? "Open the source directly for the full context.";
  }

  return (
    expandCompactHeadlineText(shortSummary) ??
    expandCompactHeadlineText(note) ??
    toSingleSentence(shortSummary) ??
    toSingleSentence(note) ??
    "Open the source directly for the full context."
  );
}

function getHeadlineOrigin(entry: DigestEntry) {
  return `${entry.item.source_name} · ${contentTypeLabel[entry.item.content_type]}`;
}

function getBriefByline(item: ItemListEntry) {
  return item.organization_name?.trim() || item.authors.join(", ") || item.source_name.trim() || "Unknown source";
}

function formatDisplayDate(value: string) {
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(new Date(`${value}T12:00:00`));
}

function formatDisplayDateCompact(value: string) {
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
  }).format(new Date(`${value}T12:00:00`));
}

function formatDateRange(start: string, end: string) {
  if (start === end) return formatDisplayDate(start);
  return `${formatDisplayDateCompact(start)} to ${formatDisplayDate(end)}`;
}

function buildSearchParams(selection: SelectedPeriod) {
  return selection.periodType === "day"
    ? new URLSearchParams({ period: "day", date: selection.value })
    : new URLSearchParams({ period: "week", start: selection.value });
}

function resolveSelectedPeriod(
  searchParams: URLSearchParams,
  availability: BriefAvailability | undefined,
): SelectedPeriod | null {
  if (!availability) return null;

  const validDays = new Set(availability.days.map((day) => day.brief_date));
  const validWeeks = new Set(availability.weeks.map((week) => week.week_start));
  const requestedPeriod = searchParams.get("period");
  const requestedDate = searchParams.get("date");
  const requestedWeekStart = searchParams.get("start");

  if (requestedPeriod === "week" && requestedWeekStart && validWeeks.has(requestedWeekStart)) {
    return { periodType: "week", value: requestedWeekStart };
  }
  if (requestedPeriod === "day" && requestedDate && validDays.has(requestedDate)) {
    return { periodType: "day", value: requestedDate };
  }
  if (availability.default_day) {
    return { periodType: "day", value: availability.default_day };
  }
  return null;
}

function Section({
  label,
  title,
  description,
  items,
}: {
  label: string;
  title: string;
  description: string;
  items: DigestEntry[];
}) {
  if (!items.length) return null;

  return (
    <section className="space-y-4">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <p className="section-kicker">{label}</p>
          <h3 className="section-title">{title}</h3>
          <SkimmableText className="mt-3 max-w-3xl text-base leading-7 text-[var(--muted)]">{description}</SkimmableText>
        </div>
        <div className="inline-flex w-fit items-center gap-2 rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
          <span>{String(items.length).padStart(2, "0")}</span>
          <span>Items</span>
        </div>
      </div>
      <div className="grid gap-4 lg:grid-cols-2 2xl:grid-cols-3">
        {items.map((entry) => (
          <ItemCard
            byline={getBriefByline(entry.item)}
            key={entry.item.id}
            item={entry.item}
            note={entry.note}
            publishedAtFormat="date"
          />
        ))}
      </div>
    </section>
  );
}

function Headlines({ items }: { items: DigestEntry[] }) {
  const [expanded, setExpanded] = useState(false);
  if (!items.length) return null;

  const maxExpandedCount = Math.min(items.length, 20);
  const collapsedCount = Math.min(6, maxExpandedCount);
  const visibleCount = expanded ? maxExpandedCount : collapsedCount;
  const visibleItems = items.slice(0, visibleCount);
  const canExpand = maxExpandedCount > collapsedCount;
  const expandLabel =
    maxExpandedCount >= 20 ? "Expand to top 20" : `Expand to ${maxExpandedCount} reads`;

  return (
    <section className="editorial-panel">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <p className="section-kicker">Headlines</p>
          <h3 className="section-title">Short AI buzz and compact news</h3>
          <SkimmableText className="mt-3 max-w-[72rem] text-base leading-7 text-[var(--muted)]">
            Short AI buzz stories, company statements, launches, and other important small-scope updates arranged for a quick first pass.
          </SkimmableText>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <div className="inline-flex w-fit items-center gap-2 rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
            <span>{String(visibleItems.length).padStart(2, "0")}</span>
            <span>Shown</span>
          </div>
          {canExpand ? (
            <button className="secondary-button" onClick={() => setExpanded((current) => !current)} type="button">
              {expanded ? "Show top 6" : expandLabel}
            </button>
          ) : null}
        </div>
      </div>

      <div className="mt-6 overflow-hidden rounded-[1.75rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.48)]">
        {visibleItems.map((entry, index) => (
          <article
            key={entry.item.id}
            className="group grid gap-3 border-b border-[var(--ink)]/8 px-5 py-4 transition last:border-b-0 hover:bg-[rgba(255,255,255,0.36)] focus-within:bg-[rgba(255,255,255,0.36)] md:grid-cols-[minmax(0,3fr)_minmax(220px,1fr)] md:items-start md:gap-6 md:px-6"
          >
            <a className="min-w-0" href={entry.item.canonical_url} rel="noreferrer" target="_blank">
              <div className="flex items-start gap-4">
                <span className="mt-0.5 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
                  {String(index + 1).padStart(2, "0")}
                </span>
                <div className="min-w-0">
                  <h4 className="text-lg font-semibold leading-7 text-[var(--ink)] transition group-hover:text-[var(--accent)]">
                    {entry.item.title}
                  </h4>
                  <SkimmableText className="mt-2 max-w-none text-sm leading-6 text-[var(--muted-strong)]">
                    {getHeadlineSummary(entry)}
                  </SkimmableText>
                </div>
              </div>
            </a>

            <div className="flex items-center justify-between gap-4 border-t border-[var(--ink)]/8 pt-3 md:border-t-0 md:pt-0">
              <div className="text-left md:text-right">
                <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">Origin</p>
                <p className="mt-1 text-sm leading-6 text-[var(--ink)]">{getHeadlineOrigin(entry)}</p>
              </div>
              <div className="flex items-center gap-2">
                <ImportantButton iconOnly itemId={entry.item.id} starred={entry.item.starred} />
                <a
                  aria-label={`Open source for ${entry.item.title}`}
                  className="flex h-11 w-11 items-center justify-center rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.78)] text-[var(--muted)] transition hover:-translate-y-0.5 hover:border-[var(--accent)]/26 hover:text-[var(--accent)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--accent)]/18"
                  href={entry.item.canonical_url}
                  rel="noreferrer"
                  target="_blank"
                  title="Open source"
                >
                  <ArrowUpRight className="h-4 w-4 shrink-0" />
                  <span className="sr-only">Open source</span>
                </a>
              </div>
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}

function PapersTable({ items }: { items: PaperTableEntry[] }) {
  if (!items.length) return null;

  return (
    <section className="space-y-4">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <p className="section-kicker">Papers</p>
          <h3 className="section-title">Top papers</h3>
          <SkimmableText className="mt-3 max-w-4xl text-base leading-7 text-[var(--muted)]">
            The strongest research papers in this window, annotated with best-effort Zotero tags and a credibility score
            based on lab prestige, citations, and source quality.
          </SkimmableText>
        </div>
        <div className="inline-flex w-fit items-center gap-2 rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
          <span>{String(items.length).padStart(2, "0")}</span>
          <span>Papers</span>
        </div>
      </div>

      <div className="overflow-hidden rounded-[1.75rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.48)]">
        <div className="hidden grid-cols-[minmax(0,2.3fr)_minmax(0,1.6fr)_120px] gap-4 border-b border-[var(--ink)]/8 px-6 py-4 md:grid">
          <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">Paper</p>
          <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">Tags</p>
          <p className="text-right font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">Credibility</p>
        </div>

        {items.map((entry) => (
          <div
            className="grid gap-4 border-b border-[var(--ink)]/8 px-5 py-5 last:border-b-0 md:grid-cols-[minmax(0,2.3fr)_minmax(0,1.6fr)_120px] md:px-6"
            key={entry.item.id}
          >
            <div className="min-w-0">
              <div className="flex items-start gap-4">
                <span className="mt-0.5 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
                  {String(entry.rank).padStart(2, "0")}
                </span>
                <div className="min-w-0">
                  <Link className="text-lg font-semibold leading-7 text-[var(--ink)] hover:text-[var(--accent)]" to={`/items/${entry.item.id}`}>
                    {entry.item.title}
                  </Link>
                  <p className="mt-2 text-sm leading-6 text-[var(--muted-strong)]">
                    {getBriefByline(entry.item)} · {entry.item.source_name}
                  </p>
                </div>
              </div>
            </div>

            <div className="flex flex-wrap gap-2">
              {entry.zotero_tags.length ? (
                entry.zotero_tags.map((tag) => (
                  <span
                    className="inline-flex items-center rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.72)] px-3 py-1 font-mono text-[11px] uppercase tracking-[0.14em] text-[var(--muted-strong)]"
                    key={tag}
                  >
                    {tag}
                  </span>
                ))
              ) : (
                <span className="text-sm leading-6 text-[var(--muted)]">No matched Zotero tags.</span>
              )}
            </div>

            <div className="flex items-center justify-between gap-4 md:justify-end">
              <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)] md:hidden">Credibility</p>
              <span className="inline-flex items-center rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.72)] px-4 py-2 font-mono text-sm uppercase tracking-[0.18em] text-[var(--ink)]">
                {entry.credibility_score ?? "--"}
              </span>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function AudioBriefSummaryStrip({ brief }: { brief: Digest }) {
  const queryClient = useQueryClient();
  const audioBrief = brief.audio_brief;
  const generateAudio = useMutation({
    mutationFn: () => api.generateAudioSummary(brief.brief_date ?? ""),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ["briefs"] });
    },
  });

  const isBuilding =
    audioBrief?.status === "running" || audioBrief?.status === "pending" || generateAudio.isPending;
  const notice =
    generateAudio.isError
      ? (generateAudio.error as ApiError).message
      : audioBrief?.error ?? null;
  const helperText =
    isBuilding
      ? "Building the voice summary."
      : audioBrief?.status === "failed"
        ? "The voice summary needs another pass."
        : "Generate the voice summary to listen from the top of the brief.";

  return (
    <div className="space-y-3 border-t border-[var(--ink)]/8 pt-5">
      {audioBrief?.status === "succeeded" ? (
        <>
          <AudioBriefPlayer audioBrief={audioBrief} briefDate={brief.brief_date ?? ""} />
          {notice ? <p className="text-sm leading-6 text-[var(--danger)]">{notice}</p> : null}
        </>
      ) : (
        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <p className={`text-sm leading-6 ${notice ? "text-[var(--danger)]" : "text-[var(--muted)]"}`}>
            {notice ?? helperText}
          </p>
          <button className="secondary-button w-fit shrink-0" disabled={isBuilding} onClick={() => generateAudio.mutate()} type="button">
            {isBuilding ? <LoaderCircle className="h-4 w-4 animate-spin" /> : null}
            {isBuilding ? "Building voice summary..." : "Generate voice summary"}
          </button>
        </div>
      )}
    </div>
  );
}

function PeriodSelector({
  availability,
  selectedPeriod,
  onChange,
}: {
  availability: BriefAvailability;
  selectedPeriod: SelectedPeriod | null;
  onChange: (selection: SelectedPeriod) => void;
}) {
  const selectedValue = selectedPeriod ? `${selectedPeriod.periodType}:${selectedPeriod.value}` : "";

  return (
    <label className="block w-full">
      <span className="field-label">Edition window</span>
      <select
        className="field-input mt-2"
        onChange={(event) => {
          const [periodType, value] = event.target.value.split(":");
          if ((periodType === "day" || periodType === "week") && value) {
            onChange({ periodType: periodType as BriefPeriodType, value });
          }
        }}
        value={selectedValue}
      >
        <optgroup label="Daily editions">
          {availability.days.map((day) => (
            <option key={day.brief_date} value={`day:${day.brief_date}`}>
              {formatDisplayDate(day.brief_date)}
            </option>
          ))}
        </optgroup>
        {availability.weeks.length ? (
          <optgroup label="Completed ISO weeks">
            {availability.weeks.map((week) => (
              <option key={week.week_start} value={`week:${week.week_start}`}>
                {formatDateRange(week.week_start, week.week_end)}
              </option>
            ))}
          </optgroup>
        ) : null}
      </select>
    </label>
  );
}

export function BriefPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const { setHeaderActions } = useOutletContext<AppShellOutletContext>();
  const availability = useQuery({
    queryKey: ["briefs", "availability"],
    queryFn: api.getBriefAvailability,
  });

  const selectedPeriod = useMemo(
    () => resolveSelectedPeriod(searchParams, availability.data),
    [availability.data, searchParams],
  );

  useEffect(() => {
    if (!selectedPeriod) return;
    const normalized = buildSearchParams(selectedPeriod).toString();
    if (searchParams.toString() === normalized) return;
    setSearchParams(buildSearchParams(selectedPeriod), { replace: true });
  }, [searchParams, selectedPeriod, setSearchParams]);

  useEffect(() => {
    if (!availability.data) {
      setHeaderActions(null);
      return;
    }

    setHeaderActions(
      <PeriodSelector
        availability={availability.data}
        onChange={(selection) => setSearchParams(buildSearchParams(selection))}
        selectedPeriod={selectedPeriod}
      />,
    );

    return () => setHeaderActions(null);
  }, [availability.data, selectedPeriod, setHeaderActions, setSearchParams]);

  const brief = useQuery({
    queryKey: ["briefs", selectedPeriod?.periodType, selectedPeriod?.value],
    queryFn: () =>
      selectedPeriod?.periodType === "week"
        ? api.getWeekBrief(selectedPeriod.value)
        : api.getBrief(selectedPeriod?.value ?? ""),
    enabled: Boolean(selectedPeriod),
  });

  if (availability.isLoading && !availability.data) {
    return <div className="page-loading">Loading available editions…</div>;
  }

  if (availability.error || !availability.data) {
    return <div className="page-empty">Brief availability is not available right now.</div>;
  }

  if (!selectedPeriod) {
    return <div className="page-empty">No brief windows are available yet.</div>;
  }

  if (brief.isLoading) {
    return <div className="page-loading">Building the selected brief…</div>;
  }

  if (brief.error || !brief.data) {
    return <div className="page-empty">That brief window is not available yet.</div>;
  }

  const generatedAt = new Intl.DateTimeFormat(undefined, {
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date(brief.data.generated_at));
  const [shortlistLead, ...restShortlist] = brief.data.editorial_shortlist;
  const firstReadItem =
    shortlistLead?.item ??
    brief.data.papers_table[0]?.item ??
    brief.data.headlines[0]?.item ??
    brief.data.interesting_side_signals[0]?.item ??
    brief.data.remaining_reads[0]?.item ??
    null;
  const quickStats = [
    {
      label: "Headlines",
      value: brief.data.headlines.length,
      blurb:
        brief.data.period_type === "week"
          ? "Short AI buzz, compact news, and company updates that stayed visible across the completed week."
          : "Short AI buzz, compact news, and company updates worth clocking early.",
      icon: Sparkles,
    },
    {
      label: "Shortlist",
      value: brief.data.editorial_shortlist.length,
      blurb:
        brief.data.period_type === "week"
          ? "The biggest articles or papers that held up across several days."
          : "The biggest articles or papers in this edition that should not be missed.",
      icon: FileText,
    },
    {
      label: "Side signals",
      value: brief.data.interesting_side_signals.length,
      blurb:
        brief.data.period_type === "week"
          ? "Peripheral movement, trend drift, and ecosystem signals that persisted through the selected week."
          : "Peripheral movement, trend drift, and ecosystem signals worth keeping in the background.",
      icon: Orbit,
    },
    {
      label: "Papers",
      value: brief.data.papers_table.length,
      blurb: "Top papers ranked independently, with tags and credibility to support quick scanning.",
      icon: FileText,
    },
    {
      label: "Remaining",
      value: brief.data.remaining_reads.length,
      blurb:
        brief.data.period_type === "week"
          ? "Additional reads that stayed worth keeping once the higher-signal weekly lanes were filled."
          : "Everything still worth keeping once the higher-signal lanes have been filled.",
      icon: ArrowUpRight,
    },
  ];
  const editionLabel =
    brief.data.period_type === "week" && brief.data.week_start && brief.data.week_end
      ? formatDateRange(brief.data.week_start, brief.data.week_end)
      : brief.data.brief_date
        ? formatDisplayDate(brief.data.brief_date)
        : "";
  const coverageLabel = formatDateRange(brief.data.coverage_start, brief.data.coverage_end);

  return (
    <div className="space-y-8 pb-10">
      <section className="editorial-panel overflow-hidden">
        <div className="grid gap-6 xl:grid-cols-[minmax(0,1.35fr)_minmax(320px,0.9fr)]">
          <div className="space-y-6">
            <div className="flex flex-wrap gap-2">
              <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.58)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.24em] text-[var(--muted)]">
                {brief.data.data_mode === "seed" ? "Seed data" : "Live data"}
              </span>
              <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.58)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.24em] text-[var(--muted)]">
                {brief.data.period_type === "week" ? "Weekly recap" : "Daily edition"}
              </span>
              <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.42)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.24em] text-[var(--muted)]">
                Updated {generatedAt}
              </span>
            </div>
            <div>
              <p className="section-kicker">{brief.data.period_type === "week" ? "Week overview" : "Overview"}</p>
              <SkimmableText className="mt-4 max-w-4xl text-lg leading-8 text-[var(--muted-strong)]">
                {brief.data.editorial_note ?? "A concise sweep of the strongest research signals in this window."}
              </SkimmableText>
              <SkimmableText className="mt-4 max-w-4xl text-sm leading-6 text-[var(--muted)]">
                Total score blends recency, novelty, source quality, author and topic matches, and Zotero affinity, then
                applies manual boosts and ignored-topic penalties using the current profile weights.
              </SkimmableText>
            </div>

            {brief.data.period_type === "day" && brief.data.brief_date ? <AudioBriefSummaryStrip brief={brief.data} /> : null}

            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
              {quickStats.map(({ label, value, blurb, icon: Icon }) => (
                <article
                  key={label}
                  className="rounded-[1.65rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.54)] px-5 py-5"
                >
                  <div className="flex items-center justify-between gap-4">
                    <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">{label}</p>
                    <Icon className="h-4 w-4 text-[var(--accent)]" />
                  </div>
                  <p className="mt-6 font-display text-4xl leading-none text-[var(--ink)]">
                    {String(value).padStart(2, "0")}
                  </p>
                  <SkimmableText className="mt-3 text-sm leading-6 text-[var(--muted)]">{blurb}</SkimmableText>
                </article>
              ))}
            </div>
          </div>

          <aside className="rounded-[2rem] border border-[var(--ink)]/8 bg-[var(--ink)] px-6 py-6 text-[var(--paper)] shadow-[0_24px_64px_rgba(17,19,18,0.18)]">
            <p className="font-mono text-[11px] uppercase tracking-[0.28em] text-[rgba(236,228,211,0.66)]">Edition framing</p>
            <h4 className="mt-4 font-display text-3xl leading-tight">
              {brief.data.period_type === "week" ? "Hold the week in one pass." : "Lead with conviction, track the edges."}
            </h4>
            <SkimmableText className="mt-4 text-sm leading-7 text-[rgba(236,228,211,0.84)]">
              {brief.data.period_type === "week"
                ? "This recap is assembled from completed daily editions only. Use it to see what persisted across several days before committing to a deeper read."
                : "Start with the editorial shortlist, scan the headlines for short-scope movement, then use the side signals and papers table to decide what deserves a deeper pass."}
            </SkimmableText>

            <div className="mt-6 rounded-[1.6rem] border border-white/10 bg-white/6 p-5">
              <p className="font-mono text-[11px] uppercase tracking-[0.24em] text-[rgba(236,228,211,0.66)]">Edition window</p>
              <p className="mt-3 text-sm leading-6 text-[rgba(236,228,211,0.88)]">{editionLabel}</p>
              <p className="mt-4 font-mono text-[11px] uppercase tracking-[0.24em] text-[rgba(236,228,211,0.66)]">Source coverage</p>
              <p className="mt-3 text-sm leading-6 text-[rgba(236,228,211,0.88)]">{coverageLabel}</p>
            </div>

            {firstReadItem ? (
              <div className="mt-4 rounded-[1.6rem] border border-white/10 bg-white/6 p-5">
                <p className="font-mono text-[11px] uppercase tracking-[0.24em] text-[rgba(236,228,211,0.66)]">First read</p>
                <p className="mt-3 font-display text-2xl leading-tight text-[var(--paper)]">{firstReadItem.title}</p>
                <p className="mt-3 text-sm leading-6 text-[rgba(236,228,211,0.7)]">{firstReadItem.source_name}</p>
              </div>
            ) : null}

            {brief.data.period_type === "day" && brief.data.suggested_follow_ups[0] ? (
              <div className="mt-4 rounded-[1.6rem] border border-white/10 bg-white/6 p-5">
                <p className="font-mono text-[11px] uppercase tracking-[0.24em] text-[rgba(236,228,211,0.66)]">Question to keep</p>
                <SkimmableText className="mt-3 text-sm leading-7 text-[rgba(236,228,211,0.88)]">
                  {brief.data.suggested_follow_ups[0]}
                </SkimmableText>
              </div>
            ) : null}
          </aside>
        </div>
      </section>

      <div className="page-breakout space-y-8">
        <Headlines items={brief.data.headlines} />

        {shortlistLead ? (
          <section className="space-y-4">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
              <div>
                <p className="section-kicker">Shortlist</p>
                <h3 className="section-title">{brief.data.period_type === "week" ? "Weekly editorial shortlist" : "Editorial shortlist"}</h3>
                <SkimmableText className="mt-3 max-w-3xl text-base leading-7 text-[var(--muted)]">
                  {brief.data.period_type === "week"
                    ? "The biggest articles or papers that held up across the completed week."
                    : "The biggest articles or papers in this edition that should not be missed."}
                </SkimmableText>
              </div>
              <div className="inline-flex w-fit items-center gap-2 rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
                <span>{String(brief.data.editorial_shortlist.length).padStart(2, "0")}</span>
                <span>Priority reads</span>
              </div>
            </div>

            <ItemCard
              byline={getBriefByline(shortlistLead.item)}
              hero
              item={shortlistLead.item}
              note={shortlistLead.note}
              publishedAtFormat="date"
            />

            {restShortlist.length ? (
              <div className="grid gap-4 md:grid-cols-2">
                {restShortlist.map((entry) => (
                  <ItemCard
                    byline={getBriefByline(entry.item)}
                    key={entry.item.id}
                    item={entry.item}
                    note={entry.note}
                    publishedAtFormat="date"
                  />
                ))}
              </div>
            ) : null}
          </section>
        ) : null}

        <Section
          description="Peripheral movement, trend indicators, and notable mentions that could mature into bigger stories."
          label="Signals"
          title="Interesting side signals"
          items={brief.data.interesting_side_signals}
        />

        <PapersTable items={brief.data.papers_table} />

        <Section
          description="Everything still worth holding onto after the higher-priority lanes have been filled."
          label="Remaining"
          title="Remaining reads"
          items={brief.data.remaining_reads}
        />

        {brief.data.period_type === "day" && brief.data.suggested_follow_ups.length ? (
          <section className="editorial-panel-dark">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
              <div>
                <p className="font-mono text-[11px] uppercase tracking-[0.28em] text-[rgba(236,228,211,0.66)]">
                  Suggested follow-ups
                </p>
                <h3 className="mt-3 font-display text-4xl leading-none text-[var(--paper)]">Questions worth carrying forward</h3>
              </div>
              <div className="inline-flex w-fit items-center gap-2 rounded-full border border-white/10 bg-white/6 px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[rgba(236,228,211,0.72)]">
                <Lightbulb className="h-4 w-4" />
                <span>{String(brief.data.suggested_follow_ups.length).padStart(2, "0")} prompts</span>
              </div>
            </div>

            <div className="mt-6 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
              {brief.data.suggested_follow_ups.map((question, index) => (
                <article key={question} className="rounded-[1.45rem] border border-white/10 bg-white/6 p-5">
                  <p className="font-mono text-[11px] uppercase tracking-[0.24em] text-[rgba(236,228,211,0.6)]">
                    {String(index + 1).padStart(2, "0")}
                  </p>
                  <SkimmableText className="mt-4 text-sm leading-7 text-[rgba(236,228,211,0.9)]">{question}</SkimmableText>
                </article>
              ))}
            </div>
          </section>
        ) : null}
      </div>
    </div>
  );
}
