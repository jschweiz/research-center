export type ScoreBucket = "must_read" | "worth_a_skim" | "archive";
export type TriageStatus = "unread" | "needs_review" | "saved" | "archived";
export type ContentType = "article" | "paper" | "newsletter" | "post" | "thread" | "signal";
export type SourceType = "rss" | "gmail_newsletter" | "arxiv" | "manual_url";
export type DataMode = "seed" | "live";
export type RunStatus = "pending" | "running" | "succeeded" | "failed";
export type IngestionRunType = "ingest" | "digest" | "zotero_sync" | "cleanup" | "deeper_summary";
export type BriefPeriodType = "day" | "week";

export interface MeResponse {
  email: string;
  authenticated: boolean;
}

export interface ItemListEntry {
  id: string;
  title: string;
  source_name: string;
  organization_name: string | null;
  authors: string[];
  published_at: string | null;
  canonical_url: string;
  content_type: ContentType;
  triage_status: TriageStatus;
  starred: boolean;
  extraction_confidence: number;
  short_summary: string | null;
  bucket: ScoreBucket;
  total_score: number;
  reason_trace: Record<string, unknown>;
  also_mentioned_in_count: number;
}

export interface ItemDetail extends ItemListEntry {
  ingest_status: string;
  cleaned_text: string | null;
  outbound_links: string[];
  raw_payload_retention_until: string | null;
  score: {
    relevance_score: number;
    novelty_score: number;
    source_quality_score: number;
    author_match_score: number;
    topic_match_score: number;
    zotero_affinity_score: number;
    total_score: number;
    bucket: ScoreBucket;
    reason_trace: Record<string, unknown>;
  };
  insight: {
    short_summary: string | null;
    why_it_matters: string | null;
    whats_new: string | null;
    caveats: string | null;
    follow_up_questions: string[];
    contribution: string | null;
    method: string | null;
    result: string | null;
    limitation: string | null;
    possible_extension: string | null;
    deeper_summary: string | null;
    experiment_ideas: string[];
  };
  also_mentioned_in: Array<{
    item_id: string;
    title: string;
    source_name: string;
    canonical_url: string;
  }>;
  zotero_matches: Array<Record<string, unknown>>;
}

export interface DigestEntry {
  item: ItemListEntry;
  note: string | null;
  rank: number;
}

export interface PaperTableEntry {
  item: ItemListEntry;
  rank: number;
  zotero_tags: string[];
  credibility_score: number | null;
}

export interface AudioBriefChapter {
  item_id: string;
  item_title: string;
  section: string;
  rank: number;
  headline: string;
  narration: string;
  offset_seconds: number;
}

export interface AudioBrief {
  status: string;
  script: string | null;
  chapters: AudioBriefChapter[];
  estimated_duration_seconds: number | null;
  audio_url: string | null;
  audio_duration_seconds: number | null;
  provider: string | null;
  voice: string | null;
  error: string | null;
  generated_at: string | null;
  metadata: Record<string, unknown>;
}

export interface BriefAvailabilityDay {
  brief_date: string;
  coverage_start: string;
  coverage_end: string;
}

export interface BriefAvailabilityWeek {
  week_start: string;
  week_end: string;
  coverage_start: string;
  coverage_end: string;
}

export interface BriefAvailability {
  default_day: string | null;
  days: BriefAvailabilityDay[];
  weeks: BriefAvailabilityWeek[];
}

export interface Digest {
  id: string;
  period_type: BriefPeriodType;
  brief_date: string | null;
  week_start: string | null;
  week_end: string | null;
  coverage_start: string;
  coverage_end: string;
  data_mode: DataMode;
  title: string;
  editorial_note: string | null;
  suggested_follow_ups: string[];
  audio_brief: AudioBrief | null;
  generated_at: string;
  headlines: DigestEntry[];
  editorial_shortlist: DigestEntry[];
  interesting_side_signals: DigestEntry[];
  remaining_reads: DigestEntry[];
  papers_table: PaperTableEntry[];
}

export interface Source {
  id: string;
  type: SourceType;
  name: string;
  url: string | null;
  query: string | null;
  description: string | null;
  active: boolean;
  priority: number;
  tags: string[];
  config_json: Record<string, unknown>;
  last_synced_at: string | null;
  created_at: string;
  updated_at: string;
  rules: Array<{ id: string; rule_type: string; value: string; active: boolean }>;
}

export interface SourceProbeResult {
  source_id: string;
  source_name: string;
  source_type: SourceType;
  total_found: number;
  sample_titles: string[];
  detail: string;
  checked_at: string;
}

export interface Connection {
  id: string;
  provider: "gmail" | "zotero";
  label: string;
  metadata_json: Record<string, unknown>;
  status: "connected" | "disconnected" | "error";
  last_synced_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface ConnectionCapabilities {
  gmail_oauth_configured: boolean;
}

export interface ActionResponse {
  item_id: string;
  triage_status: TriageStatus;
  detail: string;
}

export interface JobResponse {
  queued: boolean;
  task_name: string;
  detail: string;
  operation_run_id: string | null;
}

export interface IngestionRunItem {
  title: string;
  outcome: string;
  content_type: string;
  extraction_confidence: number;
}

export interface IngestionRunSourceStats {
  source_id: string | null;
  source_name: string;
  status: RunStatus;
  ingested_count: number;
  created_count: number;
  updated_count: number;
  duplicate_mention_count: number;
  extractor_fallback_count: number;
  ai_prompt_tokens: number;
  ai_completion_tokens: number;
  ai_total_tokens: number;
  ai_cost_usd: number;
  average_extraction_confidence: number | null;
  items: IngestionRunItem[];
  error: string | null;
}

export interface OperationBasicInfo {
  label: string;
  value: string;
}

export interface OperationLog {
  logged_at: string;
  level: string;
  message: string;
}

export interface IngestionRunHistoryEntry {
  id: string;
  run_type: IngestionRunType;
  status: RunStatus;
  operation_kind: string;
  trigger: string | null;
  title: string;
  summary: string;
  started_at: string;
  finished_at: string | null;
  affected_edition_days: string[];
  total_titles: number;
  source_count: number;
  failed_source_count: number;
  created_count: number;
  updated_count: number;
  duplicate_mention_count: number;
  extractor_fallback_count: number;
  ai_prompt_tokens: number;
  ai_completion_tokens: number;
  ai_total_tokens: number;
  ai_cost_usd: number;
  tts_cost_usd: number;
  total_cost_usd: number;
  average_extraction_confidence: number | null;
  basic_info: OperationBasicInfo[];
  logs: OperationLog[];
  source_stats: IngestionRunSourceStats[];
  errors: string[];
}

export interface Profile {
  id: string;
  favorite_topics: string[];
  favorite_authors: string[];
  favorite_sources: string[];
  ignored_topics: string[];
  digest_time: string;
  timezone: string;
  data_mode: DataMode;
  summary_depth: string;
  ranking_weights: Record<string, number>;
  created_at: string;
  updated_at: string;
}
