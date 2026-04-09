import type { ContentType, DataMode, ScoreBucket } from "../api/types";

export type AppMode = "admin" | "hosted" | "local";

export interface CloudKitRuntimeConfig {
  containerIdentifier: string;
  environment: "development" | "production";
  database: "public";
  apiToken: string;
  latestRecordName?: string;
}

export interface RuntimeConfig {
  mode: AppMode;
  apiBaseUrl?: string;
  pairedLocalUrl?: string | null;
  hostedViewerUrl?: string | null;
  cloudKit?: CloudKitRuntimeConfig | null;
}

export interface PublishedItemListEntry {
  id: string;
  kind?: string | null;
  source_id?: string | null;
  title: string;
  source_name: string;
  organization_name: string | null;
  authors: string[];
  published_at: string | null;
  canonical_url: string;
  content_type: ContentType;
  extraction_confidence: number;
  short_summary: string | null;
  bucket: ScoreBucket;
  total_score: number;
  reason_trace: Record<string, unknown>;
  also_mentioned_in_count: number;
}

export interface PublishedItemScore {
  relevance_score: number;
  novelty_score: number;
  source_quality_score: number;
  author_match_score: number;
  topic_match_score: number;
  zotero_affinity_score: number;
  total_score: number;
  bucket: ScoreBucket;
  reason_trace: Record<string, unknown>;
}

export interface PublishedItemInsight {
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
}

export interface PublishedRelatedMention {
  item_id: string;
  title: string;
  source_name: string;
  canonical_url: string;
}

export interface AlphaXivSimilarPaper {
  title: string;
  canonical_url: string;
  app_item_id: string | null;
  authors: string[];
  short_summary: string | null;
}

export interface AlphaXivPaper {
  short_summary: string | null;
  filed_text: string | null;
  audio_url: string | null;
  similar_papers: AlphaXivSimilarPaper[];
}

export interface PublishedItemDetail {
  id: string;
  kind?: string | null;
  source_id?: string | null;
  title: string;
  source_name: string;
  organization_name: string | null;
  authors: string[];
  published_at: string | null;
  canonical_url: string;
  content_type: ContentType;
  extraction_confidence: number;
  cleaned_text: string | null;
  outbound_links: string[];
  score: PublishedItemScore;
  insight: PublishedItemInsight;
  also_mentioned_in: PublishedRelatedMention[];
  doc_role: string;
  parent_id: string | null;
  asset_paths: string[];
  raw_doc_path: string | null;
  lightweight_enrichment_status: string;
  lightweight_enriched_at: string | null;
  alphaxiv: AlphaXivPaper | null;
}

export interface PublishedDigestEntry {
  item: PublishedItemListEntry;
  note: string | null;
  rank: number;
}

export interface PublishedPaperTableEntry {
  item: PublishedItemListEntry;
  rank: number;
  zotero_tags: string[];
  credibility_score: number | null;
}

export interface PublishedAudioBriefChapter {
  item_id: string;
  item_title: string;
  section: string;
  rank: number;
  headline: string;
  narration: string;
  offset_seconds: number;
}

export interface PublishedAudioBrief {
  status: string;
  script: string | null;
  chapters: PublishedAudioBriefChapter[];
  estimated_duration_seconds: number | null;
  audio_url: string | null;
  audio_duration_seconds: number | null;
  provider: string | null;
  voice: string | null;
  error: string | null;
  generated_at: string | null;
  metadata: Record<string, unknown>;
}

export interface PublishedDigest {
  id: string;
  period_type: "day" | "week";
  brief_date: string | null;
  week_start: string | null;
  week_end: string | null;
  coverage_start: string;
  coverage_end: string;
  data_mode: DataMode;
  title: string;
  editorial_note: string | null;
  suggested_follow_ups: string[];
  audio_brief: PublishedAudioBrief | null;
  generated_at: string;
  headlines: PublishedDigestEntry[];
  editorial_shortlist: PublishedDigestEntry[];
  interesting_side_signals: PublishedDigestEntry[];
  remaining_reads: PublishedDigestEntry[];
  papers_table: PublishedPaperTableEntry[];
}

export interface PublishedAvailabilityDay {
  brief_date: string;
  coverage_start: string;
  coverage_end: string;
}

export interface PublishedAvailabilityWeek {
  week_start: string;
  week_end: string;
  coverage_start: string;
  coverage_end: string;
}

export interface PublishedAvailability {
  default_day: string | null;
  days: PublishedAvailabilityDay[];
  weeks: PublishedAvailabilityWeek[];
}

export interface PublishedEditionSummary {
  edition_id: string;
  record_name: string;
  period_type: "day" | "week";
  brief_date: string | null;
  week_start: string | null;
  week_end: string | null;
  title: string;
  generated_at: string | null;
  published_at: string;
  has_audio: boolean;
  schema_version: number;
}

export interface PublishedEditionManifest {
  schema_version: number;
  edition: PublishedEditionSummary;
  availability: PublishedAvailability;
  available_editions: PublishedEditionSummary[];
  digest: PublishedDigest;
  items: Record<string, PublishedItemDetail>;
}

export interface PairRedeemResponse {
  device_label: string;
  paired_local_url: string;
  access_token: string;
  hosted_return_url: string | null;
}

export interface VaultGitStatus {
  enabled: boolean;
  repo_ready: boolean;
  branch: string | null;
  remote_name: string | null;
  remote_url: string | null;
  current_commit: string | null;
  current_summary: string | null;
  has_uncommitted_changes: boolean;
  changed_files: number;
  ahead_count: number;
  behind_count: number;
  git_lfs_available: boolean;
}

export interface ItemsIndexStatus {
  up_to_date: boolean;
  stale_document_count: number;
  indexed_item_count: number;
  generated_at: string | null;
}

export interface LocalControlInsightTopic {
  id: string;
  label: string;
  page_path: string | null;
  recent_item_count_7d: number;
  recent_item_count_30d: number;
  total_item_count: number;
  source_diversity: number;
  trend_score: number;
  novelty_score: number;
  related_topic_ids: string[];
}

export interface LocalControlInsights {
  map_page: string | null;
  trends_page: string | null;
  topics: LocalControlInsightTopic[];
  rising_topics: LocalControlInsightTopic[];
}

export interface LocalControlStatus {
  device_label: string;
  paired_local_url: string;
  vault_root_dir: string;
  viewer_bundle_dir: string;
  current_brief_date: string;
  latest_publication: PublishedEditionSummary | null;
  latest_brief_dir: string | null;
  raw_document_count: number;
  lightweight_pending_count: number;
  items_index: ItemsIndexStatus;
  wiki_page_count: number;
  topic_count: number;
  rising_topic_count: number;
  vault_sync: VaultGitStatus | null;
  ollama: {
    available: boolean;
    model: string | null;
    detail: string | null;
  } | null;
  codex: {
    available: boolean;
    authenticated: boolean;
    binary: string | null;
    model: string | null;
    profile: string | null;
    search_enabled: boolean;
    timeout_minutes: number | null;
    compile_batch_size: number | null;
    detail: string | null;
  } | null;
}

export interface LocalControlJobResponse {
  queued: boolean;
  task_name: string;
  detail: string;
  operation_run_id: string | null;
  published_edition: PublishedEditionSummary | null;
  completed_at: string | null;
}
