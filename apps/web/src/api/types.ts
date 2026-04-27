export type ScoreBucket = "must_read" | "worth_a_skim" | "archive";
export type TriageStatus = "unread" | "needs_review" | "saved" | "archived";
export type ContentType = "article" | "paper" | "newsletter" | "post" | "thread" | "signal" | "news";
export type SourceType = "website" | "gmail_newsletter";
export type SourceRawKind = "blog-post" | "newsletter" | "paper" | "article" | "news" | "thread" | "signal";
export type SourceClassificationMode = "fixed" | "written_content_auto";
export type SourceDecompositionMode = "none" | "newsletter_entries";
export type AdvancedOutputKind = "answer" | "slides" | "chart";
export type HealthCheckScope = "vault" | "wiki" | "raw";
export type DataMode = "seed" | "live";
export type RunStatus = "pending" | "running" | "succeeded" | "failed" | "interrupted";
export type IngestionRunType = "ingest" | "digest" | "zotero_sync" | "cleanup" | "deeper_summary";
export type BriefPeriodType = "day" | "week";
export type AlphaXivSort = "Hot" | "Comments" | "Views" | "Likes" | "GitHub" | "Twitter (X)" | "Recommended";
export type AlphaXivInterval = "3 Days" | "7 Days" | "30 Days" | "90 Days" | "All time";
export type AlphaXivSource = "GitHub" | "Twitter (X)";

export interface MeResponse {
  email: string;
  authenticated: boolean;
}

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
}

export interface ItemListEntry {
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
  triage_status: TriageStatus;
  read: boolean;
  starred: boolean;
  extraction_confidence: number;
  short_summary: string | null;
  bucket: ScoreBucket;
  total_score: number;
  score_breakdown: {
    relevance_score: number;
    novelty_score: number;
    source_quality_score: number;
    author_match_score: number;
    topic_match_score: number;
    zotero_affinity_score: number;
  };
  reason_trace: Record<string, unknown>;
  also_mentioned_in_count: number;
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
  doc_role: string;
  parent_id: string | null;
  asset_paths: string[];
  raw_doc_path: string | null;
  lightweight_enrichment_status: string;
  lightweight_enriched_at: string | null;
  alphaxiv: AlphaXivPaper | null;
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
  raw_kind: SourceRawKind | string;
  classification_mode: SourceClassificationMode;
  decomposition_mode: SourceDecompositionMode;
  url: string | null;
  query: string | null;
  description: string | null;
  active: boolean;
  max_items: number;
  tags: string[];
  config_json: Record<string, unknown>;
  last_synced_at: string | null;
  created_at: string;
  updated_at: string;
  has_custom_pipeline: boolean;
  custom_pipeline_id: string | null;
  latest_extraction_run: {
    id: string;
    status: RunStatus;
    operation_kind: string;
    summary: string;
    started_at: string;
    finished_at: string | null;
    emitted_kinds: string[];
  } | null;
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

export interface SourceLatestLogResult {
  run: IngestionRunHistoryEntry;
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

export interface ItemsIndexStatus {
  up_to_date: boolean;
  stale_document_count: number;
  indexed_item_count: number;
  generated_at: string | null;
}

export interface PipelineStatus {
  raw_document_count: number;
  lightweight_pending_count: number;
  lightweight_metadata_pending_count: number;
  lightweight_scoring_pending_count: number;
  items_index: ItemsIndexStatus;
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

export interface OperationStep {
  step_kind: string;
  status: RunStatus;
  started_at: string;
  finished_at: string | null;
  source_id: string | null;
  doc_id: string | null;
  created_count: number;
  updated_count: number;
  skipped_count: number;
  counts_by_kind: Record<string, number>;
  basic_info: OperationBasicInfo[];
  logs: OperationLog[];
  errors: string[];
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
  steps: OperationStep[];
  source_stats: IngestionRunSourceStats[];
  errors: string[];
  codex_command?: string[] | null;
  prompt_path?: string | null;
  manifest_path?: string | null;
  output_paths: string[];
  changed_file_count: number;
  duration_seconds?: number | null;
  exit_code?: number | null;
  stderr_excerpt?: string | null;
  final_summary?: {
    job_type: "compile" | "health_check" | "answer" | "file_output";
    summary: string;
    touched_files: string[];
    created_wiki_pages: string[];
    updated_wiki_pages: string[];
    output_paths: string[];
    unresolved_questions: string[];
    suggested_follow_up_jobs: Array<{
      job_type: "compile" | "health_check" | "answer" | "file_output";
      reason: string;
      target_path: string | null;
    }>;
  } | null;
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
  ranking_thresholds: {
    must_read_min: number;
    worth_a_skim_min: number;
  };
  brief_sections: {
    editorial_shortlist_count: number;
    headlines_count: number;
    side_signals_count: number;
    remaining_reads_count: number;
    papers_count: number;
    follow_up_questions_count: number;
  };
  audio_brief_settings: {
    target_duration_minutes: number;
    max_items_per_section: number;
  };
  prompt_guidance: {
    enrichment: string;
    editorial_note: string;
    audio_brief: string;
  };
  alphaxiv_search_settings: {
    topics: string[];
    organizations: string[];
    sort: AlphaXivSort;
    interval: AlphaXivInterval;
    source: AlphaXivSource | null;
  };
  created_at: string;
  updated_at: string;
}

export interface CodexRuntimeStatus {
  available: boolean;
  authenticated: boolean;
  binary: string | null;
  model: string | null;
  profile: string | null;
  search_enabled: boolean;
  timeout_minutes: number | null;
  compile_batch_size: number | null;
  detail: string | null;
}
