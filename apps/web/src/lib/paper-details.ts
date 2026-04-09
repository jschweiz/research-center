type SimilarPaperLike = {
  title: string;
  canonical_url: string;
  app_item_id: string | null;
  authors: string[];
  short_summary: string | null;
};

type AlphaXivPaperLike = {
  short_summary: string | null;
  filed_text: string | null;
  audio_url: string | null;
  similar_papers: SimilarPaperLike[];
};

type PaperDetailLike = {
  short_summary?: string | null;
  cleaned_text: string | null;
  insight: {
    short_summary: string | null;
  };
  alphaxiv: AlphaXivPaperLike | null;
};

export function resolvePaperSummary(item: PaperDetailLike) {
  return item.alphaxiv?.short_summary ?? item.short_summary ?? item.insight.short_summary ?? null;
}

export function resolvePaperFiledText(item: PaperDetailLike) {
  return item.alphaxiv?.filed_text ?? item.cleaned_text ?? null;
}

export function resolvePaperAudioUrl(item: PaperDetailLike) {
  const audioUrl = item.alphaxiv?.audio_url?.trim();
  return audioUrl || null;
}

export function resolveSimilarPapers(item: PaperDetailLike) {
  return item.alphaxiv?.similar_papers ?? [];
}
