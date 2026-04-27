import type { ContentType } from "../api/types";

export interface MediumDigestArticle {
  title: string;
  url: string;
  claps: string | null;
  readTime: string | null;
}

type MediumDigestCandidate = {
  source_id?: string | null;
  doc_role: string;
  content_type: ContentType;
};

const STORY_TITLE_RE = /^### \[(.+)\]\((https?:\/\/\S+)\)\s*$/;

function parseClapCount(value: string | null): number {
  if (!value) return 0;

  const normalized = value.replace(/,/g, "").trim().toUpperCase();
  const match = normalized.match(/^(\d+(?:\.\d+)?)([KM]?)$/);
  if (!match) return 0;

  const amount = Number.parseFloat(match[1] ?? "0");
  const suffix = match[2] ?? "";
  if (!Number.isFinite(amount)) return 0;
  if (suffix === "K") return amount * 1_000;
  if (suffix === "M") return amount * 1_000_000;
  return amount;
}

export function isMediumDigestItem(item: MediumDigestCandidate) {
  return item.source_id === "medium-email" && item.doc_role === "primary" && item.content_type === "newsletter";
}

export function parseMediumDigestArticles(markdown: string | null | undefined): MediumDigestArticle[] {
  if (!markdown?.trim()) return [];

  const rows: MediumDigestArticle[] = [];
  const seenUrls = new Set<string>();
  const lines = markdown.split(/\r?\n/);

  for (let index = 0; index < lines.length; index += 1) {
    const match = lines[index].trim().match(STORY_TITLE_RE);
    if (!match) continue;

    const title = match[1]?.trim();
    const url = match[2]?.trim();
    if (!title || !url || seenUrls.has(url)) continue;

    let detailsLine = "";
    for (let cursor = index + 1; cursor < Math.min(lines.length, index + 6); cursor += 1) {
      const candidate = lines[cursor].trim();
      if (!candidate) continue;
      if (candidate.startsWith("### ")) break;
      if (candidate.startsWith(">")) {
        detailsLine = candidate.slice(1).trim();
        break;
      }
    }

    const details = detailsLine
      .split("·")
      .map((part) => part.trim())
      .filter(Boolean);
    const readTime = details.find((part) => /\bmin read\b/i.test(part)) ?? null;
    const claps = details.find((part) => /\bclaps?\b/i.test(part))?.replace(/\s*claps?\s*$/i, "") ?? null;

    rows.push({
      title,
      url,
      claps,
      readTime,
    });
    seenUrls.add(url);
  }

  return rows.sort((left, right) => {
    const clapDelta = parseClapCount(right.claps) - parseClapCount(left.claps);
    if (clapDelta !== 0) return clapDelta;
    return left.title.localeCompare(right.title);
  });
}
