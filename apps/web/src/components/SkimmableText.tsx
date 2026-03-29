import type { ComponentPropsWithoutRef, ElementType, ReactNode } from "react";
import clsx from "clsx";

const SENTENCE_PATTERN = /[^.!?]+(?:[.!?]+|$)\s*/g;
const WORD_PATTERN = /[\p{L}\p{N}]+(?:['’-][\p{L}\p{N}]+)*/gu;

const ARTICLE_WORDS = new Set([
  "a",
  "an",
  "the",
  "this",
  "that",
  "these",
  "those",
  "it",
  "its",
  "their",
  "his",
  "her",
  "our",
  "your",
  "one",
  "two",
  "three",
  "four",
  "five",
  "six",
  "seven",
  "eight",
  "nine",
  "ten",
]);

const HARD_BREAK_WORDS = new Set([
  "is",
  "are",
  "was",
  "were",
  "be",
  "been",
  "being",
  "has",
  "have",
  "had",
  "can",
  "could",
  "will",
  "would",
  "should",
  "may",
  "might",
  "must",
  "do",
  "does",
  "did",
]);

const SOFT_BREAK_WORDS = new Set([
  "and",
  "or",
  "but",
  "so",
  "to",
  "for",
  "from",
  "with",
  "without",
  "into",
  "onto",
  "over",
  "under",
  "after",
  "before",
  "around",
  "across",
  "between",
  "among",
  "through",
  "while",
  "because",
  "although",
  "despite",
  "if",
  "when",
  "where",
  "which",
  "who",
  "whose",
  "that",
]);

const REPORTING_VERBS = new Set([
  "suggests",
  "reveals",
  "offers",
  "introduces",
  "shows",
  "means",
  "keeps",
  "starts",
  "uses",
  "deserves",
  "indicates",
  "highlights",
  "argues",
  "frames",
  "focuses",
  "notes",
  "presents",
  "improves",
  "lowers",
  "uncovers",
  "explains",
  "describes",
  "organizes",
  "connects",
  "turns",
]);

const LOW_SIGNAL_WORDS = new Set([
  ...ARTICLE_WORDS,
  ...HARD_BREAK_WORDS,
  ...SOFT_BREAK_WORDS,
  "as",
  "by",
  "in",
  "on",
  "at",
  "of",
  "than",
  "then",
  "very",
  "more",
  "most",
  "less",
  "least",
  "what",
  "why",
  "how",
]);

const SKIP_ACTION_WORDS = new Set([
  "gain",
  "gains",
  "make",
  "makes",
  "build",
  "builds",
  "keep",
  "keeps",
  "start",
  "starts",
  "save",
  "saves",
  "turn",
  "turns",
  "use",
  "uses",
  "add",
  "adds",
  "change",
  "changes",
  "put",
  "puts",
  "take",
  "takes",
]);

interface WordToken {
  value: string;
  lower: string;
  start: number;
  end: number;
}

type SkimmableTextProps<T extends ElementType = "p"> = {
  as?: T;
  children: string;
  className?: string;
} & Omit<ComponentPropsWithoutRef<T>, "as" | "children" | "className">;

function getSentenceChunks(text: string) {
  return text.match(SENTENCE_PATTERN) ?? [text];
}

function getWordTokens(text: string): WordToken[] {
  return Array.from(text.matchAll(WORD_PATTERN), (match) => {
    const value = match[0];
    const start = match.index ?? 0;
    return {
      value,
      lower: value.toLowerCase(),
      start,
      end: start + value.length,
    };
  });
}

function isMeaningfulWord(word: string) {
  if (/^[A-Z0-9]{2,}$/.test(word) || /^\d/.test(word)) return true;

  const lower = word.toLowerCase();
  if (LOW_SIGNAL_WORDS.has(lower)) return false;

  return lower.length > 2;
}

function findEarlyClauseRange(sentence: string) {
  const punctuationMatches = Array.from(sentence.matchAll(/[,;:]/g));

  for (const punctuationMatch of punctuationMatches) {
    const end = punctuationMatch.index ?? 0;
    const candidate = sentence.slice(0, end).trim();
    const wordCount = getWordTokens(candidate).length;

    if (candidate.length >= 12 && candidate.length <= 56 && wordCount >= 2 && wordCount <= 8) {
      const start = sentence.indexOf(candidate);
      return [start, start + candidate.length] as const;
    }
  }

  return null;
}

function findPostBreakRange(tokens: WordToken[], startIndex: number) {
  let start: number | null = null;
  let end = -1;
  let contentCount = 0;

  for (let index = startIndex; index < Math.min(tokens.length, startIndex + 8); index += 1) {
    const token = tokens[index];

    if (LOW_SIGNAL_WORDS.has(token.lower)) {
      if (start !== null && contentCount >= 2) break;
      continue;
    }

    if (start === null && SKIP_ACTION_WORDS.has(token.lower)) {
      continue;
    }

    if (start === null) start = index;
    end = index;
    contentCount += 1;

    if (contentCount >= 3) break;
  }

  if (start === null || end < start || contentCount < 2) return null;
  return [tokens[start].start, tokens[end].end] as const;
}

function findLeadRange(sentence: string) {
  const trimmed = sentence.trim();

  if (trimmed.length < 18 || trimmed.endsWith("?") || trimmed.includes("@") || trimmed.includes("://")) {
    return null;
  }

  const earlyClauseRange = findEarlyClauseRange(sentence);
  if (earlyClauseRange) return earlyClauseRange;

  const tokens = getWordTokens(sentence);
  if (tokens.length < 2) return null;

  if (tokens[1] && (HARD_BREAK_WORDS.has(tokens[1].lower) || REPORTING_VERBS.has(tokens[1].lower))) {
    const postBreakRange = findPostBreakRange(tokens, 2);
    if (postBreakRange) return postBreakRange;
  }

  const reportingVerbIndex = tokens.findIndex((token, index) => index > 0 && index < 6 && REPORTING_VERBS.has(token.lower));
  if (ARTICLE_WORDS.has(tokens[0].lower) && reportingVerbIndex > 0) {
    const postVerbRange = findPostBreakRange(tokens, reportingVerbIndex + 1);
    if (postVerbRange) return postVerbRange;
  }

  let endTokenIndex = 0;
  let contentCount = isMeaningfulWord(tokens[0].value) ? 1 : 0;
  let totalWords = 1;

  for (let index = 1; index < tokens.length; index += 1) {
    const previousToken = tokens[index - 1];
    const token = tokens[index];
    const gap = sentence.slice(previousToken.end, token.start);

    if (/[;:]/.test(gap)) break;
    if (totalWords >= 2 && HARD_BREAK_WORDS.has(token.lower)) break;
    if (contentCount >= 2 && (SOFT_BREAK_WORDS.has(token.lower) || REPORTING_VERBS.has(token.lower))) break;
    if (totalWords >= 5) break;

    endTokenIndex = index;
    totalWords += 1;

    if (isMeaningfulWord(token.value)) {
      contentCount += 1;
    }

    if (contentCount >= 3) {
      const nextToken = tokens[index + 1];
      if (
        !nextToken
        || HARD_BREAK_WORDS.has(nextToken.lower)
        || SOFT_BREAK_WORDS.has(nextToken.lower)
        || REPORTING_VERBS.has(nextToken.lower)
      ) {
        break;
      }
    }
  }

  if (contentCount === 0) {
    endTokenIndex = Math.min(tokens.length - 1, 1);
  }

  return [tokens[0].start, tokens[endTokenIndex].end] as const;
}

function renderSkimmableText(text: string): ReactNode {
  const sentences = getSentenceChunks(text);
  const ranges: Array<readonly [number, number]> = [];

  let offset = 0;
  for (const sentence of sentences) {
    const leadRange = findLeadRange(sentence);
    if (leadRange) {
      ranges.push([offset + leadRange[0], offset + leadRange[1]]);
    }
    offset += sentence.length;
  }

  if (!ranges.length) return text;

  const content: ReactNode[] = [];
  let cursor = 0;

  ranges.forEach(([start, end], index) => {
    if (cursor < start) {
      content.push(text.slice(cursor, start));
    }

    content.push(<strong key={`${start}-${end}-${index}`}>{text.slice(start, end)}</strong>);
    cursor = end;
  });

  if (cursor < text.length) {
    content.push(text.slice(cursor));
  }

  return content;
}

export function SkimmableText<T extends ElementType = "p">({
  as,
  children,
  className,
  ...props
}: SkimmableTextProps<T>) {
  const Component = (as ?? "p") as ElementType;

  return (
    <Component className={clsx("skimmable-text", className)} {...props}>
      {renderSkimmableText(children)}
    </Component>
  );
}
