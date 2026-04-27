import type { ReactNode } from "react";
import clsx from "clsx";

import { resolveExternalUrl } from "../lib/external-links";

interface MarkdownTextProps {
  children: string;
  className?: string;
}

type MarkdownBlock =
  | { type: "heading"; level: number; text: string }
  | { type: "paragraph"; text: string }
  | { type: "unordered-list"; items: string[] }
  | { type: "ordered-list"; items: string[] }
  | { type: "blockquote"; text: string }
  | { type: "code"; language: string | null; text: string };

type InlineTokenType = "code" | "link" | "auto-link" | "strong" | "emphasis";

interface InlineTokenMatch {
  type: InlineTokenType;
  index: number;
  match: RegExpExecArray;
  priority: number;
}

const HEADING_PATTERN = /^(#{1,6})\s+(.+?)\s*$/;
const UNORDERED_LIST_PATTERN = /^\s*[-*+]\s+(.+?)\s*$/;
const ORDERED_LIST_PATTERN = /^\s*\d+\.\s+(.+?)\s*$/;
const BLOCKQUOTE_PATTERN = /^\s*>\s?(.*)$/;
const FENCE_PATTERN = /^```([A-Za-z0-9_-]+)?\s*$/;
const THEMATIC_BREAK_PATTERN = /^\s*(?:---+|\*\*\*+|___+)\s*$/;

const INLINE_PATTERNS: Array<{ type: InlineTokenType; pattern: RegExp; priority: number }> = [
  { type: "code", pattern: /`([^`\n]+)`/, priority: 0 },
  { type: "link", pattern: /\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/, priority: 1 },
  { type: "auto-link", pattern: /(https?:\/\/[^\s<]+)/, priority: 2 },
  { type: "strong", pattern: /\*\*([^*]+)\*\*/, priority: 3 },
  { type: "strong", pattern: /__([^_]+)__/, priority: 4 },
  { type: "emphasis", pattern: /\*([^*\n]+)\*/, priority: 5 },
  { type: "emphasis", pattern: /_([^_\n]+)_/, priority: 6 },
];

function isBlockBoundary(line: string) {
  return (
    !line.trim()
    || HEADING_PATTERN.test(line)
    || UNORDERED_LIST_PATTERN.test(line)
    || ORDERED_LIST_PATTERN.test(line)
    || BLOCKQUOTE_PATTERN.test(line)
    || FENCE_PATTERN.test(line)
    || THEMATIC_BREAK_PATTERN.test(line)
  );
}

function joinParagraphLines(lines: string[]) {
  return lines.map((line) => line.trim()).join(" ").replace(/\s+/g, " ").trim();
}

function parseMarkdownBlocks(markdown: string): MarkdownBlock[] {
  const normalized = markdown.replace(/\r\n/g, "\n").trim();
  if (!normalized) return [];

  const lines = normalized.split("\n");
  const blocks: MarkdownBlock[] = [];

  for (let index = 0; index < lines.length;) {
    const line = lines[index];
    const trimmed = line.trim();

    if (!trimmed) {
      index += 1;
      continue;
    }

    const fenceMatch = line.match(FENCE_PATTERN);
    if (fenceMatch) {
      const language = fenceMatch[1] || null;
      const codeLines: string[] = [];
      index += 1;
      while (index < lines.length && !FENCE_PATTERN.test(lines[index])) {
        codeLines.push(lines[index]);
        index += 1;
      }
      if (index < lines.length) {
        index += 1;
      }
      blocks.push({ type: "code", language, text: codeLines.join("\n") });
      continue;
    }

    if (THEMATIC_BREAK_PATTERN.test(line)) {
      index += 1;
      continue;
    }

    const headingMatch = line.match(HEADING_PATTERN);
    if (headingMatch) {
      blocks.push({
        type: "heading",
        level: Math.min(6, headingMatch[1].length),
        text: headingMatch[2].trim(),
      });
      index += 1;
      continue;
    }

    const unorderedMatch = line.match(UNORDERED_LIST_PATTERN);
    if (unorderedMatch) {
      const items: string[] = [];
      while (index < lines.length) {
        const itemMatch = lines[index].match(UNORDERED_LIST_PATTERN);
        if (!itemMatch) break;
        items.push(itemMatch[1].trim());
        index += 1;
      }
      blocks.push({ type: "unordered-list", items });
      continue;
    }

    const orderedMatch = line.match(ORDERED_LIST_PATTERN);
    if (orderedMatch) {
      const items: string[] = [];
      while (index < lines.length) {
        const itemMatch = lines[index].match(ORDERED_LIST_PATTERN);
        if (!itemMatch) break;
        items.push(itemMatch[1].trim());
        index += 1;
      }
      blocks.push({ type: "ordered-list", items });
      continue;
    }

    const blockquoteMatch = line.match(BLOCKQUOTE_PATTERN);
    if (blockquoteMatch) {
      const quoteLines: string[] = [];
      while (index < lines.length) {
        const itemMatch = lines[index].match(BLOCKQUOTE_PATTERN);
        if (!itemMatch) break;
        quoteLines.push(itemMatch[1]);
        index += 1;
      }
      blocks.push({ type: "blockquote", text: joinParagraphLines(quoteLines) });
      continue;
    }

    const paragraphLines: string[] = [];
    while (index < lines.length && !isBlockBoundary(lines[index])) {
      paragraphLines.push(lines[index]);
      index += 1;
    }
    if (paragraphLines.length) {
      blocks.push({ type: "paragraph", text: joinParagraphLines(paragraphLines) });
      continue;
    }

    index += 1;
  }

  return blocks;
}

function findNextInlineToken(text: string): InlineTokenMatch | null {
  let candidate: InlineTokenMatch | null = null;

  for (const entry of INLINE_PATTERNS) {
    const match = entry.pattern.exec(text);
    if (!match || match.index == null) continue;

    const next: InlineTokenMatch = {
      type: entry.type,
      index: match.index,
      match,
      priority: entry.priority,
    };

    if (
      candidate == null
      || next.index < candidate.index
      || (next.index === candidate.index && next.priority < candidate.priority)
    ) {
      candidate = next;
    }
  }

  return candidate;
}

function trimAutoLink(link: string) {
  return link.replace(/[),.;:!?]+$/, "");
}

function renderInlineMarkdown(text: string, keyPrefix: string): ReactNode[] {
  const content: ReactNode[] = [];
  let remainder = text;
  let tokenIndex = 0;

  while (remainder.length) {
    const token = findNextInlineToken(remainder);
    if (!token) {
      content.push(remainder);
      break;
    }

    if (token.index > 0) {
      content.push(remainder.slice(0, token.index));
    }

    const key = `${keyPrefix}-${tokenIndex}`;
    const [matchedText, firstGroup = "", secondGroup = ""] = token.match;

    if (token.type === "code") {
      content.push(
        <code key={key} className="markdown-inline-code">
          {firstGroup}
        </code>,
      );
    } else if (token.type === "link") {
      const resolvedLink = resolveExternalUrl(secondGroup);
      content.push(
        <a key={key} href={resolvedLink} rel="noreferrer" target="_blank">
          {renderInlineMarkdown(firstGroup, `${key}-label`)}
        </a>,
      );
    } else if (token.type === "auto-link") {
      const link = trimAutoLink(firstGroup);
      const resolvedLink = resolveExternalUrl(link);
      content.push(
        <a key={key} href={resolvedLink} rel="noreferrer" target="_blank">
          {resolvedLink}
        </a>,
      );
      const suffix = firstGroup.slice(link.length);
      if (suffix) {
        content.push(suffix);
      }
    } else if (token.type === "strong") {
      content.push(<strong key={key}>{renderInlineMarkdown(firstGroup, `${key}-strong`)}</strong>);
    } else if (token.type === "emphasis") {
      content.push(<em key={key}>{renderInlineMarkdown(firstGroup, `${key}-em`)}</em>);
    }

    remainder = remainder.slice(token.index + matchedText.length);
    tokenIndex += 1;
  }

  return content;
}

function renderMarkdownBlock(block: MarkdownBlock, index: number) {
  if (block.type === "heading") {
    const content = renderInlineMarkdown(block.text, `heading-${index}`);

    if (block.level === 1) return <h1 key={`heading-${index}`}>{content}</h1>;
    if (block.level === 2) return <h2 key={`heading-${index}`}>{content}</h2>;
    if (block.level === 3) return <h3 key={`heading-${index}`}>{content}</h3>;
    if (block.level === 4) return <h4 key={`heading-${index}`}>{content}</h4>;
    if (block.level === 5) return <h5 key={`heading-${index}`}>{content}</h5>;
    return <h6 key={`heading-${index}`}>{content}</h6>;
  }

  if (block.type === "paragraph") {
    return <p key={`paragraph-${index}`}>{renderInlineMarkdown(block.text, `paragraph-${index}`)}</p>;
  }

  if (block.type === "unordered-list") {
    return (
      <ul key={`unordered-${index}`}>
        {block.items.map((item, itemIndex) => (
          <li key={`unordered-${index}-${itemIndex}`}>{renderInlineMarkdown(item, `unordered-${index}-${itemIndex}`)}</li>
        ))}
      </ul>
    );
  }

  if (block.type === "ordered-list") {
    return (
      <ol key={`ordered-${index}`}>
        {block.items.map((item, itemIndex) => (
          <li key={`ordered-${index}-${itemIndex}`}>{renderInlineMarkdown(item, `ordered-${index}-${itemIndex}`)}</li>
        ))}
      </ol>
    );
  }

  if (block.type === "blockquote") {
    return <blockquote key={`blockquote-${index}`}>{renderInlineMarkdown(block.text, `blockquote-${index}`)}</blockquote>;
  }

  return (
    <pre key={`code-${index}`}>
      <code data-language={block.language ?? undefined}>{block.text}</code>
    </pre>
  );
}

export function MarkdownText({ children, className }: MarkdownTextProps) {
  const blocks = parseMarkdownBlocks(children);

  if (!blocks.length) {
    return null;
  }

  return <div className={clsx("markdown-text", className)}>{blocks.map(renderMarkdownBlock)}</div>;
}
