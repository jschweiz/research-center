(function () {
  const MAX_TEXT_CHARS = 60000;
  const MAX_HTML_CHARS = 150000;

  const SITE_NAME_SELECTORS = [
    'meta[property="og:site_name"]',
    'meta[name="application-name"]',
    'meta[name="apple-mobile-web-app-title"]',
  ];
  const DESCRIPTION_SELECTORS = [
    'meta[name="description"]',
    'meta[property="og:description"]',
    'meta[name="twitter:description"]',
  ];
  const TITLE_SELECTORS = [
    'meta[property="og:title"]',
    'meta[name="twitter:title"]',
  ];
  const AUTHOR_SELECTORS = [
    'meta[name="author"]',
    'meta[property="author"]',
    'meta[property="article:author"]',
    'meta[name="parsely-author"]',
    'meta[name="dc.creator"]',
    'meta[name="dcterms.creator"]',
    'meta[name="citation_author"]',
    'meta[name="sailthru.author"]',
  ];
  const BYLINE_SELECTORS = [
    'meta[name="byline"]',
    'meta[property="article:author"]',
    'meta[name="author"]',
  ];
  const PUBLISHED_AT_SELECTORS = [
    'meta[property="article:published_time"]',
    'meta[property="og:published_time"]',
    'meta[name="pubdate"]',
    'meta[name="publish-date"]',
    'meta[name="parsely-pub-date"]',
    'meta[name="dc.date"]',
    'meta[name="dcterms.created"]',
    'meta[itemprop="datePublished"]',
  ];
  const LANGUAGE_SELECTORS = [
    'meta[http-equiv="content-language"]',
    'meta[name="language"]',
  ];

  function cleanString(value) {
    const normalized = String(value || "").replace(/\s+/g, " ").trim();
    return normalized || null;
  }

  function normalizeText(value) {
    if (!value) {
      return null;
    }

    const lines = [];
    let previousBlank = false;
    for (const rawLine of String(value).replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n")) {
      const line = rawLine.replace(/[ \t]+/g, " ").trim();
      if (!line) {
        if (lines.length && !previousBlank) {
          lines.push("");
        }
        previousBlank = true;
        continue;
      }
      lines.push(line);
      previousBlank = false;
    }

    return truncateAtBoundary(lines.join("\n").trim(), MAX_TEXT_CHARS);
  }

  function truncateAtBoundary(value, maxChars) {
    const text = String(value || "").trim();
    if (!text || text.length <= maxChars) {
      return text || null;
    }
    const clipped = text.slice(0, maxChars + 1);
    const boundary = clipped.lastIndexOf(" ");
    return (boundary > 0 ? clipped.slice(0, boundary) : clipped.slice(0, maxChars)).trim() || null;
  }

  function normalizeHtml(value) {
    const html = String(value || "").trim();
    if (!html || html.length > MAX_HTML_CHARS) {
      return null;
    }
    if (/^<article[\s>]/i.test(html)) {
      return html;
    }
    return `<article>${html}</article>`;
  }

  function firstMeta(selectors) {
    for (const selector of selectors) {
      const element = document.querySelector(selector);
      const candidate = cleanString(
        element?.getAttribute("content") ||
          element?.getAttribute("datetime") ||
          element?.textContent,
      );
      if (candidate) {
        return candidate;
      }
    }
    return null;
  }

  function allMeta(selectors) {
    const values = [];
    for (const selector of selectors) {
      const nodes = document.querySelectorAll(selector);
      for (const node of nodes) {
        const candidate = cleanString(
          node.getAttribute("content") ||
            node.getAttribute("datetime") ||
            node.textContent,
        );
        if (candidate) {
          values.push(candidate);
        }
      }
    }
    return values;
  }

  function uniqueStrings(values, limit) {
    const unique = [];
    const seen = new Set();

    for (const rawValue of values) {
      const candidate = cleanString(rawValue);
      if (!candidate) {
        continue;
      }
      const lowered = candidate.toLowerCase();
      if (
        seen.has(lowered) ||
        lowered.startsWith("@") ||
        lowered.startsWith("http://") ||
        lowered.startsWith("https://")
      ) {
        continue;
      }
      seen.add(lowered);
      unique.push(candidate);
      if (unique.length >= limit) {
        break;
      }
    }

    return unique;
  }

  function resolveCanonicalUrl() {
    const link = document.querySelector('link[rel~="canonical"][href]');
    if (!link) {
      return null;
    }
    try {
      return new URL(link.getAttribute("href"), document.baseURI).href;
    } catch (_error) {
      return null;
    }
  }

  function resolveSelectedText() {
    try {
      return normalizeText(window.getSelection()?.toString() || "");
    } catch (_error) {
      return null;
    }
  }

  function resolveMainText() {
    const root = document.querySelector("article, main, [role='main']");
    return normalizeText(root?.innerText || root?.textContent || "");
  }

  function resolveBodyText() {
    return normalizeText(document.body?.innerText || document.body?.textContent || "");
  }

  function resolvePublishedAt() {
    const rawValue =
      firstMeta(PUBLISHED_AT_SELECTORS) ||
      cleanString(document.querySelector("time[datetime]")?.getAttribute("datetime"));
    if (!rawValue) {
      return null;
    }
    const parsed = new Date(rawValue);
    if (Number.isNaN(parsed.getTime())) {
      return null;
    }
    return parsed.toISOString();
  }

  function resolveLanguage() {
    return (
      cleanString(document.documentElement?.lang) ||
      firstMeta(LANGUAGE_SELECTORS) ||
      null
    );
  }

  function resolveReadability() {
    if (typeof Readability !== "function") {
      return null;
    }

    try {
      const article = new Readability(document.cloneNode(true)).parse();
      if (!article) {
        return null;
      }
      return {
        title: cleanString(article.title),
        byline: cleanString(article.byline),
        siteName: cleanString(article.siteName),
        text: normalizeText(article.textContent),
        html: normalizeHtml(article.content),
      };
    } catch (_error) {
      return null;
    }
  }

  function parseJsonLdBlocks() {
    const blocks = [];
    for (const node of document.querySelectorAll('script[type="application/ld+json"]')) {
      const rawValue = cleanString(node.textContent);
      if (!rawValue) {
        continue;
      }
      try {
        blocks.push(JSON.parse(rawValue));
      } catch (_error) {
        continue;
      }
    }
    return blocks;
  }

  function walkJson(value, visitor) {
    if (Array.isArray(value)) {
      for (const item of value) {
        walkJson(item, visitor);
      }
      return;
    }
    if (!value || typeof value !== "object") {
      return;
    }
    visitor(value);
    for (const nested of Object.values(value)) {
      walkJson(nested, visitor);
    }
  }

  function collectNamedValues(value, destination) {
    if (!value) {
      return;
    }
    if (typeof value === "string") {
      destination.push(value);
      return;
    }
    if (Array.isArray(value)) {
      for (const item of value) {
        collectNamedValues(item, destination);
      }
      return;
    }
    if (typeof value !== "object") {
      return;
    }
    const combinedName = cleanString(
      [cleanString(value.givenName), cleanString(value.familyName)].filter(Boolean).join(" "),
    );
    destination.push(
      cleanString(value.name) ||
        cleanString(value.alternateName) ||
        combinedName,
    );
  }

  function resolveJsonLdAuthorHints(blocks) {
    const candidates = [];
    for (const block of blocks) {
      walkJson(block, (node) => {
        if ("author" in node) {
          collectNamedValues(node.author, candidates);
        }
        if ("creator" in node) {
          collectNamedValues(node.creator, candidates);
        }
      });
    }
    return uniqueStrings(candidates, 8);
  }

  function resolveJsonLdSiteName(blocks) {
    const candidates = [];
    for (const block of blocks) {
      walkJson(block, (node) => {
        if ("publisher" in node) {
          collectNamedValues(node.publisher, candidates);
        }
        if ("isPartOf" in node) {
          collectNamedValues(node.isPartOf, candidates);
        }
        if ("provider" in node) {
          collectNamedValues(node.provider, candidates);
        }
        const rawType = node["@type"];
        const types = Array.isArray(rawType) ? rawType : [rawType];
        if (types.some((value) => cleanString(value)?.toLowerCase() === "website")) {
          collectNamedValues(node, candidates);
        }
      });
    }
    return uniqueStrings(candidates, 4)[0] || null;
  }

  function resolveJsonLdPublishedAt(blocks) {
    let publishedAt = null;
    for (const block of blocks) {
      walkJson(block, (node) => {
        if (publishedAt) {
          return;
        }
        for (const key of ["datePublished", "dateCreated", "uploadDate"]) {
          const candidate = cleanString(node[key]);
          if (!candidate) {
            continue;
          }
          const parsed = new Date(candidate);
          if (!Number.isNaN(parsed.getTime())) {
            publishedAt = parsed.toISOString();
            return;
          }
        }
      });
      if (publishedAt) {
        break;
      }
    }
    return publishedAt;
  }

  globalThis.__researchCenterCapturePage__ = function capturePage() {
    const readability = resolveReadability();
    const jsonLdBlocks = parseJsonLdBlocks();
    const selectedText = resolveSelectedText();
    const mainText = resolveMainText();
    const bodyText = resolveBodyText();

    const textCandidates = [
      { mode: "readability", text: readability?.text || null },
      { mode: "selection", text: selectedText },
      { mode: "main", text: mainText },
      { mode: "body", text: bodyText },
    ];
    const chosenText = textCandidates.find((candidate) => candidate.text);

    const authorHints = uniqueStrings(
      [...allMeta(AUTHOR_SELECTORS), ...resolveJsonLdAuthorHints(jsonLdBlocks), readability?.byline],
      8,
    );

    return {
      url: window.location.href,
      canonical_url: resolveCanonicalUrl(),
      page_title:
        readability?.title ||
        cleanString(document.title) ||
        firstMeta(TITLE_SELECTORS) ||
        null,
      site_name:
        readability?.siteName ||
        firstMeta(SITE_NAME_SELECTORS) ||
        resolveJsonLdSiteName(jsonLdBlocks) ||
        cleanString(window.location.hostname.replace(/^www\./, "")) ||
        null,
      description: firstMeta(DESCRIPTION_SELECTORS),
      published_at: resolvePublishedAt() || resolveJsonLdPublishedAt(jsonLdBlocks),
      author_hints: authorHints,
      byline: readability?.byline || firstMeta(BYLINE_SELECTORS),
      language: resolveLanguage(),
      extraction_mode: chosenText?.mode || "empty",
      content_text: chosenText?.text || "",
      article_html: readability?.html || null,
    };
  };
})();
