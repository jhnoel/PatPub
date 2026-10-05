"""Build search snippets from a query string and a document's own text.

Quickwit retrieves the hits. This module marks the query's terms in the
document text and returns the windows on the hit. Callers do not zip a
side array and do not receive the stored body.
"""

from __future__ import annotations

import html
import re
from typing import Iterable


TEXT_FIELDS = ("title", "abstract", "claims", "description")
_IDENTITY_LINE = re.compile(r"^<!-- publication_uid: .+ -->$")
_H2 = re.compile(r"^## (.+)$")
_SECTION_HEADINGS = {
    "abstract": "abstract",
    "claims": "claims",
    "description": "description",
}
_OPERATORS = {"AND", "OR", "NOT", "TO", "IN"}
_PHRASE = re.compile(r'"([^"]*)"')
_FIELD = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}")
_WINDOW = 80
_MAX_FRAGMENTS = 2

_KEEP = (
    "patent_number",
    "kind_code",
    "doc_kind",
    "publication_date",
    "filing_date",
    "priority_date",
    "application_number",
    "title",
    "source_stem",
)


def highlight_needles(query: str) -> list[str]:
    """Positive terms and phrases a highlighter should mark."""
    needles: list[str] = []
    skip_next = False
    for token in _tokens(query):
        if token in {"NOT", "-"}:
            skip_next = True
            continue
        if token.endswith(":"):
            continue
        if token in _OPERATORS or token in {"(", ")", "[", "]", "{", "}", "+", "*", "TO"}:
            continue
        if _DATE.match(token):
            continue
        if skip_next:
            skip_next = False
            continue
        if token.startswith("-"):
            continue
        field, _, value = token.partition(":")
        if value and _FIELD.match(field):
            token = value
        token = token.strip("\"'*")
        if len(token) < 2 or token in _OPERATORS or _DATE.match(token):
            continue
        needles.append(token)
    seen: set[str] = set()
    unique: list[str] = []
    for needle in needles:
        key = needle.casefold()
        if key not in seen:
            seen.add(key)
            unique.append(needle)
    return unique


def sections_from_canonical_markdown(text: str) -> dict[str, str] | None:
    """Split patpubrender markdown into the fields snippets_for_document expects."""
    if not isinstance(text, str) or not text.strip():
        return None
    lines = text.splitlines()
    if not lines or not _IDENTITY_LINE.match(lines[0].strip()):
        return None
    sections = {field: "" for field in TEXT_FIELDS}
    brief_description = ""
    index = 1
    while index < len(lines):
        line = lines[index]
        if line.startswith("# ") and not line.startswith("## "):
            sections["title"] = line[2:].strip()
            index += 1
            break
        if line.startswith("## "):
            break
        index += 1
    current_heading: str | None = None
    body: list[str] = []

    def flush() -> None:
        nonlocal current_heading, body, brief_description
        if current_heading is None:
            body = []
            return
        content = "\n".join(body).strip()
        key = current_heading.casefold()
        if content:
            if key == "brief description" or key.startswith("brief description"):
                brief_description = content
            elif key in _SECTION_HEADINGS:
                sections[_SECTION_HEADINGS[key]] = content
            elif key.startswith("abstract"):
                sections["abstract"] = content
            elif key.startswith("claims"):
                sections["claims"] = content
            elif key.startswith("description") or "description of the invention" in key:
                # APS Green Books use "Description Of The Invention" etc.
                existing = sections["description"]
                sections["description"] = f"{existing}\n\n{content}".strip() if existing else content
            else:
                # Retain searchable body from alternate H2 labels (e.g. Summary).
                existing = sections["description"]
                sections["description"] = f"{existing}\n\n{content}".strip() if existing else content
        current_heading = None
        body = []

    for line in lines[index:]:
        match = _H2.match(line)
        if match:
            flush()
            current_heading = match.group(1).strip()
            continue
        if current_heading is not None:
            body.append(line)
    flush()
    if not sections["description"] and brief_description:
        sections["description"] = brief_description
    return sections


def snippets_for_document(query: str, document: dict) -> list[dict[str, str]]:
    needles = highlight_needles(query)
    found: list[dict[str, str]] = []
    for field in TEXT_FIELDS:
        text = document.get(field)
        if not isinstance(text, str) or not text:
            continue
        windows = _windows(text, needles)
        found.extend({"field": field, "text": window} for window in windows)
    return found


def present_hit(query: str, document: dict) -> dict:
    """Identity fields plus snippets. The stored body stays on the search node."""
    hit = {key: document[key] for key in _KEEP if key in document}
    hit["snippets"] = snippets_for_document(query, document)
    return hit


def _tokens(query: str) -> Iterable[str]:
    index = 0
    while index < len(query):
        char = query[index]
        if char.isspace():
            index += 1
            continue
        if char == '"':
            end = query.find('"', index + 1)
            if end < 0:
                yield query[index + 1 :]
                return
            yield query[index + 1 : end]
            index = end + 1
            continue
        if char in "()[]{}":
            yield char
            index += 1
            continue
        end = index + 1
        while end < len(query) and not query[end].isspace() and query[end] not in "()[]{}\"":
            end += 1
        yield query[index:end]
        index = end


def _windows(text: str, needles: list[str]) -> list[str]:
    if not needles:
        return []
    pattern = re.compile(
        "|".join(re.escape(needle) for needle in sorted(needles, key=len, reverse=True)),
        re.IGNORECASE,
    )
    spans = [(match.start(), match.end()) for match in pattern.finditer(text)]
    if not spans:
        return []
    grouped: list[tuple[int, int]] = []
    for start, end in spans:
        window_start = max(0, start - _WINDOW)
        window_end = min(len(text), end + _WINDOW)
        if grouped and window_start <= grouped[-1][1]:
            grouped[-1] = (grouped[-1][0], max(grouped[-1][1], window_end))
        else:
            grouped.append((window_start, window_end))
    fragments: list[str] = []
    for start, end in grouped[:_MAX_FRAGMENTS]:
        chunk = text[start:end]
        pieces: list[str] = []
        cursor = 0
        for match in pattern.finditer(chunk):
            pieces.append(html.escape(chunk[cursor:match.start()]))
            pieces.append("<b>" + html.escape(match.group(0)) + "</b>")
            cursor = match.end()
        pieces.append(html.escape(chunk[cursor:]))
        prefix = "…" if start else ""
        suffix = "…" if end < len(text) else ""
        fragments.append(prefix + "".join(pieces) + suffix)
    return fragments
