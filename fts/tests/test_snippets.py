from fts.production.serving.api.snippets import (
    highlight_needles,
    present_hit,
    sections_from_canonical_markdown,
    snippets_for_document,
)


DOCUMENT = {
    "patent_number": "RE28671",
    "kind_code": "E",
    "title": "Hydrophone damper assembly",
    "abstract": "A damper for use in submerged hydrophone suspension systems.",
    "claims": "1. A hydrophone damper.",
    "description": "x" * 200 + " flow past the hydrophone to minimize " + "y" * 200,
    "source_stem": "pftaps19760106_wk01",
}


def test_needles_keep_positive_terms_and_phrases():
    needles = highlight_needles(
        'title:"damper assembly" AND hydrophone NOT glove kind_code:E'
    )
    assert needles == ["damper assembly", "hydrophone"]


def test_sections_from_canonical_markdown():
    markdown = (
        "<!-- publication_uid: US:1:B2 -->\n"
        "# Hydrophone damper\n"
        "\n"
        "## Abstract\n"
        "\n"
        "A hydrophone damper assembly.\n"
        "\n"
        "## Claims\n"
        "\n"
        "The assembly comprises a damper.\n"
    )
    sections = sections_from_canonical_markdown(markdown)
    assert sections == {
        "title": "Hydrophone damper",
        "abstract": "A hydrophone damper assembly.",
        "claims": "The assembly comprises a damper.",
        "description": "",
    }
    assert sections_from_canonical_markdown("US:1:B2") is None
    snippets = snippets_for_document("hydrophone damper", sections)
    fields = {item["field"] for item in snippets}
    assert fields & {"title", "abstract"}


def test_aps_style_description_heading_is_snippetable():
    markdown = (
        "<!-- publication_uid: US:4980925:B1 -->\n"
        "# Monopulse first detector array\n"
        "## Abstract\n"
        "\n"
        "A millimeter wave array.\n"
        "\n"
        "## Description Of The Invention\n"
        "\n"
        "Semiconductor processing techniques ensure the array works.\n"
        "\n"
        "## Claims\n"
        "\n"
        "1. An array.\n"
    )
    sections = sections_from_canonical_markdown(markdown)
    assert sections is not None
    assert "Semiconductor processing" in sections["description"]
    snippets = snippets_for_document("semiconductor", sections)
    assert snippets
    assert any("semiconductor" in item["text"].casefold() for item in snippets)


def test_hit_carries_snippets_and_drops_the_body():
    hit = present_hit('title:"damper assembly" AND hydrophone', DOCUMENT)
    assert hit["patent_number"] == "RE28671"
    assert "claims" not in hit
    assert "description" not in hit
    fields = {item["field"] for item in hit["snippets"]}
    assert "title" in fields
    assert "abstract" in fields
    title = next(item["text"] for item in hit["snippets"] if item["field"] == "title")
    assert "<b>damper assembly</b>" in title
    abstract = next(item["text"] for item in hit["snippets"] if item["field"] == "abstract")
    assert "<b>hydrophone</b>" in abstract
