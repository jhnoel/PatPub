import pytest

from ingestion.search_query import generation_filter


def test_generation_selection_handles_a_full_corpus_without_boolean_expansion():
    generations = [f"sha256:{i:064x}" for i in range(4000)]
    query = generation_filter(generations)
    assert query.startswith("source_generation:IN [")
    assert query.count('"sha256:') == 4000
    assert " OR " not in query


def test_small_generation_selection_deduplicates_and_rejects_injection():
    generation = "sha256:" + "a" * 64
    assert generation_filter([generation, generation]) == f'source_generation:"{generation}"'
    with pytest.raises(ValueError):
        generation_filter([generation + '" OR *'])
    with pytest.raises(ValueError):
        generation_filter([])


def test_partial_generation_keeps_only_explicit_publications():
    old = "sha256:" + "a" * 64
    new = "sha256:" + "b" * 64
    query = generation_filter([old, new], {old: ["US:20230031452:A1"]})
    assert '(source_generation:"' + old + '" AND (publication_uid:"US:20230031452:A1"))' in query
    assert query.count(old) == 1
    assert new in query
    with pytest.raises(ValueError):
        generation_filter([new], {old: ["US:20230031452:A1"]})
    with pytest.raises(ValueError):
        generation_filter([old], {old: ['US:1:A1" OR *']})
