import json
from types import SimpleNamespace

import pytest

from ingestion import acquisition
from ingestion.config import load_config, parse_config, archive_year
from ingestion.orchestration import PipelineEvent


def manifest(items):
    return {'bulkDataProductBag': [{'productFileBag': {'fileDataBag': items}}]}


def item(name, date):
    return {'fileName': name, 'fileDataFromDate': date, 'fileTypeText': 'Data',
            'fileDownloadURI': f'https://api.uspto.gov/files/{date[:4]}/{name}'}


def test_defaults_match_selected_document_eras_and_round_trip():
    config = parse_config({})
    assert {c.name: c.enabled for c in config.categories} == {
        'pre_1976_grants': False, 'aps_grants': True, 'xml_grants': True, 'application_publications': True}
    assert parse_config(config.as_dict()) == config
    assert load_config('ingestion.example.toml') == config
    assert config.archives_per_run is None


def test_only_modern_publications_can_be_enabled(tmp_path):
    path = tmp_path / 'ingestion.toml'
    path.write_text('''archives_per_run = 5
[categories.aps_grants]
enabled = false
[categories.xml_grants]
enabled = true
[categories.application_publications]
enabled = true
''')
    config = load_config(path)
    assert config.archives_per_run == 5
    assert not next(c for c in config.categories if c.name == 'aps_grants').enabled
    disabled = PipelineEvent('render', {'category': 'aps_grants', 'publication_year': 1990})
    assert not config.accepts_event(disabled)
    assert config.accepts_event(PipelineEvent('render', {'category': 'xml_grants', 'publication_year': 2002}))


def test_year_filter_uses_data_coverage_not_upload_time():
    assert archive_year(dict(item('old.zip', '1976-01-06'), fileReleaseDate='2026-09-01')) == 1976
    category = next(c for c in parse_config({'categories': {'aps_grants': {'from_year': 2001}}}).categories if c.name == 'aps_grants')
    assert not category.accepts(2000)
    assert category.accepts(2001)
    assert not category.accepts(2002)


@pytest.mark.parametrize('value', [
    {'categories': {'apss_grants': {'enabled': False}}},
    {'categories': {'aps_grants': {'enabled': 'false'}}},
    {'categories': {'aps_grants': {'from_year': 1975}}},
    {'categories': {'pre_1976_grants': {'enabled': True}}},
    {'archives_per_run': -1},
])
def test_invalid_choices_fail_clearly(value):
    with pytest.raises(ValueError):
        parse_config(value)


def test_catalog_returns_oldest_first_while_selecting_latest_revision():
    data = manifest([item('a2002.zip', '2002-01-01'), item('a2001.zip', '2001-01-02'),
                     item('a2001_r2.zip', '2001-01-02'), item('a2003.zip', '2003-01-01')])
    assert [i['fileName'] for i in acquisition.entries(data)] == ['a2001_r2.zip', 'a2002.zip', 'a2003.zip']


def test_missing_archives_are_merged_oldest_first_across_categories(monkeypatch):
    catalogs = {
        'PTGRAPS': manifest([item('aps2001.zip', '2001-12-25'), item('aps1976.zip', '1976-01-06')]),
        'PTGRXML': manifest([item('xml2003.zip', '2003-01-07'), item('xml2002.zip', '2002-01-01')]),
        'APPXML': manifest([item('app2002.zip', '2002-01-03'), item('app2001.zip', '2001-03-15')]),
    }
    class Response:
        def __init__(self, value): self.value = value
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def read(self): return json.dumps(self.value).encode()
    calls = []
    monkeypatch.setenv('USPTO_API_KEY', 'test-key')
    monkeypatch.setattr(acquisition, 'published_archives', lambda _: {})
    monkeypatch.setattr(acquisition, 'urlopen', lambda req, **kwargs: Response(catalogs[req.full_url.rsplit('/', 1)[1]]))
    def fetch(blobs, data, product, key, *, category):
        calls.append((data['fileName'], category.profile))
        return data['fileName'] != 'aps1976.zip'  # Already present: advance to next missing archive.
    monkeypatch.setattr(acquisition, 'fetch_one', fetch)
    config = parse_config({})
    assert acquisition.fetch_archives(None, config) == 5
    assert [name for name, _ in calls] == ['aps1976.zip', 'app2001.zip', 'aps2001.zip', 'xml2002.zip', 'app2002.zip', 'xml2003.zip']
    assert calls[2][1].startswith('aps-')
    assert calls[3][1].startswith('uspto-xml-')


def test_disabled_categories_are_never_discovered(monkeypatch):
    config = parse_config({'categories': {'aps_grants': {'enabled': False},
                                         'xml_grants': {'enabled': False},
                                         'application_publications': {'enabled': False}}})
    monkeypatch.setattr(acquisition, 'urlopen', lambda *args, **kwargs: pytest.fail('must not call USPTO'))
    assert acquisition.fetch_archives(None, config) == 0
