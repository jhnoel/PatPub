"""Saved document-category choices shared by acquisition and pending-work recovery."""
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import tomllib

XML_PROFILE = 'uspto-xml-markdown-v2:zstd-9-checksum'
APS_PROFILE = 'aps-greenbook-markdown-v2:zstd-9-checksum'


@dataclass(frozen=True)
class Category:
    name: str
    product: str | None
    profile: str | None
    enabled: bool
    from_year: int
    through_year: int | None

    def accepts(self, year):
        return self.enabled and year >= self.from_year and (self.through_year is None or year <= self.through_year)


DEFINITIONS = {
    'pre_1976_grants': (None, None, False, 1790, 1975),
    'aps_grants': ('PTGRAPS', APS_PROFILE, True, 1976, 2001),
    'xml_grants': ('PTGRXML', XML_PROFILE, True, 2002, None),
    'application_publications': ('APPXML', XML_PROFILE, True, 2001, None),
}


@dataclass(frozen=True)
class IngestionConfig:
    archives_per_run: int | None
    categories: tuple[Category, ...]

    def as_dict(self):
        return {'schema_version': 1, 'archives_per_run': self.archives_per_run or 0,
                'categories': {c.name: {'enabled': c.enabled, 'from_year': c.from_year,
                                       **({'through_year': c.through_year} if c.through_year is not None else {})}
                               for c in self.categories}}

    def accepts_event(self, event):
        payload = event.payload
        product = payload.get('product')
        if product is None:
            parts = str(payload.get('raw', {}).get('key', '')).split('/')
            product = next((p for p in parts if p in {'PTGRAPS', 'PTGRXML', 'APPXML'}), None)
        name = payload.get('category')
        if name is None:
            name = next((c.name for c in self.categories if c.product == product and product is not None), None)
        if name is None and str(payload.get('renderer_profile', '')).startswith('aps-greenbook-'):
            name = 'aps_grants'
        category = next((c for c in self.categories if c.name == name), None)
        if category is None:
            raise RuntimeError('pending archive has no identifiable document category')
        if not category.enabled:
            return False
        year = payload.get('publication_year')
        if year is not None:
            return category.accepts(int(year))
        # Old stage records lack dates. Admit only an unrestricted category;
        # narrowed date ranges must not silently ingest out-of-range archives.
        default = DEFINITIONS[category.name]
        if (category.from_year, category.through_year) != (default[3], default[4]):
            raise RuntimeError('pending archive lacks a publication year required by the configured date range')
        return True


def parse_config(value):
    if not isinstance(value, dict) or set(value) - {'schema_version', 'archives_per_run', 'categories'}:
        raise ValueError('unknown ingestion configuration field')
    if value.get('schema_version', 1) != 1:
        raise ValueError('unsupported ingestion configuration schema')
    limit = value.get('archives_per_run', 0)
    if type(limit) is not int or limit < 0:
        raise ValueError('archives_per_run must be a nonnegative integer (0 means drain until complete)')
    limit = limit or None
    selected = value.get('categories', {})
    if not isinstance(selected, dict) or set(selected) - set(DEFINITIONS):
        raise ValueError('unknown document category')
    categories = []
    for name, (product, profile, enabled, start, end) in DEFINITIONS.items():
        values = selected.get(name, {})
        if not isinstance(values, dict) or set(values) - {'enabled', 'from_year', 'through_year'}:
            raise ValueError(f'invalid category settings: {name}')
        enabled = values.get('enabled', enabled)
        first = values.get('from_year', start)
        last = values.get('through_year', end)
        if type(enabled) is not bool or type(first) is not int or (last is not None and type(last) is not int):
            raise ValueError(f'invalid category types: {name}')
        if first < start or (end is not None and (last is None or last > end)) or (last is not None and last < first):
            raise ValueError(f'date range exceeds category coverage: {name}')
        if enabled and product is None:
            raise ValueError('pre_1976_grants is not supported: no pre-1976 full-text/OCR ingestion adapter is implemented')
        categories.append(Category(name, product, profile, enabled, first, last))
    return IngestionConfig(limit, tuple(categories))


def load_config(path=None):
    if path is not None:
        return parse_config(tomllib.loads(Path(path).read_text()))
    configured_path = os.environ.get('PATPUB_INGESTION_CONFIG')
    if configured_path:
        return load_config(configured_path)
    if os.environ.get('INGESTION_CONFIG_JSON'):
        return parse_config(json.loads(os.environ['INGESTION_CONFIG_JSON']))
    local = Path('ingestion.toml')
    return load_config(local) if local.exists() else parse_config({})


def archive_year(item):
    # Use the publication coverage date, never the later upload/release date.
    for field in ('fileDataFromDate', 'fileDate', 'fileDataToDate'):
        match = re.match(r'(\d{4})-', str(item.get(field, '')))
        if match:
            return int(match.group(1))
    match = re.search(r'/((?:19|20)\d{2})/', str(item.get('fileDownloadURI', '')))
    if match:
        return int(match.group(1))
    raise RuntimeError(f'archive has no publication year: {item.get("fileName")}')


def archive_date(item):
    for field in ('fileDataFromDate', 'fileDate', 'fileDataToDate'):
        value = str(item.get(field, ''))
        if re.match(r'\d{4}-\d{2}-\d{2}', value):
            return value[:10]
    # Year-only catalogs retain a deterministic filename order within that year.
    return f'{archive_year(item):04d}-01-01'
