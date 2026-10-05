"""Generation selection shared by serving, index verification and canaries."""

import json
import re
from collections.abc import Iterable


def generation_filter(generations: Iterable[str], selections: dict[str, list[str]] | None = None) -> str:
    values = list(dict.fromkeys(generations))
    if not values or any(not re.fullmatch(r"sha256:[0-9a-f]{64}", value) for value in values):
        raise ValueError("generation filter requires SHA-256 generation identities")
    if selections:
        if not set(selections) <= set(values):
            raise ValueError("publication selections reference inactive generations")
        whole = [value for value in values if value not in selections]
        clauses = ["(" + generation_filter(whole) + ")"] if whole else []
        for generation, uids in selections.items():
            if not isinstance(uids, list) or not uids or len(set(uids)) != len(uids) or any(
                not isinstance(uid, str) or not re.fullmatch(r"US:[0-9A-Z]+:[A-Z][0-9]", uid) for uid in uids
            ):
                raise ValueError("publication selection requires unique publication UIDs")
            terms = " OR ".join("publication_uid:" + json.dumps(uid) for uid in uids)
            clauses.append("(source_generation:" + json.dumps(generation) + " AND (" + terms + "))")
        return " OR ".join(clauses)
    if len(values) < 64:
        return " OR ".join(f"source_generation:{json.dumps(value)}" for value in values)
    # Quickwit's term-set query avoids thousands of Boolean OR clauses.
    return "source_generation:IN [" + " ".join(json.dumps(value) for value in values) + "]"
