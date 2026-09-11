# /// script
# requires-python = ">=3.11"
# dependencies = ["pyyaml"]
# ///

# SPDX-License-Identifier: Apache-2.0

"""Converts an upstream registry YAML into the JSON the Go test reads.

Only what the check needs is kept -- each attribute's key, and the values of its
enum members if it has any. Carrying the briefs and notes as well would make the
vendored file large and noisy in diffs for no gain: the test compares names and
member sets, so a reworded description should not show up as upstream drift.

Provenance is recorded in the file itself, so a reader of the JSON can tell
where it came from without consulting the script that fetched it.
"""

import json
import os
import sys

import yaml

src, out = sys.argv[1], sys.argv[2]
doc = yaml.safe_load(open(src))

def attribute_lists(node):
    """Yield every attribute list in the document.

    Upstream uses two shapes. semantic-conventions-genai is file_format
    definition/2, with attributes at the top level. semantic-conventions at
    v1.41.0 is the older layout, a `groups` list where each group carries its
    own attributes. Both are read rather than picking one, because the point of
    this file is to compare a frozen schema against a moving one and they do not
    have to agree about formatting.
    """
    if isinstance(node, dict):
        if isinstance(node.get("attributes"), list):
            yield node["attributes"]
        for value in node.values():
            yield from attribute_lists(value)
    elif isinstance(node, list):
        for item in node:
            yield from attribute_lists(item)


attributes = {}
for group in attribute_lists(doc):
    for attr in group:
        if not isinstance(attr, dict):
            continue  # a group can reference an attribute by id rather than define it
        # definition/2 names an attribute with `key`; the older layout used at
        # v1.41.0 uses `id`. Same thing, renamed between the two formats.
        key = attr.get("key") or attr.get("id")
        if not key or not isinstance(key, str):
            continue
        entry = {}
        t = attr.get("type")
        if isinstance(t, dict) and "members" in t:
            entry["members"] = sorted(
                str(m["value"]) for m in t["members"] if "value" in m
            )
        attributes[key] = entry

payload = {
    "_source": {
        "target": os.environ.get("TARGET", ""),
        "repo": os.environ.get("SRC_REPO", ""),
        "ref": os.environ.get("SRC_REF", ""),
        "url": os.environ.get("SRC_URL", ""),
        "note": "vendored by refresh.sh; do not edit by hand",
    },
    "attributes": dict(sorted(attributes.items())),
}

with open(out, "w") as f:
    json.dump(payload, f, indent=2, sort_keys=False)
    f.write("\n")

print(f"  {out}: {len(attributes)} attributes")
