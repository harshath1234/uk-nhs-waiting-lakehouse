"""The ADF pipeline reads manifest fields by name. Keep the JSON and the Python in step."""

import json
import re
from dataclasses import fields
from pathlib import Path

from nhs_ingest.discover import ManifestEntry

ADF = Path(__file__).resolve().parents[2] / "adf"


def load(kind: str) -> dict[str, dict]:
    return {p.stem: json.loads(p.read_text()) for p in (ADF / kind).glob("*.json")}


def walk(obj):
    if isinstance(obj, dict):
        for v in obj.values():
            yield from walk(v)
        yield obj
    elif isinstance(obj, list):
        for v in obj:
            yield from walk(v)


def test_every_adf_file_is_named_after_its_object():
    for kind in ("linkedService", "dataset", "pipeline"):
        for stem, doc in load(kind).items():
            assert doc["name"] == stem


def test_pipeline_uses_only_manifest_fields_that_exist():
    text = json.dumps(load("pipeline")["pl_land_nhs"])
    used = set(re.findall(r"item\(\)\.(\w+)", text))
    known = {f.name for f in fields(ManifestEntry)}
    assert used and used <= known, used - known


def test_pipeline_references_existing_datasets_with_their_parameters():
    datasets = load("dataset")
    for node in walk(load("pipeline")):
        if node.get("type") == "DatasetReference":
            ds = datasets[node["referenceName"]]["properties"]
            assert set(node.get("parameters", {})) == set(ds.get("parameters", {}))


def test_datasets_reference_existing_linked_services():
    linked = set(load("linkedService")) | {"ls_adls"}  # ls_adls is native Terraform (managed identity)
    for doc in load("dataset").values():
        assert doc["properties"]["linkedServiceName"]["referenceName"] in linked
