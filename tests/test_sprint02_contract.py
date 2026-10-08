"""Static deployment contract and CLI smoke checks without a Databricks account."""
import ast
import json
from pathlib import Path

import yaml

from temporalx.cli import main

ROOT = Path(__file__).resolve().parents[1]


def test_sprint02_bundle_is_valid_yaml_and_has_safe_task_dependency_chain():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    jobs = bundle["resources"]["jobs"]
    tasks = jobs["temporalx_sprint02"]["tasks"]
    assert [t["task_key"] for t in tasks] == [
        "archive_captures", "archive_refunds", "materialize_silver", "compare_with_oracle"]
    assert [t["depends_on"][0]["task_key"] for t in tasks[1:]] == [
        "archive_captures", "archive_refunds", "materialize_silver"]
    assert all(t["spark_python_task"]["python_file"] == "./spark/sprint02.py" for t in tasks)
    assert all("${var.catalog}" in t["spark_python_task"]["parameters"] for t in tasks)
    assert "temporalx_sandbox_cdf_demo" in jobs


def test_spark_script_parses_without_spark_or_workspace():
    code = (ROOT / "spark/sprint02.py").read_text(encoding="utf-8")
    tree = ast.parse(code)
    defined = {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}
    assert {"seed", "materialize_silver", "reconcile", "publish_sandbox", "read_cdf"} <= defined


def test_archive_cli_roundtrip(tmp_path, capsys):
    db = str(tmp_path / "archived.sqlite")
    path = str(ROOT / "examples" / "demo_events.jsonl")
    assert main(["archive-ingest", "--db", db, "--input", path, "--batch-id", "original"]) == 0
    assert main(["archive-status", "--db", db]) == 0
    output = capsys.readouterr().out
    assert '"deliveries": 27' in output
    assert main(["archive-compare", "--db", db, "--run-id", "run-one"]) == 0
    evidence = json.loads(capsys.readouterr().out)
    assert evidence["comparison"]["total_delta_cents"] == -7179
    assert evidence["comparison"]["candidate"]["applied_events"] == 24
