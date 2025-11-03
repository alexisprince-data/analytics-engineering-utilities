"""
metrics_definition_loader.py
Load metric definitions (YAML/JSON) and render a SELECT.
"""
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any
import json, sys

try:
    import yaml  # type: ignore
    _HAS_YAML = True
except Exception:
    _HAS_YAML = False

def load_config(path: Path) -> Dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in {".yml", ".yaml"}:
        if not _HAS_YAML:
            raise RuntimeError("Install PyYAML to use YAML configs, or provide JSON instead.")
        return yaml.safe_load(text)  # type: ignore
    return json.loads(text)

def render_sql_select(cfg: Dict[str, Any]) -> str:
    parts = []
    for m in cfg.get("metrics", []):
        name = m["name"]
        expr = m["expression"]
        parts.append(f"    {expr} AS {name}")
    select_list = ",\n".join(parts) if parts else "    1 AS no_metrics_configured"
    return f"SELECT\n{select_list}\nFROM fact_cost;"

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python metrics_definition_loader.py <metrics.yml|json>")
        raise SystemExit(1)
    cfg = load_config(Path(sys.argv[1]))
    print(render_sql_select(cfg))
