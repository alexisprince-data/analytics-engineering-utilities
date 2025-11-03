# analytics-engineering-utilities
This repo provides modular utilities aligned to dbt-style analytics engineering workflows: layered modeling (raw → staging → marts), reusable automation frameworks, and semantic layer enablement for downstream business consumption.

## Contents
- `ftp_framework.py` — class-based FTP/SFTP ingestion template (Python OOP, hash validation hooks)
- `cost_model_tests.sql` — dbt-style assertions for `fact_cost`, `dim_*` integrity (zero-rows = pass)
- `metrics_definition_loader.py` — load metric definitions from YAML/JSON and render a SELECT for quick prototyping

## Quick Start
1. Drop `ftp_framework.py` into your project and update connection details.
2. Run `cost_model_tests.sql` against your warehouse; tests pass when each query returns **0 rows**.
3. Define metrics in `metrics.yml` and run:

```bash
python metrics_definition_loader.py metrics.yml > preview.sql
