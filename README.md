# agentic_bigquery_unit_testing

`agentic_bigquery_unit_testing` is a Codex/Claude/copilot skill that generates BigQuery Pipeline E2E tests from a requirement document (e.g. `TECHSPEC.md`).

It generates Pipeline E2E tests (source mocks → pipeline run → final-table assertions) for:
- dbt
- Dataform

## What this skill does

- Uses `TECHSPEC.md` as the source of truth for business logic and expected outputs. An example `TECHSPEC.md` can be found at https://github.com/wayneweicheng/bigquery_unit_testing_demo/blob/main/dbt_projects/thelook_customer_analytics/TECHSPEC.md, based on the well known public dataset "thelook".
- Detects whether your project is dbt or Dataform.
- Generates Pipeline E2E tests for final models (source mocks → pipeline run → final-table assertions).
- Writes tests in framework-appropriate locations. Examples based on the above TECHSPEC.md:
  - dbt: [thelook_customer_analytics/integration_tests/](https://github.com/wayneweicheng/bigquery_unit_testing_demo/tree/main/dbt_projects/thelook_customer_analytics/integration_tests)
  - Dataform: [thelook_customer_analytics/integration_tests/](https://github.com/wayneweicheng/bigquery_unit_testing_demo/tree/main/dataform_projects/thelook_customer_analytics/integration_tests)

## Why Pipeline E2E Tests?

Pipeline E2E tests run the actual pipeline end-to-end — real SQL, real BigQuery execution, real dependency resolution — but with controlled mock source data:

```
mock sources → [intermediate model A] → [intermediate model B] → final table → assert rows
```

This is the **source → final scenario** pattern, and it's valuable for several key reasons:

### 1. You test the full DAG, not just one model
A unit test on your final model assumes its upstream inputs are correct. A Pipeline E2E test proves that data flowing *through* your entire chain of models produces the right final output. Broken `JOIN` keys, mismatched schemas, or wrong `ref()` wiring all get caught here — unit tests cannot.

### 2. Business logic spans multiple models
Real pipelines split logic across 3–5 intermediate models (staging → intermediate → mart). A business rule like *"a customer is 'high-value' if lifetime spend > $1,000 excluding returns"* may touch 3 different models. Only a Pipeline E2E test validates that the full chain implements the rule correctly.

### 3. TECHSPEC scenarios map directly to Pipeline E2E test cases
When you write a `TECHSPEC.md` with named scenarios (e.g., *"customer with two orders, one return"*), each scenario becomes a precise set of:
- **mock source rows** (what goes in)
- **expected final rows** (what must come out)

This is a direct, unambiguous spec → test mapping. Unit tests can't do this cleanly because they're scoped to a single model and lose the end-to-end narrative.

### 4. They catch schema/contract breakage
If an upstream team changes a source table's column name or type, your unit tests (which use inline mocks) will still pass — but a Pipeline E2E test will fail immediately because it runs against the real pipeline logic.

### 5. Refactoring safety
If you restructure your intermediate models (split one into two, merge two into one), unit tests break en masse. Pipeline E2E tests stay green as long as the **final output contract** is preserved — which is exactly what matters.

### 6. Essential in the age of AI-generated pipeline code
As teams increasingly use LLMs (Copilot, Claude, Codex) to generate intermediate models, the SQL in those models is inherently non-deterministic — two runs of the same prompt can produce different joins, CTEs, or column expressions. Unit tests tied to specific intermediate model logic become brittle and hard to maintain when that logic can change with every AI re-generation.

Pipeline E2E tests sidestep this entirely: **you only assert on what matters — the source data state and the final output state**. The intermediate models are treated as an implementation detail. Whether an LLM regenerates them tomorrow with different CTEs or a different join order, your test stays valid as long as the source → final contract holds. This makes Pipeline E2E tests the natural testing strategy for AI-assisted data engineering.

### The Complementary Picture

| | Native Unit Tests | Pipeline E2E Tests |
|---|---|---|
| **Scope** | Single model | Full DAG (source → final) |
| **Speed** | Very fast (seconds) | Slower (full pipeline run) |
| **Cost** | Near zero | Real BQ execution |
| **Catches** | Logic bugs in one SQL | Wiring, schema, end-to-end logic |
| **Spec mapping** | Partial (one node) | Complete (scenario-level) |
| **Best for** | Complex calculations, edge-case SQL | Business scenario validation |

> **Rule of thumb:** use unit tests to guard individual model logic; use Pipeline E2E tests to prove that the TECHSPEC scenarios actually work when the full pipeline runs. For final models, Pipeline E2E tests are almost always the more meaningful signal.

## Install the skill

From your working project directory:

```bash
mkdir -p {.claude|.codex>}/skills
git clone https://github.com/wayneweicheng/agentic_bigquery_unit_testing.git {.claude|.codex>}/skills/agentic_bigquery_unit_testing
```


## Prerequisites

Install and configure the tools needed by your framework:
- `pytest`
- `bq` CLI (authenticated to your GCP project)
- dbt users: `dbt` + working `profiles.yml`
- Dataform users: `dataform` + `.df-credentials.json` in project

## How to use from requirement docs

1. Prepare a `TECHSPEC.md` with:
   - source entities and columns
   - output model/table and grain
   - business rules
   - scenario expectations
   The same `TECHSPEC.md` should be used to develop the dbt or dataform pipeline
2. Ask the agent to generate tests using this skill.
3. Review generated test files.
4. Run tests in your pipeline project.

## Prompt: dbt Pipeline E2E tests from TECHSPEC

```text
Use skill agentic_bigquery_unit_testing as the governing rules.

Please create pytest Pipeline E2E tests for final model(s) in this dbt project from TECHSPEC.

Required inputs:
1) TECHSPEC path: <path_to_TECHSPEC.md>
2) dbt project path: <path_to_dbt_project>
```

Run dbt Pipeline E2E tests:

```bash
pytest <dbt_project>/integration_tests/test_dbt_integration.py -v
```

Optional dbt E2E test overrides:

```bash
export DBT_TARGET=<target_name>
export TARGET_SCHEMA=<schema_name>
# default command is dbt run; set build only when needed
export DBT_COMMAND=run
```

## Prompt: Dataform Pipeline E2E tests for final models

```text
Use skill agentic_bigquery_unit_testing as the governing rules.

Please create pytest Pipeline E2E tests for final model(s) in this Dataform project from TECHSPEC.

Required inputs:
1) TECHSPEC path: <path_to_TECHSPEC.md>
2) Dataform project path: <path_to_dataform_project>
```

Run Dataform Pipeline E2E tests:

```bash
pytest <dataform_project>/integration_tests/test_dataform_integration.py -v
```

## Generated outputs (typical)

dbt:
- E2E test harness: `<dbt_project>/integration_tests/`

Dataform:
- E2E test harness: `<dataform_project>/integration_tests/`
