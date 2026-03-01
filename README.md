# agentic_bigquery_unit_testing

`agentic_bigquery_unit_testing` is a Codex/Claude skill that generates BigQuery tests from a requirement document (`TECHSPEC.md`).

It supports both frameworks:
- dbt
- Dataform

It can generate:
- framework-native unit tests
- pytest integration tests (source mocks -> pipeline run -> final-table assertions)

## What this skill does

- Uses `TECHSPEC.md` as the source of truth for business logic and expected outputs.
- Detects whether your project is dbt or Dataform.
- Defaults final-model requests to integration tests unless you explicitly ask for unit tests.
- Writes tests in framework-appropriate locations.

## Install the skill

From your working project directory:

```bash
mkdir -p .claude/skills
git clone https://github.com/wayneweicheng/agentic_bigquery_unit_testing.git .claude/skills/agentic_bigquery_unit_testing
```

If you prefer a global install for Codex, clone into your Codex skills directory instead:

```bash
mkdir -p "$CODEX_HOME/skills"
git clone https://github.com/wayneweicheng/agentic_bigquery_unit_testing.git "$CODEX_HOME/skills/agentic_bigquery_unit_testing"
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
2. Ask the agent to generate tests using this skill.
3. Review generated test files.
4. Run tests in your pipeline project.

## Prompt: dbt unit tests from TECHSPEC

```text
Use `/Users/wayne/Repo/github/agentic/agentic_bigquery_unit_testing/SKILL.md` as the governing rules.

Please create dbt unit tests from TECHSPEC for the target model(s).

Required inputs:
1) TECHSPEC path: <path_to_TECHSPEC.md>
2) dbt project path: <path_to_dbt_project>
3) Target model(s): <model_name_or_list>

Requirements:
- Use dbt `unit_tests:` YAML format.
- Write tests under `<dbt_project>/models/tests/`.
- Derive expected rows from TECHSPEC only.
```

## Prompt: Dataform unit tests from TECHSPEC

```text
Use `/Users/wayne/Repo/github/agentic/agentic_bigquery_unit_testing/SKILL.md` as the governing rules.

Please create Dataform unit tests from TECHSPEC for the target model(s).

Required inputs:
1) TECHSPEC path: <path_to_TECHSPEC.md>
2) Dataform project path: <path_to_dataform_project>
3) Target model(s): <model_name_or_list>

Requirements:
- Use Dataform `config { type: "test" }` SQLX format.
- Write tests under `<dataform_project>/definitions/tests/`.
- Derive expected rows from TECHSPEC only.
```

## Prompt: dbt integration tests for final models (default final-model mode)

```text
Use `/Users/wayne/Repo/github/agentic/agentic_bigquery_unit_testing/SKILL.md` as the governing rules.

Please create pytest integration tests for final model(s) in this dbt project from TECHSPEC.

Required inputs:
1) TECHSPEC path: <path_to_TECHSPEC.md>
2) dbt project path: <path_to_dbt_project>
3) Target final model(s): <model_name_or_list>

Requirements:
- Mock source entities defined in TECHSPEC.
- Run dbt pipeline for selected final model(s) with dependencies.
- Assert final output rows from TECHSPEC-derived expected results.
```

Run dbt integration tests:

```bash
pytest <dbt_project>/integration_tests/test_dbt_integration.py -v
```

Optional dbt integration overrides:

```bash
export DBT_TARGET=<target_name>
export TARGET_SCHEMA=<schema_name>
# default command is dbt run; set build only when needed
export DBT_COMMAND=run
```

## Prompt: Dataform integration tests for final models (default final-model mode)

```text
Use `/Users/wayne/Repo/github/agentic/agentic_bigquery_unit_testing/SKILL.md` as the governing rules.

Please create pytest integration tests for final model(s) in this Dataform project from TECHSPEC.

Required inputs:
1) TECHSPEC path: <path_to_TECHSPEC.md>
2) Dataform project path: <path_to_dataform_project>
3) Target final model(s): <model_name_or_list>

Requirements:
- Mock source entities defined in TECHSPEC.
- Run Dataform pipeline for selected final model(s) with dependencies.
- Assert final output rows from TECHSPEC-derived expected results.
```

Run Dataform integration tests:

```bash
pytest <dataform_project>/integration_tests/test_dataform_integration.py -v
```

## Generated outputs (typical)

dbt:
- unit tests: `<dbt_project>/models/tests/*.yml`
- integration harness: `<dbt_project>/integration_tests/`

Dataform:
- unit tests: `<dataform_project>/definitions/tests/*.sqlx`
- integration harness: `<dataform_project>/integration_tests/`
