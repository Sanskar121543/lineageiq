# Contributing

## Setup

```bash
git clone https://github.com/yourhandle/lineageiq
cd lineageiq
cp .env.example .env
make up
make seed
```

## Project layout

```
backend/app/
├── models/         Domain models (Pydantic). Start here when adding a new entity.
├── parsers/        Source parsers. Subclass BaseParser and implement parse().
├── graph/          Neo4j client, schema bootstrap, graph writer.
├── analyzers/      Blast radius and other graph analyses.
├── llm/            LLM inference and Text2Cypher engine.
├── workers/        Celery task definitions.
└── api/routes/     FastAPI route modules.

frontend/src/
├── api/            Typed API client.
├── components/     Reusable React components.
└── pages/          Top-level page components.
```

## Adding a new lineage source

1. Create `backend/app/parsers/my_source_parser.py`, subclass `BaseParser`.
2. Implement `parse(config: dict) -> list[LineageEvent]`.
3. Add a Celery task in `workers/tasks.py` that calls your parser and `GraphWriter.write_event()`.
4. Add a route in `api/routes/lineage.py` that dispatches to the new task.
5. Add unit tests in `tests/unit/test_parsers.py`.

## Running tests

```bash
make test-unit          # offline, no dependencies
make test-integration   # requires docker compose stack
make test               # both
```

## Linting

```bash
make lint    # ruff + mypy (backend) + eslint (frontend)
make fmt     # auto-format
```

## Eval

After making changes to the Text2Cypher prompt or few-shot examples:

```bash
make eval   # runs 100-query eval harness, outputs accuracy report to results/
```

The baseline is 88%. PRs that drop accuracy below 85% should include an explanation.

## Pull Request checklist

- [ ] Unit tests pass (`make test-unit`)
- [ ] New parsers have at least 5 unit tests
- [ ] No `mypy` errors in changed files
- [ ] `ruff` passes with no warnings
- [ ] `CHANGELOG.md` updated (if applicable)
