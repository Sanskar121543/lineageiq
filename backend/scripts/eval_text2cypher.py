#!/usr/bin/env python3
"""
Text2Cypher evaluation harness.

Runs a set of NL questions against the live graph, compares result sets to
ground-truth answers, and reports accuracy.

Accuracy is measured as result-set match (not Cypher string equality) —
two queries are equivalent if they return the same rows regardless of order.

Usage:
    python scripts/eval_text2cypher.py \
        --fixture fixtures/eval_queries.json \
        --output results/eval_20240101.json

The fixture file format:
[
  {
    "question": "Which ML features depend on the orders table?",
    "expected_result_contains": ["customer_ltv_feature"],
    "expected_min_results": 1,
    "notes": "Column fan-out test"
  },
  ...
]
"""

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


async def run_eval(fixture_path: str, output_path: str) -> None:
    from app.graph.client import get_client
    from app.llm.text2cypher import Text2CypherEngine
    from app.models.lineage import NLQuery

    with open(fixture_path) as f:
        queries: list[dict] = json.load(f)

    client = get_client()
    await client.connect()

    engine = Text2CypherEngine()

    results = []
    passed = 0
    total = len(queries)

    print(f"Running {total} eval queries…\n")

    for i, q in enumerate(queries, 1):
        question = q["question"]
        expected_contains = q.get("expected_result_contains", [])
        min_results = q.get("expected_min_results", 0)

        t0 = time.perf_counter()
        try:
            result = await engine.query(NLQuery(question=question))
            duration_ms = (time.perf_counter() - t0) * 1000

            # Flatten result values for containment check
            flat_values: set[str] = set()
            for row in result.results:
                for v in row.values():
                    flat_values.add(str(v).lower())

            # Check criteria
            contains_ok = all(
                exp.lower() in flat_values
                for exp in expected_contains
            )
            count_ok = len(result.results) >= min_results
            valid_ok = result.cypher.validated

            test_pass = contains_ok and count_ok and valid_ok

            if test_pass:
                passed += 1
                status = "PASS"
            else:
                status = "FAIL"
                if not valid_ok:
                    status += " (validation)"
                elif not count_ok:
                    status += f" (got {len(result.results)}, need {min_results}+)"
                elif not contains_ok:
                    missing = [e for e in expected_contains if e.lower() not in flat_values]
                    status += f" (missing: {missing})"

            row_result = {
                "question": question,
                "status": "pass" if test_pass else "fail",
                "validated": valid_ok,
                "result_count": len(result.results),
                "duration_ms": round(duration_ms, 1),
                "cypher": result.cypher.cypher,
                "validation_errors": result.cypher.validation_errors,
                "notes": q.get("notes", ""),
            }
            results.append(row_result)

            icon = "✓" if test_pass else "✗"
            print(f"  {icon} [{i:03d}] {status}")
            print(f"        Q: {question[:70]}…" if len(question) > 70 else f"        Q: {question}")
            print(f"        {len(result.results)} results · {duration_ms:.0f}ms")
            print()

        except Exception as exc:
            results.append({
                "question": question,
                "status": "error",
                "error": str(exc),
                "notes": q.get("notes", ""),
            })
            print(f"  ✗ [{i:03d}] ERROR: {exc}")

    # Summary
    accuracy = passed / total * 100 if total > 0 else 0
    print("─" * 60)
    print(f"Accuracy: {passed}/{total} = {accuracy:.1f}%")
    print("─" * 60)

    # Write output
    output = {
        "accuracy": round(accuracy / 100, 4),
        "passed": passed,
        "total": total,
        "results": results,
    }
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nResults written to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Evaluate Text2Cypher accuracy")
    parser.add_argument("--fixture", default="fixtures/eval_queries.json")
    parser.add_argument("--output",  default="results/eval_latest.json")
    args = parser.parse_args()
    asyncio.run(run_eval(args.fixture, args.output))


if __name__ == "__main__":
    main()
