from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping


def canonical_json(value: Any) -> bytes:
    return json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()


@dataclass(frozen=True)
class EvaluationOutcome:
    metrics: Mapping[str,float]
    result_ref: str
    result_hash: str
    case_count: int
    result_payload: Mapping[str,Any]


def evaluate_routing_genome(config: Mapping[str,Any], suite: Mapping[str,Any], *,
                            expected_suite_hash: str) -> EvaluationOutcome:
    """Run a small deterministic capability-routing suite against a genome config."""
    suite_hash=hashlib.sha256(canonical_json(suite)).hexdigest()
    if suite_hash != expected_suite_hash:
        raise ValueError("evaluation suite integrity hash mismatch")
    cases=suite.get("cases")
    routes=config.get("routes")
    if not isinstance(cases,list) or not cases or not isinstance(routes,Mapping):
        raise ValueError("routing evaluation needs cases and a routes mapping")
    rows=[]; total_cost=0.0; passed=0
    for case in cases:
        if not isinstance(case,Mapping):
            raise ValueError("evaluation case must be an object")
        capability=case.get("capability")
        selected=routes.get(capability)
        costs=case.get("costs")
        acceptable=case.get("acceptable_executors")
        if not isinstance(selected,str) or not isinstance(costs,Mapping) or selected not in costs:
            raise ValueError(f"routing policy has no eligible costed executor for {capability}")
        success=selected in acceptable
        case_cost=float(costs[selected])
        if case_cost < 0:
            raise ValueError("evaluation costs cannot be negative")
        passed += int(success)
        total_cost += case_cost
        rows.append({"case_id":case["case_id"],"capability":capability,
                     "executor":selected,"verified":success,"cost":case_cost})
    result={"suite_hash":suite_hash,"case_results":rows,
            "metrics":{"verified_quality":passed/len(cases),"cost":total_cost}}
    result_hash=hashlib.sha256(canonical_json(result)).hexdigest()
    return EvaluationOutcome(result["metrics"],f"sha256:{result_hash}",result_hash,len(cases),result)
