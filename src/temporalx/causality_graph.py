"""Optional *actual* LangGraph orchestration for the deterministic forensic tools.

This does not require an LLM; graph nodes call grounded and tested tools.
Any optional LLM explanation should remain untrusted and must never authorize IO.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, TypedDict

from temporalx.causality import (
    collect, investigate, experiment, decide, assemble, verify_report, EvidenceRejected
)


class ResearchState(TypedDict, total=False):
    folder: str
    evidence: dict[str, Any]
    observations: dict[str, Any]
    trial: dict[str, Any]
    policy: dict[str, Any]
    report: dict[str, Any]


def build_graph():
    try:
        from langgraph.graph import StateGraph, START, END
    except ImportError as exc:
        raise RuntimeError('Install the optional graph dependency: pip install -e ".[agent]"') from exc

    def intake(state: ResearchState) -> dict:
        return {'evidence': collect(Path(state['folder']))}

    def hypotheses(state: ResearchState) -> dict:
        return {'observations': investigate(state['evidence'])}

    def sandbox_replay(state: ResearchState) -> dict:
        return {'trial': experiment(state['evidence'])}

    def review_gate(state: ResearchState) -> dict:
        return {'policy': decide(state['evidence'], state['observations'], state['trial'])}

    def fail_closed(state: ResearchState) -> dict:
        # Failures never pass through the normal candidate analysis branch.
        policy = decide(state['evidence'], state['observations'], state['trial'])
        if policy['recommendation'] != 'BLOCK':
            raise EvidenceRejected('Graph fail-closed branch must yield BLOCK')
        return {'policy': policy}

    def route_trial(state: ResearchState) -> str:
        return 'block' if state['trial']['status'] != 'PASSED' else 'review'

    def finalize(state: ResearchState) -> dict:
        report = assemble(state['evidence'], state['observations'], state['trial'], state['policy'])
        if not verify_report(report, Path(state['folder'])):
            raise EvidenceRejected('Input integrity changed during LangGraph run')
        return {'report': report}

    graph = StateGraph(ResearchState)
    graph.add_node('collect', intake)
    graph.add_node('hypothesize', hypotheses)
    graph.add_node('isolated_replay', sandbox_replay)
    graph.add_node('policy_gate', review_gate)
    graph.add_node('fail_closed', fail_closed)
    graph.add_node('produce_evidence', finalize)
    graph.add_edge(START, 'collect')
    graph.add_edge('collect', 'hypothesize')
    graph.add_edge('hypothesize', 'isolated_replay')
    graph.add_conditional_edges('isolated_replay', route_trial, {'block': 'fail_closed', 'review': 'policy_gate'})
    graph.add_edge('policy_gate', 'produce_evidence')
    graph.add_edge('fail_closed', 'produce_evidence')
    graph.add_edge('produce_evidence', END)
    return graph.compile()


def run_graph(folder: Path, *, enable_mlflow: bool = False) -> dict[str, Any]:
    if enable_mlflow:
        try:
            import mlflow
        except ImportError as exc:
            raise RuntimeError('Install optional tracing dependency: pip install -e ".[tracing]"') from exc
        mlflow.langchain.autolog()
    result = build_graph().invoke({'folder': str(folder)})
    return result['report']
