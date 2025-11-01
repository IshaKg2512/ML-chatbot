from typing import Any, Dict


class PromptTemplates:
    """Collection of simple prompt builders used by the demo app."""

    def answer_synthesis_prompt(self, question: str, results: Dict[str, Any]) -> str:
        return (
            "You are an analyst answering questions about Indian agriculture and climate data.\n"
            "Use the provided structured results to produce a short, clear answer.\n\n"
            f"Question:\n{question}\n\n"
            f"Structured results (JSON-like):\n{results}\n\n"
            "Answer:"
        )


