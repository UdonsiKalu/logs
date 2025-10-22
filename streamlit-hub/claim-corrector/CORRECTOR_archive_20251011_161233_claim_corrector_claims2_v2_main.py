#!/usr/bin/env python3
"""
Claim Corrector (Claims Domain Version)
---------------------------------------
- Retrieves claim issues from `claim_analysis_metadata`
- Searches across all `claims__` policy collections
- Uses hybrid (vector + keyword) search
- Summarizes relevant CMS policy excerpts using a local LLM (Ollama or similar)
"""

import os
import re
import json
import torch
from typing import Dict, Any, List
from qdrant_client import QdrantClient
from qdrant_client.http import models
from sentence_transformers import SentenceTransformer
import subprocess


LLM_PROMPT = """
You are a CMS policy reasoning assistant.

You are given:
1. A Medicare claim with structured fields (CPT/HCPCS, ICD-10, denial reason, and risk level)
2. Several CMS policy excerpts retrieved from the policy vector database (e.g., NCCI, LCD, NCD, or CMS Processing Manual)

Your task:
- Read the claim carefully and understand the context (procedure, diagnosis, and denial reason).
- Review each policy excerpt one by one.
- **Discard any policy that does not directly apply** to this specific claim. (For example, reject policies about unrelated body systems, diagnoses, or services.)
- **Summarize only the *relevant* CMS rules** that help explain or justify the denial (e.g., NCCI edits, PTP conflicts, coverage exclusions, MUE thresholds).
- **Be strict about consistency:** do not infer conditions or policy details not supported by the claim or excerpts.
- If the claim and policies appear mismatched (e.g., smoking-related policy for orthopedic claim), state that clearly and exclude it from reasoning.
- Cite chapters, sections, and revisions (rev) when available.

Return your answer in structured JSON format:

{
  "claim_summary": "Concise restatement of the claim (CPT, ICD-10, and denial reason).",
  "relevant_policies": [
    {
      "collection": "...",
      "section": "...",
      "rev": "...",
      "policy_summary": "Brief summary of the CMS rule that is directly relevant to this claim."
    }
  ],
  "filtered_out_policies": ["... list of irrelevant or inconsistent policy names if any ..."],
  "final_reasoning_summary": "Single cohesive explanation connecting the claim and relevant CMS policies, consistent with the claim context."
}
"""


class ClaimCorrector:
    def __init__(self, url: str = "http://localhost:6333"):
        self.client = QdrantClient(url=url)

        #  Match embedding model from your Qdrant ingestion
        self.embedder = SentenceTransformer(
            "nomic-ai/nomic-embed-text-v1.5",
            device="cuda" if torch.cuda.is_available() else "cpu",
            trust_remote_code=True
        )

        #  Load only "claims__" policy collections
        all_collections = [c.name for c in self.client.get_collections().collections]
        self.policy_collections = [c for c in all_collections if c.startswith("claims__")]

        print(f" Loaded {len(self.policy_collections)} claims collections:")
        for c in self.policy_collections:
            print(f"   - {c}")

        #  Fixed claim data collection name
        self.claim_collection = "claim_analysis_metadata"

    # ----------------------------------------------------
    # MAIN EXECUTION
    # ----------------------------------------------------
    def run_corrections(self, claim_id: str, top_k: int = 5) -> Dict[str, Any]:
        issues = self._get_claim_issues(claim_id)
        enriched_issues = []

        for issue in issues:
            query_text = self._build_query_text(issue)
            query_vector = self.embedder.encode(query_text).tolist()
            policy_hits = []

            for collection in self.policy_collections:
                hits = self._hybrid_search(collection, query_text, query_vector, top_k)
                for h in hits:
                    p = h.payload or {}
                    excerpt = p.get("text", "")[:1200]
                    policy_hits.append({
                        "collection": collection,
                        "score": round(h.score, 4),
                        "policy_id": p.get("policy_id", "unknown"),
                        "chapter": p.get("chapter") or self._extract_chapter(excerpt),
                        "section": p.get("section") or self._extract_section(excerpt),
                        "subsection": p.get("subsection"),
                        "rev": p.get("rev") or self._extract_revision(excerpt),
                        "page": p.get("page", "n/a"),
                        "path": p.get("path"),
                        "source": p.get("source"),
                        "rule_type": p.get("rule_type", "policy"),
                        "cpt_codes": p.get("cpt_codes"),
                        "icd10_codes": p.get("icd10_codes"),
                        "modifiers": p.get("modifiers"),
                        "excerpt": excerpt or "No excerpt available",
                    })

            policy_hits = sorted(policy_hits, key=lambda x: x["score"], reverse=True)[:top_k]
            issue["policy_support"] = policy_hits

            #  Run LLM summarization for consistency and relevance filtering
            issue["policy_summary"] = self._summarize_with_llm(issue, policy_hits)
            enriched_issues.append(issue)

        return {"claim_id": claim_id, "enriched_issues": enriched_issues}

    # ----------------------------------------------------
    # CLAIM RETRIEVAL
    # ----------------------------------------------------
    def _get_claim_issues(self, claim_id: str) -> List[Dict[str, Any]]:
        """Retrieve claim issues from Qdrant collection"""
        try:
            hits, _ = self.client.scroll(
                collection_name=self.claim_collection,
                scroll_filter=models.Filter(
                    must=[models.FieldCondition(key="claim_id", match=models.MatchText(text=claim_id))]
                ),
                limit=100
            )
            return [h.payload for h in hits]
        except Exception as e:
            print(f" Failed to pull claim issues: {e}")
            return []

    # ----------------------------------------------------
    # HYBRID SEARCH
    # ----------------------------------------------------
    def _hybrid_search(self, collection: str, query_text: str, query_vector: List[float], top_k: int):
        try:
            hybrid_filter = models.Filter(
                should=[
                    models.FieldCondition(key="text", match=models.MatchText(text=query_text)),
                    models.FieldCondition(key="all_codes", match=models.MatchText(text=query_text)),
                    models.FieldCondition(key="cpt_codes", match=models.MatchText(text=query_text)),
                    models.FieldCondition(key="icd10_codes", match=models.MatchText(text=query_text)),
                    models.FieldCondition(key="modifiers", match=models.MatchText(text=query_text)),
                ]
            )

            results = self.client.query_points(
                collection_name=collection,
                query=query_vector,
                query_filter=hybrid_filter,
                limit=top_k,
                with_payload=True,
                with_vectors=False
            )

            if not results.points:
                results = self.client.query_points(
                    collection_name=collection,
                    query=query_vector,
                    limit=top_k,
                    with_payload=True,
                    with_vectors=False
                )

            return results.points
        except Exception as e:
            print(f" Hybrid search failed for {collection}: {e}")
            return []

    # ----------------------------------------------------
    # LLM SUMMARIZATION (Local)
    # ----------------------------------------------------
    def _summarize_with_llm(self, issue: Dict[str, Any], policies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run Ollama or local LLM to summarize policies"""
        input_data = {
            "claim": issue,
            "policies": policies
        }
        try:
            cmd = ["ollama", "run", "mistral", f"{LLM_PROMPT}\n\n{json.dumps(input_data, indent=2)}"]
            result = subprocess.run(cmd, capture_output=True, text=True)
            output = result.stdout.strip()
            return {"summary": output}
        except Exception as e:
            print(f" LLM summarization failed: {e}")
            return {"summary": "LLM summarization unavailable"}

    # ----------------------------------------------------
    # HELPERS
    # ----------------------------------------------------
    def _build_query_text(self, issue: Dict[str, Any]) -> str:
        dx = issue.get("icd10_code") or issue.get("icd9_code")
        proc = issue.get("hcpcs_code") or issue.get("cpt_code")
        risk = issue.get("denial_risk_level", "unspecified")
        return (
            f"CMS policy guidance for CPT/HCPCS {proc} and diagnosis {dx}. "
            f"Denial risk: {risk}. Include NCCI, LCD, and CMS manual sections."
        )

    def _extract_chapter(self, text: str) -> str:
        m = re.search(r"(Chapter\s+\d+)", text, re.IGNORECASE)
        return m.group(1) if m else None

    def _extract_section(self, text: str) -> str:
        m = re.search(r"(\d{1,3}\.\d+)", text)
        return m.group(1) if m else None

    def _extract_revision(self, text: str) -> str:
        m = re.search(r"(Rev\.\s*\d+)", text, re.IGNORECASE)
        return m.group(1) if m else None


# --------------------------
# Example Usage
# --------------------------
if __name__ == "__main__":
    corrector = ClaimCorrector()
    claim_id = "123456789012345"
    enriched = corrector.run_corrections(claim_id)
    print(json.dumps(enriched, indent=2))
