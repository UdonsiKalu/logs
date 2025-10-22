#!/usr/bin/env python3
"""
Claim Corrector (Full Fidelity + Hybrid Qdrant Version + Full LLM Summarization)
- Dynamically detects all policy collections in Qdrant.
- Uses hybrid (vector + keyword) search.
- Uses a full structured LLM prompt to filter and summarize policies.
"""

import os
import re
import json
import torch
from typing import Dict, Any, List
from qdrant_client import QdrantClient
from qdrant_client.http import models
from sentence_transformers import SentenceTransformer

try:
    import ollama
except ImportError:
    ollama = None


# ----------------------------------------------------
# FULL LLM PROMPT TEMPLATE
# ----------------------------------------------------
LLM_PROMPT = """
You are a CMS policy reasoning assistant.

You are given:
1. A Medicare claim with structured fields (CPT/HCPCS, ICD-10, denial reason, risk level)
2. Several CMS policy excerpts retrieved from the policy vector database

Your task:
- Read the claim carefully.
- Review each policy excerpt.
- **Discard any policy that does not directly apply** to the procedure, diagnosis, or denial reason.
- Summarize only the *relevant* CMS rules that help explain or justify the denial (e.g., NCCI edits, coverage limitations, MUEs, etc.).
- If some policies are unrelated, say so briefly and ignore them in your reasoning.
- Cite sections (chapter, section, rev) from each relevant policy.

Return your answer in structured JSON format:

{
  "claim_summary": "Concise restatement of the claim (procedure, diagnosis, denial reason).",
  "relevant_policies": [
    {
      "collection": "...",
      "section": "...",
      "rev": "...",
      "policy_summary": "Short summary of the CMS rule relevant to this claim."
    }
  ],
  "filtered_out_policies": ["... list of irrelevant policy names if any ..."],
  "final_reasoning_summary": "Single cohesive explanation connecting the claim and the CMS policies."
}
"""


class ClaimCorrector:
    def __init__(self, url: str = "http://localhost:6333", api_key: str = None, llm_model: str = "llama3"):
        self.client = QdrantClient(url=url, api_key=api_key)
        self.llm_model = llm_model

        # Embedding model used in cms_policies_qdrant.py
        self.embedder = SentenceTransformer(
            "nomic-ai/nomic-embed-text-v1.5",
            device="cuda" if torch.cuda.is_available() else "cpu",
            trust_remote_code=True
        )

        # Detect all policy-related collections
        try:
            all_collections = [c.name for c in self.client.get_collections().collections]
            self.policy_collections = [
                c for c in all_collections
                if any(k in c.lower() for k in ["policy", "policies", "manual", "cms", "lcd", "ncd"])
            ]
            if not self.policy_collections:
                print(" No policy-type collections detected — using fallback ['cms_policies']")
                self.policy_collections = ["cms_policies"]
            else:
                print(f" Detected policy collections: {self.policy_collections}")
        except Exception as e:
            print(f" Could not list collections, defaulting to ['cms_policies']: {e}")
            self.policy_collections = ["cms_policies"]

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
                try:
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
                            "subsection": p.get("subsection") or self._extract_subsection(excerpt),
                            "rev": p.get("rev") or self._extract_revision(excerpt),
                            "page": p.get("page", "n/a"),
                            "path": p.get("path", "n/a"),
                            "source": p.get("source", "n/a"),
                            "rule_type": p.get("rule_type", "policy"),
                            "cpt_codes": p.get("cpt_codes"),
                            "icd10_codes": p.get("icd10_codes"),
                            "modifiers": p.get("modifiers"),
                            "excerpt": excerpt or "No excerpt available",
                            "url": p.get("source_url")
                        })
                except Exception as e:
                    print(f" Hybrid search failed for {collection}: {e}")

            # Sort and trim
            policy_hits = sorted(policy_hits, key=lambda x: x["score"], reverse=True)[:top_k]
            issue["policy_support"] = policy_hits

            # Summarize via LLM
            issue["policy_summary"] = self._summarize_with_llm(issue)

            enriched_issues.append(issue)

        return {"claim_id": claim_id, "enriched_issues": enriched_issues}

    # ----------------------------------------------------
    # HYBRID SEARCH
    # ----------------------------------------------------
    def _hybrid_search(self, collection: str, query_text: str, query_vector: List[float], top_k: int):
        """Hybrid search: combines vector + keyword + code fields, with fallback to pure vector."""
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
    # LLM SUMMARIZATION
    # ----------------------------------------------------
    def _summarize_with_llm(self, issue: Dict[str, Any]) -> Dict[str, Any]:
        """Ask the LLM to filter and summarize CMS policy snippets consistently with the claim."""
        if not issue.get("policy_support"):
            return {"summary": "No relevant CMS policies found.", "relevant_policies": []}

        # Build context and claim info
        context_blocks = [
            f"[{p['collection']} - Section {p.get('section')}]: {p['excerpt']}"
            for p in issue["policy_support"]
        ]
        context = "\n\n".join(context_blocks)

        claim_info = f"""
CPT/HCPCS: {issue.get('hcpcs_code')}
ICD-10: {issue.get('icd10_code')}
Denial Reason: {issue.get('ptp_denial_reason')}
Risk Level: {issue.get('denial_risk_level')}
"""

        # Combine the static LLM prompt with dynamic claim/policy context
        prompt = f"{LLM_PROMPT}\n\nClaim:\n{claim_info}\n\nPolicy Excerpts:\n{context}"

        if not ollama:
            print(" Ollama not configured; returning prompt only.")
            return {"summary": "LLM not configured.", "prompt": prompt}

        try:
            response = ollama.chat(model=self.llm_model, messages=[{"role": "user", "content": prompt}])
            text = response["message"]["content"]
            return json.loads(text) if text.strip().startswith("{") else {"summary": text}
        except Exception as e:
            print(f" LLM summarization failed: {e}")
            return {"summary": "LLM summarization error.", "error": str(e)}

    # ----------------------------------------------------
    # HELPERS
    # ----------------------------------------------------
    def _get_claim_issues(self, claim_id: str) -> List[Dict[str, Any]]:
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

    def _build_query_text(self, issue: Dict[str, Any]) -> str:
        dx = issue.get("icd10_code") or issue.get("icd9_code")
        proc = issue.get("hcpcs_code") or issue.get("cpt_code")
        denial = issue.get("ptp_denial_reason", "")
        risk = issue.get("denial_risk_level", "unspecified")
        return f"{proc} {dx} {denial} {risk} CMS policy NCCI MUE PTP"

    def _extract_chapter(self, text: str) -> str:
        m = re.search(r"(Chapter\s+\d+)", text, re.IGNORECASE)
        return m.group(1) if m else None

    def _extract_section(self, text: str) -> str:
        m = re.search(r"(\d{1,3}\.\d+)", text)
        return m.group(1) if m else None

    def _extract_subsection(self, text: str) -> str:
        m = re.search(r"\(([A-Z]\d{1,3})\)", text)
        return m.group(1) if m else None

    def _extract_revision(self, text: str) -> str:
        m = re.search(r"(Rev\.\s*\d+)", text, re.IGNORECASE)
        return m.group(1) if m else None


# --------------------------
# Example Usage
# --------------------------
if __name__ == "__main__":
    corrector = ClaimCorrector(llm_model="llama3")
    claim_id = "cms-claim-complex-0001"
    enriched = corrector.run_corrections(claim_id)
    print(json.dumps(enriched, indent=2))
