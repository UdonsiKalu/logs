#!/usr/bin/env python3
"""
Claim Corrector Module (Dual-Mode: Denial vs Quality)
- Primary mode: Denial prevention (NCCI, CMS, LCD, PFS, Claims Manual)
- Secondary mode: Quality/Contextual (MIPS, Managed Care)
- Pulls risky DXPROC combos from claim_analysis_metadata
- Queries chosen policy group
- Attaches evidence + contextual synthesis
"""

import re
import json
from typing import Dict, Any, List
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchText
from sentence_transformers import SentenceTransformer
import ollama


# ----------------------
# CLAIM CORRECTOR CLASS
# ----------------------
class ClaimCorrector:
    def __init__(self, host: str = "localhost", port: int = 6333, ollama_model: str = "llama3:8b-instruct-q4_0"):
        self.client = QdrantClient(host=host, port=port)

        self.embedder = SentenceTransformer(
            "nomic-ai/nomic-embed-text-v1.5", trust_remote_code=True
        )
        self.ollama_model = ollama_model

        # Structured claims DB
        self.claim_collection = "claim_analysis_metadata"

        # ----------------------
        # Policy Groups
        # ----------------------
        self.denial_collections = [
            "claim_analysis_metadata",
            "Ncci_procedure_to_procedure_edits",
            "cms_policies",
            "lcd_policies",
            "Physician_fee_schedule_policy",
            "med_claims_policies"
        ]

        self.quality_collections = [
            "Merit_based_incentive_payment_system_policy",
            "medicare_managed_care_policies"
        ]

    def run_corrections(self, claim_id: str, mode: str = "denial", top_k: int = 3) -> Dict[str, Any]:
        """
        mode = "denial" (default) -> NCCI, CMS, LCD, PFS
        mode = "quality" -> MIPS, Managed Care
        """
        collections = self.denial_collections if mode == "denial" else self.quality_collections

        # ---- Step 1: Pull risky combos ----
        issues = self._get_claim_issues(claim_id)
        enriched_issues = []

        for issue in issues:
            query_text = self._build_query_text(issue)
            query_vector = self.embedder.encode(query_text).tolist()

            policy_hits = []
            for collection in collections:
                try:
                    hits = self.client.search(
                        collection_name=collection,
                        query_vector=query_vector,
                        limit=top_k
                    )
                    for h in hits:
                        payload = h.payload or {}
                        excerpt = payload.get("text", "")

                        # Regex fallback if metadata missing
                        chapter = payload.get("chapter") or self._extract_chapter(excerpt)
                        section = payload.get("section") or self._extract_section(excerpt)
                        subsection = payload.get("subsection") or self._extract_subsection(excerpt)
                        rev = payload.get("rev") or self._extract_revision(excerpt)

                        policy_hits.append({
                            "collection": collection,
                            "score": h.score,
                            "policy_id": payload.get("policy_id", "unknown"),
                            "chapter": chapter or "unknown",
                            "section": section or "unknown",
                            "subsection": subsection or "unknown",
                            "rev": rev or "unknown",
                            "excerpt": excerpt or "No excerpt",
                            "url": payload.get("source_url"),
                            "page": payload.get("page", "n/a"),
                            "source": payload.get("source", "n/a")
                        })
                except Exception as e:
                    print(f" Skipping {collection}: {e}")

            # Keep only top_k across all collections
            policy_hits = sorted(policy_hits, key=lambda x: x["score"], reverse=True)[:top_k]

            issue["policy_support"] = policy_hits
            enriched_issues.append(issue)

        # ---- Step 2: Contextualize with LLM ----
        return self._summarize_with_llm(enriched_issues, mode)

    # ----------------------
    # Regex helpers
    # ----------------------
    def _extract_chapter(self, text: str): 
        m = re.search(r"(Chapter\s+\d+)", text, re.IGNORECASE)
        return m.group(1) if m else None

    def _extract_section(self, text: str):
        m = re.search(r"(\d{1,3}\.\d+)", text)
        return m.group(1) if m else None

    def _extract_subsection(self, text: str):
        m = re.search(r"\(([A-Z]\d{1,3})\)", text)
        return m.group(1) if m else None

    def _extract_revision(self, text: str):
        m = re.search(r"(Rev\.\s*\d+)", text, re.IGNORECASE)
        return m.group(1) if m else None

    # ----------------------
    # Qdrant + LLM
    # ----------------------
    def _get_claim_issues(self, claim_id: str) -> List[Dict[str, Any]]:
        try:
            hits = self.client.scroll(
                collection_name=self.claim_collection,
                scroll_filter=Filter(must=[
                    FieldCondition(key="claim_id", match=MatchText(text=claim_id))
                ]),
                limit=100
            )
            return [point.payload for point in hits[0]]
        except Exception as e:
            print(f" Failed to pull issues: {e}")
            return []

    def _build_query_text(self, issue: Dict[str, Any]) -> str:
        dx = issue.get("icd10_code") or issue.get("icd9_code")
        proc = issue.get("hcpcs_code")
        risk = issue.get("denial_risk_level")
        return f"Policy guidance for CPT {proc} with diagnosis {dx}. Risk: {risk}."

    def _summarize_with_llm(self, enriched_issues: List[Dict[str, Any]], mode: str) -> Dict[str, Any]:
        try:
            context = json.dumps(enriched_issues, indent=2)
            response = ollama.chat(
                model=self.ollama_model,
                messages=[
                    {"role": "system", "content": f"You are analyzing Medicare claim issues in {mode.upper()} mode."},
                    {"role": "user", "content": f"Here are the issues and policy evidence:\n{context}\n\nSummarize the relevance of these excerpts in plain English (max 200 words)."}
                ]
            )
            return {"enriched_issues": enriched_issues, "llm_contextual_summary": response["message"]["content"]}
        except Exception as e:
            print(f" LLM summarization failed: {e}")
            return {"enriched_issues": enriched_issues}


# --------------------------
# Example Usage
# --------------------------
if __name__ == "__main__":
    corrector = ClaimCorrector()
    claim_id = "cms-claim-complex-0001"

    # Run in denial mode
    denial_out = corrector.run_corrections(claim_id, mode="denial")
    print(json.dumps(denial_out, indent=2))

    # Run in quality mode
    quality_out = corrector.run_corrections(claim_id, mode="quality")
    print(json.dumps(quality_out, indent=2))
