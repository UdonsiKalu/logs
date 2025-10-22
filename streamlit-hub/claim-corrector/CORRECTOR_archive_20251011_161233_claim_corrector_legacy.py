#!/usr/bin/env python3
"""
Claim Corrector Module (Enhanced with Regex Fallback)
- Pulls risky DXPROC combos from claim_analysis_metadata (structured store)
- Queries policy RAG collections (cms_policies, lcd_policies, etc.)
- Attaches retrieved evidence (with regex metadata fallback) to each issue
"""

import re
import json
from typing import Dict, Any, List
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchText
from sentence_transformers import SentenceTransformer


class ClaimCorrector:
    def __init__(self, host: str = "localhost", port: int = 6333):
        self.client = QdrantClient(host=host, port=port)

        # 768-dim embedder (must match your RAG collections)
        self.embedder = SentenceTransformer(
            "nomic-ai/nomic-embed-text-v1.5", trust_remote_code=True
        )

        # Structured claim metadata collection
        self.claim_collection = "claim_analysis_metadata"

        # Unstructured policy collections (768-dim only, with regex metadata)
        self.policy_collections = [
            "cms_policies",
            "lcd_policies",
            "medicare_managed_care_policies",
            "Ncci_procedure_to_procedure_edits",
            "Physician_fee_schedule_policy",
            " Merit_based_incentive_payment_system_policy"
        ]

    def run_corrections(self, claim_id: str, top_k: int = 3) -> Dict[str, Any]:
        issues = self._get_claim_issues(claim_id)
        enriched_issues = []

        for issue in issues:
            query_text = self._build_query_text(issue)
            query_vector = self.embedder.encode(query_text).tolist()

            policy_hits = []
            for collection in self.policy_collections:
                try:
                    hits = self.client.search(
                        collection_name=collection,
                        query_vector=query_vector,
                        limit=top_k
                    )
                    for h in hits:
                        payload = h.payload or {}
                        excerpt = payload.get("text", "")

                        # Use stored metadata OR regex fallback
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

            # Sort across all policy collections
            policy_hits = sorted(policy_hits, key=lambda x: x["score"], reverse=True)[:top_k]

            issue["policy_support"] = policy_hits
            enriched_issues.append(issue)

        return {"enriched_issues": enriched_issues}

    # ----------------------
    # Regex Fallbacks
    # ----------------------
    def _extract_chapter(self, text: str) -> str:
        m = re.search(r"(Chapter\s+\d+)", text, re.IGNORECASE)
        return m.group(1) if m else None

    def _extract_section(self, text: str) -> str:
        m = re.search(r"(\d{1,3}\.\d+)", text)  # e.g., "260.2"
        return m.group(1) if m else None

    def _extract_subsection(self, text: str) -> str:
        m = re.search(r"\(([A-Z]\d{1,3})\)", text)  # e.g., "(A3-3661)"
        return m.group(1) if m else None

    def _extract_revision(self, text: str) -> str:
        m = re.search(r"(Rev\.\s*\d+)", text, re.IGNORECASE)
        return m.group(1) if m else None

    # ----------------------
    # Helpers
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
        return (
            f"Policy guidance for CPT {proc} with diagnosis {dx}. "
            f"Issue identified: {risk}. Include relevant Medicare manual sections."
        )


# --------------------------
# Example Usage
# --------------------------
if __name__ == "__main__":
    corrector = ClaimCorrector()
    claim_id = "cms-claim-complex-0001"
    enriched = corrector.run_corrections(claim_id)
    print(json.dumps(enriched, indent=2))
