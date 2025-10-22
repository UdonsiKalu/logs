#!/usr/bin/env python3
"""
Claim Corrector (Full Fidelity + Hybrid Qdrant Version)
- Dynamically detects all policy collections in Qdrant.
- Uses hybrid (vector + keyword) search.
- Preserves full metadata schema from original claim_corrector.py.
"""

import os
import re
import json
import torch
from typing import Dict, Any, List
from qdrant_client import QdrantClient
from qdrant_client.http import models
from sentence_transformers import SentenceTransformer


class ClaimCorrector:
    def __init__(self, url: str = "http://localhost:6333", api_key: str = None):
        self.client = QdrantClient(url=url, api_key=api_key)

        # Match embedding model used in cms_policies_qdrant.py
        self.embedder = SentenceTransformer(
            "nomic-ai/nomic-embed-text-v1.5",
            device="cuda" if torch.cuda.is_available() else "cpu",
            trust_remote_code=True
        )

        # Dynamically detect all available collections
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

        # Claims metadata collection
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

            # Loop through all detected policy collections
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

            # Sort across all policy collections
            policy_hits = sorted(policy_hits, key=lambda x: x["score"], reverse=True)[:top_k]

            issue["policy_support"] = policy_hits
            enriched_issues.append(issue)

        return {"claim_id": claim_id, "enriched_issues": enriched_issues}

    # ----------------------------------------------------
    # HYBRID SEARCH
    # ----------------------------------------------------
    def _hybrid_search(self, collection: str, query_text: str, query_vector: List[float], top_k: int):
        """Hybrid search: combines vector + keyword + code fields, with fallback to pure vector."""
        try:
            # Attempt hybrid filter search
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

            # Fallback: if hybrid returns nothing, run pure vector search
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
    # HELPERS
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

    def _build_query_text(self, issue: Dict[str, Any]) -> str:
        dx = issue.get("icd10_code") or issue.get("icd9_code")
        proc = issue.get("hcpcs_code") or issue.get("cpt_code")
        risk = issue.get("denial_risk_level", "unspecified")
        return (
            f"CMS policy guidance for CPT/HCPCS {proc} and diagnosis {dx}. "
            f"Denial risk: {risk}. Include related Medicare manual sections and LCD/NCD rules."
        )

    # ----------------------------------------------------
    # REGEX FALLBACKS
    # ----------------------------------------------------
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
    corrector = ClaimCorrector()
    claim_id = "cms-claim-complex-0001"
    enriched = corrector.run_corrections(claim_id)
    print(json.dumps(enriched, indent=2))
