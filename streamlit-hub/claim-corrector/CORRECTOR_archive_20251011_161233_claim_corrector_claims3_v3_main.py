#!/usr/bin/env python3
"""
Claim Corrector (ICD/CPT Hybrid Version)
----------------------------------------
- Works with Qdrant client v1.6.x
- Retrieves claim issues from `claim_analysis_metadata`
- Searches across all `claims__` policy collections using hybrid ICD/CPT logic
- Summarizes relevant CMS policy excerpts via Ollama (local LLM)
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


# -------------------------------------------------------------------------
# PROMPT: Focused CMS Denial Reasoning Prompt (strict ICD/CPT filtering)
# -------------------------------------------------------------------------

LLM_PROMPT = """
You are a CMS Denial Reasoning Assistant that interprets Medicare claims in light of official CMS policies.

You are given:
1. A structured Medicare claim object (including CPT/HCPCS, ICD-10, denial reason, modifiers, and risk level)
2. Several CMS policy excerpts retrieved from the Qdrant policy vector database

Your task:
- Carefully analyze both the claim and each policy excerpt.
- Identify which policies are *directly relevant* to this specific claims CPT/HCPCS and ICD codes.
- Discard or downplay irrelevant policies (for unrelated codes, body systems, or services).
- For each relevant policy, produce a short summary explaining how that rule affects the claim (coverage, edit restrictions, modifier use, etc.).
- If applicable, mention any allowable corrections (e.g., valid modifiers like XU or 59, coverage documentation, or resubmission options).
- Connect the logic between the claims denial reason and the matching CMS policy rule.

Return your reasoning as structured JSON:

{
  "claim_summary": "Summarize the claim (include CPT, ICD, and denial reason).",
  "relevant_policies": [
    {
      "collection": "claims__ncci_edits",
      "section": "20.4.5",
      "rev": "Rev. 12500",
      "policy_summary": "NCCI policy prevents separate billing for CPT 74170 when imaging is bundled. Modifier XU may be used only if imaging was distinct and separately documented."
    }
  ],
  "filtered_out_policies": [
    "claims__cms_policies (Cardiac Rehab Section)",
    "claims__med_claims_policies (Ambulance Modifiers)"
  ],
  "final_reasoning_summary": "This claim (CPT 74170, ICD-10 S82201A) was denied due to NCCI bundling rules for imaging. Policy supports denial unless the service was distinct and billed with proper modifier XU or 59."
}
"""



# -------------------------------------------------------------------------
# CLAIM CORRECTOR
# -------------------------------------------------------------------------
class ClaimCorrector:
    def __init__(self, url: str = "http://localhost:6333"):
        self.client = QdrantClient(url=url)

        #  Use the same model as the embeddings in Qdrant
        self.embedder = SentenceTransformer(
            "nomic-ai/nomic-embed-text-v1.5",
            device="cuda" if torch.cuda.is_available() else "cpu",
            trust_remote_code=True
        )

        #  Load only claims-related collections
        all_collections = [c.name for c in self.client.get_collections().collections]
        self.policy_collections = [c for c in all_collections if c.startswith("claims__")]

        print(f" Loaded {len(self.policy_collections)} claims collections:")
        for c in self.policy_collections:
            print(f"   - {c}")

        #  Claims source collection
        self.claim_collection = "claim_analysis_metadata"

    # ---------------------------------------------------------------------
    # MAIN EXECUTION
    # ---------------------------------------------------------------------
    def run_corrections(self, claim_id: str, top_k: int = 5) -> Dict[str, Any]:
        issues = self._get_claim_issues(claim_id)
        enriched_issues = []

        for issue in issues:
            policy_hits = []
            for collection in self.policy_collections:
                hits = self._hybrid_search(collection, issue, top_k)
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

            #  Run local LLM summarization (Ollama)
            issue["policy_summary"] = self._summarize_with_llm(issue, policy_hits)
            enriched_issues.append(issue)

        return {"claim_id": claim_id, "enriched_issues": enriched_issues}

    # ---------------------------------------------------------------------
    # CLAIM RETRIEVAL
    # ---------------------------------------------------------------------
    def _get_claim_issues(self, claim_id: str) -> List[Dict[str, Any]]:
        """Retrieve claim issues from claim_analysis_metadata"""
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

    # ---------------------------------------------------------------------
    # HYBRID SEARCH (Qdrant v1.6.x compatible)
    # ---------------------------------------------------------------------
    def _hybrid_search(self, collection: str, issue: Dict[str, Any], top_k: int = 5):
        """Hybrid ICD + CPT search compatible with qdrant-client 1.6.x"""
        try:
            icd_code = issue.get("icd10_code") or issue.get("icd9_code")
            hcpcs_code = issue.get("hcpcs_code") or issue.get("cpt_code")
            denial_reason = issue.get("ptp_denial_reason", "unspecified")

            query_text = (
                f"CMS policy for CPT/HCPCS {hcpcs_code}, diagnosis {icd_code}, "
                f"denial reason {denial_reason}. Include NCCI, LCD, and CMS manual sections."
            )
            query_vector = self.embedder.encode(query_text).tolist()

            # Build ICD/CPT hybrid filter
            filter_obj = models.Filter(
                should=[
                    models.FieldCondition(key="text", match=models.MatchText(text=str(hcpcs_code))),
                    models.FieldCondition(key="text", match=models.MatchText(text=str(icd_code))),
                    models.FieldCondition(key="all_codes", match=models.MatchText(text=str(hcpcs_code))),
                    models.FieldCondition(key="all_codes", match=models.MatchText(text=str(icd_code))),
                    models.FieldCondition(key="cpt_codes", match=models.MatchText(text=str(hcpcs_code))),
                    models.FieldCondition(key="icd10_codes", match=models.MatchText(text=str(icd_code))),
                    models.FieldCondition(key="modifiers", match=models.MatchText(text=issue.get("modifiers", ""))),
                ]
            )

            hits = self.client.search(
                collection_name=collection,
                query_vector=query_vector,
                query_filter=filter_obj,
                limit=top_k,
                with_payload=True,
                with_vectors=False,
            )

            return hits or []
        except Exception as e:
            print(f" Search failed for {collection}: {e}")
            return []

    # ---------------------------------------------------------------------
    # LLM SUMMARIZATION
    # ---------------------------------------------------------------------
    def _summarize_with_llm(self, issue: Dict[str, Any], policies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run local LLM summarization with Ollama (Mistral)"""
        try:
            input_data = {"claim": issue, "policies": policies}
            prompt_input = f"{LLM_PROMPT}\n\n{json.dumps(input_data, indent=2)}"
            cmd = ["ollama", "run", "mistral", prompt_input]
            result = subprocess.run(cmd, capture_output=True, text=True)
            return {"summary": result.stdout.strip() or "No output from LLM"}
        except Exception as e:
            print(f" LLM summarization failed: {e}")
            return {"summary": "LLM summarization unavailable"}

    # ---------------------------------------------------------------------
    # HELPERS
    # ---------------------------------------------------------------------
    def _extract_chapter(self, text: str) -> str:
        m = re.search(r"(Chapter\s+\d+)", text, re.IGNORECASE)
        return m.group(1) if m else None

    def _extract_section(self, text: str) -> str:
        m = re.search(r"(\d{1,3}\.\d+)", text)
        return m.group(1) if m else None

    def _extract_revision(self, text: str) -> str:
        m = re.search(r"(Rev\.\s*\d+)", text, re.IGNORECASE)
        return m.group(1) if m else None


# ---------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------
if __name__ == "__main__":
    corrector = ClaimCorrector()
    claim_id = "123456789012345"
    enriched = corrector.run_corrections(claim_id)
    print(json.dumps(enriched, indent=2))
