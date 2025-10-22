#!/usr/bin/env python3
"""
Two-Stage CMS Claim Corrector with Calibrated Stage 1
Stage 1: Uses calibrated denial reasoning (from claim_corrector_claims3_calibrated.py)
Stage 2: Corrective Reasoning Pass - Find how to fix the claim
"""

import json
import re
import subprocess
from typing import Dict, Any, List, Tuple, Optional
from sentence_transformers import SentenceTransformer
import torch
from qdrant_client import QdrantClient
from qdrant_client import models

# -------------------------------------------------------------------------
# STATIC CPT MAPPING (No Database Dependency)
# -------------------------------------------------------------------------

def get_cpt_description(cpt_code: str) -> str:
    """Get CPT description from static mapping (database-free approach)"""
    cpt_mappings = {
        "74170": "CT abdomen and pelvis with contrast",
        "99213": "Office visit, established patient, 20-29 minutes",
        "99214": "Office visit, established patient, 30-39 minutes",
        "99215": "Office visit, established patient, 40-54 minutes",
        "99201": "Office visit, new patient, 10 minutes",
        "99202": "Office visit, new patient, 20 minutes",
        "99203": "Office visit, new patient, 30 minutes",
        "99204": "Office visit, new patient, 45 minutes",
        "99205": "Office visit, new patient, 60 minutes",
        "74176": "CT abdomen with contrast",
        "74177": "CT pelvis with contrast",
        "74178": "CT abdomen and pelvis without contrast"
    }
    
    return cpt_mappings.get(cpt_code, f"Medical procedure {cpt_code}")

# -------------------------------------------------------------------------
# STAGE 1: CALIBRATED DENIAL REASONING PROMPT (from calibrated version)
# -------------------------------------------------------------------------

CALIBRATED_STAGE1_PROMPT = """
You are a CMS Policy Reasoning Assistant specializing in Medicare claim denial analysis.

CRITICAL VALIDATION RULES - READ CAREFULLY:
1. Use ONLY the exact claim data provided below - DO NOT infer patient conditions not present
2. Do NOT hallucinate medical conditions (ESRD, diabetes, etc.) unless explicitly mentioned in claim
3. Base ALL reasoning ONLY on retrieved policy text and provided ICD/CPT codes
4. If policy excerpt doesn't mention the specific CPT/ICD codes, mark as LOW relevance
5. Identify policy sources accurately based on file paths provided

EXACT CLAIM DATA (DO NOT MODIFY OR INFER):
- CPT/HCPCS: {hcpcs_code} ({procedure_name})
- ICD-10: {icd10_code} ({diagnosis_name})
- Denial Reason: {denial_reason}
- Risk Level: {denial_risk_level}
- Action Required: {action_required}

RETRIEVED POLICIES WITH RELEVANCE VALIDATION:
{policy_excerpts}

MANUAL TYPE RESTRICTIONS:
- pim*.pdf (Program Integrity Manual): ONLY for administrative/fraud issues
- clm104*.pdf (Claims Processing Manual): For coding conflicts and procedure definitions
- ncci*.pdf (NCCI): For bundling conflicts and PTP edits
- lcd*.pdf (LCD): For coverage determinations and local policies

REQUIRED OUTPUT FORMAT (valid JSON only):
{{
  "claim_summary": "Brief description using EXACT claim data - NO inferred conditions",
  "relevant_policies": [
    {{
      "collection": "Exact manual name based on source file",
      "chapter": "Chapter from source data",
      "section": "Section from source data", 
      "rev": "Revision from source data",
      "source_file": "exact filename (e.g., clm104c12.pdf)",
      "page": "page number",
      "policy_summary": "HOW this specific policy explains the denial - MUST mention CPT/ICD codes from claim",
      "relevance_score": "HIGH/MEDIUM/LOW based on CPT/ICD code mention in policy",
      "retrieval_confidence": "score from search results",
      "validation_status": "PASS/FAIL - whether policy mentions claim CPT/ICD codes"
    }}
  ],
  "filtered_out_policies": [
    "List policies that don't mention CPT/ICD codes or are wrong manual type"
  ],
  "final_reasoning_summary": "Complete explanation using EXACT claim data. NO inferred medical conditions. Include specific policy citations.",
  "data_consistency_check": "Confirm: Used exact CPT/ICD descriptions without inferring patient conditions",
  "validation_summary": "Summary of policy relevance validation results",
  "denial_keywords": ["keyword1", "keyword2", "keyword3"]
}}

STRICT VALIDATION REQUIREMENTS:
- Only include policies that directly mention the claim's CPT/ICD codes
- Reject policies from wrong manual types (e.g., pim* for clinical coding issues)
- Do NOT infer patient medical conditions not explicitly stated
- Use EXACT CPT/ICD descriptions provided, not generic terms
"""

# -------------------------------------------------------------------------
# STAGE 2: CORRECTIVE REASONING PROMPT
# -------------------------------------------------------------------------

STAGE2_CORRECTION_PROMPT = """
You are a CMS Correction Assistant specializing in Medicare claim remediation.

CRITICAL INSTRUCTIONS:
1. Based on the calibrated denial reasoning from Stage 1, find HOW to fix the claim
2. Look for modifier exceptions, covered ICD variants, allowable combinations
3. Provide specific, actionable corrections with policy references
4. Focus on making the claim compliant

ORIGINAL CLAIM DATA:
- CPT/HCPCS: {hcpcs_code} ({procedure_name})
- ICD-10: {icd10_code} ({diagnosis_name})
- Denial Reason: {denial_reason}

STAGE 1 CALIBRATED DENIAL ANALYSIS:
{denial_analysis}

CORRECTION-FOCUSED POLICIES:
{correction_policies}

REQUIRED OUTPUT FORMAT (valid JSON only):
{{
  "claim_id": "{claim_id}",
  "recommended_corrections": [
    {{
      "field": "modifier|diagnosis_code|procedure_code|documentation",
      "suggestion": "Specific actionable correction with policy reference",
      "confidence": 0.85,
      "policy_reference": "Manual Chapter Section",
      "implementation_guidance": "Step-by-step instructions"
    }}
  ],
  "policy_references": [
    "Medicare Claims Processing Manual Ch.12 20.4.5",
    "LCD L34696  Fracture and Bone Imaging Coverage"
  ],
  "final_guidance": "Summary of how this claim can be fixed to pass",
  "compliance_checklist": [
    "Action item 1",
    "Action item 2"
  ]
}}

FOCUS: Find HOW to fix the claim to make it compliant with CMS policy.
"""

# -------------------------------------------------------------------------
# TWO-STAGE CLAIM CORRECTOR WITH CALIBRATED STAGE 1
# -------------------------------------------------------------------------

class TwoStageCalibratedClaimCorrector:
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
        self.claims_collection = "claim_analysis_metadata"
        
        #  Source mapping for policy identification - EXACT SAME AS STANDALONE
        self.source_mapping = {
            "clm104c": "Medicare Claims Processing Manual",
            "pim83c": "Program Integrity Manual (Administrative Only)", 
            "ncci": "National Correct Coding Initiative",
            "lcd": "Local Coverage Determination",
            "mcm": "Medicare Claims Manual",
            "bpm": "Medicare Benefit Policy Manual"
        }

    def run_two_stage_corrections(self, claim_id: str) -> Dict[str, Any]:
        """Run both calibrated denial reasoning and corrective reasoning stages"""
        print(f"\n TWO-STAGE CALIBRATED PROCESSING: {claim_id}")
        
        # Get claim issues
        issues = self._get_claim_issues(claim_id)
        if not issues:
            return {"claim_id": claim_id, "enriched_issues": []}

        enriched_issues = []
        for issue in issues:
            print(f"\n Processing issue: {issue.get('hcpcs_code', 'N/A')} + {issue.get('icd10_code', 'N/A')}")
            
            #  Get CPT description
            cpt_code = issue.get('hcpcs_code', '')
            if cpt_code:
                dynamic_procedure_name = get_cpt_description(cpt_code)
                issue['procedure_name'] = dynamic_procedure_name
                print(f"    Updated procedure name: {dynamic_procedure_name}")
            
            #  STAGE 1: Calibrated Denial Reasoning Pass
            print(f"    STAGE 1: Calibrated denial reasoning analysis...")
            stage1_result = self._stage1_calibrated_denial_reasoning(issue)
            
            #  STAGE 2: Corrective Reasoning Pass
            print(f"    STAGE 2: Finding corrective solutions...")
            stage2_result = self._stage2_corrective_reasoning(issue, stage1_result)
            
            # Combine results
            enriched_issue = {
                **issue,
                "stage1_calibrated_denial_analysis": stage1_result,
                "stage2_correction_analysis": stage2_result,
                "two_stage_calibrated_complete": True
            }
            enriched_issues.append(enriched_issue)

        return {"claim_id": claim_id, "enriched_issues": enriched_issues}

    # ---------------------------------------------------------------------
    # STAGE 1: CALIBRATED DENIAL REASONING (from calibrated version)
    # ---------------------------------------------------------------------
    def _stage1_calibrated_denial_reasoning(self, issue: Dict[str, Any]) -> Dict[str, Any]:
        """Stage 1: Calibrated denial reasoning using enhanced validation"""
        # Get policies for denial reasoning
        all_policies = []
        for collection in self.policy_collections:
            policies = self._hybrid_search(collection, issue, top_k=3)
            all_policies.extend(policies)
        
        # Validate and deduplicate policies using calibrated logic
        validated_policies = self._calibrated_validate_and_deduplicate_policies(all_policies, issue)
        print(f"    Stage 1: Retrieved {len(validated_policies)} calibrated policies for denial analysis")
        
        # Run Stage 1 calibrated LLM analysis
        stage1_analysis = self._run_calibrated_stage1_llm(issue, validated_policies)
        
        return {
            "policies_analyzed": validated_policies,
            "denial_analysis": stage1_analysis,
            "stage": "calibrated_denial_reasoning"
        }

    # ---------------------------------------------------------------------
    # STAGE 2: CORRECTIVE REASONING
    # ---------------------------------------------------------------------
    def _stage2_corrective_reasoning(self, issue: Dict[str, Any], stage1_result: Dict[str, Any]) -> Dict[str, Any]:
        """Stage 2: Find how to fix the claim"""
        # Extract keywords from Stage 1 calibrated denial analysis
        denial_keywords = self._extract_correction_keywords(stage1_result, issue)
        
        # Re-query Qdrant with correction-focused intent
        correction_policies = self._search_correction_policies(denial_keywords, issue)
        print(f"    Stage 2: Retrieved {len(correction_policies)} policies for correction analysis")
        
        # Run Stage 2 LLM analysis
        stage2_analysis = self._run_stage2_llm(issue, stage1_result, correction_policies)
        
        return {
            "correction_policies": correction_policies,
            "correction_analysis": stage2_analysis,
            "stage": "corrective_reasoning"
        }

    # ---------------------------------------------------------------------
    # CALIBRATED POLICY VALIDATION (from calibrated version)
    # ---------------------------------------------------------------------
    def _calibrated_validate_and_deduplicate_policies(self, policies: List[Any], issue: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Calibrated policy validation with enhanced relevance checks - EXACT SAME AS STANDALONE VERSION"""
        if not policies:
            return []
        
        # Convert ScoredPoint objects to dictionaries
        policy_dicts = []
        for policy in policies:
            if hasattr(policy, 'payload'):
                policy_dict = policy.payload.copy()
                policy_dict['score'] = policy.score
                policy_dicts.append(policy_dict)
            elif isinstance(policy, dict):
                policy_dicts.append(policy)
        
        # Validate policy relevance using calibrated logic - EXACT SAME AS STANDALONE
        validated_policies = []
        for policy in policy_dicts:
            validation_result = self._validate_policy_relevance(policy, issue)
            if validation_result.get('is_relevant', False):
                # Add validation metadata
                policy.update({
                    'validation_status': validation_result.get('validation_status', 'UNKNOWN'),
                    'relevance_reason': validation_result.get('relevance_reason', ''),
                    'manual_appropriate': validation_result.get('manual_appropriate', True)
                })
                validated_policies.append(policy)
        
        # Deduplicate
        return self._deduplicate_policies(validated_policies)

    def _validate_policy_relevance(self, policy: Dict[str, Any], issue: Dict[str, Any]) -> Dict[str, Any]:
        """Validate if policy is relevant to the claim with more flexible criteria - EXACT COPY FROM STANDALONE"""
        cpt_code = issue.get('hcpcs_code', '')
        icd_code = issue.get('icd10_code', '')
        denial_reason = issue.get('ptp_denial_reason', '')
        source_file = policy.get('source', '')
        
        #  Check if policy mentions the CPT/ICD codes (use 'text' field, not 'excerpt')
        policy_text = policy.get('text', '').lower()
        mentions_cpt = cpt_code.lower() in policy_text if cpt_code else False
        mentions_icd = icd_code.lower() in policy_text if icd_code else False
        
        #  Check for broader relevance indicators - more flexible
        relevance_keywords = []
        if 'ptp' in denial_reason.lower():
            relevance_keywords.extend(['ptp', 'procedure', 'bundling', 'ncci', 'edit', 'coding'])
        if 'coding' in denial_reason.lower():
            relevance_keywords.extend(['coding', 'cpt', 'hcpcs', 'procedure', 'medical'])
        if 'coverage' in denial_reason.lower():
            relevance_keywords.extend(['coverage', 'lcd', 'determination', 'medical'])
        if 'definition' in denial_reason.lower():
            relevance_keywords.extend(['definition', 'coding', 'procedure', 'medical'])
        
        mentions_relevance_keywords = any(keyword in policy_text for keyword in relevance_keywords)
        
        #  Check manual type appropriateness
        manual_appropriate = self._check_manual_appropriateness(source_file, denial_reason)
        
        #  More flexible relevance - allow general medical policies
        general_medical_keywords = ['medical', 'procedure', 'service', 'coding', 'billing', 'claim']
        mentions_general = any(keyword in policy_text for keyword in general_medical_keywords)
        
        #  More flexible relevance determination
        if mentions_cpt or mentions_icd:
            return {
                'is_relevant': True,
                'validation_status': 'PASS',
                'relevance_reason': f'Policy mentions CPT/ICD codes directly',
                'manual_appropriate': manual_appropriate
            }
        elif mentions_relevance_keywords:
            return {
                'is_relevant': True,
                'validation_status': 'PASS',
                'relevance_reason': f'Policy mentions relevant keywords for {denial_reason}',
                'manual_appropriate': manual_appropriate
            }
        elif mentions_general and len(policy_text) > 200:  # Substantial content
            return {
                'is_relevant': True,
                'validation_status': 'PASS',
                'relevance_reason': f'Policy contains general medical content relevant to claims processing',
                'manual_appropriate': manual_appropriate
            }
        else:
            return {
                'is_relevant': False,
                'validation_status': 'FAIL',
                'relevance_reason': f'Policy does not contain relevant medical or coding content',
                'manual_appropriate': manual_appropriate
            }

    def _check_manual_appropriateness(self, source_file: str, denial_reason: str) -> bool:
        """Check if manual type is appropriate for the denial reason - EXACT COPY FROM STANDALONE"""
        if not source_file or not denial_reason:
            return True  # Default to allow if unclear
        
        source_lower = source_file.lower()
        denial_lower = denial_reason.lower()
        
        #  Restrict pim* to administrative issues only
        if source_lower.startswith('pim'):
            return 'administrative' in denial_lower or 'integrity' in denial_lower
        
        #  Prefer clm104* for coding conflicts
        if source_lower.startswith('clm104'):
            return any(keyword in denial_lower for keyword in ['coding', 'procedure', 'definition', 'ptp', 'conflict'])
        
        #  Prefer ncci* for bundling/PTP issues
        if source_lower.startswith('ncci'):
            return any(keyword in denial_lower for keyword in ['ptp', 'bundling', 'conflict', 'ncci'])
        
        #  Allow lcd* for coverage issues
        if source_lower.startswith('lcd'):
            return any(keyword in denial_lower for keyword in ['coverage', 'determination', 'local'])
        
        return True  # Default to allow other manuals

    def _identify_policy_source(self, source_file: str) -> str:
        """Identify policy manual based on source file name - EXACT COPY FROM STANDALONE"""
        if not source_file:
            return "Unknown Source"
        
        source_lower = source_file.lower()
        for prefix, manual_name in self.source_mapping.items():
            if source_lower.startswith(prefix):
                return manual_name
        
        return f"Policy Manual ({source_file})"

    # ---------------------------------------------------------------------
    # KEYWORD EXTRACTION FOR STAGE 2
    # ---------------------------------------------------------------------
    def _extract_correction_keywords(self, stage1_result: Dict[str, Any], issue: Dict[str, Any]) -> List[str]:
        """Extract keywords for correction-focused search from calibrated Stage 1"""
        denial_analysis = stage1_result.get("denial_analysis", {})
        final_summary = denial_analysis.get("final_reasoning_summary", "")
        denial_reason = issue.get("ptp_denial_reason", "")
        
        # Base keywords from denial reason
        correction_keywords = []
        
        if "ptp" in denial_reason.lower():
            correction_keywords.extend(["modifier", "XU", "59", "separately reportable", "bundling exception"])
        if "mue" in denial_reason.lower():
            correction_keywords.extend(["MUE", "unit limit", "threshold exception", "maximum units"])
        if "coverage" in denial_reason.lower():
            correction_keywords.extend(["covered diagnosis", "covered group", "ICD10", "LCD", "covered when"])
        if "definition" in denial_reason.lower():
            correction_keywords.extend(["procedure definition", "coding guidance", "documentation requirements"])
        
        # Add keywords from final summary
        summary_keywords = re.findall(r'\b[A-Z]{2,}\b|\b\w{3,}\b', final_summary)
        correction_keywords.extend([kw.lower() for kw in summary_keywords[:5]])
        
        # Remove duplicates and limit
        return list(set(correction_keywords))[:10]

    # ---------------------------------------------------------------------
    # CORRECTION-FOCUSED POLICY SEARCH
    # ---------------------------------------------------------------------
    def _search_correction_policies(self, keywords: List[str], issue: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Search for policies focused on corrections and allowances"""
        correction_policies = []
        
        # Create correction-focused query
        cpt_code = issue.get('hcpcs_code', '')
        icd_code = issue.get('icd10_code', '')
        keyword_str = " ".join(keywords)
        
        query_text = (
            f"CMS policy corrections allowances exceptions for CPT {cpt_code} ICD {icd_code} "
            f"modifier exceptions covered variants allowable combinations {keyword_str}"
        )
        
        query_vector = self.embedder.encode(query_text).tolist()
        
        # Search all collections for correction-focused policies
        for collection in self.policy_collections:
            try:
                hits = self.client.query_points(
                    collection_name=collection,
                    query=query_vector,
                    limit=3,
                    with_payload=True,
                    with_vectors=False,
                ).points
                
                # Convert to dictionaries
                for hit in hits:
                    policy_dict = hit.payload.copy()
                    policy_dict['score'] = hit.score
                    policy_dict['collection'] = collection
                    correction_policies.append(policy_dict)
                    
            except Exception as e:
                print(f" Correction search failed for {collection}: {e}")
        
        # Sort by score and deduplicate
        correction_policies.sort(key=lambda x: x.get('score', 0), reverse=True)
        return self._deduplicate_policies(correction_policies)[:8]

    # ---------------------------------------------------------------------
    # LLM PROCESSING FOR BOTH STAGES
    # ---------------------------------------------------------------------
    def _run_calibrated_stage1_llm(self, issue: Dict[str, Any], policies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run Stage 1 calibrated LLM for denial reasoning - EXACT SAME AS STANDALONE VERSION"""
        try:
            # Format policies with enhanced validation information - EXACT SAME AS STANDALONE
            policy_excerpts = ""
            for i, policy in enumerate(policies, 1):
                source_file = policy.get('source', 'unknown.pdf')
                manual_name = self._identify_policy_source(source_file)
                validation_status = policy.get('validation_status', 'UNKNOWN')
                
                policy_excerpts += f"\nPOLICY {i}:\n"
                policy_excerpts += f"Source File: {source_file}\n"
                policy_excerpts += f"Manual: {manual_name}\n"
                policy_excerpts += f"Validation: {validation_status}\n"
                policy_excerpts += f"Chapter: {policy.get('chapter', 'N/A')}\n"
                policy_excerpts += f"Section: {policy.get('section', 'N/A')}\n"
                policy_excerpts += f"Revision: {policy.get('rev', 'N/A')}\n"
                policy_excerpts += f"Page: {policy.get('page', 'N/A')}\n"
                policy_excerpts += f"Retrieval Score: {policy.get('score', 0.0):.4f}\n"
                policy_excerpts += f"Text: {policy.get('text', '')[:500]}...\n"
            
            # Format the calibrated prompt with exact claim data - EXACT SAME AS STANDALONE
            prompt = CALIBRATED_STAGE1_PROMPT.format(
                hcpcs_code=issue.get('hcpcs_code', 'N/A'),
                procedure_name=issue.get('procedure_name', 'N/A'),
                icd10_code=issue.get('icd10_code', 'N/A'),
                diagnosis_name=issue.get('diagnosis_name', 'N/A'),
                denial_reason=issue.get('ptp_denial_reason', 'N/A'),
                denial_risk_level=issue.get('denial_risk_level', 'N/A'),
                action_required=issue.get('action_required', 'N/A'),
                policy_excerpts=policy_excerpts
            )
            
            # Run LLM
            cmd = ["ollama", "run", "mistral", prompt]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                return {"error": f"Calibrated Stage 1 LLM failed: {result.stderr}"}
            
            # Parse JSON response
            llm_output = result.stdout.strip()
            try:
                json_start = llm_output.find('{')
                json_end = llm_output.rfind('}') + 1
                
                if json_start >= 0 and json_end > json_start:
                    json_str = llm_output[json_start:json_end]
                    return json.loads(json_str)
                else:
                    return {"summary": llm_output, "error": "No valid JSON found"}
                    
            except json.JSONDecodeError as e:
                return {"summary": llm_output, "error": f"JSON parsing failed: {e}"}
                
        except Exception as e:
            print(f" Calibrated Stage 1 LLM failed: {e}")
            return {"error": f"Calibrated Stage 1 processing failed: {e}"}

    def _run_stage2_llm(self, issue: Dict[str, Any], stage1_result: Dict[str, Any], correction_policies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run Stage 2 LLM for corrective reasoning"""
        try:
            # Format Stage 1 calibrated analysis
            denial_analysis = stage1_result.get("denial_analysis", {})
            denial_summary = json.dumps(denial_analysis, indent=2)
            
            # Format correction policies
            correction_policies_text = ""
            for i, policy in enumerate(correction_policies, 1):
                correction_policies_text += f"\nCORRECTION POLICY {i}:\n"
                correction_policies_text += f"Source: {policy.get('source', 'N/A')}\n"
                correction_policies_text += f"Collection: {policy.get('collection', 'N/A')}\n"
                correction_policies_text += f"Text: {policy.get('text', '')[:400]}...\n"
            
            # Format Stage 2 prompt
            prompt = STAGE2_CORRECTION_PROMPT.format(
                claim_id=issue.get('claim_id', 'N/A'),
                hcpcs_code=issue.get('hcpcs_code', 'N/A'),
                procedure_name=issue.get('procedure_name', 'N/A'),
                icd10_code=issue.get('icd10_code', 'N/A'),
                diagnosis_name=issue.get('diagnosis_name', 'N/A'),
                denial_reason=issue.get('ptp_denial_reason', 'N/A'),
                denial_analysis=denial_summary,
                correction_policies=correction_policies_text
            )
            
            # Run LLM
            cmd = ["ollama", "run", "mistral", prompt]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                return {"error": f"Stage 2 LLM failed: {result.stderr}"}
            
            # Parse JSON response
            llm_output = result.stdout.strip()
            try:
                json_start = llm_output.find('{')
                json_end = llm_output.rfind('}') + 1
                
                if json_start >= 0 and json_end > json_start:
                    json_str = llm_output[json_start:json_end]
                    return json.loads(json_str)
                else:
                    return {"summary": llm_output, "error": "No valid JSON found"}
                    
            except json.JSONDecodeError as e:
                return {"summary": llm_output, "error": f"JSON parsing failed: {e}"}
                
        except Exception as e:
            print(f" Stage 2 LLM failed: {e}")
            return {"error": f"Stage 2 processing failed: {e}"}

    # ---------------------------------------------------------------------
    # HELPER METHODS (from calibrated version)
    # ---------------------------------------------------------------------
    def _get_claim_issues(self, claim_id: str) -> List[Dict[str, Any]]:
        """Get claim issues from the claims collection"""
        try:
            hits = self.client.query_points(
                collection_name=self.claims_collection,
                query=[0] * 768,  # Dummy vector
                query_filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="claim_id",
                            match=models.MatchValue(value=claim_id)
                        )
                    ]
                ),
                limit=100,
                with_payload=True,
                with_vectors=False,
            ).points
            
            issues = []
            for hit in hits:
                if hit.payload:
                    issues.append(hit.payload)
            
            print(f"    Found {len(issues)} claim issues")
            return issues
            
        except Exception as e:
            print(f" Failed to get claim issues: {e}")
            return []

    def _hybrid_search(self, collection: str, issue: Dict[str, Any], top_k: int = 5):
        """Hybrid search with proper array matching"""
        try:
            icd_code = issue.get("icd10_code") or issue.get("icd9_code")
            hcpcs_code = issue.get("hcpcs_code") or issue.get("cpt_code")
            denial_reason = issue.get("ptp_denial_reason", "unspecified")

            query_text = (
                f"CMS policy for CPT/HCPCS {hcpcs_code}, diagnosis {icd_code}, "
                f"denial reason {denial_reason}. Include NCCI, LCD, and CMS manual sections."
            )
            query_vector = self.embedder.encode(query_text).tolist()

            # Try strict code matching first
            strict_filter = models.Filter(
                should=[
                    models.FieldCondition(
                        key="cpt_codes",
                        match=models.MatchAny(any=[str(hcpcs_code).upper()])
                    ) if hcpcs_code else None,
                    models.FieldCondition(
                        key="hcpcs_codes",
                        match=models.MatchAny(any=[str(hcpcs_code).upper()])
                    ) if hcpcs_code else None,
                    models.FieldCondition(
                        key="icd10_codes",
                        match=models.MatchAny(any=[str(icd_code).upper().replace(".", "")])
                    ) if icd_code else None,
                    models.FieldCondition(
                        key="text",
                        match=models.MatchText(text=str(hcpcs_code))
                    ) if hcpcs_code else None,
                    models.FieldCondition(
                        key="text",
                        match=models.MatchText(text=str(icd_code))
                    ) if icd_code else None,
                ]
            )

            strict_filter.should = [f for f in strict_filter.should if f is not None]

            hits = self.client.query_points(
                collection_name=collection,
                query=query_vector,
                query_filter=strict_filter if strict_filter.should else None,
                limit=top_k,
                with_payload=True,
                with_vectors=False,
            ).points

            # Fallback to semantic search if no strict matches
            if not hits or len(hits) == 0:
                print(f"    No strict matches for {hcpcs_code}/{icd_code}, falling back to semantic search...")
                hits = self.client.query_points(
                    collection_name=collection,
                    query=query_vector,
                    query_filter=None,
                    limit=top_k,
                    with_payload=True,
                    with_vectors=False,
                ).points

            return hits or []
        except Exception as e:
            print(f" Search failed for {collection}: {e}")
            return []

    def _deduplicate_policies(self, policies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate policies"""
        seen_excerpts = {}
        deduplicated = []
        
        for policy in policies:
            excerpt_key = policy.get('text', '')[:200]
            
            if excerpt_key not in seen_excerpts:
                seen_excerpts[excerpt_key] = policy
                deduplicated.append(policy)
        
        return deduplicated


# ---------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------
if __name__ == "__main__":
    corrector = TwoStageCalibratedClaimCorrector()
    claim_id = "123456789012345"
    enriched = corrector.run_two_stage_corrections(claim_id)
    print(json.dumps(enriched, indent=2))


Two-Stage CMS Claim Corrector with Calibrated Stage 1
Stage 1: Uses calibrated denial reasoning (from claim_corrector_claims3_calibrated.py)
Stage 2: Corrective Reasoning Pass - Find how to fix the claim
"""

import json
import re
import subprocess
from typing import Dict, Any, List, Tuple, Optional
from sentence_transformers import SentenceTransformer
import torch
from qdrant_client import QdrantClient
from qdrant_client import models

# -------------------------------------------------------------------------
# STATIC CPT MAPPING (No Database Dependency)
# -------------------------------------------------------------------------

def get_cpt_description(cpt_code: str) -> str:
    """Get CPT description from static mapping (database-free approach)"""
    cpt_mappings = {
        "74170": "CT abdomen and pelvis with contrast",
        "99213": "Office visit, established patient, 20-29 minutes",
        "99214": "Office visit, established patient, 30-39 minutes",
        "99215": "Office visit, established patient, 40-54 minutes",
        "99201": "Office visit, new patient, 10 minutes",
        "99202": "Office visit, new patient, 20 minutes",
        "99203": "Office visit, new patient, 30 minutes",
        "99204": "Office visit, new patient, 45 minutes",
        "99205": "Office visit, new patient, 60 minutes",
        "74176": "CT abdomen with contrast",
        "74177": "CT pelvis with contrast",
        "74178": "CT abdomen and pelvis without contrast"
    }
    
    return cpt_mappings.get(cpt_code, f"Medical procedure {cpt_code}")

# -------------------------------------------------------------------------
# STAGE 1: CALIBRATED DENIAL REASONING PROMPT (from calibrated version)
# -------------------------------------------------------------------------

CALIBRATED_STAGE1_PROMPT = """
You are a CMS Policy Reasoning Assistant specializing in Medicare claim denial analysis.

CRITICAL VALIDATION RULES - READ CAREFULLY:
1. Use ONLY the exact claim data provided below - DO NOT infer patient conditions not present
2. Do NOT hallucinate medical conditions (ESRD, diabetes, etc.) unless explicitly mentioned in claim
3. Base ALL reasoning ONLY on retrieved policy text and provided ICD/CPT codes
4. If policy excerpt doesn't mention the specific CPT/ICD codes, mark as LOW relevance
5. Identify policy sources accurately based on file paths provided

EXACT CLAIM DATA (DO NOT MODIFY OR INFER):
- CPT/HCPCS: {hcpcs_code} ({procedure_name})
- ICD-10: {icd10_code} ({diagnosis_name})
- Denial Reason: {denial_reason}
- Risk Level: {denial_risk_level}
- Action Required: {action_required}

RETRIEVED POLICIES WITH RELEVANCE VALIDATION:
{policy_excerpts}

MANUAL TYPE RESTRICTIONS:
- pim*.pdf (Program Integrity Manual): ONLY for administrative/fraud issues
- clm104*.pdf (Claims Processing Manual): For coding conflicts and procedure definitions
- ncci*.pdf (NCCI): For bundling conflicts and PTP edits
- lcd*.pdf (LCD): For coverage determinations and local policies

REQUIRED OUTPUT FORMAT (valid JSON only):
{{
  "claim_summary": "Brief description using EXACT claim data - NO inferred conditions",
  "relevant_policies": [
    {{
      "collection": "Exact manual name based on source file",
      "chapter": "Chapter from source data",
      "section": "Section from source data", 
      "rev": "Revision from source data",
      "source_file": "exact filename (e.g., clm104c12.pdf)",
      "page": "page number",
      "policy_summary": "HOW this specific policy explains the denial - MUST mention CPT/ICD codes from claim",
      "relevance_score": "HIGH/MEDIUM/LOW based on CPT/ICD code mention in policy",
      "retrieval_confidence": "score from search results",
      "validation_status": "PASS/FAIL - whether policy mentions claim CPT/ICD codes"
    }}
  ],
  "filtered_out_policies": [
    "List policies that don't mention CPT/ICD codes or are wrong manual type"
  ],
  "final_reasoning_summary": "Complete explanation using EXACT claim data. NO inferred medical conditions. Include specific policy citations.",
  "data_consistency_check": "Confirm: Used exact CPT/ICD descriptions without inferring patient conditions",
  "validation_summary": "Summary of policy relevance validation results",
  "denial_keywords": ["keyword1", "keyword2", "keyword3"]
}}

STRICT VALIDATION REQUIREMENTS:
- Only include policies that directly mention the claim's CPT/ICD codes
- Reject policies from wrong manual types (e.g., pim* for clinical coding issues)
- Do NOT infer patient medical conditions not explicitly stated
- Use EXACT CPT/ICD descriptions provided, not generic terms
"""

# -------------------------------------------------------------------------
# STAGE 2: CORRECTIVE REASONING PROMPT
# -------------------------------------------------------------------------

STAGE2_CORRECTION_PROMPT = """
You are a CMS Correction Assistant specializing in Medicare claim remediation.

CRITICAL INSTRUCTIONS:
1. Based on the calibrated denial reasoning from Stage 1, find HOW to fix the claim
2. Look for modifier exceptions, covered ICD variants, allowable combinations
3. Provide specific, actionable corrections with policy references
4. Focus on making the claim compliant

ORIGINAL CLAIM DATA:
- CPT/HCPCS: {hcpcs_code} ({procedure_name})
- ICD-10: {icd10_code} ({diagnosis_name})
- Denial Reason: {denial_reason}

STAGE 1 CALIBRATED DENIAL ANALYSIS:
{denial_analysis}

CORRECTION-FOCUSED POLICIES:
{correction_policies}

REQUIRED OUTPUT FORMAT (valid JSON only):
{{
  "claim_id": "{claim_id}",
  "recommended_corrections": [
    {{
      "field": "modifier|diagnosis_code|procedure_code|documentation",
      "suggestion": "Specific actionable correction with policy reference",
      "confidence": 0.85,
      "policy_reference": "Manual Chapter Section",
      "implementation_guidance": "Step-by-step instructions"
    }}
  ],
  "policy_references": [
    "Medicare Claims Processing Manual Ch.12 20.4.5",
    "LCD L34696  Fracture and Bone Imaging Coverage"
  ],
  "final_guidance": "Summary of how this claim can be fixed to pass",
  "compliance_checklist": [
    "Action item 1",
    "Action item 2"
  ]
}}

FOCUS: Find HOW to fix the claim to make it compliant with CMS policy.
"""

# -------------------------------------------------------------------------
# TWO-STAGE CLAIM CORRECTOR WITH CALIBRATED STAGE 1
# -------------------------------------------------------------------------

class TwoStageCalibratedClaimCorrector:
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
        self.claims_collection = "claim_analysis_metadata"
        
        #  Source mapping for policy identification - EXACT SAME AS STANDALONE
        self.source_mapping = {
            "clm104c": "Medicare Claims Processing Manual",
            "pim83c": "Program Integrity Manual (Administrative Only)", 
            "ncci": "National Correct Coding Initiative",
            "lcd": "Local Coverage Determination",
            "mcm": "Medicare Claims Manual",
            "bpm": "Medicare Benefit Policy Manual"
        }

    def run_two_stage_corrections(self, claim_id: str) -> Dict[str, Any]:
        """Run both calibrated denial reasoning and corrective reasoning stages"""
        print(f"\n TWO-STAGE CALIBRATED PROCESSING: {claim_id}")
        
        # Get claim issues
        issues = self._get_claim_issues(claim_id)
        if not issues:
            return {"claim_id": claim_id, "enriched_issues": []}

        enriched_issues = []
        for issue in issues:
            print(f"\n Processing issue: {issue.get('hcpcs_code', 'N/A')} + {issue.get('icd10_code', 'N/A')}")
            
            #  Get CPT description
            cpt_code = issue.get('hcpcs_code', '')
            if cpt_code:
                dynamic_procedure_name = get_cpt_description(cpt_code)
                issue['procedure_name'] = dynamic_procedure_name
                print(f"    Updated procedure name: {dynamic_procedure_name}")
            
            #  STAGE 1: Calibrated Denial Reasoning Pass
            print(f"    STAGE 1: Calibrated denial reasoning analysis...")
            stage1_result = self._stage1_calibrated_denial_reasoning(issue)
            
            #  STAGE 2: Corrective Reasoning Pass
            print(f"    STAGE 2: Finding corrective solutions...")
            stage2_result = self._stage2_corrective_reasoning(issue, stage1_result)
            
            # Combine results
            enriched_issue = {
                **issue,
                "stage1_calibrated_denial_analysis": stage1_result,
                "stage2_correction_analysis": stage2_result,
                "two_stage_calibrated_complete": True
            }
            enriched_issues.append(enriched_issue)

        return {"claim_id": claim_id, "enriched_issues": enriched_issues}

    # ---------------------------------------------------------------------
    # STAGE 1: CALIBRATED DENIAL REASONING (from calibrated version)
    # ---------------------------------------------------------------------
    def _stage1_calibrated_denial_reasoning(self, issue: Dict[str, Any]) -> Dict[str, Any]:
        """Stage 1: Calibrated denial reasoning using enhanced validation"""
        # Get policies for denial reasoning
        all_policies = []
        for collection in self.policy_collections:
            policies = self._hybrid_search(collection, issue, top_k=3)
            all_policies.extend(policies)
        
        # Validate and deduplicate policies using calibrated logic
        validated_policies = self._calibrated_validate_and_deduplicate_policies(all_policies, issue)
        print(f"    Stage 1: Retrieved {len(validated_policies)} calibrated policies for denial analysis")
        
        # Run Stage 1 calibrated LLM analysis
        stage1_analysis = self._run_calibrated_stage1_llm(issue, validated_policies)
        
        return {
            "policies_analyzed": validated_policies,
            "denial_analysis": stage1_analysis,
            "stage": "calibrated_denial_reasoning"
        }

    # ---------------------------------------------------------------------
    # STAGE 2: CORRECTIVE REASONING
    # ---------------------------------------------------------------------
    def _stage2_corrective_reasoning(self, issue: Dict[str, Any], stage1_result: Dict[str, Any]) -> Dict[str, Any]:
        """Stage 2: Find how to fix the claim"""
        # Extract keywords from Stage 1 calibrated denial analysis
        denial_keywords = self._extract_correction_keywords(stage1_result, issue)
        
        # Re-query Qdrant with correction-focused intent
        correction_policies = self._search_correction_policies(denial_keywords, issue)
        print(f"    Stage 2: Retrieved {len(correction_policies)} policies for correction analysis")
        
        # Run Stage 2 LLM analysis
        stage2_analysis = self._run_stage2_llm(issue, stage1_result, correction_policies)
        
        return {
            "correction_policies": correction_policies,
            "correction_analysis": stage2_analysis,
            "stage": "corrective_reasoning"
        }

    # ---------------------------------------------------------------------
    # CALIBRATED POLICY VALIDATION (from calibrated version)
    # ---------------------------------------------------------------------
    def _calibrated_validate_and_deduplicate_policies(self, policies: List[Any], issue: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Calibrated policy validation with enhanced relevance checks - EXACT SAME AS STANDALONE VERSION"""
        if not policies:
            return []
        
        # Convert ScoredPoint objects to dictionaries
        policy_dicts = []
        for policy in policies:
            if hasattr(policy, 'payload'):
                policy_dict = policy.payload.copy()
                policy_dict['score'] = policy.score
                policy_dicts.append(policy_dict)
            elif isinstance(policy, dict):
                policy_dicts.append(policy)
        
        # Validate policy relevance using calibrated logic - EXACT SAME AS STANDALONE
        validated_policies = []
        for policy in policy_dicts:
            validation_result = self._validate_policy_relevance(policy, issue)
            if validation_result.get('is_relevant', False):
                # Add validation metadata
                policy.update({
                    'validation_status': validation_result.get('validation_status', 'UNKNOWN'),
                    'relevance_reason': validation_result.get('relevance_reason', ''),
                    'manual_appropriate': validation_result.get('manual_appropriate', True)
                })
                validated_policies.append(policy)
        
        # Deduplicate
        return self._deduplicate_policies(validated_policies)

    def _validate_policy_relevance(self, policy: Dict[str, Any], issue: Dict[str, Any]) -> Dict[str, Any]:
        """Validate if policy is relevant to the claim with more flexible criteria - EXACT COPY FROM STANDALONE"""
        cpt_code = issue.get('hcpcs_code', '')
        icd_code = issue.get('icd10_code', '')
        denial_reason = issue.get('ptp_denial_reason', '')
        source_file = policy.get('source', '')
        
        #  Check if policy mentions the CPT/ICD codes (use 'text' field, not 'excerpt')
        policy_text = policy.get('text', '').lower()
        mentions_cpt = cpt_code.lower() in policy_text if cpt_code else False
        mentions_icd = icd_code.lower() in policy_text if icd_code else False
        
        #  Check for broader relevance indicators - more flexible
        relevance_keywords = []
        if 'ptp' in denial_reason.lower():
            relevance_keywords.extend(['ptp', 'procedure', 'bundling', 'ncci', 'edit', 'coding'])
        if 'coding' in denial_reason.lower():
            relevance_keywords.extend(['coding', 'cpt', 'hcpcs', 'procedure', 'medical'])
        if 'coverage' in denial_reason.lower():
            relevance_keywords.extend(['coverage', 'lcd', 'determination', 'medical'])
        if 'definition' in denial_reason.lower():
            relevance_keywords.extend(['definition', 'coding', 'procedure', 'medical'])
        
        mentions_relevance_keywords = any(keyword in policy_text for keyword in relevance_keywords)
        
        #  Check manual type appropriateness
        manual_appropriate = self._check_manual_appropriateness(source_file, denial_reason)
        
        #  More flexible relevance - allow general medical policies
        general_medical_keywords = ['medical', 'procedure', 'service', 'coding', 'billing', 'claim']
        mentions_general = any(keyword in policy_text for keyword in general_medical_keywords)
        
        #  More flexible relevance determination
        if mentions_cpt or mentions_icd:
            return {
                'is_relevant': True,
                'validation_status': 'PASS',
                'relevance_reason': f'Policy mentions CPT/ICD codes directly',
                'manual_appropriate': manual_appropriate
            }
        elif mentions_relevance_keywords:
            return {
                'is_relevant': True,
                'validation_status': 'PASS',
                'relevance_reason': f'Policy mentions relevant keywords for {denial_reason}',
                'manual_appropriate': manual_appropriate
            }
        elif mentions_general and len(policy_text) > 200:  # Substantial content
            return {
                'is_relevant': True,
                'validation_status': 'PASS',
                'relevance_reason': f'Policy contains general medical content relevant to claims processing',
                'manual_appropriate': manual_appropriate
            }
        else:
            return {
                'is_relevant': False,
                'validation_status': 'FAIL',
                'relevance_reason': f'Policy does not contain relevant medical or coding content',
                'manual_appropriate': manual_appropriate
            }

    def _check_manual_appropriateness(self, source_file: str, denial_reason: str) -> bool:
        """Check if manual type is appropriate for the denial reason - EXACT COPY FROM STANDALONE"""
        if not source_file or not denial_reason:
            return True  # Default to allow if unclear
        
        source_lower = source_file.lower()
        denial_lower = denial_reason.lower()
        
        #  Restrict pim* to administrative issues only
        if source_lower.startswith('pim'):
            return 'administrative' in denial_lower or 'integrity' in denial_lower
        
        #  Prefer clm104* for coding conflicts
        if source_lower.startswith('clm104'):
            return any(keyword in denial_lower for keyword in ['coding', 'procedure', 'definition', 'ptp', 'conflict'])
        
        #  Prefer ncci* for bundling/PTP issues
        if source_lower.startswith('ncci'):
            return any(keyword in denial_lower for keyword in ['ptp', 'bundling', 'conflict', 'ncci'])
        
        #  Allow lcd* for coverage issues
        if source_lower.startswith('lcd'):
            return any(keyword in denial_lower for keyword in ['coverage', 'determination', 'local'])
        
        return True  # Default to allow other manuals

    def _identify_policy_source(self, source_file: str) -> str:
        """Identify policy manual based on source file name - EXACT COPY FROM STANDALONE"""
        if not source_file:
            return "Unknown Source"
        
        source_lower = source_file.lower()
        for prefix, manual_name in self.source_mapping.items():
            if source_lower.startswith(prefix):
                return manual_name
        
        return f"Policy Manual ({source_file})"

    # ---------------------------------------------------------------------
    # KEYWORD EXTRACTION FOR STAGE 2
    # ---------------------------------------------------------------------
    def _extract_correction_keywords(self, stage1_result: Dict[str, Any], issue: Dict[str, Any]) -> List[str]:
        """Extract keywords for correction-focused search from calibrated Stage 1"""
        denial_analysis = stage1_result.get("denial_analysis", {})
        final_summary = denial_analysis.get("final_reasoning_summary", "")
        denial_reason = issue.get("ptp_denial_reason", "")
        
        # Base keywords from denial reason
        correction_keywords = []
        
        if "ptp" in denial_reason.lower():
            correction_keywords.extend(["modifier", "XU", "59", "separately reportable", "bundling exception"])
        if "mue" in denial_reason.lower():
            correction_keywords.extend(["MUE", "unit limit", "threshold exception", "maximum units"])
        if "coverage" in denial_reason.lower():
            correction_keywords.extend(["covered diagnosis", "covered group", "ICD10", "LCD", "covered when"])
        if "definition" in denial_reason.lower():
            correction_keywords.extend(["procedure definition", "coding guidance", "documentation requirements"])
        
        # Add keywords from final summary
        summary_keywords = re.findall(r'\b[A-Z]{2,}\b|\b\w{3,}\b', final_summary)
        correction_keywords.extend([kw.lower() for kw in summary_keywords[:5]])
        
        # Remove duplicates and limit
        return list(set(correction_keywords))[:10]

    # ---------------------------------------------------------------------
    # CORRECTION-FOCUSED POLICY SEARCH
    # ---------------------------------------------------------------------
    def _search_correction_policies(self, keywords: List[str], issue: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Search for policies focused on corrections and allowances"""
        correction_policies = []
        
        # Create correction-focused query
        cpt_code = issue.get('hcpcs_code', '')
        icd_code = issue.get('icd10_code', '')
        keyword_str = " ".join(keywords)
        
        query_text = (
            f"CMS policy corrections allowances exceptions for CPT {cpt_code} ICD {icd_code} "
            f"modifier exceptions covered variants allowable combinations {keyword_str}"
        )
        
        query_vector = self.embedder.encode(query_text).tolist()
        
        # Search all collections for correction-focused policies
        for collection in self.policy_collections:
            try:
                hits = self.client.query_points(
                    collection_name=collection,
                    query=query_vector,
                    limit=3,
                    with_payload=True,
                    with_vectors=False,
                ).points
                
                # Convert to dictionaries
                for hit in hits:
                    policy_dict = hit.payload.copy()
                    policy_dict['score'] = hit.score
                    policy_dict['collection'] = collection
                    correction_policies.append(policy_dict)
                    
            except Exception as e:
                print(f" Correction search failed for {collection}: {e}")
        
        # Sort by score and deduplicate
        correction_policies.sort(key=lambda x: x.get('score', 0), reverse=True)
        return self._deduplicate_policies(correction_policies)[:8]

    # ---------------------------------------------------------------------
    # LLM PROCESSING FOR BOTH STAGES
    # ---------------------------------------------------------------------
    def _run_calibrated_stage1_llm(self, issue: Dict[str, Any], policies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run Stage 1 calibrated LLM for denial reasoning - EXACT SAME AS STANDALONE VERSION"""
        try:
            # Format policies with enhanced validation information - EXACT SAME AS STANDALONE
            policy_excerpts = ""
            for i, policy in enumerate(policies, 1):
                source_file = policy.get('source', 'unknown.pdf')
                manual_name = self._identify_policy_source(source_file)
                validation_status = policy.get('validation_status', 'UNKNOWN')
                
                policy_excerpts += f"\nPOLICY {i}:\n"
                policy_excerpts += f"Source File: {source_file}\n"
                policy_excerpts += f"Manual: {manual_name}\n"
                policy_excerpts += f"Validation: {validation_status}\n"
                policy_excerpts += f"Chapter: {policy.get('chapter', 'N/A')}\n"
                policy_excerpts += f"Section: {policy.get('section', 'N/A')}\n"
                policy_excerpts += f"Revision: {policy.get('rev', 'N/A')}\n"
                policy_excerpts += f"Page: {policy.get('page', 'N/A')}\n"
                policy_excerpts += f"Retrieval Score: {policy.get('score', 0.0):.4f}\n"
                policy_excerpts += f"Text: {policy.get('text', '')[:500]}...\n"
            
            # Format the calibrated prompt with exact claim data - EXACT SAME AS STANDALONE
            prompt = CALIBRATED_STAGE1_PROMPT.format(
                hcpcs_code=issue.get('hcpcs_code', 'N/A'),
                procedure_name=issue.get('procedure_name', 'N/A'),
                icd10_code=issue.get('icd10_code', 'N/A'),
                diagnosis_name=issue.get('diagnosis_name', 'N/A'),
                denial_reason=issue.get('ptp_denial_reason', 'N/A'),
                denial_risk_level=issue.get('denial_risk_level', 'N/A'),
                action_required=issue.get('action_required', 'N/A'),
                policy_excerpts=policy_excerpts
            )
            
            # Run LLM
            cmd = ["ollama", "run", "mistral", prompt]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                return {"error": f"Calibrated Stage 1 LLM failed: {result.stderr}"}
            
            # Parse JSON response
            llm_output = result.stdout.strip()
            try:
                json_start = llm_output.find('{')
                json_end = llm_output.rfind('}') + 1
                
                if json_start >= 0 and json_end > json_start:
                    json_str = llm_output[json_start:json_end]
                    return json.loads(json_str)
                else:
                    return {"summary": llm_output, "error": "No valid JSON found"}
                    
            except json.JSONDecodeError as e:
                return {"summary": llm_output, "error": f"JSON parsing failed: {e}"}
                
        except Exception as e:
            print(f" Calibrated Stage 1 LLM failed: {e}")
            return {"error": f"Calibrated Stage 1 processing failed: {e}"}

    def _run_stage2_llm(self, issue: Dict[str, Any], stage1_result: Dict[str, Any], correction_policies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run Stage 2 LLM for corrective reasoning"""
        try:
            # Format Stage 1 calibrated analysis
            denial_analysis = stage1_result.get("denial_analysis", {})
            denial_summary = json.dumps(denial_analysis, indent=2)
            
            # Format correction policies
            correction_policies_text = ""
            for i, policy in enumerate(correction_policies, 1):
                correction_policies_text += f"\nCORRECTION POLICY {i}:\n"
                correction_policies_text += f"Source: {policy.get('source', 'N/A')}\n"
                correction_policies_text += f"Collection: {policy.get('collection', 'N/A')}\n"
                correction_policies_text += f"Text: {policy.get('text', '')[:400]}...\n"
            
            # Format Stage 2 prompt
            prompt = STAGE2_CORRECTION_PROMPT.format(
                claim_id=issue.get('claim_id', 'N/A'),
                hcpcs_code=issue.get('hcpcs_code', 'N/A'),
                procedure_name=issue.get('procedure_name', 'N/A'),
                icd10_code=issue.get('icd10_code', 'N/A'),
                diagnosis_name=issue.get('diagnosis_name', 'N/A'),
                denial_reason=issue.get('ptp_denial_reason', 'N/A'),
                denial_analysis=denial_summary,
                correction_policies=correction_policies_text
            )
            
            # Run LLM
            cmd = ["ollama", "run", "mistral", prompt]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                return {"error": f"Stage 2 LLM failed: {result.stderr}"}
            
            # Parse JSON response
            llm_output = result.stdout.strip()
            try:
                json_start = llm_output.find('{')
                json_end = llm_output.rfind('}') + 1
                
                if json_start >= 0 and json_end > json_start:
                    json_str = llm_output[json_start:json_end]
                    return json.loads(json_str)
                else:
                    return {"summary": llm_output, "error": "No valid JSON found"}
                    
            except json.JSONDecodeError as e:
                return {"summary": llm_output, "error": f"JSON parsing failed: {e}"}
                
        except Exception as e:
            print(f" Stage 2 LLM failed: {e}")
            return {"error": f"Stage 2 processing failed: {e}"}

    # ---------------------------------------------------------------------
    # HELPER METHODS (from calibrated version)
    # ---------------------------------------------------------------------
    def _get_claim_issues(self, claim_id: str) -> List[Dict[str, Any]]:
        """Get claim issues from the claims collection"""
        try:
            hits = self.client.query_points(
                collection_name=self.claims_collection,
                query=[0] * 768,  # Dummy vector
                query_filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="claim_id",
                            match=models.MatchValue(value=claim_id)
                        )
                    ]
                ),
                limit=100,
                with_payload=True,
                with_vectors=False,
            ).points
            
            issues = []
            for hit in hits:
                if hit.payload:
                    issues.append(hit.payload)
            
            print(f"    Found {len(issues)} claim issues")
            return issues
            
        except Exception as e:
            print(f" Failed to get claim issues: {e}")
            return []

    def _hybrid_search(self, collection: str, issue: Dict[str, Any], top_k: int = 5):
        """Hybrid search with proper array matching"""
        try:
            icd_code = issue.get("icd10_code") or issue.get("icd9_code")
            hcpcs_code = issue.get("hcpcs_code") or issue.get("cpt_code")
            denial_reason = issue.get("ptp_denial_reason", "unspecified")

            query_text = (
                f"CMS policy for CPT/HCPCS {hcpcs_code}, diagnosis {icd_code}, "
                f"denial reason {denial_reason}. Include NCCI, LCD, and CMS manual sections."
            )
            query_vector = self.embedder.encode(query_text).tolist()

            # Try strict code matching first
            strict_filter = models.Filter(
                should=[
                    models.FieldCondition(
                        key="cpt_codes",
                        match=models.MatchAny(any=[str(hcpcs_code).upper()])
                    ) if hcpcs_code else None,
                    models.FieldCondition(
                        key="hcpcs_codes",
                        match=models.MatchAny(any=[str(hcpcs_code).upper()])
                    ) if hcpcs_code else None,
                    models.FieldCondition(
                        key="icd10_codes",
                        match=models.MatchAny(any=[str(icd_code).upper().replace(".", "")])
                    ) if icd_code else None,
                    models.FieldCondition(
                        key="text",
                        match=models.MatchText(text=str(hcpcs_code))
                    ) if hcpcs_code else None,
                    models.FieldCondition(
                        key="text",
                        match=models.MatchText(text=str(icd_code))
                    ) if icd_code else None,
                ]
            )

            strict_filter.should = [f for f in strict_filter.should if f is not None]

            hits = self.client.query_points(
                collection_name=collection,
                query=query_vector,
                query_filter=strict_filter if strict_filter.should else None,
                limit=top_k,
                with_payload=True,
                with_vectors=False,
            ).points

            # Fallback to semantic search if no strict matches
            if not hits or len(hits) == 0:
                print(f"    No strict matches for {hcpcs_code}/{icd_code}, falling back to semantic search...")
                hits = self.client.query_points(
                    collection_name=collection,
                    query=query_vector,
                    query_filter=None,
                    limit=top_k,
                    with_payload=True,
                    with_vectors=False,
                ).points

            return hits or []
        except Exception as e:
            print(f" Search failed for {collection}: {e}")
            return []

    def _deduplicate_policies(self, policies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate policies"""
        seen_excerpts = {}
        deduplicated = []
        
        for policy in policies:
            excerpt_key = policy.get('text', '')[:200]
            
            if excerpt_key not in seen_excerpts:
                seen_excerpts[excerpt_key] = policy
                deduplicated.append(policy)
        
        return deduplicated


# ---------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------
if __name__ == "__main__":
    corrector = TwoStageCalibratedClaimCorrector()
    claim_id = "123456789012345"
    enriched = corrector.run_two_stage_corrections(claim_id)
    print(json.dumps(enriched, indent=2))




Two-Stage CMS Claim Corrector with Calibrated Stage 1
Stage 1: Uses calibrated denial reasoning (from claim_corrector_claims3_calibrated.py)
Stage 2: Corrective Reasoning Pass - Find how to fix the claim
"""

import json
import re
import subprocess
from typing import Dict, Any, List, Tuple, Optional
from sentence_transformers import SentenceTransformer
import torch
from qdrant_client import QdrantClient
from qdrant_client import models

# -------------------------------------------------------------------------
# STATIC CPT MAPPING (No Database Dependency)
# -------------------------------------------------------------------------

def get_cpt_description(cpt_code: str) -> str:
    """Get CPT description from static mapping (database-free approach)"""
    cpt_mappings = {
        "74170": "CT abdomen and pelvis with contrast",
        "99213": "Office visit, established patient, 20-29 minutes",
        "99214": "Office visit, established patient, 30-39 minutes",
        "99215": "Office visit, established patient, 40-54 minutes",
        "99201": "Office visit, new patient, 10 minutes",
        "99202": "Office visit, new patient, 20 minutes",
        "99203": "Office visit, new patient, 30 minutes",
        "99204": "Office visit, new patient, 45 minutes",
        "99205": "Office visit, new patient, 60 minutes",
        "74176": "CT abdomen with contrast",
        "74177": "CT pelvis with contrast",
        "74178": "CT abdomen and pelvis without contrast"
    }
    
    return cpt_mappings.get(cpt_code, f"Medical procedure {cpt_code}")

# -------------------------------------------------------------------------
# STAGE 1: CALIBRATED DENIAL REASONING PROMPT (from calibrated version)
# -------------------------------------------------------------------------

CALIBRATED_STAGE1_PROMPT = """
You are a CMS Policy Reasoning Assistant specializing in Medicare claim denial analysis.

CRITICAL VALIDATION RULES - READ CAREFULLY:
1. Use ONLY the exact claim data provided below - DO NOT infer patient conditions not present
2. Do NOT hallucinate medical conditions (ESRD, diabetes, etc.) unless explicitly mentioned in claim
3. Base ALL reasoning ONLY on retrieved policy text and provided ICD/CPT codes
4. If policy excerpt doesn't mention the specific CPT/ICD codes, mark as LOW relevance
5. Identify policy sources accurately based on file paths provided

EXACT CLAIM DATA (DO NOT MODIFY OR INFER):
- CPT/HCPCS: {hcpcs_code} ({procedure_name})
- ICD-10: {icd10_code} ({diagnosis_name})
- Denial Reason: {denial_reason}
- Risk Level: {denial_risk_level}
- Action Required: {action_required}

RETRIEVED POLICIES WITH RELEVANCE VALIDATION:
{policy_excerpts}

MANUAL TYPE RESTRICTIONS:
- pim*.pdf (Program Integrity Manual): ONLY for administrative/fraud issues
- clm104*.pdf (Claims Processing Manual): For coding conflicts and procedure definitions
- ncci*.pdf (NCCI): For bundling conflicts and PTP edits
- lcd*.pdf (LCD): For coverage determinations and local policies

REQUIRED OUTPUT FORMAT (valid JSON only):
{{
  "claim_summary": "Brief description using EXACT claim data - NO inferred conditions",
  "relevant_policies": [
    {{
      "collection": "Exact manual name based on source file",
      "chapter": "Chapter from source data",
      "section": "Section from source data", 
      "rev": "Revision from source data",
      "source_file": "exact filename (e.g., clm104c12.pdf)",
      "page": "page number",
      "policy_summary": "HOW this specific policy explains the denial - MUST mention CPT/ICD codes from claim",
      "relevance_score": "HIGH/MEDIUM/LOW based on CPT/ICD code mention in policy",
      "retrieval_confidence": "score from search results",
      "validation_status": "PASS/FAIL - whether policy mentions claim CPT/ICD codes"
    }}
  ],
  "filtered_out_policies": [
    "List policies that don't mention CPT/ICD codes or are wrong manual type"
  ],
  "final_reasoning_summary": "Complete explanation using EXACT claim data. NO inferred medical conditions. Include specific policy citations.",
  "data_consistency_check": "Confirm: Used exact CPT/ICD descriptions without inferring patient conditions",
  "validation_summary": "Summary of policy relevance validation results",
  "denial_keywords": ["keyword1", "keyword2", "keyword3"]
}}

STRICT VALIDATION REQUIREMENTS:
- Only include policies that directly mention the claim's CPT/ICD codes
- Reject policies from wrong manual types (e.g., pim* for clinical coding issues)
- Do NOT infer patient medical conditions not explicitly stated
- Use EXACT CPT/ICD descriptions provided, not generic terms
"""

# -------------------------------------------------------------------------
# STAGE 2: CORRECTIVE REASONING PROMPT
# -------------------------------------------------------------------------

STAGE2_CORRECTION_PROMPT = """
You are a CMS Correction Assistant specializing in Medicare claim remediation.

CRITICAL INSTRUCTIONS:
1. Based on the calibrated denial reasoning from Stage 1, find HOW to fix the claim
2. Look for modifier exceptions, covered ICD variants, allowable combinations
3. Provide specific, actionable corrections with policy references
4. Focus on making the claim compliant

ORIGINAL CLAIM DATA:
- CPT/HCPCS: {hcpcs_code} ({procedure_name})
- ICD-10: {icd10_code} ({diagnosis_name})
- Denial Reason: {denial_reason}

STAGE 1 CALIBRATED DENIAL ANALYSIS:
{denial_analysis}

CORRECTION-FOCUSED POLICIES:
{correction_policies}

REQUIRED OUTPUT FORMAT (valid JSON only):
{{
  "claim_id": "{claim_id}",
  "recommended_corrections": [
    {{
      "field": "modifier|diagnosis_code|procedure_code|documentation",
      "suggestion": "Specific actionable correction with policy reference",
      "confidence": 0.85,
      "policy_reference": "Manual Chapter Section",
      "implementation_guidance": "Step-by-step instructions"
    }}
  ],
  "policy_references": [
    "Medicare Claims Processing Manual Ch.12 20.4.5",
    "LCD L34696  Fracture and Bone Imaging Coverage"
  ],
  "final_guidance": "Summary of how this claim can be fixed to pass",
  "compliance_checklist": [
    "Action item 1",
    "Action item 2"
  ]
}}

FOCUS: Find HOW to fix the claim to make it compliant with CMS policy.
"""

# -------------------------------------------------------------------------
# TWO-STAGE CLAIM CORRECTOR WITH CALIBRATED STAGE 1
# -------------------------------------------------------------------------

class TwoStageCalibratedClaimCorrector:
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
        self.claims_collection = "claim_analysis_metadata"
        
        #  Source mapping for policy identification - EXACT SAME AS STANDALONE
        self.source_mapping = {
            "clm104c": "Medicare Claims Processing Manual",
            "pim83c": "Program Integrity Manual (Administrative Only)", 
            "ncci": "National Correct Coding Initiative",
            "lcd": "Local Coverage Determination",
            "mcm": "Medicare Claims Manual",
            "bpm": "Medicare Benefit Policy Manual"
        }

    def run_two_stage_corrections(self, claim_id: str) -> Dict[str, Any]:
        """Run both calibrated denial reasoning and corrective reasoning stages"""
        print(f"\n TWO-STAGE CALIBRATED PROCESSING: {claim_id}")
        
        # Get claim issues
        issues = self._get_claim_issues(claim_id)
        if not issues:
            return {"claim_id": claim_id, "enriched_issues": []}

        enriched_issues = []
        for issue in issues:
            print(f"\n Processing issue: {issue.get('hcpcs_code', 'N/A')} + {issue.get('icd10_code', 'N/A')}")
            
            #  Get CPT description
            cpt_code = issue.get('hcpcs_code', '')
            if cpt_code:
                dynamic_procedure_name = get_cpt_description(cpt_code)
                issue['procedure_name'] = dynamic_procedure_name
                print(f"    Updated procedure name: {dynamic_procedure_name}")
            
            #  STAGE 1: Calibrated Denial Reasoning Pass
            print(f"    STAGE 1: Calibrated denial reasoning analysis...")
            stage1_result = self._stage1_calibrated_denial_reasoning(issue)
            
            #  STAGE 2: Corrective Reasoning Pass
            print(f"    STAGE 2: Finding corrective solutions...")
            stage2_result = self._stage2_corrective_reasoning(issue, stage1_result)
            
            # Combine results
            enriched_issue = {
                **issue,
                "stage1_calibrated_denial_analysis": stage1_result,
                "stage2_correction_analysis": stage2_result,
                "two_stage_calibrated_complete": True
            }
            enriched_issues.append(enriched_issue)

        return {"claim_id": claim_id, "enriched_issues": enriched_issues}

    # ---------------------------------------------------------------------
    # STAGE 1: CALIBRATED DENIAL REASONING (from calibrated version)
    # ---------------------------------------------------------------------
    def _stage1_calibrated_denial_reasoning(self, issue: Dict[str, Any]) -> Dict[str, Any]:
        """Stage 1: Calibrated denial reasoning using enhanced validation"""
        # Get policies for denial reasoning
        all_policies = []
        for collection in self.policy_collections:
            policies = self._hybrid_search(collection, issue, top_k=3)
            all_policies.extend(policies)
        
        # Validate and deduplicate policies using calibrated logic
        validated_policies = self._calibrated_validate_and_deduplicate_policies(all_policies, issue)
        print(f"    Stage 1: Retrieved {len(validated_policies)} calibrated policies for denial analysis")
        
        # Run Stage 1 calibrated LLM analysis
        stage1_analysis = self._run_calibrated_stage1_llm(issue, validated_policies)
        
        return {
            "policies_analyzed": validated_policies,
            "denial_analysis": stage1_analysis,
            "stage": "calibrated_denial_reasoning"
        }

    # ---------------------------------------------------------------------
    # STAGE 2: CORRECTIVE REASONING
    # ---------------------------------------------------------------------
    def _stage2_corrective_reasoning(self, issue: Dict[str, Any], stage1_result: Dict[str, Any]) -> Dict[str, Any]:
        """Stage 2: Find how to fix the claim"""
        # Extract keywords from Stage 1 calibrated denial analysis
        denial_keywords = self._extract_correction_keywords(stage1_result, issue)
        
        # Re-query Qdrant with correction-focused intent
        correction_policies = self._search_correction_policies(denial_keywords, issue)
        print(f"    Stage 2: Retrieved {len(correction_policies)} policies for correction analysis")
        
        # Run Stage 2 LLM analysis
        stage2_analysis = self._run_stage2_llm(issue, stage1_result, correction_policies)
        
        return {
            "correction_policies": correction_policies,
            "correction_analysis": stage2_analysis,
            "stage": "corrective_reasoning"
        }

    # ---------------------------------------------------------------------
    # CALIBRATED POLICY VALIDATION (from calibrated version)
    # ---------------------------------------------------------------------
    def _calibrated_validate_and_deduplicate_policies(self, policies: List[Any], issue: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Calibrated policy validation with enhanced relevance checks - EXACT SAME AS STANDALONE VERSION"""
        if not policies:
            return []
        
        # Convert ScoredPoint objects to dictionaries
        policy_dicts = []
        for policy in policies:
            if hasattr(policy, 'payload'):
                policy_dict = policy.payload.copy()
                policy_dict['score'] = policy.score
                policy_dicts.append(policy_dict)
            elif isinstance(policy, dict):
                policy_dicts.append(policy)
        
        # Validate policy relevance using calibrated logic - EXACT SAME AS STANDALONE
        validated_policies = []
        for policy in policy_dicts:
            validation_result = self._validate_policy_relevance(policy, issue)
            if validation_result.get('is_relevant', False):
                # Add validation metadata
                policy.update({
                    'validation_status': validation_result.get('validation_status', 'UNKNOWN'),
                    'relevance_reason': validation_result.get('relevance_reason', ''),
                    'manual_appropriate': validation_result.get('manual_appropriate', True)
                })
                validated_policies.append(policy)
        
        # Deduplicate
        return self._deduplicate_policies(validated_policies)

    def _validate_policy_relevance(self, policy: Dict[str, Any], issue: Dict[str, Any]) -> Dict[str, Any]:
        """Validate if policy is relevant to the claim with more flexible criteria - EXACT COPY FROM STANDALONE"""
        cpt_code = issue.get('hcpcs_code', '')
        icd_code = issue.get('icd10_code', '')
        denial_reason = issue.get('ptp_denial_reason', '')
        source_file = policy.get('source', '')
        
        #  Check if policy mentions the CPT/ICD codes (use 'text' field, not 'excerpt')
        policy_text = policy.get('text', '').lower()
        mentions_cpt = cpt_code.lower() in policy_text if cpt_code else False
        mentions_icd = icd_code.lower() in policy_text if icd_code else False
        
        #  Check for broader relevance indicators - more flexible
        relevance_keywords = []
        if 'ptp' in denial_reason.lower():
            relevance_keywords.extend(['ptp', 'procedure', 'bundling', 'ncci', 'edit', 'coding'])
        if 'coding' in denial_reason.lower():
            relevance_keywords.extend(['coding', 'cpt', 'hcpcs', 'procedure', 'medical'])
        if 'coverage' in denial_reason.lower():
            relevance_keywords.extend(['coverage', 'lcd', 'determination', 'medical'])
        if 'definition' in denial_reason.lower():
            relevance_keywords.extend(['definition', 'coding', 'procedure', 'medical'])
        
        mentions_relevance_keywords = any(keyword in policy_text for keyword in relevance_keywords)
        
        #  Check manual type appropriateness
        manual_appropriate = self._check_manual_appropriateness(source_file, denial_reason)
        
        #  More flexible relevance - allow general medical policies
        general_medical_keywords = ['medical', 'procedure', 'service', 'coding', 'billing', 'claim']
        mentions_general = any(keyword in policy_text for keyword in general_medical_keywords)
        
        #  More flexible relevance determination
        if mentions_cpt or mentions_icd:
            return {
                'is_relevant': True,
                'validation_status': 'PASS',
                'relevance_reason': f'Policy mentions CPT/ICD codes directly',
                'manual_appropriate': manual_appropriate
            }
        elif mentions_relevance_keywords:
            return {
                'is_relevant': True,
                'validation_status': 'PASS',
                'relevance_reason': f'Policy mentions relevant keywords for {denial_reason}',
                'manual_appropriate': manual_appropriate
            }
        elif mentions_general and len(policy_text) > 200:  # Substantial content
            return {
                'is_relevant': True,
                'validation_status': 'PASS',
                'relevance_reason': f'Policy contains general medical content relevant to claims processing',
                'manual_appropriate': manual_appropriate
            }
        else:
            return {
                'is_relevant': False,
                'validation_status': 'FAIL',
                'relevance_reason': f'Policy does not contain relevant medical or coding content',
                'manual_appropriate': manual_appropriate
            }

    def _check_manual_appropriateness(self, source_file: str, denial_reason: str) -> bool:
        """Check if manual type is appropriate for the denial reason - EXACT COPY FROM STANDALONE"""
        if not source_file or not denial_reason:
            return True  # Default to allow if unclear
        
        source_lower = source_file.lower()
        denial_lower = denial_reason.lower()
        
        #  Restrict pim* to administrative issues only
        if source_lower.startswith('pim'):
            return 'administrative' in denial_lower or 'integrity' in denial_lower
        
        #  Prefer clm104* for coding conflicts
        if source_lower.startswith('clm104'):
            return any(keyword in denial_lower for keyword in ['coding', 'procedure', 'definition', 'ptp', 'conflict'])
        
        #  Prefer ncci* for bundling/PTP issues
        if source_lower.startswith('ncci'):
            return any(keyword in denial_lower for keyword in ['ptp', 'bundling', 'conflict', 'ncci'])
        
        #  Allow lcd* for coverage issues
        if source_lower.startswith('lcd'):
            return any(keyword in denial_lower for keyword in ['coverage', 'determination', 'local'])
        
        return True  # Default to allow other manuals

    def _identify_policy_source(self, source_file: str) -> str:
        """Identify policy manual based on source file name - EXACT COPY FROM STANDALONE"""
        if not source_file:
            return "Unknown Source"
        
        source_lower = source_file.lower()
        for prefix, manual_name in self.source_mapping.items():
            if source_lower.startswith(prefix):
                return manual_name
        
        return f"Policy Manual ({source_file})"

    # ---------------------------------------------------------------------
    # KEYWORD EXTRACTION FOR STAGE 2
    # ---------------------------------------------------------------------
    def _extract_correction_keywords(self, stage1_result: Dict[str, Any], issue: Dict[str, Any]) -> List[str]:
        """Extract keywords for correction-focused search from calibrated Stage 1"""
        denial_analysis = stage1_result.get("denial_analysis", {})
        final_summary = denial_analysis.get("final_reasoning_summary", "")
        denial_reason = issue.get("ptp_denial_reason", "")
        
        # Base keywords from denial reason
        correction_keywords = []
        
        if "ptp" in denial_reason.lower():
            correction_keywords.extend(["modifier", "XU", "59", "separately reportable", "bundling exception"])
        if "mue" in denial_reason.lower():
            correction_keywords.extend(["MUE", "unit limit", "threshold exception", "maximum units"])
        if "coverage" in denial_reason.lower():
            correction_keywords.extend(["covered diagnosis", "covered group", "ICD10", "LCD", "covered when"])
        if "definition" in denial_reason.lower():
            correction_keywords.extend(["procedure definition", "coding guidance", "documentation requirements"])
        
        # Add keywords from final summary
        summary_keywords = re.findall(r'\b[A-Z]{2,}\b|\b\w{3,}\b', final_summary)
        correction_keywords.extend([kw.lower() for kw in summary_keywords[:5]])
        
        # Remove duplicates and limit
        return list(set(correction_keywords))[:10]

    # ---------------------------------------------------------------------
    # CORRECTION-FOCUSED POLICY SEARCH
    # ---------------------------------------------------------------------
    def _search_correction_policies(self, keywords: List[str], issue: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Search for policies focused on corrections and allowances"""
        correction_policies = []
        
        # Create correction-focused query
        cpt_code = issue.get('hcpcs_code', '')
        icd_code = issue.get('icd10_code', '')
        keyword_str = " ".join(keywords)
        
        query_text = (
            f"CMS policy corrections allowances exceptions for CPT {cpt_code} ICD {icd_code} "
            f"modifier exceptions covered variants allowable combinations {keyword_str}"
        )
        
        query_vector = self.embedder.encode(query_text).tolist()
        
        # Search all collections for correction-focused policies
        for collection in self.policy_collections:
            try:
                hits = self.client.query_points(
                    collection_name=collection,
                    query=query_vector,
                    limit=3,
                    with_payload=True,
                    with_vectors=False,
                ).points
                
                # Convert to dictionaries
                for hit in hits:
                    policy_dict = hit.payload.copy()
                    policy_dict['score'] = hit.score
                    policy_dict['collection'] = collection
                    correction_policies.append(policy_dict)
                    
            except Exception as e:
                print(f" Correction search failed for {collection}: {e}")
        
        # Sort by score and deduplicate
        correction_policies.sort(key=lambda x: x.get('score', 0), reverse=True)
        return self._deduplicate_policies(correction_policies)[:8]

    # ---------------------------------------------------------------------
    # LLM PROCESSING FOR BOTH STAGES
    # ---------------------------------------------------------------------
    def _run_calibrated_stage1_llm(self, issue: Dict[str, Any], policies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run Stage 1 calibrated LLM for denial reasoning - EXACT SAME AS STANDALONE VERSION"""
        try:
            # Format policies with enhanced validation information - EXACT SAME AS STANDALONE
            policy_excerpts = ""
            for i, policy in enumerate(policies, 1):
                source_file = policy.get('source', 'unknown.pdf')
                manual_name = self._identify_policy_source(source_file)
                validation_status = policy.get('validation_status', 'UNKNOWN')
                
                policy_excerpts += f"\nPOLICY {i}:\n"
                policy_excerpts += f"Source File: {source_file}\n"
                policy_excerpts += f"Manual: {manual_name}\n"
                policy_excerpts += f"Validation: {validation_status}\n"
                policy_excerpts += f"Chapter: {policy.get('chapter', 'N/A')}\n"
                policy_excerpts += f"Section: {policy.get('section', 'N/A')}\n"
                policy_excerpts += f"Revision: {policy.get('rev', 'N/A')}\n"
                policy_excerpts += f"Page: {policy.get('page', 'N/A')}\n"
                policy_excerpts += f"Retrieval Score: {policy.get('score', 0.0):.4f}\n"
                policy_excerpts += f"Text: {policy.get('text', '')[:500]}...\n"
            
            # Format the calibrated prompt with exact claim data - EXACT SAME AS STANDALONE
            prompt = CALIBRATED_STAGE1_PROMPT.format(
                hcpcs_code=issue.get('hcpcs_code', 'N/A'),
                procedure_name=issue.get('procedure_name', 'N/A'),
                icd10_code=issue.get('icd10_code', 'N/A'),
                diagnosis_name=issue.get('diagnosis_name', 'N/A'),
                denial_reason=issue.get('ptp_denial_reason', 'N/A'),
                denial_risk_level=issue.get('denial_risk_level', 'N/A'),
                action_required=issue.get('action_required', 'N/A'),
                policy_excerpts=policy_excerpts
            )
            
            # Run LLM
            cmd = ["ollama", "run", "mistral", prompt]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                return {"error": f"Calibrated Stage 1 LLM failed: {result.stderr}"}
            
            # Parse JSON response
            llm_output = result.stdout.strip()
            try:
                json_start = llm_output.find('{')
                json_end = llm_output.rfind('}') + 1
                
                if json_start >= 0 and json_end > json_start:
                    json_str = llm_output[json_start:json_end]
                    return json.loads(json_str)
                else:
                    return {"summary": llm_output, "error": "No valid JSON found"}
                    
            except json.JSONDecodeError as e:
                return {"summary": llm_output, "error": f"JSON parsing failed: {e}"}
                
        except Exception as e:
            print(f" Calibrated Stage 1 LLM failed: {e}")
            return {"error": f"Calibrated Stage 1 processing failed: {e}"}

    def _run_stage2_llm(self, issue: Dict[str, Any], stage1_result: Dict[str, Any], correction_policies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run Stage 2 LLM for corrective reasoning"""
        try:
            # Format Stage 1 calibrated analysis
            denial_analysis = stage1_result.get("denial_analysis", {})
            denial_summary = json.dumps(denial_analysis, indent=2)
            
            # Format correction policies
            correction_policies_text = ""
            for i, policy in enumerate(correction_policies, 1):
                correction_policies_text += f"\nCORRECTION POLICY {i}:\n"
                correction_policies_text += f"Source: {policy.get('source', 'N/A')}\n"
                correction_policies_text += f"Collection: {policy.get('collection', 'N/A')}\n"
                correction_policies_text += f"Text: {policy.get('text', '')[:400]}...\n"
            
            # Format Stage 2 prompt
            prompt = STAGE2_CORRECTION_PROMPT.format(
                claim_id=issue.get('claim_id', 'N/A'),
                hcpcs_code=issue.get('hcpcs_code', 'N/A'),
                procedure_name=issue.get('procedure_name', 'N/A'),
                icd10_code=issue.get('icd10_code', 'N/A'),
                diagnosis_name=issue.get('diagnosis_name', 'N/A'),
                denial_reason=issue.get('ptp_denial_reason', 'N/A'),
                denial_analysis=denial_summary,
                correction_policies=correction_policies_text
            )
            
            # Run LLM
            cmd = ["ollama", "run", "mistral", prompt]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                return {"error": f"Stage 2 LLM failed: {result.stderr}"}
            
            # Parse JSON response
            llm_output = result.stdout.strip()
            try:
                json_start = llm_output.find('{')
                json_end = llm_output.rfind('}') + 1
                
                if json_start >= 0 and json_end > json_start:
                    json_str = llm_output[json_start:json_end]
                    return json.loads(json_str)
                else:
                    return {"summary": llm_output, "error": "No valid JSON found"}
                    
            except json.JSONDecodeError as e:
                return {"summary": llm_output, "error": f"JSON parsing failed: {e}"}
                
        except Exception as e:
            print(f" Stage 2 LLM failed: {e}")
            return {"error": f"Stage 2 processing failed: {e}"}

    # ---------------------------------------------------------------------
    # HELPER METHODS (from calibrated version)
    # ---------------------------------------------------------------------
    def _get_claim_issues(self, claim_id: str) -> List[Dict[str, Any]]:
        """Get claim issues from the claims collection"""
        try:
            hits = self.client.query_points(
                collection_name=self.claims_collection,
                query=[0] * 768,  # Dummy vector
                query_filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="claim_id",
                            match=models.MatchValue(value=claim_id)
                        )
                    ]
                ),
                limit=100,
                with_payload=True,
                with_vectors=False,
            ).points
            
            issues = []
            for hit in hits:
                if hit.payload:
                    issues.append(hit.payload)
            
            print(f"    Found {len(issues)} claim issues")
            return issues
            
        except Exception as e:
            print(f" Failed to get claim issues: {e}")
            return []

    def _hybrid_search(self, collection: str, issue: Dict[str, Any], top_k: int = 5):
        """Hybrid search with proper array matching"""
        try:
            icd_code = issue.get("icd10_code") or issue.get("icd9_code")
            hcpcs_code = issue.get("hcpcs_code") or issue.get("cpt_code")
            denial_reason = issue.get("ptp_denial_reason", "unspecified")

            query_text = (
                f"CMS policy for CPT/HCPCS {hcpcs_code}, diagnosis {icd_code}, "
                f"denial reason {denial_reason}. Include NCCI, LCD, and CMS manual sections."
            )
            query_vector = self.embedder.encode(query_text).tolist()

            # Try strict code matching first
            strict_filter = models.Filter(
                should=[
                    models.FieldCondition(
                        key="cpt_codes",
                        match=models.MatchAny(any=[str(hcpcs_code).upper()])
                    ) if hcpcs_code else None,
                    models.FieldCondition(
                        key="hcpcs_codes",
                        match=models.MatchAny(any=[str(hcpcs_code).upper()])
                    ) if hcpcs_code else None,
                    models.FieldCondition(
                        key="icd10_codes",
                        match=models.MatchAny(any=[str(icd_code).upper().replace(".", "")])
                    ) if icd_code else None,
                    models.FieldCondition(
                        key="text",
                        match=models.MatchText(text=str(hcpcs_code))
                    ) if hcpcs_code else None,
                    models.FieldCondition(
                        key="text",
                        match=models.MatchText(text=str(icd_code))
                    ) if icd_code else None,
                ]
            )

            strict_filter.should = [f for f in strict_filter.should if f is not None]

            hits = self.client.query_points(
                collection_name=collection,
                query=query_vector,
                query_filter=strict_filter if strict_filter.should else None,
                limit=top_k,
                with_payload=True,
                with_vectors=False,
            ).points

            # Fallback to semantic search if no strict matches
            if not hits or len(hits) == 0:
                print(f"    No strict matches for {hcpcs_code}/{icd_code}, falling back to semantic search...")
                hits = self.client.query_points(
                    collection_name=collection,
                    query=query_vector,
                    query_filter=None,
                    limit=top_k,
                    with_payload=True,
                    with_vectors=False,
                ).points

            return hits or []
        except Exception as e:
            print(f" Search failed for {collection}: {e}")
            return []

    def _deduplicate_policies(self, policies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate policies"""
        seen_excerpts = {}
        deduplicated = []
        
        for policy in policies:
            excerpt_key = policy.get('text', '')[:200]
            
            if excerpt_key not in seen_excerpts:
                seen_excerpts[excerpt_key] = policy
                deduplicated.append(policy)
        
        return deduplicated


# ---------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------
if __name__ == "__main__":
    corrector = TwoStageCalibratedClaimCorrector()
    claim_id = "123456789012345"
    enriched = corrector.run_two_stage_corrections(claim_id)
    print(json.dumps(enriched, indent=2))


Two-Stage CMS Claim Corrector with Calibrated Stage 1
Stage 1: Uses calibrated denial reasoning (from claim_corrector_claims3_calibrated.py)
Stage 2: Corrective Reasoning Pass - Find how to fix the claim
"""

import json
import re
import subprocess
from typing import Dict, Any, List, Tuple, Optional
from sentence_transformers import SentenceTransformer
import torch
from qdrant_client import QdrantClient
from qdrant_client import models

# -------------------------------------------------------------------------
# STATIC CPT MAPPING (No Database Dependency)
# -------------------------------------------------------------------------

def get_cpt_description(cpt_code: str) -> str:
    """Get CPT description from static mapping (database-free approach)"""
    cpt_mappings = {
        "74170": "CT abdomen and pelvis with contrast",
        "99213": "Office visit, established patient, 20-29 minutes",
        "99214": "Office visit, established patient, 30-39 minutes",
        "99215": "Office visit, established patient, 40-54 minutes",
        "99201": "Office visit, new patient, 10 minutes",
        "99202": "Office visit, new patient, 20 minutes",
        "99203": "Office visit, new patient, 30 minutes",
        "99204": "Office visit, new patient, 45 minutes",
        "99205": "Office visit, new patient, 60 minutes",
        "74176": "CT abdomen with contrast",
        "74177": "CT pelvis with contrast",
        "74178": "CT abdomen and pelvis without contrast"
    }
    
    return cpt_mappings.get(cpt_code, f"Medical procedure {cpt_code}")

# -------------------------------------------------------------------------
# STAGE 1: CALIBRATED DENIAL REASONING PROMPT (from calibrated version)
# -------------------------------------------------------------------------

CALIBRATED_STAGE1_PROMPT = """
You are a CMS Policy Reasoning Assistant specializing in Medicare claim denial analysis.

CRITICAL VALIDATION RULES - READ CAREFULLY:
1. Use ONLY the exact claim data provided below - DO NOT infer patient conditions not present
2. Do NOT hallucinate medical conditions (ESRD, diabetes, etc.) unless explicitly mentioned in claim
3. Base ALL reasoning ONLY on retrieved policy text and provided ICD/CPT codes
4. If policy excerpt doesn't mention the specific CPT/ICD codes, mark as LOW relevance
5. Identify policy sources accurately based on file paths provided

EXACT CLAIM DATA (DO NOT MODIFY OR INFER):
- CPT/HCPCS: {hcpcs_code} ({procedure_name})
- ICD-10: {icd10_code} ({diagnosis_name})
- Denial Reason: {denial_reason}
- Risk Level: {denial_risk_level}
- Action Required: {action_required}

RETRIEVED POLICIES WITH RELEVANCE VALIDATION:
{policy_excerpts}

MANUAL TYPE RESTRICTIONS:
- pim*.pdf (Program Integrity Manual): ONLY for administrative/fraud issues
- clm104*.pdf (Claims Processing Manual): For coding conflicts and procedure definitions
- ncci*.pdf (NCCI): For bundling conflicts and PTP edits
- lcd*.pdf (LCD): For coverage determinations and local policies

REQUIRED OUTPUT FORMAT (valid JSON only):
{{
  "claim_summary": "Brief description using EXACT claim data - NO inferred conditions",
  "relevant_policies": [
    {{
      "collection": "Exact manual name based on source file",
      "chapter": "Chapter from source data",
      "section": "Section from source data", 
      "rev": "Revision from source data",
      "source_file": "exact filename (e.g., clm104c12.pdf)",
      "page": "page number",
      "policy_summary": "HOW this specific policy explains the denial - MUST mention CPT/ICD codes from claim",
      "relevance_score": "HIGH/MEDIUM/LOW based on CPT/ICD code mention in policy",
      "retrieval_confidence": "score from search results",
      "validation_status": "PASS/FAIL - whether policy mentions claim CPT/ICD codes"
    }}
  ],
  "filtered_out_policies": [
    "List policies that don't mention CPT/ICD codes or are wrong manual type"
  ],
  "final_reasoning_summary": "Complete explanation using EXACT claim data. NO inferred medical conditions. Include specific policy citations.",
  "data_consistency_check": "Confirm: Used exact CPT/ICD descriptions without inferring patient conditions",
  "validation_summary": "Summary of policy relevance validation results",
  "denial_keywords": ["keyword1", "keyword2", "keyword3"]
}}

STRICT VALIDATION REQUIREMENTS:
- Only include policies that directly mention the claim's CPT/ICD codes
- Reject policies from wrong manual types (e.g., pim* for clinical coding issues)
- Do NOT infer patient medical conditions not explicitly stated
- Use EXACT CPT/ICD descriptions provided, not generic terms
"""

# -------------------------------------------------------------------------
# STAGE 2: CORRECTIVE REASONING PROMPT
# -------------------------------------------------------------------------

STAGE2_CORRECTION_PROMPT = """
You are a CMS Correction Assistant specializing in Medicare claim remediation.

CRITICAL INSTRUCTIONS:
1. Based on the calibrated denial reasoning from Stage 1, find HOW to fix the claim
2. Look for modifier exceptions, covered ICD variants, allowable combinations
3. Provide specific, actionable corrections with policy references
4. Focus on making the claim compliant

ORIGINAL CLAIM DATA:
- CPT/HCPCS: {hcpcs_code} ({procedure_name})
- ICD-10: {icd10_code} ({diagnosis_name})
- Denial Reason: {denial_reason}

STAGE 1 CALIBRATED DENIAL ANALYSIS:
{denial_analysis}

CORRECTION-FOCUSED POLICIES:
{correction_policies}

REQUIRED OUTPUT FORMAT (valid JSON only):
{{
  "claim_id": "{claim_id}",
  "recommended_corrections": [
    {{
      "field": "modifier|diagnosis_code|procedure_code|documentation",
      "suggestion": "Specific actionable correction with policy reference",
      "confidence": 0.85,
      "policy_reference": "Manual Chapter Section",
      "implementation_guidance": "Step-by-step instructions"
    }}
  ],
  "policy_references": [
    "Medicare Claims Processing Manual Ch.12 20.4.5",
    "LCD L34696  Fracture and Bone Imaging Coverage"
  ],
  "final_guidance": "Summary of how this claim can be fixed to pass",
  "compliance_checklist": [
    "Action item 1",
    "Action item 2"
  ]
}}

FOCUS: Find HOW to fix the claim to make it compliant with CMS policy.
"""

# -------------------------------------------------------------------------
# TWO-STAGE CLAIM CORRECTOR WITH CALIBRATED STAGE 1
# -------------------------------------------------------------------------

class TwoStageCalibratedClaimCorrector:
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
        self.claims_collection = "claim_analysis_metadata"
        
        #  Source mapping for policy identification - EXACT SAME AS STANDALONE
        self.source_mapping = {
            "clm104c": "Medicare Claims Processing Manual",
            "pim83c": "Program Integrity Manual (Administrative Only)", 
            "ncci": "National Correct Coding Initiative",
            "lcd": "Local Coverage Determination",
            "mcm": "Medicare Claims Manual",
            "bpm": "Medicare Benefit Policy Manual"
        }

    def run_two_stage_corrections(self, claim_id: str) -> Dict[str, Any]:
        """Run both calibrated denial reasoning and corrective reasoning stages"""
        print(f"\n TWO-STAGE CALIBRATED PROCESSING: {claim_id}")
        
        # Get claim issues
        issues = self._get_claim_issues(claim_id)
        if not issues:
            return {"claim_id": claim_id, "enriched_issues": []}

        enriched_issues = []
        for issue in issues:
            print(f"\n Processing issue: {issue.get('hcpcs_code', 'N/A')} + {issue.get('icd10_code', 'N/A')}")
            
            #  Get CPT description
            cpt_code = issue.get('hcpcs_code', '')
            if cpt_code:
                dynamic_procedure_name = get_cpt_description(cpt_code)
                issue['procedure_name'] = dynamic_procedure_name
                print(f"    Updated procedure name: {dynamic_procedure_name}")
            
            #  STAGE 1: Calibrated Denial Reasoning Pass
            print(f"    STAGE 1: Calibrated denial reasoning analysis...")
            stage1_result = self._stage1_calibrated_denial_reasoning(issue)
            
            #  STAGE 2: Corrective Reasoning Pass
            print(f"    STAGE 2: Finding corrective solutions...")
            stage2_result = self._stage2_corrective_reasoning(issue, stage1_result)
            
            # Combine results
            enriched_issue = {
                **issue,
                "stage1_calibrated_denial_analysis": stage1_result,
                "stage2_correction_analysis": stage2_result,
                "two_stage_calibrated_complete": True
            }
            enriched_issues.append(enriched_issue)

        return {"claim_id": claim_id, "enriched_issues": enriched_issues}

    # ---------------------------------------------------------------------
    # STAGE 1: CALIBRATED DENIAL REASONING (from calibrated version)
    # ---------------------------------------------------------------------
    def _stage1_calibrated_denial_reasoning(self, issue: Dict[str, Any]) -> Dict[str, Any]:
        """Stage 1: Calibrated denial reasoning using enhanced validation"""
        # Get policies for denial reasoning
        all_policies = []
        for collection in self.policy_collections:
            policies = self._hybrid_search(collection, issue, top_k=3)
            all_policies.extend(policies)
        
        # Validate and deduplicate policies using calibrated logic
        validated_policies = self._calibrated_validate_and_deduplicate_policies(all_policies, issue)
        print(f"    Stage 1: Retrieved {len(validated_policies)} calibrated policies for denial analysis")
        
        # Run Stage 1 calibrated LLM analysis
        stage1_analysis = self._run_calibrated_stage1_llm(issue, validated_policies)
        
        return {
            "policies_analyzed": validated_policies,
            "denial_analysis": stage1_analysis,
            "stage": "calibrated_denial_reasoning"
        }

    # ---------------------------------------------------------------------
    # STAGE 2: CORRECTIVE REASONING
    # ---------------------------------------------------------------------
    def _stage2_corrective_reasoning(self, issue: Dict[str, Any], stage1_result: Dict[str, Any]) -> Dict[str, Any]:
        """Stage 2: Find how to fix the claim"""
        # Extract keywords from Stage 1 calibrated denial analysis
        denial_keywords = self._extract_correction_keywords(stage1_result, issue)
        
        # Re-query Qdrant with correction-focused intent
        correction_policies = self._search_correction_policies(denial_keywords, issue)
        print(f"    Stage 2: Retrieved {len(correction_policies)} policies for correction analysis")
        
        # Run Stage 2 LLM analysis
        stage2_analysis = self._run_stage2_llm(issue, stage1_result, correction_policies)
        
        return {
            "correction_policies": correction_policies,
            "correction_analysis": stage2_analysis,
            "stage": "corrective_reasoning"
        }

    # ---------------------------------------------------------------------
    # CALIBRATED POLICY VALIDATION (from calibrated version)
    # ---------------------------------------------------------------------
    def _calibrated_validate_and_deduplicate_policies(self, policies: List[Any], issue: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Calibrated policy validation with enhanced relevance checks - EXACT SAME AS STANDALONE VERSION"""
        if not policies:
            return []
        
        # Convert ScoredPoint objects to dictionaries
        policy_dicts = []
        for policy in policies:
            if hasattr(policy, 'payload'):
                policy_dict = policy.payload.copy()
                policy_dict['score'] = policy.score
                policy_dicts.append(policy_dict)
            elif isinstance(policy, dict):
                policy_dicts.append(policy)
        
        # Validate policy relevance using calibrated logic - EXACT SAME AS STANDALONE
        validated_policies = []
        for policy in policy_dicts:
            validation_result = self._validate_policy_relevance(policy, issue)
            if validation_result.get('is_relevant', False):
                # Add validation metadata
                policy.update({
                    'validation_status': validation_result.get('validation_status', 'UNKNOWN'),
                    'relevance_reason': validation_result.get('relevance_reason', ''),
                    'manual_appropriate': validation_result.get('manual_appropriate', True)
                })
                validated_policies.append(policy)
        
        # Deduplicate
        return self._deduplicate_policies(validated_policies)

    def _validate_policy_relevance(self, policy: Dict[str, Any], issue: Dict[str, Any]) -> Dict[str, Any]:
        """Validate if policy is relevant to the claim with more flexible criteria - EXACT COPY FROM STANDALONE"""
        cpt_code = issue.get('hcpcs_code', '')
        icd_code = issue.get('icd10_code', '')
        denial_reason = issue.get('ptp_denial_reason', '')
        source_file = policy.get('source', '')
        
        #  Check if policy mentions the CPT/ICD codes (use 'text' field, not 'excerpt')
        policy_text = policy.get('text', '').lower()
        mentions_cpt = cpt_code.lower() in policy_text if cpt_code else False
        mentions_icd = icd_code.lower() in policy_text if icd_code else False
        
        #  Check for broader relevance indicators - more flexible
        relevance_keywords = []
        if 'ptp' in denial_reason.lower():
            relevance_keywords.extend(['ptp', 'procedure', 'bundling', 'ncci', 'edit', 'coding'])
        if 'coding' in denial_reason.lower():
            relevance_keywords.extend(['coding', 'cpt', 'hcpcs', 'procedure', 'medical'])
        if 'coverage' in denial_reason.lower():
            relevance_keywords.extend(['coverage', 'lcd', 'determination', 'medical'])
        if 'definition' in denial_reason.lower():
            relevance_keywords.extend(['definition', 'coding', 'procedure', 'medical'])
        
        mentions_relevance_keywords = any(keyword in policy_text for keyword in relevance_keywords)
        
        #  Check manual type appropriateness
        manual_appropriate = self._check_manual_appropriateness(source_file, denial_reason)
        
        #  More flexible relevance - allow general medical policies
        general_medical_keywords = ['medical', 'procedure', 'service', 'coding', 'billing', 'claim']
        mentions_general = any(keyword in policy_text for keyword in general_medical_keywords)
        
        #  More flexible relevance determination
        if mentions_cpt or mentions_icd:
            return {
                'is_relevant': True,
                'validation_status': 'PASS',
                'relevance_reason': f'Policy mentions CPT/ICD codes directly',
                'manual_appropriate': manual_appropriate
            }
        elif mentions_relevance_keywords:
            return {
                'is_relevant': True,
                'validation_status': 'PASS',
                'relevance_reason': f'Policy mentions relevant keywords for {denial_reason}',
                'manual_appropriate': manual_appropriate
            }
        elif mentions_general and len(policy_text) > 200:  # Substantial content
            return {
                'is_relevant': True,
                'validation_status': 'PASS',
                'relevance_reason': f'Policy contains general medical content relevant to claims processing',
                'manual_appropriate': manual_appropriate
            }
        else:
            return {
                'is_relevant': False,
                'validation_status': 'FAIL',
                'relevance_reason': f'Policy does not contain relevant medical or coding content',
                'manual_appropriate': manual_appropriate
            }

    def _check_manual_appropriateness(self, source_file: str, denial_reason: str) -> bool:
        """Check if manual type is appropriate for the denial reason - EXACT COPY FROM STANDALONE"""
        if not source_file or not denial_reason:
            return True  # Default to allow if unclear
        
        source_lower = source_file.lower()
        denial_lower = denial_reason.lower()
        
        #  Restrict pim* to administrative issues only
        if source_lower.startswith('pim'):
            return 'administrative' in denial_lower or 'integrity' in denial_lower
        
        #  Prefer clm104* for coding conflicts
        if source_lower.startswith('clm104'):
            return any(keyword in denial_lower for keyword in ['coding', 'procedure', 'definition', 'ptp', 'conflict'])
        
        #  Prefer ncci* for bundling/PTP issues
        if source_lower.startswith('ncci'):
            return any(keyword in denial_lower for keyword in ['ptp', 'bundling', 'conflict', 'ncci'])
        
        #  Allow lcd* for coverage issues
        if source_lower.startswith('lcd'):
            return any(keyword in denial_lower for keyword in ['coverage', 'determination', 'local'])
        
        return True  # Default to allow other manuals

    def _identify_policy_source(self, source_file: str) -> str:
        """Identify policy manual based on source file name - EXACT COPY FROM STANDALONE"""
        if not source_file:
            return "Unknown Source"
        
        source_lower = source_file.lower()
        for prefix, manual_name in self.source_mapping.items():
            if source_lower.startswith(prefix):
                return manual_name
        
        return f"Policy Manual ({source_file})"

    # ---------------------------------------------------------------------
    # KEYWORD EXTRACTION FOR STAGE 2
    # ---------------------------------------------------------------------
    def _extract_correction_keywords(self, stage1_result: Dict[str, Any], issue: Dict[str, Any]) -> List[str]:
        """Extract keywords for correction-focused search from calibrated Stage 1"""
        denial_analysis = stage1_result.get("denial_analysis", {})
        final_summary = denial_analysis.get("final_reasoning_summary", "")
        denial_reason = issue.get("ptp_denial_reason", "")
        
        # Base keywords from denial reason
        correction_keywords = []
        
        if "ptp" in denial_reason.lower():
            correction_keywords.extend(["modifier", "XU", "59", "separately reportable", "bundling exception"])
        if "mue" in denial_reason.lower():
            correction_keywords.extend(["MUE", "unit limit", "threshold exception", "maximum units"])
        if "coverage" in denial_reason.lower():
            correction_keywords.extend(["covered diagnosis", "covered group", "ICD10", "LCD", "covered when"])
        if "definition" in denial_reason.lower():
            correction_keywords.extend(["procedure definition", "coding guidance", "documentation requirements"])
        
        # Add keywords from final summary
        summary_keywords = re.findall(r'\b[A-Z]{2,}\b|\b\w{3,}\b', final_summary)
        correction_keywords.extend([kw.lower() for kw in summary_keywords[:5]])
        
        # Remove duplicates and limit
        return list(set(correction_keywords))[:10]

    # ---------------------------------------------------------------------
    # CORRECTION-FOCUSED POLICY SEARCH
    # ---------------------------------------------------------------------
    def _search_correction_policies(self, keywords: List[str], issue: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Search for policies focused on corrections and allowances"""
        correction_policies = []
        
        # Create correction-focused query
        cpt_code = issue.get('hcpcs_code', '')
        icd_code = issue.get('icd10_code', '')
        keyword_str = " ".join(keywords)
        
        query_text = (
            f"CMS policy corrections allowances exceptions for CPT {cpt_code} ICD {icd_code} "
            f"modifier exceptions covered variants allowable combinations {keyword_str}"
        )
        
        query_vector = self.embedder.encode(query_text).tolist()
        
        # Search all collections for correction-focused policies
        for collection in self.policy_collections:
            try:
                hits = self.client.query_points(
                    collection_name=collection,
                    query=query_vector,
                    limit=3,
                    with_payload=True,
                    with_vectors=False,
                ).points
                
                # Convert to dictionaries
                for hit in hits:
                    policy_dict = hit.payload.copy()
                    policy_dict['score'] = hit.score
                    policy_dict['collection'] = collection
                    correction_policies.append(policy_dict)
                    
            except Exception as e:
                print(f" Correction search failed for {collection}: {e}")
        
        # Sort by score and deduplicate
        correction_policies.sort(key=lambda x: x.get('score', 0), reverse=True)
        return self._deduplicate_policies(correction_policies)[:8]

    # ---------------------------------------------------------------------
    # LLM PROCESSING FOR BOTH STAGES
    # ---------------------------------------------------------------------
    def _run_calibrated_stage1_llm(self, issue: Dict[str, Any], policies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run Stage 1 calibrated LLM for denial reasoning - EXACT SAME AS STANDALONE VERSION"""
        try:
            # Format policies with enhanced validation information - EXACT SAME AS STANDALONE
            policy_excerpts = ""
            for i, policy in enumerate(policies, 1):
                source_file = policy.get('source', 'unknown.pdf')
                manual_name = self._identify_policy_source(source_file)
                validation_status = policy.get('validation_status', 'UNKNOWN')
                
                policy_excerpts += f"\nPOLICY {i}:\n"
                policy_excerpts += f"Source File: {source_file}\n"
                policy_excerpts += f"Manual: {manual_name}\n"
                policy_excerpts += f"Validation: {validation_status}\n"
                policy_excerpts += f"Chapter: {policy.get('chapter', 'N/A')}\n"
                policy_excerpts += f"Section: {policy.get('section', 'N/A')}\n"
                policy_excerpts += f"Revision: {policy.get('rev', 'N/A')}\n"
                policy_excerpts += f"Page: {policy.get('page', 'N/A')}\n"
                policy_excerpts += f"Retrieval Score: {policy.get('score', 0.0):.4f}\n"
                policy_excerpts += f"Text: {policy.get('text', '')[:500]}...\n"
            
            # Format the calibrated prompt with exact claim data - EXACT SAME AS STANDALONE
            prompt = CALIBRATED_STAGE1_PROMPT.format(
                hcpcs_code=issue.get('hcpcs_code', 'N/A'),
                procedure_name=issue.get('procedure_name', 'N/A'),
                icd10_code=issue.get('icd10_code', 'N/A'),
                diagnosis_name=issue.get('diagnosis_name', 'N/A'),
                denial_reason=issue.get('ptp_denial_reason', 'N/A'),
                denial_risk_level=issue.get('denial_risk_level', 'N/A'),
                action_required=issue.get('action_required', 'N/A'),
                policy_excerpts=policy_excerpts
            )
            
            # Run LLM
            cmd = ["ollama", "run", "mistral", prompt]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                return {"error": f"Calibrated Stage 1 LLM failed: {result.stderr}"}
            
            # Parse JSON response
            llm_output = result.stdout.strip()
            try:
                json_start = llm_output.find('{')
                json_end = llm_output.rfind('}') + 1
                
                if json_start >= 0 and json_end > json_start:
                    json_str = llm_output[json_start:json_end]
                    return json.loads(json_str)
                else:
                    return {"summary": llm_output, "error": "No valid JSON found"}
                    
            except json.JSONDecodeError as e:
                return {"summary": llm_output, "error": f"JSON parsing failed: {e}"}
                
        except Exception as e:
            print(f" Calibrated Stage 1 LLM failed: {e}")
            return {"error": f"Calibrated Stage 1 processing failed: {e}"}

    def _run_stage2_llm(self, issue: Dict[str, Any], stage1_result: Dict[str, Any], correction_policies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run Stage 2 LLM for corrective reasoning"""
        try:
            # Format Stage 1 calibrated analysis
            denial_analysis = stage1_result.get("denial_analysis", {})
            denial_summary = json.dumps(denial_analysis, indent=2)
            
            # Format correction policies
            correction_policies_text = ""
            for i, policy in enumerate(correction_policies, 1):
                correction_policies_text += f"\nCORRECTION POLICY {i}:\n"
                correction_policies_text += f"Source: {policy.get('source', 'N/A')}\n"
                correction_policies_text += f"Collection: {policy.get('collection', 'N/A')}\n"
                correction_policies_text += f"Text: {policy.get('text', '')[:400]}...\n"
            
            # Format Stage 2 prompt
            prompt = STAGE2_CORRECTION_PROMPT.format(
                claim_id=issue.get('claim_id', 'N/A'),
                hcpcs_code=issue.get('hcpcs_code', 'N/A'),
                procedure_name=issue.get('procedure_name', 'N/A'),
                icd10_code=issue.get('icd10_code', 'N/A'),
                diagnosis_name=issue.get('diagnosis_name', 'N/A'),
                denial_reason=issue.get('ptp_denial_reason', 'N/A'),
                denial_analysis=denial_summary,
                correction_policies=correction_policies_text
            )
            
            # Run LLM
            cmd = ["ollama", "run", "mistral", prompt]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                return {"error": f"Stage 2 LLM failed: {result.stderr}"}
            
            # Parse JSON response
            llm_output = result.stdout.strip()
            try:
                json_start = llm_output.find('{')
                json_end = llm_output.rfind('}') + 1
                
                if json_start >= 0 and json_end > json_start:
                    json_str = llm_output[json_start:json_end]
                    return json.loads(json_str)
                else:
                    return {"summary": llm_output, "error": "No valid JSON found"}
                    
            except json.JSONDecodeError as e:
                return {"summary": llm_output, "error": f"JSON parsing failed: {e}"}
                
        except Exception as e:
            print(f" Stage 2 LLM failed: {e}")
            return {"error": f"Stage 2 processing failed: {e}"}

    # ---------------------------------------------------------------------
    # HELPER METHODS (from calibrated version)
    # ---------------------------------------------------------------------
    def _get_claim_issues(self, claim_id: str) -> List[Dict[str, Any]]:
        """Get claim issues from the claims collection"""
        try:
            hits = self.client.query_points(
                collection_name=self.claims_collection,
                query=[0] * 768,  # Dummy vector
                query_filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="claim_id",
                            match=models.MatchValue(value=claim_id)
                        )
                    ]
                ),
                limit=100,
                with_payload=True,
                with_vectors=False,
            ).points
            
            issues = []
            for hit in hits:
                if hit.payload:
                    issues.append(hit.payload)
            
            print(f"    Found {len(issues)} claim issues")
            return issues
            
        except Exception as e:
            print(f" Failed to get claim issues: {e}")
            return []

    def _hybrid_search(self, collection: str, issue: Dict[str, Any], top_k: int = 5):
        """Hybrid search with proper array matching"""
        try:
            icd_code = issue.get("icd10_code") or issue.get("icd9_code")
            hcpcs_code = issue.get("hcpcs_code") or issue.get("cpt_code")
            denial_reason = issue.get("ptp_denial_reason", "unspecified")

            query_text = (
                f"CMS policy for CPT/HCPCS {hcpcs_code}, diagnosis {icd_code}, "
                f"denial reason {denial_reason}. Include NCCI, LCD, and CMS manual sections."
            )
            query_vector = self.embedder.encode(query_text).tolist()

            # Try strict code matching first
            strict_filter = models.Filter(
                should=[
                    models.FieldCondition(
                        key="cpt_codes",
                        match=models.MatchAny(any=[str(hcpcs_code).upper()])
                    ) if hcpcs_code else None,
                    models.FieldCondition(
                        key="hcpcs_codes",
                        match=models.MatchAny(any=[str(hcpcs_code).upper()])
                    ) if hcpcs_code else None,
                    models.FieldCondition(
                        key="icd10_codes",
                        match=models.MatchAny(any=[str(icd_code).upper().replace(".", "")])
                    ) if icd_code else None,
                    models.FieldCondition(
                        key="text",
                        match=models.MatchText(text=str(hcpcs_code))
                    ) if hcpcs_code else None,
                    models.FieldCondition(
                        key="text",
                        match=models.MatchText(text=str(icd_code))
                    ) if icd_code else None,
                ]
            )

            strict_filter.should = [f for f in strict_filter.should if f is not None]

            hits = self.client.query_points(
                collection_name=collection,
                query=query_vector,
                query_filter=strict_filter if strict_filter.should else None,
                limit=top_k,
                with_payload=True,
                with_vectors=False,
            ).points

            # Fallback to semantic search if no strict matches
            if not hits or len(hits) == 0:
                print(f"    No strict matches for {hcpcs_code}/{icd_code}, falling back to semantic search...")
                hits = self.client.query_points(
                    collection_name=collection,
                    query=query_vector,
                    query_filter=None,
                    limit=top_k,
                    with_payload=True,
                    with_vectors=False,
                ).points

            return hits or []
        except Exception as e:
            print(f" Search failed for {collection}: {e}")
            return []

    def _deduplicate_policies(self, policies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate policies"""
        seen_excerpts = {}
        deduplicated = []
        
        for policy in policies:
            excerpt_key = policy.get('text', '')[:200]
            
            if excerpt_key not in seen_excerpts:
                seen_excerpts[excerpt_key] = policy
                deduplicated.append(policy)
        
        return deduplicated


# ---------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------
if __name__ == "__main__":
    corrector = TwoStageCalibratedClaimCorrector()
    claim_id = "123456789012345"
    enriched = corrector.run_two_stage_corrections(claim_id)
    print(json.dumps(enriched, indent=2))



