#!/usr/bin/env python3
"""
Enhanced Archetype-Driven Claim Corrector v3.0
==============================================

Key updates vs v2:
- ICD version–aware SQL for DX-driven archetypes (ICD-10 or ICD-9)
- GEMs-based fallback mapping when direct version returns 0 rows
- Adds Secondary_DX_Not_Covered definition explicitly
"""

import json
import logging
import warnings
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from claim_corrector_claims3_calibrated import CalibratedClaimCorrector

try:
    import pyodbc
    import pandas as pd
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, VectorParams, PointStruct
    from sentence_transformers import SentenceTransformer
    import ollama
    import numpy as np
except ImportError as e:
    print(f" Missing required library: {e}")
    sys.exit(1)

# Suppress pandas SQLAlchemy warning for pyodbc connections
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*', category=UserWarning)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Archetype Definitions (v3)
# -----------------------------------------------------------------------------

ARCHETYPE_DEFINITIONS = {
    "NCCI_PTP_Conflict": {
        "description": "The CPT/HCPCS combination violates an NCCI Procedure-to-Procedure (PTP) rule.",
        "qdrant_collections": ["claims__ncci_edits", "claims__med_claims_policies"],
        "sql_query": """
            SELECT 
                ncci.procedure_code,
                ncci.ptp_denial_reason,
                ncci.instructions,
                ncci.ptp_edit_type,
                ncci.modifier_status,
                ncci.mue_threshold,
                ncci.mue_denial_type
            FROM [_ncci_].[dbo].[vw_NCCI_Daily_Denial_Alerts] ncci
            WHERE ncci.procedure_code = ?
              AND (ncci.ptp_denial_reason IS NOT NULL OR ncci.mue_threshold IS NOT NULL)
        """,
    },

    # DX-driven archetypes use templated WHERE clause decided at runtime
    "Primary_DX_Not_Covered": {
        "description": "Primary diagnosis is not covered under the relevant LCD or NCD.",
        "qdrant_collections": ["claims__lcd_policies", "claims__ncd_policies"],
        "sql_query": """
            SELECT 
                g.icd9_code,
                g.icd10_code,
                l.lcd_id,
                l.title as lcd_title,
                l.coverage_group_description as coverage_group,
                l.lcd_last_updated as last_updated,
                l.covered_codes,
                l.diagnoses_support,
                l.diagnoses_dont_support
            FROM [_gems].[dbo].[table_2018_I9gem_fixed] g
            LEFT JOIN [_lcd].[dbo].[vw_Master_Denial_Analysis] l
                ON g.icd10_code = l.covered_codes
            WHERE {DX_WHERE}
        """,
    },

    "Secondary_DX_Not_Covered": {
        "description": "Secondary diagnosis is not covered under the relevant LCD or NCD.",
        "qdrant_collections": ["claims__lcd_policies", "claims__ncd_policies"],
        "sql_query": """
            SELECT 
                g.icd9_code,
                g.icd10_code,
                l.lcd_id,
                l.title as lcd_title,
                l.coverage_group_description as coverage_group,
                l.lcd_last_updated as last_updated,
                l.covered_codes,
                l.diagnoses_support,
                l.diagnoses_dont_support
            FROM [_gems].[dbo].[table_2018_I9gem_fixed] g
            LEFT JOIN [_lcd].[dbo].[vw_Master_Denial_Analysis] l
                ON g.icd10_code = l.covered_codes
            WHERE {DX_WHERE}
        """,
    },

    "MUE_Risk": {
        "description": "Billed units exceed the Medically Unlikely Edit (MUE) threshold.",
        "qdrant_collections": ["claims__ncci_edits"],
        "sql_query": """
            SELECT 
                ncci.procedure_code,
                ncci.mue_threshold,
                ncci.mue_denial_type,
                ncci.mue_rationale,
                ncci.instructions
            FROM [_ncci_].[dbo].[vw_NCCI_Daily_Denial_Alerts] ncci
            WHERE ncci.procedure_code = ?
              AND ncci.mue_threshold IS NOT NULL
        """,
    },

    "Bundled_Payment_Conflict": {
        "description": "Claim includes services bundled under DRG/APC or OPPS rules.",
        "qdrant_collections": ["claims__ncci_edits", "claims__med_claims_policies"],
        "sql_query": """
            SELECT 
                ptp.primary_cpt_hcpcs_code,
                ptp.secondary_cpt_hcpcs_code,
                ptp.edit_type,
                ptp.ptp_edit_rationale,
                ptp.modifier_guidance,
                ptp.effective_date
            FROM [_ncci_].[dbo].[vw_NCCI_Modifier_Compliance] ptp
            WHERE ptp.secondary_cpt_hcpcs_code = ?
              AND ptp.edit_type IN ('Bundled', 'Component')
        """,
    },
}


class SQLDatabaseConnector:
    def __init__(self):
        self.connection = None
        self._connect()

    def _connect(self):
        try:
            cs = (
                'Driver={ODBC Driver 18 for SQL Server};'
                'Server=localhost,1433;UID=SA;PWD=Bbanwo@1980!;'
                'Database=_claims;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;'
            )
            self.connection = pyodbc.connect(cs)
            print(" SQL Database connection established successfully")
        except Exception as e:
            print(f" SQL Database connection failed: {e}")
            self.connection = None

    def _map_icd10_to_icd9(self, icd10: str) -> List[str]:
        try:
            q = """
                SELECT DISTINCT icd9_code
                FROM [_gems].[dbo].[table_2018_I9gem_fixed]
                WHERE icd10_code = ?
            """
            df = pd.read_sql(q, self.connection, params=[icd10])
            return [r.icd9_code for _, r in df.iterrows()]
        except Exception:
            return []

    def _map_icd9_to_icd10(self, icd9: str) -> List[str]:
        try:
            q = """
                SELECT DISTINCT icd10_code
                FROM [_gems].[dbo].[table_2018_I9gem_fixed]
                WHERE icd9_code = ?
            """
            df = pd.read_sql(q, self.connection, params=[icd9])
            return [r.icd10_code for _, r in df.iterrows()]
        except Exception:
            return []

    def execute_archetype_query(self, archetype: str, codes: Dict[str, str]) -> List[Dict[str, Any]]:
        if not self.connection:
            return []

        info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        base_sql = info.get('sql_query', '')
        if not base_sql:
            return []

        try:
            # HCPCS-driven archetypes
            if archetype in ["NCCI_PTP_Conflict", "MUE_Risk", "Bundled_Payment_Conflict"]:
                hcpcs = codes.get('hcpcs_code')
                if not hcpcs:
                    return []
                df = pd.read_sql(base_sql, self.connection, params=[hcpcs])
                return df.to_dict('records')

            # DX-driven archetypes with ICD version awareness
            if archetype in ["Primary_DX_Not_Covered", "Secondary_DX_Not_Covered"]:
                icd10 = codes.get('icd10_code') or ''
                icd9 = codes.get('icd9_code') or ''

                results: List[Dict[str, Any]] = []

                def run_dx_query(dx_where: str, param: str) -> List[Dict[str, Any]]:
                    sql = base_sql.replace('{DX_WHERE}', dx_where)
                    df_local = pd.read_sql(sql, self.connection, params=[param])
                    return df_local.to_dict('records')

                # Prefer ICD-10 if provided (starts with a letter)
                tried = []
                if icd10 and icd10[:1].isalpha():
                    tried.append(('g.icd10_code = ?', icd10))
                if icd9 and not icd9[:1].isalpha():
                    tried.append(('g.icd9_code = ?', icd9))

                # If no clear version, still attempt both in order icd10 -> icd9 using available values
                if not tried:
                    if icd10:
                        tried.append(('g.icd10_code = ?', icd10))
                    if icd9:
                        tried.append(('g.icd9_code = ?', icd9))

                # Execute attempts
                for dx_where, param in tried:
                    rows = run_dx_query(dx_where, param)
                    if rows:
                        results.extend(rows)
                        break

                # Fallback via GEMs mapping if empty
                if not results:
                    if icd10:
                        mapped_icd9 = self._map_icd10_to_icd9(icd10)
                        for m in mapped_icd9:
                            rows = run_dx_query('g.icd9_code = ?', m)
                            if rows:
                                results.extend(rows)
                                break
                    if not results and icd9:
                        mapped_icd10 = self._map_icd9_to_icd10(icd9)
                        for m in mapped_icd10:
                            rows = run_dx_query('g.icd10_code = ?', m)
                            if rows:
                                results.extend(rows)
                                break

                return results

            return []
        except Exception as e:
            print(f" SQL query failed for archetype '{archetype}': {e}")
            return []

    def close(self):
        if self.connection:
            self.connection.close()


class ArchetypeDrivenClaimCorrectorV3:
    def __init__(self):
        print(" Initializing Enhanced Archetype-Driven Claim Corrector v3.0...")
        self.calibrated_corrector = CalibratedClaimCorrector()
        self.sql_connector = SQLDatabaseConnector()
        self.qdrant_client = None
        self.embedder = None
        self._init_qdrant()
        self._init_embedder()
        if hasattr(self.calibrated_corrector, 'embedder'):
            self.embedder = self.calibrated_corrector.embedder
        print(" Enhanced Archetype-Driven Claim Corrector v3.0 initialized successfully")

    def _init_qdrant(self):
        try:
            self.qdrant_client = QdrantClient(host="localhost", port=6333)
            print(" Qdrant client connected successfully")
        except Exception as e:
            print(f" Qdrant initialization failed: {e}")
            self.qdrant_client = None

    def _init_embedder(self):
        try:
            self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
            print(" Sentence transformer model loaded successfully")
        except Exception as e:
            print(f" Embedder initialization failed: {e}")
            self.embedder = None

    def _detect_archetype(self, issue: Dict[str, Any]) -> str:
        if issue.get('ptp_denial_reason') and issue.get('hcpcs_position') == 1:
            return "NCCI_PTP_Conflict"
        if issue.get('mue_threshold'):
            return "MUE_Risk"
        if issue.get('lcd_icd10_covered_group') == 'N' and issue.get('dx_position') == 1:
            return "Primary_DX_Not_Covered"
        if issue.get('lcd_icd10_covered_group') == 'N' and issue.get('dx_position') > 1:
            return "Secondary_DX_Not_Covered"
        return "NCCI_PTP_Conflict" if issue.get('ptp_denial_reason') else "MUE_Risk"

    def _build_query_text(self, issue: Dict[str, Any], archetype: str) -> str:
        parts = [f"CPT {issue.get('hcpcs_code','')}"]
        if issue.get('icd10_code'):
            parts.append(f"ICD {issue.get('icd10_code')}")
        if issue.get('icd9_code'):
            parts.append(f"ICD {issue.get('icd9_code')}")
        if archetype == "NCCI_PTP_Conflict":
            parts.extend(["NCCI","PTP","modifier"]) 
        elif archetype.endswith("Not_Covered"):
            parts.extend(["LCD","coverage","diagnosis"]) 
        elif archetype == "MUE_Risk":
            parts.extend(["MUE","units","threshold"]) 
        return " ".join(parts)

    def _search_policies(self, issue: Dict[str, Any], archetype: str) -> List[Dict[str, Any]]:
        if not self.qdrant_client or not self.embedder:
            return []
        colls = ARCHETYPE_DEFINITIONS.get(archetype, {}).get('qdrant_collections', [])
        vector = self.embedder.encode(self._build_query_text(issue, archetype)).tolist()
        results: List[Dict[str, Any]] = []
        try:
            all_colls = [c.name for c in self.qdrant_client.get_collections().collections]
            for c in colls:
                if c not in all_colls:
                    print(f"    Collection {c} does not exist, skipping")
                    continue
                hits = self.qdrant_client.search(collection_name=c, query_vector=vector, limit=3, score_threshold=0.7)
                for h in hits:
                    p = h.payload or {}
                    p['score'] = h.score
                    p['collection'] = c
                    results.append(p)
        except Exception as e:
            print(f"    Policy search failed: {e}")
        results.sort(key=lambda x: x.get('score',0), reverse=True)
        return results[:5]

    def _run_stage2_llm(self, issue: Dict[str, Any], archetype: str, sql_evidence: List[Dict[str, Any]], policies: List[Dict[str, Any]]) -> Dict[str, Any]:
        # Extract codes and identify types
        dx_code = issue.get('icd10_code') or issue.get('icd9_code') or ''
        proc_code = issue.get('hcpcs_code') or ''
        
        # Build code type information
        code_info = f"""
CODE IDENTIFICATION:
- PROCEDURE CODE (CPT/HCPCS): {proc_code} → Use for: modifier checks, PTP edits, MUE review, prior auth, bundling
- DIAGNOSIS CODE (ICD-10/ICD-9): {dx_code} → Use for: coverage validation, medical necessity, LCD/NCD compliance

CRITICAL RULES:
1. Modifiers (59, 25, XE, XS, XP, XU) apply ONLY to PROCEDURE codes, NEVER to DIAGNOSIS codes
2. If a code starts with a letter and is ≤7 chars (e.g., M16.11, Z99.89), it is ICD - do NOT assign modifiers
3. Only 5-digit numeric or alphanumeric codes (e.g., 27130, G0299) are procedures that can have modifiers
"""
        
        # Create a structured prompt for correction recommendations
        prompt = f"""
You are a CMS-compliant claim denial correction expert with deep knowledge of medical billing.

{code_info}

ARCHETYPE: {archetype}
RISK LEVEL: {issue.get('denial_risk_level', '')}
ACTION REQUIRED: {issue.get('action_required', '')}

SQL EVIDENCE (Historical Claims Data):
{json.dumps(sql_evidence[:3], indent=2) if sql_evidence else "No SQL evidence available"}

POLICY EXCERPTS (CMS Guidelines):
{json.dumps([{k: v for k, v in p.items() if k in ['title', 'text', 'source', 'policy_number']} for p in policies[:3]], indent=2) if policies else "No policies found"}

COMPLIANCE REQUIREMENTS:
- Only recommend actions a PROVIDER or CODER can perform on their claim
- NEVER instruct users to "revise," "update," or "modify" CMS policy documents (LCDs, NCDs, Articles)
- Providers can only COMPLY with policies, not change them
- Be SPECIFIC: reference exact claim fields, documentation requirements, or SQL queries
- Separate diagnosis logic from procedure logic:
  * Diagnosis ({dx_code}): coverage verification, medical necessity documentation
  * Procedure ({proc_code}): modifier application, bundling checks, prior authorization
- YOU MUST GENERATE AT LEAST ONE ACTIONABLE RECOMMENDATION - this is a denial risk that needs resolution!

REQUIRED OUTPUT FORMAT (valid JSON only):
{{
  "claim_id": "{issue.get('claim_id', '')}",
  "archetype": "{archetype}",
  "sql_evidence_summary": "Summary of what the SQL database evidence shows (denial rates, common issues, patterns)",
  "recommended_corrections": [
    {{
      "field": "specific_claim_field_name (e.g., 'procedure_modifier', 'primary_diagnosis_position', 'units_billed')",
      "current_value": "what is currently on the claim",
      "suggested_value": "exact new value to enter or add",
      "rationale": "why this change is needed based on CMS policy",
      "policy_citation": "specific LCD/NCD/NCCI edit number and section (e.g., 'LCD L12345 §3.2, NCCI PTP edit 27130-27447')",
      "implementation_steps": [
        "Step 1: Specific action with exact field/form location (e.g., 'In claim line 2, locate the Modifier field')",
        "Step 2: Specific action with exact value (e.g., 'Enter modifier 59 to indicate distinct procedural service')",
        "Step 3: Specific verification (e.g., 'Verify operative note documents separate incision site')"
      ],
      "confidence": 0.85
    }}
  ],
  "policy_references": ["LCD L12345", "NCCI PTP Manual Ch.2"],
  "final_guidance": "Overall strategy to resolve this denial risk",
  "compliance_checklist": ["Verification step 1 with specific requirement", "Verification step 2 with specific requirement"]
}}

EXAMPLES OF GOOD CORRECTIONS:
✓ Field: "procedure_modifier" | Current: "none" | Suggested: "59" | Rationale: "NCCI PTP edit requires modifier 59 to bypass bundling" | Steps: ["Open claim line for CPT {proc_code}", "Add modifier 59 to indicate distinct procedural service", "Attach operative note documenting separate incision site"]
✓ Field: "primary_diagnosis" | Current: "{dx_code}" | Suggested: "Add covered diagnosis from LCD L33822" | Rationale: "{dx_code} is not in LCD covered codes list" | Steps: ["Review LCD L33822 Table 1", "Select appropriate covered diagnosis", "Reorder diagnoses with covered code first"]

EXAMPLES OF BAD CORRECTIONS (AVOID):
✗ "Revise the LCD to include {dx_code}" → Providers cannot modify LCDs, only comply with them
✗ "Remove modifier from {dx_code}" → Cannot apply modifiers to diagnoses (only to procedures)
✗ "Review medical records" → Too vague; specify what documentation to look for

Focus on clear, specific, implementable actions that will get the claim paid.
"""
        
        try:
            response = ollama.generate(
                model="mistral", 
                prompt=prompt,
                options={
                    "num_predict": 4096,  # Increased from 2048 to allow longer responses
                    "temperature": 0.3,
                    "top_p": 0.85
                }
            )
            text = response.get('response','{}')
            
            # Debug: Log raw LLM response
            print(f"\n{'='*80}")
            print(f"RAW LLM RESPONSE for {proc_code} + {dx_code} (Archetype: {archetype}):")
            print(f"{'='*80}")
            print(text[:2000])  # First 2000 chars (increased from 1000)
            print(f"{'='*80}\n")
            
            try:
                parsed = json.loads(text)
                
                # Debug: Log how many recommendations were generated
                num_recs = len(parsed.get('recommended_corrections', []))
                print(f" Generated {num_recs} recommendation(s) for {proc_code} + {dx_code}")
                
                return parsed
            except json.JSONDecodeError as e:
                print(f" JSON parsing failed for {proc_code} + {dx_code}: {e}")
                return {"raw": text, "parse_error": "LLM response was not valid JSON"}
        except Exception as e:
            print(f" LLM call failed for {proc_code} + {dx_code}: {e}")
            return {"error": str(e)}

    def _stage2(self, issue: Dict[str, Any], stage1: Dict[str, Any]) -> Dict[str, Any]:
        archetype = self._detect_archetype(issue)
        codes = {
            'hcpcs_code': issue.get('hcpcs_code',''),
            'icd9_code': issue.get('icd9_code',''),
            'icd10_code': issue.get('icd10_code',''),
        }
        sql_ev = self.sql_connector.execute_archetype_query(archetype, codes)
        policies = self._search_policies(issue, archetype)
        llm_out = self._run_stage2_llm(issue, archetype, sql_ev, policies)
        return {
            "archetype": archetype,
            "sql_evidence": sql_ev,
            "correction_policies": policies,
            "correction_analysis": llm_out,
            "stage": "sql_driven_archetype_corrective_reasoning"
        }

    def run_archetype_driven_corrections(self, claim_id: str) -> Dict[str, Any]:
        stage1 = self.calibrated_corrector.run_corrections(claim_id)
        issues = stage1.get('enriched_issues') or []
        if not issues:
            return {"claim_id": claim_id, "error": "No claim issues found", "stage1_result": stage1}
        
        print(f"\n Starting Stage 2 processing for {len(issues)} issues...")
        enriched = []
        for idx, issue in enumerate(issues, 1):
            dx = issue.get('icd10_code') or issue.get('icd9_code', 'Unknown')
            proc = issue.get('hcpcs_code', 'Unknown')
            print(f"\n📋 Processing issue {idx}/{len(issues)}: DX={dx}, PROC={proc}")
            
            res = self._stage2(issue, stage1)
            i2 = issue.copy()
            i2['stage2_archetype_correction_analysis'] = res
            i2['archetype_driven_complete'] = True
            enriched.append(i2)
        
        # Count total recommendations generated
        total_recs = sum(len(i.get('stage2_archetype_correction_analysis', {}).get('correction_analysis', {}).get('recommended_corrections', [])) for i in enriched)
        print(f"\n Stage 2 Complete: Processed {len(enriched)} issues, generated {total_recs} total recommendations\n")
        
        return {"claim_id": claim_id, "enriched_issues": enriched, "total_issues": len(enriched), "processing_timestamp": datetime.now().isoformat(), "version": "3.0"}

    def cleanup(self):
        if self.sql_connector:
            self.sql_connector.close()


if __name__ == "__main__":
    c = ArchetypeDrivenClaimCorrectorV3()
    try:
        out = c.run_archetype_driven_corrections("123456789012345")
        print(json.dumps(out, indent=2))
    finally:
        c.cleanup()



Enhanced Archetype-Driven Claim Corrector v3.0
==============================================

Key updates vs v2:
- ICD version–aware SQL for DX-driven archetypes (ICD-10 or ICD-9)
- GEMs-based fallback mapping when direct version returns 0 rows
- Adds Secondary_DX_Not_Covered definition explicitly
"""

import json
import logging
import warnings
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from claim_corrector_claims3_calibrated import CalibratedClaimCorrector

try:
    import pyodbc
    import pandas as pd
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, VectorParams, PointStruct
    from sentence_transformers import SentenceTransformer
    import ollama
    import numpy as np
except ImportError as e:
    print(f" Missing required library: {e}")
    sys.exit(1)

# Suppress pandas SQLAlchemy warning for pyodbc connections
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*', category=UserWarning)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Archetype Definitions (v3)
# -----------------------------------------------------------------------------

ARCHETYPE_DEFINITIONS = {
    "NCCI_PTP_Conflict": {
        "description": "The CPT/HCPCS combination violates an NCCI Procedure-to-Procedure (PTP) rule.",
        "qdrant_collections": ["claims__ncci_edits", "claims__med_claims_policies"],
        "sql_query": """
            SELECT 
                ncci.procedure_code,
                ncci.ptp_denial_reason,
                ncci.instructions,
                ncci.ptp_edit_type,
                ncci.modifier_status,
                ncci.mue_threshold,
                ncci.mue_denial_type
            FROM [_ncci_].[dbo].[vw_NCCI_Daily_Denial_Alerts] ncci
            WHERE ncci.procedure_code = ?
              AND (ncci.ptp_denial_reason IS NOT NULL OR ncci.mue_threshold IS NOT NULL)
        """,
    },

    # DX-driven archetypes use templated WHERE clause decided at runtime
    "Primary_DX_Not_Covered": {
        "description": "Primary diagnosis is not covered under the relevant LCD or NCD.",
        "qdrant_collections": ["claims__lcd_policies", "claims__ncd_policies"],
        "sql_query": """
            SELECT 
                g.icd9_code,
                g.icd10_code,
                l.lcd_id,
                l.title as lcd_title,
                l.coverage_group_description as coverage_group,
                l.lcd_last_updated as last_updated,
                l.covered_codes,
                l.diagnoses_support,
                l.diagnoses_dont_support
            FROM [_gems].[dbo].[table_2018_I9gem_fixed] g
            LEFT JOIN [_lcd].[dbo].[vw_Master_Denial_Analysis] l
                ON g.icd10_code = l.covered_codes
            WHERE {DX_WHERE}
        """,
    },

    "Secondary_DX_Not_Covered": {
        "description": "Secondary diagnosis is not covered under the relevant LCD or NCD.",
        "qdrant_collections": ["claims__lcd_policies", "claims__ncd_policies"],
        "sql_query": """
            SELECT 
                g.icd9_code,
                g.icd10_code,
                l.lcd_id,
                l.title as lcd_title,
                l.coverage_group_description as coverage_group,
                l.lcd_last_updated as last_updated,
                l.covered_codes,
                l.diagnoses_support,
                l.diagnoses_dont_support
            FROM [_gems].[dbo].[table_2018_I9gem_fixed] g
            LEFT JOIN [_lcd].[dbo].[vw_Master_Denial_Analysis] l
                ON g.icd10_code = l.covered_codes
            WHERE {DX_WHERE}
        """,
    },

    "MUE_Risk": {
        "description": "Billed units exceed the Medically Unlikely Edit (MUE) threshold.",
        "qdrant_collections": ["claims__ncci_edits"],
        "sql_query": """
            SELECT 
                ncci.procedure_code,
                ncci.mue_threshold,
                ncci.mue_denial_type,
                ncci.mue_rationale,
                ncci.instructions
            FROM [_ncci_].[dbo].[vw_NCCI_Daily_Denial_Alerts] ncci
            WHERE ncci.procedure_code = ?
              AND ncci.mue_threshold IS NOT NULL
        """,
    },

    "Bundled_Payment_Conflict": {
        "description": "Claim includes services bundled under DRG/APC or OPPS rules.",
        "qdrant_collections": ["claims__ncci_edits", "claims__med_claims_policies"],
        "sql_query": """
            SELECT 
                ptp.primary_cpt_hcpcs_code,
                ptp.secondary_cpt_hcpcs_code,
                ptp.edit_type,
                ptp.ptp_edit_rationale,
                ptp.modifier_guidance,
                ptp.effective_date
            FROM [_ncci_].[dbo].[vw_NCCI_Modifier_Compliance] ptp
            WHERE ptp.secondary_cpt_hcpcs_code = ?
              AND ptp.edit_type IN ('Bundled', 'Component')
        """,
    },
}


class SQLDatabaseConnector:
    def __init__(self):
        self.connection = None
        self._connect()

    def _connect(self):
        try:
            cs = (
                'Driver={ODBC Driver 18 for SQL Server};'
                'Server=localhost,1433;UID=SA;PWD=Bbanwo@1980!;'
                'Database=_claims;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;'
            )
            self.connection = pyodbc.connect(cs)
            print(" SQL Database connection established successfully")
        except Exception as e:
            print(f" SQL Database connection failed: {e}")
            self.connection = None

    def _map_icd10_to_icd9(self, icd10: str) -> List[str]:
        try:
            q = """
                SELECT DISTINCT icd9_code
                FROM [_gems].[dbo].[table_2018_I9gem_fixed]
                WHERE icd10_code = ?
            """
            df = pd.read_sql(q, self.connection, params=[icd10])
            return [r.icd9_code for _, r in df.iterrows()]
        except Exception:
            return []

    def _map_icd9_to_icd10(self, icd9: str) -> List[str]:
        try:
            q = """
                SELECT DISTINCT icd10_code
                FROM [_gems].[dbo].[table_2018_I9gem_fixed]
                WHERE icd9_code = ?
            """
            df = pd.read_sql(q, self.connection, params=[icd9])
            return [r.icd10_code for _, r in df.iterrows()]
        except Exception:
            return []

    def execute_archetype_query(self, archetype: str, codes: Dict[str, str]) -> List[Dict[str, Any]]:
        if not self.connection:
            return []

        info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        base_sql = info.get('sql_query', '')
        if not base_sql:
            return []

        try:
            # HCPCS-driven archetypes
            if archetype in ["NCCI_PTP_Conflict", "MUE_Risk", "Bundled_Payment_Conflict"]:
                hcpcs = codes.get('hcpcs_code')
                if not hcpcs:
                    return []
                df = pd.read_sql(base_sql, self.connection, params=[hcpcs])
                return df.to_dict('records')

            # DX-driven archetypes with ICD version awareness
            if archetype in ["Primary_DX_Not_Covered", "Secondary_DX_Not_Covered"]:
                icd10 = codes.get('icd10_code') or ''
                icd9 = codes.get('icd9_code') or ''

                results: List[Dict[str, Any]] = []

                def run_dx_query(dx_where: str, param: str) -> List[Dict[str, Any]]:
                    sql = base_sql.replace('{DX_WHERE}', dx_where)
                    df_local = pd.read_sql(sql, self.connection, params=[param])
                    return df_local.to_dict('records')

                # Prefer ICD-10 if provided (starts with a letter)
                tried = []
                if icd10 and icd10[:1].isalpha():
                    tried.append(('g.icd10_code = ?', icd10))
                if icd9 and not icd9[:1].isalpha():
                    tried.append(('g.icd9_code = ?', icd9))

                # If no clear version, still attempt both in order icd10 -> icd9 using available values
                if not tried:
                    if icd10:
                        tried.append(('g.icd10_code = ?', icd10))
                    if icd9:
                        tried.append(('g.icd9_code = ?', icd9))

                # Execute attempts
                for dx_where, param in tried:
                    rows = run_dx_query(dx_where, param)
                    if rows:
                        results.extend(rows)
                        break

                # Fallback via GEMs mapping if empty
                if not results:
                    if icd10:
                        mapped_icd9 = self._map_icd10_to_icd9(icd10)
                        for m in mapped_icd9:
                            rows = run_dx_query('g.icd9_code = ?', m)
                            if rows:
                                results.extend(rows)
                                break
                    if not results and icd9:
                        mapped_icd10 = self._map_icd9_to_icd10(icd9)
                        for m in mapped_icd10:
                            rows = run_dx_query('g.icd10_code = ?', m)
                            if rows:
                                results.extend(rows)
                                break

                return results

            return []
        except Exception as e:
            print(f" SQL query failed for archetype '{archetype}': {e}")
            return []

    def close(self):
        if self.connection:
            self.connection.close()


class ArchetypeDrivenClaimCorrectorV3:
    def __init__(self):
        print(" Initializing Enhanced Archetype-Driven Claim Corrector v3.0...")
        self.calibrated_corrector = CalibratedClaimCorrector()
        self.sql_connector = SQLDatabaseConnector()
        self.qdrant_client = None
        self.embedder = None
        self._init_qdrant()
        self._init_embedder()
        if hasattr(self.calibrated_corrector, 'embedder'):
            self.embedder = self.calibrated_corrector.embedder
        print(" Enhanced Archetype-Driven Claim Corrector v3.0 initialized successfully")

    def _init_qdrant(self):
        try:
            self.qdrant_client = QdrantClient(host="localhost", port=6333)
            print(" Qdrant client connected successfully")
        except Exception as e:
            print(f" Qdrant initialization failed: {e}")
            self.qdrant_client = None

    def _init_embedder(self):
        try:
            self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
            print(" Sentence transformer model loaded successfully")
        except Exception as e:
            print(f" Embedder initialization failed: {e}")
            self.embedder = None

    def _detect_archetype(self, issue: Dict[str, Any]) -> str:
        if issue.get('ptp_denial_reason') and issue.get('hcpcs_position') == 1:
            return "NCCI_PTP_Conflict"
        if issue.get('mue_threshold'):
            return "MUE_Risk"
        if issue.get('lcd_icd10_covered_group') == 'N' and issue.get('dx_position') == 1:
            return "Primary_DX_Not_Covered"
        if issue.get('lcd_icd10_covered_group') == 'N' and issue.get('dx_position') > 1:
            return "Secondary_DX_Not_Covered"
        return "NCCI_PTP_Conflict" if issue.get('ptp_denial_reason') else "MUE_Risk"

    def _build_query_text(self, issue: Dict[str, Any], archetype: str) -> str:
        parts = [f"CPT {issue.get('hcpcs_code','')}"]
        if issue.get('icd10_code'):
            parts.append(f"ICD {issue.get('icd10_code')}")
        if issue.get('icd9_code'):
            parts.append(f"ICD {issue.get('icd9_code')}")
        if archetype == "NCCI_PTP_Conflict":
            parts.extend(["NCCI","PTP","modifier"]) 
        elif archetype.endswith("Not_Covered"):
            parts.extend(["LCD","coverage","diagnosis"]) 
        elif archetype == "MUE_Risk":
            parts.extend(["MUE","units","threshold"]) 
        return " ".join(parts)

    def _search_policies(self, issue: Dict[str, Any], archetype: str) -> List[Dict[str, Any]]:
        if not self.qdrant_client or not self.embedder:
            return []
        colls = ARCHETYPE_DEFINITIONS.get(archetype, {}).get('qdrant_collections', [])
        vector = self.embedder.encode(self._build_query_text(issue, archetype)).tolist()
        results: List[Dict[str, Any]] = []
        try:
            all_colls = [c.name for c in self.qdrant_client.get_collections().collections]
            for c in colls:
                if c not in all_colls:
                    print(f"    Collection {c} does not exist, skipping")
                    continue
                hits = self.qdrant_client.search(collection_name=c, query_vector=vector, limit=3, score_threshold=0.7)
                for h in hits:
                    p = h.payload or {}
                    p['score'] = h.score
                    p['collection'] = c
                    results.append(p)
        except Exception as e:
            print(f"    Policy search failed: {e}")
        results.sort(key=lambda x: x.get('score',0), reverse=True)
        return results[:5]

    def _run_stage2_llm(self, issue: Dict[str, Any], archetype: str, sql_evidence: List[Dict[str, Any]], policies: List[Dict[str, Any]]) -> Dict[str, Any]:
        # Extract codes and identify types
        dx_code = issue.get('icd10_code') or issue.get('icd9_code') or ''
        proc_code = issue.get('hcpcs_code') or ''
        
        # Build code type information
        code_info = f"""
CODE IDENTIFICATION:
- PROCEDURE CODE (CPT/HCPCS): {proc_code} → Use for: modifier checks, PTP edits, MUE review, prior auth, bundling
- DIAGNOSIS CODE (ICD-10/ICD-9): {dx_code} → Use for: coverage validation, medical necessity, LCD/NCD compliance

CRITICAL RULES:
1. Modifiers (59, 25, XE, XS, XP, XU) apply ONLY to PROCEDURE codes, NEVER to DIAGNOSIS codes
2. If a code starts with a letter and is ≤7 chars (e.g., M16.11, Z99.89), it is ICD - do NOT assign modifiers
3. Only 5-digit numeric or alphanumeric codes (e.g., 27130, G0299) are procedures that can have modifiers
"""
        
        # Create a structured prompt for correction recommendations
        prompt = f"""
You are a CMS-compliant claim denial correction expert with deep knowledge of medical billing.

{code_info}

ARCHETYPE: {archetype}
RISK LEVEL: {issue.get('denial_risk_level', '')}
ACTION REQUIRED: {issue.get('action_required', '')}

SQL EVIDENCE (Historical Claims Data):
{json.dumps(sql_evidence[:3], indent=2) if sql_evidence else "No SQL evidence available"}

POLICY EXCERPTS (CMS Guidelines):
{json.dumps([{k: v for k, v in p.items() if k in ['title', 'text', 'source', 'policy_number']} for p in policies[:3]], indent=2) if policies else "No policies found"}

COMPLIANCE REQUIREMENTS:
- Only recommend actions a PROVIDER or CODER can perform on their claim
- NEVER instruct users to "revise," "update," or "modify" CMS policy documents (LCDs, NCDs, Articles)
- Providers can only COMPLY with policies, not change them
- Be SPECIFIC: reference exact claim fields, documentation requirements, or SQL queries
- Separate diagnosis logic from procedure logic:
  * Diagnosis ({dx_code}): coverage verification, medical necessity documentation
  * Procedure ({proc_code}): modifier application, bundling checks, prior authorization
- YOU MUST GENERATE AT LEAST ONE ACTIONABLE RECOMMENDATION - this is a denial risk that needs resolution!

REQUIRED OUTPUT FORMAT (valid JSON only):
{{
  "claim_id": "{issue.get('claim_id', '')}",
  "archetype": "{archetype}",
  "sql_evidence_summary": "Summary of what the SQL database evidence shows (denial rates, common issues, patterns)",
  "recommended_corrections": [
    {{
      "field": "specific_claim_field_name (e.g., 'procedure_modifier', 'primary_diagnosis_position', 'units_billed')",
      "current_value": "what is currently on the claim",
      "suggested_value": "exact new value to enter or add",
      "rationale": "why this change is needed based on CMS policy",
      "policy_citation": "specific LCD/NCD/NCCI edit number and section (e.g., 'LCD L12345 §3.2, NCCI PTP edit 27130-27447')",
      "implementation_steps": [
        "Step 1: Specific action with exact field/form location (e.g., 'In claim line 2, locate the Modifier field')",
        "Step 2: Specific action with exact value (e.g., 'Enter modifier 59 to indicate distinct procedural service')",
        "Step 3: Specific verification (e.g., 'Verify operative note documents separate incision site')"
      ],
      "confidence": 0.85
    }}
  ],
  "policy_references": ["LCD L12345", "NCCI PTP Manual Ch.2"],
  "final_guidance": "Overall strategy to resolve this denial risk",
  "compliance_checklist": ["Verification step 1 with specific requirement", "Verification step 2 with specific requirement"]
}}

EXAMPLES OF GOOD CORRECTIONS:
✓ Field: "procedure_modifier" | Current: "none" | Suggested: "59" | Rationale: "NCCI PTP edit requires modifier 59 to bypass bundling" | Steps: ["Open claim line for CPT {proc_code}", "Add modifier 59 to indicate distinct procedural service", "Attach operative note documenting separate incision site"]
✓ Field: "primary_diagnosis" | Current: "{dx_code}" | Suggested: "Add covered diagnosis from LCD L33822" | Rationale: "{dx_code} is not in LCD covered codes list" | Steps: ["Review LCD L33822 Table 1", "Select appropriate covered diagnosis", "Reorder diagnoses with covered code first"]

EXAMPLES OF BAD CORRECTIONS (AVOID):
✗ "Revise the LCD to include {dx_code}" → Providers cannot modify LCDs, only comply with them
✗ "Remove modifier from {dx_code}" → Cannot apply modifiers to diagnoses (only to procedures)
✗ "Review medical records" → Too vague; specify what documentation to look for

Focus on clear, specific, implementable actions that will get the claim paid.
"""
        
        try:
            response = ollama.generate(
                model="mistral", 
                prompt=prompt,
                options={
                    "num_predict": 4096,  # Increased from 2048 to allow longer responses
                    "temperature": 0.3,
                    "top_p": 0.85
                }
            )
            text = response.get('response','{}')
            
            # Debug: Log raw LLM response
            print(f"\n{'='*80}")
            print(f"RAW LLM RESPONSE for {proc_code} + {dx_code} (Archetype: {archetype}):")
            print(f"{'='*80}")
            print(text[:2000])  # First 2000 chars (increased from 1000)
            print(f"{'='*80}\n")
            
            try:
                parsed = json.loads(text)
                
                # Debug: Log how many recommendations were generated
                num_recs = len(parsed.get('recommended_corrections', []))
                print(f" Generated {num_recs} recommendation(s) for {proc_code} + {dx_code}")
                
                return parsed
            except json.JSONDecodeError as e:
                print(f" JSON parsing failed for {proc_code} + {dx_code}: {e}")
                return {"raw": text, "parse_error": "LLM response was not valid JSON"}
        except Exception as e:
            print(f" LLM call failed for {proc_code} + {dx_code}: {e}")
            return {"error": str(e)}

    def _stage2(self, issue: Dict[str, Any], stage1: Dict[str, Any]) -> Dict[str, Any]:
        archetype = self._detect_archetype(issue)
        codes = {
            'hcpcs_code': issue.get('hcpcs_code',''),
            'icd9_code': issue.get('icd9_code',''),
            'icd10_code': issue.get('icd10_code',''),
        }
        sql_ev = self.sql_connector.execute_archetype_query(archetype, codes)
        policies = self._search_policies(issue, archetype)
        llm_out = self._run_stage2_llm(issue, archetype, sql_ev, policies)
        return {
            "archetype": archetype,
            "sql_evidence": sql_ev,
            "correction_policies": policies,
            "correction_analysis": llm_out,
            "stage": "sql_driven_archetype_corrective_reasoning"
        }

    def run_archetype_driven_corrections(self, claim_id: str) -> Dict[str, Any]:
        stage1 = self.calibrated_corrector.run_corrections(claim_id)
        issues = stage1.get('enriched_issues') or []
        if not issues:
            return {"claim_id": claim_id, "error": "No claim issues found", "stage1_result": stage1}
        
        print(f"\n Starting Stage 2 processing for {len(issues)} issues...")
        enriched = []
        for idx, issue in enumerate(issues, 1):
            dx = issue.get('icd10_code') or issue.get('icd9_code', 'Unknown')
            proc = issue.get('hcpcs_code', 'Unknown')
            print(f"\n📋 Processing issue {idx}/{len(issues)}: DX={dx}, PROC={proc}")
            
            res = self._stage2(issue, stage1)
            i2 = issue.copy()
            i2['stage2_archetype_correction_analysis'] = res
            i2['archetype_driven_complete'] = True
            enriched.append(i2)
        
        # Count total recommendations generated
        total_recs = sum(len(i.get('stage2_archetype_correction_analysis', {}).get('correction_analysis', {}).get('recommended_corrections', [])) for i in enriched)
        print(f"\n Stage 2 Complete: Processed {len(enriched)} issues, generated {total_recs} total recommendations\n")
        
        return {"claim_id": claim_id, "enriched_issues": enriched, "total_issues": len(enriched), "processing_timestamp": datetime.now().isoformat(), "version": "3.0"}

    def cleanup(self):
        if self.sql_connector:
            self.sql_connector.close()


if __name__ == "__main__":
    c = ArchetypeDrivenClaimCorrectorV3()
    try:
        out = c.run_archetype_driven_corrections("123456789012345")
        print(json.dumps(out, indent=2))
    finally:
        c.cleanup()





Enhanced Archetype-Driven Claim Corrector v3.0
==============================================

Key updates vs v2:
- ICD version–aware SQL for DX-driven archetypes (ICD-10 or ICD-9)
- GEMs-based fallback mapping when direct version returns 0 rows
- Adds Secondary_DX_Not_Covered definition explicitly
"""

import json
import logging
import warnings
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from claim_corrector_claims3_calibrated import CalibratedClaimCorrector

try:
    import pyodbc
    import pandas as pd
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, VectorParams, PointStruct
    from sentence_transformers import SentenceTransformer
    import ollama
    import numpy as np
except ImportError as e:
    print(f" Missing required library: {e}")
    sys.exit(1)

# Suppress pandas SQLAlchemy warning for pyodbc connections
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*', category=UserWarning)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Archetype Definitions (v3)
# -----------------------------------------------------------------------------

ARCHETYPE_DEFINITIONS = {
    "NCCI_PTP_Conflict": {
        "description": "The CPT/HCPCS combination violates an NCCI Procedure-to-Procedure (PTP) rule.",
        "qdrant_collections": ["claims__ncci_edits", "claims__med_claims_policies"],
        "sql_query": """
            SELECT 
                ncci.procedure_code,
                ncci.ptp_denial_reason,
                ncci.instructions,
                ncci.ptp_edit_type,
                ncci.modifier_status,
                ncci.mue_threshold,
                ncci.mue_denial_type
            FROM [_ncci_].[dbo].[vw_NCCI_Daily_Denial_Alerts] ncci
            WHERE ncci.procedure_code = ?
              AND (ncci.ptp_denial_reason IS NOT NULL OR ncci.mue_threshold IS NOT NULL)
        """,
    },

    # DX-driven archetypes use templated WHERE clause decided at runtime
    "Primary_DX_Not_Covered": {
        "description": "Primary diagnosis is not covered under the relevant LCD or NCD.",
        "qdrant_collections": ["claims__lcd_policies", "claims__ncd_policies"],
        "sql_query": """
            SELECT 
                g.icd9_code,
                g.icd10_code,
                l.lcd_id,
                l.title as lcd_title,
                l.coverage_group_description as coverage_group,
                l.lcd_last_updated as last_updated,
                l.covered_codes,
                l.diagnoses_support,
                l.diagnoses_dont_support
            FROM [_gems].[dbo].[table_2018_I9gem_fixed] g
            LEFT JOIN [_lcd].[dbo].[vw_Master_Denial_Analysis] l
                ON g.icd10_code = l.covered_codes
            WHERE {DX_WHERE}
        """,
    },

    "Secondary_DX_Not_Covered": {
        "description": "Secondary diagnosis is not covered under the relevant LCD or NCD.",
        "qdrant_collections": ["claims__lcd_policies", "claims__ncd_policies"],
        "sql_query": """
            SELECT 
                g.icd9_code,
                g.icd10_code,
                l.lcd_id,
                l.title as lcd_title,
                l.coverage_group_description as coverage_group,
                l.lcd_last_updated as last_updated,
                l.covered_codes,
                l.diagnoses_support,
                l.diagnoses_dont_support
            FROM [_gems].[dbo].[table_2018_I9gem_fixed] g
            LEFT JOIN [_lcd].[dbo].[vw_Master_Denial_Analysis] l
                ON g.icd10_code = l.covered_codes
            WHERE {DX_WHERE}
        """,
    },

    "MUE_Risk": {
        "description": "Billed units exceed the Medically Unlikely Edit (MUE) threshold.",
        "qdrant_collections": ["claims__ncci_edits"],
        "sql_query": """
            SELECT 
                ncci.procedure_code,
                ncci.mue_threshold,
                ncci.mue_denial_type,
                ncci.mue_rationale,
                ncci.instructions
            FROM [_ncci_].[dbo].[vw_NCCI_Daily_Denial_Alerts] ncci
            WHERE ncci.procedure_code = ?
              AND ncci.mue_threshold IS NOT NULL
        """,
    },

    "Bundled_Payment_Conflict": {
        "description": "Claim includes services bundled under DRG/APC or OPPS rules.",
        "qdrant_collections": ["claims__ncci_edits", "claims__med_claims_policies"],
        "sql_query": """
            SELECT 
                ptp.primary_cpt_hcpcs_code,
                ptp.secondary_cpt_hcpcs_code,
                ptp.edit_type,
                ptp.ptp_edit_rationale,
                ptp.modifier_guidance,
                ptp.effective_date
            FROM [_ncci_].[dbo].[vw_NCCI_Modifier_Compliance] ptp
            WHERE ptp.secondary_cpt_hcpcs_code = ?
              AND ptp.edit_type IN ('Bundled', 'Component')
        """,
    },
}


class SQLDatabaseConnector:
    def __init__(self):
        self.connection = None
        self._connect()

    def _connect(self):
        try:
            cs = (
                'Driver={ODBC Driver 18 for SQL Server};'
                'Server=localhost,1433;UID=SA;PWD=Bbanwo@1980!;'
                'Database=_claims;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;'
            )
            self.connection = pyodbc.connect(cs)
            print(" SQL Database connection established successfully")
        except Exception as e:
            print(f" SQL Database connection failed: {e}")
            self.connection = None

    def _map_icd10_to_icd9(self, icd10: str) -> List[str]:
        try:
            q = """
                SELECT DISTINCT icd9_code
                FROM [_gems].[dbo].[table_2018_I9gem_fixed]
                WHERE icd10_code = ?
            """
            df = pd.read_sql(q, self.connection, params=[icd10])
            return [r.icd9_code for _, r in df.iterrows()]
        except Exception:
            return []

    def _map_icd9_to_icd10(self, icd9: str) -> List[str]:
        try:
            q = """
                SELECT DISTINCT icd10_code
                FROM [_gems].[dbo].[table_2018_I9gem_fixed]
                WHERE icd9_code = ?
            """
            df = pd.read_sql(q, self.connection, params=[icd9])
            return [r.icd10_code for _, r in df.iterrows()]
        except Exception:
            return []

    def execute_archetype_query(self, archetype: str, codes: Dict[str, str]) -> List[Dict[str, Any]]:
        if not self.connection:
            return []

        info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        base_sql = info.get('sql_query', '')
        if not base_sql:
            return []

        try:
            # HCPCS-driven archetypes
            if archetype in ["NCCI_PTP_Conflict", "MUE_Risk", "Bundled_Payment_Conflict"]:
                hcpcs = codes.get('hcpcs_code')
                if not hcpcs:
                    return []
                df = pd.read_sql(base_sql, self.connection, params=[hcpcs])
                return df.to_dict('records')

            # DX-driven archetypes with ICD version awareness
            if archetype in ["Primary_DX_Not_Covered", "Secondary_DX_Not_Covered"]:
                icd10 = codes.get('icd10_code') or ''
                icd9 = codes.get('icd9_code') or ''

                results: List[Dict[str, Any]] = []

                def run_dx_query(dx_where: str, param: str) -> List[Dict[str, Any]]:
                    sql = base_sql.replace('{DX_WHERE}', dx_where)
                    df_local = pd.read_sql(sql, self.connection, params=[param])
                    return df_local.to_dict('records')

                # Prefer ICD-10 if provided (starts with a letter)
                tried = []
                if icd10 and icd10[:1].isalpha():
                    tried.append(('g.icd10_code = ?', icd10))
                if icd9 and not icd9[:1].isalpha():
                    tried.append(('g.icd9_code = ?', icd9))

                # If no clear version, still attempt both in order icd10 -> icd9 using available values
                if not tried:
                    if icd10:
                        tried.append(('g.icd10_code = ?', icd10))
                    if icd9:
                        tried.append(('g.icd9_code = ?', icd9))

                # Execute attempts
                for dx_where, param in tried:
                    rows = run_dx_query(dx_where, param)
                    if rows:
                        results.extend(rows)
                        break

                # Fallback via GEMs mapping if empty
                if not results:
                    if icd10:
                        mapped_icd9 = self._map_icd10_to_icd9(icd10)
                        for m in mapped_icd9:
                            rows = run_dx_query('g.icd9_code = ?', m)
                            if rows:
                                results.extend(rows)
                                break
                    if not results and icd9:
                        mapped_icd10 = self._map_icd9_to_icd10(icd9)
                        for m in mapped_icd10:
                            rows = run_dx_query('g.icd10_code = ?', m)
                            if rows:
                                results.extend(rows)
                                break

                return results

            return []
        except Exception as e:
            print(f" SQL query failed for archetype '{archetype}': {e}")
            return []

    def close(self):
        if self.connection:
            self.connection.close()


class ArchetypeDrivenClaimCorrectorV3:
    def __init__(self):
        print(" Initializing Enhanced Archetype-Driven Claim Corrector v3.0...")
        self.calibrated_corrector = CalibratedClaimCorrector()
        self.sql_connector = SQLDatabaseConnector()
        self.qdrant_client = None
        self.embedder = None
        self._init_qdrant()
        self._init_embedder()
        if hasattr(self.calibrated_corrector, 'embedder'):
            self.embedder = self.calibrated_corrector.embedder
        print(" Enhanced Archetype-Driven Claim Corrector v3.0 initialized successfully")

    def _init_qdrant(self):
        try:
            self.qdrant_client = QdrantClient(host="localhost", port=6333)
            print(" Qdrant client connected successfully")
        except Exception as e:
            print(f" Qdrant initialization failed: {e}")
            self.qdrant_client = None

    def _init_embedder(self):
        try:
            self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
            print(" Sentence transformer model loaded successfully")
        except Exception as e:
            print(f" Embedder initialization failed: {e}")
            self.embedder = None

    def _detect_archetype(self, issue: Dict[str, Any]) -> str:
        if issue.get('ptp_denial_reason') and issue.get('hcpcs_position') == 1:
            return "NCCI_PTP_Conflict"
        if issue.get('mue_threshold'):
            return "MUE_Risk"
        if issue.get('lcd_icd10_covered_group') == 'N' and issue.get('dx_position') == 1:
            return "Primary_DX_Not_Covered"
        if issue.get('lcd_icd10_covered_group') == 'N' and issue.get('dx_position') > 1:
            return "Secondary_DX_Not_Covered"
        return "NCCI_PTP_Conflict" if issue.get('ptp_denial_reason') else "MUE_Risk"

    def _build_query_text(self, issue: Dict[str, Any], archetype: str) -> str:
        parts = [f"CPT {issue.get('hcpcs_code','')}"]
        if issue.get('icd10_code'):
            parts.append(f"ICD {issue.get('icd10_code')}")
        if issue.get('icd9_code'):
            parts.append(f"ICD {issue.get('icd9_code')}")
        if archetype == "NCCI_PTP_Conflict":
            parts.extend(["NCCI","PTP","modifier"]) 
        elif archetype.endswith("Not_Covered"):
            parts.extend(["LCD","coverage","diagnosis"]) 
        elif archetype == "MUE_Risk":
            parts.extend(["MUE","units","threshold"]) 
        return " ".join(parts)

    def _search_policies(self, issue: Dict[str, Any], archetype: str) -> List[Dict[str, Any]]:
        if not self.qdrant_client or not self.embedder:
            return []
        colls = ARCHETYPE_DEFINITIONS.get(archetype, {}).get('qdrant_collections', [])
        vector = self.embedder.encode(self._build_query_text(issue, archetype)).tolist()
        results: List[Dict[str, Any]] = []
        try:
            all_colls = [c.name for c in self.qdrant_client.get_collections().collections]
            for c in colls:
                if c not in all_colls:
                    print(f"    Collection {c} does not exist, skipping")
                    continue
                hits = self.qdrant_client.search(collection_name=c, query_vector=vector, limit=3, score_threshold=0.7)
                for h in hits:
                    p = h.payload or {}
                    p['score'] = h.score
                    p['collection'] = c
                    results.append(p)
        except Exception as e:
            print(f"    Policy search failed: {e}")
        results.sort(key=lambda x: x.get('score',0), reverse=True)
        return results[:5]

    def _run_stage2_llm(self, issue: Dict[str, Any], archetype: str, sql_evidence: List[Dict[str, Any]], policies: List[Dict[str, Any]]) -> Dict[str, Any]:
        # Extract codes and identify types
        dx_code = issue.get('icd10_code') or issue.get('icd9_code') or ''
        proc_code = issue.get('hcpcs_code') or ''
        
        # Build code type information
        code_info = f"""
CODE IDENTIFICATION:
- PROCEDURE CODE (CPT/HCPCS): {proc_code} → Use for: modifier checks, PTP edits, MUE review, prior auth, bundling
- DIAGNOSIS CODE (ICD-10/ICD-9): {dx_code} → Use for: coverage validation, medical necessity, LCD/NCD compliance

CRITICAL RULES:
1. Modifiers (59, 25, XE, XS, XP, XU) apply ONLY to PROCEDURE codes, NEVER to DIAGNOSIS codes
2. If a code starts with a letter and is ≤7 chars (e.g., M16.11, Z99.89), it is ICD - do NOT assign modifiers
3. Only 5-digit numeric or alphanumeric codes (e.g., 27130, G0299) are procedures that can have modifiers
"""
        
        # Create a structured prompt for correction recommendations
        prompt = f"""
You are a CMS-compliant claim denial correction expert with deep knowledge of medical billing.

{code_info}

ARCHETYPE: {archetype}
RISK LEVEL: {issue.get('denial_risk_level', '')}
ACTION REQUIRED: {issue.get('action_required', '')}

SQL EVIDENCE (Historical Claims Data):
{json.dumps(sql_evidence[:3], indent=2) if sql_evidence else "No SQL evidence available"}

POLICY EXCERPTS (CMS Guidelines):
{json.dumps([{k: v for k, v in p.items() if k in ['title', 'text', 'source', 'policy_number']} for p in policies[:3]], indent=2) if policies else "No policies found"}

COMPLIANCE REQUIREMENTS:
- Only recommend actions a PROVIDER or CODER can perform on their claim
- NEVER instruct users to "revise," "update," or "modify" CMS policy documents (LCDs, NCDs, Articles)
- Providers can only COMPLY with policies, not change them
- Be SPECIFIC: reference exact claim fields, documentation requirements, or SQL queries
- Separate diagnosis logic from procedure logic:
  * Diagnosis ({dx_code}): coverage verification, medical necessity documentation
  * Procedure ({proc_code}): modifier application, bundling checks, prior authorization
- YOU MUST GENERATE AT LEAST ONE ACTIONABLE RECOMMENDATION - this is a denial risk that needs resolution!

REQUIRED OUTPUT FORMAT (valid JSON only):
{{
  "claim_id": "{issue.get('claim_id', '')}",
  "archetype": "{archetype}",
  "sql_evidence_summary": "Summary of what the SQL database evidence shows (denial rates, common issues, patterns)",
  "recommended_corrections": [
    {{
      "field": "specific_claim_field_name (e.g., 'procedure_modifier', 'primary_diagnosis_position', 'units_billed')",
      "current_value": "what is currently on the claim",
      "suggested_value": "exact new value to enter or add",
      "rationale": "why this change is needed based on CMS policy",
      "policy_citation": "specific LCD/NCD/NCCI edit number and section (e.g., 'LCD L12345 §3.2, NCCI PTP edit 27130-27447')",
      "implementation_steps": [
        "Step 1: Specific action with exact field/form location (e.g., 'In claim line 2, locate the Modifier field')",
        "Step 2: Specific action with exact value (e.g., 'Enter modifier 59 to indicate distinct procedural service')",
        "Step 3: Specific verification (e.g., 'Verify operative note documents separate incision site')"
      ],
      "confidence": 0.85
    }}
  ],
  "policy_references": ["LCD L12345", "NCCI PTP Manual Ch.2"],
  "final_guidance": "Overall strategy to resolve this denial risk",
  "compliance_checklist": ["Verification step 1 with specific requirement", "Verification step 2 with specific requirement"]
}}

EXAMPLES OF GOOD CORRECTIONS:
✓ Field: "procedure_modifier" | Current: "none" | Suggested: "59" | Rationale: "NCCI PTP edit requires modifier 59 to bypass bundling" | Steps: ["Open claim line for CPT {proc_code}", "Add modifier 59 to indicate distinct procedural service", "Attach operative note documenting separate incision site"]
✓ Field: "primary_diagnosis" | Current: "{dx_code}" | Suggested: "Add covered diagnosis from LCD L33822" | Rationale: "{dx_code} is not in LCD covered codes list" | Steps: ["Review LCD L33822 Table 1", "Select appropriate covered diagnosis", "Reorder diagnoses with covered code first"]

EXAMPLES OF BAD CORRECTIONS (AVOID):
✗ "Revise the LCD to include {dx_code}" → Providers cannot modify LCDs, only comply with them
✗ "Remove modifier from {dx_code}" → Cannot apply modifiers to diagnoses (only to procedures)
✗ "Review medical records" → Too vague; specify what documentation to look for

Focus on clear, specific, implementable actions that will get the claim paid.
"""
        
        try:
            response = ollama.generate(
                model="mistral", 
                prompt=prompt,
                options={
                    "num_predict": 4096,  # Increased from 2048 to allow longer responses
                    "temperature": 0.3,
                    "top_p": 0.85
                }
            )
            text = response.get('response','{}')
            
            # Debug: Log raw LLM response
            print(f"\n{'='*80}")
            print(f"RAW LLM RESPONSE for {proc_code} + {dx_code} (Archetype: {archetype}):")
            print(f"{'='*80}")
            print(text[:2000])  # First 2000 chars (increased from 1000)
            print(f"{'='*80}\n")
            
            try:
                parsed = json.loads(text)
                
                # Debug: Log how many recommendations were generated
                num_recs = len(parsed.get('recommended_corrections', []))
                print(f" Generated {num_recs} recommendation(s) for {proc_code} + {dx_code}")
                
                return parsed
            except json.JSONDecodeError as e:
                print(f" JSON parsing failed for {proc_code} + {dx_code}: {e}")
                return {"raw": text, "parse_error": "LLM response was not valid JSON"}
        except Exception as e:
            print(f" LLM call failed for {proc_code} + {dx_code}: {e}")
            return {"error": str(e)}

    def _stage2(self, issue: Dict[str, Any], stage1: Dict[str, Any]) -> Dict[str, Any]:
        archetype = self._detect_archetype(issue)
        codes = {
            'hcpcs_code': issue.get('hcpcs_code',''),
            'icd9_code': issue.get('icd9_code',''),
            'icd10_code': issue.get('icd10_code',''),
        }
        sql_ev = self.sql_connector.execute_archetype_query(archetype, codes)
        policies = self._search_policies(issue, archetype)
        llm_out = self._run_stage2_llm(issue, archetype, sql_ev, policies)
        return {
            "archetype": archetype,
            "sql_evidence": sql_ev,
            "correction_policies": policies,
            "correction_analysis": llm_out,
            "stage": "sql_driven_archetype_corrective_reasoning"
        }

    def run_archetype_driven_corrections(self, claim_id: str) -> Dict[str, Any]:
        stage1 = self.calibrated_corrector.run_corrections(claim_id)
        issues = stage1.get('enriched_issues') or []
        if not issues:
            return {"claim_id": claim_id, "error": "No claim issues found", "stage1_result": stage1}
        
        print(f"\n Starting Stage 2 processing for {len(issues)} issues...")
        enriched = []
        for idx, issue in enumerate(issues, 1):
            dx = issue.get('icd10_code') or issue.get('icd9_code', 'Unknown')
            proc = issue.get('hcpcs_code', 'Unknown')
            print(f"\n📋 Processing issue {idx}/{len(issues)}: DX={dx}, PROC={proc}")
            
            res = self._stage2(issue, stage1)
            i2 = issue.copy()
            i2['stage2_archetype_correction_analysis'] = res
            i2['archetype_driven_complete'] = True
            enriched.append(i2)
        
        # Count total recommendations generated
        total_recs = sum(len(i.get('stage2_archetype_correction_analysis', {}).get('correction_analysis', {}).get('recommended_corrections', [])) for i in enriched)
        print(f"\n Stage 2 Complete: Processed {len(enriched)} issues, generated {total_recs} total recommendations\n")
        
        return {"claim_id": claim_id, "enriched_issues": enriched, "total_issues": len(enriched), "processing_timestamp": datetime.now().isoformat(), "version": "3.0"}

    def cleanup(self):
        if self.sql_connector:
            self.sql_connector.close()


if __name__ == "__main__":
    c = ArchetypeDrivenClaimCorrectorV3()
    try:
        out = c.run_archetype_driven_corrections("123456789012345")
        print(json.dumps(out, indent=2))
    finally:
        c.cleanup()



Enhanced Archetype-Driven Claim Corrector v3.0
==============================================

Key updates vs v2:
- ICD version–aware SQL for DX-driven archetypes (ICD-10 or ICD-9)
- GEMs-based fallback mapping when direct version returns 0 rows
- Adds Secondary_DX_Not_Covered definition explicitly
"""

import json
import logging
import warnings
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from claim_corrector_claims3_calibrated import CalibratedClaimCorrector

try:
    import pyodbc
    import pandas as pd
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, VectorParams, PointStruct
    from sentence_transformers import SentenceTransformer
    import ollama
    import numpy as np
except ImportError as e:
    print(f" Missing required library: {e}")
    sys.exit(1)

# Suppress pandas SQLAlchemy warning for pyodbc connections
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*', category=UserWarning)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Archetype Definitions (v3)
# -----------------------------------------------------------------------------

ARCHETYPE_DEFINITIONS = {
    "NCCI_PTP_Conflict": {
        "description": "The CPT/HCPCS combination violates an NCCI Procedure-to-Procedure (PTP) rule.",
        "qdrant_collections": ["claims__ncci_edits", "claims__med_claims_policies"],
        "sql_query": """
            SELECT 
                ncci.procedure_code,
                ncci.ptp_denial_reason,
                ncci.instructions,
                ncci.ptp_edit_type,
                ncci.modifier_status,
                ncci.mue_threshold,
                ncci.mue_denial_type
            FROM [_ncci_].[dbo].[vw_NCCI_Daily_Denial_Alerts] ncci
            WHERE ncci.procedure_code = ?
              AND (ncci.ptp_denial_reason IS NOT NULL OR ncci.mue_threshold IS NOT NULL)
        """,
    },

    # DX-driven archetypes use templated WHERE clause decided at runtime
    "Primary_DX_Not_Covered": {
        "description": "Primary diagnosis is not covered under the relevant LCD or NCD.",
        "qdrant_collections": ["claims__lcd_policies", "claims__ncd_policies"],
        "sql_query": """
            SELECT 
                g.icd9_code,
                g.icd10_code,
                l.lcd_id,
                l.title as lcd_title,
                l.coverage_group_description as coverage_group,
                l.lcd_last_updated as last_updated,
                l.covered_codes,
                l.diagnoses_support,
                l.diagnoses_dont_support
            FROM [_gems].[dbo].[table_2018_I9gem_fixed] g
            LEFT JOIN [_lcd].[dbo].[vw_Master_Denial_Analysis] l
                ON g.icd10_code = l.covered_codes
            WHERE {DX_WHERE}
        """,
    },

    "Secondary_DX_Not_Covered": {
        "description": "Secondary diagnosis is not covered under the relevant LCD or NCD.",
        "qdrant_collections": ["claims__lcd_policies", "claims__ncd_policies"],
        "sql_query": """
            SELECT 
                g.icd9_code,
                g.icd10_code,
                l.lcd_id,
                l.title as lcd_title,
                l.coverage_group_description as coverage_group,
                l.lcd_last_updated as last_updated,
                l.covered_codes,
                l.diagnoses_support,
                l.diagnoses_dont_support
            FROM [_gems].[dbo].[table_2018_I9gem_fixed] g
            LEFT JOIN [_lcd].[dbo].[vw_Master_Denial_Analysis] l
                ON g.icd10_code = l.covered_codes
            WHERE {DX_WHERE}
        """,
    },

    "MUE_Risk": {
        "description": "Billed units exceed the Medically Unlikely Edit (MUE) threshold.",
        "qdrant_collections": ["claims__ncci_edits"],
        "sql_query": """
            SELECT 
                ncci.procedure_code,
                ncci.mue_threshold,
                ncci.mue_denial_type,
                ncci.mue_rationale,
                ncci.instructions
            FROM [_ncci_].[dbo].[vw_NCCI_Daily_Denial_Alerts] ncci
            WHERE ncci.procedure_code = ?
              AND ncci.mue_threshold IS NOT NULL
        """,
    },

    "Bundled_Payment_Conflict": {
        "description": "Claim includes services bundled under DRG/APC or OPPS rules.",
        "qdrant_collections": ["claims__ncci_edits", "claims__med_claims_policies"],
        "sql_query": """
            SELECT 
                ptp.primary_cpt_hcpcs_code,
                ptp.secondary_cpt_hcpcs_code,
                ptp.edit_type,
                ptp.ptp_edit_rationale,
                ptp.modifier_guidance,
                ptp.effective_date
            FROM [_ncci_].[dbo].[vw_NCCI_Modifier_Compliance] ptp
            WHERE ptp.secondary_cpt_hcpcs_code = ?
              AND ptp.edit_type IN ('Bundled', 'Component')
        """,
    },
}


class SQLDatabaseConnector:
    def __init__(self):
        self.connection = None
        self._connect()

    def _connect(self):
        try:
            cs = (
                'Driver={ODBC Driver 18 for SQL Server};'
                'Server=localhost,1433;UID=SA;PWD=Bbanwo@1980!;'
                'Database=_claims;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;'
            )
            self.connection = pyodbc.connect(cs)
            print(" SQL Database connection established successfully")
        except Exception as e:
            print(f" SQL Database connection failed: {e}")
            self.connection = None

    def _map_icd10_to_icd9(self, icd10: str) -> List[str]:
        try:
            q = """
                SELECT DISTINCT icd9_code
                FROM [_gems].[dbo].[table_2018_I9gem_fixed]
                WHERE icd10_code = ?
            """
            df = pd.read_sql(q, self.connection, params=[icd10])
            return [r.icd9_code for _, r in df.iterrows()]
        except Exception:
            return []

    def _map_icd9_to_icd10(self, icd9: str) -> List[str]:
        try:
            q = """
                SELECT DISTINCT icd10_code
                FROM [_gems].[dbo].[table_2018_I9gem_fixed]
                WHERE icd9_code = ?
            """
            df = pd.read_sql(q, self.connection, params=[icd9])
            return [r.icd10_code for _, r in df.iterrows()]
        except Exception:
            return []

    def execute_archetype_query(self, archetype: str, codes: Dict[str, str]) -> List[Dict[str, Any]]:
        if not self.connection:
            return []

        info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        base_sql = info.get('sql_query', '')
        if not base_sql:
            return []

        try:
            # HCPCS-driven archetypes
            if archetype in ["NCCI_PTP_Conflict", "MUE_Risk", "Bundled_Payment_Conflict"]:
                hcpcs = codes.get('hcpcs_code')
                if not hcpcs:
                    return []
                df = pd.read_sql(base_sql, self.connection, params=[hcpcs])
                return df.to_dict('records')

            # DX-driven archetypes with ICD version awareness
            if archetype in ["Primary_DX_Not_Covered", "Secondary_DX_Not_Covered"]:
                icd10 = codes.get('icd10_code') or ''
                icd9 = codes.get('icd9_code') or ''

                results: List[Dict[str, Any]] = []

                def run_dx_query(dx_where: str, param: str) -> List[Dict[str, Any]]:
                    sql = base_sql.replace('{DX_WHERE}', dx_where)
                    df_local = pd.read_sql(sql, self.connection, params=[param])
                    return df_local.to_dict('records')

                # Prefer ICD-10 if provided (starts with a letter)
                tried = []
                if icd10 and icd10[:1].isalpha():
                    tried.append(('g.icd10_code = ?', icd10))
                if icd9 and not icd9[:1].isalpha():
                    tried.append(('g.icd9_code = ?', icd9))

                # If no clear version, still attempt both in order icd10 -> icd9 using available values
                if not tried:
                    if icd10:
                        tried.append(('g.icd10_code = ?', icd10))
                    if icd9:
                        tried.append(('g.icd9_code = ?', icd9))

                # Execute attempts
                for dx_where, param in tried:
                    rows = run_dx_query(dx_where, param)
                    if rows:
                        results.extend(rows)
                        break

                # Fallback via GEMs mapping if empty
                if not results:
                    if icd10:
                        mapped_icd9 = self._map_icd10_to_icd9(icd10)
                        for m in mapped_icd9:
                            rows = run_dx_query('g.icd9_code = ?', m)
                            if rows:
                                results.extend(rows)
                                break
                    if not results and icd9:
                        mapped_icd10 = self._map_icd9_to_icd10(icd9)
                        for m in mapped_icd10:
                            rows = run_dx_query('g.icd10_code = ?', m)
                            if rows:
                                results.extend(rows)
                                break

                return results

            return []
        except Exception as e:
            print(f" SQL query failed for archetype '{archetype}': {e}")
            return []

    def close(self):
        if self.connection:
            self.connection.close()


class ArchetypeDrivenClaimCorrectorV3:
    def __init__(self):
        print(" Initializing Enhanced Archetype-Driven Claim Corrector v3.0...")
        self.calibrated_corrector = CalibratedClaimCorrector()
        self.sql_connector = SQLDatabaseConnector()
        self.qdrant_client = None
        self.embedder = None
        self._init_qdrant()
        self._init_embedder()
        if hasattr(self.calibrated_corrector, 'embedder'):
            self.embedder = self.calibrated_corrector.embedder
        print(" Enhanced Archetype-Driven Claim Corrector v3.0 initialized successfully")

    def _init_qdrant(self):
        try:
            self.qdrant_client = QdrantClient(host="localhost", port=6333)
            print(" Qdrant client connected successfully")
        except Exception as e:
            print(f" Qdrant initialization failed: {e}")
            self.qdrant_client = None

    def _init_embedder(self):
        try:
            self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
            print(" Sentence transformer model loaded successfully")
        except Exception as e:
            print(f" Embedder initialization failed: {e}")
            self.embedder = None

    def _detect_archetype(self, issue: Dict[str, Any]) -> str:
        if issue.get('ptp_denial_reason') and issue.get('hcpcs_position') == 1:
            return "NCCI_PTP_Conflict"
        if issue.get('mue_threshold'):
            return "MUE_Risk"
        if issue.get('lcd_icd10_covered_group') == 'N' and issue.get('dx_position') == 1:
            return "Primary_DX_Not_Covered"
        if issue.get('lcd_icd10_covered_group') == 'N' and issue.get('dx_position') > 1:
            return "Secondary_DX_Not_Covered"
        return "NCCI_PTP_Conflict" if issue.get('ptp_denial_reason') else "MUE_Risk"

    def _build_query_text(self, issue: Dict[str, Any], archetype: str) -> str:
        parts = [f"CPT {issue.get('hcpcs_code','')}"]
        if issue.get('icd10_code'):
            parts.append(f"ICD {issue.get('icd10_code')}")
        if issue.get('icd9_code'):
            parts.append(f"ICD {issue.get('icd9_code')}")
        if archetype == "NCCI_PTP_Conflict":
            parts.extend(["NCCI","PTP","modifier"]) 
        elif archetype.endswith("Not_Covered"):
            parts.extend(["LCD","coverage","diagnosis"]) 
        elif archetype == "MUE_Risk":
            parts.extend(["MUE","units","threshold"]) 
        return " ".join(parts)

    def _search_policies(self, issue: Dict[str, Any], archetype: str) -> List[Dict[str, Any]]:
        if not self.qdrant_client or not self.embedder:
            return []
        colls = ARCHETYPE_DEFINITIONS.get(archetype, {}).get('qdrant_collections', [])
        vector = self.embedder.encode(self._build_query_text(issue, archetype)).tolist()
        results: List[Dict[str, Any]] = []
        try:
            all_colls = [c.name for c in self.qdrant_client.get_collections().collections]
            for c in colls:
                if c not in all_colls:
                    print(f"    Collection {c} does not exist, skipping")
                    continue
                hits = self.qdrant_client.search(collection_name=c, query_vector=vector, limit=3, score_threshold=0.7)
                for h in hits:
                    p = h.payload or {}
                    p['score'] = h.score
                    p['collection'] = c
                    results.append(p)
        except Exception as e:
            print(f"    Policy search failed: {e}")
        results.sort(key=lambda x: x.get('score',0), reverse=True)
        return results[:5]

    def _run_stage2_llm(self, issue: Dict[str, Any], archetype: str, sql_evidence: List[Dict[str, Any]], policies: List[Dict[str, Any]]) -> Dict[str, Any]:
        # Extract codes and identify types
        dx_code = issue.get('icd10_code') or issue.get('icd9_code') or ''
        proc_code = issue.get('hcpcs_code') or ''
        
        # Build code type information
        code_info = f"""
CODE IDENTIFICATION:
- PROCEDURE CODE (CPT/HCPCS): {proc_code} → Use for: modifier checks, PTP edits, MUE review, prior auth, bundling
- DIAGNOSIS CODE (ICD-10/ICD-9): {dx_code} → Use for: coverage validation, medical necessity, LCD/NCD compliance

CRITICAL RULES:
1. Modifiers (59, 25, XE, XS, XP, XU) apply ONLY to PROCEDURE codes, NEVER to DIAGNOSIS codes
2. If a code starts with a letter and is ≤7 chars (e.g., M16.11, Z99.89), it is ICD - do NOT assign modifiers
3. Only 5-digit numeric or alphanumeric codes (e.g., 27130, G0299) are procedures that can have modifiers
"""
        
        # Create a structured prompt for correction recommendations
        prompt = f"""
You are a CMS-compliant claim denial correction expert with deep knowledge of medical billing.

{code_info}

ARCHETYPE: {archetype}
RISK LEVEL: {issue.get('denial_risk_level', '')}
ACTION REQUIRED: {issue.get('action_required', '')}

SQL EVIDENCE (Historical Claims Data):
{json.dumps(sql_evidence[:3], indent=2) if sql_evidence else "No SQL evidence available"}

POLICY EXCERPTS (CMS Guidelines):
{json.dumps([{k: v for k, v in p.items() if k in ['title', 'text', 'source', 'policy_number']} for p in policies[:3]], indent=2) if policies else "No policies found"}

COMPLIANCE REQUIREMENTS:
- Only recommend actions a PROVIDER or CODER can perform on their claim
- NEVER instruct users to "revise," "update," or "modify" CMS policy documents (LCDs, NCDs, Articles)
- Providers can only COMPLY with policies, not change them
- Be SPECIFIC: reference exact claim fields, documentation requirements, or SQL queries
- Separate diagnosis logic from procedure logic:
  * Diagnosis ({dx_code}): coverage verification, medical necessity documentation
  * Procedure ({proc_code}): modifier application, bundling checks, prior authorization
- YOU MUST GENERATE AT LEAST ONE ACTIONABLE RECOMMENDATION - this is a denial risk that needs resolution!

REQUIRED OUTPUT FORMAT (valid JSON only):
{{
  "claim_id": "{issue.get('claim_id', '')}",
  "archetype": "{archetype}",
  "sql_evidence_summary": "Summary of what the SQL database evidence shows (denial rates, common issues, patterns)",
  "recommended_corrections": [
    {{
      "field": "specific_claim_field_name (e.g., 'procedure_modifier', 'primary_diagnosis_position', 'units_billed')",
      "current_value": "what is currently on the claim",
      "suggested_value": "exact new value to enter or add",
      "rationale": "why this change is needed based on CMS policy",
      "policy_citation": "specific LCD/NCD/NCCI edit number and section (e.g., 'LCD L12345 §3.2, NCCI PTP edit 27130-27447')",
      "implementation_steps": [
        "Step 1: Specific action with exact field/form location (e.g., 'In claim line 2, locate the Modifier field')",
        "Step 2: Specific action with exact value (e.g., 'Enter modifier 59 to indicate distinct procedural service')",
        "Step 3: Specific verification (e.g., 'Verify operative note documents separate incision site')"
      ],
      "confidence": 0.85
    }}
  ],
  "policy_references": ["LCD L12345", "NCCI PTP Manual Ch.2"],
  "final_guidance": "Overall strategy to resolve this denial risk",
  "compliance_checklist": ["Verification step 1 with specific requirement", "Verification step 2 with specific requirement"]
}}

EXAMPLES OF GOOD CORRECTIONS:
✓ Field: "procedure_modifier" | Current: "none" | Suggested: "59" | Rationale: "NCCI PTP edit requires modifier 59 to bypass bundling" | Steps: ["Open claim line for CPT {proc_code}", "Add modifier 59 to indicate distinct procedural service", "Attach operative note documenting separate incision site"]
✓ Field: "primary_diagnosis" | Current: "{dx_code}" | Suggested: "Add covered diagnosis from LCD L33822" | Rationale: "{dx_code} is not in LCD covered codes list" | Steps: ["Review LCD L33822 Table 1", "Select appropriate covered diagnosis", "Reorder diagnoses with covered code first"]

EXAMPLES OF BAD CORRECTIONS (AVOID):
✗ "Revise the LCD to include {dx_code}" → Providers cannot modify LCDs, only comply with them
✗ "Remove modifier from {dx_code}" → Cannot apply modifiers to diagnoses (only to procedures)
✗ "Review medical records" → Too vague; specify what documentation to look for

Focus on clear, specific, implementable actions that will get the claim paid.
"""
        
        try:
            response = ollama.generate(
                model="mistral", 
                prompt=prompt,
                options={
                    "num_predict": 4096,  # Increased from 2048 to allow longer responses
                    "temperature": 0.3,
                    "top_p": 0.85
                }
            )
            text = response.get('response','{}')
            
            # Debug: Log raw LLM response
            print(f"\n{'='*80}")
            print(f"RAW LLM RESPONSE for {proc_code} + {dx_code} (Archetype: {archetype}):")
            print(f"{'='*80}")
            print(text[:2000])  # First 2000 chars (increased from 1000)
            print(f"{'='*80}\n")
            
            try:
                parsed = json.loads(text)
                
                # Debug: Log how many recommendations were generated
                num_recs = len(parsed.get('recommended_corrections', []))
                print(f" Generated {num_recs} recommendation(s) for {proc_code} + {dx_code}")
                
                return parsed
            except json.JSONDecodeError as e:
                print(f" JSON parsing failed for {proc_code} + {dx_code}: {e}")
                return {"raw": text, "parse_error": "LLM response was not valid JSON"}
        except Exception as e:
            print(f" LLM call failed for {proc_code} + {dx_code}: {e}")
            return {"error": str(e)}

    def _stage2(self, issue: Dict[str, Any], stage1: Dict[str, Any]) -> Dict[str, Any]:
        archetype = self._detect_archetype(issue)
        codes = {
            'hcpcs_code': issue.get('hcpcs_code',''),
            'icd9_code': issue.get('icd9_code',''),
            'icd10_code': issue.get('icd10_code',''),
        }
        sql_ev = self.sql_connector.execute_archetype_query(archetype, codes)
        policies = self._search_policies(issue, archetype)
        llm_out = self._run_stage2_llm(issue, archetype, sql_ev, policies)
        return {
            "archetype": archetype,
            "sql_evidence": sql_ev,
            "correction_policies": policies,
            "correction_analysis": llm_out,
            "stage": "sql_driven_archetype_corrective_reasoning"
        }

    def run_archetype_driven_corrections(self, claim_id: str) -> Dict[str, Any]:
        stage1 = self.calibrated_corrector.run_corrections(claim_id)
        issues = stage1.get('enriched_issues') or []
        if not issues:
            return {"claim_id": claim_id, "error": "No claim issues found", "stage1_result": stage1}
        
        print(f"\n Starting Stage 2 processing for {len(issues)} issues...")
        enriched = []
        for idx, issue in enumerate(issues, 1):
            dx = issue.get('icd10_code') or issue.get('icd9_code', 'Unknown')
            proc = issue.get('hcpcs_code', 'Unknown')
            print(f"\n📋 Processing issue {idx}/{len(issues)}: DX={dx}, PROC={proc}")
            
            res = self._stage2(issue, stage1)
            i2 = issue.copy()
            i2['stage2_archetype_correction_analysis'] = res
            i2['archetype_driven_complete'] = True
            enriched.append(i2)
        
        # Count total recommendations generated
        total_recs = sum(len(i.get('stage2_archetype_correction_analysis', {}).get('correction_analysis', {}).get('recommended_corrections', [])) for i in enriched)
        print(f"\n Stage 2 Complete: Processed {len(enriched)} issues, generated {total_recs} total recommendations\n")
        
        return {"claim_id": claim_id, "enriched_issues": enriched, "total_issues": len(enriched), "processing_timestamp": datetime.now().isoformat(), "version": "3.0"}

    def cleanup(self):
        if self.sql_connector:
            self.sql_connector.close()


if __name__ == "__main__":
    c = ArchetypeDrivenClaimCorrectorV3()
    try:
        out = c.run_archetype_driven_corrections("123456789012345")
        print(json.dumps(out, indent=2))
    finally:
        c.cleanup()




