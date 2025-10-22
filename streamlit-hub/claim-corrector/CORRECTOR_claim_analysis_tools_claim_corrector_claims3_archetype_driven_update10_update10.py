#!/usr/bin/env python3
"""
Archetype-Driven CMS Claim Corrector with Second Handshake - UPDATE 4
Stage 1: Uses calibrated denial reasoning (from claim_corrector_claims3_calibrated.py)
Stage 2: Archetype-driven corrective reasoning with targeted policy search

UPDATE 1 CHANGES:
- Fixed ICD-9/ICD-10 mismatch in SQL queries
- Added smart ICD version detection (ICD-10 starts with letter, ICD-9 is numeric)
- Added GEMs fallback mapping for ICD-10  ICD-9 conversion
- Dynamic SQL query construction based on available ICD version

UPDATE 2 CHANGES:
- Suppressed pandas SQLAlchemy warning for pyodbc connections
- Added warnings filter to silence UserWarning about DBAPI2 connections

UPDATE 3 CHANGES:
- Fixed LCD JOIN logic (was joining ICD to HCPCS - impossible match)
- Added robust LLM JSON parsing with multiple fallback strategies
- Added specific code recommendation mappings (no more "find a code")
- Added SQL evidence validation (detect empty/NULL-only results)
- Added modifier logic validation (respect "Modifier Not Allowed" status)
- Added structured fallback corrections when LLM or SQL fails
- Removed broken LCD JOIN (diagnoses_support is always NULL in your DB)

UPDATE 4 CHANGES:
- Replaced hardcoded ICD alternatives with DATABASE-DRIVEN lookups
- Added _get_icd10_alternatives_from_db() method using GEMS shared ICD-9 strategy
- Added _get_icd10_description() method using icd10cm_codes_2018_fixed table
- Alternative codes now dynamically queried from SQL with descriptions
- Fallback to pattern-based search (same code family) if no GEMS alternatives
- No more static ICD10_COVERAGE_ALTERNATIVES dictionary

UPDATE 5 CHANGES:
- Fixed ICD-10 decimal normalization issue in GEMS table queries
- Added _normalize_icd10_for_gems() helper to remove decimals for GEMS queries
- GEMS table stores codes without decimals (M1611) but description table has decimals (M16.11)
- Updated _map_icd10_to_icd9(), _get_icd10_alternatives_from_db() to normalize codes
- Ensures M16.11  M1611 when querying GEMS, then converts back for display

UPDATE 6 CHANGES:
- Migrated from table_2018_I9gem_fixed to vw_icd9_to_icd10_master VIEW
- Master view contains descriptions (no separate lookup needed)
- Bidirectional mapping support (ICD-9  ICD-10)
- Added mapping_type='CM' filter for diagnosis codes
- Simplified _get_icd10_description() - now uses master view
- Updated _map_icd10_to_icd9() to use master view
- Updated _get_icd10_alternatives_from_db() to use master view with built-in descriptions
- M1611 now correctly maps to ICD-9 71515 with description
"""

import json
import re
import subprocess
import warnings
from typing import Dict, Any, List, Tuple, Optional
from sentence_transformers import SentenceTransformer
import torch
from qdrant_client import QdrantClient
from qdrant_client import models
import pyodbc
import pandas as pd

# Suppress pandas SQLAlchemy warning for pyodbc connections
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*', category=UserWarning)

# -------------------------------------------------------------------------
# SAFE OLLAMA EXECUTION (Prevents subprocess deadlocks)
# -------------------------------------------------------------------------

def run_ollama_safe(prompt: str, timeout: int = 60) -> Tuple[str, str, int]:
    """
    Safe Ollama execution that avoids subprocess deadlocks.
    Uses temp file input and Popen with proper buffering.
    
    Returns: (stdout, stderr, return_code)
    """
    import tempfile
    import os
    
    cmd = ["ollama", "run", "mistral"]
    
    # Write prompt to temporary file to avoid command line limits
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
        f.write(prompt)
        temp_file = f.name
    
    try:
        # Use file input instead of command line argument
        with open(temp_file, 'r', encoding='utf-8') as f:
            process = subprocess.Popen(
                cmd,
                stdin=f,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,  # Line buffered
                universal_newlines=True
            )
        
        # Collect output with timeout
        try:
            stdout, stderr = process.communicate(timeout=timeout)
            return_code = process.returncode
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            return_code = -1
            stderr = f"Timeout after {timeout} seconds"
        
        return stdout, stderr, return_code
        
    except Exception as e:
        return "", f"Subprocess error: {str(e)}", -1
    finally:
        # Clean up temp file
        try:
            os.unlink(temp_file)
        except:
            pass

# -------------------------------------------------------------------------
# STATIC CPT MAPPING (No Database Dependency)
# -------------------------------------------------------------------------

def get_cpt_description(cpt_code: str) -> str:
    """Get CPT description from static mapping (database-free approach)"""
    cpt_mappings = {
        "27130": "Total hip arthroplasty",
        "27447": "Total knee arthroplasty",
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
        "74178": "CT abdomen and pelvis without contrast",
        "93000": "Electrocardiogram, routine ECG with at least 12 leads",
        "80053": "Comprehensive metabolic panel",
        "G0299": "Direct skilled nursing services of a registered nurse"
    }
    
    return cpt_mappings.get(cpt_code, f"Medical procedure {cpt_code}")

# -------------------------------------------------------------------------
# ARCHETYPE DEFINITIONS REGISTRY (UPDATE3: Removed broken LCD JOIN)
# -------------------------------------------------------------------------

ARCHETYPE_DEFINITIONS = {
    "NCCI_PTP_Conflict": {
        "description": "The CPT/HCPCS combination violates an NCCI Procedure-to-Procedure (PTP) rule.",
        "trigger_condition": "ptp_denial_reason IS NOT NULL AND hcpcs_position = 1",
        "risk_category": "CRITICAL",
        "business_impact": "FULL DENIAL: Primary procedure will be denied",
        "action_required": "IMMEDIATE: Fix PTP conflict or claim will be denied",
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
        "sql_insight": "Finds primary procedures that violate NCCI edits and provides the edit rationale and type.",
        "correction_strategies": [
            "Add a valid NCCI modifier (59, XE, XP, XS, XU) to indicate distinct procedural service.",
            "Split procedures into separate claim lines when appropriate.",
            "Verify same-day procedure compatibility per NCCI edits."
        ],
        "sample_reference": "Medicare NCCI Policy Manual, Chapter I, E.1"
    },
    "Primary_DX_Not_Covered": {
        "description": "Primary ICD-10 diagnosis is not covered under the relevant LCD or NCD.",
        "trigger_condition": "lcd_icd10_covered_group = 'N' AND dx_position = 1",
        "risk_category": "CRITICAL",
        "business_impact": "FULL DENIAL: Entire claim will be rejected",
        "action_required": "IMMEDIATE: Add covered diagnosis or claim will be rejected",
        "qdrant_collections": ["claims__lcd_policies", "claims__ncd_policies"],
        #  UPDATE3: Simplified query - removed broken LCD JOIN
        "sql_query": """
            SELECT 
                g.icd9_code,
                g.icd10_code,
                '2018 GEMS Crosswalk' as source_table
            FROM [_gems].[dbo].[table_2018_I9gem_fixed] g
            WHERE {DX_WHERE}
        """,
        "sql_insight": "Provides ICD-9/ICD-10 crosswalk data. LCD coverage determined by hardcoded mappings.",
        "correction_strategies": [
            "Replace the ICD-10 diagnosis with a covered diagnosis per clinical guidelines.",
            "Validate medical necessity using standard coverage criteria.",
            "Ensure diagnosis supports medical necessity of CPT/HCPCS code."
        ],
        "sample_reference": "CMS ICD-10 Coverage Guidelines"
    },
    "MUE_Risk": {
        "description": "Billed units exceed the Medically Unlikely Edit (MUE) threshold for this HCPCS/CPT code.",
        "trigger_condition": "mue_threshold IS NOT NULL",
        "risk_category": "HIGH",
        "business_impact": "PARTIAL DENIAL: Units may be reduced",
        "action_required": "REVIEW: Verify documentation supports units billed",
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
        "sql_insight": "Identifies where claim line units exceed CMS MUE limits and flags corresponding MAI (13) levels.",
        "correction_strategies": [
            "Reduce billed units to  MUE limit for the HCPCS/CPT code.",
            "Include medical necessity documentation for exceeding MUE threshold.",
            "Verify if MUE has MAI of 1 (line edit) or 2/3 (date of service edit)."
        ],
        "sample_reference": "NCCI MUE Table  CMS Transmittal 12674"
    },
    "NCD_Terminated": {
        "description": "National Coverage Determination for this procedure is terminated or expired.",
        "trigger_condition": "ncd_status = 'Terminated'",
        "risk_category": "HIGH",
        "business_impact": "COVERAGE RISK: May affect reimbursement",
        "action_required": "REVIEW: Check if NCD termination affects coverage",
        "qdrant_collections": ["claims__ncd_policies"],
        "sql_query": """
            SELECT 
                ncd.NCD_id,
                ncd.NCD_mnl_sect_title AS ncd_title,
                ncd.NCD_trmntn_dt,
                ncd.NCD_efctv_dt,
                ncd.NCD_impltn_dt,
                ncd.itm_srvc_desc,
                ncd.indctn_lmtn
            FROM [_ncd].[dbo].[ncd_trkg] ncd
            WHERE ncd.NCD_lab = ?
              AND ncd.NCD_trmntn_dt IS NOT NULL
        """,
        "sql_insight": "Shows claims linked to terminated NCDs so you can suggest replacement NCDs or LCD alternatives.",
        "correction_strategies": [
            "Check for new or replacement NCD covering the procedure.",
            "If no active NCD, seek local LCD guidance from MAC.",
            "Document medical necessity to support coverage under general benefit category rules."
        ],
        "sample_reference": "NCD Manual Pub 100-03, Terminated Sections"
    },
    "Secondary_DX_Not_Covered": {
        "description": "A secondary diagnosis is non-covered but not primary; limited impact on payment.",
        "trigger_condition": "lcd_icd10_covered_group = 'N' AND dx_position > 1",
        "risk_category": "MEDIUM",
        "business_impact": "MINIMAL IMPACT: Secondary diagnosis issue",
        "action_required": "MONITOR: Secondary diagnosis not covered",
        "qdrant_collections": ["claims__lcd_policies"],
        #  UPDATE3: Simplified query - removed broken LCD JOIN
        "sql_query": """
            SELECT 
                g.icd9_code,
                g.icd10_code,
                '2018 GEMS Crosswalk' as source_table
            FROM [_gems].[dbo].[table_2018_I9gem_fixed] g
            WHERE {DX_WHERE}
        """,
        "sql_insight": "Provides ICD-9/ICD-10 crosswalk data. Secondary diagnosis coverage has minimal impact.",
        "correction_strategies": [
            "No immediate action required unless secondary DX is used to justify medical necessity.",
            "Review for co-diagnosis pairings and update if necessary."
        ],
        "sample_reference": "Secondary Diagnosis Coverage Guidelines"
    },
    "Medical_Necessity_Review": {
        "description": "Procedure may lack clinical justification for the diagnosis provided",
        "trigger_condition": "Diagnostic test with musculoskeletal diagnosis",
        "risk_category": "MEDIUM",
        "business_impact": "REVIEW RECOMMENDED: May be denied for lack of medical necessity",
        "action_required": "REVIEW: Verify medical necessity documentation and clinical context",
        "qdrant_collections": ["claims__ncd_policies", "claims__lcd_policies", "claims__med_claims_policies"],
        "sql_query": """
            SELECT 
                'Medical_Necessity' as source_type,
                'No specific NCD/LCD for this procedure' as note,
                'General medical necessity criteria apply' as guidance
        """,
        "sql_insight": "Medical necessity requires documentation of clinical appropriateness",
        "correction_strategies": [
            "Include medical necessity documentation in claim submission",
            "Verify diagnosis supports the medical need for the procedure",
            "Add appropriate diagnosis codes that justify the procedure",
            "Review clinical guidelines for procedure appropriateness"
        ],
        "sample_reference": "CMS Medical Necessity Guidelines"
    },
    "Compliant": {
        "description": "Claim appears compliant and passes all denial risk checks.",
        "trigger_condition": "Default when no other condition is met",
        "risk_category": "LOW",
        "business_impact": "NO IMPACT: Claim should process normally",
        "action_required": "NO ACTION: Claim appears compliant",
        "qdrant_collections": [],
        "sql_query": """
            SELECT 
                'OK' as denial_risk_level,
                'No specific code-level issues detected' as compliance_status,
                'Standard billing process applies' as guidance
        """,
        "sql_insight": "Identifies claims that are clean and can serve as positive training examples for the model.",
        "correction_strategies": [
            "Maintain documentation and continue standard billing process."
        ],
        "sample_reference": "CMS Claims Processing Manual Ch. 12 40"
    }
}

# -------------------------------------------------------------------------
# STAGE 1: CALIBRATED DENIAL REASONING PROMPT
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
# STAGE 2: ARCHETYPE-DRIVEN CORRECTIVE REASONING PROMPT
# -------------------------------------------------------------------------

STAGE2_SQL_DRIVEN_ARCHETYPE_CORRECTION_PROMPT = """
You are a CMS policy correction expert specializing in SQL-driven archetype claim remediation.

ARCHETYPE-BASED INSTRUCTIONS:
1. The detected archetype is: {archetype}
2. Archetype description: {archetype_description}
3. SQL insight: {sql_insight}
4. Correction strategies for this archetype: {correction_strategies}
5. Use SQL evidence + CMS policies to provide fact-driven corrections

ORIGINAL CLAIM DATA:
- CPT/HCPCS: {hcpcs_code} ({procedure_name})
- ICD-10: {icd10_code} ({diagnosis_name})
- Denial Reason: {denial_reason}
- Risk Level: {denial_risk_level}
- Action Required: {action_required}

STAGE 1 CALIBRATED DENIAL ANALYSIS:
{denial_analysis}

SQL EVIDENCE FROM DATABASE:
{sql_evidence}

ARCHETYPE-SPECIFIC CORRECTION POLICIES:
{correction_policies}

{sub_archetype_guidance}

 CRITICAL POLICY CITATION RULES:
1. DO NOT use "ARCHETYPE CORRECTION POLICY 1/2/3" as citations
2. ALWAYS use the " CITE THIS AS:" line shown above each policy
3. Extract the EXACT Source Document, Chapter, and Section from the policy header
4. Format: "source_document.pdf - Chapter X, Section Y"
5. Example: "clm104c23.pdf - Chapter 23, Section 10.1"
6. If no chapter/section, use: "source_document.pdf"

REQUIRED OUTPUT FORMAT (MUST BE VALID JSON):
{{
  "claim_id": "{claim_id}",
  "archetype": "{archetype}",
  "sql_evidence_summary": "Summary of database evidence found",
  "recommended_corrections": [
    {{
      "field": "diagnosis_code|procedure_code|modifier|units|documentation",
      "suggestion": "Specific actionable correction based on SQL evidence + CMS policy",
      "confidence": 0.85,
      "sql_evidence_reference": "Specific database field/table that supports this correction",
      "policy_reference": "USE THE ' CITE THIS AS:' FORMAT - source.pdf - Chapter X, Section Y",
      "implementation_guidance": "Step-by-step instructions for applying the correction"
    }}
  ],
  "policy_references": [
    "Specific manual references from retrieved policies"
  ],
  "final_guidance": "Overall corrective summary based on SQL evidence + archetype",
  "compliance_checklist": [
    "Archetype-specific compliance actions based on database evidence"
  ],
  "evidence_traceability": "Links between SQL data, policies, and recommendations"
}}

CRITICAL: Output MUST be valid JSON. No narrative text outside the JSON structure.
"""

# -------------------------------------------------------------------------
# SQL DATABASE CONNECTION (UPDATE3: Enhanced validation & fallbacks)
# -------------------------------------------------------------------------

class SQLDatabaseConnector:
    """SQL Server connection for archetype-specific evidence gathering"""
    
    def __init__(self, connection_string: str = None):
        """Initialize SQL connection"""
        if connection_string is None:
            self.connection_string = (
                "Driver={ODBC Driver 18 for SQL Server};"
                "Server=localhost,1433;"
                "UID=SA;"
                "PWD=Bbanwo@1980!;"
                "Database=_claims;"
                "Encrypt=yes;"
                "TrustServerCertificate=yes;"
                "Connection Timeout=30;"
            )
        else:
            self.connection_string = connection_string
        
        self.connection = None
        self._connect()
    
    def _connect(self):
        """Establish database connection"""
        try:
            self.connection = pyodbc.connect(self.connection_string)
            print(" SQL Database connection established")
        except Exception as e:
            print(f" SQL Database connection failed: {e}")
            self.connection = None
    
    def _is_icd10(self, code: str) -> bool:
        """Check if code is ICD-10 (starts with letter, 7 chars)"""
        if not code:
            return False
        return code[0].isalpha() and len(code) <= 7
    
    def _normalize_icd10_for_gems(self, icd10_code: str) -> str:
        """
        Normalize ICD-10 code for GEMS table queries (remove decimal).
        GEMS table: M1611 (no decimal)
        Description table: M16.11 (with decimal)
        """
        return icd10_code.replace('.', '').replace('-', '').strip().upper()
    
    def _denormalize_icd10_for_display(self, gems_code: str) -> str:
        """
        Convert GEMS format back to standard ICD-10 format with decimal.
        M1611  M16.11
        """
        if not gems_code or len(gems_code) < 4:
            return gems_code
        
        # Standard ICD-10 format: Letter + 2 digits + decimal + remaining digits
        # e.g., M16.11, S82.201A
        if gems_code[0].isalpha() and gems_code[1:3].isdigit():
            if len(gems_code) > 3:
                return f"{gems_code[:3]}.{gems_code[3:]}"
        
        return gems_code
    
    def _map_icd10_to_icd9(self, icd10: str) -> List[str]:
        """Map ICD-10 code to ICD-9 code(s) using master view (UPDATE6)"""
        if not self.connection or not icd10:
            return []
        try:
            # Normalize: M16.11  M1611 for GEMS query
            normalized_icd10 = self._normalize_icd10_for_gems(icd10)
            
            query = """
                SELECT DISTINCT icd9_code
                FROM [_gems].[dbo].[vw_icd9_to_icd10_master]
                WHERE icd10_code = ? AND mapping_type = 'CM'
            """
            df = pd.read_sql(query, self.connection, params=[normalized_icd10])
            return df['icd9_code'].tolist() if not df.empty else []
        except Exception as e:
            print(f"    ICD-10 to ICD-9 mapping failed: {e}")
            return []
    
    def _map_icd9_to_icd10(self, icd9: str) -> List[str]:
        """Map ICD-9 code to ICD-10 code(s) using master view (UPDATE6)"""
        if not self.connection or not icd9:
            return []
        try:
            query = """
                SELECT DISTINCT icd10_code
                FROM [_gems].[dbo].[vw_icd9_to_icd10_master]
                WHERE icd9_code = ? AND mapping_type = 'CM'
            """
            df = pd.read_sql(query, self.connection, params=[icd9])
            return df['icd10_code'].tolist() if not df.empty else []
        except Exception as e:
            print(f"    ICD-9 to ICD-10 mapping failed: {e}")
            return []
    
    #  UPDATE6: Simplified to use master view (includes descriptions)
    def _get_icd10_description(self, icd10_code: str) -> str:
        """Get ICD-10 description from master view (UPDATE6)"""
        if not self.connection or not icd10_code:
            return ""
        try:
            # Normalize: M16.11  M1611
            normalized_icd10 = self._normalize_icd10_for_gems(icd10_code)
            
            query = """
                SELECT TOP 1 icd10_description
                FROM [_gems].[dbo].[vw_icd9_to_icd10_master]
                WHERE icd10_code = ? AND mapping_type = 'CM'
            """
            df = pd.read_sql(query, self.connection, params=[normalized_icd10])
            return df['icd10_description'].values[0] if not df.empty else ""
        except Exception as e:
            print(f"    ICD-10 description lookup failed: {e}")
            return ""
    
    #  UPDATE4: New method to get database-driven ICD-10 alternatives
    #  UPDATE5: Fixed to normalize ICD-10 codes for GEMS table queries
    #  UPDATE6: Updated to use master view with built-in descriptions
    def _get_icd10_alternatives_from_db(self, icd10_code: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get alternative ICD-10 codes from database using multiple strategies:
        1. GEMS shared ICD-9 mapping (clinically related codes)
        2. Pattern-based (same code family)
        
        UPDATE6: Uses vw_icd9_to_icd10_master with built-in descriptions
        """
        if not self.connection or not icd10_code:
            return []
        
        alternatives = []
        
        try:
            # Normalize: M16.11  M1611 for GEMS query
            normalized_icd10 = self._normalize_icd10_for_gems(icd10_code)
            
            # Strategy 1: Find alternatives via shared ICD-9 mapping (most reliable)
            query_shared_icd9 = f"""
                WITH source_icd9 AS (
                    SELECT DISTINCT icd9_code
                    FROM [_gems].[dbo].[vw_icd9_to_icd10_master]
                    WHERE icd10_code = ? AND mapping_type = 'CM'
                ),
                alternatives AS (
                    SELECT DISTINCT m.icd10_code, m.icd9_code, m.icd10_description
                    FROM [_gems].[dbo].[vw_icd9_to_icd10_master] m
                    INNER JOIN source_icd9 s ON m.icd9_code = s.icd9_code
                    WHERE m.icd10_code != ? AND m.mapping_type = 'CM'
                )
                SELECT TOP {limit} icd10_code, icd9_code, icd10_description
                FROM alternatives
                ORDER BY icd10_code
            """
            df_shared = pd.read_sql(query_shared_icd9, self.connection, params=[normalized_icd10, normalized_icd10])
            
            if not df_shared.empty:
                for _, row in df_shared.iterrows():
                    # Convert GEMS format to display format: M1610  M16.10
                    display_code = self._denormalize_icd10_for_display(row['icd10_code'])
                    alternatives.append({
                        "code": display_code,
                        "description": row['icd10_description'] or "Description not available",
                        "strategy": "GEMS_shared_ICD9",
                        "shared_icd9": row['icd9_code'],
                        "confidence": 0.85  # High confidence - clinically related
                    })
                print(f"    Found {len(alternatives)} alternatives via GEMS shared ICD-9 mapping")
                return alternatives
            
            # Strategy 2: Pattern-based fallback (same code family)
            # Extract category from normalized code (first 3-4 chars)
            normalized_pattern = normalized_icd10[:3] + '%'
            
            query_pattern = f"""
                SELECT TOP {limit} icd10_code, icd10_description
                FROM [_gems].[dbo].[vw_icd9_to_icd10_master]
                WHERE icd10_code LIKE ?
                  AND icd10_code != ?
                  AND mapping_type = 'CM'
                ORDER BY icd10_code
            """
            df_pattern = pd.read_sql(query_pattern, self.connection, params=[normalized_pattern, normalized_icd10])
            
            if not df_pattern.empty:
                for _, row in df_pattern.iterrows():
                    # Convert GEMS format to display format
                    display_code = self._denormalize_icd10_for_display(row['icd10_code'])
                    alternatives.append({
                        "code": display_code,
                        "description": row['icd10_description'] or "Description not available",
                        "strategy": "pattern_based_family",
                        "pattern": normalized_pattern,
                        "confidence": 0.70  # Lower confidence - pattern match only
                    })
                print(f"    Found {len(alternatives)} alternatives via pattern matching ({normalized_pattern})")
                return alternatives
            
            print(f"    No alternatives found for {icd10_code}")
            return []
            
        except Exception as e:
            print(f"    Database-driven alternative lookup failed: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    #  UPDATE3: Added SQL evidence validation
    def _is_empty_record(self, record: Dict) -> bool:
        """Check if SQL record has no useful data (all NULL/None values)"""
        if not record:
            return True
        
        # Ignore metadata fields
        ignore_fields = {'source_table', 'icd9_code', 'icd10_code'}
        
        # Check if all substantive values are None/NULL/empty
        values = [
            v for k, v in record.items() 
            if k not in ignore_fields and v not in [None, '', 'None', 'NULL', 'null']
        ]
        return len(values) == 0
    
    #  UPDATE3: Enhanced execute_archetype_query with validation
    def execute_archetype_query(self, archetype: str, codes: Dict[str, str]) -> List[Dict[str, Any]]:
        """Execute archetype-specific SQL query with smart ICD version detection and validation"""
        if not self.connection:
            print(" No SQL connection available")
            return []
        
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        base_sql = archetype_info.get('sql_query', '')
        
        if not base_sql:
            print(f" No SQL query defined for archetype: {archetype}")
            return []
        
        try:
            # HCPCS-driven archetypes
            if archetype in ["NCCI_PTP_Conflict", "MUE_Risk", "NCD_Terminated", "Compliant"]:
                query_param = codes.get('hcpcs_code')
                if archetype == "Compliant" or not query_param:
                    df = pd.read_sql(base_sql, self.connection)
                else:
                    df = pd.read_sql(base_sql, self.connection, params=[query_param])
                evidence = df.to_dict('records')
                
                #  UPDATE3: Validate evidence quality
                if evidence and not all(self._is_empty_record(r) for r in evidence):
                    print(f"      SQL Evidence: {len(evidence)} records")
                    return evidence
                else:
                    print(f"    SQL Evidence: Empty/NULL records for archetype '{archetype}'")
                    return self._get_fallback_evidence(archetype, codes, "sql_returned_nulls")
            
            # DX-driven archetypes with ICD version awareness
            elif archetype in ["Primary_DX_Not_Covered", "Secondary_DX_Not_Covered"]:
                icd10 = codes.get('icd10_code', '')
                icd9 = codes.get('icd9_code', '')
                
                results: List[Dict[str, Any]] = []
                
                def run_dx_query(dx_where: str, param: str) -> List[Dict[str, Any]]:
                    """Helper to execute diagnosis query with specific WHERE clause"""
                    sql = base_sql.replace('{DX_WHERE}', dx_where)
                    df_local = pd.read_sql(sql, self.connection, params=[param])
                    return df_local.to_dict('records')
                
                # Try available codes in order of preference
                tried = []
                
                if icd10 and self._is_icd10(icd10):
                    tried.append(('g.icd10_code = ?', icd10, 'ICD-10'))
                
                if icd9 and not self._is_icd10(icd9):
                    tried.append(('g.icd9_code = ?', icd9, 'ICD-9'))
                
                if not tried:
                    if icd10:
                        tried.append(('g.icd10_code = ?', icd10, 'ICD-10 (fallback)'))
                    if icd9:
                        tried.append(('g.icd9_code = ?', icd9, 'ICD-9 (fallback)'))
                
                # Execute attempts
                for dx_where, param, version_label in tried:
                    print(f"    Trying {version_label} query: {param}")
                    rows = run_dx_query(dx_where, param)
                    if rows and not all(self._is_empty_record(r) for r in rows):
                        print(f"    Found {len(rows)} valid records using {version_label}")
                        results.extend(rows)
                        break
                
                # GEMs fallback if no results
                if not results:
                    print(f"    No direct match, attempting GEMs mapping...")
                    
                    if icd10:
                        mapped_icd9 = self._map_icd10_to_icd9(icd10)
                        print(f"    Mapped {icd10}  {mapped_icd9}")
                        for mapped_code in mapped_icd9:
                            rows = run_dx_query('g.icd9_code = ?', mapped_code)
                            if rows and not all(self._is_empty_record(r) for r in rows):
                                print(f"    Found {len(rows)} valid records using mapped ICD-9: {mapped_code}")
                                results.extend(rows)
                                break
                    
                    if not results and icd9:
                        mapped_icd10 = self._map_icd9_to_icd10(icd9)
                        print(f"    Mapped {icd9}  {mapped_icd10}")
                        for mapped_code in mapped_icd10:
                            rows = run_dx_query('g.icd10_code = ?', mapped_code)
                            if rows and not all(self._is_empty_record(r) for r in rows):
                                print(f"    Found {len(rows)} valid records using mapped ICD-10: {mapped_code}")
                                results.extend(rows)
                                break
                
                #  UPDATE3: Return enriched fallback if still empty
                if not results or all(self._is_empty_record(r) for r in results):
                    print(f"    No valid SQL evidence, using fallback data")
                    return self._get_fallback_evidence(archetype, codes, "no_lcd_coverage_data")
                
                return results
            
            return []
            
        except Exception as e:
            print(f" SQL query failed for archetype '{archetype}': {e}")
            return self._get_fallback_evidence(archetype, codes, f"sql_error: {str(e)[:100]}")
    
    #  UPDATE3: New method for fallback evidence
    def _get_fallback_evidence(self, archetype: str, codes: Dict, reason: str) -> List[Dict]:
        """Provide meaningful fallback when SQL returns empty/NULL results"""
        
        if archetype == "Primary_DX_Not_Covered":
            return [{
                "icd10_code": codes.get('icd10_code'),
                "icd9_code": codes.get('icd9_code'),
                "status": "NO_LCD_DATA_AVAILABLE",
                "guidance": f"LCD coverage data not available for {codes.get('icd10_code')}",
                "reason": reason,
                "suggested_action": "Use hardcoded alternative diagnosis mappings",
                "data_source": "Fallback - No SQL evidence"
            }]
        
        elif archetype == "Secondary_DX_Not_Covered":
            return [{
                "icd10_code": codes.get('icd10_code'),
                "status": "SECONDARY_DX_LOW_IMPACT",
                "guidance": "Secondary diagnosis not covered - minimal claim impact",
                "reason": reason,
                "suggested_action": "No immediate action required",
                "data_source": "Fallback - No SQL evidence"
            }]
        
        elif archetype == "NCCI_PTP_Conflict":
            return [{
                "procedure_code": codes.get('hcpcs_code'),
                "status": "NO_NCCI_DATA",
                "guidance": "NCCI data not available for this procedure",
                "reason": reason,
                "suggested_action": "Verify procedure compatibility manually",
                "data_source": "Fallback - No SQL evidence"
            }]
        
        return [{
            "status": "NO_EVIDENCE_FOUND",
            "guidance": "No database evidence available",
            "reason": reason,
            "data_source": "Fallback"
        }]
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print(" SQL Database connection closed")

# -------------------------------------------------------------------------
# ARCHETYPE-DRIVEN CLAIM CORRECTOR (UPDATE3: Enhanced with fallbacks)
# -------------------------------------------------------------------------

class ArchetypeDrivenClaimCorrector:
    def __init__(self, url: str = "http://localhost:6333", sql_connection_string: str = None):
        self.client = QdrantClient(url=url)

        self.embedder = SentenceTransformer(
            "nomic-ai/nomic-embed-text-v1.5",
            device="cuda" if torch.cuda.is_available() else "cpu",
            trust_remote_code=True
        )

        all_collections = [c.name for c in self.client.get_collections().collections]
        self.policy_collections = [c for c in all_collections if c.startswith("claims__")]

        print(f" Loaded {len(self.policy_collections)} claims collections:")
        for c in self.policy_collections:
            print(f"   - {c}")

        self.claims_collection = "claim_analysis_metadata"
        
        self.source_mapping = {
            "clm104c": "Medicare Claims Processing Manual",
            "pim83c": "Program Integrity Manual (Administrative Only)", 
            "ncci": "National Correct Coding Initiative",
            "lcd": "Local Coverage Determination",
            "mcm": "Medicare Claims Manual",
            "bpm": "Medicare Benefit Policy Manual"
        }
        
        self.sql_connector = SQLDatabaseConnector(sql_connection_string)

    def run_archetype_driven_corrections(self, claim_id: str) -> Dict[str, Any]:
        """Run archetype-driven two-stage corrections"""
        print("\n" + "="*80)
        print(f"  CLAIM PROCESSING: {claim_id}")
        print("="*80)
        
        issues = self._get_claim_issues(claim_id)
        if not issues:
            print(f"  No issues found for claim {claim_id}")
            print("="*80 + "\n")
            return {"claim_id": claim_id, "enriched_issues": [], "total_issues": 0}

        print(f"  Found {len(issues)} issue(s) to process")
        print("-"*80)
        
        enriched_issues = []
        for idx, issue in enumerate(issues, 1):
            print(f"\n  ISSUE {idx}/{len(issues)}: {issue.get('hcpcs_code', 'N/A')} + {issue.get('icd10_code', 'N/A')}")
            
            cpt_code = issue.get('hcpcs_code', '')
            if cpt_code:
                dynamic_procedure_name = get_cpt_description(cpt_code)
                issue['procedure_name'] = dynamic_procedure_name
                print(f"    Procedure: {dynamic_procedure_name}")
            
            print(f"     STAGE 1: Calibrated denial reasoning analysis...")
            stage1_result = self._stage1_calibrated_denial_reasoning(issue)
            
            print(f"     STAGE 2: Archetype-driven corrective reasoning...")
            stage2_result = self._stage2_archetype_corrective_reasoning(issue, stage1_result)
            
            enriched_issue = {
                **issue,
                "stage1_calibrated_denial_analysis": stage1_result,
                "stage2_archetype_correction_analysis": stage2_result,
                "archetype_driven_complete": True
            }
            enriched_issues.append(enriched_issue)
            
            # Add spacing between issues
            if idx < len(issues):
                print("\n" + "  " + ""*76 + "\n")

        print("\n" + "-"*80)
        print(f"  CLAIM {claim_id} COMPLETE: Processed {len(enriched_issues)} issue(s)")
        print("="*80 + "\n")
        
        return {
            "claim_id": claim_id,
            "enriched_issues": enriched_issues,
            "total_issues": len(enriched_issues)
        }

    def _stage1_calibrated_denial_reasoning(self, issue: Dict[str, Any]) -> Dict[str, Any]:
        """Stage 1: Calibrated denial reasoning using enhanced validation"""
        all_policies = []
        for collection in self.policy_collections:
            policies = self._hybrid_search(collection, issue, top_k=3)
            all_policies.extend(policies)
        
        validated_policies = self._calibrated_validate_and_deduplicate_policies(all_policies, issue)
        print(f"      Retrieved {len(validated_policies)} policies")
        
        stage1_analysis = self._run_calibrated_stage1_llm(issue, validated_policies)
        
        return {
            "policies_analyzed": validated_policies,
            "denial_analysis": stage1_analysis,
            "stage": "calibrated_denial_reasoning"
        }

    def _stage2_archetype_corrective_reasoning(self, issue: Dict[str, Any], stage1_result: Dict[str, Any]) -> Dict[str, Any]:
        """Stage 2: SQL-driven archetype corrective reasoning with sub-archetype classification"""
        archetype = self._detect_archetype(issue)
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        
        print(f"      Archetype: {archetype} - {archetype_info.get('description', '')}")
        
        codes = {
            'hcpcs_code': issue.get('hcpcs_code', ''),
            'icd9_code': issue.get('icd9_code', ''),
            'icd10_code': issue.get('icd10_code', '')
        }
        sql_evidence = self.sql_connector.execute_archetype_query(archetype, codes)
        print(f"      Evidence: {len(sql_evidence)} SQL records")
        
        #  UPDATE10: Classify into sub-archetype for enhanced guidance
        sub_archetype_info = {}
        if archetype == "NCCI_PTP_Conflict":
            sub_archetype_info = self._classify_ptp_subtype(issue, sql_evidence)
            print(f"      Sub-type: {sub_archetype_info.get('sub_archetype')} (Modifier: {'Yes' if sub_archetype_info.get('modifier_allowed') else 'No'})")
        
        elif archetype == "MUE_Risk":
            sub_archetype_info = self._classify_mue_subtype(issue, sql_evidence)
            print(f"      Sub-type: {sub_archetype_info.get('sub_archetype')} (Strictness: {sub_archetype_info.get('strictness')})")
        
        correction_policies = self._search_archetype_corrections(issue, archetype)
        print(f"      Policies: {len(correction_policies)} archetype-specific")
        
        #  UPDATE10: Pass sub-archetype info to LLM for enhanced guidance
        stage2_analysis = self._run_sql_driven_archetype_stage2_llm_robust(
            issue, stage1_result, correction_policies, archetype, sql_evidence, sub_archetype_info
        )
        
        return {
            "archetype": archetype,
            "archetype_info": archetype_info,
            "sub_archetype_info": sub_archetype_info,  #  UPDATE10: Include sub-type metadata
            "sql_evidence": sql_evidence,
            "correction_policies": correction_policies,
            "correction_analysis": stage2_analysis,
            "stage": "sql_driven_archetype_corrective_reasoning"
        }

    #  UPDATE10: Sub-archetype classification functions
    def _classify_ptp_subtype(self, issue: Dict[str, Any], sql_evidence: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Classify PTP conflict into specific sub-type based on rationale"""
        
        # Get rationale from SQL evidence or issue
        rationale = ""
        if sql_evidence:
            rationale = sql_evidence[0].get('ptp_edit_rationale', '') or sql_evidence[0].get('ptp_denial_reason', '')
        if not rationale:
            rationale = issue.get('ptp_denial_reason', '')
        
        if not rationale:
            return {'sub_archetype': 'PTP_UNCLASSIFIED', 'modifier_allowed': True, 'guidance': 'Review specific PTP edit'}
        
        text_lower = rationale.lower()
        
        # Classify based on discovered patterns
        if "mutually exclusive" in text_lower or "cannot be reported together" in text_lower:
            return {
                'sub_archetype': 'PTP_MUTUALLY_EXCLUSIVE',
                'modifier_allowed': False,
                'guidance': 'Procedures are mutually exclusive - bill only one (typically the more comprehensive)',
                'reference': 'NCCI Manual Chapter 11, Section 11.1',
                'business_impact': 'CRITICAL - Absolute denial if both billed'
            }
        
        elif "separate procedure" in text_lower:
            return {
                'sub_archetype': 'PTP_SEPARATE_PROCEDURE',
                'modifier_allowed': True,
                'guidance': 'Add modifier 59, XE, XP, XS, or XU to indicate distinct procedural service',
                'reference': 'NCCI Manual Chapter 11, Section 11.2 + CPT Appendix E',
                'business_impact': 'MEDIUM - May be separately billable with modifier'
            }
        
        elif "anesthesia" in text_lower or "standard preparation" in text_lower or "monitoring" in text_lower:
            return {
                'sub_archetype': 'PTP_ANESTHESIA_INCLUDED',
                'modifier_allowed': False,
                'guidance': 'Service is included in anesthesia/surgical global package - do not bill separately',
                'reference': 'NCCI Manual Chapter 11, Section 11.3',
                'business_impact': 'MEDIUM - Bundled into primary procedure'
            }
        
        elif "bundled" in text_lower or "component" in text_lower or "included" in text_lower:
            return {
                'sub_archetype': 'PTP_BUNDLED_SERVICE',
                'modifier_allowed': True,
                'guidance': 'Component service bundled into comprehensive code - may require modifier if distinct',
                'reference': 'NCCI Manual Chapter 11',
                'business_impact': 'HIGH - Typically bundled unless documented as distinct'
            }
        
        elif "cpt manual" in text_lower or "cms manual" in text_lower or "coding instruction" in text_lower:
            return {
                'sub_archetype': 'PTP_MANUAL_INSTRUCTION',
                'modifier_allowed': True,
                'guidance': 'Consult CPT Manual or CMS manual for specific coding instructions',
                'reference': 'CPT Manual + NCCI Manual Chapter 11',
                'business_impact': 'HIGH - Requires case-by-case review'
            }
        
        elif "hcpcs" in text_lower and "definition" in text_lower:
            return {
                'sub_archetype': 'PTP_CODE_DEFINITION',
                'modifier_allowed': True,
                'guidance': 'Review HCPCS code definition for bundling rules',
                'reference': 'HCPCS Code Definitions + NCCI Manual',
                'business_impact': 'HIGH - Based on code definition'
            }
        
        elif "standard" in text_lower or "routine" in text_lower:
            return {
                'sub_archetype': 'PTP_STANDARD_SERVICE',
                'modifier_allowed': False,
                'guidance': 'Standard/routine service included in primary procedure',
                'reference': 'NCCI Manual Chapter 11, Section 11.3',
                'business_impact': 'MEDIUM - Bundled into global package'
            }
        
        else:
            return {
                'sub_archetype': 'PTP_OTHER',
                'modifier_allowed': True,
                'guidance': 'Review specific PTP edit rationale for guidance',
                'reference': 'NCCI Manual Chapter 11',
                'business_impact': 'VARIES - Case-by-case'
            }
    
    def _classify_mue_subtype(self, issue: Dict[str, Any], sql_evidence: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Classify MUE into specific sub-type based on rationale and adjudication"""
        
        # Get rationale and adjudication from SQL evidence
        rationale = ""
        adjudication = ""
        
        if sql_evidence:
            rationale = sql_evidence[0].get('mue_rationale', '')
            adjudication = sql_evidence[0].get('mue_adjudication_indicator', '')
        
        if not rationale:
            return {'sub_archetype': 'MUE_UNCLASSIFIED', 'strictness': 'MEDIUM', 'guidance': 'Review MUE limit'}
        
        text_lower = rationale.lower()
        
        # Classify based on discovered patterns
        if "cms policy" in text_lower:
            return {
                'sub_archetype': 'MUE_CMS_POLICY',
                'adjudication_type': adjudication,
                'strictness': 'CRITICAL',
                'guidance': 'Policy-based limit - non-negotiable, adhere strictly to MUE',
                'reference': 'NCCI Manual Chapter 10 + Specific CMS Policy',
                'business_impact': 'CRITICAL - Hard policy limit'
            }
        
        elif "clinical" in text_lower:
            return {
                'sub_archetype': 'MUE_CLINICAL_JUDGMENT',
                'adjudication_type': adjudication,
                'strictness': 'HIGH',
                'guidance': 'Clinical judgment threshold - may require medical necessity documentation for exceptions',
                'reference': 'NCCI Manual Chapter 10, Section 10.3',
                'business_impact': 'HIGH - Clinical review required for exceptions'
            }
        
        elif "anatomic" in text_lower or "bilateral" in text_lower or "unilateral" in text_lower:
            return {
                'sub_archetype': 'MUE_ANATOMIC_CONSIDERATION',
                'adjudication_type': adjudication,
                'strictness': 'CRITICAL',
                'guidance': 'Hard limit based on anatomy (e.g., 2 for bilateral) - verify anatomical accuracy',
                'reference': 'NCCI Manual Chapter 10, Section 10.2',
                'business_impact': 'CRITICAL - Hard anatomic limit'
            }
        
        elif "code descriptor" in text_lower or "cpt instruction" in text_lower:
            return {
                'sub_archetype': 'MUE_CODE_DESCRIPTOR',
                'adjudication_type': adjudication,
                'strictness': 'MEDIUM',
                'guidance': 'Limit based on CPT code descriptor - consult CPT Manual for definition',
                'reference': 'CPT Manual + NCCI Manual Chapter 10',
                'business_impact': 'MEDIUM - Based on code definition'
            }
        
        elif "nature of" in text_lower:
            return {
                'sub_archetype': 'MUE_NATURE_OF_SERVICE',
                'adjudication_type': adjudication,
                'strictness': 'MEDIUM',
                'guidance': 'Limit based on service nature (analyte, equipment, procedure)',
                'reference': 'NCCI Manual Chapter 10, Section 10.4',
                'business_impact': 'MEDIUM - Service-specific limit'
            }
        
        elif "prescribing information" in text_lower:
            return {
                'sub_archetype': 'MUE_PRESCRIBING_INFO',
                'adjudication_type': adjudication,
                'strictness': 'MEDIUM',
                'guidance': 'Limit based on drug prescribing information',
                'reference': 'NCCI Manual Chapter 10 + Drug prescribing info',
                'business_impact': 'MEDIUM - RX-specific limit'
            }
        
        elif "discontinued" in text_lower:
            return {
                'sub_archetype': 'MUE_DISCONTINUED',
                'adjudication_type': adjudication,
                'strictness': 'CRITICAL',
                'guidance': 'Drug/code discontinued - use alternative code',
                'reference': 'CMS Code Updates',
                'business_impact': 'CRITICAL - Code no longer valid'
            }
        
        elif "oral medication" in text_lower:
            return {
                'sub_archetype': 'MUE_ORAL_MEDICATION',
                'adjudication_type': adjudication,
                'strictness': 'HIGH',
                'guidance': 'Oral medication restrictions apply',
                'reference': 'NCCI Manual Chapter 10',
                'business_impact': 'HIGH - May not be payable'
            }
        
        elif "workgroup" in text_lower:
            return {
                'sub_archetype': 'MUE_WORKGROUP_DETERMINATION',
                'adjudication_type': adjudication,
                'strictness': 'HIGH',
                'guidance': 'Limit determined by CMS clinical workgroup',
                'reference': 'NCCI Manual Chapter 10, CMS Workgroup',
                'business_impact': 'HIGH - Expert clinical determination'
            }
        
        elif "data" in text_lower:
            return {
                'sub_archetype': 'MUE_DATA_DRIVEN',
                'adjudication_type': adjudication,
                'strictness': 'HIGH',
                'guidance': 'Limit based on claims data analysis',
                'reference': 'NCCI Manual Chapter 10, Claims Data',
                'business_impact': 'HIGH - Statistically derived limit'
            }
        
        else:
            return {
                'sub_archetype': 'MUE_OTHER',
                'adjudication_type': adjudication,
                'strictness': 'MEDIUM',
                'guidance': 'Review specific MUE rationale for guidance',
                'reference': 'NCCI Manual Chapter 10',
                'business_impact': 'VARIES - Case-by-case'
            }

    def _detect_archetype(self, issue: Dict[str, Any]) -> str:
        """Detect the denial archetype based on trigger conditions"""
        if (issue.get('ptp_denial_reason') and issue.get('ptp_denial_reason') != 'None' and 
            issue.get('hcpcs_position') == 1):
            return "NCCI_PTP_Conflict"
        
        if (issue.get('lcd_icd10_covered_group') == 'N' and 
            issue.get('dx_position') == 1):
            return "Primary_DX_Not_Covered"
        
        if issue.get('mue_denial_type') and issue.get('mue_denial_type') != 'None':
            return "MUE_Risk"
        
        if issue.get('ncd_status') == 'Terminated':
            return "NCD_Terminated"
        
        if (issue.get('lcd_icd10_covered_group') == 'N' and 
            issue.get('dx_position', 0) > 1):
            return "Secondary_DX_Not_Covered"
        
        # TESTING: Simple Medical Necessity check (labs/EKG + M-codes)
        hcpcs = issue.get('hcpcs_code', '')
        dx = issue.get('icd10_code', '')
        if hcpcs in ['80053', '93000', '85025'] and dx.startswith('M'):
            return "Medical_Necessity_Review"
        
        return "Compliant"

    def _build_archetype_query(self, issue: Dict[str, Any], archetype: str) -> str:
        """Build targeted query based on archetype and claim data"""
        cpt_code = issue.get('hcpcs_code', '')
        icd_code = issue.get('icd10_code', '')
        procedure_name = issue.get('procedure_name', '')
        diagnosis_name = issue.get('diagnosis_name', '')
        
        if archetype == "Primary_DX_Not_Covered":
            query = (
                f"covered ICD-10 codes for CPT {cpt_code} {procedure_name} "
                f"LCD crosswalk covered diagnosis alternatives for {diagnosis_name} "
                f"medicare coverage criteria medical necessity"
            )
        elif archetype == "NCCI_PTP_Conflict":
            query = (
                f"NCCI PTP edits for CPT {cpt_code} modifier exceptions "
                f"59 XE XP XS XU bundling conflicts separate procedural service "
                f"procedure to procedure edits"
            )
        elif archetype == "MUE_Risk":
            query = (
                f"MUE medically unlikely edit for CPT {cpt_code} unit limits "
                f"maximum units threshold documentation medical necessity"
            )
        elif archetype == "NCD_Terminated":
            query = (
                f"NCD terminated replacement coverage for CPT {cpt_code} "
                f"national coverage determination successor policy"
            )
        elif archetype == "Secondary_DX_Not_Covered":
            query = (
                f"secondary diagnosis coverage LCD crosswalk for CPT {cpt_code} "
                f"co-diagnosis pairings medical necessity"
            )
        else:
            query = (
                f"CMS policy compliance for CPT {cpt_code} ICD {icd_code} "
                f"medicare billing guidelines documentation requirements"
            )
        
        return query

    def _search_archetype_corrections(self, issue: Dict[str, Any], archetype: str) -> List[Dict[str, Any]]:
        """Search for archetype-specific correction policies"""
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        target_collections = archetype_info.get('qdrant_collections', self.policy_collections)
        
        query_text = self._build_archetype_query(issue, archetype)
        query_vector = self.embedder.encode(query_text).tolist()
        
        correction_policies = []
        
        for collection in target_collections:
            if collection in self.policy_collections:
                try:
                    hits = self.client.query_points(
                        collection_name=collection,
                        query=query_vector,
                        limit=3,
                        with_payload=True,
                        with_vectors=False,
                    ).points
                    
                    for hit in hits:
                        policy_dict = hit.payload.copy()
                        policy_dict['score'] = hit.score
                        policy_dict['collection'] = collection
                        correction_policies.append(policy_dict)
                        
                except Exception as e:
                    print(f" Archetype search failed for {collection}: {e}")
        
        correction_policies.sort(key=lambda x: x.get('score', 0), reverse=True)
        return self._deduplicate_policies(correction_policies)[:6]

    #  UPDATE3: New robust LLM method with fallbacks
    def _run_sql_driven_archetype_stage2_llm_robust(self, issue: Dict[str, Any], stage1_result: Dict[str, Any], 
                                                     correction_policies: List[Dict[str, Any]], archetype: str, 
                                                     sql_evidence: List[Dict[str, Any]], 
                                                     sub_archetype_info: Dict[str, Any] = None) -> Dict[str, Any]:
        """Run SQL-driven archetype Stage 2 LLM with robust parsing and fallbacks"""
        try:
            archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
            archetype_description = archetype_info.get('description', '')
            sql_insight = archetype_info.get('sql_insight', '')
            correction_strategies = archetype_info.get('correction_strategies', [])
            
            denial_analysis = stage1_result.get("denial_analysis", {})
            denial_summary = json.dumps(denial_analysis, indent=2)
            
            # Format SQL evidence
            sql_evidence_text = ""
            if sql_evidence:
                for i, evidence in enumerate(sql_evidence, 1):
                    sql_evidence_text += f"\nSQL EVIDENCE {i}:\n"
                    for key, value in evidence.items():
                        sql_evidence_text += f"  {key}: {value}\n"
            else:
                sql_evidence_text = "No SQL evidence found for this claim/archetype combination."
            
            # Format archetype-specific correction policies with explicit citation format
            correction_policies_text = ""
            for i, policy in enumerate(correction_policies, 1):
                source = policy.get('source', 'Unknown')
                chapter = policy.get('chapter', 'None')
                section = policy.get('section', 'None')
                
                # Build the citation string
                citation = f"{source}"
                if chapter and chapter != 'None':
                    citation += f" - Chapter {chapter}"
                if section and section != 'None':
                    citation += f", Section {section}"
                
                correction_policies_text += f"\n{'='*80}\n"
                correction_policies_text += f"POLICY {i}\n"
                correction_policies_text += f"{'='*80}\n"
                correction_policies_text += f" CITE THIS AS: {citation}\n"
                correction_policies_text += f"Source Document: {source}\n"
                correction_policies_text += f"Chapter: {chapter}\n"
                correction_policies_text += f"Section: {section}\n"
                correction_policies_text += f"Collection: {policy.get('collection', 'N/A')}\n"
                correction_policies_text += f"Relevance Score: {policy.get('score', 0.0):.4f}\n"
                correction_policies_text += f"\nPolicy Text:\n{policy.get('text', '')[:500]}...\n"
            
            #  UPDATE10: Add sub-archetype specific guidance to prompt
            sub_archetype_guidance = ""
            if sub_archetype_info:
                sub_archetype_guidance = f"""
{'='*80}
 SUB-ARCHETYPE SPECIFIC GUIDANCE
{'='*80}
Sub-Type: {sub_archetype_info.get('sub_archetype', 'N/A')}
Guidance: {sub_archetype_info.get('guidance', 'N/A')}
Reference: {sub_archetype_info.get('reference', 'N/A')}
Business Impact: {sub_archetype_info.get('business_impact', 'N/A')}
"""
                
                if 'modifier_allowed' in sub_archetype_info:
                    sub_archetype_guidance += f"Modifier Allowed: {'YES - Use modifier 59/XE/XP/XS/XU' if sub_archetype_info['modifier_allowed'] else 'NO - Modifier not allowed, absolute denial'}\n"
                
                if 'strictness' in sub_archetype_info:
                    sub_archetype_guidance += f"Strictness Level: {sub_archetype_info.get('strictness')}\n"
                
                if 'adjudication_type' in sub_archetype_info:
                    sub_archetype_guidance += f"Adjudication: {sub_archetype_info.get('adjudication_type')}\n"
                
                sub_archetype_guidance += f"{'='*80}\n"
            
            prompt = STAGE2_SQL_DRIVEN_ARCHETYPE_CORRECTION_PROMPT.format(
                archetype=archetype,
                archetype_description=archetype_description,
                sql_insight=sql_insight,
                correction_strategies="\n".join([f"- {strategy}" for strategy in correction_strategies]),
                claim_id=issue.get('claim_id', 'N/A'),
                hcpcs_code=issue.get('hcpcs_code', 'N/A'),
                procedure_name=issue.get('procedure_name', 'N/A'),
                icd10_code=issue.get('icd10_code', 'N/A'),
                diagnosis_name=issue.get('diagnosis_name', 'N/A'),
                denial_reason=issue.get('ptp_denial_reason', 'N/A'),
                denial_risk_level=issue.get('denial_risk_level', 'N/A'),
                action_required=issue.get('action_required', 'N/A'),
                denial_analysis=denial_summary,
                sql_evidence=sql_evidence_text,
                correction_policies=correction_policies_text,
                sub_archetype_guidance=sub_archetype_guidance  #  UPDATE10: Add sub-type guidance
            )
            
            # Run LLM with safe execution (prevents subprocess deadlock)
            print(f"       Generating recommendation...")
            
            stdout, stderr, return_code = run_ollama_safe(prompt, timeout=60)
            
            if return_code != 0:
                print(f"       LLM failed: {stderr[:100]}, using fallback")
                return self._generate_fallback_correction(issue, archetype, sql_evidence, f"LLM failed: {stderr[:100]}")
            
            print(f"       Recommendation: {len(stdout)} chars")
            
            #  UPDATE3: Robust JSON parsing
            llm_output = stdout.strip()
            parsed_result = self._robust_parse_llm_output(llm_output, issue, archetype, sql_evidence)
            
            return parsed_result
                
        except Exception as e:
            print(f" SQL-driven Archetype Stage 2 LLM failed: {e}")
            return self._generate_fallback_correction(issue, archetype, sql_evidence, f"Exception: {str(e)[:100]}")

    #  UPDATE3: Robust LLM output parser
    def _robust_parse_llm_output(self, llm_output: str, issue: Dict, archetype: str, sql_evidence: List[Dict]) -> Dict[str, Any]:
        """Robust LLM output parsing with multiple fallback strategies"""
        
        # Strategy 1: Direct JSON parsing
        try:
            return json.loads(llm_output.strip())
        except json.JSONDecodeError:
            pass
        
        # Strategy 2: Extract JSON from markdown code blocks
        try:
            json_match = re.search(r'```json\n(.*?)\n```', llm_output, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(1))
        except:
            pass
        
        # Strategy 3: Extract between first { and last }
        try:
            json_start = llm_output.find('{')
            json_end = llm_output.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                json_str = llm_output[json_start:json_end]
                return json.loads(json_str)
        except:
            pass
        
        # Strategy 4: Generate structured fallback
        print(f"    All JSON parsing strategies failed, using structured fallback")
        return self._generate_fallback_correction(issue, archetype, sql_evidence, llm_output[:200])

    #  UPDATE3: Structured fallback correction generator
    def _generate_fallback_correction(self, issue: Dict, archetype: str, sql_evidence: List[Dict], reason: str) -> Dict[str, Any]:
        """Generate structured correction when LLM fails to produce valid JSON"""
        
        if archetype == "Primary_DX_Not_Covered":
            return {
                "claim_id": issue.get('claim_id', 'N/A'),
                "archetype": archetype,
                "sql_evidence_summary": "LCD coverage data not available - using hardcoded alternatives",
                "recommended_corrections": self._get_specific_dx_alternatives(issue.get('icd10_code', '')),
                "policy_references": ["CMS ICD-10 Coverage Guidelines"],
                "final_guidance": "Replace non-covered diagnosis with clinically appropriate covered alternative",
                "compliance_checklist": [
                    "Verify medical necessity documentation supports alternative diagnosis",
                    "Ensure alternative diagnosis is clinically accurate"
                ],
                "evidence_traceability": "Hardcoded coverage mapping + clinical guidelines",
                "fallback_reason": f"LLM parse failed: {reason}"
            }
        
        elif archetype == "NCCI_PTP_Conflict":
            return {
                "claim_id": issue.get('claim_id', 'N/A'),
                "archetype": archetype,
                "sql_evidence_summary": f"PTP conflict detected for {issue.get('hcpcs_code')}",
                "recommended_corrections": self._get_specific_modifier_strategies(
                    issue.get('hcpcs_code', ''), sql_evidence
                ),
                "policy_references": ["NCCI Policy Manual Chapter I"],
                "final_guidance": "Apply appropriate resolution strategy based on modifier status",
                "compliance_checklist": [
                    "Document medical necessity for separate services",
                    "Verify modifier usage aligns with NCCI guidelines"
                ],
                "evidence_traceability": "SQL NCCI data + policy guidelines",
                "fallback_reason": f"LLM parse failed: {reason}"
            }
        
        elif archetype == "MUE_Risk":
            mue_threshold = sql_evidence[0].get('mue_threshold') if sql_evidence else 'Unknown'
            return {
                "claim_id": issue.get('claim_id', 'N/A'),
                "archetype": archetype,
                "sql_evidence_summary": f"MUE threshold: {mue_threshold}",
                "recommended_corrections": [{
                    "field": "units",
                    "suggestion": f"Reduce units to  {mue_threshold}",
                    "confidence": 0.90,
                    "sql_evidence_reference": "mue_threshold from NCCI table",
                    "policy_reference": "NCCI MUE Guidelines",
                    "implementation_guidance": f"Adjust billed units to not exceed {mue_threshold}"
                }],
                "policy_references": ["NCCI MUE Table"],
                "final_guidance": "Reduce units or provide medical necessity documentation",
                "compliance_checklist": ["Verify documentation supports exceeding MUE"],
                "evidence_traceability": "SQL MUE data",
                "fallback_reason": f"LLM parse failed: {reason}"
            }
        
        # Default fallback
        return {
            "claim_id": issue.get('claim_id', 'N/A'),
            "archetype": archetype,
            "sql_evidence_summary": "Processing completed with fallback logic",
            "recommended_corrections": [],
            "error": "LLM output parsing failed",
            "fallback_reason": reason,
            "raw_llm_output": reason[:500]
        }

    #  UPDATE4: Database-driven diagnosis alternatives (no more hardcoded mappings!)
    def _get_specific_dx_alternatives(self, current_icd10: str) -> List[Dict]:
        """Provide SPECIFIC alternative ICD-10 codes from database"""
        
        #  UPDATE4: Query database for alternatives using GEMS shared ICD-9 strategy
        db_alternatives = self.sql_connector._get_icd10_alternatives_from_db(current_icd10, limit=5)
        
        if db_alternatives:
            corrections = []
            for alt in db_alternatives:
                corrections.append({
                    "field": "diagnosis_code",
                    "suggestion": f"Replace {current_icd10} with {alt['code']} - {alt['description']}",
                    "specific_code": alt['code'],
                    "current_code": current_icd10,
                    "confidence": alt['confidence'],
                    "sql_evidence_reference": f"GEMS crosswalk - {alt['strategy']} (shared ICD-9: {alt.get('shared_icd9', 'N/A')})",
                    "policy_reference": "CMS ICD-10-CM 2018 Code Set + GEMS Mappings",
                    "implementation_guidance": f"Update primary diagnosis field from {current_icd10} to {alt['code']}",
                    "alternative_strategy": alt['strategy']
                })
            print(f"    Generated {len(corrections)} database-driven diagnosis alternatives")
            return corrections
        else:
            # Fallback when database has no alternatives
            print(f"    No database alternatives found for {current_icd10}, using generic guidance")
            return [{
                "field": "diagnosis_code",
                "suggestion": f"Review LCD coverage guidelines for {current_icd10}",
                "specific_code": "MANUAL_REVIEW_REQUIRED",
                "current_code": current_icd10,
                "confidence": 0.50,
                "sql_evidence_reference": "No database alternatives available",
                "policy_reference": "CMS LCD Database",
                "implementation_guidance": "Consult CMS LCD database or MAC for covered diagnosis alternatives",
                "alternative_strategy": "manual_review_required"
            }]

    #  UPDATE3: Specific modifier strategies (respects SQL modifier_status)
    def _get_specific_modifier_strategies(self, hcpcs_code: str, sql_evidence: List[Dict]) -> List[Dict]:
        """Provide SPECIFIC modifier strategies based on SQL evidence"""
        
        modifier_status = sql_evidence[0].get('modifier_status', 'Unknown') if sql_evidence else 'Unknown'
        
        #  UPDATE3: Respect "Modifier Not Allowed" status
        if modifier_status == "Modifier Not Allowed":
            return [{
                "field": "procedure_code",
                "suggestion": "Split services to separate claims or dates of service - modifiers NOT allowed",
                "specific_action": "BILL_SEPARATELY",
                "current_value": f"{hcpcs_code} (same date)",
                "suggested_value": f"{hcpcs_code} (separate date or claim)",
                "confidence": 0.95,
                "sql_evidence_reference": f"modifier_status = 'Modifier Not Allowed' for {hcpcs_code}",
                "policy_reference": "NCCI PTP Manual - Modifier Not Allowed edits",
                "implementation_guidance": f"Bill {hcpcs_code} on a different date of service OR as a completely separate claim"
            }]
        else:
            # Modifiers are allowed
            return [{
                "field": "modifier",
                "suggestion": f"Add modifier 59 to {hcpcs_code} to indicate distinct procedural service",
                "specific_code": "59",
                "current_value": "none",
                "suggested_value": f"{hcpcs_code}-59",
                "confidence": 0.85,
                "sql_evidence_reference": f"modifier_status = '{modifier_status}' for {hcpcs_code}",
                "policy_reference": "NCCI PTP Manual Ch.2 - Modifier 59 Guidelines",
                "implementation_guidance": f"Append modifier 59 to procedure code: {hcpcs_code}-59. Document separate anatomical site or patient encounter."
            }]

    # Existing methods below (calibrated validation, policy search, etc.)
    
    def _calibrated_validate_and_deduplicate_policies(self, policies: List[Any], issue: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Calibrated policy validation with enhanced relevance checks"""
        if not policies:
            return []
        
        policy_dicts = []
        for policy in policies:
            if hasattr(policy, 'payload'):
                policy_dict = policy.payload.copy()
                policy_dict['score'] = policy.score
                policy_dicts.append(policy_dict)
            elif isinstance(policy, dict):
                policy_dicts.append(policy)
        
        validated_policies = []
        for policy in policy_dicts:
            validation_result = self._validate_policy_relevance(policy, issue)
            if validation_result.get('is_relevant', False):
                policy.update({
                    'validation_status': validation_result.get('validation_status', 'UNKNOWN'),
                    'relevance_reason': validation_result.get('relevance_reason', ''),
                    'manual_appropriate': validation_result.get('manual_appropriate', True)
                })
                validated_policies.append(policy)
        
        return self._deduplicate_policies(validated_policies)

    def _validate_policy_relevance(self, policy: Dict[str, Any], issue: Dict[str, Any]) -> Dict[str, Any]:
        """Validate if policy is relevant to the claim with more flexible criteria"""
        cpt_code = issue.get('hcpcs_code', '')
        icd_code = issue.get('icd10_code', '')
        denial_reason = issue.get('ptp_denial_reason', '')
        source_file = policy.get('source', '')
        
        policy_text = policy.get('text', '').lower()
        mentions_cpt = cpt_code.lower() in policy_text if cpt_code else False
        mentions_icd = icd_code.lower() in policy_text if icd_code else False
        
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
        
        manual_appropriate = self._check_manual_appropriateness(source_file, denial_reason)
        
        general_medical_keywords = ['medical', 'procedure', 'service', 'coding', 'billing', 'claim']
        mentions_general = any(keyword in policy_text for keyword in general_medical_keywords)
        
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
        elif mentions_general and len(policy_text) > 200:
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
        """Check if manual type is appropriate for the denial reason"""
        if not source_file or not denial_reason:
            return True
        
        source_lower = source_file.lower()
        denial_lower = denial_reason.lower()
        
        if source_lower.startswith('pim'):
            return 'administrative' in denial_lower or 'integrity' in denial_lower
        
        if source_lower.startswith('clm104'):
            return any(keyword in denial_lower for keyword in ['coding', 'procedure', 'definition', 'ptp', 'conflict'])
        
        if source_lower.startswith('ncci'):
            return any(keyword in denial_lower for keyword in ['ptp', 'bundling', 'conflict', 'ncci'])
        
        if source_lower.startswith('lcd'):
            return any(keyword in denial_lower for keyword in ['coverage', 'determination', 'local'])
        
        return True

    def _identify_policy_source(self, source_file: str) -> str:
        """Identify policy manual based on source file name"""
        if not source_file:
            return "Unknown Source"
        
        source_lower = source_file.lower()
        for prefix, manual_name in self.source_mapping.items():
            if source_lower.startswith(prefix):
                return manual_name
        
        return f"Policy Manual ({source_file})"

    def _run_calibrated_stage1_llm(self, issue: Dict[str, Any], policies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run Stage 1 calibrated LLM for denial reasoning"""
        try:
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
            
            print(f"       Analyzing...")
            
            stdout, stderr, return_code = run_ollama_safe(prompt, timeout=60)
            
            if return_code != 0:
                print(f"       LLM failed: {stderr[:100]}")
                return {"error": f"Calibrated Stage 1 LLM failed: {stderr[:100]}"}
            
            print(f"       Response: {len(stdout)} chars")
            llm_output = stdout.strip()
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

    def _get_claim_issues(self, claim_id: str) -> List[Dict[str, Any]]:
        """Get claim issues from the claims collection"""
        try:
            hits = self.client.query_points(
                collection_name=self.claims_collection,
                query=[0] * 768,
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
    
    def cleanup(self):
        """Clean up resources"""
        if self.sql_connector:
            self.sql_connector.close()


if __name__ == "__main__":
    try:
        corrector = ArchetypeDrivenClaimCorrector()
        claim_id = "123456789012345"
        enriched = corrector.run_archetype_driven_corrections(claim_id)
        print(json.dumps(enriched, indent=2))
    finally:
        corrector.cleanup()



if __name__ == "__main__":
    try:
        corrector = ArchetypeDrivenClaimCorrector()
        claim_id = "123456789012345"
        enriched = corrector.run_archetype_driven_corrections(claim_id)
        print(json.dumps(enriched, indent=2))
    finally:
        corrector.cleanup()

