#!/usr/bin/env python3
"""
Archetype-Driven CMS Claim Corrector with Second Handshake - UPDATE 2
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
# ARCHETYPE DEFINITIONS REGISTRY (UPDATED WITH FLEXIBLE ICD QUERIES)
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
        #  UPDATE1: SQL query now uses placeholder {DX_WHERE} for dynamic ICD version handling
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
        "sql_insight": "Lists claims whose mapped ICD-10 diagnosis is not covered by LCD crosswalk.",
        "correction_strategies": [
            "Replace the ICD-10 diagnosis with a covered diagnosis per LCD crosswalk.",
            "Validate medical necessity using LCD/NCD coverage criteria.",
            "Ensure diagnosis supports medical necessity of CPT/HCPCS code."
        ],
        "sample_reference": "LCD L34696  Fracture and Bone Imaging Coverage"
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
        #  UPDATE1: SQL query now uses placeholder {DX_WHERE} for dynamic ICD version handling
        "sql_query": """
            SELECT 
                g.icd9_code,
                g.icd10_code,
                l.lcd_id,
                l.title as lcd_title,
                l.coverage_group_description as coverage_group,
                l.covered_codes,
                l.diagnoses_support,
                l.diagnoses_dont_support
            FROM [_gems].[dbo].[table_2018_I9gem_fixed] g
            LEFT JOIN [_lcd].[dbo].[vw_Master_Denial_Analysis] l
                ON g.icd10_code = l.covered_codes
            WHERE {DX_WHERE}
        """,
        "sql_insight": "Surfaces non-covered secondary diagnoses, useful for suggesting ICD updates or documentation clarification.",
        "correction_strategies": [
            "No immediate action required unless secondary DX is used to justify medical necessity.",
            "Review LCD for co-diagnosis pairings and update if necessary."
        ],
        "sample_reference": "LCD Crosswalk Guidelines  Secondary Diagnosis Coverage"
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

REQUIRED OUTPUT FORMAT (valid JSON only):
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
      "policy_reference": "Manual name + chapter/section from retrieved policies",
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

FOCUS: Use SQL evidence + archetype-specific correction strategies to provide fact-driven, explainable corrections.
"""

# -------------------------------------------------------------------------
# SQL DATABASE CONNECTION (UPDATE1: Enhanced with ICD mapping methods)
# -------------------------------------------------------------------------

class SQLDatabaseConnector:
    """SQL Server connection for archetype-specific evidence gathering"""
    
    def __init__(self, connection_string: str = None):
        """Initialize SQL connection"""
        if connection_string is None:
            # Use the same connection string as the original database ingestion script
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
    
    #  UPDATE1: New method to detect ICD version
    def _is_icd10(self, code: str) -> bool:
        """Check if code is ICD-10 (starts with letter, 7 chars)"""
        if not code:
            return False
        return code[0].isalpha() and len(code) <= 7
    
    #  UPDATE1: New method to map ICD-10 to ICD-9 using GEMS
    def _map_icd10_to_icd9(self, icd10: str) -> List[str]:
        """Map ICD-10 code to ICD-9 code(s) using GEMS table"""
        if not self.connection or not icd10:
            return []
        try:
            query = """
                SELECT DISTINCT icd9_code
                FROM [_gems].[dbo].[table_2018_I9gem_fixed]
                WHERE icd10_code = ?
            """
            df = pd.read_sql(query, self.connection, params=[icd10])
            return df['icd9_code'].tolist() if not df.empty else []
        except Exception as e:
            print(f"    ICD-10 to ICD-9 mapping failed: {e}")
            return []
    
    #  UPDATE1: New method to map ICD-9 to ICD-10 using GEMS
    def _map_icd9_to_icd10(self, icd9: str) -> List[str]:
        """Map ICD-9 code to ICD-10 code(s) using GEMS table"""
        if not self.connection or not icd9:
            return []
        try:
            query = """
                SELECT DISTINCT icd10_code
                FROM [_gems].[dbo].[table_2018_I9gem_fixed]
                WHERE icd9_code = ?
            """
            df = pd.read_sql(query, self.connection, params=[icd9])
            return df['icd10_code'].tolist() if not df.empty else []
        except Exception as e:
            print(f"    ICD-9 to ICD-10 mapping failed: {e}")
            return []
    
    #  UPDATE1: Enhanced execute_archetype_query with smart ICD version handling
    def execute_archetype_query(self, archetype: str, codes: Dict[str, str]) -> List[Dict[str, Any]]:
        """Execute archetype-specific SQL query with smart ICD version detection"""
        if not self.connection:
            print(" No SQL connection available")
            return []
        
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        base_sql = archetype_info.get('sql_query', '')
        
        if not base_sql:
            print(f" No SQL query defined for archetype: {archetype}")
            return []
        
        try:
            # HCPCS-driven archetypes (no changes needed)
            if archetype in ["NCCI_PTP_Conflict", "MUE_Risk", "NCD_Terminated", "Compliant"]:
                query_param = codes.get('hcpcs_code')
                if archetype == "Compliant" or not query_param:
                    df = pd.read_sql(base_sql, self.connection)
                else:
                    df = pd.read_sql(base_sql, self.connection, params=[query_param])
                evidence = df.to_dict('records')
                print(f"    SQL Evidence: Found {len(evidence)} records for archetype '{archetype}'")
                return evidence
            
            #  UPDATE1: DX-driven archetypes with ICD version awareness
            elif archetype in ["Primary_DX_Not_Covered", "Secondary_DX_Not_Covered"]:
                icd10 = codes.get('icd10_code', '')
                icd9 = codes.get('icd9_code', '')
                
                results: List[Dict[str, Any]] = []
                
                def run_dx_query(dx_where: str, param: str) -> List[Dict[str, Any]]:
                    """Helper to execute diagnosis query with specific WHERE clause"""
                    sql = base_sql.replace('{DX_WHERE}', dx_where)
                    df_local = pd.read_sql(sql, self.connection, params=[param])
                    return df_local.to_dict('records')
                
                # Strategy: Try available codes in order of preference
                tried = []
                
                # Prefer ICD-10 if it looks like ICD-10 (starts with letter)
                if icd10 and self._is_icd10(icd10):
                    tried.append(('g.icd10_code = ?', icd10, 'ICD-10'))
                
                # Try ICD-9 if it looks numeric
                if icd9 and not self._is_icd10(icd9):
                    tried.append(('g.icd9_code = ?', icd9, 'ICD-9'))
                
                # If unclear, try both in order
                if not tried:
                    if icd10:
                        tried.append(('g.icd10_code = ?', icd10, 'ICD-10 (fallback)'))
                    if icd9:
                        tried.append(('g.icd9_code = ?', icd9, 'ICD-9 (fallback)'))
                
                # Execute attempts
                for dx_where, param, version_label in tried:
                    print(f"    Trying {version_label} query: {param}")
                    rows = run_dx_query(dx_where, param)
                    if rows:
                        print(f"    Found {len(rows)} records using {version_label}")
                        results.extend(rows)
                        break
                
                #  UPDATE1: GEMs fallback if no results
                if not results:
                    print(f"    No direct match, attempting GEMs mapping...")
                    
                    # Try mapping ICD-10  ICD-9
                    if icd10:
                        mapped_icd9 = self._map_icd10_to_icd9(icd10)
                        print(f"    Mapped {icd10}  {mapped_icd9}")
                        for mapped_code in mapped_icd9:
                            rows = run_dx_query('g.icd9_code = ?', mapped_code)
                            if rows:
                                print(f"    Found {len(rows)} records using mapped ICD-9: {mapped_code}")
                                results.extend(rows)
                                break
                    
                    # Try mapping ICD-9  ICD-10 if still empty
                    if not results and icd9:
                        mapped_icd10 = self._map_icd9_to_icd10(icd9)
                        print(f"    Mapped {icd9}  {mapped_icd10}")
                        for mapped_code in mapped_icd10:
                            rows = run_dx_query('g.icd10_code = ?', mapped_code)
                            if rows:
                                print(f"    Found {len(rows)} records using mapped ICD-10: {mapped_code}")
                                results.extend(rows)
                                break
                
                if not results:
                    print(f"    No SQL evidence found for archetype '{archetype}' with codes {codes}")
                
                return results
            
            return []
            
        except Exception as e:
            print(f" SQL query failed for archetype '{archetype}': {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print(" SQL Database connection closed")

# -------------------------------------------------------------------------
# ARCHETYPE-DRIVEN CLAIM CORRECTOR (Rest of code unchanged)
# -------------------------------------------------------------------------

class ArchetypeDrivenClaimCorrector:
    def __init__(self, url: str = "http://localhost:6333", sql_connection_string: str = None):
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
        
        #  Source mapping for policy identification
        self.source_mapping = {
            "clm104c": "Medicare Claims Processing Manual",
            "pim83c": "Program Integrity Manual (Administrative Only)", 
            "ncci": "National Correct Coding Initiative",
            "lcd": "Local Coverage Determination",
            "mcm": "Medicare Claims Manual",
            "bpm": "Medicare Benefit Policy Manual"
        }
        
        #  UPDATE1: SQL Database connector with enhanced ICD mapping
        self.sql_connector = SQLDatabaseConnector(sql_connection_string)

    def run_archetype_driven_corrections(self, claim_id: str) -> Dict[str, Any]:
        """Run archetype-driven two-stage corrections"""
        print(f"\n ARCHETYPE-DRIVEN PROCESSING (UPDATE1): {claim_id}")
        
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
            
            #  STAGE 2: Archetype-Driven Corrective Reasoning Pass
            print(f"    STAGE 2: Archetype-driven corrective reasoning...")
            stage2_result = self._stage2_archetype_corrective_reasoning(issue, stage1_result)
            
            # Combine results
            enriched_issue = {
                **issue,
                "stage1_calibrated_denial_analysis": stage1_result,
                "stage2_archetype_correction_analysis": stage2_result,
                "archetype_driven_complete": True
            }
            enriched_issues.append(enriched_issue)

        return {"claim_id": claim_id, "enriched_issues": enriched_issues}

    # ---------------------------------------------------------------------
    # STAGE 1: CALIBRATED DENIAL REASONING
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
    # STAGE 2: ARCHETYPE-DRIVEN CORRECTIVE REASONING
    # ---------------------------------------------------------------------
    def _stage2_archetype_corrective_reasoning(self, issue: Dict[str, Any], stage1_result: Dict[str, Any]) -> Dict[str, Any]:
        """Stage 2: SQL-driven archetype corrective reasoning"""
        # Detect the denial archetype
        archetype = self._detect_archetype(issue)
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        
        print(f"    Stage 2: Detected archetype '{archetype}' - {archetype_info.get('description', '')}")
        
        #  STEP 1: Gather SQL evidence for this archetype based on code combinations
        codes = {
            'hcpcs_code': issue.get('hcpcs_code', ''),
            'icd9_code': issue.get('icd9_code', ''),
            'icd10_code': issue.get('icd10_code', '')
        }
        sql_evidence = self.sql_connector.execute_archetype_query(archetype, codes)
        print(f"    Stage 2: Retrieved {len(sql_evidence)} SQL evidence records for archetype '{archetype}'")
        
        #  STEP 2: Search for archetype-specific correction policies
        correction_policies = self._search_archetype_corrections(issue, archetype)
        print(f"    Stage 2: Retrieved {len(correction_policies)} archetype-specific policies for correction analysis")
        
        #  STEP 3: Run SQL-driven archetype Stage 2 LLM analysis
        stage2_analysis = self._run_sql_driven_archetype_stage2_llm(issue, stage1_result, correction_policies, archetype, sql_evidence)
        
        return {
            "archetype": archetype,
            "archetype_info": archetype_info,
            "sql_evidence": sql_evidence,
            "correction_policies": correction_policies,
            "correction_analysis": stage2_analysis,
            "stage": "sql_driven_archetype_corrective_reasoning"
        }

    # ---------------------------------------------------------------------
    # ARCHETYPE DETECTION AND TARGETED SEARCH
    # ---------------------------------------------------------------------
    def _detect_archetype(self, issue: Dict[str, Any]) -> str:
        """Detect the denial archetype based on trigger conditions"""
        # Check each archetype in priority order (most specific first)
        
        # NCCI_PTP_Conflict
        if (issue.get('ptp_denial_reason') and issue.get('ptp_denial_reason') != 'None' and 
            issue.get('hcpcs_position') == 1):
            return "NCCI_PTP_Conflict"
        
        # Primary_DX_Not_Covered
        if (issue.get('lcd_icd10_covered_group') == 'N' and 
            issue.get('dx_position') == 1):
            return "Primary_DX_Not_Covered"
        
        # MUE_Risk
        if issue.get('mue_denial_type') and issue.get('mue_denial_type') != 'None':
            return "MUE_Risk"
        
        # NCD_Terminated
        if issue.get('ncd_status') == 'Terminated':
            return "NCD_Terminated"
        
        # Secondary_DX_Not_Covered
        if (issue.get('lcd_icd10_covered_group') == 'N' and 
            issue.get('dx_position', 0) > 1):
            return "Secondary_DX_Not_Covered"
        
        # Default to Compliant
        return "Compliant"

    def _build_archetype_query(self, issue: Dict[str, Any], archetype: str) -> str:
        """Build targeted query based on archetype and claim data"""
        cpt_code = issue.get('hcpcs_code', '')
        icd_code = issue.get('icd10_code', '')
        procedure_name = issue.get('procedure_name', '')
        diagnosis_name = issue.get('diagnosis_name', '')
        
        # Build archetype-specific query
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
        else:  # Compliant
            query = (
                f"CMS policy compliance for CPT {cpt_code} ICD {icd_code} "
                f"medicare billing guidelines documentation requirements"
            )
        
        return query

    def _search_archetype_corrections(self, issue: Dict[str, Any], archetype: str) -> List[Dict[str, Any]]:
        """Search for archetype-specific correction policies"""
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        target_collections = archetype_info.get('qdrant_collections', self.policy_collections)
        
        # Build targeted query
        query_text = self._build_archetype_query(issue, archetype)
        query_vector = self.embedder.encode(query_text).tolist()
        
        correction_policies = []
        
        # Search only the archetype-specific collections
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
                    
                    # Convert to dictionaries
                    for hit in hits:
                        policy_dict = hit.payload.copy()
                        policy_dict['score'] = hit.score
                        policy_dict['collection'] = collection
                        correction_policies.append(policy_dict)
                        
                except Exception as e:
                    print(f" Archetype search failed for {collection}: {e}")
        
        # Sort by score and deduplicate
        correction_policies.sort(key=lambda x: x.get('score', 0), reverse=True)
        return self._deduplicate_policies(correction_policies)[:6]

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

    def _run_sql_driven_archetype_stage2_llm(self, issue: Dict[str, Any], stage1_result: Dict[str, Any], 
                                             correction_policies: List[Dict[str, Any]], archetype: str, 
                                             sql_evidence: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run SQL-driven archetype Stage 2 LLM for corrective reasoning"""
        try:
            # Get archetype information
            archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
            archetype_description = archetype_info.get('description', '')
            sql_insight = archetype_info.get('sql_insight', '')
            correction_strategies = archetype_info.get('correction_strategies', [])
            
            # Format Stage 1 calibrated analysis
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
            
            # Format archetype-specific correction policies
            correction_policies_text = ""
            for i, policy in enumerate(correction_policies, 1):
                correction_policies_text += f"\nARCHETYPE CORRECTION POLICY {i}:\n"
                correction_policies_text += f"Source: {policy.get('source', 'N/A')}\n"
                correction_policies_text += f"Collection: {policy.get('collection', 'N/A')}\n"
                correction_policies_text += f"Chapter: {policy.get('chapter', 'N/A')}\n"
                correction_policies_text += f"Section: {policy.get('section', 'N/A')}\n"
                correction_policies_text += f"Score: {policy.get('score', 0.0):.4f}\n"
                correction_policies_text += f"Text: {policy.get('text', '')[:400]}...\n"
            
            # Format SQL-driven archetype Stage 2 prompt
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
                correction_policies=correction_policies_text
            )
            
            # Run LLM
            cmd = ["ollama", "run", "mistral", prompt]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                return {"error": f"SQL-driven Archetype Stage 2 LLM failed: {result.stderr}"}
            
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
            print(f" SQL-driven Archetype Stage 2 LLM failed: {e}")
            return {"error": f"SQL-driven Archetype Stage 2 processing failed: {e}"}

    # ---------------------------------------------------------------------
    # HELPER METHODS
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
    
    def cleanup(self):
        """Clean up resources"""
        if self.sql_connector:
            self.sql_connector.close()


# ---------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------
if __name__ == "__main__":
    try:
        corrector = ArchetypeDrivenClaimCorrector()
        # Use test claim ID to demonstrate code-level SQL queries
        claim_id = "123456789012345"
        enriched = corrector.run_archetype_driven_corrections(claim_id)
        print(json.dumps(enriched, indent=2))
    finally:
        corrector.cleanup()

