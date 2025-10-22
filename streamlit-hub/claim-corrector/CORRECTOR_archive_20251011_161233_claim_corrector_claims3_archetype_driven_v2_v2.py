#!/usr/bin/env python3
"""
Enhanced Archetype-Driven Claim Corrector v2.0
==============================================

This system implements a comprehensive CMS policy reasoning architecture with:
- 9 comprehensive archetypes for denial pattern detection
- SQL-driven evidence gathering for fact-based corrections
- Code-level policy logic targeting (ICD-HCPCS-MUE-LCD/NCD linkages)
- Enhanced Qdrant policy retrieval with archetype-specific collections
- Structured JSON output with confidence scoring and traceability

Author: AI Assistant
Version: 2.0
Date: 2025-01-04
"""

import json
import logging
import time
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import os
import sys

# Add the parent directory to the path to import the calibrated claim corrector
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from claim_corrector_claims3_calibrated import CalibratedClaimCorrector

# Import required libraries for database and vector operations
try:
    import pyodbc
    import pandas as pd
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
    from sentence_transformers import SentenceTransformer
    import ollama
    import numpy as np
except ImportError as e:
    print(f" Missing required library: {e}")
    print("Please install required packages: pip install pyodbc pandas qdrant-client sentence-transformers ollama numpy")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =============================================================================
# ENHANCED ARCHETYPE DEFINITIONS v2.0
# =============================================================================

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
        "sql_insight": "Finds procedures that violate NCCI edits, identifying conflict type and allowed modifiers.",
        "correction_strategies": [
            "Add valid NCCI modifier (59, XE, XP, XS, XU).",
            "Split procedures into separate claim lines.",
            "Verify same-day compatibility using NCCI table."
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
            WHERE g.icd9_code = ?
        """,
        "sql_insight": "Retrieves coverage mapping for ICD-10 code to identify whether diagnosis is covered under current LCDs/NCDs.",
        "correction_strategies": [
            "Replace the ICD-10 diagnosis with a covered one per LCD crosswalk.",
            "Validate medical necessity using LCD/NCD coverage criteria."
        ],
        "sample_reference": "LCD L34696  Fracture and Bone Imaging Coverage"
    },

    "MUE_Risk": {
        "description": "Billed units exceed the Medically Unlikely Edit (MUE) threshold.",
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
        "sql_insight": "Retrieves MUE thresholds for the given CPT/HCPCS code.",
        "correction_strategies": [
            "Reduce billed units to  MUE limit.",
            "Include justification documentation.",
            "Check if MUE has MAI of 1 (line edit) or 2/3 (date-of-service edit)."
        ],
        "sample_reference": "CMS NCCI MUE Table  Transmittal 12674"
    },

    "Bundled_Payment_Conflict": {
        "description": "Claim includes services bundled under DRG/APC or OPPS rules.",
        "trigger_condition": "bundle_type IN ('DRG','APC')",
        "risk_category": "HIGH",
        "business_impact": "FULL DENIAL: Secondary procedures may be bundled.",
        "action_required": "REVIEW: Bill under DRG/APC bundle instead of separate CPT lines.",
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
        "sql_insight": "Finds HCPCS codes that are secondary to bundled payments using NCCI modifier compliance data.",
        "correction_strategies": [
            "Remove bundled CPT from separate line.",
            "Submit under primary procedure bundle.",
            "Use appropriate modifier if unbundling is medically necessary."
        ],
        "sample_reference": "OPPS/APC Payment System Guidelines"
    },

    "Frequency_Limit_Exceeded": {
        "description": "Annual visit or unit frequency exceeds allowable limits.",
        "trigger_condition": "COUNT(hcpcs_code) > mue_threshold",
        "risk_category": "HIGH",
        "business_impact": "PARTIAL DENIAL: Frequency exceeded.",
        "action_required": "REVIEW: Reduce service frequency or add justification.",
        "qdrant_collections": ["claims__ncci_edits"],
        "sql_query": """
            SELECT 
                mue.hcpcs_cpt_code,
                mue.practitioner_mue_values as mue_threshold,
                mue.mue_adjudication_indicator,
                mue.mue_rationale
            FROM [_ncci_].[dbo].[mue_practitioner] mue
            WHERE mue.hcpcs_cpt_code = ?
              AND mue.practitioner_mue_values IS NOT NULL
        """,
        "sql_insight": "Retrieves MUE frequency limits for practitioner services to validate annual usage patterns.",
        "correction_strategies": [
            "Reduce number of billed units to within MUE limits.",
            "Provide supporting documentation for medical necessity.",
            "Consider splitting across multiple dates if medically appropriate."
        ],
        "sample_reference": "CMS MUE / Frequency Edit Policy"
    },

    "Missing_Modifier": {
        "description": "Procedure requires but lacks a CPT modifier (26, TC, 50, etc.).",
        "trigger_condition": "required_modifiers IS NOT NULL AND NOT EXISTS IN claim",
        "risk_category": "MEDIUM",
        "business_impact": "DENIAL: Claim incomplete due to missing modifier.",
        "action_required": "Add required modifier per CPT guidance.",
        "qdrant_collections": ["claims__med_claims_policies"],
        "sql_query": """
            SELECT 
                ptp.primary_cpt_hcpcs_code,
                ptp.secondary_cpt_hcpcs_code,
                ptp.modifier_guidance,
                ptp.ptp_edit_rationale,
                ptp.edit_type
            FROM [_ncci_].[dbo].[vw_NCCI_Modifier_Compliance] ptp
            WHERE ptp.primary_cpt_hcpcs_code = ?
              AND ptp.modifier_guidance IS NOT NULL
        """,
        "sql_insight": "Retrieves modifier requirements for CPT/HCPCS codes from NCCI compliance data.",
        "correction_strategies": [
            "Add missing modifier (26, TC, 50, LT, RT) per NCCI guidance.",
            "Ensure claim line corresponds to professional/technical component.",
            "Verify modifier appropriateness for procedure combination."
        ],
        "sample_reference": "CPT Modifier Appendix A"
    },

    "Site_of_Service_Mismatch": {
        "description": "Place of service code conflicts with CPT/HCPCS requirements.",
        "trigger_condition": "place_of_service_code NOT IN allowed_place_of_service",
        "risk_category": "HIGH",
        "business_impact": "DENIAL: Wrong site of service for CPT code.",
        "action_required": "Bill under correct site of service.",
        "qdrant_collections": ["claims__med_claims_policies"],
        "sql_query": """
            SELECT 
                lcd.hcpc_code_group,
                lcd.coverage_group_description,
                lcd.covered_codes,
                lcd.code_description
            FROM [_lcd].[dbo].[vw_LCD_HCPC_Coverage] lcd
            WHERE lcd.hcpc_code_group LIKE '%' + ? + '%'
        """,
        "sql_insight": "Checks LCD HCPC coverage to identify site-of-service requirements for specific procedures.",
        "correction_strategies": [
            "Change POS code to match procedure requirements (11 for Office, 22 for Outpatient, etc.).",
            "Bill under correct facility/non-facility rate schedule.",
            "Verify LCD coverage for specific site of service."
        ],
        "sample_reference": "CMS POS-CPT Crosswalk 2024"
    },

    "Terminated_Retired_Code": {
        "description": "CPT/HCPCS code is obsolete or replaced by newer code.",
        "trigger_condition": "termination_date < GETDATE()",
        "risk_category": "MEDIUM",
        "business_impact": "DENIAL: Code no longer active.",
        "action_required": "Replace with current CPT/HCPCS code.",
        "qdrant_collections": ["claims__ncd_policies"],
        "sql_query": """
            SELECT 
                ncd.NCD_lab,
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
        "sql_insight": "Identifies terminated or retired codes with replacement mappings from NCD tracking.",
        "correction_strategies": [
            "Replace retired CPT with successor code from NCD updates.",
            "Update billing system with active code sets.",
            "Check NCD termination notices for replacement guidance."
        ],
        "sample_reference": "NCD Manual Pub 100-03, Terminated Codes"
    },

    "Compliant": {
        "description": "Claim appears compliant and passes all denial checks.",
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
        "sql_insight": "Identifies claims that passed all other denial checks for positive examples.",
        "correction_strategies": [
            "Maintain documentation and proceed with billing."
        ],
        "sample_reference": "CMS Claims Processing Manual, Ch.12 40"
    }
}

# =============================================================================
# ENHANCED LLM PROMPTS v2.0
# =============================================================================

STAGE2_SQL_DRIVEN_ARCHETYPE_CORRECTION_PROMPT = """
You are a CMS policy correction expert specializing in SQL-driven archetype claim remediation.

CONTEXT:
You are analyzing a claim that has been identified with a specific denial archetype: {archetype}
Archetype Description: {archetype_description}
Business Impact: {business_impact}
Action Required: {action_required}

CLAIM DATA:
- Claim ID: {claim_id}
- Patient ID: {patient_id}
- Provider ID: {provider_id}
- Service Date: {service_date}
- Primary Diagnosis: {primary_diagnosis} ({icd9_code}  {icd10_code})
- Primary Procedure: {primary_procedure} ({hcpcs_code})
- Denial Risk Level: {denial_risk_level}
- Risk Score: {risk_score}

STAGE 1 DENIAL ANALYSIS:
{denial_analysis}

SQL EVIDENCE FROM DATABASE:
{sql_evidence}

ARCHETYPE-SPECIFIC CORRECTION POLICIES:
{correction_policies}

CORRECTION STRATEGIES FOR THIS ARCHETYPE:
{correction_strategies}

INSTRUCTIONS:
1. Analyze the SQL evidence to understand the specific policy violations
2. Cross-reference with retrieved CMS policies to find corrective guidance
3. Provide specific, actionable corrections based on database facts
4. Link each recommendation to specific SQL evidence and policy references
5. Include implementation guidance with confidence scoring
6. Ensure corrections are fact-driven, not speculative

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

CRITICAL REQUIREMENTS:
- Use EXACT claim data provided (no hallucinations)
- Base corrections on SQL evidence, not assumptions
- Provide specific policy references for each recommendation
- Include confidence scores (0.0-1.0) for each correction
- Ensure JSON is valid and parseable
"""

# =============================================================================
# ENHANCED SQL DATABASE CONNECTOR v2.0
# =============================================================================

class SQLDatabaseConnector:
    """Enhanced SQL database connector for archetype-specific evidence gathering"""
    
    def __init__(self):
        self.connection = None
        self._connect()
    
    def _connect(self):
        """Establish SQL Server connection with proper credentials"""
        try:
            connection_string = (
                'Driver={ODBC Driver 18 for SQL Server};'
                'Server=localhost,1433;'
                'UID=SA;'
                'PWD=Bbanwo@1980!;'
                'Database=_claims;'
                'Encrypt=yes;'
                'TrustServerCertificate=yes;'
                'Connection Timeout=30;'
            )
            
            self.connection = pyodbc.connect(connection_string)
            print(" SQL Database connection established successfully")
            
        except Exception as e:
            print(f" SQL Database connection failed: {e}")
            self.connection = None
    
    def execute_archetype_query(self, archetype: str, codes: Dict[str, str]) -> List[Dict[str, Any]]:
        """Execute archetype-specific SQL query based on code combinations"""
        if not self.connection:
            print(" No SQL connection available")
            return []
        
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        sql_query = archetype_info.get('sql_query', '')
        
        if not sql_query:
            print(f" No SQL query defined for archetype: {archetype}")
            return []
        
        try:
            # Determine which code to query based on archetype
            query_param = None
            if archetype in ["NCCI_PTP_Conflict", "MUE_Risk", "Bundled_Payment_Conflict", "Missing_Modifier", "Terminated_Retired_Code"]:
                query_param = codes.get('hcpcs_code')
            elif archetype in ["Primary_DX_Not_Covered", "Secondary_DX_Not_Covered"]:
                query_param = codes.get('icd9_code')
            elif archetype == "Site_of_Service_Mismatch":
                query_param = codes.get('hcpcs_code')
            elif archetype == "Compliant":
                query_param = None  # No parameter needed
            
            if query_param:
                df = pd.read_sql(sql_query, self.connection, params=[query_param])
            else:
                df = pd.read_sql(sql_query, self.connection)
            
            evidence = df.to_dict('records')
            print(f"    SQL Evidence: Found {len(evidence)} records for archetype '{archetype}' with codes {codes}")
            return evidence
            
        except Exception as e:
            print(f" SQL query failed for archetype '{archetype}': {e}")
            return []
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print(" SQL Database connection closed")

# =============================================================================
# ENHANCED ARCHETYPE-DRIVEN CLAIM CORRECTOR v2.0
# =============================================================================

class ArchetypeDrivenClaimCorrectorV2:
    """
    Enhanced Archetype-Driven Claim Corrector v2.0
    
    Features:
    - 9 comprehensive archetypes for denial pattern detection
    - SQL-driven evidence gathering for fact-based corrections
    - Code-level policy logic targeting
    - Enhanced Qdrant policy retrieval
    - Structured JSON output with confidence scoring
    """
    
    def __init__(self):
        """Initialize the enhanced archetype-driven corrector"""
        print(" Initializing Enhanced Archetype-Driven Claim Corrector v2.0...")
        
        # Initialize components
        self.calibrated_corrector = CalibratedClaimCorrector()
        self.sql_connector = SQLDatabaseConnector()
        self.qdrant_client = None
        self.embedder = None
        
        # Initialize Qdrant and embedding model
        self._initialize_qdrant()
        self._initialize_embedder()
        
        # Use the same embedder as the calibrated corrector to avoid dimension mismatches
        if hasattr(self.calibrated_corrector, 'embedder'):
            self.embedder = self.calibrated_corrector.embedder
        
        print(" Enhanced Archetype-Driven Claim Corrector v2.0 initialized successfully")
    
    def _initialize_qdrant(self):
        """Initialize Qdrant client and collections"""
        try:
            self.qdrant_client = QdrantClient(host="localhost", port=6333)
            print(" Qdrant client connected successfully")
            
            # Verify collections exist
            collections = self.qdrant_client.get_collections()
            collection_names = [col.name for col in collections.collections]
            print(f" Available collections: {collection_names}")
            
        except Exception as e:
            print(f" Qdrant initialization failed: {e}")
            self.qdrant_client = None
    
    def _initialize_embedder(self):
        """Initialize sentence transformer model"""
        try:
            self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
            print(" Sentence transformer model loaded successfully")
        except Exception as e:
            print(f" Embedder initialization failed: {e}")
            self.embedder = None
    
    def _detect_archetype(self, issue: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Enhanced archetype detection with improved logic"""
        # Check for PTP conflicts
        if issue.get('ptp_denial_reason') and issue.get('hcpcs_position') == 1:
            return "NCCI_PTP_Conflict", ARCHETYPE_DEFINITIONS["NCCI_PTP_Conflict"]
        
        # Check for MUE risks
        if issue.get('mue_threshold'):
            return "MUE_Risk", ARCHETYPE_DEFINITIONS["MUE_Risk"]
        
        # Check for primary diagnosis not covered
        if issue.get('lcd_icd10_covered_group') == 'N' and issue.get('dx_position') == 1:
            return "Primary_DX_Not_Covered", ARCHETYPE_DEFINITIONS["Primary_DX_Not_Covered"]
        
        # Check for secondary diagnosis not covered
        if issue.get('lcd_icd10_covered_group') == 'N' and issue.get('dx_position') > 1:
            return "Secondary_DX_Not_Covered", ARCHETYPE_DEFINITIONS["Secondary_DX_Not_Covered"]
        
        # Check for NCD termination
        if issue.get('ncd_status') == 'Terminated':
            return "NCD_Terminated", ARCHETYPE_DEFINITIONS["NCD_Terminated"]
        
        # Check for bundled payment conflicts (heuristic based on multiple HCPCS codes)
        if issue.get('hcpcs_position') > 1 and issue.get('ptp_denial_reason'):
            return "Bundled_Payment_Conflict", ARCHETYPE_DEFINITIONS["Bundled_Payment_Conflict"]
        
        # Check for missing modifiers (heuristic based on procedure type)
        hcpcs_code = issue.get('hcpcs_code', '')
        if hcpcs_code and any(x in hcpcs_code for x in ['26', 'TC', '50', 'RT', 'LT']):
            # If modifier codes are present but not in claim structure, flag as missing
            return "Missing_Modifier", ARCHETYPE_DEFINITIONS["Missing_Modifier"]
        
        # Check for site of service mismatch (heuristic based on procedure type)
        if issue.get('hcpcs_code') and issue.get('procedure_name'):
            procedure_name = issue.get('procedure_name', '').lower()
            if any(keyword in procedure_name for keyword in ['office', 'outpatient', 'inpatient', 'facility']):
                return "Site_of_Service_Mismatch", ARCHETYPE_DEFINITIONS["Site_of_Service_Mismatch"]
        
        # Check for terminated/retired codes (heuristic based on old codes)
        if hcpcs_code and len(hcpcs_code) == 5 and hcpcs_code.startswith('9'):
            # Some old 90000 series codes might be retired
            return "Terminated_Retired_Code", ARCHETYPE_DEFINITIONS["Terminated_Retired_Code"]
        
        # Check for frequency limits (heuristic based on multiple units)
        if issue.get('billed_units', 0) > 10:  # High unit count might indicate frequency issues
            return "Frequency_Limit_Exceeded", ARCHETYPE_DEFINITIONS["Frequency_Limit_Exceeded"]
        
        # Default to compliant if no other conditions match
        return "Compliant", ARCHETYPE_DEFINITIONS["Compliant"]
    
    def _build_archetype_query(self, issue: Dict[str, Any], archetype: str) -> str:
        """Build archetype-specific search query for Qdrant"""
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        collections = archetype_info.get('qdrant_collections', [])
        
        # Build query based on archetype
        query_parts = []
        
        # Always include basic claim information
        query_parts.append(f"CPT {issue.get('hcpcs_code', '')}")
        query_parts.append(f"ICD {issue.get('icd9_code', '')}")
        
        # Add archetype-specific terms
        if archetype == "NCCI_PTP_Conflict":
            query_parts.extend(["NCCI", "procedure to procedure", "PTP", "edit", "modifier"])
        elif archetype == "Primary_DX_Not_Covered":
            query_parts.extend(["LCD", "local coverage", "diagnosis", "coverage", "not covered"])
        elif archetype == "MUE_Risk":
            query_parts.extend(["MUE", "medically unlikely", "units", "threshold"])
        elif archetype == "Bundled_Payment_Conflict":
            query_parts.extend(["bundled", "DRG", "APC", "OPPS", "secondary"])
        elif archetype == "Frequency_Limit_Exceeded":
            query_parts.extend(["frequency", "annual", "limit", "exceeded"])
        elif archetype == "Missing_Modifier":
            query_parts.extend(["modifier", "26", "TC", "50", "LT", "RT"])
        elif archetype == "Site_of_Service_Mismatch":
            query_parts.extend(["place of service", "POS", "facility", "office"])
        elif archetype == "Terminated_Retired_Code":
            query_parts.extend(["terminated", "retired", "obsolete", "replaced"])
        
        return " ".join(query_parts)
    
    def _search_archetype_corrections(self, issue: Dict[str, Any], archetype: str) -> List[Dict[str, Any]]:
        """Search for archetype-specific correction policies in Qdrant"""
        if not self.qdrant_client or not self.embedder:
            print(" Qdrant client or embedder not available")
            return []
        
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        collections = archetype_info.get('qdrant_collections', [])
        
        if not collections:
            print(f" No Qdrant collections defined for archetype: {archetype}")
            return []
        
        # Build search query
        query_text = self._build_archetype_query(issue, archetype)
        print(f"    Searching for: {query_text}")
        
        # Generate embedding
        query_vector = self.embedder.encode(query_text).tolist()
        
        all_results = []
        
        # Search in each collection
        for collection_name in collections:
            try:
                # Check if collection exists
                collections_info = self.qdrant_client.get_collections()
                collection_names = [col.name for col in collections_info.collections]
                
                if collection_name not in collection_names:
                    print(f"    Collection {collection_name} does not exist, skipping")
                    continue
                
                search_result = self.qdrant_client.search(
                    collection_name=collection_name,
                    query_vector=query_vector,
                    limit=3,
                    score_threshold=0.7
                )
                
                for hit in search_result:
                    result = hit.payload.copy()
                    result['score'] = hit.score
                    result['collection'] = collection_name
                    all_results.append(result)
                
                print(f"    Found {len(search_result)} results in {collection_name}")
                
            except Exception as e:
                print(f"    Search failed in {collection_name}: {e}")
        
        # Sort by score and return top results
        all_results.sort(key=lambda x: x.get('score', 0), reverse=True)
        return all_results[:5]  # Return top 5 results
    
    def _run_sql_driven_archetype_stage2_llm(self, issue: Dict[str, Any], stage1_result: Dict[str, Any], 
                                           correction_policies: List[Dict[str, Any]], archetype: str, 
                                           sql_evidence: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run SQL-driven archetype Stage 2 LLM analysis"""
        try:
            import ollama
        except ImportError:
            return {"error": "Ollama not available"}
        
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        
        # Format archetype info
        archetype_text = f"Archetype: {archetype}\n"
        archetype_text += f"Description: {archetype_info.get('description', '')}\n"
        archetype_text += f"Business Impact: {archetype_info.get('business_impact', '')}\n"
        archetype_text += f"Action Required: {archetype_info.get('action_required', '')}"
        
        # Format denial analysis
        denial_analysis = stage1_result.get('denial_analysis', {})
        denial_text = f"Summary: {denial_analysis.get('summary', 'No analysis available')}"
        
        # Format correction policies
        policies_text = ""
        if correction_policies:
            for i, policy in enumerate(correction_policies, 1):
                policies_text += f"\nPolicy {i}:\n"
                policies_text += f"Source: {policy.get('source', 'Unknown')}\n"
                policies_text += f"Text: {policy.get('text', '')[:500]}...\n"
                policies_text += f"Score: {policy.get('score', 0):.3f}\n"
        else:
            policies_text = "No specific correction policies found"
        
        # Format SQL evidence
        sql_evidence_text = ""
        if sql_evidence:
            for i, evidence in enumerate(sql_evidence, 1):
                sql_evidence_text += f"\nSQL EVIDENCE {i}:\n"
                for key, value in evidence.items():
                    sql_evidence_text += f"  {key}: {value}\n"
        else:
            sql_evidence_text = "No SQL evidence found for this claim/archetype combination."
        
        # Format correction strategies
        strategies = archetype_info.get('correction_strategies', [])
        strategies_text = "\n".join([f"- {strategy}" for strategy in strategies])
        
        # Format SQL-driven archetype Stage 2 prompt
        prompt = STAGE2_SQL_DRIVEN_ARCHETYPE_CORRECTION_PROMPT.format(
            archetype=archetype,
            archetype_description=archetype_info.get('description', ''),
            business_impact=archetype_info.get('business_impact', ''),
            action_required=archetype_info.get('action_required', ''),
            claim_id=issue.get('claim_id', ''),
            patient_id=issue.get('patient_id', ''),
            provider_id=issue.get('provider_id', ''),
            service_date=issue.get('service_date', ''),
            primary_diagnosis=issue.get('diagnosis_name', ''),
            icd9_code=issue.get('icd9_code', ''),
            icd10_code=issue.get('icd10_code', ''),
            primary_procedure=issue.get('procedure_name', ''),
            hcpcs_code=issue.get('hcpcs_code', ''),
            denial_risk_level=issue.get('denial_risk_level', ''),
            risk_score=issue.get('denial_risk_score', 0),
            denial_analysis=denial_text,
            sql_evidence=sql_evidence_text,
            correction_policies=policies_text,
            correction_strategies=strategies_text
        )
        
        try:
            print(f"    Running SQL-driven archetype Stage 2 LLM for '{archetype}'...")
            
            response = ollama.generate(
                model="mistral",
                prompt=prompt,
                options={
                    "temperature": 0.1,
                    "top_p": 0.9,
                    "num_predict": 2048
                }
            )
            
            response_text = response['response'].strip()
            print(f"    LLM Response Length: {len(response_text)} characters")
            
            # Try to parse JSON response
            try:
                correction_analysis = json.loads(response_text)
                print(f"    Successfully parsed JSON response")
                return correction_analysis
            except json.JSONDecodeError as e:
                print(f"    JSON parsing failed: {e}")
                return {
                    "error": f"JSON parsing failed: {e}",
                    "raw_response": response_text[:500] + "..." if len(response_text) > 500 else response_text
                }
        
        except Exception as e:
            print(f"    LLM call failed: {e}")
            return {"error": f"LLM call failed: {e}"}
    
    def _stage2_archetype_corrective_reasoning(self, issue: Dict[str, Any], stage1_result: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced Stage 2 archetype corrective reasoning with SQL evidence"""
        print(f"    STAGE 2: Archetype-driven corrective reasoning...")
        
        # Detect archetype
        archetype, archetype_info = self._detect_archetype(issue)
        print(f"    Stage 2: Detected archetype '{archetype}' - {archetype_info['description']}")
        
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
    
    def run_archetype_driven_corrections(self, claim_id: str) -> Dict[str, Any]:
        """Run enhanced archetype-driven corrections for a claim"""
        print(f" ENHANCED ARCHETYPE-DRIVEN PROCESSING: {claim_id}")
        
        # Stage 1: Get calibrated denial analysis
        print(" STAGE 1: Calibrated denial reasoning analysis...")
        stage1_result = self.calibrated_corrector.run_corrections(claim_id)
        
        if not stage1_result.get('enriched_issues'):
            print(" No claim issues found in Stage 1")
            return {
                "claim_id": claim_id,
                "error": "No claim issues found",
                "stage1_result": stage1_result
            }
        
        print(f" Found {len(stage1_result['enriched_issues'])} claim issues")
        
        # Stage 2: Process each issue with archetype-driven corrections
        enriched_issues = []
        for i, issue in enumerate(stage1_result['enriched_issues'], 1):
            print(f" Processing issue {i}: {issue.get('hcpcs_code', '')} + {issue.get('icd9_code', '')}")
            
            # Run Stage 2 archetype corrective reasoning
            stage2_result = self._stage2_archetype_corrective_reasoning(issue, stage1_result)
            
            # Combine results
            enriched_issue = issue.copy()
            enriched_issue['stage2_archetype_correction_analysis'] = stage2_result
            enriched_issue['archetype_driven_complete'] = True
            
            enriched_issues.append(enriched_issue)
        
        return {
            "claim_id": claim_id,
            "enriched_issues": enriched_issues,
            "total_issues": len(enriched_issues),
            "processing_timestamp": datetime.now().isoformat(),
            "version": "2.0"
        }
    
    def cleanup(self):
        """Clean up resources"""
        if self.sql_connector:
            self.sql_connector.close()
        
        # Don't close the calibrated corrector's SQL connection
        # as it manages its own lifecycle

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    try:
        corrector = ArchetypeDrivenClaimCorrectorV2()
        
        # Use test claim ID to demonstrate enhanced archetype system
        claim_id = "123456789012345"
        enriched = corrector.run_archetype_driven_corrections(claim_id)
        
        print("\n" + "="*80)
        print(" ENHANCED ARCHETYPE-DRIVEN CORRECTION RESULTS")
        print("="*80)
        print(json.dumps(enriched, indent=2))
        
    finally:
        corrector.cleanup()

"""
Enhanced Archetype-Driven Claim Corrector v2.0
==============================================

This system implements a comprehensive CMS policy reasoning architecture with:
- 9 comprehensive archetypes for denial pattern detection
- SQL-driven evidence gathering for fact-based corrections
- Code-level policy logic targeting (ICD-HCPCS-MUE-LCD/NCD linkages)
- Enhanced Qdrant policy retrieval with archetype-specific collections
- Structured JSON output with confidence scoring and traceability

Author: AI Assistant
Version: 2.0
Date: 2025-01-04
"""

import json
import logging
import time
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import os
import sys

# Add the parent directory to the path to import the calibrated claim corrector
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from claim_corrector_claims3_calibrated import CalibratedClaimCorrector

# Import required libraries for database and vector operations
try:
    import pyodbc
    import pandas as pd
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
    from sentence_transformers import SentenceTransformer
    import ollama
    import numpy as np
except ImportError as e:
    print(f" Missing required library: {e}")
    print("Please install required packages: pip install pyodbc pandas qdrant-client sentence-transformers ollama numpy")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =============================================================================
# ENHANCED ARCHETYPE DEFINITIONS v2.0
# =============================================================================

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
        "sql_insight": "Finds procedures that violate NCCI edits, identifying conflict type and allowed modifiers.",
        "correction_strategies": [
            "Add valid NCCI modifier (59, XE, XP, XS, XU).",
            "Split procedures into separate claim lines.",
            "Verify same-day compatibility using NCCI table."
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
            WHERE g.icd9_code = ?
        """,
        "sql_insight": "Retrieves coverage mapping for ICD-10 code to identify whether diagnosis is covered under current LCDs/NCDs.",
        "correction_strategies": [
            "Replace the ICD-10 diagnosis with a covered one per LCD crosswalk.",
            "Validate medical necessity using LCD/NCD coverage criteria."
        ],
        "sample_reference": "LCD L34696  Fracture and Bone Imaging Coverage"
    },

    "MUE_Risk": {
        "description": "Billed units exceed the Medically Unlikely Edit (MUE) threshold.",
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
        "sql_insight": "Retrieves MUE thresholds for the given CPT/HCPCS code.",
        "correction_strategies": [
            "Reduce billed units to  MUE limit.",
            "Include justification documentation.",
            "Check if MUE has MAI of 1 (line edit) or 2/3 (date-of-service edit)."
        ],
        "sample_reference": "CMS NCCI MUE Table  Transmittal 12674"
    },

    "Bundled_Payment_Conflict": {
        "description": "Claim includes services bundled under DRG/APC or OPPS rules.",
        "trigger_condition": "bundle_type IN ('DRG','APC')",
        "risk_category": "HIGH",
        "business_impact": "FULL DENIAL: Secondary procedures may be bundled.",
        "action_required": "REVIEW: Bill under DRG/APC bundle instead of separate CPT lines.",
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
        "sql_insight": "Finds HCPCS codes that are secondary to bundled payments using NCCI modifier compliance data.",
        "correction_strategies": [
            "Remove bundled CPT from separate line.",
            "Submit under primary procedure bundle.",
            "Use appropriate modifier if unbundling is medically necessary."
        ],
        "sample_reference": "OPPS/APC Payment System Guidelines"
    },

    "Frequency_Limit_Exceeded": {
        "description": "Annual visit or unit frequency exceeds allowable limits.",
        "trigger_condition": "COUNT(hcpcs_code) > mue_threshold",
        "risk_category": "HIGH",
        "business_impact": "PARTIAL DENIAL: Frequency exceeded.",
        "action_required": "REVIEW: Reduce service frequency or add justification.",
        "qdrant_collections": ["claims__ncci_edits"],
        "sql_query": """
            SELECT 
                mue.hcpcs_cpt_code,
                mue.practitioner_mue_values as mue_threshold,
                mue.mue_adjudication_indicator,
                mue.mue_rationale
            FROM [_ncci_].[dbo].[mue_practitioner] mue
            WHERE mue.hcpcs_cpt_code = ?
              AND mue.practitioner_mue_values IS NOT NULL
        """,
        "sql_insight": "Retrieves MUE frequency limits for practitioner services to validate annual usage patterns.",
        "correction_strategies": [
            "Reduce number of billed units to within MUE limits.",
            "Provide supporting documentation for medical necessity.",
            "Consider splitting across multiple dates if medically appropriate."
        ],
        "sample_reference": "CMS MUE / Frequency Edit Policy"
    },

    "Missing_Modifier": {
        "description": "Procedure requires but lacks a CPT modifier (26, TC, 50, etc.).",
        "trigger_condition": "required_modifiers IS NOT NULL AND NOT EXISTS IN claim",
        "risk_category": "MEDIUM",
        "business_impact": "DENIAL: Claim incomplete due to missing modifier.",
        "action_required": "Add required modifier per CPT guidance.",
        "qdrant_collections": ["claims__med_claims_policies"],
        "sql_query": """
            SELECT 
                ptp.primary_cpt_hcpcs_code,
                ptp.secondary_cpt_hcpcs_code,
                ptp.modifier_guidance,
                ptp.ptp_edit_rationale,
                ptp.edit_type
            FROM [_ncci_].[dbo].[vw_NCCI_Modifier_Compliance] ptp
            WHERE ptp.primary_cpt_hcpcs_code = ?
              AND ptp.modifier_guidance IS NOT NULL
        """,
        "sql_insight": "Retrieves modifier requirements for CPT/HCPCS codes from NCCI compliance data.",
        "correction_strategies": [
            "Add missing modifier (26, TC, 50, LT, RT) per NCCI guidance.",
            "Ensure claim line corresponds to professional/technical component.",
            "Verify modifier appropriateness for procedure combination."
        ],
        "sample_reference": "CPT Modifier Appendix A"
    },

    "Site_of_Service_Mismatch": {
        "description": "Place of service code conflicts with CPT/HCPCS requirements.",
        "trigger_condition": "place_of_service_code NOT IN allowed_place_of_service",
        "risk_category": "HIGH",
        "business_impact": "DENIAL: Wrong site of service for CPT code.",
        "action_required": "Bill under correct site of service.",
        "qdrant_collections": ["claims__med_claims_policies"],
        "sql_query": """
            SELECT 
                lcd.hcpc_code_group,
                lcd.coverage_group_description,
                lcd.covered_codes,
                lcd.code_description
            FROM [_lcd].[dbo].[vw_LCD_HCPC_Coverage] lcd
            WHERE lcd.hcpc_code_group LIKE '%' + ? + '%'
        """,
        "sql_insight": "Checks LCD HCPC coverage to identify site-of-service requirements for specific procedures.",
        "correction_strategies": [
            "Change POS code to match procedure requirements (11 for Office, 22 for Outpatient, etc.).",
            "Bill under correct facility/non-facility rate schedule.",
            "Verify LCD coverage for specific site of service."
        ],
        "sample_reference": "CMS POS-CPT Crosswalk 2024"
    },

    "Terminated_Retired_Code": {
        "description": "CPT/HCPCS code is obsolete or replaced by newer code.",
        "trigger_condition": "termination_date < GETDATE()",
        "risk_category": "MEDIUM",
        "business_impact": "DENIAL: Code no longer active.",
        "action_required": "Replace with current CPT/HCPCS code.",
        "qdrant_collections": ["claims__ncd_policies"],
        "sql_query": """
            SELECT 
                ncd.NCD_lab,
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
        "sql_insight": "Identifies terminated or retired codes with replacement mappings from NCD tracking.",
        "correction_strategies": [
            "Replace retired CPT with successor code from NCD updates.",
            "Update billing system with active code sets.",
            "Check NCD termination notices for replacement guidance."
        ],
        "sample_reference": "NCD Manual Pub 100-03, Terminated Codes"
    },

    "Compliant": {
        "description": "Claim appears compliant and passes all denial checks.",
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
        "sql_insight": "Identifies claims that passed all other denial checks for positive examples.",
        "correction_strategies": [
            "Maintain documentation and proceed with billing."
        ],
        "sample_reference": "CMS Claims Processing Manual, Ch.12 40"
    }
}

# =============================================================================
# ENHANCED LLM PROMPTS v2.0
# =============================================================================

STAGE2_SQL_DRIVEN_ARCHETYPE_CORRECTION_PROMPT = """
You are a CMS policy correction expert specializing in SQL-driven archetype claim remediation.

CONTEXT:
You are analyzing a claim that has been identified with a specific denial archetype: {archetype}
Archetype Description: {archetype_description}
Business Impact: {business_impact}
Action Required: {action_required}

CLAIM DATA:
- Claim ID: {claim_id}
- Patient ID: {patient_id}
- Provider ID: {provider_id}
- Service Date: {service_date}
- Primary Diagnosis: {primary_diagnosis} ({icd9_code}  {icd10_code})
- Primary Procedure: {primary_procedure} ({hcpcs_code})
- Denial Risk Level: {denial_risk_level}
- Risk Score: {risk_score}

STAGE 1 DENIAL ANALYSIS:
{denial_analysis}

SQL EVIDENCE FROM DATABASE:
{sql_evidence}

ARCHETYPE-SPECIFIC CORRECTION POLICIES:
{correction_policies}

CORRECTION STRATEGIES FOR THIS ARCHETYPE:
{correction_strategies}

INSTRUCTIONS:
1. Analyze the SQL evidence to understand the specific policy violations
2. Cross-reference with retrieved CMS policies to find corrective guidance
3. Provide specific, actionable corrections based on database facts
4. Link each recommendation to specific SQL evidence and policy references
5. Include implementation guidance with confidence scoring
6. Ensure corrections are fact-driven, not speculative

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

CRITICAL REQUIREMENTS:
- Use EXACT claim data provided (no hallucinations)
- Base corrections on SQL evidence, not assumptions
- Provide specific policy references for each recommendation
- Include confidence scores (0.0-1.0) for each correction
- Ensure JSON is valid and parseable
"""

# =============================================================================
# ENHANCED SQL DATABASE CONNECTOR v2.0
# =============================================================================

class SQLDatabaseConnector:
    """Enhanced SQL database connector for archetype-specific evidence gathering"""
    
    def __init__(self):
        self.connection = None
        self._connect()
    
    def _connect(self):
        """Establish SQL Server connection with proper credentials"""
        try:
            connection_string = (
                'Driver={ODBC Driver 18 for SQL Server};'
                'Server=localhost,1433;'
                'UID=SA;'
                'PWD=Bbanwo@1980!;'
                'Database=_claims;'
                'Encrypt=yes;'
                'TrustServerCertificate=yes;'
                'Connection Timeout=30;'
            )
            
            self.connection = pyodbc.connect(connection_string)
            print(" SQL Database connection established successfully")
            
        except Exception as e:
            print(f" SQL Database connection failed: {e}")
            self.connection = None
    
    def execute_archetype_query(self, archetype: str, codes: Dict[str, str]) -> List[Dict[str, Any]]:
        """Execute archetype-specific SQL query based on code combinations"""
        if not self.connection:
            print(" No SQL connection available")
            return []
        
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        sql_query = archetype_info.get('sql_query', '')
        
        if not sql_query:
            print(f" No SQL query defined for archetype: {archetype}")
            return []
        
        try:
            # Determine which code to query based on archetype
            query_param = None
            if archetype in ["NCCI_PTP_Conflict", "MUE_Risk", "Bundled_Payment_Conflict", "Missing_Modifier", "Terminated_Retired_Code"]:
                query_param = codes.get('hcpcs_code')
            elif archetype in ["Primary_DX_Not_Covered", "Secondary_DX_Not_Covered"]:
                query_param = codes.get('icd9_code')
            elif archetype == "Site_of_Service_Mismatch":
                query_param = codes.get('hcpcs_code')
            elif archetype == "Compliant":
                query_param = None  # No parameter needed
            
            if query_param:
                df = pd.read_sql(sql_query, self.connection, params=[query_param])
            else:
                df = pd.read_sql(sql_query, self.connection)
            
            evidence = df.to_dict('records')
            print(f"    SQL Evidence: Found {len(evidence)} records for archetype '{archetype}' with codes {codes}")
            return evidence
            
        except Exception as e:
            print(f" SQL query failed for archetype '{archetype}': {e}")
            return []
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print(" SQL Database connection closed")

# =============================================================================
# ENHANCED ARCHETYPE-DRIVEN CLAIM CORRECTOR v2.0
# =============================================================================

class ArchetypeDrivenClaimCorrectorV2:
    """
    Enhanced Archetype-Driven Claim Corrector v2.0
    
    Features:
    - 9 comprehensive archetypes for denial pattern detection
    - SQL-driven evidence gathering for fact-based corrections
    - Code-level policy logic targeting
    - Enhanced Qdrant policy retrieval
    - Structured JSON output with confidence scoring
    """
    
    def __init__(self):
        """Initialize the enhanced archetype-driven corrector"""
        print(" Initializing Enhanced Archetype-Driven Claim Corrector v2.0...")
        
        # Initialize components
        self.calibrated_corrector = CalibratedClaimCorrector()
        self.sql_connector = SQLDatabaseConnector()
        self.qdrant_client = None
        self.embedder = None
        
        # Initialize Qdrant and embedding model
        self._initialize_qdrant()
        self._initialize_embedder()
        
        # Use the same embedder as the calibrated corrector to avoid dimension mismatches
        if hasattr(self.calibrated_corrector, 'embedder'):
            self.embedder = self.calibrated_corrector.embedder
        
        print(" Enhanced Archetype-Driven Claim Corrector v2.0 initialized successfully")
    
    def _initialize_qdrant(self):
        """Initialize Qdrant client and collections"""
        try:
            self.qdrant_client = QdrantClient(host="localhost", port=6333)
            print(" Qdrant client connected successfully")
            
            # Verify collections exist
            collections = self.qdrant_client.get_collections()
            collection_names = [col.name for col in collections.collections]
            print(f" Available collections: {collection_names}")
            
        except Exception as e:
            print(f" Qdrant initialization failed: {e}")
            self.qdrant_client = None
    
    def _initialize_embedder(self):
        """Initialize sentence transformer model"""
        try:
            self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
            print(" Sentence transformer model loaded successfully")
        except Exception as e:
            print(f" Embedder initialization failed: {e}")
            self.embedder = None
    
    def _detect_archetype(self, issue: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Enhanced archetype detection with improved logic"""
        # Check for PTP conflicts
        if issue.get('ptp_denial_reason') and issue.get('hcpcs_position') == 1:
            return "NCCI_PTP_Conflict", ARCHETYPE_DEFINITIONS["NCCI_PTP_Conflict"]
        
        # Check for MUE risks
        if issue.get('mue_threshold'):
            return "MUE_Risk", ARCHETYPE_DEFINITIONS["MUE_Risk"]
        
        # Check for primary diagnosis not covered
        if issue.get('lcd_icd10_covered_group') == 'N' and issue.get('dx_position') == 1:
            return "Primary_DX_Not_Covered", ARCHETYPE_DEFINITIONS["Primary_DX_Not_Covered"]
        
        # Check for secondary diagnosis not covered
        if issue.get('lcd_icd10_covered_group') == 'N' and issue.get('dx_position') > 1:
            return "Secondary_DX_Not_Covered", ARCHETYPE_DEFINITIONS["Secondary_DX_Not_Covered"]
        
        # Check for NCD termination
        if issue.get('ncd_status') == 'Terminated':
            return "NCD_Terminated", ARCHETYPE_DEFINITIONS["NCD_Terminated"]
        
        # Check for bundled payment conflicts (heuristic based on multiple HCPCS codes)
        if issue.get('hcpcs_position') > 1 and issue.get('ptp_denial_reason'):
            return "Bundled_Payment_Conflict", ARCHETYPE_DEFINITIONS["Bundled_Payment_Conflict"]
        
        # Check for missing modifiers (heuristic based on procedure type)
        hcpcs_code = issue.get('hcpcs_code', '')
        if hcpcs_code and any(x in hcpcs_code for x in ['26', 'TC', '50', 'RT', 'LT']):
            # If modifier codes are present but not in claim structure, flag as missing
            return "Missing_Modifier", ARCHETYPE_DEFINITIONS["Missing_Modifier"]
        
        # Check for site of service mismatch (heuristic based on procedure type)
        if issue.get('hcpcs_code') and issue.get('procedure_name'):
            procedure_name = issue.get('procedure_name', '').lower()
            if any(keyword in procedure_name for keyword in ['office', 'outpatient', 'inpatient', 'facility']):
                return "Site_of_Service_Mismatch", ARCHETYPE_DEFINITIONS["Site_of_Service_Mismatch"]
        
        # Check for terminated/retired codes (heuristic based on old codes)
        if hcpcs_code and len(hcpcs_code) == 5 and hcpcs_code.startswith('9'):
            # Some old 90000 series codes might be retired
            return "Terminated_Retired_Code", ARCHETYPE_DEFINITIONS["Terminated_Retired_Code"]
        
        # Check for frequency limits (heuristic based on multiple units)
        if issue.get('billed_units', 0) > 10:  # High unit count might indicate frequency issues
            return "Frequency_Limit_Exceeded", ARCHETYPE_DEFINITIONS["Frequency_Limit_Exceeded"]
        
        # Default to compliant if no other conditions match
        return "Compliant", ARCHETYPE_DEFINITIONS["Compliant"]
    
    def _build_archetype_query(self, issue: Dict[str, Any], archetype: str) -> str:
        """Build archetype-specific search query for Qdrant"""
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        collections = archetype_info.get('qdrant_collections', [])
        
        # Build query based on archetype
        query_parts = []
        
        # Always include basic claim information
        query_parts.append(f"CPT {issue.get('hcpcs_code', '')}")
        query_parts.append(f"ICD {issue.get('icd9_code', '')}")
        
        # Add archetype-specific terms
        if archetype == "NCCI_PTP_Conflict":
            query_parts.extend(["NCCI", "procedure to procedure", "PTP", "edit", "modifier"])
        elif archetype == "Primary_DX_Not_Covered":
            query_parts.extend(["LCD", "local coverage", "diagnosis", "coverage", "not covered"])
        elif archetype == "MUE_Risk":
            query_parts.extend(["MUE", "medically unlikely", "units", "threshold"])
        elif archetype == "Bundled_Payment_Conflict":
            query_parts.extend(["bundled", "DRG", "APC", "OPPS", "secondary"])
        elif archetype == "Frequency_Limit_Exceeded":
            query_parts.extend(["frequency", "annual", "limit", "exceeded"])
        elif archetype == "Missing_Modifier":
            query_parts.extend(["modifier", "26", "TC", "50", "LT", "RT"])
        elif archetype == "Site_of_Service_Mismatch":
            query_parts.extend(["place of service", "POS", "facility", "office"])
        elif archetype == "Terminated_Retired_Code":
            query_parts.extend(["terminated", "retired", "obsolete", "replaced"])
        
        return " ".join(query_parts)
    
    def _search_archetype_corrections(self, issue: Dict[str, Any], archetype: str) -> List[Dict[str, Any]]:
        """Search for archetype-specific correction policies in Qdrant"""
        if not self.qdrant_client or not self.embedder:
            print(" Qdrant client or embedder not available")
            return []
        
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        collections = archetype_info.get('qdrant_collections', [])
        
        if not collections:
            print(f" No Qdrant collections defined for archetype: {archetype}")
            return []
        
        # Build search query
        query_text = self._build_archetype_query(issue, archetype)
        print(f"    Searching for: {query_text}")
        
        # Generate embedding
        query_vector = self.embedder.encode(query_text).tolist()
        
        all_results = []
        
        # Search in each collection
        for collection_name in collections:
            try:
                # Check if collection exists
                collections_info = self.qdrant_client.get_collections()
                collection_names = [col.name for col in collections_info.collections]
                
                if collection_name not in collection_names:
                    print(f"    Collection {collection_name} does not exist, skipping")
                    continue
                
                search_result = self.qdrant_client.search(
                    collection_name=collection_name,
                    query_vector=query_vector,
                    limit=3,
                    score_threshold=0.7
                )
                
                for hit in search_result:
                    result = hit.payload.copy()
                    result['score'] = hit.score
                    result['collection'] = collection_name
                    all_results.append(result)
                
                print(f"    Found {len(search_result)} results in {collection_name}")
                
            except Exception as e:
                print(f"    Search failed in {collection_name}: {e}")
        
        # Sort by score and return top results
        all_results.sort(key=lambda x: x.get('score', 0), reverse=True)
        return all_results[:5]  # Return top 5 results
    
    def _run_sql_driven_archetype_stage2_llm(self, issue: Dict[str, Any], stage1_result: Dict[str, Any], 
                                           correction_policies: List[Dict[str, Any]], archetype: str, 
                                           sql_evidence: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run SQL-driven archetype Stage 2 LLM analysis"""
        try:
            import ollama
        except ImportError:
            return {"error": "Ollama not available"}
        
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        
        # Format archetype info
        archetype_text = f"Archetype: {archetype}\n"
        archetype_text += f"Description: {archetype_info.get('description', '')}\n"
        archetype_text += f"Business Impact: {archetype_info.get('business_impact', '')}\n"
        archetype_text += f"Action Required: {archetype_info.get('action_required', '')}"
        
        # Format denial analysis
        denial_analysis = stage1_result.get('denial_analysis', {})
        denial_text = f"Summary: {denial_analysis.get('summary', 'No analysis available')}"
        
        # Format correction policies
        policies_text = ""
        if correction_policies:
            for i, policy in enumerate(correction_policies, 1):
                policies_text += f"\nPolicy {i}:\n"
                policies_text += f"Source: {policy.get('source', 'Unknown')}\n"
                policies_text += f"Text: {policy.get('text', '')[:500]}...\n"
                policies_text += f"Score: {policy.get('score', 0):.3f}\n"
        else:
            policies_text = "No specific correction policies found"
        
        # Format SQL evidence
        sql_evidence_text = ""
        if sql_evidence:
            for i, evidence in enumerate(sql_evidence, 1):
                sql_evidence_text += f"\nSQL EVIDENCE {i}:\n"
                for key, value in evidence.items():
                    sql_evidence_text += f"  {key}: {value}\n"
        else:
            sql_evidence_text = "No SQL evidence found for this claim/archetype combination."
        
        # Format correction strategies
        strategies = archetype_info.get('correction_strategies', [])
        strategies_text = "\n".join([f"- {strategy}" for strategy in strategies])
        
        # Format SQL-driven archetype Stage 2 prompt
        prompt = STAGE2_SQL_DRIVEN_ARCHETYPE_CORRECTION_PROMPT.format(
            archetype=archetype,
            archetype_description=archetype_info.get('description', ''),
            business_impact=archetype_info.get('business_impact', ''),
            action_required=archetype_info.get('action_required', ''),
            claim_id=issue.get('claim_id', ''),
            patient_id=issue.get('patient_id', ''),
            provider_id=issue.get('provider_id', ''),
            service_date=issue.get('service_date', ''),
            primary_diagnosis=issue.get('diagnosis_name', ''),
            icd9_code=issue.get('icd9_code', ''),
            icd10_code=issue.get('icd10_code', ''),
            primary_procedure=issue.get('procedure_name', ''),
            hcpcs_code=issue.get('hcpcs_code', ''),
            denial_risk_level=issue.get('denial_risk_level', ''),
            risk_score=issue.get('denial_risk_score', 0),
            denial_analysis=denial_text,
            sql_evidence=sql_evidence_text,
            correction_policies=policies_text,
            correction_strategies=strategies_text
        )
        
        try:
            print(f"    Running SQL-driven archetype Stage 2 LLM for '{archetype}'...")
            
            response = ollama.generate(
                model="mistral",
                prompt=prompt,
                options={
                    "temperature": 0.1,
                    "top_p": 0.9,
                    "num_predict": 2048
                }
            )
            
            response_text = response['response'].strip()
            print(f"    LLM Response Length: {len(response_text)} characters")
            
            # Try to parse JSON response
            try:
                correction_analysis = json.loads(response_text)
                print(f"    Successfully parsed JSON response")
                return correction_analysis
            except json.JSONDecodeError as e:
                print(f"    JSON parsing failed: {e}")
                return {
                    "error": f"JSON parsing failed: {e}",
                    "raw_response": response_text[:500] + "..." if len(response_text) > 500 else response_text
                }
        
        except Exception as e:
            print(f"    LLM call failed: {e}")
            return {"error": f"LLM call failed: {e}"}
    
    def _stage2_archetype_corrective_reasoning(self, issue: Dict[str, Any], stage1_result: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced Stage 2 archetype corrective reasoning with SQL evidence"""
        print(f"    STAGE 2: Archetype-driven corrective reasoning...")
        
        # Detect archetype
        archetype, archetype_info = self._detect_archetype(issue)
        print(f"    Stage 2: Detected archetype '{archetype}' - {archetype_info['description']}")
        
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
    
    def run_archetype_driven_corrections(self, claim_id: str) -> Dict[str, Any]:
        """Run enhanced archetype-driven corrections for a claim"""
        print(f" ENHANCED ARCHETYPE-DRIVEN PROCESSING: {claim_id}")
        
        # Stage 1: Get calibrated denial analysis
        print(" STAGE 1: Calibrated denial reasoning analysis...")
        stage1_result = self.calibrated_corrector.run_corrections(claim_id)
        
        if not stage1_result.get('enriched_issues'):
            print(" No claim issues found in Stage 1")
            return {
                "claim_id": claim_id,
                "error": "No claim issues found",
                "stage1_result": stage1_result
            }
        
        print(f" Found {len(stage1_result['enriched_issues'])} claim issues")
        
        # Stage 2: Process each issue with archetype-driven corrections
        enriched_issues = []
        for i, issue in enumerate(stage1_result['enriched_issues'], 1):
            print(f" Processing issue {i}: {issue.get('hcpcs_code', '')} + {issue.get('icd9_code', '')}")
            
            # Run Stage 2 archetype corrective reasoning
            stage2_result = self._stage2_archetype_corrective_reasoning(issue, stage1_result)
            
            # Combine results
            enriched_issue = issue.copy()
            enriched_issue['stage2_archetype_correction_analysis'] = stage2_result
            enriched_issue['archetype_driven_complete'] = True
            
            enriched_issues.append(enriched_issue)
        
        return {
            "claim_id": claim_id,
            "enriched_issues": enriched_issues,
            "total_issues": len(enriched_issues),
            "processing_timestamp": datetime.now().isoformat(),
            "version": "2.0"
        }
    
    def cleanup(self):
        """Clean up resources"""
        if self.sql_connector:
            self.sql_connector.close()
        
        # Don't close the calibrated corrector's SQL connection
        # as it manages its own lifecycle

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    try:
        corrector = ArchetypeDrivenClaimCorrectorV2()
        
        # Use test claim ID to demonstrate enhanced archetype system
        claim_id = "123456789012345"
        enriched = corrector.run_archetype_driven_corrections(claim_id)
        
        print("\n" + "="*80)
        print(" ENHANCED ARCHETYPE-DRIVEN CORRECTION RESULTS")
        print("="*80)
        print(json.dumps(enriched, indent=2))
        
    finally:
        corrector.cleanup()



Enhanced Archetype-Driven Claim Corrector v2.0
==============================================

This system implements a comprehensive CMS policy reasoning architecture with:
- 9 comprehensive archetypes for denial pattern detection
- SQL-driven evidence gathering for fact-based corrections
- Code-level policy logic targeting (ICD-HCPCS-MUE-LCD/NCD linkages)
- Enhanced Qdrant policy retrieval with archetype-specific collections
- Structured JSON output with confidence scoring and traceability

Author: AI Assistant
Version: 2.0
Date: 2025-01-04
"""

import json
import logging
import time
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import os
import sys

# Add the parent directory to the path to import the calibrated claim corrector
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from claim_corrector_claims3_calibrated import CalibratedClaimCorrector

# Import required libraries for database and vector operations
try:
    import pyodbc
    import pandas as pd
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
    from sentence_transformers import SentenceTransformer
    import ollama
    import numpy as np
except ImportError as e:
    print(f" Missing required library: {e}")
    print("Please install required packages: pip install pyodbc pandas qdrant-client sentence-transformers ollama numpy")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =============================================================================
# ENHANCED ARCHETYPE DEFINITIONS v2.0
# =============================================================================

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
        "sql_insight": "Finds procedures that violate NCCI edits, identifying conflict type and allowed modifiers.",
        "correction_strategies": [
            "Add valid NCCI modifier (59, XE, XP, XS, XU).",
            "Split procedures into separate claim lines.",
            "Verify same-day compatibility using NCCI table."
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
            WHERE g.icd9_code = ?
        """,
        "sql_insight": "Retrieves coverage mapping for ICD-10 code to identify whether diagnosis is covered under current LCDs/NCDs.",
        "correction_strategies": [
            "Replace the ICD-10 diagnosis with a covered one per LCD crosswalk.",
            "Validate medical necessity using LCD/NCD coverage criteria."
        ],
        "sample_reference": "LCD L34696  Fracture and Bone Imaging Coverage"
    },

    "MUE_Risk": {
        "description": "Billed units exceed the Medically Unlikely Edit (MUE) threshold.",
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
        "sql_insight": "Retrieves MUE thresholds for the given CPT/HCPCS code.",
        "correction_strategies": [
            "Reduce billed units to  MUE limit.",
            "Include justification documentation.",
            "Check if MUE has MAI of 1 (line edit) or 2/3 (date-of-service edit)."
        ],
        "sample_reference": "CMS NCCI MUE Table  Transmittal 12674"
    },

    "Bundled_Payment_Conflict": {
        "description": "Claim includes services bundled under DRG/APC or OPPS rules.",
        "trigger_condition": "bundle_type IN ('DRG','APC')",
        "risk_category": "HIGH",
        "business_impact": "FULL DENIAL: Secondary procedures may be bundled.",
        "action_required": "REVIEW: Bill under DRG/APC bundle instead of separate CPT lines.",
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
        "sql_insight": "Finds HCPCS codes that are secondary to bundled payments using NCCI modifier compliance data.",
        "correction_strategies": [
            "Remove bundled CPT from separate line.",
            "Submit under primary procedure bundle.",
            "Use appropriate modifier if unbundling is medically necessary."
        ],
        "sample_reference": "OPPS/APC Payment System Guidelines"
    },

    "Frequency_Limit_Exceeded": {
        "description": "Annual visit or unit frequency exceeds allowable limits.",
        "trigger_condition": "COUNT(hcpcs_code) > mue_threshold",
        "risk_category": "HIGH",
        "business_impact": "PARTIAL DENIAL: Frequency exceeded.",
        "action_required": "REVIEW: Reduce service frequency or add justification.",
        "qdrant_collections": ["claims__ncci_edits"],
        "sql_query": """
            SELECT 
                mue.hcpcs_cpt_code,
                mue.practitioner_mue_values as mue_threshold,
                mue.mue_adjudication_indicator,
                mue.mue_rationale
            FROM [_ncci_].[dbo].[mue_practitioner] mue
            WHERE mue.hcpcs_cpt_code = ?
              AND mue.practitioner_mue_values IS NOT NULL
        """,
        "sql_insight": "Retrieves MUE frequency limits for practitioner services to validate annual usage patterns.",
        "correction_strategies": [
            "Reduce number of billed units to within MUE limits.",
            "Provide supporting documentation for medical necessity.",
            "Consider splitting across multiple dates if medically appropriate."
        ],
        "sample_reference": "CMS MUE / Frequency Edit Policy"
    },

    "Missing_Modifier": {
        "description": "Procedure requires but lacks a CPT modifier (26, TC, 50, etc.).",
        "trigger_condition": "required_modifiers IS NOT NULL AND NOT EXISTS IN claim",
        "risk_category": "MEDIUM",
        "business_impact": "DENIAL: Claim incomplete due to missing modifier.",
        "action_required": "Add required modifier per CPT guidance.",
        "qdrant_collections": ["claims__med_claims_policies"],
        "sql_query": """
            SELECT 
                ptp.primary_cpt_hcpcs_code,
                ptp.secondary_cpt_hcpcs_code,
                ptp.modifier_guidance,
                ptp.ptp_edit_rationale,
                ptp.edit_type
            FROM [_ncci_].[dbo].[vw_NCCI_Modifier_Compliance] ptp
            WHERE ptp.primary_cpt_hcpcs_code = ?
              AND ptp.modifier_guidance IS NOT NULL
        """,
        "sql_insight": "Retrieves modifier requirements for CPT/HCPCS codes from NCCI compliance data.",
        "correction_strategies": [
            "Add missing modifier (26, TC, 50, LT, RT) per NCCI guidance.",
            "Ensure claim line corresponds to professional/technical component.",
            "Verify modifier appropriateness for procedure combination."
        ],
        "sample_reference": "CPT Modifier Appendix A"
    },

    "Site_of_Service_Mismatch": {
        "description": "Place of service code conflicts with CPT/HCPCS requirements.",
        "trigger_condition": "place_of_service_code NOT IN allowed_place_of_service",
        "risk_category": "HIGH",
        "business_impact": "DENIAL: Wrong site of service for CPT code.",
        "action_required": "Bill under correct site of service.",
        "qdrant_collections": ["claims__med_claims_policies"],
        "sql_query": """
            SELECT 
                lcd.hcpc_code_group,
                lcd.coverage_group_description,
                lcd.covered_codes,
                lcd.code_description
            FROM [_lcd].[dbo].[vw_LCD_HCPC_Coverage] lcd
            WHERE lcd.hcpc_code_group LIKE '%' + ? + '%'
        """,
        "sql_insight": "Checks LCD HCPC coverage to identify site-of-service requirements for specific procedures.",
        "correction_strategies": [
            "Change POS code to match procedure requirements (11 for Office, 22 for Outpatient, etc.).",
            "Bill under correct facility/non-facility rate schedule.",
            "Verify LCD coverage for specific site of service."
        ],
        "sample_reference": "CMS POS-CPT Crosswalk 2024"
    },

    "Terminated_Retired_Code": {
        "description": "CPT/HCPCS code is obsolete or replaced by newer code.",
        "trigger_condition": "termination_date < GETDATE()",
        "risk_category": "MEDIUM",
        "business_impact": "DENIAL: Code no longer active.",
        "action_required": "Replace with current CPT/HCPCS code.",
        "qdrant_collections": ["claims__ncd_policies"],
        "sql_query": """
            SELECT 
                ncd.NCD_lab,
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
        "sql_insight": "Identifies terminated or retired codes with replacement mappings from NCD tracking.",
        "correction_strategies": [
            "Replace retired CPT with successor code from NCD updates.",
            "Update billing system with active code sets.",
            "Check NCD termination notices for replacement guidance."
        ],
        "sample_reference": "NCD Manual Pub 100-03, Terminated Codes"
    },

    "Compliant": {
        "description": "Claim appears compliant and passes all denial checks.",
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
        "sql_insight": "Identifies claims that passed all other denial checks for positive examples.",
        "correction_strategies": [
            "Maintain documentation and proceed with billing."
        ],
        "sample_reference": "CMS Claims Processing Manual, Ch.12 40"
    }
}

# =============================================================================
# ENHANCED LLM PROMPTS v2.0
# =============================================================================

STAGE2_SQL_DRIVEN_ARCHETYPE_CORRECTION_PROMPT = """
You are a CMS policy correction expert specializing in SQL-driven archetype claim remediation.

CONTEXT:
You are analyzing a claim that has been identified with a specific denial archetype: {archetype}
Archetype Description: {archetype_description}
Business Impact: {business_impact}
Action Required: {action_required}

CLAIM DATA:
- Claim ID: {claim_id}
- Patient ID: {patient_id}
- Provider ID: {provider_id}
- Service Date: {service_date}
- Primary Diagnosis: {primary_diagnosis} ({icd9_code}  {icd10_code})
- Primary Procedure: {primary_procedure} ({hcpcs_code})
- Denial Risk Level: {denial_risk_level}
- Risk Score: {risk_score}

STAGE 1 DENIAL ANALYSIS:
{denial_analysis}

SQL EVIDENCE FROM DATABASE:
{sql_evidence}

ARCHETYPE-SPECIFIC CORRECTION POLICIES:
{correction_policies}

CORRECTION STRATEGIES FOR THIS ARCHETYPE:
{correction_strategies}

INSTRUCTIONS:
1. Analyze the SQL evidence to understand the specific policy violations
2. Cross-reference with retrieved CMS policies to find corrective guidance
3. Provide specific, actionable corrections based on database facts
4. Link each recommendation to specific SQL evidence and policy references
5. Include implementation guidance with confidence scoring
6. Ensure corrections are fact-driven, not speculative

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

CRITICAL REQUIREMENTS:
- Use EXACT claim data provided (no hallucinations)
- Base corrections on SQL evidence, not assumptions
- Provide specific policy references for each recommendation
- Include confidence scores (0.0-1.0) for each correction
- Ensure JSON is valid and parseable
"""

# =============================================================================
# ENHANCED SQL DATABASE CONNECTOR v2.0
# =============================================================================

class SQLDatabaseConnector:
    """Enhanced SQL database connector for archetype-specific evidence gathering"""
    
    def __init__(self):
        self.connection = None
        self._connect()
    
    def _connect(self):
        """Establish SQL Server connection with proper credentials"""
        try:
            connection_string = (
                'Driver={ODBC Driver 18 for SQL Server};'
                'Server=localhost,1433;'
                'UID=SA;'
                'PWD=Bbanwo@1980!;'
                'Database=_claims;'
                'Encrypt=yes;'
                'TrustServerCertificate=yes;'
                'Connection Timeout=30;'
            )
            
            self.connection = pyodbc.connect(connection_string)
            print(" SQL Database connection established successfully")
            
        except Exception as e:
            print(f" SQL Database connection failed: {e}")
            self.connection = None
    
    def execute_archetype_query(self, archetype: str, codes: Dict[str, str]) -> List[Dict[str, Any]]:
        """Execute archetype-specific SQL query based on code combinations"""
        if not self.connection:
            print(" No SQL connection available")
            return []
        
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        sql_query = archetype_info.get('sql_query', '')
        
        if not sql_query:
            print(f" No SQL query defined for archetype: {archetype}")
            return []
        
        try:
            # Determine which code to query based on archetype
            query_param = None
            if archetype in ["NCCI_PTP_Conflict", "MUE_Risk", "Bundled_Payment_Conflict", "Missing_Modifier", "Terminated_Retired_Code"]:
                query_param = codes.get('hcpcs_code')
            elif archetype in ["Primary_DX_Not_Covered", "Secondary_DX_Not_Covered"]:
                query_param = codes.get('icd9_code')
            elif archetype == "Site_of_Service_Mismatch":
                query_param = codes.get('hcpcs_code')
            elif archetype == "Compliant":
                query_param = None  # No parameter needed
            
            if query_param:
                df = pd.read_sql(sql_query, self.connection, params=[query_param])
            else:
                df = pd.read_sql(sql_query, self.connection)
            
            evidence = df.to_dict('records')
            print(f"    SQL Evidence: Found {len(evidence)} records for archetype '{archetype}' with codes {codes}")
            return evidence
            
        except Exception as e:
            print(f" SQL query failed for archetype '{archetype}': {e}")
            return []
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print(" SQL Database connection closed")

# =============================================================================
# ENHANCED ARCHETYPE-DRIVEN CLAIM CORRECTOR v2.0
# =============================================================================

class ArchetypeDrivenClaimCorrectorV2:
    """
    Enhanced Archetype-Driven Claim Corrector v2.0
    
    Features:
    - 9 comprehensive archetypes for denial pattern detection
    - SQL-driven evidence gathering for fact-based corrections
    - Code-level policy logic targeting
    - Enhanced Qdrant policy retrieval
    - Structured JSON output with confidence scoring
    """
    
    def __init__(self):
        """Initialize the enhanced archetype-driven corrector"""
        print(" Initializing Enhanced Archetype-Driven Claim Corrector v2.0...")
        
        # Initialize components
        self.calibrated_corrector = CalibratedClaimCorrector()
        self.sql_connector = SQLDatabaseConnector()
        self.qdrant_client = None
        self.embedder = None
        
        # Initialize Qdrant and embedding model
        self._initialize_qdrant()
        self._initialize_embedder()
        
        # Use the same embedder as the calibrated corrector to avoid dimension mismatches
        if hasattr(self.calibrated_corrector, 'embedder'):
            self.embedder = self.calibrated_corrector.embedder
        
        print(" Enhanced Archetype-Driven Claim Corrector v2.0 initialized successfully")
    
    def _initialize_qdrant(self):
        """Initialize Qdrant client and collections"""
        try:
            self.qdrant_client = QdrantClient(host="localhost", port=6333)
            print(" Qdrant client connected successfully")
            
            # Verify collections exist
            collections = self.qdrant_client.get_collections()
            collection_names = [col.name for col in collections.collections]
            print(f" Available collections: {collection_names}")
            
        except Exception as e:
            print(f" Qdrant initialization failed: {e}")
            self.qdrant_client = None
    
    def _initialize_embedder(self):
        """Initialize sentence transformer model"""
        try:
            self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
            print(" Sentence transformer model loaded successfully")
        except Exception as e:
            print(f" Embedder initialization failed: {e}")
            self.embedder = None
    
    def _detect_archetype(self, issue: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Enhanced archetype detection with improved logic"""
        # Check for PTP conflicts
        if issue.get('ptp_denial_reason') and issue.get('hcpcs_position') == 1:
            return "NCCI_PTP_Conflict", ARCHETYPE_DEFINITIONS["NCCI_PTP_Conflict"]
        
        # Check for MUE risks
        if issue.get('mue_threshold'):
            return "MUE_Risk", ARCHETYPE_DEFINITIONS["MUE_Risk"]
        
        # Check for primary diagnosis not covered
        if issue.get('lcd_icd10_covered_group') == 'N' and issue.get('dx_position') == 1:
            return "Primary_DX_Not_Covered", ARCHETYPE_DEFINITIONS["Primary_DX_Not_Covered"]
        
        # Check for secondary diagnosis not covered
        if issue.get('lcd_icd10_covered_group') == 'N' and issue.get('dx_position') > 1:
            return "Secondary_DX_Not_Covered", ARCHETYPE_DEFINITIONS["Secondary_DX_Not_Covered"]
        
        # Check for NCD termination
        if issue.get('ncd_status') == 'Terminated':
            return "NCD_Terminated", ARCHETYPE_DEFINITIONS["NCD_Terminated"]
        
        # Check for bundled payment conflicts (heuristic based on multiple HCPCS codes)
        if issue.get('hcpcs_position') > 1 and issue.get('ptp_denial_reason'):
            return "Bundled_Payment_Conflict", ARCHETYPE_DEFINITIONS["Bundled_Payment_Conflict"]
        
        # Check for missing modifiers (heuristic based on procedure type)
        hcpcs_code = issue.get('hcpcs_code', '')
        if hcpcs_code and any(x in hcpcs_code for x in ['26', 'TC', '50', 'RT', 'LT']):
            # If modifier codes are present but not in claim structure, flag as missing
            return "Missing_Modifier", ARCHETYPE_DEFINITIONS["Missing_Modifier"]
        
        # Check for site of service mismatch (heuristic based on procedure type)
        if issue.get('hcpcs_code') and issue.get('procedure_name'):
            procedure_name = issue.get('procedure_name', '').lower()
            if any(keyword in procedure_name for keyword in ['office', 'outpatient', 'inpatient', 'facility']):
                return "Site_of_Service_Mismatch", ARCHETYPE_DEFINITIONS["Site_of_Service_Mismatch"]
        
        # Check for terminated/retired codes (heuristic based on old codes)
        if hcpcs_code and len(hcpcs_code) == 5 and hcpcs_code.startswith('9'):
            # Some old 90000 series codes might be retired
            return "Terminated_Retired_Code", ARCHETYPE_DEFINITIONS["Terminated_Retired_Code"]
        
        # Check for frequency limits (heuristic based on multiple units)
        if issue.get('billed_units', 0) > 10:  # High unit count might indicate frequency issues
            return "Frequency_Limit_Exceeded", ARCHETYPE_DEFINITIONS["Frequency_Limit_Exceeded"]
        
        # Default to compliant if no other conditions match
        return "Compliant", ARCHETYPE_DEFINITIONS["Compliant"]
    
    def _build_archetype_query(self, issue: Dict[str, Any], archetype: str) -> str:
        """Build archetype-specific search query for Qdrant"""
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        collections = archetype_info.get('qdrant_collections', [])
        
        # Build query based on archetype
        query_parts = []
        
        # Always include basic claim information
        query_parts.append(f"CPT {issue.get('hcpcs_code', '')}")
        query_parts.append(f"ICD {issue.get('icd9_code', '')}")
        
        # Add archetype-specific terms
        if archetype == "NCCI_PTP_Conflict":
            query_parts.extend(["NCCI", "procedure to procedure", "PTP", "edit", "modifier"])
        elif archetype == "Primary_DX_Not_Covered":
            query_parts.extend(["LCD", "local coverage", "diagnosis", "coverage", "not covered"])
        elif archetype == "MUE_Risk":
            query_parts.extend(["MUE", "medically unlikely", "units", "threshold"])
        elif archetype == "Bundled_Payment_Conflict":
            query_parts.extend(["bundled", "DRG", "APC", "OPPS", "secondary"])
        elif archetype == "Frequency_Limit_Exceeded":
            query_parts.extend(["frequency", "annual", "limit", "exceeded"])
        elif archetype == "Missing_Modifier":
            query_parts.extend(["modifier", "26", "TC", "50", "LT", "RT"])
        elif archetype == "Site_of_Service_Mismatch":
            query_parts.extend(["place of service", "POS", "facility", "office"])
        elif archetype == "Terminated_Retired_Code":
            query_parts.extend(["terminated", "retired", "obsolete", "replaced"])
        
        return " ".join(query_parts)
    
    def _search_archetype_corrections(self, issue: Dict[str, Any], archetype: str) -> List[Dict[str, Any]]:
        """Search for archetype-specific correction policies in Qdrant"""
        if not self.qdrant_client or not self.embedder:
            print(" Qdrant client or embedder not available")
            return []
        
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        collections = archetype_info.get('qdrant_collections', [])
        
        if not collections:
            print(f" No Qdrant collections defined for archetype: {archetype}")
            return []
        
        # Build search query
        query_text = self._build_archetype_query(issue, archetype)
        print(f"    Searching for: {query_text}")
        
        # Generate embedding
        query_vector = self.embedder.encode(query_text).tolist()
        
        all_results = []
        
        # Search in each collection
        for collection_name in collections:
            try:
                # Check if collection exists
                collections_info = self.qdrant_client.get_collections()
                collection_names = [col.name for col in collections_info.collections]
                
                if collection_name not in collection_names:
                    print(f"    Collection {collection_name} does not exist, skipping")
                    continue
                
                search_result = self.qdrant_client.search(
                    collection_name=collection_name,
                    query_vector=query_vector,
                    limit=3,
                    score_threshold=0.7
                )
                
                for hit in search_result:
                    result = hit.payload.copy()
                    result['score'] = hit.score
                    result['collection'] = collection_name
                    all_results.append(result)
                
                print(f"    Found {len(search_result)} results in {collection_name}")
                
            except Exception as e:
                print(f"    Search failed in {collection_name}: {e}")
        
        # Sort by score and return top results
        all_results.sort(key=lambda x: x.get('score', 0), reverse=True)
        return all_results[:5]  # Return top 5 results
    
    def _run_sql_driven_archetype_stage2_llm(self, issue: Dict[str, Any], stage1_result: Dict[str, Any], 
                                           correction_policies: List[Dict[str, Any]], archetype: str, 
                                           sql_evidence: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run SQL-driven archetype Stage 2 LLM analysis"""
        try:
            import ollama
        except ImportError:
            return {"error": "Ollama not available"}
        
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        
        # Format archetype info
        archetype_text = f"Archetype: {archetype}\n"
        archetype_text += f"Description: {archetype_info.get('description', '')}\n"
        archetype_text += f"Business Impact: {archetype_info.get('business_impact', '')}\n"
        archetype_text += f"Action Required: {archetype_info.get('action_required', '')}"
        
        # Format denial analysis
        denial_analysis = stage1_result.get('denial_analysis', {})
        denial_text = f"Summary: {denial_analysis.get('summary', 'No analysis available')}"
        
        # Format correction policies
        policies_text = ""
        if correction_policies:
            for i, policy in enumerate(correction_policies, 1):
                policies_text += f"\nPolicy {i}:\n"
                policies_text += f"Source: {policy.get('source', 'Unknown')}\n"
                policies_text += f"Text: {policy.get('text', '')[:500]}...\n"
                policies_text += f"Score: {policy.get('score', 0):.3f}\n"
        else:
            policies_text = "No specific correction policies found"
        
        # Format SQL evidence
        sql_evidence_text = ""
        if sql_evidence:
            for i, evidence in enumerate(sql_evidence, 1):
                sql_evidence_text += f"\nSQL EVIDENCE {i}:\n"
                for key, value in evidence.items():
                    sql_evidence_text += f"  {key}: {value}\n"
        else:
            sql_evidence_text = "No SQL evidence found for this claim/archetype combination."
        
        # Format correction strategies
        strategies = archetype_info.get('correction_strategies', [])
        strategies_text = "\n".join([f"- {strategy}" for strategy in strategies])
        
        # Format SQL-driven archetype Stage 2 prompt
        prompt = STAGE2_SQL_DRIVEN_ARCHETYPE_CORRECTION_PROMPT.format(
            archetype=archetype,
            archetype_description=archetype_info.get('description', ''),
            business_impact=archetype_info.get('business_impact', ''),
            action_required=archetype_info.get('action_required', ''),
            claim_id=issue.get('claim_id', ''),
            patient_id=issue.get('patient_id', ''),
            provider_id=issue.get('provider_id', ''),
            service_date=issue.get('service_date', ''),
            primary_diagnosis=issue.get('diagnosis_name', ''),
            icd9_code=issue.get('icd9_code', ''),
            icd10_code=issue.get('icd10_code', ''),
            primary_procedure=issue.get('procedure_name', ''),
            hcpcs_code=issue.get('hcpcs_code', ''),
            denial_risk_level=issue.get('denial_risk_level', ''),
            risk_score=issue.get('denial_risk_score', 0),
            denial_analysis=denial_text,
            sql_evidence=sql_evidence_text,
            correction_policies=policies_text,
            correction_strategies=strategies_text
        )
        
        try:
            print(f"    Running SQL-driven archetype Stage 2 LLM for '{archetype}'...")
            
            response = ollama.generate(
                model="mistral",
                prompt=prompt,
                options={
                    "temperature": 0.1,
                    "top_p": 0.9,
                    "num_predict": 2048
                }
            )
            
            response_text = response['response'].strip()
            print(f"    LLM Response Length: {len(response_text)} characters")
            
            # Try to parse JSON response
            try:
                correction_analysis = json.loads(response_text)
                print(f"    Successfully parsed JSON response")
                return correction_analysis
            except json.JSONDecodeError as e:
                print(f"    JSON parsing failed: {e}")
                return {
                    "error": f"JSON parsing failed: {e}",
                    "raw_response": response_text[:500] + "..." if len(response_text) > 500 else response_text
                }
        
        except Exception as e:
            print(f"    LLM call failed: {e}")
            return {"error": f"LLM call failed: {e}"}
    
    def _stage2_archetype_corrective_reasoning(self, issue: Dict[str, Any], stage1_result: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced Stage 2 archetype corrective reasoning with SQL evidence"""
        print(f"    STAGE 2: Archetype-driven corrective reasoning...")
        
        # Detect archetype
        archetype, archetype_info = self._detect_archetype(issue)
        print(f"    Stage 2: Detected archetype '{archetype}' - {archetype_info['description']}")
        
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
    
    def run_archetype_driven_corrections(self, claim_id: str) -> Dict[str, Any]:
        """Run enhanced archetype-driven corrections for a claim"""
        print(f" ENHANCED ARCHETYPE-DRIVEN PROCESSING: {claim_id}")
        
        # Stage 1: Get calibrated denial analysis
        print(" STAGE 1: Calibrated denial reasoning analysis...")
        stage1_result = self.calibrated_corrector.run_corrections(claim_id)
        
        if not stage1_result.get('enriched_issues'):
            print(" No claim issues found in Stage 1")
            return {
                "claim_id": claim_id,
                "error": "No claim issues found",
                "stage1_result": stage1_result
            }
        
        print(f" Found {len(stage1_result['enriched_issues'])} claim issues")
        
        # Stage 2: Process each issue with archetype-driven corrections
        enriched_issues = []
        for i, issue in enumerate(stage1_result['enriched_issues'], 1):
            print(f" Processing issue {i}: {issue.get('hcpcs_code', '')} + {issue.get('icd9_code', '')}")
            
            # Run Stage 2 archetype corrective reasoning
            stage2_result = self._stage2_archetype_corrective_reasoning(issue, stage1_result)
            
            # Combine results
            enriched_issue = issue.copy()
            enriched_issue['stage2_archetype_correction_analysis'] = stage2_result
            enriched_issue['archetype_driven_complete'] = True
            
            enriched_issues.append(enriched_issue)
        
        return {
            "claim_id": claim_id,
            "enriched_issues": enriched_issues,
            "total_issues": len(enriched_issues),
            "processing_timestamp": datetime.now().isoformat(),
            "version": "2.0"
        }
    
    def cleanup(self):
        """Clean up resources"""
        if self.sql_connector:
            self.sql_connector.close()
        
        # Don't close the calibrated corrector's SQL connection
        # as it manages its own lifecycle

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    try:
        corrector = ArchetypeDrivenClaimCorrectorV2()
        
        # Use test claim ID to demonstrate enhanced archetype system
        claim_id = "123456789012345"
        enriched = corrector.run_archetype_driven_corrections(claim_id)
        
        print("\n" + "="*80)
        print(" ENHANCED ARCHETYPE-DRIVEN CORRECTION RESULTS")
        print("="*80)
        print(json.dumps(enriched, indent=2))
        
    finally:
        corrector.cleanup()

"""
Enhanced Archetype-Driven Claim Corrector v2.0
==============================================

This system implements a comprehensive CMS policy reasoning architecture with:
- 9 comprehensive archetypes for denial pattern detection
- SQL-driven evidence gathering for fact-based corrections
- Code-level policy logic targeting (ICD-HCPCS-MUE-LCD/NCD linkages)
- Enhanced Qdrant policy retrieval with archetype-specific collections
- Structured JSON output with confidence scoring and traceability

Author: AI Assistant
Version: 2.0
Date: 2025-01-04
"""

import json
import logging
import time
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import os
import sys

# Add the parent directory to the path to import the calibrated claim corrector
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from claim_corrector_claims3_calibrated import CalibratedClaimCorrector

# Import required libraries for database and vector operations
try:
    import pyodbc
    import pandas as pd
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
    from sentence_transformers import SentenceTransformer
    import ollama
    import numpy as np
except ImportError as e:
    print(f" Missing required library: {e}")
    print("Please install required packages: pip install pyodbc pandas qdrant-client sentence-transformers ollama numpy")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =============================================================================
# ENHANCED ARCHETYPE DEFINITIONS v2.0
# =============================================================================

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
        "sql_insight": "Finds procedures that violate NCCI edits, identifying conflict type and allowed modifiers.",
        "correction_strategies": [
            "Add valid NCCI modifier (59, XE, XP, XS, XU).",
            "Split procedures into separate claim lines.",
            "Verify same-day compatibility using NCCI table."
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
            WHERE g.icd9_code = ?
        """,
        "sql_insight": "Retrieves coverage mapping for ICD-10 code to identify whether diagnosis is covered under current LCDs/NCDs.",
        "correction_strategies": [
            "Replace the ICD-10 diagnosis with a covered one per LCD crosswalk.",
            "Validate medical necessity using LCD/NCD coverage criteria."
        ],
        "sample_reference": "LCD L34696  Fracture and Bone Imaging Coverage"
    },

    "MUE_Risk": {
        "description": "Billed units exceed the Medically Unlikely Edit (MUE) threshold.",
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
        "sql_insight": "Retrieves MUE thresholds for the given CPT/HCPCS code.",
        "correction_strategies": [
            "Reduce billed units to  MUE limit.",
            "Include justification documentation.",
            "Check if MUE has MAI of 1 (line edit) or 2/3 (date-of-service edit)."
        ],
        "sample_reference": "CMS NCCI MUE Table  Transmittal 12674"
    },

    "Bundled_Payment_Conflict": {
        "description": "Claim includes services bundled under DRG/APC or OPPS rules.",
        "trigger_condition": "bundle_type IN ('DRG','APC')",
        "risk_category": "HIGH",
        "business_impact": "FULL DENIAL: Secondary procedures may be bundled.",
        "action_required": "REVIEW: Bill under DRG/APC bundle instead of separate CPT lines.",
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
        "sql_insight": "Finds HCPCS codes that are secondary to bundled payments using NCCI modifier compliance data.",
        "correction_strategies": [
            "Remove bundled CPT from separate line.",
            "Submit under primary procedure bundle.",
            "Use appropriate modifier if unbundling is medically necessary."
        ],
        "sample_reference": "OPPS/APC Payment System Guidelines"
    },

    "Frequency_Limit_Exceeded": {
        "description": "Annual visit or unit frequency exceeds allowable limits.",
        "trigger_condition": "COUNT(hcpcs_code) > mue_threshold",
        "risk_category": "HIGH",
        "business_impact": "PARTIAL DENIAL: Frequency exceeded.",
        "action_required": "REVIEW: Reduce service frequency or add justification.",
        "qdrant_collections": ["claims__ncci_edits"],
        "sql_query": """
            SELECT 
                mue.hcpcs_cpt_code,
                mue.practitioner_mue_values as mue_threshold,
                mue.mue_adjudication_indicator,
                mue.mue_rationale
            FROM [_ncci_].[dbo].[mue_practitioner] mue
            WHERE mue.hcpcs_cpt_code = ?
              AND mue.practitioner_mue_values IS NOT NULL
        """,
        "sql_insight": "Retrieves MUE frequency limits for practitioner services to validate annual usage patterns.",
        "correction_strategies": [
            "Reduce number of billed units to within MUE limits.",
            "Provide supporting documentation for medical necessity.",
            "Consider splitting across multiple dates if medically appropriate."
        ],
        "sample_reference": "CMS MUE / Frequency Edit Policy"
    },

    "Missing_Modifier": {
        "description": "Procedure requires but lacks a CPT modifier (26, TC, 50, etc.).",
        "trigger_condition": "required_modifiers IS NOT NULL AND NOT EXISTS IN claim",
        "risk_category": "MEDIUM",
        "business_impact": "DENIAL: Claim incomplete due to missing modifier.",
        "action_required": "Add required modifier per CPT guidance.",
        "qdrant_collections": ["claims__med_claims_policies"],
        "sql_query": """
            SELECT 
                ptp.primary_cpt_hcpcs_code,
                ptp.secondary_cpt_hcpcs_code,
                ptp.modifier_guidance,
                ptp.ptp_edit_rationale,
                ptp.edit_type
            FROM [_ncci_].[dbo].[vw_NCCI_Modifier_Compliance] ptp
            WHERE ptp.primary_cpt_hcpcs_code = ?
              AND ptp.modifier_guidance IS NOT NULL
        """,
        "sql_insight": "Retrieves modifier requirements for CPT/HCPCS codes from NCCI compliance data.",
        "correction_strategies": [
            "Add missing modifier (26, TC, 50, LT, RT) per NCCI guidance.",
            "Ensure claim line corresponds to professional/technical component.",
            "Verify modifier appropriateness for procedure combination."
        ],
        "sample_reference": "CPT Modifier Appendix A"
    },

    "Site_of_Service_Mismatch": {
        "description": "Place of service code conflicts with CPT/HCPCS requirements.",
        "trigger_condition": "place_of_service_code NOT IN allowed_place_of_service",
        "risk_category": "HIGH",
        "business_impact": "DENIAL: Wrong site of service for CPT code.",
        "action_required": "Bill under correct site of service.",
        "qdrant_collections": ["claims__med_claims_policies"],
        "sql_query": """
            SELECT 
                lcd.hcpc_code_group,
                lcd.coverage_group_description,
                lcd.covered_codes,
                lcd.code_description
            FROM [_lcd].[dbo].[vw_LCD_HCPC_Coverage] lcd
            WHERE lcd.hcpc_code_group LIKE '%' + ? + '%'
        """,
        "sql_insight": "Checks LCD HCPC coverage to identify site-of-service requirements for specific procedures.",
        "correction_strategies": [
            "Change POS code to match procedure requirements (11 for Office, 22 for Outpatient, etc.).",
            "Bill under correct facility/non-facility rate schedule.",
            "Verify LCD coverage for specific site of service."
        ],
        "sample_reference": "CMS POS-CPT Crosswalk 2024"
    },

    "Terminated_Retired_Code": {
        "description": "CPT/HCPCS code is obsolete or replaced by newer code.",
        "trigger_condition": "termination_date < GETDATE()",
        "risk_category": "MEDIUM",
        "business_impact": "DENIAL: Code no longer active.",
        "action_required": "Replace with current CPT/HCPCS code.",
        "qdrant_collections": ["claims__ncd_policies"],
        "sql_query": """
            SELECT 
                ncd.NCD_lab,
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
        "sql_insight": "Identifies terminated or retired codes with replacement mappings from NCD tracking.",
        "correction_strategies": [
            "Replace retired CPT with successor code from NCD updates.",
            "Update billing system with active code sets.",
            "Check NCD termination notices for replacement guidance."
        ],
        "sample_reference": "NCD Manual Pub 100-03, Terminated Codes"
    },

    "Compliant": {
        "description": "Claim appears compliant and passes all denial checks.",
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
        "sql_insight": "Identifies claims that passed all other denial checks for positive examples.",
        "correction_strategies": [
            "Maintain documentation and proceed with billing."
        ],
        "sample_reference": "CMS Claims Processing Manual, Ch.12 40"
    }
}

# =============================================================================
# ENHANCED LLM PROMPTS v2.0
# =============================================================================

STAGE2_SQL_DRIVEN_ARCHETYPE_CORRECTION_PROMPT = """
You are a CMS policy correction expert specializing in SQL-driven archetype claim remediation.

CONTEXT:
You are analyzing a claim that has been identified with a specific denial archetype: {archetype}
Archetype Description: {archetype_description}
Business Impact: {business_impact}
Action Required: {action_required}

CLAIM DATA:
- Claim ID: {claim_id}
- Patient ID: {patient_id}
- Provider ID: {provider_id}
- Service Date: {service_date}
- Primary Diagnosis: {primary_diagnosis} ({icd9_code}  {icd10_code})
- Primary Procedure: {primary_procedure} ({hcpcs_code})
- Denial Risk Level: {denial_risk_level}
- Risk Score: {risk_score}

STAGE 1 DENIAL ANALYSIS:
{denial_analysis}

SQL EVIDENCE FROM DATABASE:
{sql_evidence}

ARCHETYPE-SPECIFIC CORRECTION POLICIES:
{correction_policies}

CORRECTION STRATEGIES FOR THIS ARCHETYPE:
{correction_strategies}

INSTRUCTIONS:
1. Analyze the SQL evidence to understand the specific policy violations
2. Cross-reference with retrieved CMS policies to find corrective guidance
3. Provide specific, actionable corrections based on database facts
4. Link each recommendation to specific SQL evidence and policy references
5. Include implementation guidance with confidence scoring
6. Ensure corrections are fact-driven, not speculative

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

CRITICAL REQUIREMENTS:
- Use EXACT claim data provided (no hallucinations)
- Base corrections on SQL evidence, not assumptions
- Provide specific policy references for each recommendation
- Include confidence scores (0.0-1.0) for each correction
- Ensure JSON is valid and parseable
"""

# =============================================================================
# ENHANCED SQL DATABASE CONNECTOR v2.0
# =============================================================================

class SQLDatabaseConnector:
    """Enhanced SQL database connector for archetype-specific evidence gathering"""
    
    def __init__(self):
        self.connection = None
        self._connect()
    
    def _connect(self):
        """Establish SQL Server connection with proper credentials"""
        try:
            connection_string = (
                'Driver={ODBC Driver 18 for SQL Server};'
                'Server=localhost,1433;'
                'UID=SA;'
                'PWD=Bbanwo@1980!;'
                'Database=_claims;'
                'Encrypt=yes;'
                'TrustServerCertificate=yes;'
                'Connection Timeout=30;'
            )
            
            self.connection = pyodbc.connect(connection_string)
            print(" SQL Database connection established successfully")
            
        except Exception as e:
            print(f" SQL Database connection failed: {e}")
            self.connection = None
    
    def execute_archetype_query(self, archetype: str, codes: Dict[str, str]) -> List[Dict[str, Any]]:
        """Execute archetype-specific SQL query based on code combinations"""
        if not self.connection:
            print(" No SQL connection available")
            return []
        
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        sql_query = archetype_info.get('sql_query', '')
        
        if not sql_query:
            print(f" No SQL query defined for archetype: {archetype}")
            return []
        
        try:
            # Determine which code to query based on archetype
            query_param = None
            if archetype in ["NCCI_PTP_Conflict", "MUE_Risk", "Bundled_Payment_Conflict", "Missing_Modifier", "Terminated_Retired_Code"]:
                query_param = codes.get('hcpcs_code')
            elif archetype in ["Primary_DX_Not_Covered", "Secondary_DX_Not_Covered"]:
                query_param = codes.get('icd9_code')
            elif archetype == "Site_of_Service_Mismatch":
                query_param = codes.get('hcpcs_code')
            elif archetype == "Compliant":
                query_param = None  # No parameter needed
            
            if query_param:
                df = pd.read_sql(sql_query, self.connection, params=[query_param])
            else:
                df = pd.read_sql(sql_query, self.connection)
            
            evidence = df.to_dict('records')
            print(f"    SQL Evidence: Found {len(evidence)} records for archetype '{archetype}' with codes {codes}")
            return evidence
            
        except Exception as e:
            print(f" SQL query failed for archetype '{archetype}': {e}")
            return []
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print(" SQL Database connection closed")

# =============================================================================
# ENHANCED ARCHETYPE-DRIVEN CLAIM CORRECTOR v2.0
# =============================================================================

class ArchetypeDrivenClaimCorrectorV2:
    """
    Enhanced Archetype-Driven Claim Corrector v2.0
    
    Features:
    - 9 comprehensive archetypes for denial pattern detection
    - SQL-driven evidence gathering for fact-based corrections
    - Code-level policy logic targeting
    - Enhanced Qdrant policy retrieval
    - Structured JSON output with confidence scoring
    """
    
    def __init__(self):
        """Initialize the enhanced archetype-driven corrector"""
        print(" Initializing Enhanced Archetype-Driven Claim Corrector v2.0...")
        
        # Initialize components
        self.calibrated_corrector = CalibratedClaimCorrector()
        self.sql_connector = SQLDatabaseConnector()
        self.qdrant_client = None
        self.embedder = None
        
        # Initialize Qdrant and embedding model
        self._initialize_qdrant()
        self._initialize_embedder()
        
        # Use the same embedder as the calibrated corrector to avoid dimension mismatches
        if hasattr(self.calibrated_corrector, 'embedder'):
            self.embedder = self.calibrated_corrector.embedder
        
        print(" Enhanced Archetype-Driven Claim Corrector v2.0 initialized successfully")
    
    def _initialize_qdrant(self):
        """Initialize Qdrant client and collections"""
        try:
            self.qdrant_client = QdrantClient(host="localhost", port=6333)
            print(" Qdrant client connected successfully")
            
            # Verify collections exist
            collections = self.qdrant_client.get_collections()
            collection_names = [col.name for col in collections.collections]
            print(f" Available collections: {collection_names}")
            
        except Exception as e:
            print(f" Qdrant initialization failed: {e}")
            self.qdrant_client = None
    
    def _initialize_embedder(self):
        """Initialize sentence transformer model"""
        try:
            self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
            print(" Sentence transformer model loaded successfully")
        except Exception as e:
            print(f" Embedder initialization failed: {e}")
            self.embedder = None
    
    def _detect_archetype(self, issue: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Enhanced archetype detection with improved logic"""
        # Check for PTP conflicts
        if issue.get('ptp_denial_reason') and issue.get('hcpcs_position') == 1:
            return "NCCI_PTP_Conflict", ARCHETYPE_DEFINITIONS["NCCI_PTP_Conflict"]
        
        # Check for MUE risks
        if issue.get('mue_threshold'):
            return "MUE_Risk", ARCHETYPE_DEFINITIONS["MUE_Risk"]
        
        # Check for primary diagnosis not covered
        if issue.get('lcd_icd10_covered_group') == 'N' and issue.get('dx_position') == 1:
            return "Primary_DX_Not_Covered", ARCHETYPE_DEFINITIONS["Primary_DX_Not_Covered"]
        
        # Check for secondary diagnosis not covered
        if issue.get('lcd_icd10_covered_group') == 'N' and issue.get('dx_position') > 1:
            return "Secondary_DX_Not_Covered", ARCHETYPE_DEFINITIONS["Secondary_DX_Not_Covered"]
        
        # Check for NCD termination
        if issue.get('ncd_status') == 'Terminated':
            return "NCD_Terminated", ARCHETYPE_DEFINITIONS["NCD_Terminated"]
        
        # Check for bundled payment conflicts (heuristic based on multiple HCPCS codes)
        if issue.get('hcpcs_position') > 1 and issue.get('ptp_denial_reason'):
            return "Bundled_Payment_Conflict", ARCHETYPE_DEFINITIONS["Bundled_Payment_Conflict"]
        
        # Check for missing modifiers (heuristic based on procedure type)
        hcpcs_code = issue.get('hcpcs_code', '')
        if hcpcs_code and any(x in hcpcs_code for x in ['26', 'TC', '50', 'RT', 'LT']):
            # If modifier codes are present but not in claim structure, flag as missing
            return "Missing_Modifier", ARCHETYPE_DEFINITIONS["Missing_Modifier"]
        
        # Check for site of service mismatch (heuristic based on procedure type)
        if issue.get('hcpcs_code') and issue.get('procedure_name'):
            procedure_name = issue.get('procedure_name', '').lower()
            if any(keyword in procedure_name for keyword in ['office', 'outpatient', 'inpatient', 'facility']):
                return "Site_of_Service_Mismatch", ARCHETYPE_DEFINITIONS["Site_of_Service_Mismatch"]
        
        # Check for terminated/retired codes (heuristic based on old codes)
        if hcpcs_code and len(hcpcs_code) == 5 and hcpcs_code.startswith('9'):
            # Some old 90000 series codes might be retired
            return "Terminated_Retired_Code", ARCHETYPE_DEFINITIONS["Terminated_Retired_Code"]
        
        # Check for frequency limits (heuristic based on multiple units)
        if issue.get('billed_units', 0) > 10:  # High unit count might indicate frequency issues
            return "Frequency_Limit_Exceeded", ARCHETYPE_DEFINITIONS["Frequency_Limit_Exceeded"]
        
        # Default to compliant if no other conditions match
        return "Compliant", ARCHETYPE_DEFINITIONS["Compliant"]
    
    def _build_archetype_query(self, issue: Dict[str, Any], archetype: str) -> str:
        """Build archetype-specific search query for Qdrant"""
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        collections = archetype_info.get('qdrant_collections', [])
        
        # Build query based on archetype
        query_parts = []
        
        # Always include basic claim information
        query_parts.append(f"CPT {issue.get('hcpcs_code', '')}")
        query_parts.append(f"ICD {issue.get('icd9_code', '')}")
        
        # Add archetype-specific terms
        if archetype == "NCCI_PTP_Conflict":
            query_parts.extend(["NCCI", "procedure to procedure", "PTP", "edit", "modifier"])
        elif archetype == "Primary_DX_Not_Covered":
            query_parts.extend(["LCD", "local coverage", "diagnosis", "coverage", "not covered"])
        elif archetype == "MUE_Risk":
            query_parts.extend(["MUE", "medically unlikely", "units", "threshold"])
        elif archetype == "Bundled_Payment_Conflict":
            query_parts.extend(["bundled", "DRG", "APC", "OPPS", "secondary"])
        elif archetype == "Frequency_Limit_Exceeded":
            query_parts.extend(["frequency", "annual", "limit", "exceeded"])
        elif archetype == "Missing_Modifier":
            query_parts.extend(["modifier", "26", "TC", "50", "LT", "RT"])
        elif archetype == "Site_of_Service_Mismatch":
            query_parts.extend(["place of service", "POS", "facility", "office"])
        elif archetype == "Terminated_Retired_Code":
            query_parts.extend(["terminated", "retired", "obsolete", "replaced"])
        
        return " ".join(query_parts)
    
    def _search_archetype_corrections(self, issue: Dict[str, Any], archetype: str) -> List[Dict[str, Any]]:
        """Search for archetype-specific correction policies in Qdrant"""
        if not self.qdrant_client or not self.embedder:
            print(" Qdrant client or embedder not available")
            return []
        
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        collections = archetype_info.get('qdrant_collections', [])
        
        if not collections:
            print(f" No Qdrant collections defined for archetype: {archetype}")
            return []
        
        # Build search query
        query_text = self._build_archetype_query(issue, archetype)
        print(f"    Searching for: {query_text}")
        
        # Generate embedding
        query_vector = self.embedder.encode(query_text).tolist()
        
        all_results = []
        
        # Search in each collection
        for collection_name in collections:
            try:
                # Check if collection exists
                collections_info = self.qdrant_client.get_collections()
                collection_names = [col.name for col in collections_info.collections]
                
                if collection_name not in collection_names:
                    print(f"    Collection {collection_name} does not exist, skipping")
                    continue
                
                search_result = self.qdrant_client.search(
                    collection_name=collection_name,
                    query_vector=query_vector,
                    limit=3,
                    score_threshold=0.7
                )
                
                for hit in search_result:
                    result = hit.payload.copy()
                    result['score'] = hit.score
                    result['collection'] = collection_name
                    all_results.append(result)
                
                print(f"    Found {len(search_result)} results in {collection_name}")
                
            except Exception as e:
                print(f"    Search failed in {collection_name}: {e}")
        
        # Sort by score and return top results
        all_results.sort(key=lambda x: x.get('score', 0), reverse=True)
        return all_results[:5]  # Return top 5 results
    
    def _run_sql_driven_archetype_stage2_llm(self, issue: Dict[str, Any], stage1_result: Dict[str, Any], 
                                           correction_policies: List[Dict[str, Any]], archetype: str, 
                                           sql_evidence: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run SQL-driven archetype Stage 2 LLM analysis"""
        try:
            import ollama
        except ImportError:
            return {"error": "Ollama not available"}
        
        archetype_info = ARCHETYPE_DEFINITIONS.get(archetype, {})
        
        # Format archetype info
        archetype_text = f"Archetype: {archetype}\n"
        archetype_text += f"Description: {archetype_info.get('description', '')}\n"
        archetype_text += f"Business Impact: {archetype_info.get('business_impact', '')}\n"
        archetype_text += f"Action Required: {archetype_info.get('action_required', '')}"
        
        # Format denial analysis
        denial_analysis = stage1_result.get('denial_analysis', {})
        denial_text = f"Summary: {denial_analysis.get('summary', 'No analysis available')}"
        
        # Format correction policies
        policies_text = ""
        if correction_policies:
            for i, policy in enumerate(correction_policies, 1):
                policies_text += f"\nPolicy {i}:\n"
                policies_text += f"Source: {policy.get('source', 'Unknown')}\n"
                policies_text += f"Text: {policy.get('text', '')[:500]}...\n"
                policies_text += f"Score: {policy.get('score', 0):.3f}\n"
        else:
            policies_text = "No specific correction policies found"
        
        # Format SQL evidence
        sql_evidence_text = ""
        if sql_evidence:
            for i, evidence in enumerate(sql_evidence, 1):
                sql_evidence_text += f"\nSQL EVIDENCE {i}:\n"
                for key, value in evidence.items():
                    sql_evidence_text += f"  {key}: {value}\n"
        else:
            sql_evidence_text = "No SQL evidence found for this claim/archetype combination."
        
        # Format correction strategies
        strategies = archetype_info.get('correction_strategies', [])
        strategies_text = "\n".join([f"- {strategy}" for strategy in strategies])
        
        # Format SQL-driven archetype Stage 2 prompt
        prompt = STAGE2_SQL_DRIVEN_ARCHETYPE_CORRECTION_PROMPT.format(
            archetype=archetype,
            archetype_description=archetype_info.get('description', ''),
            business_impact=archetype_info.get('business_impact', ''),
            action_required=archetype_info.get('action_required', ''),
            claim_id=issue.get('claim_id', ''),
            patient_id=issue.get('patient_id', ''),
            provider_id=issue.get('provider_id', ''),
            service_date=issue.get('service_date', ''),
            primary_diagnosis=issue.get('diagnosis_name', ''),
            icd9_code=issue.get('icd9_code', ''),
            icd10_code=issue.get('icd10_code', ''),
            primary_procedure=issue.get('procedure_name', ''),
            hcpcs_code=issue.get('hcpcs_code', ''),
            denial_risk_level=issue.get('denial_risk_level', ''),
            risk_score=issue.get('denial_risk_score', 0),
            denial_analysis=denial_text,
            sql_evidence=sql_evidence_text,
            correction_policies=policies_text,
            correction_strategies=strategies_text
        )
        
        try:
            print(f"    Running SQL-driven archetype Stage 2 LLM for '{archetype}'...")
            
            response = ollama.generate(
                model="mistral",
                prompt=prompt,
                options={
                    "temperature": 0.1,
                    "top_p": 0.9,
                    "num_predict": 2048
                }
            )
            
            response_text = response['response'].strip()
            print(f"    LLM Response Length: {len(response_text)} characters")
            
            # Try to parse JSON response
            try:
                correction_analysis = json.loads(response_text)
                print(f"    Successfully parsed JSON response")
                return correction_analysis
            except json.JSONDecodeError as e:
                print(f"    JSON parsing failed: {e}")
                return {
                    "error": f"JSON parsing failed: {e}",
                    "raw_response": response_text[:500] + "..." if len(response_text) > 500 else response_text
                }
        
        except Exception as e:
            print(f"    LLM call failed: {e}")
            return {"error": f"LLM call failed: {e}"}
    
    def _stage2_archetype_corrective_reasoning(self, issue: Dict[str, Any], stage1_result: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced Stage 2 archetype corrective reasoning with SQL evidence"""
        print(f"    STAGE 2: Archetype-driven corrective reasoning...")
        
        # Detect archetype
        archetype, archetype_info = self._detect_archetype(issue)
        print(f"    Stage 2: Detected archetype '{archetype}' - {archetype_info['description']}")
        
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
    
    def run_archetype_driven_corrections(self, claim_id: str) -> Dict[str, Any]:
        """Run enhanced archetype-driven corrections for a claim"""
        print(f" ENHANCED ARCHETYPE-DRIVEN PROCESSING: {claim_id}")
        
        # Stage 1: Get calibrated denial analysis
        print(" STAGE 1: Calibrated denial reasoning analysis...")
        stage1_result = self.calibrated_corrector.run_corrections(claim_id)
        
        if not stage1_result.get('enriched_issues'):
            print(" No claim issues found in Stage 1")
            return {
                "claim_id": claim_id,
                "error": "No claim issues found",
                "stage1_result": stage1_result
            }
        
        print(f" Found {len(stage1_result['enriched_issues'])} claim issues")
        
        # Stage 2: Process each issue with archetype-driven corrections
        enriched_issues = []
        for i, issue in enumerate(stage1_result['enriched_issues'], 1):
            print(f" Processing issue {i}: {issue.get('hcpcs_code', '')} + {issue.get('icd9_code', '')}")
            
            # Run Stage 2 archetype corrective reasoning
            stage2_result = self._stage2_archetype_corrective_reasoning(issue, stage1_result)
            
            # Combine results
            enriched_issue = issue.copy()
            enriched_issue['stage2_archetype_correction_analysis'] = stage2_result
            enriched_issue['archetype_driven_complete'] = True
            
            enriched_issues.append(enriched_issue)
        
        return {
            "claim_id": claim_id,
            "enriched_issues": enriched_issues,
            "total_issues": len(enriched_issues),
            "processing_timestamp": datetime.now().isoformat(),
            "version": "2.0"
        }
    
    def cleanup(self):
        """Clean up resources"""
        if self.sql_connector:
            self.sql_connector.close()
        
        # Don't close the calibrated corrector's SQL connection
        # as it manages its own lifecycle

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    try:
        corrector = ArchetypeDrivenClaimCorrectorV2()
        
        # Use test claim ID to demonstrate enhanced archetype system
        claim_id = "123456789012345"
        enriched = corrector.run_archetype_driven_corrections(claim_id)
        
        print("\n" + "="*80)
        print(" ENHANCED ARCHETYPE-DRIVEN CORRECTION RESULTS")
        print("="*80)
        print(json.dumps(enriched, indent=2))
        
    finally:
        corrector.cleanup()


