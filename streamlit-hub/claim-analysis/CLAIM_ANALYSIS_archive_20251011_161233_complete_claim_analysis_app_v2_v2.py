#!/usr/bin/env python3
"""
CMS Claim Analysis Streamlit App (v2 Minimal)
=============================================
- Minimalist, uniform typography
- Load CMS inpatient/outpatient claims from SQL Server (`the_claims`)
- Run full analysis flows including Archetype-Driven v2
"""

import streamlit as st
import json
import pandas as pd
import pyodbc
import sys
import os
from datetime import datetime
from typing import Dict, Any, Optional

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from new_claim_analyzer1 import NewClaimAnalyzer
from claim_corrector_claims3_archetype_driven_v2 import ArchetypeDrivenClaimCorrectorV2
from claim_corrector_claims3_archetype_driven_v3 import ArchetypeDrivenClaimCorrectorV3
from claim_corrector_claims3_two_stage_calibrated import TwoStageCalibratedClaimCorrector
from claim_corrector_claims3_calibrated import CalibratedClaimCorrector
from claim_corrector_claims import ClaimCorrector
from fhir_adapter import validate_fhir_claim as _validate_fhir, convert_fhir_claim as _convert_fhir

# ----------------------------
# Page + Minimal Styles
# ----------------------------
st.set_page_config(page_title="CMS Claim Analysis v2", page_icon="", layout="wide")

st.markdown(
"""
<style>
:root { 
    --base-font: 14px; 
    --muted: #6b7280; 
    --text: #111827;
    --border: #e5e7eb;
}
html, body, [class*="css"] { 
    font-size: var(--base-font); 
    font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; 
    color: var(--text);
    line-height: 1.5;
}
h1, h2, h3, h4, h5, h6 { 
    font-size: var(--base-font) !important; 
    font-weight: 600 !important; 
    margin: 1rem 0 0.5rem 0 !important;
    color: var(--text) !important;
}
p, div, span, li { 
    font-size: var(--base-font) !important; 
    color: var(--text) !important;
    margin: 0.5rem 0 !important;
}
.block-title { 
    font-size: var(--base-font) !important; 
    font-weight: 600 !important; 
    margin: 1rem 0 0.5rem 0 !important; 
    color: var(--text) !important;
}
.small { 
    color: var(--muted); 
    font-size: var(--base-font) !important; 
}
.card { 
    border: 1px solid var(--border); 
    border-radius: 6px; 
    padding: 16px; 
    margin: 8px 0;
}
.btn-primary { 
    background: var(--text); 
    color: white; 
    padding: 8px 16px; 
    border-radius: 4px; 
    text-decoration: none; 
    font-size: var(--base-font) !important;
}
.metric { 
    font-size: var(--base-font) !important; 
    color: var(--text) !important;
}
.metric strong { 
    font-size: var(--base-font) !important; 
    color: var(--text) !important;
    font-weight: 600 !important;
}
/* Clean separators */
hr {
    border: none;
    border-top: 1px solid var(--border);
    margin: 1.5rem 0;
}
/* Uniform bullet points */
ul {
    margin-left: 20px;
    padding-left: 0;
}
li {
    font-size: var(--base-font) !important;
    margin: 0.25rem 0;
    list-style-type: disc;
}
/* Compact radio buttons - force single spacing */
.stRadio > div {
    gap: 0px !important;
    padding: 0 !important;
}
.stRadio > div > label {
    margin: 0 !important;
    padding: 2px 0 !important;
    gap: 6px !important;
}
.stRadio > div > label > div {
    padding: 0 !important;
    line-height: 1.2 !important;
}
.stRadio > div > label > div > div {
    margin: 0 !important;
    padding: 0 !important;
}
/* Analysis mode dropdown - visible selector, compact menu */
.stSelectbox > div > div {
    min-height: 42px !important;
}
.stSelectbox [data-baseweb="select"] > div {
    padding: 10px 14px !important;
    line-height: 1.5 !important;
    min-height: 42px !important;
    font-size: 14px !important;
}
.stSelectbox [data-baseweb="select"] {
    min-height: 42px !important;
}
/* Dropdown menu options - single spacing */
[data-baseweb="menu"] {
    padding: 4px !important;
}
[data-baseweb="menu"] li {
    padding: 6px 12px !important;
    margin: 0 !important;
    line-height: 1.3 !important;
}
[role="option"] {
    margin: 0 !important;
    padding: 6px 12px !important;
    line-height: 1.3 !important;
}
/* Fix JSON display - single spacing and compact */
pre {
    line-height: 1.1 !important;
    margin: 0 !important;
    padding: 6px 8px !important;
    font-size: 12px !important;
    background-color: #f8f9fa !important;
    border-radius: 4px !important;
}
code {
    line-height: 1.1 !important;
    font-size: 12px !important;
}
/* Streamlit JSON component - force compact */
.stJson {
    line-height: 1.1 !important;
}
.stJson pre {
    line-height: 1.1 !important;
    margin: 0 !important;
    padding: 6px !important;
}
/* Expander content - compact */
.streamlit-expanderContent {
    padding: 4px 8px !important;
}
/* Dataframe text wrapping - show full text */
.stDataFrame td, .stDataFrame th {
    white-space: normal !important;
    word-wrap: break-word !important;
    max-width: none !important;
}
.stDataFrame [data-testid="stDataFrameResizable"] {
    overflow-x: auto !important;
}
</style>
""",
unsafe_allow_html=True,
)

# ----------------------------
# DB Access for CMS Claims
# ----------------------------

def get_sql_conn():
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=localhost,1433;Database=_claims;"
        "UID=SA;PWD=Bbanwo@1980!;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)

@st.cache_data(show_spinner=False)
def list_tables() -> Dict[str, str]:
    try:
        with get_sql_conn() as conn:
            df = pd.read_sql("SELECT name FROM sys.tables ORDER BY name", conn)
        names = df['name'].str.lower().tolist()
        candidates = {
            'inpatient': next((n for n in names if 'inpatient' in n or 'ip' in n), None),
            'outpatient': next((n for n in names if 'outpatient' in n or 'op' in n), None),
        }
        return candidates
    except Exception as e:
        st.error(f"DB table discovery failed: {e}")
        return {'inpatient': None, 'outpatient': None}

@st.cache_data(show_spinner=False)
def load_claims(table: str, limit: int = 2000) -> pd.DataFrame:
    with get_sql_conn() as conn:
        q = f"SELECT TOP {limit} * FROM [{table}]"
        return pd.read_sql(q, conn)

# ----------------------------
# Normalization to App Schema
# ----------------------------

def normalize_row_to_claim(row: pd.Series) -> Dict[str, Any]:
    def pick(*keys):
        for k in keys:
            if k in row and pd.notna(row[k]):
                return str(row[k]).strip()
        return None

    clm_id = pick('CLM_ID','claim_id','clm_id','claimid') or f"cms-{row.name}"
    desyn = pick('DESYNPUF_ID','bene_id','patient_id') or "unknown-patient"
    from_dt = pick('CLM_FROM_DT','service_date','from_date','srvc_from_dt') or ""
    thru_dt = pick('CLM_THRU_DT','thru_date','srvc_thru_dt') or from_dt
    prvdr = pick('PRVDR_NUM','provider_id','npi','tin') or "unknown-provider"

    # Collect ICD-9/10 columns heuristically
    dx_map: Dict[str,str] = {}
    dx_cols = [c for c in row.index if str(c).lower().startswith(('icd9','icd10','dx','diag'))]
    pos = 1
    for c in dx_cols:
        val = row[c]
        if pd.notna(val) and str(val).strip():
            dx_map[f"ICD9_DGNS_CD_{pos}"] = str(val).strip()
            pos += 1
            if pos > 10:
                break

    # Collect HCPCS/CPT columns heuristically
    proc_map: Dict[str,str] = {}
    hcpcs_cols = [c for c in row.index if str(c).lower().startswith(('hcpcs','cpt','proc'))]
    ppos = 1
    for c in hcpcs_cols:
        val = row[c]
        if pd.notna(val) and str(val).strip():
            proc_map[f"HCPCS_CD_{ppos}"] = str(val).strip()
            ppos += 1
            if ppos > 45:
                break

    return {
        "CLM_ID": clm_id,
        "DESYNPUF_ID": desyn,
        "CLM_FROM_DT": from_dt,
        "CLM_THRU_DT": thru_dt,
        "PRVDR_NUM": prvdr,
        "diagnosis_codes": dx_map or {"ICD9_DGNS_CD_1": pick('ICD9_DGNS_CD_1','ICD10_DGNS_CD_1','DX1') or ""},
        "procedure_codes": proc_map or {"HCPCS_CD_1": pick('HCPCS_CD_1','CPT1','PROC1') or ""},
    }

# ----------------------------
# Analysis Runners
# ----------------------------

def run_basic(claim: Dict[str,Any]):
    analyzer = NewClaimAnalyzer()
    return analyzer.analyze_new_claim(claim)

def run_pipeline(claim: Dict[str,Any]):
    analyzer = NewClaimAnalyzer()
    diagnostics = analyzer.analyze_new_claim(claim)
    corrector = ClaimCorrector()
    enriched = corrector.run_corrections(diagnostics.get('claim_summary',{}).get('CLM_ID', claim['CLM_ID']))
    return {"stage1": diagnostics, "stage2": enriched}

def run_calibrated(claim_id: str):
    return CalibratedClaimCorrector().run_corrections(claim_id)

def run_two_stage_calibrated(claim_id: str):
    return TwoStageCalibratedClaimCorrector().run_two_stage_corrections(claim_id)

def run_archetype_v2(claim_id: str):
    return ArchetypeDrivenClaimCorrectorV2().run_archetype_driven_corrections(claim_id)

def run_archetype_v3(claim_id: str):
    return ArchetypeDrivenClaimCorrectorV3().run_archetype_driven_corrections(claim_id)

def show_compact_claim(claim: Dict[str, Any]):
    """Display claim in compact JSON format with custom CSS."""
    
    # Create compact JSON string
    json_str = json.dumps(claim, indent=2)
    
    # Display with custom styling
    st.markdown(f"""
    <div style='background:#f8f9fa; border:1px solid #e1e4e8; border-radius:6px; padding:10px 14px; font-family:monospace; font-size:12px; line-height:1.5; overflow-x:auto;'>
        <pre style='margin:0; padding:0; line-height:1.5; font-size:12px;'><code>{json_str}</code></pre>
    </div>
    """, unsafe_allow_html=True)

def show_v3_summary(data):
    """Show a clean, minimalist summary of v3 results with structured tables."""
    
    claim_id = data.get('claim_id', 'Unknown')
    total_issues = data.get('total_issues', 0)
    enriched_issues = data.get('enriched_issues', [])
    
    # Header Card
    st.markdown("---")
    st.markdown("### Analysis Summary")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**Claim ID:** {claim_id}")
    with col2:
        st.markdown(f"**Total Issues:** {total_issues}")
    st.markdown("---")
    
    # Stage 1: Issues Analysis Table
    st.markdown("### Stage 1: Issues Analysis")
    
    if enriched_issues:
        # Build table data
        table_data = []
        for idx, issue in enumerate(enriched_issues[:8], 1):  # Show top 8
            dx_code = issue.get('icd10_code', issue.get('icd9_code', 'Unknown'))
            dx_name = issue.get('diagnosis_name', '')
            proc_code = issue.get('hcpcs_code', 'Unknown')
            proc_name = issue.get('procedure_name', '')
            risk_level = issue.get('denial_risk_level', 'Unknown').replace('HIGH: ', '')
            risk_score = issue.get('denial_risk_score', 0)
            action = issue.get('action_required', 'Unknown').replace('IMMEDIATE: ', '').replace('REVIEW: ', '').replace('NO ACTION: ', '')
            
            # Format DX and PROC with names
            dx_display = f"DX {idx}: {dx_code}"
            if dx_name:
                dx_display += f" ({dx_name})"
            
            proc_display = f"Procedure {idx}: {proc_code}"
            if proc_name:
                proc_display += f" ({proc_name})"
            
            table_data.append({
                'Diagnosis': dx_display,
                'Procedure': proc_display,
                'Risk': risk_level,
                'Score': f"{risk_score:.0f}",
                'Action': action
            })
        
        if table_data:
            df_issues = pd.DataFrame(table_data)
            st.dataframe(df_issues, use_container_width=True, hide_index=True)
    
    st.markdown("")
    
    # Stage 2: Archetype Analysis Table
    st.markdown("### Stage 2: Archetype Analysis")
    
    # Count SQL evidence
    total_sql_evidence = 0
    archetype_counts = {}
    
    for issue in enriched_issues:
        stage2 = issue.get('stage2_archetype_correction_analysis')
        if stage2:
            archetype = stage2.get('archetype', 'Unknown')
            sql_evidence = stage2.get('sql_evidence', [])
            
            if archetype not in archetype_counts:
                archetype_counts[archetype] = 0
            archetype_counts[archetype] += len(sql_evidence)
            total_sql_evidence += len(sql_evidence)
    
    # Build archetype table
    archetype_table = []
    for archetype, count in archetype_counts.items():
        status = "Evidence Found" if count > 0 else "No Evidence"
        archetype_table.append({
            'Archetype': archetype.replace('_', ' '),
            'SQL Records': count,
            'Status': status
        })
    
    if archetype_table:
        df_archetypes = pd.DataFrame(archetype_table)
        st.dataframe(df_archetypes, use_container_width=True, hide_index=True)
    
    st.markdown(f"**Total SQL Evidence:** {total_sql_evidence} records")
    st.markdown("")
    
    # Correction Recommendations Table
    st.markdown("### Correction Recommendations")
    
    recommendations_data = []
    correction_count = 0
    
    # Debug: Check what's in the data
    debug_info = []
    
    for issue in enriched_issues:
        stage2 = issue.get('stage2_archetype_correction_analysis')
        if stage2:
            correction_analysis = stage2.get('correction_analysis', {})
            debug_info.append(f"Type: {type(correction_analysis)}, Keys: {list(correction_analysis.keys()) if isinstance(correction_analysis, dict) else 'N/A'}")
            
            if isinstance(correction_analysis, dict) and 'recommended_corrections' in correction_analysis:
                recommendations = correction_analysis.get('recommended_corrections', [])
                if recommendations:
                    correction_count += len(recommendations)
                    
                    for rec in recommendations:  # Show all recommendations
                        suggestion = rec.get('suggestion', 'No suggestion')
                        confidence = rec.get('confidence', 0)
                        field = rec.get('field', 'Unknown field')
                        implementation = rec.get('implementation_guidance', 'N/A')
                        
                        recommendations_data.append({
                            'Field': field,
                            'Suggestion': suggestion,  # Full text
                            'Confidence': f"{confidence:.0%}",
                            'Implementation': implementation  # Full text
                        })
    
    # Show debug info if no recommendations found
    if not recommendations_data and debug_info:
        with st.expander("Debug: Why no recommendations?"):
            for info in debug_info:
                st.text(info)
    
    if recommendations_data:
        df_recommendations = pd.DataFrame(recommendations_data)
        st.dataframe(df_recommendations, use_container_width=True, hide_index=True, height=None)
        st.markdown(f"**Total Recommendations:** {correction_count}")
    else:
        st.markdown("No specific correction recommendations generated")
        st.markdown("System found SQL evidence but correction reasoning needs improvement")
    
    st.markdown("")
    
    # System Status Card
    st.markdown("### System Status")
    if total_sql_evidence > 0:
        st.success("OPERATIONAL - System successfully connected to CMS policy database")
    else:
        st.error("NEEDS ATTENTION - No SQL evidence found, check database connection")
    
    st.markdown("---")
    
    # Raw data in expander
    with st.expander("View Raw Data"):
        st.json(data)

# ----------------------------
# UI
# ----------------------------

def main():
    # Header at top, left-justified, higher position
    st.markdown("""
    <style>
    /* Custom header styling */
    .custom-header {
        background: white;
        border-bottom: 1px solid #e1e4e8;
        padding: 14px 0;
        margin-bottom: 24px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    .custom-header h1 {
        font-size: 18px !important;
        font-weight: 600 !important;
        color: #111827 !important;
        margin: 0 0 4px 0 !important;
        text-align: left !important;
    }
    .custom-header p {
        font-size: 13px !important;
        color: #6b7280 !important;
        margin: 0 !important;
        text-align: left !important;
    }
    </style>
    <div class='custom-header'>
        <h1>CXR Exchange  Powered by the Claims Reasoning Kernel</h1>
        <p>A unified platform for automated claim validation, correction, and compliance.</p>
    </div>
    """, unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("""
        <div style='margin-bottom:16px;'>
            <div style='font-weight:600; margin-bottom:6px;'>Instructions</div>
            <div style='font-size:13px; line-height:1.4; margin:0;'>
                1. Select data source<br>
                2. Choose analysis mode<br>
                3. Load/preview data<br>
                4. Run analysis<br>
                5. Review results
            </div>
        </div>
        <hr style='margin:12px 0; border:none; border-top:1px solid #e1e4e8;'>
        """, unsafe_allow_html=True)
        
        st.markdown("<div class='block-title'>Data Source</div>", unsafe_allow_html=True)
        source = st.radio(
            "Select source",
            [
                "Upload JSON",
                "Upload FHIR (JSON)",
            ],
            index=0,
            label_visibility="collapsed",
        )

        st.markdown("<div class='block-title'>Analysis Mode</div>", unsafe_allow_html=True)
        mode = st.selectbox(
            "Analysis mode", 
            [
                "Basic Analysis",
                "Two-Stage Pipeline",
                "Calibrated Analysis",
                "Two-Stage Calibrated",
                "Archetype-Driven (v2)",
                "Archetype-Driven (v3)"
            ], 
            index=4, 
            label_visibility="collapsed",
            key="analysis_mode"
        )
        
        # File uploads in sidebar
        if source == "Upload JSON":
            st.markdown("<div class='block-title'>Upload Claim JSON</div>", unsafe_allow_html=True)
            f = st.file_uploader("Upload claim JSON file", type=["json"], label_visibility="collapsed")
        elif source == "Upload FHIR (JSON)":
            st.markdown("<div class='block-title'>Upload FHIR Claim (R4) JSON</div>", unsafe_allow_html=True)
            fhirf = st.file_uploader("Upload FHIR Claim JSON file", type=["json"], label_visibility="collapsed")

    claim: Optional[Dict[str,Any]] = None

    if source == "Upload JSON":
        if 'f' in locals() and f:
            try:
                claim = json.load(f)
            except Exception as e:
                st.error(f"Invalid JSON: {e}")
    elif source == "Upload FHIR (JSON)":
        if 'fhirf' in locals() and fhirf:
            try:
                fhir_obj = json.load(fhirf)
                if not _validate_fhir(fhir_obj):
                    st.error("Not a valid FHIR Claim resource or missing diagnosis/procedure content.")
                else:
                    claim = _convert_fhir(fhir_obj)
            except Exception as e:
                st.error(f"Invalid FHIR JSON: {e}")

    # Display selected claim in main panel
    if claim:
        show_compact_claim(claim)
        st.markdown("")
        
        st.markdown("<div class='block-title'>Run Analysis</div>", unsafe_allow_html=True)
        run = st.button("Run", use_container_width=False)
        if run:
            # Show progress indicator
            with st.spinner(f"Running {mode}..."):
                try:
                    if mode == "Basic Analysis":
                        out = run_basic(claim)
                    elif mode == "Two-Stage Pipeline":
                        out = run_pipeline(claim)
                    elif mode == "Calibrated Analysis":
                        out = run_calibrated(claim['CLM_ID'])
                    elif mode == "Two-Stage Calibrated":
                        out = run_two_stage_calibrated(claim['CLM_ID'])
                    elif mode == "Archetype-Driven (v2)":
                        # Archetype-Driven (v2): ensure Stage 1 issues exist
                        try:
                            _ = run_basic(claim)  # populates Qdrant with DX-PROC issues
                        except Exception:
                            pass
                        out = run_archetype_v2(claim['CLM_ID'])
                    else:
                        # Archetype-Driven (v3): ensure Stage 1 issues exist
                        try:
                            _ = run_basic(claim)  # populates Qdrant with DX-PROC issues
                        except Exception:
                            pass
                        out = run_archetype_v3(claim['CLM_ID'])
                    
                    st.success("Analysis complete")
                    
                    # Show simple summary instead of raw JSON
                    if mode == "Archetype-Driven (v3)" and isinstance(out, dict):
                        try:
                            show_v3_summary(out)
                        except Exception as e:
                            st.error(f"Error displaying summary: {e}")
                            import traceback
                            st.text(traceback.format_exc())
                            st.json(out)  # Fallback to JSON
                    else:
                        st.json(out)
                except Exception as e:
                    st.error(f"Analysis failed: {e}")

if __name__ == "__main__":
    main()

CMS Claim Analysis Streamlit App (v2 Minimal)
=============================================
- Minimalist, uniform typography
- Load CMS inpatient/outpatient claims from SQL Server (`the_claims`)
- Run full analysis flows including Archetype-Driven v2
"""

import streamlit as st
import json
import pandas as pd
import pyodbc
import sys
import os
from datetime import datetime
from typing import Dict, Any, Optional

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from new_claim_analyzer1 import NewClaimAnalyzer
from claim_corrector_claims3_archetype_driven_v2 import ArchetypeDrivenClaimCorrectorV2
from claim_corrector_claims3_archetype_driven_v3 import ArchetypeDrivenClaimCorrectorV3
from claim_corrector_claims3_two_stage_calibrated import TwoStageCalibratedClaimCorrector
from claim_corrector_claims3_calibrated import CalibratedClaimCorrector
from claim_corrector_claims import ClaimCorrector
from fhir_adapter import validate_fhir_claim as _validate_fhir, convert_fhir_claim as _convert_fhir

# ----------------------------
# Page + Minimal Styles
# ----------------------------
st.set_page_config(page_title="CMS Claim Analysis v2", page_icon="", layout="wide")

st.markdown(
"""
<style>
:root { 
    --base-font: 14px; 
    --muted: #6b7280; 
    --text: #111827;
    --border: #e5e7eb;
}
html, body, [class*="css"] { 
    font-size: var(--base-font); 
    font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; 
    color: var(--text);
    line-height: 1.5;
}
h1, h2, h3, h4, h5, h6 { 
    font-size: var(--base-font) !important; 
    font-weight: 600 !important; 
    margin: 1rem 0 0.5rem 0 !important;
    color: var(--text) !important;
}
p, div, span, li { 
    font-size: var(--base-font) !important; 
    color: var(--text) !important;
    margin: 0.5rem 0 !important;
}
.block-title { 
    font-size: var(--base-font) !important; 
    font-weight: 600 !important; 
    margin: 1rem 0 0.5rem 0 !important; 
    color: var(--text) !important;
}
.small { 
    color: var(--muted); 
    font-size: var(--base-font) !important; 
}
.card { 
    border: 1px solid var(--border); 
    border-radius: 6px; 
    padding: 16px; 
    margin: 8px 0;
}
.btn-primary { 
    background: var(--text); 
    color: white; 
    padding: 8px 16px; 
    border-radius: 4px; 
    text-decoration: none; 
    font-size: var(--base-font) !important;
}
.metric { 
    font-size: var(--base-font) !important; 
    color: var(--text) !important;
}
.metric strong { 
    font-size: var(--base-font) !important; 
    color: var(--text) !important;
    font-weight: 600 !important;
}
/* Clean separators */
hr {
    border: none;
    border-top: 1px solid var(--border);
    margin: 1.5rem 0;
}
/* Uniform bullet points */
ul {
    margin-left: 20px;
    padding-left: 0;
}
li {
    font-size: var(--base-font) !important;
    margin: 0.25rem 0;
    list-style-type: disc;
}
/* Compact radio buttons - force single spacing */
.stRadio > div {
    gap: 0px !important;
    padding: 0 !important;
}
.stRadio > div > label {
    margin: 0 !important;
    padding: 2px 0 !important;
    gap: 6px !important;
}
.stRadio > div > label > div {
    padding: 0 !important;
    line-height: 1.2 !important;
}
.stRadio > div > label > div > div {
    margin: 0 !important;
    padding: 0 !important;
}
/* Analysis mode dropdown - visible selector, compact menu */
.stSelectbox > div > div {
    min-height: 42px !important;
}
.stSelectbox [data-baseweb="select"] > div {
    padding: 10px 14px !important;
    line-height: 1.5 !important;
    min-height: 42px !important;
    font-size: 14px !important;
}
.stSelectbox [data-baseweb="select"] {
    min-height: 42px !important;
}
/* Dropdown menu options - single spacing */
[data-baseweb="menu"] {
    padding: 4px !important;
}
[data-baseweb="menu"] li {
    padding: 6px 12px !important;
    margin: 0 !important;
    line-height: 1.3 !important;
}
[role="option"] {
    margin: 0 !important;
    padding: 6px 12px !important;
    line-height: 1.3 !important;
}
/* Fix JSON display - single spacing and compact */
pre {
    line-height: 1.1 !important;
    margin: 0 !important;
    padding: 6px 8px !important;
    font-size: 12px !important;
    background-color: #f8f9fa !important;
    border-radius: 4px !important;
}
code {
    line-height: 1.1 !important;
    font-size: 12px !important;
}
/* Streamlit JSON component - force compact */
.stJson {
    line-height: 1.1 !important;
}
.stJson pre {
    line-height: 1.1 !important;
    margin: 0 !important;
    padding: 6px !important;
}
/* Expander content - compact */
.streamlit-expanderContent {
    padding: 4px 8px !important;
}
/* Dataframe text wrapping - show full text */
.stDataFrame td, .stDataFrame th {
    white-space: normal !important;
    word-wrap: break-word !important;
    max-width: none !important;
}
.stDataFrame [data-testid="stDataFrameResizable"] {
    overflow-x: auto !important;
}
</style>
""",
unsafe_allow_html=True,
)

# ----------------------------
# DB Access for CMS Claims
# ----------------------------

def get_sql_conn():
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=localhost,1433;Database=_claims;"
        "UID=SA;PWD=Bbanwo@1980!;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)

@st.cache_data(show_spinner=False)
def list_tables() -> Dict[str, str]:
    try:
        with get_sql_conn() as conn:
            df = pd.read_sql("SELECT name FROM sys.tables ORDER BY name", conn)
        names = df['name'].str.lower().tolist()
        candidates = {
            'inpatient': next((n for n in names if 'inpatient' in n or 'ip' in n), None),
            'outpatient': next((n for n in names if 'outpatient' in n or 'op' in n), None),
        }
        return candidates
    except Exception as e:
        st.error(f"DB table discovery failed: {e}")
        return {'inpatient': None, 'outpatient': None}

@st.cache_data(show_spinner=False)
def load_claims(table: str, limit: int = 2000) -> pd.DataFrame:
    with get_sql_conn() as conn:
        q = f"SELECT TOP {limit} * FROM [{table}]"
        return pd.read_sql(q, conn)

# ----------------------------
# Normalization to App Schema
# ----------------------------

def normalize_row_to_claim(row: pd.Series) -> Dict[str, Any]:
    def pick(*keys):
        for k in keys:
            if k in row and pd.notna(row[k]):
                return str(row[k]).strip()
        return None

    clm_id = pick('CLM_ID','claim_id','clm_id','claimid') or f"cms-{row.name}"
    desyn = pick('DESYNPUF_ID','bene_id','patient_id') or "unknown-patient"
    from_dt = pick('CLM_FROM_DT','service_date','from_date','srvc_from_dt') or ""
    thru_dt = pick('CLM_THRU_DT','thru_date','srvc_thru_dt') or from_dt
    prvdr = pick('PRVDR_NUM','provider_id','npi','tin') or "unknown-provider"

    # Collect ICD-9/10 columns heuristically
    dx_map: Dict[str,str] = {}
    dx_cols = [c for c in row.index if str(c).lower().startswith(('icd9','icd10','dx','diag'))]
    pos = 1
    for c in dx_cols:
        val = row[c]
        if pd.notna(val) and str(val).strip():
            dx_map[f"ICD9_DGNS_CD_{pos}"] = str(val).strip()
            pos += 1
            if pos > 10:
                break

    # Collect HCPCS/CPT columns heuristically
    proc_map: Dict[str,str] = {}
    hcpcs_cols = [c for c in row.index if str(c).lower().startswith(('hcpcs','cpt','proc'))]
    ppos = 1
    for c in hcpcs_cols:
        val = row[c]
        if pd.notna(val) and str(val).strip():
            proc_map[f"HCPCS_CD_{ppos}"] = str(val).strip()
            ppos += 1
            if ppos > 45:
                break

    return {
        "CLM_ID": clm_id,
        "DESYNPUF_ID": desyn,
        "CLM_FROM_DT": from_dt,
        "CLM_THRU_DT": thru_dt,
        "PRVDR_NUM": prvdr,
        "diagnosis_codes": dx_map or {"ICD9_DGNS_CD_1": pick('ICD9_DGNS_CD_1','ICD10_DGNS_CD_1','DX1') or ""},
        "procedure_codes": proc_map or {"HCPCS_CD_1": pick('HCPCS_CD_1','CPT1','PROC1') or ""},
    }

# ----------------------------
# Analysis Runners
# ----------------------------

def run_basic(claim: Dict[str,Any]):
    analyzer = NewClaimAnalyzer()
    return analyzer.analyze_new_claim(claim)

def run_pipeline(claim: Dict[str,Any]):
    analyzer = NewClaimAnalyzer()
    diagnostics = analyzer.analyze_new_claim(claim)
    corrector = ClaimCorrector()
    enriched = corrector.run_corrections(diagnostics.get('claim_summary',{}).get('CLM_ID', claim['CLM_ID']))
    return {"stage1": diagnostics, "stage2": enriched}

def run_calibrated(claim_id: str):
    return CalibratedClaimCorrector().run_corrections(claim_id)

def run_two_stage_calibrated(claim_id: str):
    return TwoStageCalibratedClaimCorrector().run_two_stage_corrections(claim_id)

def run_archetype_v2(claim_id: str):
    return ArchetypeDrivenClaimCorrectorV2().run_archetype_driven_corrections(claim_id)

def run_archetype_v3(claim_id: str):
    return ArchetypeDrivenClaimCorrectorV3().run_archetype_driven_corrections(claim_id)

def show_compact_claim(claim: Dict[str, Any]):
    """Display claim in compact JSON format with custom CSS."""
    
    # Create compact JSON string
    json_str = json.dumps(claim, indent=2)
    
    # Display with custom styling
    st.markdown(f"""
    <div style='background:#f8f9fa; border:1px solid #e1e4e8; border-radius:6px; padding:10px 14px; font-family:monospace; font-size:12px; line-height:1.5; overflow-x:auto;'>
        <pre style='margin:0; padding:0; line-height:1.5; font-size:12px;'><code>{json_str}</code></pre>
    </div>
    """, unsafe_allow_html=True)

def show_v3_summary(data):
    """Show a clean, minimalist summary of v3 results with structured tables."""
    
    claim_id = data.get('claim_id', 'Unknown')
    total_issues = data.get('total_issues', 0)
    enriched_issues = data.get('enriched_issues', [])
    
    # Header Card
    st.markdown("---")
    st.markdown("### Analysis Summary")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**Claim ID:** {claim_id}")
    with col2:
        st.markdown(f"**Total Issues:** {total_issues}")
    st.markdown("---")
    
    # Stage 1: Issues Analysis Table
    st.markdown("### Stage 1: Issues Analysis")
    
    if enriched_issues:
        # Build table data
        table_data = []
        for idx, issue in enumerate(enriched_issues[:8], 1):  # Show top 8
            dx_code = issue.get('icd10_code', issue.get('icd9_code', 'Unknown'))
            dx_name = issue.get('diagnosis_name', '')
            proc_code = issue.get('hcpcs_code', 'Unknown')
            proc_name = issue.get('procedure_name', '')
            risk_level = issue.get('denial_risk_level', 'Unknown').replace('HIGH: ', '')
            risk_score = issue.get('denial_risk_score', 0)
            action = issue.get('action_required', 'Unknown').replace('IMMEDIATE: ', '').replace('REVIEW: ', '').replace('NO ACTION: ', '')
            
            # Format DX and PROC with names
            dx_display = f"DX {idx}: {dx_code}"
            if dx_name:
                dx_display += f" ({dx_name})"
            
            proc_display = f"Procedure {idx}: {proc_code}"
            if proc_name:
                proc_display += f" ({proc_name})"
            
            table_data.append({
                'Diagnosis': dx_display,
                'Procedure': proc_display,
                'Risk': risk_level,
                'Score': f"{risk_score:.0f}",
                'Action': action
            })
        
        if table_data:
            df_issues = pd.DataFrame(table_data)
            st.dataframe(df_issues, use_container_width=True, hide_index=True)
    
    st.markdown("")
    
    # Stage 2: Archetype Analysis Table
    st.markdown("### Stage 2: Archetype Analysis")
    
    # Count SQL evidence
    total_sql_evidence = 0
    archetype_counts = {}
    
    for issue in enriched_issues:
        stage2 = issue.get('stage2_archetype_correction_analysis')
        if stage2:
            archetype = stage2.get('archetype', 'Unknown')
            sql_evidence = stage2.get('sql_evidence', [])
            
            if archetype not in archetype_counts:
                archetype_counts[archetype] = 0
            archetype_counts[archetype] += len(sql_evidence)
            total_sql_evidence += len(sql_evidence)
    
    # Build archetype table
    archetype_table = []
    for archetype, count in archetype_counts.items():
        status = "Evidence Found" if count > 0 else "No Evidence"
        archetype_table.append({
            'Archetype': archetype.replace('_', ' '),
            'SQL Records': count,
            'Status': status
        })
    
    if archetype_table:
        df_archetypes = pd.DataFrame(archetype_table)
        st.dataframe(df_archetypes, use_container_width=True, hide_index=True)
    
    st.markdown(f"**Total SQL Evidence:** {total_sql_evidence} records")
    st.markdown("")
    
    # Correction Recommendations Table
    st.markdown("### Correction Recommendations")
    
    recommendations_data = []
    correction_count = 0
    
    # Debug: Check what's in the data
    debug_info = []
    
    for issue in enriched_issues:
        stage2 = issue.get('stage2_archetype_correction_analysis')
        if stage2:
            correction_analysis = stage2.get('correction_analysis', {})
            debug_info.append(f"Type: {type(correction_analysis)}, Keys: {list(correction_analysis.keys()) if isinstance(correction_analysis, dict) else 'N/A'}")
            
            if isinstance(correction_analysis, dict) and 'recommended_corrections' in correction_analysis:
                recommendations = correction_analysis.get('recommended_corrections', [])
                if recommendations:
                    correction_count += len(recommendations)
                    
                    for rec in recommendations:  # Show all recommendations
                        suggestion = rec.get('suggestion', 'No suggestion')
                        confidence = rec.get('confidence', 0)
                        field = rec.get('field', 'Unknown field')
                        implementation = rec.get('implementation_guidance', 'N/A')
                        
                        recommendations_data.append({
                            'Field': field,
                            'Suggestion': suggestion,  # Full text
                            'Confidence': f"{confidence:.0%}",
                            'Implementation': implementation  # Full text
                        })
    
    # Show debug info if no recommendations found
    if not recommendations_data and debug_info:
        with st.expander("Debug: Why no recommendations?"):
            for info in debug_info:
                st.text(info)
    
    if recommendations_data:
        df_recommendations = pd.DataFrame(recommendations_data)
        st.dataframe(df_recommendations, use_container_width=True, hide_index=True, height=None)
        st.markdown(f"**Total Recommendations:** {correction_count}")
    else:
        st.markdown("No specific correction recommendations generated")
        st.markdown("System found SQL evidence but correction reasoning needs improvement")
    
    st.markdown("")
    
    # System Status Card
    st.markdown("### System Status")
    if total_sql_evidence > 0:
        st.success("OPERATIONAL - System successfully connected to CMS policy database")
    else:
        st.error("NEEDS ATTENTION - No SQL evidence found, check database connection")
    
    st.markdown("---")
    
    # Raw data in expander
    with st.expander("View Raw Data"):
        st.json(data)

# ----------------------------
# UI
# ----------------------------

def main():
    # Header at top, left-justified, higher position
    st.markdown("""
    <style>
    /* Custom header styling */
    .custom-header {
        background: white;
        border-bottom: 1px solid #e1e4e8;
        padding: 14px 0;
        margin-bottom: 24px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    .custom-header h1 {
        font-size: 18px !important;
        font-weight: 600 !important;
        color: #111827 !important;
        margin: 0 0 4px 0 !important;
        text-align: left !important;
    }
    .custom-header p {
        font-size: 13px !important;
        color: #6b7280 !important;
        margin: 0 !important;
        text-align: left !important;
    }
    </style>
    <div class='custom-header'>
        <h1>CXR Exchange  Powered by the Claims Reasoning Kernel</h1>
        <p>A unified platform for automated claim validation, correction, and compliance.</p>
    </div>
    """, unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("""
        <div style='margin-bottom:16px;'>
            <div style='font-weight:600; margin-bottom:6px;'>Instructions</div>
            <div style='font-size:13px; line-height:1.4; margin:0;'>
                1. Select data source<br>
                2. Choose analysis mode<br>
                3. Load/preview data<br>
                4. Run analysis<br>
                5. Review results
            </div>
        </div>
        <hr style='margin:12px 0; border:none; border-top:1px solid #e1e4e8;'>
        """, unsafe_allow_html=True)
        
        st.markdown("<div class='block-title'>Data Source</div>", unsafe_allow_html=True)
        source = st.radio(
            "Select source",
            [
                "Upload JSON",
                "Upload FHIR (JSON)",
            ],
            index=0,
            label_visibility="collapsed",
        )

        st.markdown("<div class='block-title'>Analysis Mode</div>", unsafe_allow_html=True)
        mode = st.selectbox(
            "Analysis mode", 
            [
                "Basic Analysis",
                "Two-Stage Pipeline",
                "Calibrated Analysis",
                "Two-Stage Calibrated",
                "Archetype-Driven (v2)",
                "Archetype-Driven (v3)"
            ], 
            index=4, 
            label_visibility="collapsed",
            key="analysis_mode"
        )
        
        # File uploads in sidebar
        if source == "Upload JSON":
            st.markdown("<div class='block-title'>Upload Claim JSON</div>", unsafe_allow_html=True)
            f = st.file_uploader("Upload claim JSON file", type=["json"], label_visibility="collapsed")
        elif source == "Upload FHIR (JSON)":
            st.markdown("<div class='block-title'>Upload FHIR Claim (R4) JSON</div>", unsafe_allow_html=True)
            fhirf = st.file_uploader("Upload FHIR Claim JSON file", type=["json"], label_visibility="collapsed")

    claim: Optional[Dict[str,Any]] = None

    if source == "Upload JSON":
        if 'f' in locals() and f:
            try:
                claim = json.load(f)
            except Exception as e:
                st.error(f"Invalid JSON: {e}")
    elif source == "Upload FHIR (JSON)":
        if 'fhirf' in locals() and fhirf:
            try:
                fhir_obj = json.load(fhirf)
                if not _validate_fhir(fhir_obj):
                    st.error("Not a valid FHIR Claim resource or missing diagnosis/procedure content.")
                else:
                    claim = _convert_fhir(fhir_obj)
            except Exception as e:
                st.error(f"Invalid FHIR JSON: {e}")

    # Display selected claim in main panel
    if claim:
        show_compact_claim(claim)
        st.markdown("")
        
        st.markdown("<div class='block-title'>Run Analysis</div>", unsafe_allow_html=True)
        run = st.button("Run", use_container_width=False)
        if run:
            # Show progress indicator
            with st.spinner(f"Running {mode}..."):
                try:
                    if mode == "Basic Analysis":
                        out = run_basic(claim)
                    elif mode == "Two-Stage Pipeline":
                        out = run_pipeline(claim)
                    elif mode == "Calibrated Analysis":
                        out = run_calibrated(claim['CLM_ID'])
                    elif mode == "Two-Stage Calibrated":
                        out = run_two_stage_calibrated(claim['CLM_ID'])
                    elif mode == "Archetype-Driven (v2)":
                        # Archetype-Driven (v2): ensure Stage 1 issues exist
                        try:
                            _ = run_basic(claim)  # populates Qdrant with DX-PROC issues
                        except Exception:
                            pass
                        out = run_archetype_v2(claim['CLM_ID'])
                    else:
                        # Archetype-Driven (v3): ensure Stage 1 issues exist
                        try:
                            _ = run_basic(claim)  # populates Qdrant with DX-PROC issues
                        except Exception:
                            pass
                        out = run_archetype_v3(claim['CLM_ID'])
                    
                    st.success("Analysis complete")
                    
                    # Show simple summary instead of raw JSON
                    if mode == "Archetype-Driven (v3)" and isinstance(out, dict):
                        try:
                            show_v3_summary(out)
                        except Exception as e:
                            st.error(f"Error displaying summary: {e}")
                            import traceback
                            st.text(traceback.format_exc())
                            st.json(out)  # Fallback to JSON
                    else:
                        st.json(out)
                except Exception as e:
                    st.error(f"Analysis failed: {e}")

if __name__ == "__main__":
    main()



CMS Claim Analysis Streamlit App (v2 Minimal)
=============================================
- Minimalist, uniform typography
- Load CMS inpatient/outpatient claims from SQL Server (`the_claims`)
- Run full analysis flows including Archetype-Driven v2
"""

import streamlit as st
import json
import pandas as pd
import pyodbc
import sys
import os
from datetime import datetime
from typing import Dict, Any, Optional

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from new_claim_analyzer1 import NewClaimAnalyzer
from claim_corrector_claims3_archetype_driven_v2 import ArchetypeDrivenClaimCorrectorV2
from claim_corrector_claims3_archetype_driven_v3 import ArchetypeDrivenClaimCorrectorV3
from claim_corrector_claims3_two_stage_calibrated import TwoStageCalibratedClaimCorrector
from claim_corrector_claims3_calibrated import CalibratedClaimCorrector
from claim_corrector_claims import ClaimCorrector
from fhir_adapter import validate_fhir_claim as _validate_fhir, convert_fhir_claim as _convert_fhir

# ----------------------------
# Page + Minimal Styles
# ----------------------------
st.set_page_config(page_title="CMS Claim Analysis v2", page_icon="", layout="wide")

st.markdown(
"""
<style>
:root { 
    --base-font: 14px; 
    --muted: #6b7280; 
    --text: #111827;
    --border: #e5e7eb;
}
html, body, [class*="css"] { 
    font-size: var(--base-font); 
    font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; 
    color: var(--text);
    line-height: 1.5;
}
h1, h2, h3, h4, h5, h6 { 
    font-size: var(--base-font) !important; 
    font-weight: 600 !important; 
    margin: 1rem 0 0.5rem 0 !important;
    color: var(--text) !important;
}
p, div, span, li { 
    font-size: var(--base-font) !important; 
    color: var(--text) !important;
    margin: 0.5rem 0 !important;
}
.block-title { 
    font-size: var(--base-font) !important; 
    font-weight: 600 !important; 
    margin: 1rem 0 0.5rem 0 !important; 
    color: var(--text) !important;
}
.small { 
    color: var(--muted); 
    font-size: var(--base-font) !important; 
}
.card { 
    border: 1px solid var(--border); 
    border-radius: 6px; 
    padding: 16px; 
    margin: 8px 0;
}
.btn-primary { 
    background: var(--text); 
    color: white; 
    padding: 8px 16px; 
    border-radius: 4px; 
    text-decoration: none; 
    font-size: var(--base-font) !important;
}
.metric { 
    font-size: var(--base-font) !important; 
    color: var(--text) !important;
}
.metric strong { 
    font-size: var(--base-font) !important; 
    color: var(--text) !important;
    font-weight: 600 !important;
}
/* Clean separators */
hr {
    border: none;
    border-top: 1px solid var(--border);
    margin: 1.5rem 0;
}
/* Uniform bullet points */
ul {
    margin-left: 20px;
    padding-left: 0;
}
li {
    font-size: var(--base-font) !important;
    margin: 0.25rem 0;
    list-style-type: disc;
}
/* Compact radio buttons - force single spacing */
.stRadio > div {
    gap: 0px !important;
    padding: 0 !important;
}
.stRadio > div > label {
    margin: 0 !important;
    padding: 2px 0 !important;
    gap: 6px !important;
}
.stRadio > div > label > div {
    padding: 0 !important;
    line-height: 1.2 !important;
}
.stRadio > div > label > div > div {
    margin: 0 !important;
    padding: 0 !important;
}
/* Analysis mode dropdown - visible selector, compact menu */
.stSelectbox > div > div {
    min-height: 42px !important;
}
.stSelectbox [data-baseweb="select"] > div {
    padding: 10px 14px !important;
    line-height: 1.5 !important;
    min-height: 42px !important;
    font-size: 14px !important;
}
.stSelectbox [data-baseweb="select"] {
    min-height: 42px !important;
}
/* Dropdown menu options - single spacing */
[data-baseweb="menu"] {
    padding: 4px !important;
}
[data-baseweb="menu"] li {
    padding: 6px 12px !important;
    margin: 0 !important;
    line-height: 1.3 !important;
}
[role="option"] {
    margin: 0 !important;
    padding: 6px 12px !important;
    line-height: 1.3 !important;
}
/* Fix JSON display - single spacing and compact */
pre {
    line-height: 1.1 !important;
    margin: 0 !important;
    padding: 6px 8px !important;
    font-size: 12px !important;
    background-color: #f8f9fa !important;
    border-radius: 4px !important;
}
code {
    line-height: 1.1 !important;
    font-size: 12px !important;
}
/* Streamlit JSON component - force compact */
.stJson {
    line-height: 1.1 !important;
}
.stJson pre {
    line-height: 1.1 !important;
    margin: 0 !important;
    padding: 6px !important;
}
/* Expander content - compact */
.streamlit-expanderContent {
    padding: 4px 8px !important;
}
/* Dataframe text wrapping - show full text */
.stDataFrame td, .stDataFrame th {
    white-space: normal !important;
    word-wrap: break-word !important;
    max-width: none !important;
}
.stDataFrame [data-testid="stDataFrameResizable"] {
    overflow-x: auto !important;
}
</style>
""",
unsafe_allow_html=True,
)

# ----------------------------
# DB Access for CMS Claims
# ----------------------------

def get_sql_conn():
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=localhost,1433;Database=_claims;"
        "UID=SA;PWD=Bbanwo@1980!;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)

@st.cache_data(show_spinner=False)
def list_tables() -> Dict[str, str]:
    try:
        with get_sql_conn() as conn:
            df = pd.read_sql("SELECT name FROM sys.tables ORDER BY name", conn)
        names = df['name'].str.lower().tolist()
        candidates = {
            'inpatient': next((n for n in names if 'inpatient' in n or 'ip' in n), None),
            'outpatient': next((n for n in names if 'outpatient' in n or 'op' in n), None),
        }
        return candidates
    except Exception as e:
        st.error(f"DB table discovery failed: {e}")
        return {'inpatient': None, 'outpatient': None}

@st.cache_data(show_spinner=False)
def load_claims(table: str, limit: int = 2000) -> pd.DataFrame:
    with get_sql_conn() as conn:
        q = f"SELECT TOP {limit} * FROM [{table}]"
        return pd.read_sql(q, conn)

# ----------------------------
# Normalization to App Schema
# ----------------------------

def normalize_row_to_claim(row: pd.Series) -> Dict[str, Any]:
    def pick(*keys):
        for k in keys:
            if k in row and pd.notna(row[k]):
                return str(row[k]).strip()
        return None

    clm_id = pick('CLM_ID','claim_id','clm_id','claimid') or f"cms-{row.name}"
    desyn = pick('DESYNPUF_ID','bene_id','patient_id') or "unknown-patient"
    from_dt = pick('CLM_FROM_DT','service_date','from_date','srvc_from_dt') or ""
    thru_dt = pick('CLM_THRU_DT','thru_date','srvc_thru_dt') or from_dt
    prvdr = pick('PRVDR_NUM','provider_id','npi','tin') or "unknown-provider"

    # Collect ICD-9/10 columns heuristically
    dx_map: Dict[str,str] = {}
    dx_cols = [c for c in row.index if str(c).lower().startswith(('icd9','icd10','dx','diag'))]
    pos = 1
    for c in dx_cols:
        val = row[c]
        if pd.notna(val) and str(val).strip():
            dx_map[f"ICD9_DGNS_CD_{pos}"] = str(val).strip()
            pos += 1
            if pos > 10:
                break

    # Collect HCPCS/CPT columns heuristically
    proc_map: Dict[str,str] = {}
    hcpcs_cols = [c for c in row.index if str(c).lower().startswith(('hcpcs','cpt','proc'))]
    ppos = 1
    for c in hcpcs_cols:
        val = row[c]
        if pd.notna(val) and str(val).strip():
            proc_map[f"HCPCS_CD_{ppos}"] = str(val).strip()
            ppos += 1
            if ppos > 45:
                break

    return {
        "CLM_ID": clm_id,
        "DESYNPUF_ID": desyn,
        "CLM_FROM_DT": from_dt,
        "CLM_THRU_DT": thru_dt,
        "PRVDR_NUM": prvdr,
        "diagnosis_codes": dx_map or {"ICD9_DGNS_CD_1": pick('ICD9_DGNS_CD_1','ICD10_DGNS_CD_1','DX1') or ""},
        "procedure_codes": proc_map or {"HCPCS_CD_1": pick('HCPCS_CD_1','CPT1','PROC1') or ""},
    }

# ----------------------------
# Analysis Runners
# ----------------------------

def run_basic(claim: Dict[str,Any]):
    analyzer = NewClaimAnalyzer()
    return analyzer.analyze_new_claim(claim)

def run_pipeline(claim: Dict[str,Any]):
    analyzer = NewClaimAnalyzer()
    diagnostics = analyzer.analyze_new_claim(claim)
    corrector = ClaimCorrector()
    enriched = corrector.run_corrections(diagnostics.get('claim_summary',{}).get('CLM_ID', claim['CLM_ID']))
    return {"stage1": diagnostics, "stage2": enriched}

def run_calibrated(claim_id: str):
    return CalibratedClaimCorrector().run_corrections(claim_id)

def run_two_stage_calibrated(claim_id: str):
    return TwoStageCalibratedClaimCorrector().run_two_stage_corrections(claim_id)

def run_archetype_v2(claim_id: str):
    return ArchetypeDrivenClaimCorrectorV2().run_archetype_driven_corrections(claim_id)

def run_archetype_v3(claim_id: str):
    return ArchetypeDrivenClaimCorrectorV3().run_archetype_driven_corrections(claim_id)

def show_compact_claim(claim: Dict[str, Any]):
    """Display claim in compact JSON format with custom CSS."""
    
    # Create compact JSON string
    json_str = json.dumps(claim, indent=2)
    
    # Display with custom styling
    st.markdown(f"""
    <div style='background:#f8f9fa; border:1px solid #e1e4e8; border-radius:6px; padding:10px 14px; font-family:monospace; font-size:12px; line-height:1.5; overflow-x:auto;'>
        <pre style='margin:0; padding:0; line-height:1.5; font-size:12px;'><code>{json_str}</code></pre>
    </div>
    """, unsafe_allow_html=True)

def show_v3_summary(data):
    """Show a clean, minimalist summary of v3 results with structured tables."""
    
    claim_id = data.get('claim_id', 'Unknown')
    total_issues = data.get('total_issues', 0)
    enriched_issues = data.get('enriched_issues', [])
    
    # Header Card
    st.markdown("---")
    st.markdown("### Analysis Summary")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**Claim ID:** {claim_id}")
    with col2:
        st.markdown(f"**Total Issues:** {total_issues}")
    st.markdown("---")
    
    # Stage 1: Issues Analysis Table
    st.markdown("### Stage 1: Issues Analysis")
    
    if enriched_issues:
        # Build table data
        table_data = []
        for idx, issue in enumerate(enriched_issues[:8], 1):  # Show top 8
            dx_code = issue.get('icd10_code', issue.get('icd9_code', 'Unknown'))
            dx_name = issue.get('diagnosis_name', '')
            proc_code = issue.get('hcpcs_code', 'Unknown')
            proc_name = issue.get('procedure_name', '')
            risk_level = issue.get('denial_risk_level', 'Unknown').replace('HIGH: ', '')
            risk_score = issue.get('denial_risk_score', 0)
            action = issue.get('action_required', 'Unknown').replace('IMMEDIATE: ', '').replace('REVIEW: ', '').replace('NO ACTION: ', '')
            
            # Format DX and PROC with names
            dx_display = f"DX {idx}: {dx_code}"
            if dx_name:
                dx_display += f" ({dx_name})"
            
            proc_display = f"Procedure {idx}: {proc_code}"
            if proc_name:
                proc_display += f" ({proc_name})"
            
            table_data.append({
                'Diagnosis': dx_display,
                'Procedure': proc_display,
                'Risk': risk_level,
                'Score': f"{risk_score:.0f}",
                'Action': action
            })
        
        if table_data:
            df_issues = pd.DataFrame(table_data)
            st.dataframe(df_issues, use_container_width=True, hide_index=True)
    
    st.markdown("")
    
    # Stage 2: Archetype Analysis Table
    st.markdown("### Stage 2: Archetype Analysis")
    
    # Count SQL evidence
    total_sql_evidence = 0
    archetype_counts = {}
    
    for issue in enriched_issues:
        stage2 = issue.get('stage2_archetype_correction_analysis')
        if stage2:
            archetype = stage2.get('archetype', 'Unknown')
            sql_evidence = stage2.get('sql_evidence', [])
            
            if archetype not in archetype_counts:
                archetype_counts[archetype] = 0
            archetype_counts[archetype] += len(sql_evidence)
            total_sql_evidence += len(sql_evidence)
    
    # Build archetype table
    archetype_table = []
    for archetype, count in archetype_counts.items():
        status = "Evidence Found" if count > 0 else "No Evidence"
        archetype_table.append({
            'Archetype': archetype.replace('_', ' '),
            'SQL Records': count,
            'Status': status
        })
    
    if archetype_table:
        df_archetypes = pd.DataFrame(archetype_table)
        st.dataframe(df_archetypes, use_container_width=True, hide_index=True)
    
    st.markdown(f"**Total SQL Evidence:** {total_sql_evidence} records")
    st.markdown("")
    
    # Correction Recommendations Table
    st.markdown("### Correction Recommendations")
    
    recommendations_data = []
    correction_count = 0
    
    # Debug: Check what's in the data
    debug_info = []
    
    for issue in enriched_issues:
        stage2 = issue.get('stage2_archetype_correction_analysis')
        if stage2:
            correction_analysis = stage2.get('correction_analysis', {})
            debug_info.append(f"Type: {type(correction_analysis)}, Keys: {list(correction_analysis.keys()) if isinstance(correction_analysis, dict) else 'N/A'}")
            
            if isinstance(correction_analysis, dict) and 'recommended_corrections' in correction_analysis:
                recommendations = correction_analysis.get('recommended_corrections', [])
                if recommendations:
                    correction_count += len(recommendations)
                    
                    for rec in recommendations:  # Show all recommendations
                        suggestion = rec.get('suggestion', 'No suggestion')
                        confidence = rec.get('confidence', 0)
                        field = rec.get('field', 'Unknown field')
                        implementation = rec.get('implementation_guidance', 'N/A')
                        
                        recommendations_data.append({
                            'Field': field,
                            'Suggestion': suggestion,  # Full text
                            'Confidence': f"{confidence:.0%}",
                            'Implementation': implementation  # Full text
                        })
    
    # Show debug info if no recommendations found
    if not recommendations_data and debug_info:
        with st.expander("Debug: Why no recommendations?"):
            for info in debug_info:
                st.text(info)
    
    if recommendations_data:
        df_recommendations = pd.DataFrame(recommendations_data)
        st.dataframe(df_recommendations, use_container_width=True, hide_index=True, height=None)
        st.markdown(f"**Total Recommendations:** {correction_count}")
    else:
        st.markdown("No specific correction recommendations generated")
        st.markdown("System found SQL evidence but correction reasoning needs improvement")
    
    st.markdown("")
    
    # System Status Card
    st.markdown("### System Status")
    if total_sql_evidence > 0:
        st.success("OPERATIONAL - System successfully connected to CMS policy database")
    else:
        st.error("NEEDS ATTENTION - No SQL evidence found, check database connection")
    
    st.markdown("---")
    
    # Raw data in expander
    with st.expander("View Raw Data"):
        st.json(data)

# ----------------------------
# UI
# ----------------------------

def main():
    # Header at top, left-justified, higher position
    st.markdown("""
    <style>
    /* Custom header styling */
    .custom-header {
        background: white;
        border-bottom: 1px solid #e1e4e8;
        padding: 14px 0;
        margin-bottom: 24px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    .custom-header h1 {
        font-size: 18px !important;
        font-weight: 600 !important;
        color: #111827 !important;
        margin: 0 0 4px 0 !important;
        text-align: left !important;
    }
    .custom-header p {
        font-size: 13px !important;
        color: #6b7280 !important;
        margin: 0 !important;
        text-align: left !important;
    }
    </style>
    <div class='custom-header'>
        <h1>CXR Exchange  Powered by the Claims Reasoning Kernel</h1>
        <p>A unified platform for automated claim validation, correction, and compliance.</p>
    </div>
    """, unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("""
        <div style='margin-bottom:16px;'>
            <div style='font-weight:600; margin-bottom:6px;'>Instructions</div>
            <div style='font-size:13px; line-height:1.4; margin:0;'>
                1. Select data source<br>
                2. Choose analysis mode<br>
                3. Load/preview data<br>
                4. Run analysis<br>
                5. Review results
            </div>
        </div>
        <hr style='margin:12px 0; border:none; border-top:1px solid #e1e4e8;'>
        """, unsafe_allow_html=True)
        
        st.markdown("<div class='block-title'>Data Source</div>", unsafe_allow_html=True)
        source = st.radio(
            "Select source",
            [
                "Upload JSON",
                "Upload FHIR (JSON)",
            ],
            index=0,
            label_visibility="collapsed",
        )

        st.markdown("<div class='block-title'>Analysis Mode</div>", unsafe_allow_html=True)
        mode = st.selectbox(
            "Analysis mode", 
            [
                "Basic Analysis",
                "Two-Stage Pipeline",
                "Calibrated Analysis",
                "Two-Stage Calibrated",
                "Archetype-Driven (v2)",
                "Archetype-Driven (v3)"
            ], 
            index=4, 
            label_visibility="collapsed",
            key="analysis_mode"
        )
        
        # File uploads in sidebar
        if source == "Upload JSON":
            st.markdown("<div class='block-title'>Upload Claim JSON</div>", unsafe_allow_html=True)
            f = st.file_uploader("Upload claim JSON file", type=["json"], label_visibility="collapsed")
        elif source == "Upload FHIR (JSON)":
            st.markdown("<div class='block-title'>Upload FHIR Claim (R4) JSON</div>", unsafe_allow_html=True)
            fhirf = st.file_uploader("Upload FHIR Claim JSON file", type=["json"], label_visibility="collapsed")

    claim: Optional[Dict[str,Any]] = None

    if source == "Upload JSON":
        if 'f' in locals() and f:
            try:
                claim = json.load(f)
            except Exception as e:
                st.error(f"Invalid JSON: {e}")
    elif source == "Upload FHIR (JSON)":
        if 'fhirf' in locals() and fhirf:
            try:
                fhir_obj = json.load(fhirf)
                if not _validate_fhir(fhir_obj):
                    st.error("Not a valid FHIR Claim resource or missing diagnosis/procedure content.")
                else:
                    claim = _convert_fhir(fhir_obj)
            except Exception as e:
                st.error(f"Invalid FHIR JSON: {e}")

    # Display selected claim in main panel
    if claim:
        show_compact_claim(claim)
        st.markdown("")
        
        st.markdown("<div class='block-title'>Run Analysis</div>", unsafe_allow_html=True)
        run = st.button("Run", use_container_width=False)
        if run:
            # Show progress indicator
            with st.spinner(f"Running {mode}..."):
                try:
                    if mode == "Basic Analysis":
                        out = run_basic(claim)
                    elif mode == "Two-Stage Pipeline":
                        out = run_pipeline(claim)
                    elif mode == "Calibrated Analysis":
                        out = run_calibrated(claim['CLM_ID'])
                    elif mode == "Two-Stage Calibrated":
                        out = run_two_stage_calibrated(claim['CLM_ID'])
                    elif mode == "Archetype-Driven (v2)":
                        # Archetype-Driven (v2): ensure Stage 1 issues exist
                        try:
                            _ = run_basic(claim)  # populates Qdrant with DX-PROC issues
                        except Exception:
                            pass
                        out = run_archetype_v2(claim['CLM_ID'])
                    else:
                        # Archetype-Driven (v3): ensure Stage 1 issues exist
                        try:
                            _ = run_basic(claim)  # populates Qdrant with DX-PROC issues
                        except Exception:
                            pass
                        out = run_archetype_v3(claim['CLM_ID'])
                    
                    st.success("Analysis complete")
                    
                    # Show simple summary instead of raw JSON
                    if mode == "Archetype-Driven (v3)" and isinstance(out, dict):
                        try:
                            show_v3_summary(out)
                        except Exception as e:
                            st.error(f"Error displaying summary: {e}")
                            import traceback
                            st.text(traceback.format_exc())
                            st.json(out)  # Fallback to JSON
                    else:
                        st.json(out)
                except Exception as e:
                    st.error(f"Analysis failed: {e}")

if __name__ == "__main__":
    main()

CMS Claim Analysis Streamlit App (v2 Minimal)
=============================================
- Minimalist, uniform typography
- Load CMS inpatient/outpatient claims from SQL Server (`the_claims`)
- Run full analysis flows including Archetype-Driven v2
"""

import streamlit as st
import json
import pandas as pd
import pyodbc
import sys
import os
from datetime import datetime
from typing import Dict, Any, Optional

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from new_claim_analyzer1 import NewClaimAnalyzer
from claim_corrector_claims3_archetype_driven_v2 import ArchetypeDrivenClaimCorrectorV2
from claim_corrector_claims3_archetype_driven_v3 import ArchetypeDrivenClaimCorrectorV3
from claim_corrector_claims3_two_stage_calibrated import TwoStageCalibratedClaimCorrector
from claim_corrector_claims3_calibrated import CalibratedClaimCorrector
from claim_corrector_claims import ClaimCorrector
from fhir_adapter import validate_fhir_claim as _validate_fhir, convert_fhir_claim as _convert_fhir

# ----------------------------
# Page + Minimal Styles
# ----------------------------
st.set_page_config(page_title="CMS Claim Analysis v2", page_icon="", layout="wide")

st.markdown(
"""
<style>
:root { 
    --base-font: 14px; 
    --muted: #6b7280; 
    --text: #111827;
    --border: #e5e7eb;
}
html, body, [class*="css"] { 
    font-size: var(--base-font); 
    font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; 
    color: var(--text);
    line-height: 1.5;
}
h1, h2, h3, h4, h5, h6 { 
    font-size: var(--base-font) !important; 
    font-weight: 600 !important; 
    margin: 1rem 0 0.5rem 0 !important;
    color: var(--text) !important;
}
p, div, span, li { 
    font-size: var(--base-font) !important; 
    color: var(--text) !important;
    margin: 0.5rem 0 !important;
}
.block-title { 
    font-size: var(--base-font) !important; 
    font-weight: 600 !important; 
    margin: 1rem 0 0.5rem 0 !important; 
    color: var(--text) !important;
}
.small { 
    color: var(--muted); 
    font-size: var(--base-font) !important; 
}
.card { 
    border: 1px solid var(--border); 
    border-radius: 6px; 
    padding: 16px; 
    margin: 8px 0;
}
.btn-primary { 
    background: var(--text); 
    color: white; 
    padding: 8px 16px; 
    border-radius: 4px; 
    text-decoration: none; 
    font-size: var(--base-font) !important;
}
.metric { 
    font-size: var(--base-font) !important; 
    color: var(--text) !important;
}
.metric strong { 
    font-size: var(--base-font) !important; 
    color: var(--text) !important;
    font-weight: 600 !important;
}
/* Clean separators */
hr {
    border: none;
    border-top: 1px solid var(--border);
    margin: 1.5rem 0;
}
/* Uniform bullet points */
ul {
    margin-left: 20px;
    padding-left: 0;
}
li {
    font-size: var(--base-font) !important;
    margin: 0.25rem 0;
    list-style-type: disc;
}
/* Compact radio buttons - force single spacing */
.stRadio > div {
    gap: 0px !important;
    padding: 0 !important;
}
.stRadio > div > label {
    margin: 0 !important;
    padding: 2px 0 !important;
    gap: 6px !important;
}
.stRadio > div > label > div {
    padding: 0 !important;
    line-height: 1.2 !important;
}
.stRadio > div > label > div > div {
    margin: 0 !important;
    padding: 0 !important;
}
/* Analysis mode dropdown - visible selector, compact menu */
.stSelectbox > div > div {
    min-height: 42px !important;
}
.stSelectbox [data-baseweb="select"] > div {
    padding: 10px 14px !important;
    line-height: 1.5 !important;
    min-height: 42px !important;
    font-size: 14px !important;
}
.stSelectbox [data-baseweb="select"] {
    min-height: 42px !important;
}
/* Dropdown menu options - single spacing */
[data-baseweb="menu"] {
    padding: 4px !important;
}
[data-baseweb="menu"] li {
    padding: 6px 12px !important;
    margin: 0 !important;
    line-height: 1.3 !important;
}
[role="option"] {
    margin: 0 !important;
    padding: 6px 12px !important;
    line-height: 1.3 !important;
}
/* Fix JSON display - single spacing and compact */
pre {
    line-height: 1.1 !important;
    margin: 0 !important;
    padding: 6px 8px !important;
    font-size: 12px !important;
    background-color: #f8f9fa !important;
    border-radius: 4px !important;
}
code {
    line-height: 1.1 !important;
    font-size: 12px !important;
}
/* Streamlit JSON component - force compact */
.stJson {
    line-height: 1.1 !important;
}
.stJson pre {
    line-height: 1.1 !important;
    margin: 0 !important;
    padding: 6px !important;
}
/* Expander content - compact */
.streamlit-expanderContent {
    padding: 4px 8px !important;
}
/* Dataframe text wrapping - show full text */
.stDataFrame td, .stDataFrame th {
    white-space: normal !important;
    word-wrap: break-word !important;
    max-width: none !important;
}
.stDataFrame [data-testid="stDataFrameResizable"] {
    overflow-x: auto !important;
}
</style>
""",
unsafe_allow_html=True,
)

# ----------------------------
# DB Access for CMS Claims
# ----------------------------

def get_sql_conn():
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=localhost,1433;Database=_claims;"
        "UID=SA;PWD=Bbanwo@1980!;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)

@st.cache_data(show_spinner=False)
def list_tables() -> Dict[str, str]:
    try:
        with get_sql_conn() as conn:
            df = pd.read_sql("SELECT name FROM sys.tables ORDER BY name", conn)
        names = df['name'].str.lower().tolist()
        candidates = {
            'inpatient': next((n for n in names if 'inpatient' in n or 'ip' in n), None),
            'outpatient': next((n for n in names if 'outpatient' in n or 'op' in n), None),
        }
        return candidates
    except Exception as e:
        st.error(f"DB table discovery failed: {e}")
        return {'inpatient': None, 'outpatient': None}

@st.cache_data(show_spinner=False)
def load_claims(table: str, limit: int = 2000) -> pd.DataFrame:
    with get_sql_conn() as conn:
        q = f"SELECT TOP {limit} * FROM [{table}]"
        return pd.read_sql(q, conn)

# ----------------------------
# Normalization to App Schema
# ----------------------------

def normalize_row_to_claim(row: pd.Series) -> Dict[str, Any]:
    def pick(*keys):
        for k in keys:
            if k in row and pd.notna(row[k]):
                return str(row[k]).strip()
        return None

    clm_id = pick('CLM_ID','claim_id','clm_id','claimid') or f"cms-{row.name}"
    desyn = pick('DESYNPUF_ID','bene_id','patient_id') or "unknown-patient"
    from_dt = pick('CLM_FROM_DT','service_date','from_date','srvc_from_dt') or ""
    thru_dt = pick('CLM_THRU_DT','thru_date','srvc_thru_dt') or from_dt
    prvdr = pick('PRVDR_NUM','provider_id','npi','tin') or "unknown-provider"

    # Collect ICD-9/10 columns heuristically
    dx_map: Dict[str,str] = {}
    dx_cols = [c for c in row.index if str(c).lower().startswith(('icd9','icd10','dx','diag'))]
    pos = 1
    for c in dx_cols:
        val = row[c]
        if pd.notna(val) and str(val).strip():
            dx_map[f"ICD9_DGNS_CD_{pos}"] = str(val).strip()
            pos += 1
            if pos > 10:
                break

    # Collect HCPCS/CPT columns heuristically
    proc_map: Dict[str,str] = {}
    hcpcs_cols = [c for c in row.index if str(c).lower().startswith(('hcpcs','cpt','proc'))]
    ppos = 1
    for c in hcpcs_cols:
        val = row[c]
        if pd.notna(val) and str(val).strip():
            proc_map[f"HCPCS_CD_{ppos}"] = str(val).strip()
            ppos += 1
            if ppos > 45:
                break

    return {
        "CLM_ID": clm_id,
        "DESYNPUF_ID": desyn,
        "CLM_FROM_DT": from_dt,
        "CLM_THRU_DT": thru_dt,
        "PRVDR_NUM": prvdr,
        "diagnosis_codes": dx_map or {"ICD9_DGNS_CD_1": pick('ICD9_DGNS_CD_1','ICD10_DGNS_CD_1','DX1') or ""},
        "procedure_codes": proc_map or {"HCPCS_CD_1": pick('HCPCS_CD_1','CPT1','PROC1') or ""},
    }

# ----------------------------
# Analysis Runners
# ----------------------------

def run_basic(claim: Dict[str,Any]):
    analyzer = NewClaimAnalyzer()
    return analyzer.analyze_new_claim(claim)

def run_pipeline(claim: Dict[str,Any]):
    analyzer = NewClaimAnalyzer()
    diagnostics = analyzer.analyze_new_claim(claim)
    corrector = ClaimCorrector()
    enriched = corrector.run_corrections(diagnostics.get('claim_summary',{}).get('CLM_ID', claim['CLM_ID']))
    return {"stage1": diagnostics, "stage2": enriched}

def run_calibrated(claim_id: str):
    return CalibratedClaimCorrector().run_corrections(claim_id)

def run_two_stage_calibrated(claim_id: str):
    return TwoStageCalibratedClaimCorrector().run_two_stage_corrections(claim_id)

def run_archetype_v2(claim_id: str):
    return ArchetypeDrivenClaimCorrectorV2().run_archetype_driven_corrections(claim_id)

def run_archetype_v3(claim_id: str):
    return ArchetypeDrivenClaimCorrectorV3().run_archetype_driven_corrections(claim_id)

def show_compact_claim(claim: Dict[str, Any]):
    """Display claim in compact JSON format with custom CSS."""
    
    # Create compact JSON string
    json_str = json.dumps(claim, indent=2)
    
    # Display with custom styling
    st.markdown(f"""
    <div style='background:#f8f9fa; border:1px solid #e1e4e8; border-radius:6px; padding:10px 14px; font-family:monospace; font-size:12px; line-height:1.5; overflow-x:auto;'>
        <pre style='margin:0; padding:0; line-height:1.5; font-size:12px;'><code>{json_str}</code></pre>
    </div>
    """, unsafe_allow_html=True)

def show_v3_summary(data):
    """Show a clean, minimalist summary of v3 results with structured tables."""
    
    claim_id = data.get('claim_id', 'Unknown')
    total_issues = data.get('total_issues', 0)
    enriched_issues = data.get('enriched_issues', [])
    
    # Header Card
    st.markdown("---")
    st.markdown("### Analysis Summary")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**Claim ID:** {claim_id}")
    with col2:
        st.markdown(f"**Total Issues:** {total_issues}")
    st.markdown("---")
    
    # Stage 1: Issues Analysis Table
    st.markdown("### Stage 1: Issues Analysis")
    
    if enriched_issues:
        # Build table data
        table_data = []
        for idx, issue in enumerate(enriched_issues[:8], 1):  # Show top 8
            dx_code = issue.get('icd10_code', issue.get('icd9_code', 'Unknown'))
            dx_name = issue.get('diagnosis_name', '')
            proc_code = issue.get('hcpcs_code', 'Unknown')
            proc_name = issue.get('procedure_name', '')
            risk_level = issue.get('denial_risk_level', 'Unknown').replace('HIGH: ', '')
            risk_score = issue.get('denial_risk_score', 0)
            action = issue.get('action_required', 'Unknown').replace('IMMEDIATE: ', '').replace('REVIEW: ', '').replace('NO ACTION: ', '')
            
            # Format DX and PROC with names
            dx_display = f"DX {idx}: {dx_code}"
            if dx_name:
                dx_display += f" ({dx_name})"
            
            proc_display = f"Procedure {idx}: {proc_code}"
            if proc_name:
                proc_display += f" ({proc_name})"
            
            table_data.append({
                'Diagnosis': dx_display,
                'Procedure': proc_display,
                'Risk': risk_level,
                'Score': f"{risk_score:.0f}",
                'Action': action
            })
        
        if table_data:
            df_issues = pd.DataFrame(table_data)
            st.dataframe(df_issues, use_container_width=True, hide_index=True)
    
    st.markdown("")
    
    # Stage 2: Archetype Analysis Table
    st.markdown("### Stage 2: Archetype Analysis")
    
    # Count SQL evidence
    total_sql_evidence = 0
    archetype_counts = {}
    
    for issue in enriched_issues:
        stage2 = issue.get('stage2_archetype_correction_analysis')
        if stage2:
            archetype = stage2.get('archetype', 'Unknown')
            sql_evidence = stage2.get('sql_evidence', [])
            
            if archetype not in archetype_counts:
                archetype_counts[archetype] = 0
            archetype_counts[archetype] += len(sql_evidence)
            total_sql_evidence += len(sql_evidence)
    
    # Build archetype table
    archetype_table = []
    for archetype, count in archetype_counts.items():
        status = "Evidence Found" if count > 0 else "No Evidence"
        archetype_table.append({
            'Archetype': archetype.replace('_', ' '),
            'SQL Records': count,
            'Status': status
        })
    
    if archetype_table:
        df_archetypes = pd.DataFrame(archetype_table)
        st.dataframe(df_archetypes, use_container_width=True, hide_index=True)
    
    st.markdown(f"**Total SQL Evidence:** {total_sql_evidence} records")
    st.markdown("")
    
    # Correction Recommendations Table
    st.markdown("### Correction Recommendations")
    
    recommendations_data = []
    correction_count = 0
    
    # Debug: Check what's in the data
    debug_info = []
    
    for issue in enriched_issues:
        stage2 = issue.get('stage2_archetype_correction_analysis')
        if stage2:
            correction_analysis = stage2.get('correction_analysis', {})
            debug_info.append(f"Type: {type(correction_analysis)}, Keys: {list(correction_analysis.keys()) if isinstance(correction_analysis, dict) else 'N/A'}")
            
            if isinstance(correction_analysis, dict) and 'recommended_corrections' in correction_analysis:
                recommendations = correction_analysis.get('recommended_corrections', [])
                if recommendations:
                    correction_count += len(recommendations)
                    
                    for rec in recommendations:  # Show all recommendations
                        suggestion = rec.get('suggestion', 'No suggestion')
                        confidence = rec.get('confidence', 0)
                        field = rec.get('field', 'Unknown field')
                        implementation = rec.get('implementation_guidance', 'N/A')
                        
                        recommendations_data.append({
                            'Field': field,
                            'Suggestion': suggestion,  # Full text
                            'Confidence': f"{confidence:.0%}",
                            'Implementation': implementation  # Full text
                        })
    
    # Show debug info if no recommendations found
    if not recommendations_data and debug_info:
        with st.expander("Debug: Why no recommendations?"):
            for info in debug_info:
                st.text(info)
    
    if recommendations_data:
        df_recommendations = pd.DataFrame(recommendations_data)
        st.dataframe(df_recommendations, use_container_width=True, hide_index=True, height=None)
        st.markdown(f"**Total Recommendations:** {correction_count}")
    else:
        st.markdown("No specific correction recommendations generated")
        st.markdown("System found SQL evidence but correction reasoning needs improvement")
    
    st.markdown("")
    
    # System Status Card
    st.markdown("### System Status")
    if total_sql_evidence > 0:
        st.success("OPERATIONAL - System successfully connected to CMS policy database")
    else:
        st.error("NEEDS ATTENTION - No SQL evidence found, check database connection")
    
    st.markdown("---")
    
    # Raw data in expander
    with st.expander("View Raw Data"):
        st.json(data)

# ----------------------------
# UI
# ----------------------------

def main():
    # Header at top, left-justified, higher position
    st.markdown("""
    <style>
    /* Custom header styling */
    .custom-header {
        background: white;
        border-bottom: 1px solid #e1e4e8;
        padding: 14px 0;
        margin-bottom: 24px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    .custom-header h1 {
        font-size: 18px !important;
        font-weight: 600 !important;
        color: #111827 !important;
        margin: 0 0 4px 0 !important;
        text-align: left !important;
    }
    .custom-header p {
        font-size: 13px !important;
        color: #6b7280 !important;
        margin: 0 !important;
        text-align: left !important;
    }
    </style>
    <div class='custom-header'>
        <h1>CXR Exchange  Powered by the Claims Reasoning Kernel</h1>
        <p>A unified platform for automated claim validation, correction, and compliance.</p>
    </div>
    """, unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("""
        <div style='margin-bottom:16px;'>
            <div style='font-weight:600; margin-bottom:6px;'>Instructions</div>
            <div style='font-size:13px; line-height:1.4; margin:0;'>
                1. Select data source<br>
                2. Choose analysis mode<br>
                3. Load/preview data<br>
                4. Run analysis<br>
                5. Review results
            </div>
        </div>
        <hr style='margin:12px 0; border:none; border-top:1px solid #e1e4e8;'>
        """, unsafe_allow_html=True)
        
        st.markdown("<div class='block-title'>Data Source</div>", unsafe_allow_html=True)
        source = st.radio(
            "Select source",
            [
                "Upload JSON",
                "Upload FHIR (JSON)",
            ],
            index=0,
            label_visibility="collapsed",
        )

        st.markdown("<div class='block-title'>Analysis Mode</div>", unsafe_allow_html=True)
        mode = st.selectbox(
            "Analysis mode", 
            [
                "Basic Analysis",
                "Two-Stage Pipeline",
                "Calibrated Analysis",
                "Two-Stage Calibrated",
                "Archetype-Driven (v2)",
                "Archetype-Driven (v3)"
            ], 
            index=4, 
            label_visibility="collapsed",
            key="analysis_mode"
        )
        
        # File uploads in sidebar
        if source == "Upload JSON":
            st.markdown("<div class='block-title'>Upload Claim JSON</div>", unsafe_allow_html=True)
            f = st.file_uploader("Upload claim JSON file", type=["json"], label_visibility="collapsed")
        elif source == "Upload FHIR (JSON)":
            st.markdown("<div class='block-title'>Upload FHIR Claim (R4) JSON</div>", unsafe_allow_html=True)
            fhirf = st.file_uploader("Upload FHIR Claim JSON file", type=["json"], label_visibility="collapsed")

    claim: Optional[Dict[str,Any]] = None

    if source == "Upload JSON":
        if 'f' in locals() and f:
            try:
                claim = json.load(f)
            except Exception as e:
                st.error(f"Invalid JSON: {e}")
    elif source == "Upload FHIR (JSON)":
        if 'fhirf' in locals() and fhirf:
            try:
                fhir_obj = json.load(fhirf)
                if not _validate_fhir(fhir_obj):
                    st.error("Not a valid FHIR Claim resource or missing diagnosis/procedure content.")
                else:
                    claim = _convert_fhir(fhir_obj)
            except Exception as e:
                st.error(f"Invalid FHIR JSON: {e}")

    # Display selected claim in main panel
    if claim:
        show_compact_claim(claim)
        st.markdown("")
        
        st.markdown("<div class='block-title'>Run Analysis</div>", unsafe_allow_html=True)
        run = st.button("Run", use_container_width=False)
        if run:
            # Show progress indicator
            with st.spinner(f"Running {mode}..."):
                try:
                    if mode == "Basic Analysis":
                        out = run_basic(claim)
                    elif mode == "Two-Stage Pipeline":
                        out = run_pipeline(claim)
                    elif mode == "Calibrated Analysis":
                        out = run_calibrated(claim['CLM_ID'])
                    elif mode == "Two-Stage Calibrated":
                        out = run_two_stage_calibrated(claim['CLM_ID'])
                    elif mode == "Archetype-Driven (v2)":
                        # Archetype-Driven (v2): ensure Stage 1 issues exist
                        try:
                            _ = run_basic(claim)  # populates Qdrant with DX-PROC issues
                        except Exception:
                            pass
                        out = run_archetype_v2(claim['CLM_ID'])
                    else:
                        # Archetype-Driven (v3): ensure Stage 1 issues exist
                        try:
                            _ = run_basic(claim)  # populates Qdrant with DX-PROC issues
                        except Exception:
                            pass
                        out = run_archetype_v3(claim['CLM_ID'])
                    
                    st.success("Analysis complete")
                    
                    # Show simple summary instead of raw JSON
                    if mode == "Archetype-Driven (v3)" and isinstance(out, dict):
                        try:
                            show_v3_summary(out)
                        except Exception as e:
                            st.error(f"Error displaying summary: {e}")
                            import traceback
                            st.text(traceback.format_exc())
                            st.json(out)  # Fallback to JSON
                    else:
                        st.json(out)
                except Exception as e:
                    st.error(f"Analysis failed: {e}")

if __name__ == "__main__":
    main()


