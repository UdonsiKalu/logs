#!/usr/bin/env python3
"""
CXR Exchange — Final Unified Version
--------------------------------------
✓ Combined minimalist global styling + risk-colored cards
✓ Stage 1 & 3 as stacked cards inside dropdowns
✓ Global UI polish for sidebar, buttons, tables, typography
✓ Risk-level color legend included
"""

import streamlit as st
import json
import pandas as pd
import pyodbc
import sys
import os
from typing import Dict, Any, Optional

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from new_claim_analyzer1 import NewClaimAnalyzer
from claim_corrector_claims3_archetype_driven_v2 import ArchetypeDrivenClaimCorrectorV2
from claim_corrector_claims3_archetype_driven_update5 import ArchetypeDrivenClaimCorrector as ArchetypeDrivenClaimCorrectorV3
from claim_corrector_claims3_two_stage_calibrated import TwoStageCalibratedClaimCorrector
from claim_corrector_claims3_calibrated import CalibratedClaimCorrector
from claim_corrector_claims import ClaimCorrector
from fhir_adapter import validate_fhir_claim as _validate_fhir, convert_fhir_claim as _convert_fhir

# ----------------------------------------------------
# Page + Unified CSS
# ----------------------------------------------------
st.set_page_config(page_title="CXR Exchange", page_icon="🏥", layout="wide")

st.markdown("""
<style>
:root {
  --font: 'Inter', system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
  --text: #1e1e1e;
  --muted: #6b7280;
  --border: #e5e7eb;
  --surface: #ffffff;
  --surface-alt: #f9fafb;
  --accent: #2563eb;
  --accent-light: #eff6ff;
  --high-bg: #fee2e2;
  --medium-bg: #fef3c7;
  --low-bg: #dcfce7;
}

html, body, [class*="css"] {
  font-family: var(--font);
  color: var(--text);
  background-color: var(--surface);
  line-height: 1.5;
  font-size: 14px;
}

/* Sidebar */
section[data-testid="stSidebar"] {
  background-color: var(--surface-alt);
  border-right: 1px solid var(--border);
  padding: 1.25rem 1rem;
}

/* Buttons */
.stButton > button {
  background-color: var(--accent);
  color: white;
  border: none;
  border-radius: 6px;
  padding: 0.5rem 1rem;
  font-weight: 500;
  transition: all 0.15s ease-in-out;
}
.stButton > button:hover {
  background-color: #1d4ed8;
  transform: translateY(-1px);
}

/* Risk + Info Cards */
.card {
  border-radius: 8px;
  padding: 1rem 1.25rem;
  margin: 0.75rem 0;
  box-shadow: 0 1px 3px rgba(0,0,0,0.05);
  animation: fadeIn 0.4s ease-in;
}
.card.high {background-color: var(--high-bg); border-left: 4px solid #dc2626;}
.card.medium {background-color: var(--medium-bg); border-left: 4px solid #f59e0b;}
.card.low {background-color: var(--low-bg); border-left: 4px solid #16a34a;}
.card.neutral {background-color: var(--surface-alt); border-left: 4px solid var(--border);}

/* Tables + JSON */
.stDataFrame {
  background-color: var(--surface);
  border-radius: 8px;
  border: 1px solid var(--border);
  overflow: hidden;
  margin: 0.75rem 0;
}
.stDataFrame th {
  background-color: var(--accent);
  color: white;
}
pre, code {
  font-family: 'JetBrains Mono', monospace;
  background-color: var(--surface-alt);
  border-radius: 6px;
  padding: 0.5rem 0.75rem;
  font-size: 12px;
}

/* Expander */
.streamlit-expanderContent {
  background-color: var(--surface-alt);
  border-left: 3px solid var(--border);
  border-radius: 6px;
  padding: 0.75rem;
}

/* Animation */
@keyframes fadeIn {
  from {opacity: 0; transform: translateY(4px);}
  to {opacity: 1; transform: translateY(0);}
}

/* Scrollbars */
::-webkit-scrollbar {height: 8px; width: 8px;}
::-webkit-scrollbar-thumb {background: #d1d5db; border-radius: 4px;}
::-webkit-scrollbar-thumb:hover {background: #9ca3af;}
</style>
""", unsafe_allow_html=True)




####################################################

# ----------------------------------------------------
# Database functions
# ----------------------------------------------------
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
        return {
            'inpatient': next((n for n in names if 'inpatient' in n or 'ip' in n), None),
            'outpatient': next((n for n in names if 'outpatient' in n or 'op' in n), None),
        }
    except Exception as e:
        st.error(f"DB table discovery failed: {e}")
        return {'inpatient': None, 'outpatient': None}

@st.cache_data(show_spinner=False)
def load_claims(table: str, limit: int = 2000) -> pd.DataFrame:
    with get_sql_conn() as conn:
        q = f"SELECT TOP {limit} * FROM [{table}]"
        return pd.read_sql(q, conn)

# ----------------------------------------------------
# Claim normalization + runners
# ----------------------------------------------------
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
    dx_map, proc_map = {}, {}
    dx_cols = [c for c in row.index if str(c).lower().startswith(('icd9','icd10','dx','diag'))]
    for i,c in enumerate(dx_cols[:10],1):
        if pd.notna(row[c]): dx_map[f"ICD9_DGNS_CD_{i}"] = str(row[c]).strip()
    hcpcs_cols = [c for c in row.index if str(c).lower().startswith(('hcpcs','cpt','proc'))]
    for i,c in enumerate(hcpcs_cols[:45],1):
        if pd.notna(row[c]): proc_map[f"HCPCS_CD_{i}"] = str(row[c]).strip()
    return {
        "CLM_ID": clm_id,
        "DESYNPUF_ID": desyn,
        "CLM_FROM_DT": from_dt,
        "CLM_THRU_DT": thru_dt,
        "PRVDR_NUM": prvdr,
        "diagnosis_codes": dx_map,
        "procedure_codes": proc_map,
    }

# Runner wrappers
def run_basic(claim): return NewClaimAnalyzer().analyze_new_claim(claim)
def run_pipeline(claim):
    a = NewClaimAnalyzer().analyze_new_claim(claim)
    c = ClaimCorrector().run_corrections(a.get('claim_summary',{}).get('CLM_ID', claim['CLM_ID']))
    return {"stage1": a, "stage2": c}

def run_calibrated(cid): return CalibratedClaimCorrector().run_corrections(cid)
def run_two_stage_calibrated(cid): return TwoStageCalibratedClaimCorrector().run_two_stage_corrections(cid)
def run_archetype_v2(cid): return ArchetypeDrivenClaimCorrectorV2().run_archetype_driven_corrections(cid)
def run_archetype_v3(cid): return ArchetypeDrivenClaimCorrectorV3().run_archetype_driven_corrections(cid)



##########################################

# ----------------------------------------------------
# Display + Main
# ----------------------------------------------------
def get_risk_class(action: str):
    """
    Map action types to color classes.
    - IMMEDIATE → high (red)
    - REVIEW → medium (yellow)
    - NO ACTION → low (green)
    """
    if not action:
        return "neutral"

    a = action.strip().lower()
    if "immediate" in a:
        return "high"
    elif "review" in a:
        return "medium"
    elif "no action" in a:
        return "low"
    else:
        return "neutral"


def show_v3_summary(data):
    """Display multi-stage claim summary results in expandable cards."""
    claim_id = data.get("claim_id", "Unknown")
    total_issues = data.get("total_issues", 0)
    enriched_issues = data.get("enriched_issues", [])

    st.markdown(f"### Claim ID: {claim_id} | Total Issues: {total_issues}")

    # Color Legend
    st.markdown(
        """
        <div style='margin-top:10px;'>
          <span style='background:#fee2e2;border-radius:6px;padding:4px 10px;margin-right:8px;'>🔴 IMMEDIATE — Critical Action Required</span>
          <span style='background:#fef3c7;border-radius:6px;padding:4px 10px;margin-right:8px;'>🟠 REVIEW — Manual Review or Verification</span>
          <span style='background:#dcfce7;border-radius:6px;padding:4px 10px;'>🟢 NO ACTION — Informational / Cleared</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ---------------------------
    # Stage 1 – Issues Analysis
    # ---------------------------
    with st.expander("Stage 1 – Issues Analysis", expanded=True):
        if not enriched_issues:
            st.info("No issues found.")
        else:
            for idx, issue in enumerate(enriched_issues, 1):
                dx = issue.get("icd10_code", issue.get("icd9_code", "Unknown"))
                dx_name = issue.get("diagnosis_name", "")
                proc = issue.get("hcpcs_code", "Unknown")
                proc_name = issue.get("procedure_name", "")
                risk = issue.get("denial_risk_level", "Unknown")
                score = issue.get("denial_risk_score", 0)
                action = issue.get("action_required", "N/A")

                color_class = get_risk_class(action)

                st.markdown(
                    f"""
                    <div class='card {color_class}'>
                    <b>DX {idx}:</b> {dx} ({dx_name})<br>
                    <b>Procedure {idx}:</b> {proc} ({proc_name})<br>
                    <b>Issue:</b> {risk}<br>
                    <b>Action:</b> {action}<br>
                    <b>Risk Score:</b> {score}
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    # ---------------------------
    # Stage 2 – Archetype Analysis
    # ---------------------------
    with st.expander("Stage 2 – Archetype Analysis", expanded=False):
        archetype_counts = {}
        total_sql_evidence = 0

        for issue in enriched_issues:
            s2 = issue.get("stage2_archetype_correction_analysis")
            if s2:
                a = s2.get("archetype", "Unknown")
                sql_ev = s2.get("sql_evidence", [])
                archetype_counts[a] = archetype_counts.get(a, 0) + len(sql_ev)
                total_sql_evidence += len(sql_ev)

        if archetype_counts:
            df = pd.DataFrame(
                [{"Archetype": k.replace("_", " "), "SQL Records": v} for k, v in archetype_counts.items()]
            )
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.caption(f"Total SQL Evidence: {total_sql_evidence}")
        else:
            st.info("No archetype data found.")

    # ---------------------------
    # Stage 3 – Correction Recommendations
    # ---------------------------
    with st.expander("Stage 3 – Correction Recommendations", expanded=False):
        rec_cards = []
        for issue in enriched_issues:
            s2 = issue.get("stage2_archetype_correction_analysis")
            if s2:
                ca = s2.get("correction_analysis", {})
                for r in ca.get("recommended_corrections", []):
                    rec_cards.append(r)

        if rec_cards:
            for r in rec_cards:
                # Extract values from LLM output structure
                field = r.get('field', 'Unknown')
                current_val = r.get('current_value', '')
                suggested_val = r.get('suggested_value', '')
                rationale = r.get('rationale', '')
                policy_cite = r.get('policy_citation', '')
                impl_steps = r.get('implementation_steps', [])
                confidence = r.get('confidence', 0)
                
                # Format suggestion text
                suggestion_text = f"Change from '{current_val}' to '{suggested_val}'" if current_val and suggested_val else suggested_val
                
                # Format implementation steps
                if impl_steps and isinstance(impl_steps, list):
                    impl_text = "<br>".join([f"• {step}" for step in impl_steps])
                else:
                    impl_text = impl_steps if isinstance(impl_steps, str) else ""
                
                # Build recommendation card
                card_html = f"""
                <div class='card neutral'>
                <b>Field:</b> {field}<br>
                <b>Suggestion:</b> {suggestion_text}<br>
                """
                
                if rationale:
                    card_html += f"<b>Rationale:</b> {rationale}<br>"
                
                if policy_cite:
                    card_html += f"<b>Policy Citation:</b> {policy_cite}<br>"
                
                card_html += f"<b>Confidence:</b> {confidence:.0%}<br>"
                
                if impl_text:
                    card_html += f"<b>Implementation Steps:</b><br>{impl_text}<br>"
                
                card_html += "</div>"
                
                st.markdown(card_html, unsafe_allow_html=True)
        else:
            st.info("No recommendations available.")


# ----------------------------------------------------
# Main
# ----------------------------------------------------
def main():
    st.markdown(
        "<div class='custom-header'><h1>CXR Exchange — Claims Reasoning Kernel</h1><p>Automated claim validation, correction, and compliance.</p></div>",
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.markdown("**Instructions**")
        st.markdown("1. Upload claim JSON<br>2. Choose analysis mode<br>3. Run analysis", unsafe_allow_html=True)
        st.divider()

        source = st.radio("Source", ["Upload JSON", "Upload FHIR (JSON)"], index=0)
        mode = st.selectbox(
            "Mode",
            [
                "Basic Analysis",
                "Two-Stage Pipeline",
                "Calibrated Analysis",
                "Two-Stage Calibrated",
                "Archetype-Driven (v2)",
                "Archetype-Driven (UPDATE5)",
            ],
            index=5,
        )

        f = fhirf = None
        if source == "Upload JSON":
            f = st.file_uploader("Upload claim JSON", type=["json"])
        elif source == "Upload FHIR (JSON)":
            fhirf = st.file_uploader("Upload FHIR JSON", type=["json"])

    claim = None
    if f:
        claim = json.load(f)
    elif fhirf:
        fj = json.load(fhirf)
        if _validate_fhir(fj):
            claim = _convert_fhir(fj)
        else:
            st.error("Invalid FHIR Claim")

    if claim:
        st.markdown("<div class='result-card info'><b>Uploaded Claim:</b></div>", unsafe_allow_html=True)
        st.json(claim)

        if st.button("Run Analysis"):
            with st.spinner(f"Running {mode}..."):
                try:
                    if mode == "Basic Analysis":
                        out = run_basic(claim)
                    elif mode == "Two-Stage Pipeline":
                        out = run_pipeline(claim)
                    elif mode == "Calibrated Analysis":
                        out = run_calibrated(claim["CLM_ID"])
                    elif mode == "Two-Stage Calibrated":
                        out = run_two_stage_calibrated(claim["CLM_ID"])
                    elif mode == "Archetype-Driven (v2)":
                        _ = run_basic(claim)
                        out = run_archetype_v2(claim["CLM_ID"])
                    else:
                        _ = run_basic(claim)
                        out = run_archetype_v3(claim["CLM_ID"])

                    st.success("Analysis complete ")

                    if mode.endswith("(UPDATE5)") and isinstance(out, dict):
                        show_v3_summary(out)
                    else:
                        st.json(out)
                except Exception as e:
                    st.error(f"Analysis failed: {e}")


if __name__ == "__main__":
    main()
