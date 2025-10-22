import streamlit as st
import pandas as pd
import json
from io import BytesIO
from agent_runner import generate_agent_decision
from tools import cms_tools
from formatter import (
    render_claim_analysis,
    render_agent_trace,
    render_retrieved_policy_docs
)

# --- Config ---
st.set_page_config(page_title="Medicare Denial Risk Assistant", layout="wide")
st.title("Medicare Denial Risk Assistant")
st.markdown(
    "Assess Medicare claim denial risk based on CPT codes, diagnoses, modifiers, and CMS policy. "
    "Input a single scenario or upload multiple claims for batch processing."
)

# --- Utility for Export ---
def generate_download(results, filename="results.json"):
    buffer = BytesIO()
    buffer.write(json.dumps(results, indent=2).encode())
    buffer.seek(0)
    return buffer

# --- Main Tabs ---
tab1, tab2, tab3 = st.tabs([" Single Claim", " Multiple (Text)", " Upload CSV"])

# --- Single Claim Input ---
with tab1:
    default_query = "Would CPT 99214 with diagnosis Z79.899 and modifier -25 be denied under Medicare?"
    user_input = st.text_area("Claim Scenario", value=default_query, height=140)

    if st.button("Run Analysis"):
        if not user_input.strip():
            st.warning("Please enter a valid claim scenario.")
        else:
            progress = st.progress(0)
            status = st.empty()
            progress.progress(10)
            status.text(" Interpreting input...")

            parsed = generate_agent_decision(user_input)

            if not parsed:
                progress.empty()
                status.error(" Could not parse input.")
                st.error("Agent failed to extract structured action.")
            else:
                progress.progress(35)
                status.text(f" Tool selected: `{parsed['action']}`")
                tool = {t.name: t for t in cms_tools}.get(parsed["action"])

                if not tool:
                    progress.empty()
                    status.error(" No matching tool.")
                    st.error(f"No tool for `{parsed['action']}`")
                else:
                    try:
                        progress.progress(65)
                        status.text(" Executing tool...")
                        result = tool.func(parsed["action_input"])

                        progress.progress(90)
                        status.text(" Rendering results...")
                        render_claim_analysis(parsed, result)
                        st.markdown("---")
                        render_agent_trace(parsed, result)
                        render_retrieved_policy_docs(parsed["action"], result)

                        progress.progress(100)
                        status.success(" Done.")

                    except Exception as e:
                        progress.empty()
                        status.error(" Tool error.")
                        st.error(f"Tool execution failed: {e}")

# --- Multiple Claims from Text ---
with tab2:
    st.markdown("Paste multiple claim scenarios (one per line):")
    default_multi = """CPT 99214 with diagnosis Z79.899 and modifier -25
CPT 93000 with diagnosis I10
CPT 99223 with diagnosis E11.9 and modifier -25"""
    multi_input = st.text_area("Multiple Claims", value=default_multi, height=160)

    if st.button("Run Batch Text Analysis"):
        claims = [line.strip() for line in multi_input.splitlines() if line.strip()]
        if not claims:
            st.warning("Please enter at least one claim.")
        else:
            success_count, fail_count = 0, 0
            batch_results = []
            progress = st.progress(0)
            status = st.empty()

            for i, claim_text in enumerate(claims):
                st.markdown(f"### Claim #{i + 1}")
                try:
                    parsed = generate_agent_decision(claim_text)
                    tool = {t.name: t for t in cms_tools}.get(parsed["action"])
                    result = tool.func(parsed["action_input"])
                    render_claim_analysis(parsed, result)
                    render_agent_trace(parsed, result)
                    render_retrieved_policy_docs(parsed["action"], result)
                    success_count += 1
                    batch_results.append({"claim": claim_text, "result": result})
                except Exception as e:
                    st.error(f"Error: {e}")
                    fail_count += 1
                    batch_results.append({"claim": claim_text, "error": str(e)})
                st.markdown("---")
                percent = int(100 * (i + 1) / len(claims))
                progress.progress(percent)
                status.text(f"Processed {i + 1}/{len(claims)}")

            status.empty()
            st.success(f"Finished {len(claims)} claims  {success_count} success, {fail_count} errors.")
            st.download_button(" Download Results", data=generate_download(batch_results), file_name="batch_results.json", mime="application/json")

# --- Upload CSV File ---
with tab3:
    uploaded_file = st.file_uploader("Upload CSV with 'claim_scenario' column", type="csv")
    if uploaded_file:
        df = pd.read_csv(uploaded_file)
        if "claim_scenario" not in df.columns:
            st.error("CSV must include 'claim_scenario' column.")
        else:
            st.success(f"Loaded {len(df)} claims.")
            if st.button("Run CSV Batch Analysis"):
                success_count, fail_count = 0, 0
                batch_results = []
                progress = st.progress(0)
                status = st.empty()

                for i, row in df.iterrows():
                    st.markdown(f"### Claim #{i + 1}")
                    try:
                        parsed = generate_agent_decision(row["claim_scenario"])
                        tool = {t.name: t for t in cms_tools}.get(parsed["action"])
                        result = tool.func(parsed["action_input"])
                        render_claim_analysis(parsed, result)
                        render_agent_trace(parsed, result)
                        render_retrieved_policy_docs(parsed["action"], result)
                        success_count += 1
                        batch_results.append({"claim": row["claim_scenario"], "result": result})
                    except Exception as e:
                        st.error(f"Error: {e}")
                        fail_count += 1
                        batch_results.append({"claim": row["claim_scenario"], "error": str(e)})
                    st.markdown("---")
                    percent = int(100 * (i + 1) / len(df))
                    progress.progress(percent)
                    status.text(f"Processed {i + 1}/{len(df)}")

                status.empty()
                st.success(f"Finished {len(df)} claims  {success_count} success, {fail_count} errors.")
                st.download_button(" Download Results", data=generate_download(batch_results), file_name="csv_claim_results.json", mime="application/json")
