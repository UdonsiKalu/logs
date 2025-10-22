import streamlit as st
import json
import time

from faiss_gpu import CMSDenialAnalyzer

st.set_page_config(page_title="CMS Denial Analyzer", layout="wide")

st.title("🩺 CMS Denial Analyzer")
st.markdown("Paste a single claim or upload a batch file in `.json` or `.jsonl` format.")

# --- Cache the analyzer so FAISS loads only once ---
@st.cache_resource(show_spinner=" Initializing CMS Denial Analyzer...")
def get_analyzer():
    return CMSDenialAnalyzer()

analyzer = get_analyzer()


# --- Manual Input ---
with st.expander("➕ Paste a Single Claim (JSON)", expanded=True):
    default = '''{
  "cpt_code": "99213",
  "diagnosis": "E11.9",
  "modifiers": ["25"],
  "payer": "Medicare"
}'''
    claim_input = st.text_area("Claim Input", value=default, height=200)

# --- Display Helper ---
def display_result(result, output_placeholder):
    if isinstance(result, str):
        try:
            parsed = json.loads(result)
        except Exception:
            parsed = result
    else:
        parsed = result

    if isinstance(parsed, dict):
        output_placeholder.markdown(f"### 🧮 Risk Score: `{parsed.get('risk_score', 'N/A')}`")

        if 'potential_denial_reasons' in parsed:
            output_placeholder.markdown("**🚫 Potential Denial Reasons:**")
            for reason in parsed['potential_denial_reasons']:
                output_placeholder.markdown(f"- {reason}")

        if 'required_corrections' in parsed:
            output_placeholder.markdown("** Required Corrections:**")
            for correction in parsed['required_corrections']:
                output_placeholder.markdown(f"- {correction}")

        if 'appeal_excerpts' in parsed:
            output_placeholder.markdown("**📄 Appeal Excerpts:**")
            for excerpt in parsed['appeal_excerpts']:
                output_placeholder.markdown(f"- {excerpt}")
    else:
        output_placeholder.text(str(parsed))


# --- Manual Analyze Button ---
if st.button("Analyze"):
    try:
        data = json.loads(claim_input.strip())
        output_placeholder = st.empty()
        status = st.empty()
        start = time.time()

        if isinstance(data, list):
            total = len(data)
            progress_bar = st.progress(0)

            for i, claim in enumerate(data, start=1):
                result = analyzer.analyze_claim(claim)
                output_placeholder.markdown(f"### Claim {i}")
                display_result(result, output_placeholder)
                progress_bar.progress(i / total)
                status.text(f" Processing claim {i}/{total}...")
                time.sleep(0.001)

            elapsed = time.time() - start
            progress_bar.empty()
            status.success(f" Batch processed in {elapsed:.2f} seconds.")

        else:
            result = analyzer.analyze_claim(data)
            elapsed = time.time() - start
            status.success(f" Analyzed in {elapsed:.2f} seconds.")
            display_result(result, st)

    except Exception as e:
        st.error(f"Error: {e}")

# --- File Upload ---
st.markdown("---")
uploaded_file = st.file_uploader(" Or Upload Batch File (.json or .jsonl)", type=["json", "jsonl"])

if uploaded_file:
    try:
        content = uploaded_file.read().decode("utf-8")
        if uploaded_file.name.endswith(".jsonl"):
            claims = [json.loads(line) for line in content.splitlines()]
        else:
            claims = json.loads(content)

        output_placeholder = st.empty()
        total = len(claims)
        progress_bar = st.progress(0)
        status = st.empty()
        start = time.time()

        for i, claim in enumerate(claims, start=1):
            result = analyzer.analyze_claim(claim)
            output_placeholder.markdown(f"### Claim {i}")
            display_result(result, output_placeholder)
            progress_bar.progress(i / total)
            status.text(f" Processing claim {i}/{total}...")
            time.sleep(0.001)

        elapsed = time.time() - start
        progress_bar.empty()
        status.success(f" File processed in {elapsed:.2f} seconds.")

    except Exception as e:
        st.error(f"Error processing file: {e}")
