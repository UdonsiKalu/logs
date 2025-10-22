import streamlit as st
import json
import time
import os
from faiss_gpu import CMSDenialAnalyzer

# --- Setup ---
st.set_page_config(page_title="CMS Denial Analyzer", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
    <style>
    html, body, [class*="css"] {
        font-family: Arial, sans-serif !important;
        font-size: 13px !important;
        line-height: 1.2 !important;
    }

    .block-container {
        padding-top: 0.5rem !important;
    }

    .stContainer {
        padding-top: 0.5rem !important;
        padding-bottom: 0.5rem !important;
    }

    h1, h2, h3, h4, h5, h6 {
        margin-bottom: 0.5rem !important;
    }

    .stTextInput, .stTextArea, .stButton, .stSelectbox, .stFileUploader {
        margin-bottom: 0.5rem !important;
    }
    </style>
""", unsafe_allow_html=True)


st.markdown("""
<h4 style='font-size:18px; margin-top:1.5rem; margin-bottom:0.5rem;'>CMS Denial Analyzer</h4>
""", unsafe_allow_html=True)


# --- Session State ---
if 'results' not in st.session_state:
    st.session_state.results = []

# --- Cache Analyzer ---
@st.cache_resource(show_spinner=False)
def load_analyzer():
    return CMSDenialAnalyzer()

analyzer = load_analyzer()

# --- UI Components ---
def show_sidebar():
    with st.sidebar:
        st.header("Configuration")
        st.info("""
        Processing Tips:
        - Use JSON format for claims
        - Required fields: cpt_code
        - Supports single or multiple claims
        """)
        
        if st.button("Clear Results"):
            st.session_state.results = []
            st.rerun()

def display_result(result):
    with st.container(border=True):
        if "error" in result:
            st.error(f"Error: {result['error']}")
            if "input_claim" in result:
                with st.expander("View Problematic Claim"):
                    st.json(result["input_claim"])
            return
        
        cols = st.columns([1,3])
        cols[0].metric("Risk Score", f"{result.get('risk_score', 'N/A')}")
        
        with cols[1].expander("Details", expanded=False):
            if 'potential_denial_reasons' in result:
                st.markdown("**Denial Reasons:**")
                for reason in result['potential_denial_reasons']:
                    st.markdown(f"- {reason}")
            
            if 'required_corrections' in result:
                st.markdown("**Required Actions:**")
                for action in result['required_corrections']:
                    st.markdown(f"- {action}")
            
            if 'appeal_excerpts' in result:
                st.markdown("**Policy References:**")
                st.text("\n".join(result['appeal_excerpts']))

# --- Main App ---
def main():
    #st.title("CMS Denial Analyzer")
    st.caption("Analyze claims via direct input or file upload")
    show_sidebar()

    # --- Tab Interface ---
    tab1, tab2 = st.tabs(["Direct Input", "File Upload"])
    
    with tab1:
        st.subheader("Paste Claim(s)")
        default_claims = '''[
  {
    "cpt_code": "99214",
    "diagnosis": "M54.5",
    "modifiers": ["59"],
    "payer": "Medicare"
  },
  {
    "cpt_code": "G0439",
    "diagnosis": "Z79.899",
    "payer": "Medicare"
  }
]'''
        claims_input = st.text_area("Input Claim(s) (JSON)", 
                                  value=default_claims, 
                                  height=200,
                                  help="Paste a single claim object or an array of claims")
        
        if st.button("Analyze Claims", type="primary"):
            try:
                input_data = json.loads(claims_input.strip())
                claims = input_data if isinstance(input_data, list) else [input_data]
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                batch_results = []
                
                for i, claim in enumerate(claims):
                    try:
                        if not isinstance(claim, dict) or "cpt_code" not in claim:
                            raise ValueError("Missing required 'cpt_code' field")
                        
                        with st.spinner(f"Analyzing CPT {claim['cpt_code']}..."):
                            result = analyzer.analyze_claim(claim)
                            batch_results.append(result)
                        
                        progress = (i + 1) / len(claims)
                        progress_bar.progress(progress)
                        status_text.text(f"Processed {i + 1}/{len(claims)} | Errors: {len([r for r in batch_results if 'error' in r])}")
                    
                    except Exception as e:
                        batch_results.append({
                            "input_claim": claim,
                            "error": str(e)
                        })
                
                st.session_state.results = batch_results + st.session_state.results
                progress_bar.empty()
                status_text.empty()
                
                st.success(f"Processed {len(batch_results)} claims")
                
                # Show summary
                cols = st.columns(3)
                cols[0].metric("Total", len(batch_results))
                cols[1].metric("Success", len([r for r in batch_results if 'error' not in r]))
                cols[2].metric("Errors", len([r for r in batch_results if 'error' in r]))
                
                # Display results
                for result in batch_results:
                    display_result(result)
                
            except json.JSONDecodeError:
                st.error("Invalid JSON format. Please check your input.")
                st.json({"Valid Example": json.loads(default_claims)})
            except Exception as e:
                st.error(f"Processing failed: {str(e)}")
    
    with tab2:
        st.subheader("Upload Claims File")
        uploaded_file = st.file_uploader("Choose file", 
                                      type=["json", "jsonl"], 
                                      accept_multiple_files=False,
                                      help="Upload JSON array or JSONL file")
        
        if uploaded_file:
            try:
                content = uploaded_file.getvalue().decode("utf-8")
                
                # Parse based on file type
                if uploaded_file.name.endswith(".jsonl"):
                    claims = [json.loads(line) for line in content.splitlines() if line.strip()]
                else:
                    file_data = json.loads(content)
                    claims = file_data if isinstance(file_data, list) else [file_data]
                
                st.success(f"Loaded {len(claims)} claims")
                
                if st.button("Process Uploaded File", type="primary"):
                    progress_bar = st.progress(0)
                    status = st.empty()
                    batch_results = []
                    
                    for i, claim in enumerate(claims):
                        try:
                            if not isinstance(claim, dict) or "cpt_code" not in claim:
                                raise ValueError("Missing cpt_code")
                                
                            result = analyzer.analyze_claim(claim)
                            batch_results.append(result)
                            
                        except Exception as e:
                            batch_results.append({
                                "input_claim": claim,
                                "error": str(e)
                            })
                        
                        # Update UI
                        progress = (i + 1) / len(claims)
                        progress_bar.progress(progress)
                        status.text(f"Processed {i + 1}/{len(claims)} | Errors: {len([r for r in batch_results if 'error' in r])}")
                    
                    st.session_state.results = batch_results + st.session_state.results
                    progress_bar.empty()
                    status.empty()
                    
                    st.success(f"Processed {len(batch_results)} claims with {len([r for r in batch_results if 'error' in r])} errors")
            
            except Exception as e:
                st.error(f"File error: {str(e)}")
        
    # --- Results Section ---
    if st.session_state.results:
        st.divider()
        st.subheader(f"All Results ({len(st.session_state.results)} total)")
        
        # Filter tabs
        tab_all, tab_success, tab_errors = st.tabs(["All", "Successes", "Errors"])
        
        with tab_all:
            for i, result in enumerate(st.session_state.results):
                st.markdown(f"#### Analysis #{i + 1}")
                display_result(result)
        
        with tab_success:
            successes = [r for r in st.session_state.results if 'error' not in r]
            for i, result in enumerate(successes):
                st.markdown(f"#### Success #{i + 1}")
                display_result(result)
        
        with tab_errors:
            errors = [r for r in st.session_state.results if 'error' in r]
            for i, result in enumerate(errors):
                st.markdown(f"#### Error #{i + 1}")
                display_result(result)
        
        # Export option
        st.download_button(
            label="Export All Results",
            data=json.dumps(st.session_state.results, indent=2),
            file_name=f"denial-analysis-{time.strftime('%Y%m%d-%H%M%S')}.json",
            mime="application/json"
        )

if __name__ == "__main__":
    main()