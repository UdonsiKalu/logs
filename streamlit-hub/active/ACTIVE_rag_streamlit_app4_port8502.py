#!/usr/bin/env python3
import os
import torch
import streamlit as st
from rag_query_gpu4 import query_rag, get_available_collections

# -------------------------
# Streamlit Page Config
# -------------------------
st.set_page_config(
    page_title="CMS Multi-RAG QA Hub",
    layout="wide"
)

# -------------------------
# Custom CSS for Minimalist Design
# -------------------------
st.markdown("""
<style>
/* Overall font size */
html, body, [class*="css"] {
    font-size: 14px;
    color: #222;
    font-family: "Inter", sans-serif;
}

/* Main title */
h1 {
    font-size: 20px !important;
    font-weight: 500;
    margin-bottom: 0.5rem;
}

/* Section headers */
h2, h3, h4 {
    font-size: 16px !important;
    font-weight: 500;
    margin-top: 0.75rem;
    margin-bottom: 0.25rem;
}

/* Sidebar */
.sidebar .sidebar-content {
    background-color: #f8f8f8;
    padding: 1rem;
}

/* Input boxes */
.stTextInput>div>div>input {
    font-size: 14px;
    padding: 0.35rem;
}

/* Button styling */
.stButton>button {
    background-color: #f2f2f2;
    color: #222;
    font-size: 14px;
    border: 1px solid #ccc;
    border-radius: 4px;
    padding: 0.3rem 0.75rem;
}

.stButton>button:hover {
    border-color: #999;
}

/* Expander section */
.streamlit-expanderHeader {
    font-size: 13px !important;
    font-weight: 400;
}
</style>
""", unsafe_allow_html=True)

# -------------------------
# Title
# -------------------------
st.title("CMS Multi-RAG Policy Hub")

# -------------------------
# Get Available Collections
# -------------------------
COLLECTIONS = get_available_collections()

# -------------------------
# Sidebar Controls
# -------------------------
llm_backend = os.getenv("LLM_BACKEND", "ollama")
embedding_model = os.getenv("EMBEDDING_MODEL", "nomic-ai/nomic-embed-text-v1.5")
reranker_model = os.getenv("RERANKER_MODEL", "BAAI/bge-reranker-large")

with st.sidebar:
    st.header("Knowledge Base Selection")
    
    # Collection dropdown
    selected_collection_name = st.selectbox(
        "Select Knowledge Base:",
        options=list(COLLECTIONS.keys()),
        index=0
    )
    
    selected_collection = COLLECTIONS[selected_collection_name]
    
    st.markdown("---")
    st.header("Search Settings")
    
    top_k = st.slider("Retriever Top K", 3, 32, 12, 1)
    top_r = st.slider("Reranker Top R", 3, 12, 6, 1)
    use_reranker = st.checkbox("Enable Reranker", value=True)
    filter_source = st.text_input("Filter by PDF filename (optional)", placeholder="e.g. clm104c32.pdf")
    
    st.markdown("---")
    st.header("System Info")
    st.write(f"LLM Backend: {llm_backend}")
    st.write(f"Embedding Model: {embedding_model}")
    st.write(f"Reranker Model: {reranker_model}")
    st.write(f"Selected Collection: {selected_collection}")
    st.write(f"GPU Available: {torch.cuda.is_available()}")

# -------------------------
# Example Questions per Collection
# -------------------------
EXAMPLE_QUESTIONS = {
    'CMS Policies': [
        'Who can supervise a pulmonary rehab session?',
        'What are modifiers for bilateral procedures?',
        'What are the coverage requirements for telehealth services?'
    ],
    'Medicare Managed Care': [
        'What are the enrollment requirements for Medicare Advantage?',
        'How are prior authorization rules different in managed care?',
        'What are the appeals process timelines for Medicare Advantage?'
    ],
    'NCCI Procedure Edits': [
        'What are the NCCI edits for CPT 99214 and 93000?',
        'How do I bill for multiple procedures on the same day?',
        'What are the modifier rules for NCCI edits?'
    ],
    'MIPS Policy': [
        'What are the reporting requirements for MIPS in 2024?',
        'How is the MIPS final score calculated?',
        'What are the quality measures for primary care in MIPS?'
    ],
    'Physician Fee Schedule': [
        'What is the conversion factor for 2024?',
        'How are RVUs calculated for new procedures?',
        'What are the geographic practice cost indices?'
    ],
    'LCD Policies': [
        'What LCD covers knee arthroplasty?',
        'What documentation is required per LCD for CT abdomen?',
        'What are the coverage limitations for cardiac monitoring?'
    ]
}

# -------------------------
# Question Input
# -------------------------
st.subheader(f"Ask a Question about {selected_collection_name}")

query = st.text_input(
    "Enter your question:",
    placeholder=f"Example: {EXAMPLE_QUESTIONS.get(selected_collection_name, [''])[0]}"
)

# Show example questions for selected collection
with st.expander("Example Questions"):
    examples = EXAMPLE_QUESTIONS.get(selected_collection_name, [])
    if examples:
        for example in examples:
            st.write(f"• {example}")
    else:
        st.write("No example questions available for this collection.")

# -------------------------
# Run the Query
# -------------------------
if st.button("Search") and query.strip():
    with st.spinner(f"Searching {selected_collection_name}..."):
        answer, docs = query_rag(
            collection=selected_collection,
            user_query=query,
            filter_source=filter_source.strip() or None,
            use_reranker=use_reranker,
            top_k=top_k,
            top_r=top_r
        )

    # -------------------------
    # Display Answer
    # -------------------------
    st.subheader("Answer")
    st.write(f"<div style='font-size:13px; line-height:1.5;'>{answer}</div>", unsafe_allow_html=True)

    # -------------------------
    # Display Context
    # -------------------------
    st.subheader("Retrieved Contexts")
    if docs:
        for i, d in enumerate(docs, start=1):
            with st.expander(f"{i}. {d['source']} (Page {d['page']}) | Score: {d['score']:.4f}"):
                st.write(f"<div style='font-size:12px; line-height:1.4;'>{d['text']}</div>", unsafe_allow_html=True)
                if d.get("path"):
                    st.caption(f"File: {d['path']}")
    else:
        st.info("No contexts retrieved for this query.")

else:
    st.markdown("<p style='font-size:13px;'>Select a knowledge base, enter a question and click 'Search' to start.</p>", unsafe_allow_html=True)

# -------------------------
# Footer with Collection Info
# -------------------------
st.markdown("---")
st.markdown(f"**Current Knowledge Base:** {selected_collection_name} (`{selected_collection}`)")