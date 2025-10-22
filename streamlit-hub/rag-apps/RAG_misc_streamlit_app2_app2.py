#!/usr/bin/env python3
import os
import torch
import streamlit as st
from rag_query_gpu2 import query_rag

# -------------------------
# Streamlit Page Config
# -------------------------
st.set_page_config(
    page_title="CMS RAG QA",
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
st.title("CMS Policy Question Answering")

# -------------------------
# Sidebar Controls
# -------------------------
llm_backend = os.getenv("LLM_BACKEND", "ollama")
embedding_model = os.getenv("EMBEDDING_MODEL", "nomic-ai/nomic-embed-text-v1.5")
reranker_model = os.getenv("RERANKER_MODEL", "BAAI/bge-reranker-large")
collection = os.getenv("QDRANT_COLLECTION", "cms_policies")

with st.sidebar:
    st.header("Settings")
    st.write(f"LLM Backend: {llm_backend}")
    st.write(f"Embedding Model: {embedding_model}")
    st.write(f"Reranker Model: {reranker_model}")
    st.write(f"Qdrant Collection: {collection}")
    st.write(f"GPU Available: {torch.cuda.is_available()}")

    st.markdown("---")
    top_k = st.slider("Retriever Top K", 3, 32, 12, 1)
    top_r = st.slider("Reranker Top R", 3, 12, 6, 1)
    use_reranker = st.checkbox("Enable Reranker", value=True)
    filter_source = st.text_input("Filter by PDF filename (optional)", placeholder="e.g. clm104c32.pdf")

# -------------------------
# Question Input
# -------------------------
query = st.text_input(
    "Enter your CMS policy question:",
    placeholder="Example: Who can supervise a pulmonary rehab session?"
)

# -------------------------
# Run the Query
# -------------------------
if st.button("Search") and query.strip():
    with st.spinner("Processing..."):
        answer, docs = query_rag(
            query,
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
    for i, d in enumerate(docs, start=1):
        with st.expander(f"{i}. {d['source']} (Page {d['page']}) | Score: {d['score']:.4f}"):
            st.write(f"<div style='font-size:12px; line-height:1.4;'>{d['text']}</div>", unsafe_allow_html=True)
            if d.get("path"):
                st.caption(f"File: {d['path']}")

else:
    st.markdown("<p style='font-size:13px;'>Enter a question and click 'Search' to start.</p>", unsafe_allow_html=True)
