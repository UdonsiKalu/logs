#!/usr/bin/env python3
import os
import torch
import streamlit as st
from rag_query_gpu1 import query_rag

# -------------------------
# Streamlit Configuration
# -------------------------
st.set_page_config(
    page_title="CMS RAG QA",
    page_icon="📘",
    layout="wide"
)

st.title("📘 CMS Policy Q&A - GPU Accelerated RAG")
st.caption("Ask CMS policy questions with citations using Qdrant, GPU embeddings, reranker, and Ollama.")

# -------------------------
# Environment Variables
# -------------------------
llm_backend = os.getenv("LLM_BACKEND", "ollama")
embedding_model = os.getenv("EMBEDDING_MODEL", "nomic-ai/nomic-embed-text-v1.5")
reranker_model = os.getenv("RERANKER_MODEL", "BAAI/bge-reranker-large")
collection = os.getenv("QDRANT_COLLECTION", "cms_policies")

with st.sidebar:
    st.header("⚙️ Settings")
    st.write(f"**LLM Backend:** {llm_backend}")
    st.write(f"**Embedding Model:** {embedding_model}")
    st.write(f"**Reranker Model:** {reranker_model}")
    st.write(f"**Qdrant Collection:** {collection}")
    st.write(f"**GPU Available:** {torch.cuda.is_available()}")

    st.markdown("---")
    st.write("### Retrieval Parameters")
    top_k = st.slider("Retriever Top K", 3, 32, 12, 1)
    top_r = st.slider("Reranker Top R", 3, 12, 6, 1)
    use_reranker = st.checkbox("Use GPU Reranker", value=True)

    st.markdown("---")
    filter_source = st.text_input("Filter by PDF filename (optional)", placeholder="e.g. clm104c32.pdf")

# -------------------------
# Ask a Question
# -------------------------
query = st.text_input(
    "Ask a CMS policy question:",
    placeholder="e.g., Who can supervise a pulmonary rehab session?"
)

if st.button("Search") and query.strip():
    st.info("Running RAG pipeline...")

    with st.spinner("Retrieving top matches and generating answer..."):
        # Run your RAG pipeline
        answer, docs = query_rag(
            query,
            filter_source=filter_source.strip() or None,
            use_reranker=use_reranker,
            top_k=top_k,
            top_r=top_r
        )

    # -------------------------
    # Display the Answer
    # -------------------------
    st.subheader("Answer")
    st.write(answer)

    # -------------------------
    # Display Top Retrieved Contexts
    # -------------------------
    st.subheader("Top Retrieved Contexts")
    for i, d in enumerate(docs, start=1):
        with st.expander(f"[{i}] {d['source']} (Page {d['page']}) | Score: {d['score']:.4f}"):
            st.write(d["text"])
            if d.get("path"):
                st.caption(f"File: {d['path']}")

else:
    st.markdown("💬 **Enter a question and click 'Search' to start.**")
