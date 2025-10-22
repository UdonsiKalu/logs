import streamlit as st
import pandas as pd
import plotly.express as px

# Load the entropy map CSV
@st.cache_data
def load_entropy_map(csv_file):
    df = pd.read_csv(csv_file)
    if 'chunk_id' not in df.columns or 'retrieval_count' not in df.columns:
        st.error("Invalid CSV: must contain 'chunk_id' and 'retrieval_count'")
        st.stop()
    return df

st.set_page_config(page_title="Retrieval Studio - Entropy Map", layout="wide")
st.title(" Retrieval Entropy Dashboard")

# File uploader
uploaded_file = st.file_uploader("Upload Entropy Map CSV", type="csv")
if uploaded_file:
    df = load_entropy_map(uploaded_file)

    # Filter options
    min_count = st.slider("Minimum Retrieval Count to Display", 0, int(df["retrieval_count"].max()), 0)
    df_filtered = df[df["retrieval_count"] >= min_count]

    # Extract source and page info from chunk_id if formatted as source::Page_x
    if "::" in df_filtered.iloc[0]["chunk_id"]:
        df_filtered["source"] = df_filtered["chunk_id"].apply(lambda x: x.split("::")[0])
        df_filtered["page"] = df_filtered["chunk_id"].apply(lambda x: x.split("::")[-1])
    else:
        df_filtered["source"] = "unknown"
        df_filtered["page"] = "0"

    # Bar chart: Retrieval frequency per chunk
    st.subheader("🔢 Retrieval Frequency by Chunk")
    fig = px.bar(df_filtered.sort_values("retrieval_count", ascending=False).head(50),
                 x="chunk_id", y="retrieval_count",
                 color="source",
                 labels={"retrieval_count": "Frequency", "chunk_id": "Chunk"},
                 title="Top Retrieved Chunks")
    st.plotly_chart(fig, use_container_width=True)

    # Heatmap: Source vs Page retrieval distribution
    st.subheader("🔥 Retrieval Density by Document Section")
    heat_df = df_filtered.copy()
    heat_df["page"] = heat_df["page"].str.extract(r'(\d+)').astype(float)
    heatmap = px.density_heatmap(heat_df, x="page", y="source",
                                 z="retrieval_count", nbinsx=30, color_continuous_scale="Viridis",
                                 labels={"retrieval_count": "Hits", "page": "Page", "source": "Document"})
    st.plotly_chart(heatmap, use_container_width=True)

    # Table view
    st.subheader("📋 Raw Entropy Table")
    st.dataframe(df_filtered.sort_values("retrieval_count", ascending=False), use_container_width=True)
else:
    st.info("⬆️ Upload an entropy map CSV to get started.")
