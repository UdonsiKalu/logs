#!/usr/bin/env python3
"""
Streamlit Hub Launcher
======================
Unified interface to launch any Streamlit application from the organized hub.
"""

import os
import sys
import subprocess
import streamlit as st
from pathlib import Path

# Configuration
HUB_DIR = Path(__file__).parent.parent
BASE_DIR = Path("/media/udonsi-kalu/New Volume/denials")

# App categories and their descriptions
APP_CATEGORIES = {
    "active": {
        "name": "Active Apps",
        "description": "Currently running applications",
        "color": "#00ff00"
    },
    "claim-analysis": {
        "name": "Claim Analysis",
        "description": "Complete claim analysis applications",
        "color": "#0066cc"
    },
    "rag-apps": {
        "name": "RAG Applications", 
        "description": "Retrieval-Augmented Generation apps",
        "color": "#ff6600"
    },
    "denial-apps": {
        "name": "Denial Risk Assessment",
        "description": "Medicare denial risk assessment tools",
        "color": "#cc0000"
    },
    "claim-corrector": {
        "name": "Claim Corrector",
        "description": "Claim correction and validation tools",
        "color": "#9900cc"
    },
    "new-claim-analyzer": {
        "name": "New Claim Analyzer",
        "description": "New claim analysis and processing tools",
        "color": "#00cc99"
    },
    "archive": {
        "name": "Archive",
        "description": "Backup and archived versions",
        "color": "#666666"
    }
}

def get_apps_in_category(category):
    """Get all apps in a specific category."""
    category_dir = HUB_DIR / category
    if not category_dir.exists():
        return []
    
    apps = []
    for file_path in category_dir.glob("*.py"):
        if file_path.is_symlink():
            # Get the original file path
            original_path = file_path.resolve()
            apps.append({
                "name": file_path.stem,
                "path": str(original_path),
                "display_name": file_path.stem.replace("_", " ").title(),
                "category": category
            })
    return sorted(apps, key=lambda x: x["name"])

def launch_app(app_path, port=None):
    """Launch a Streamlit app on a specific port."""
    try:
        # Find available port if not specified
        if port is None:
            port = 8501
            while is_port_in_use(port):
                port += 1
        
        # Launch the app
        cmd = ["streamlit", "run", app_path, "--server.port", str(port)]
        subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return port
    except Exception as e:
        st.error(f"Failed to launch app: {e}")
        return None

def is_port_in_use(port):
    """Check if a port is in use."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

def main():
    st.set_page_config(
        page_title="Streamlit Hub Launcher",
        page_icon="",
        layout="wide"
    )
    
    st.title("Streamlit Hub Launcher")
    st.markdown("**Unified interface to launch any Streamlit application**")
    st.markdown("---")
    
    # Sidebar for category selection
    st.sidebar.title("Categories")
    selected_category = st.sidebar.selectbox(
        "Select a category:",
        list(APP_CATEGORIES.keys()),
        format_func=lambda x: APP_CATEGORIES[x]["name"]
    )
    
    # Display category info
    category_info = APP_CATEGORIES[selected_category]
    st.sidebar.markdown(f"**{category_info['name']}**")
    st.sidebar.markdown(f"*{category_info['description']}*")
    
    # Get apps in selected category
    apps = get_apps_in_category(selected_category)
    
    if not apps:
        st.warning(f"No apps found in {category_info['name']}")
        return
    
    # Display apps in a grid
    st.subheader(f"{category_info['name']} ({len(apps)} apps)")
    
    # Create columns for app cards
    cols = st.columns(3)
    
    for i, app in enumerate(apps):
        col = cols[i % 3]
        
        with col:
            # Create app card
            st.markdown(f"""
            <div style="
                border: 1px solid #ddd;
                border-radius: 8px;
                padding: 15px;
                margin: 10px 0;
                background-color: #f9f9f9;
            ">
                <h4 style="margin: 0 0 10px 0; color: {category_info['color']};">{app['display_name']}</h4>
                <p style="margin: 0 0 10px 0; font-size: 12px; color: #666;">
                    {app['path']}
                </p>
            </div>
            """, unsafe_allow_html=True)
            
            # Launch button
            if st.button(f"Launch", key=f"launch_{i}"):
                with st.spinner(f"Launching {app['display_name']}..."):
                    port = launch_app(app['path'])
                    if port:
                        st.success(f"App launched on port {port}")
                        st.markdown(f"**URL:** http://localhost:{port}")
                    else:
                        st.error("Failed to launch app")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #666; font-size: 12px;">
        Streamlit Hub Launcher | All apps maintain their original dependencies
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
