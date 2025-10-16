# Streamlit Hub

**Unified interface for all your Streamlit applications**

## Organization

Your Streamlit apps are organized into categories:

- **Active Apps** - Currently running applications (ports 8509, 8502)
- **Claim Analysis** - Complete claim analysis applications  
- **RAG Apps** - Retrieval-Augmented Generation applications
- **Denial Apps** - Medicare denial risk assessment tools
- **Claim Corrector** - Claim correction and validation tools
- **New Claim Analyzer** - New claim analysis and processing tools
- **Archive** - Backup and archived versions

## Smart Naming System

Each file has a descriptive name that includes:
- **Category prefix** (ACTIVE_, RAG_, DENIAL_, etc.)
- **Source directory** (claim_analysis_tools, rag, denials, etc.)
- **Original filename** (streamlit_app4, complete_claim_analysis_app, etc.)
- **Version/type suffix** (update7, port8509, v1, etc.)

**Example:** `ACTIVE_claim_analysis_tools_complete_claim_analysis_app_cgpt3_update7_port8509.py`

## Usage

### Command Line Launcher

```bash
# List all available apps
./launchers/launch_app.sh list

# Launch a specific app
./launchers/launch_app.sh launch ACTIVE_rag_streamlit_app4_port8502.py

# Launch with custom port
./launchers/launch_app.sh launch RAG_app4.py 8503

# Open web launcher
./launchers/launch_app.sh gui
```

### Web Launcher

```bash
# Open the web interface
streamlit run launchers/streamlit_launcher.py --server.port 8500
```

Then visit: http://localhost:8500

## Benefits

- **No broken dependencies** - All apps maintain their original file locations
- **Easy organization** - Find any app by category and function
- **Clear naming** - No more confusion about similar filenames
- **Unified access** - Launch any app from one place
- **Preserved functionality** - Everything works exactly as before

## Technical Details

- Uses **symbolic links** to preserve original file locations
- **No file copying** - Original files remain untouched
- **Dependency preservation** - All imports and relative paths work
- **Port management** - Automatic port detection and assignment

## Summary

- **Total Apps:** 55+ Streamlit applications
- **Active Apps:** 2 (currently running)
- **Categories:** 7 organized groups
- **Zero Breaking Changes** - All apps work as before!

---

*Created to solve the "too many similar filenames" problem*
