#!/bin/bash
# Streamlit Hub Creator - Safe Aggregation with Smart Naming
# Creates symbolic links and organized structure without breaking dependencies

HUB_DIR="/home/udonsi-kalu/workspace/streamlit-hub"
BASE_DIR="/media/udonsi-kalu/New Volume/denials"

echo " Creating Streamlit Hub with Smart Naming System..."
echo "=================================================="

# Create main hub directory structure
mkdir -p "$HUB_DIR"/{active,claim-analysis,rag-apps,denial-apps,claim-corrector,new-claim-analyzer,archive,launchers}

echo " Created directory structure:"
echo "   active/           - Currently running apps"
echo "   claim-analysis/   - Complete claim analysis apps"
echo "   rag-apps/         - RAG-based applications"
echo "   denial-apps/      - Denial risk assessment apps"
echo "   claim-corrector/  - Claim correction tools"
echo "   new-claim-analyzer/ - New claim analysis tools"
echo "   archive/          - Archived/backup versions"
echo "   launchers/        - Unified launcher scripts"
echo ""

# Function to create smart symlink with descriptive name
create_smart_link() {
    local source_file="$1"
    local target_dir="$2"
    local prefix="$3"
    local suffix="$4"
    
    if [ -f "$source_file" ]; then
        local filename=$(basename "$source_file")
        local dirname=$(basename "$(dirname "$source_file")")
        local parent_dir=$(basename "$(dirname "$(dirname "$source_file")")")
        
        # Create descriptive name
        local new_name="${prefix}_${dirname}_${filename%.py}"
        if [ -n "$suffix" ]; then
            new_name="${new_name}_${suffix}"
        fi
        new_name="${new_name}.py"
        
        # Create symlink
        ln -sf "$source_file" "$target_dir/$new_name"
        echo "    $new_name -> $(basename "$source_file")"
    fi
}

echo "🔗 Creating symbolic links with smart naming..."
echo ""

# === ACTIVE APPS (Currently Running) ===
echo " Active Apps (Currently Running):"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/complete_claim_analysis_app_cgpt3_update7.py" "$HUB_DIR/active" "ACTIVE" "port8509"
create_smart_link "$BASE_DIR/cms/manuals/rag/streamlit_app4.py" "$HUB_DIR/active" "ACTIVE" "port8502"
echo ""

# === CLAIM ANALYSIS APPS ===
echo " Claim Analysis Apps:"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/complete_claim_analysis_app_cgpt3_update8.py" "$HUB_DIR/claim-analysis" "CLAIM_ANALYSIS" "update8"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/complete_claim_analysis_app_cgpt3_update7.py" "$HUB_DIR/claim-analysis" "CLAIM_ANALYSIS" "update7"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/complete_claim_analysis_app_cgpt3_update6.py" "$HUB_DIR/claim-analysis" "CLAIM_ANALYSIS" "update6"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/complete_claim_analysis_app_cgpt3_update5.py" "$HUB_DIR/claim-analysis" "CLAIM_ANALYSIS" "update5"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/complete_claim_analysis_app_cgpt3_update4.py" "$HUB_DIR/claim-analysis" "CLAIM_ANALYSIS" "update4"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/complete_claim_analysis_app_cgpt3.py" "$HUB_DIR/claim-analysis" "CLAIM_ANALYSIS" "update3"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/complete_claim_analysis_app_v2.py" "$HUB_DIR/claim-analysis" "CLAIM_ANALYSIS" "v2"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/complete_claim_analysis_app.py" "$HUB_DIR/claim-analysis" "CLAIM_ANALYSIS" "v1"
echo ""

# === RAG APPS ===
echo "🤖 RAG Applications:"
create_smart_link "$BASE_DIR/cms/manuals/rag/streamlit_app4.py" "$HUB_DIR/rag-apps" "RAG" "app4"
create_smart_link "$BASE_DIR/cms/manuals/rag/misc/streamlit_app3.py" "$HUB_DIR/rag-apps" "RAG" "app3"
create_smart_link "$BASE_DIR/cms/manuals/rag/misc/streamlit_app2.py" "$HUB_DIR/rag-apps" "RAG" "app2"
create_smart_link "$BASE_DIR/cms/manuals/rag/misc/streamlit_app.py" "$HUB_DIR/rag-apps" "RAG" "app1"
create_smart_link "$BASE_DIR/cms/claim_analysis_tools/streamlit_app.py" "$HUB_DIR/rag-apps" "RAG" "cms_tools"
create_smart_link "$BASE_DIR/cms/streamlit_app.py" "$HUB_DIR/rag-apps" "RAG" "cms_main"
echo ""

# === DENIAL APPS ===
echo "  Denial Risk Assessment Apps:"
create_smart_link "$BASE_DIR/denials/streamlit_app.py" "$HUB_DIR/denial-apps" "DENIAL" "main"
create_smart_link "$BASE_DIR/denials/app.py" "$HUB_DIR/denial-apps" "DENIAL" "agentic"
create_smart_link "$BASE_DIR/denials/streamlit_entropy.py" "$HUB_DIR/denial-apps" "DENIAL" "entropy"
create_smart_link "$BASE_DIR/denials_agentic/streamlit_app.py" "$HUB_DIR/denial-apps" "DENIAL" "agentic_main"
create_smart_link "$BASE_DIR/input_clean/input/streamlit_app.py" "$HUB_DIR/denial-apps" "DENIAL" "input_clean"
create_smart_link "$BASE_DIR/git/rag_portfolio/projects/1-policy-denial-agent/streamlit_app.py" "$HUB_DIR/denial-apps" "DENIAL" "portfolio"
echo ""

# === CLAIM CORRECTOR APPS ===
echo "🔧 Claim Corrector Apps:"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/claim_corrector_claims3_archetype_driven_update10.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "update10"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/claim_corrector_claims3_archetype_driven_update9.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "update9"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/claim_corrector_claims.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "main"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/claim_corrector_claims3_archetype_driven_update6.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "update6"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/claim_corrector_claims3_archetype_driven_update5.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "update5"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/claim_corrector_claims3_archetype_driven_update4.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "update4"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/claim_corrector_claims3_archetype_driven_update3.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "update3"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/claim_corrector_claims3_archetype_driven_update2.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "update2"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/claim_corrector_claims3_archetype_driven_update1.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "update1"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/claim_corrector_claims3_archetype_driven_v3.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "v3"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/claim_corrector_claims3_archetype_driven_v2.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "v2"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/claim_corrector_claims3_archetype_driven.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "v1"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/claim_corrector_claims3_two_stage_calibrated.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "two_stage"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/claim_corrector_claims3_calibrated.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "calibrated"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/claim_corrector_claims3_two_stage.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "two_stage_v1"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/claim_corrector_claims3.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "v3_main"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/claim_corrector_claims2.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "v2_main"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/claim_corrector3.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "v3_legacy"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/claim_corrector2.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "v2_legacy"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/claim_corrector_prompt.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "prompt"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/claim_corrector.py" "$HUB_DIR/claim-corrector" "CORRECTOR" "legacy"
echo ""

# === NEW CLAIM ANALYZER APPS ===
echo "🆕 New Claim Analyzer Apps:"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/new_claim_analyzer1.py" "$HUB_DIR/new-claim-analyzer" "NEW_ANALYZER" "v1"
create_smart_link "$BASE_DIR/cms/claim_analysis_tools/new_claim_analyzer.py" "$HUB_DIR/new-claim-analyzer" "NEW_ANALYZER" "cms_tools"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/new_claim_analyzer.py" "$HUB_DIR/new-claim-analyzer" "NEW_ANALYZER" "archive"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/new_claim_analyzer_bckup_768.py" "$HUB_DIR/new-claim-analyzer" "NEW_ANALYZER" "backup_768"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/new_claim_analyzer_bckup2.py" "$HUB_DIR/new-claim-analyzer" "NEW_ANALYZER" "backup2"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/new_claim_analyzer_bckup.py" "$HUB_DIR/new-claim-analyzer" "NEW_ANALYZER" "backup1"
echo ""

# === COPY/BACKUP FILES ===
echo "📋 Copy/Backup Files:"
create_smart_link "$BASE_DIR/denials_agentic/misc/streamlit_app (Copy).py" "$HUB_DIR/archive" "COPY" "agentic_misc"
create_smart_link "$BASE_DIR/denials/misc/streamlit_app (Copy).py" "$HUB_DIR/archive" "COPY" "denials_misc"
create_smart_link "$BASE_DIR/denials_agentic/misc/streamlit_app (Copy)2.py" "$HUB_DIR/archive" "COPY" "agentic_misc2"
create_smart_link "$BASE_DIR/denials/misc/streamlit_app (Copy)2.py" "$HUB_DIR/archive" "COPY" "denials_misc2"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/misc/streamlit_app.py" "$HUB_DIR/archive" "COPY" "claim_tools_misc"
create_smart_link "$BASE_DIR/cms/manuals/Raw Data/claim_analysis_tools/archive_20251011_161233/misc/streamlit_app.py" "$HUB_DIR/archive" "COPY" "claim_tools_misc2"
echo ""

echo " Streamlit Hub created successfully!"
echo ""
echo " Summary:"
echo "   Active Apps: $(ls -1 "$HUB_DIR/active" | wc -l)"
echo "   Claim Analysis: $(ls -1 "$HUB_DIR/claim-analysis" | wc -l)"
echo "   RAG Apps: $(ls -1 "$HUB_DIR/rag-apps" | wc -l)"
echo "   Denial Apps: $(ls -1 "$HUB_DIR/denial-apps" | wc -l)"
echo "   Claim Corrector: $(ls -1 "$HUB_DIR/claim-corrector" | wc -l)"
echo "   New Claim Analyzer: $(ls -1 "$HUB_DIR/new-claim-analyzer" | wc -l)"
echo "   Archive: $(ls -1 "$HUB_DIR/archive" | wc -l)"
echo ""
echo " Next: Run the launcher script to see all apps organized!"

