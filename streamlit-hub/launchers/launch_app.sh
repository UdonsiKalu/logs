#!/bin/bash
# Command-line Streamlit App Launcher

HUB_DIR="/home/udonsi-kalu/workspace/streamlit-hub"

echo "Streamlit Hub - Command Line Launcher"
echo "====================================="
echo ""

# Function to list apps in a category
list_apps() {
    local category="$1"
    local category_dir="$HUB_DIR/$category"
    
    if [ -d "$category_dir" ]; then
        echo "Category: $category"
        ls -la "$category_dir"/*.py 2>/dev/null | while read -r line; do
            if [[ $line == *"->"* ]]; then
                local filename=$(echo "$line" | awk '{print $9}' | xargs basename)
                local target=$(echo "$line" | awk '{print $11}')
                echo "   $filename -> $(basename "$target" 2>/dev/null || echo "Unknown")"
            fi
        done
        echo ""
    fi
}

# Function to launch an app
launch_app() {
    local app_name="$1"
    local port="$2"
    
    # Find the app in any category
    local app_path=""
    for category in active claim-analysis rag-apps denial-apps claim-corrector new-claim-analyzer archive; do
        if [ -f "$HUB_DIR/$category/$app_name" ]; then
            app_path="$HUB_DIR/$category/$app_name"
            break
        fi
    done
    
    if [ -z "$app_path" ]; then
        echo "ERROR: App '$app_name' not found!"
        echo "Available apps:"
        for category in active claim-analysis rag-apps denial-apps claim-corrector new-claim-analyzer archive; do
            list_apps "$category"
        done
        return 1
    fi
    
    # Find available port if not specified
    if [ -z "$port" ]; then
        port=8501
        while netstat -tlnp 2>/dev/null | grep -q ":$port "; do
            port=$((port + 1))
        done
    fi
    
    echo "Launching $app_name on port $port..."
    echo "Path: $app_path"
    echo "URL: http://localhost:$port"
    echo ""
    
    # Launch the app
    streamlit run "$app_path" --server.port "$port"
}

# Main menu
case "$1" in
    "list"|"ls")
        echo "Available Streamlit Apps:"
        echo ""
        for category in active claim-analysis rag-apps denial-apps claim-corrector new-claim-analyzer archive; do
            list_apps "$category"
        done
        ;;
    "launch"|"run")
        if [ -z "$2" ]; then
            echo "Usage: $0 launch <app_name> [port]"
            echo "Example: $0 launch ACTIVE_claim_analysis_tools_complete_claim_analysis_app_cgpt3_update7_port8509.py"
            echo ""
            echo "Run '$0 list' to see available apps"
            exit 1
        fi
        launch_app "$2" "$3"
        ;;
    "gui"|"web")
        echo "Opening web launcher..."
        streamlit run "$HUB_DIR/launchers/streamlit_launcher.py" --server.port 8500
        ;;
    *)
        echo "Streamlit Hub Launcher"
        echo ""
        echo "Usage:"
        echo "  $0 list                    - List all available apps"
        echo "  $0 launch <app> [port]     - Launch specific app"
        echo "  $0 gui                     - Open web launcher"
        echo ""
        echo "Examples:"
        echo "  $0 list"
        echo "  $0 launch ACTIVE_claim_analysis_tools_complete_claim_analysis_app_cgpt3_update7_port8509.py"
        echo "  $0 launch RAG_app4.py 8503"
        echo "  $0 gui"
        echo ""
        echo "Quick access to active apps:"
        echo "  $0 launch ACTIVE_claim_analysis_tools_complete_claim_analysis_app_cgpt3_update7_port8509.py"
        echo "  $0 launch ACTIVE_rag_streamlit_app4_port8502.py"
        ;;
esac
