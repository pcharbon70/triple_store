#!/bin/bash
# Run All Examples
#
# This script runs all example query scripts against the Ash Framework RDF data.
# It automatically handles data loading if needed.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m' # No Color

DATA_DIR="./tmp/ash_data"
DATA_FILE="examples/ash.ttl"

# List of examples to run (in logical order)
EXAMPLES=(
    "hub_modules.exs:Hub Modules:Core architectural components"
    "entry_points.exs:Entry Points:Edge modules with few dependencies"
    "module_clusters.exs:Module Clusters:Domain organization by namespace"
    "api_surface.exs:API Surface:Public function counts per module"
    "type_usage.exs:Type Usage:Type definitions across codebase"
    "complexity.exs:Complexity:Modules with most outgoing dependencies"
    "error_patterns.exs:Error Patterns:Error module hierarchy"
    "call_graph_query.exs:Call Graph:Function call relationships"
    "impact_analysis.exs:Impact Analysis:Change impact assessment"
)

print_header() {
    echo ""
    echo -e "${BLUE}${BOLD}======================================================================${NC}"
    echo -e "${BLUE}${BOLD}  $1${NC}"
    echo -e "${BLUE}${BOLD}======================================================================${NC}"
    echo ""
}

print_step() {
    echo -e "${CYAN}>>>${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}!${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${DIM}  $1${NC}"
}

# Check if data is loaded by testing for RocksDB files in the data directory
check_data_loaded() {
    if [ ! -d "$DATA_DIR" ]; then
        return 1
    fi

    # Check for RocksDB SST files which indicate data has been written
    local sst_count
    sst_count=$(find "$DATA_DIR" -name "*.sst" 2>/dev/null | wc -l)

    if [ "$sst_count" -gt 0 ]; then
        return 0
    fi

    # Fallback: check for any substantial files
    local file_count
    file_count=$(find "$DATA_DIR" -type f 2>/dev/null | wc -l)

    if [ "$file_count" -gt 5 ]; then
        return 0
    fi

    return 1
}

# Load the RDF data into the store
load_data() {
    print_step "Loading Ash Framework RDF data..."
    echo ""

    # Show file info
    local lines
    lines=$(wc -l < "$DATA_FILE")
    print_info "Source: $DATA_FILE (~$lines lines)"
    print_info "Target: $DATA_DIR"
    echo ""

    # Run the loader with progress indication
    mix run -e '
        IO.write("  Opening store... ")
        {:ok, store} = TripleStore.open("./tmp/ash_data")
        IO.puts("done")

        IO.write("  Loading triples (this may take a moment)... ")
        start = System.monotonic_time(:millisecond)
        {:ok, count} = TripleStore.load(store, "examples/ash.ttl")
        elapsed = System.monotonic_time(:millisecond) - start
        IO.puts("done")

        IO.puts("")
        IO.puts("  Loaded #{count} triples in #{elapsed}ms")

        TripleStore.close(store)
    '

    echo ""
    print_success "Data loaded successfully!"
}

# Prompt user for data loading
prompt_load_data() {
    echo ""
    print_warning "The example data hasn't been loaded yet."
    echo ""
    print_info "These examples analyze the Ash Framework codebase represented as RDF."
    print_info "The data file contains ~437,000 triples describing modules, functions,"
    print_info "types, and call relationships."
    echo ""

    # Check if we're in interactive mode
    if [ -t 0 ]; then
        echo -en "${CYAN}>>>${NC} Load the data now? ${DIM}[Y/n]${NC} "
        read -r response
        case "$response" in
            [nN]|[nN][oO])
                echo ""
                print_info "You can load the data manually with:"
                echo ""
                echo "    ./examples/run_all.sh --load"
                echo ""
                exit 0
                ;;
            *)
                echo ""
                load_data
                ;;
        esac
    else
        # Non-interactive mode
        print_error "Run with --load flag to load data in non-interactive mode:"
        echo ""
        echo "    ./examples/run_all.sh --load"
        echo ""
        exit 1
    fi
}

run_example() {
    local script="$1"
    local title="$2"
    local description="$3"

    echo ""
    echo -e "${GREEN}----------------------------------------------------------------------${NC}"
    echo -e "${GREEN}  ${BOLD}${title}${NC}"
    echo -e "${GREEN}  ${DIM}${description}${NC}"
    echo -e "${GREEN}----------------------------------------------------------------------${NC}"
    echo ""

    if mix run "examples/${script}" 2>&1; then
        return 0
    else
        print_error "Script failed: ${script}"
        return 1
    fi
}

show_help() {
    echo -e "${BOLD}Codebase Insight Examples${NC}"
    echo ""
    echo "Run SPARQL queries against the Ash Framework codebase represented as RDF."
    echo ""
    echo -e "${BOLD}USAGE${NC}"
    echo "    ./examples/run_all.sh [OPTIONS] [SCRIPT]"
    echo ""
    echo -e "${BOLD}OPTIONS${NC}"
    echo "    -l, --list      List available examples"
    echo "    --load          Load/reload the RDF data"
    echo "    --status        Check if data is loaded"
    echo "    -h, --help      Show this help"
    echo ""
    echo -e "${BOLD}EXAMPLES${NC}"
    echo "    ./examples/run_all.sh                  Run all examples"
    echo "    ./examples/run_all.sh hub_modules.exs  Run specific example"
    echo "    ./examples/run_all.sh --list           List available examples"
    echo "    ./examples/run_all.sh --load           Load the RDF data"
    echo ""
    echo -e "${BOLD}AVAILABLE SCRIPTS${NC}"
    for entry in "${EXAMPLES[@]}"; do
        IFS=':' read -r script title desc <<< "$entry"
        printf "    ${CYAN}%-24s${NC} %s\n" "$script" "$desc"
    done
    echo ""
}

show_list() {
    echo ""
    echo -e "${BOLD}Available Examples${NC}"
    echo ""
    for entry in "${EXAMPLES[@]}"; do
        IFS=':' read -r script title desc <<< "$entry"
        echo -e "  ${CYAN}${script}${NC}"
        echo -e "    ${title}: ${DIM}${desc}${NC}"
        echo ""
    done
}

show_status() {
    print_header "Data Status"

    if [ ! -d "$DATA_DIR" ]; then
        print_warning "Data directory does not exist: $DATA_DIR"
        print_info "Run './examples/run_all.sh --load' to load data"
        exit 1
    fi

    print_step "Checking store..."
    if check_data_loaded >/dev/null; then
        print_success "Data is loaded and ready"
        echo ""
        print_info "Ready to run examples!"
    else
        print_warning "Data directory exists but appears empty"
        print_info "Run './examples/run_all.sh --load' to load data"
        exit 1
    fi
}

# Parse command line arguments
SPECIFIC_SCRIPT=""
LIST_ONLY=false
LOAD_ONLY=false
STATUS_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -l|--list)
            LIST_ONLY=true
            shift
            ;;
        --load)
            LOAD_ONLY=true
            shift
            ;;
        --status)
            STATUS_ONLY=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        -*)
            print_error "Unknown option: $1"
            echo "Use --help for usage information."
            exit 1
            ;;
        *)
            SPECIFIC_SCRIPT="$1"
            shift
            ;;
    esac
done

# Handle --status
if [ "$STATUS_ONLY" = true ]; then
    show_status
    exit 0
fi

# Handle --list
if [ "$LIST_ONLY" = true ]; then
    show_list
    exit 0
fi

# Handle --load
if [ "$LOAD_ONLY" = true ]; then
    print_header "Loading RDF Data"

    if [ ! -f "$DATA_FILE" ]; then
        print_error "Data file not found: $DATA_FILE"
        exit 1
    fi

    load_data
    exit 0
fi

# Main execution: check data and run examples
print_header "Codebase Insight Examples"

# Check if data is loaded
print_step "Checking data status..."

if check_data_loaded >/dev/null; then
    print_success "Data is loaded and ready"
else
    prompt_load_data
fi

echo ""

# Run specific script or all
if [ -n "$SPECIFIC_SCRIPT" ]; then
    # Find and run specific script
    found=false
    for entry in "${EXAMPLES[@]}"; do
        IFS=':' read -r script title desc <<< "$entry"
        if [ "$script" = "$SPECIFIC_SCRIPT" ]; then
            run_example "$script" "$title" "$desc"
            found=true
            break
        fi
    done

    if [ "$found" = false ]; then
        print_error "Unknown script: ${SPECIFIC_SCRIPT}"
        echo ""
        echo "Available scripts:"
        for entry in "${EXAMPLES[@]}"; do
            IFS=':' read -r script title desc <<< "$entry"
            echo "  - $script"
        done
        exit 1
    fi
else
    # Run all examples
    total=${#EXAMPLES[@]}
    current=0
    failed=0

    for entry in "${EXAMPLES[@]}"; do
        IFS=':' read -r script title desc <<< "$entry"
        current=$((current + 1))

        echo -e "${DIM}[$current/$total]${NC}"
        if ! run_example "$script" "$title" "$desc"; then
            failed=$((failed + 1))
        fi
    done

    # Summary
    print_header "Summary"

    passed=$((total - failed))

    if [ $failed -eq 0 ]; then
        print_success "All $total examples completed successfully!"
    else
        print_success "$passed examples completed"
        print_error "$failed examples failed"
    fi

    echo ""
    print_info "Tip: Run a specific example with './examples/run_all.sh <script>'"
    print_info "     See all options with './examples/run_all.sh --help'"
    echo ""
fi
