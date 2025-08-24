import json
import logging
import requests

import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_extras.metric_cards import style_metric_cards
from utils.connection import (
    TRINO_CATALOG,
    TRINO_HOST,
    execute_alter_table,
    fetch_stats,
    get_schemas,
    get_tables,
    init_connection,
    load_column_sizes,
    load_daily_growth,
    load_file_details,
    load_snapshot_history,
)
from utils.helper import display_dataframe, format_bytes, safe_float

# --- Helper Functions ---
def get_environment_from_host(host):
    """Determine environment (prod/dev) from TRINO_HOST"""
    if "prod" in host.lower():
        return "prod"
    elif "dev" in host.lower():
        return "dev"
    else:
        return "prod"  # Default to prod

def call_glue_optimizer_api(table_name, schema_name, operation_type):
    """Make API call to enable glue optimizer operations"""
    env = get_environment_from_host(TRINO_HOST)
    if env == "prod":
        url = "http://data-lake-manager.prod.attentivemobile.com:4567/api/glueOptimizer/enable"
    else:
        url = f"http://data-lake-manager.{env}.attentivemobile.com:4567/api/glueOptimizer/enable"
    
    payload = {
        "tableNames": [table_name],
        "operationTypes": [operation_type],
        "databaseName": schema_name
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Handle empty or non-JSON responses
        if response.text.strip():
            try:
                json_response = response.json()
                return True, json_response
            except json.JSONDecodeError:
                # Parse the text response to check for failures
                response_text = response.text
                if "failures" in response_text.lower() and "0 successful operations" in response_text:
                    return False, response_text
                elif "AlreadyExistsException" in response_text:
                    return False, f"Already enabled: {response_text}"
                else:
                    return True, {"message": "Success", "response_text": response_text}
        else:
            return True, {"message": "Success - no response body"}
            
    except requests.exceptions.RequestException as e:
        return False, str(e)

# --- Page Configuration ---
st.set_page_config(
    page_title="Iceberg Metadata Insights",
    page_icon="🧊",  # Using an Iceberg emoji
    layout="wide",
)

# theme = st_theme()  # Commented out due to duplicate ID error

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --- Main Application Logic ---
def main():
    st.title("🧊 Iceberg Metadata Insights")

    # Initialize connection
    conn = init_connection()
    if not conn:
        st.error(
            "Database connection failed. Please check configuration and ensure Trino is running."
        )
        st.stop()  # Stop execution if connection fails

    cursor = conn.cursor()

    # --- Sidebar for Selection and Actions ---
    with st.sidebar:
        st.header("📍 Select Table")

        # Fetch schemas only once and cache them
        @st.cache_data
        def cached_get_schemas():
            return get_schemas(cursor)

        schemas = cached_get_schemas()
        if not schemas:
            st.warning("No schemas found or error fetching schemas.")
            st.stop()

        # Initialize session state for schema, table and query execution flag
        if "selected_schema" not in st.session_state:
            st.session_state.selected_schema = schemas[0] if schemas else None
        if "selected_table" not in st.session_state:
            st.session_state.selected_table = None
        if "execute_queries" not in st.session_state:
            st.session_state.execute_queries = False

        # Schema selection
        selected_schema = st.selectbox(
            "Schema",
            schemas,
            index=(
                schemas.index(st.session_state.selected_schema)
                if st.session_state.selected_schema in schemas
                else 0
            ),
            key="schema_select",
        )

        # Update session state if schema changes
        if selected_schema != st.session_state.selected_schema:
            st.session_state.selected_schema = selected_schema
            st.session_state.selected_table = (
                None  # Reset table selection when schema changes
            )
            st.session_state.execute_queries = False  # Reset query execution flag

        # Fetch tables based on selected schema
        @st.cache_data
        def cached_get_tables(schema):
            return get_tables(cursor, schema)

        tables = cached_get_tables(st.session_state.selected_schema)
        if not tables:
            st.warning(
                f"No tables found in schema '{st.session_state.selected_schema}' or error fetching tables."
            )
            st.session_state.selected_table = None
            st.session_state.execute_queries = False
        else:
            # Define a callback for table selection
            def on_table_select():
                st.session_state.execute_queries = True

            selected_table = st.selectbox(
                "Table",
                tables,
                index=(
                    tables.index(st.session_state.selected_table)
                    if st.session_state.selected_table in tables
                    else 0
                ),
                key="table_select",
                on_change=on_table_select,
            )

            st.session_state.selected_table = selected_table

            # Add an explicit analyze button if preferred
            if not st.session_state.execute_queries and st.session_state.selected_table:
                if st.button(
                    "Load Table Details", type="primary", use_container_width=True
                ):
                    st.session_state.execute_queries = True
                    st.rerun()

        st.divider()

        # --- Table Actions ---
        if st.session_state.selected_table and st.session_state.execute_queries:
            st.header("⚙️ Table Actions")
            st.warning("Actions modify table state. Use with caution!")

            # Analyze Table
            if st.button(
                "📊 Analyze Table (Compute Stats)",
                use_container_width=True,
                help="Updates table and column statistics.",
            ):
                with st.spinner("Executing ANALYZE..."):
                    try:
                        analyze_query = f'ANALYZE "{st.session_state.selected_schema}"."{st.session_state.selected_table}"'
                        logger.info(f"Executing: {analyze_query}")
                        cursor.execute(analyze_query).fetchall()
                        st.success("ANALYZE command executed successfully.")
                        st.cache_data.clear()
                    except Exception as e:
                        logger.error(f"Error executing ANALYZE: {e}", exc_info=True)
                        st.error(f"Error executing ANALYZE: {e}")

            # Optimize/Vacuum with smart threshold suggestions
            st.markdown("**Optimize Files**")
            
            # Get table stats for smart suggestions (fetch fresh if needed)
            try:
                table_stats = fetch_stats(cursor, st.session_state.selected_schema, st.session_state.selected_table)
                if table_stats:
                    small_files_count = int(safe_float(table_stats.get('Small Files (<100MB)', 0)))
                    avg_file_size_mb = safe_float(table_stats.get("Average File Size (MB)", 0))
                else:
                    small_files_count = 0
                    avg_file_size_mb = 0
            except Exception as e:
                logger.warning(f"Could not fetch stats for optimization suggestions: {e}")
                small_files_count = 0
                avg_file_size_mb = 0
            
            # Smart threshold calculation based on table size
            if st.session_state.get("selected_schema") and st.session_state.get("selected_table"):
                stats = fetch_stats(cursor, st.session_state.selected_schema, st.session_state.selected_table)
                if stats:
                    small_files_count = stats.get("Small Files (<100MB)", 0)
                    
                    # Calculate smart threshold based on number of small files
                    if small_files_count > 2000:
                        suggested_threshold = "32MB"
                        help_text = f"🧠 Smart suggestion: {small_files_count} small files detected. Using lower threshold to limit memory usage."
                    elif small_files_count > 500:
                        suggested_threshold = "128MB" 
                        help_text = f"🧠 Smart suggestion: {small_files_count} small files detected. Using moderate threshold."
                    else:
                        suggested_threshold = "512MB"
                        help_text = f"🧠 Smart suggestion: {small_files_count} small files detected. Safe to use higher threshold."
                else:
                    suggested_threshold = "128MB"
                    help_text = "Files smaller than this threshold will be compacted. Lower thresholds process fewer files and use less memory."
            else:
                suggested_threshold = "128MB"
                help_text = "Files smaller than this threshold will be compacted. Lower thresholds process fewer files and use less memory."
            
            optimize_threshold = st.text_input(
                "File Size Threshold",
                suggested_threshold,
                help=help_text
            )
            
            # Advanced optimization options
            with st.expander("⚙️ Advanced Options"):
                partition_filter = st.text_input("Partition Filter (Optional)", "", help="e.g., date >= '2024-01-01' to optimize only recent partitions")
                    
            if st.button(
                "🔧 Optimize/Compact Files",
                use_container_width=True,
                help="Rewrites small files into larger ones. Smart threshold prevents memory issues.",
            ):
                # Regular optimization with smart threshold
                with st.spinner(f"Optimizing table with threshold {optimize_threshold}..."):
                    # Build optimized command with supported parameters
                    optimize_params = [f"file_size_threshold => '{optimize_threshold}'"]
                    
                    # Add partition filter if specified
                    if partition_filter.strip():
                        optimize_command = f"optimize(WHERE {partition_filter}, {', '.join(optimize_params)})"
                    else:
                        optimize_command = f"optimize({', '.join(optimize_params)})"
                    
                    execute_alter_table(
                        cursor,
                        st.session_state.selected_schema,
                        st.session_state.selected_table,
                        optimize_command,
                    )

            st.divider()
            st.markdown("### 🔧 Glue Optimizer Operations")
            st.caption("Enable automated optimization operations via Data Lake Manager API")
            
            # Enable Compaction
            if st.button(
                "📦 Enable Compaction",
                use_container_width=True,
                help="Enable automated file compaction for this table",
            ):
                with st.spinner("Enabling compaction..."):
                    success, result = call_glue_optimizer_api(
                        st.session_state.selected_table,
                        st.session_state.selected_schema,
                        "compaction"
                    )
                    if success:
                        st.success("✅ Compaction enabled successfully!")
                        if result:
                            st.json(result)
                    else:
                        st.error(f"❌ Failed to enable compaction: {result}")

            # Enable Snapshot Cleanup (Retention)
            if st.button(
                "🧹 Enable Snapshot Cleanup",
                use_container_width=True,
                help="Enable automated snapshot cleanup/retention for this table",
            ):
                with st.spinner("Enabling snapshot cleanup..."):
                    success, result = call_glue_optimizer_api(
                        st.session_state.selected_table,
                        st.session_state.selected_schema,
                        "retention"
                    )
                    if success:
                        st.success("✅ Snapshot cleanup enabled successfully!")
                        if result:
                            st.json(result)
                    else:
                        st.error(f"❌ Failed to enable snapshot cleanup: {result}")

            # Delete Orphaned Files
            if st.button(
                "🗑️ Enable Orphaned File Deletion",
                use_container_width=True,
                help="Enable automated deletion of orphaned files for this table",
            ):
                with st.spinner("Enabling orphaned file deletion..."):
                    success, result = call_glue_optimizer_api(
                        st.session_state.selected_table,
                        st.session_state.selected_schema,
                        "orphan_file_deletion"
                    )
                    if success:
                        st.success("✅ Orphaned file deletion enabled successfully!")
                        if result:
                            st.json(result)
                    else:
                        st.error(f"❌ Failed to enable orphaned file deletion: {result}")

            st.divider()

            # Optimize Manifests
            if st.button(
                "📑 Optimize Manifests",
                use_container_width=True,
                help="Compacts metadata manifest files.",
            ):
                with st.spinner("Optimizing manifests..."):
                    execute_alter_table(
                        cursor,
                        selected_schema,
                        selected_table,
                        "optimize_manifests",
                    )

            # Expire Snapshots
            expire_retention = st.text_input(
                "Expire Snapshots Older Than",
                "7d",
                help="e.g., '7d', '1h'. Snapshots older than this threshold will be expired.",
            )
            if st.button(
                "⏳ Expire Snapshots",
                use_container_width=True,
                help="Removes old snapshot metadata according to retention.",
            ):
                with st.spinner("Expiring snapshots..."):
                    execute_alter_table(
                        cursor,
                        selected_schema,
                        selected_table,
                        f"expire_snapshots(retention_threshold => '{expire_retention}')",
                    )

            # Remove Orphan Files
            orphan_retention = st.text_input(
                "Remove Orphan Files Older Than",
                "7d",
                help="e.g., '7d', '1h'. Files not referenced by metadata older than this will be removed.",
            )
            if st.button(
                "🗑️ Remove Orphan Files",
                use_container_width=True,
                help="Deletes data files no longer referenced by valid snapshots.",
            ):
                with st.spinner("Removing orphan files..."):
                    execute_alter_table(
                        cursor,
                        selected_schema,
                        selected_table,
                        f"remove_orphan_files(retention_threshold => '{orphan_retention}')",
                    )

            # Drop Extended Stats (Less common action)
            if st.button(
                "❌ Drop Extended Stats",
                use_container_width=True,
                type="secondary",
                help="Removes extended statistics like histograms (if collected).",
            ):
                with st.spinner("Dropping extended stats..."):
                    execute_alter_table(
                        cursor,
                        selected_schema,
                        selected_table,
                        "drop_extended_stats",
                    )
        else:
            st.sidebar.info("Select a table to view details and actions.")

    # --- Main Content Area ---
    if selected_table and selected_schema:
        st.header(f"Inspecting: `{selected_schema}`.`{selected_table}`")

        if not st.session_state.execute_queries:
            st.info("Select a table and click 'Load Table Details' to view analytics.")
            st.stop()  # Stop execution until user explicitly loads data

        # From this point on, all your existing query-dependent code executes
        # only when execute_queries is True

        # Fetch and display summary stats
        stats = fetch_stats(cursor, selected_schema, selected_table)
        
        # Force show the table overview regardless of stats
        st.subheader("📌 Table Overview")
        
        if stats:
            # Use safe_float and format_bytes for robust display
            row1_cols = st.columns(6)
            row1_cols[0].metric("Files", f"{int(safe_float(stats.get('Files', 0))):,}")
            row1_cols[1].metric(
                "Partitions", f"{int(safe_float(stats.get('Partitions', 0))):,}"
            )
            row1_cols[2].metric("Rows", f"{int(safe_float(stats.get('Rows', 0))):,}")
            row1_cols[3].metric(
                "Snapshots", f"{int(safe_float(stats.get('Snapshots', 0))):,}"
            )
            row1_cols[4].metric(
                "History Entries", f"{int(safe_float(stats.get('History', 0))):,}"
            )
            row1_cols[5].metric(
                "Small Files (<100MB)",
                f"{int(safe_float(stats.get('Small Files (<100MB)', 0))):,}",
            )

            # Second row for storage size metrics
            row2_cols = st.columns(6)
            row2_cols[0].metric(
                "Current Table Size",
                format_bytes(safe_float(stats.get("Current Snapshot Storage Size (Bytes)", 0))),
                help="Total storage size of all data files currently part of the table"
            )

            st.subheader("📏 File Size Metrics")
            row4_cols = st.columns(6)
            row4_cols[0].metric(
                "Avg File Size",
                format_bytes(
                    safe_float(stats.get("Average File Size (MB)", 0)) * 1024 * 1024
                ),
            )  # Convert MB back to Bytes for formatter
            row4_cols[1].metric(
                "Largest File",
                format_bytes(
                    safe_float(stats.get("Largest File Size (MB)", 0)) * 1024 * 1024
                ),
            )
            row4_cols[2].metric(
                "Smallest File",
                format_bytes(
                    safe_float(stats.get("Smallest File Size (MB)", 0)) * 1024 * 1024
                ),
            )
            row4_cols[3].metric(
                "Avg Records/File",
                f"{int(safe_float(stats.get('Average Records per File', 0))):,}",
            )
            row4_cols[4].metric(
                "Std Dev File Size",
                format_bytes(
                    safe_float(stats.get("Std Dev File Size (MB)", 0)) * 1024 * 1024
                ),
            )
            # Apply metric card styling with explicit dark theme colors
            style_metric_cards(
                background_color="#0E1117",
                border_color="#262730",
                border_left_color="#FF6B6B",
                border_size_px=1,
                border_radius_px=5,
                box_shadow="0 0.15rem 1.75rem 0 rgba(58, 59, 69, 0.15) !important"
            )

        else:
            st.warning("Could not fetch summary statistics for the table.")
            # Show at least the storage metrics even if other stats fail
            st.subheader("💾 Storage Size Information (Fallback)")
            try:
                # Try to get storage info directly
                conn = init_connection()
                if conn:
                    cursor_direct = conn.cursor()
                    current_size_query = f'SELECT COALESCE(SUM(CAST(file_size_in_bytes AS BIGINT)), 0) FROM "{selected_schema}"."{selected_table}$files"'
                    result = cursor_direct.execute(current_size_query).fetchone()
                    if result and result[0]:
                        st.metric("Current Snapshot Size", format_bytes(result[0]))
                    else:
                        st.metric("Current Snapshot Size", "Unable to calculate")
            except Exception as e:
                st.error(f"Could not calculate storage size: {e}")

        st.divider()

        # --- Charts Section ---
        st.subheader("📈 Charts & Trends")
        chart_tabs = st.tabs(
            [
                "Snapshot Timeline",
                "Data Growth (Rows)",
                "Data Growth (Files)",
                "File Size Distribution",
                "Column Sizes",
            ]
        )

        with chart_tabs[0]:
            st.markdown("#### 📸 Snapshot Timeline")
            snapshot_history_df = load_snapshot_history(
                cursor, selected_schema, selected_table
            )
            if not snapshot_history_df.empty:
                # Ensure datetime conversion for plotting
                snapshot_history_df["Committed At"] = pd.to_datetime(
                    snapshot_history_df["Committed At"], errors="coerce"
                )
                fig_snapshots = px.scatter(
                    snapshot_history_df.dropna(
                        subset=["Committed At"]
                    ),  # Drop rows where conversion failed
                    x="Committed At",
                    # Use 'Snapshot ID' or index if y doesn't make sense
                    y=snapshot_history_df.index,  # Plot against index to show sequence
                    color="Operation",
                    hover_data=["Snapshot ID", "Parent ID", "Operation", "Summary"],
                    title="Snapshot Timeline by Operation",
                    labels={"y": "Snapshot Sequence Index"},
                )
                fig_snapshots.update_layout(
                    xaxis_title="Commit Time", yaxis_title="Snapshot Sequence Index"
                )
                st.plotly_chart(fig_snapshots, use_container_width=True)
            else:
                st.info("No snapshot history available to plot.")

        with chart_tabs[1]:
            st.markdown("#### 📈 Data Growth (Rows)")
            daily_growth_df = load_daily_growth(cursor, selected_schema, selected_table)
            if not daily_growth_df.empty:
                daily_growth_df["Committed At"] = pd.to_datetime(
                    daily_growth_df["Committed At"], errors="coerce"
                )
                # Plot added vs deleted rows over time
                fig_growth_rows = px.line(
                    daily_growth_df.dropna(subset=["Committed At"]),
                    x="Committed At",
                    y=["Added Rows Count", "Deleted Rows Count"],
                    title="Added vs Deleted Rows per Snapshot",
                    labels={"value": "Row Count", "variable": "Action"},
                    markers=True,
                )
                fig_growth_rows.update_layout(
                    xaxis_title="Commit Time", yaxis_title="Number of Rows"
                )
                st.plotly_chart(fig_growth_rows, use_container_width=True)
            else:
                st.info("No daily growth data available to plot.")

        with chart_tabs[2]:
            st.markdown("#### 📈 Data Growth (Files)")
            # Use the same daily_growth_df loaded previously
            if not daily_growth_df.empty:
                # Plot added vs deleted files over time
                fig_growth_files = px.line(
                    daily_growth_df.dropna(subset=["Committed At"]),
                    x="Committed At",
                    y=["Added Data Files Count", "Deleted Data Files Count"],
                    title="Added vs Deleted Data Files per Snapshot",
                    labels={"value": "File Count", "variable": "Action"},
                    markers=True,
                )
                fig_growth_files.update_layout(
                    xaxis_title="Commit Time", yaxis_title="Number of Data Files"
                )
                st.plotly_chart(fig_growth_files, use_container_width=True)
            else:
                st.info(
                    "No daily growth data available to plot."
                )  # Should be covered by previous check

        with chart_tabs[3]:
            st.markdown("#### 📂 File Size Distribution")
            file_details_df = load_file_details(cursor, selected_schema, selected_table)
            if not file_details_df.empty:
                # Convert size to MB for histogram readability
                file_details_df["Size (MB)"] = safe_float(file_details_df["Size"]) / (
                    1024 * 1024
                )

                # Create size categories for better visualization
                bins = [0, 1, 10, 50, 100, 500, float("inf")]
                labels = [
                    "<1MB",
                    "1-10MB",
                    "10-50MB",
                    "50-100MB",
                    "100-500MB",
                    ">500MB",
                ]
                file_details_df["Size Category"] = pd.cut(
                    file_details_df["Size (MB)"], bins=bins, labels=labels
                )

                # Count files by size category
                size_counts = (
                    file_details_df["Size Category"].value_counts().sort_index()
                )

                # Create bar chart
                fig_size_dist = px.bar(
                    x=size_counts.index,
                    y=size_counts.values,
                    title="File Size Distribution by Category",
                    labels={"x": "File Size Category", "y": "Number of Files"},
                )
                st.plotly_chart(fig_size_dist, use_container_width=True)

                # Show detailed stats
                with st.expander("Show detailed statistics"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Total files", len(file_details_df))
                        st.metric(
                            "Average file size",
                            f"{file_details_df['Size (MB)'].mean():.2f} MB",
                        )
                        st.metric(
                            "Median file size",
                            f"{file_details_df['Size (MB)'].median():.2f} MB",
                        )
                    with col2:
                        st.metric(
                            "Min file size",
                            f"{file_details_df['Size (MB)'].min():.2f} MB",
                        )
                        st.metric(
                            "Max file size",
                            f"{file_details_df['Size (MB)'].max():.2f} MB",
                        )
                        st.metric(
                            "Std dev", f"{file_details_df['Size (MB)'].std():.2f} MB"
                        )
            else:
                st.info("No file details available to plot distribution.")

        with chart_tabs[4]:
            st.markdown("#### 📊 Column Sizes")
            column_sizes_df = load_column_sizes(cursor, selected_schema, selected_table)
            if (
                not column_sizes_df.empty
                and "Size (Bytes)" in column_sizes_df.columns
                and column_sizes_df["Size (Bytes)"].notna().any()
            ):
                # Plot if size data is available and not all null
                column_sizes_df["Size (MB)"] = pd.to_numeric(
                    column_sizes_df["Size (Bytes)"], errors="coerce"
                ).fillna(0) / (1024 * 1024)
                fig_col_sizes = px.bar(
                    column_sizes_df,
                    x="Column Name",
                    y="Size (MB)",
                    color="Data Type",
                    title="Estimated Aggregate Column Sizes",
                    labels={"Size (MB)": "Total Size (MB)"},
                    # text="Size (MB)" # Adding text might clutter the bar chart
                )
                # fig_col_sizes.update_traces(texttemplate='%{text:.2s} MB', textposition='outside') # Formatting text
                fig_col_sizes.update_layout(
                    xaxis_title="Column Name", yaxis_title="Estimated Size (MB)"
                )
                st.plotly_chart(fig_col_sizes, use_container_width=True)
            elif not column_sizes_df.empty:
                st.info(
                    "Aggregate column size data could not be calculated from file metadata. Displaying column list instead."
                )
                st.dataframe(
                    column_sizes_df[["Column Name", "Data Type"]],
                    use_container_width=True,
                )
            else:
                st.info("No column size information available.")

        st.divider()

        # --- Metadata Tables Section ---
        st.subheader("📋 Detailed Metadata Tables")
        # Define all metadata tables
        metadata_tables = {
            "$properties": {
                "cols": ["Key", "Value"],
                "desc": "Table configuration properties.",
            },
            "$history": {
                "cols": [
                    "Made Current At",
                    "Snapshot ID",
                    "Parent ID",
                    "Is Current Ancestor",
                ],
                "desc": "Log of metadata changes (snapshot commits).",
            },
            "$metadata_log_entries": {
                "cols": [
                    "Timestamp",
                    "File",
                    "Latest Snapshot ID",
                    "Latest Schema ID",
                    "Latest Sequence Number",
                ],
                "desc": "Log of metadata file updates.",
            },
            "$snapshots": {
                "cols": [
                    "Committed At",
                    "Snapshot ID",
                    "Parent ID",
                    "Operation",
                    "Manifest List",
                    "Summary",
                ],
                "desc": "Detailed view of table snapshots.",
            },
            "$manifests": {
                "cols": [
                    "Path",
                    "Length",
                    "Partition Spec ID",
                    "Added Snapshot ID",
                    "Added Data Files Count",
                    "Added Rows Count",
                    "Existing Data Files Count",
                    "Existing Rows Count",
                    "Deleted Data Files Count",
                    "Deleted Rows Count",
                ],
                "desc": "Manifest files for the current snapshot.",
            },
            "$all_manifests": {
                "cols": [
                    "Path",
                    "Length",
                    "Partition Spec ID",
                    "Added Snapshot ID",
                    "Added Data Files Count",
                    "Existing Data Files Count",
                    "Deleted Data Files Count",
                ],
                "desc": "Manifest files for all snapshots.",
            },
            "$partitions": {
                "cols": [
                    "Partition",
                    "Record Count",
                    "File Count",
                    "Total Size",
                    "Data",
                ],
                "desc": "Detailed view of table partitions.",
            },
            "$files": {
                "cols": [
                    "Content",
                    "File Path",
                    "File Format",
                    "Partition",
                    "Record Count",
                    "File Size (Bytes)",
                    "Column Sizes",
                    "Value Counts",
                    "Null Value Counts",
                    "NaN Value Counts",
                    "Lower Bounds",
                    "Upper Bounds",
                    "Key Metadata",
                    "Split Offsets",
                    "Equality IDs",
                    "Sort Order ID",
                ],
                "desc": "Data files in the current snapshot.",
            },
            "$entries": {
                "cols": [
                    "Status",
                    "Snapshot ID",
                    "Sequence Number",
                    "File Sequence Number",
                    "Data File",
                    "Readable Metrics",
                ],
                "desc": "Manifest entries (data/delete files) for the current snapshot.",
            },
            "$all_entries": {
                "cols": [
                    "Status",
                    "Snapshot ID",
                    "Sequence Number",
                    "File Sequence Number",
                    "Data File",
                    "Readable Metrics",
                ],
                "desc": "Manifest entries (data/delete files) for all snapshots.",
            },
            "$refs": {
                "cols": [
                    "Name",
                    "Type",
                    "Snapshot ID",
                    "Max Reference Age (ms)",
                    "Min Snapshots to Keep",
                    "Max Snapshot Age (ms)",
                ],
                "desc": "Table references (branches and tags).",
            },
        }

        tab_names = ["SHOW DDL"] + list(metadata_tables.keys())
        meta_tabs = st.tabs(tab_names)

        # Show DDL Tab
        with meta_tabs[0]:
            st.markdown("#### `SHOW CREATE TABLE`")
            try:
                # Ensure correct quoting for schema and table names
                ddl_query = f'SHOW CREATE TABLE "{selected_schema}"."{selected_table}"'
                logger.info(f"Executing: {ddl_query}")
                ddl = cursor.execute(ddl_query).fetchall()[0][0]
                st.code(ddl, language="sql")
            except Exception as e:
                logger.error(f"Error executing SHOW CREATE TABLE: {e}", exc_info=True)
                st.error(f"Error loading table DDL: {e}")

        # Loop through metadata tables for other tabs
        for i, (meta_name, meta_info) in enumerate(metadata_tables.items()):
            with meta_tabs[i + 1]:  # Offset by 1 due to DDL tab
                st.markdown(f"#### `{meta_name}`")
                st.caption(meta_info["desc"])
                try:
                    # Construct query carefully, quoting table name with suffix
                    # Catalog and schema should be part of the connection or prefixed if needed
                    query = f'SELECT * FROM "{selected_schema}"."{selected_table}{meta_name}"'
                    logger.info(f"Executing: {query}")

                    df = pd.read_sql(query, conn)
                    df.drop(
                        columns=["partition_summaries"], inplace=True, errors="ignore"
                    )
                    
                    # Wrap display_dataframe in additional try/catch to prevent page crashes
                    try:
                        display_dataframe(df)  # Use helper to display
                    except Exception as display_error:
                        logger.error(f"Error displaying metadata table {meta_name}: {display_error}", exc_info=True)
                        st.warning(f"Data loaded but cannot display `{meta_name}` due to formatting issues. Showing raw data instead.")
                        st.write(f"Rows: {len(df)}, Columns: {list(df.columns)}")
                        # Try to show at least the first few rows as text
                        try:
                            st.text(df.head().to_string())
                        except:
                            st.text("Raw data cannot be displayed due to formatting issues")

                except Exception as e:
                    logger.error(
                        f"Error loading metadata table {meta_name}: {e}", exc_info=True
                    )
                    st.error(f"Error loading `{meta_name}`: {e}")


        st.divider()

        # --- Data Profiling Section (Optional) ---
        st.subheader("🔬 Data Profiling (Sample)")
        profile_limit = st.number_input(
            "Number of rows to profile",
            min_value=100,
            max_value=10000,
            value=1000,
            step=100,
            help="Limits the data fetched for profiling.",
        )
        if st.button("📊 Generate Profile Report (Sample)", use_container_width=True):
            st.warning(
                "Profiling fetches data and can be slow on large tables, even with limits."
            )
            with st.spinner(
                f"Fetching {profile_limit} rows and generating profile report..."
            ):
                try:
                    # Import profiling libraries here to avoid loading them if not used
                    from streamlit_ydata_profiling import st_profile_report
                    from ydata_profiling import ProfileReport

                    query = f'SELECT * FROM "{selected_schema}"."{selected_table}" LIMIT {profile_limit}'
                    logger.info(f"Executing profiling query: {query}")
                    # Use connection object directly with pandas for type handling
                    df_sample = pd.read_sql(query, conn)
                    st.success(f"Fetched {len(df_sample)} rows for profiling.")

                    if not df_sample.empty:
                        pr = ProfileReport(
                            df_sample,
                            title=f"Data Profiling Report (Sample) - {selected_schema}.{selected_table}",
                            explorative=True,
                            minimal=True,  # Use minimal mode for faster reports
                        )
                        st_profile_report(pr, navbar=True)
                        # Optionally show the sampled data
                        # with st.expander("View Sampled Data Used for Profiling"):
                        #    st.dataframe(df_sample)
                    else:
                        st.warning("No data returned for profiling.")

                except ImportError:
                    st.error(
                        "Please install 'ydata-profiling' and 'streamlit-ydata-profiling' to use this feature."
                    )
                except Exception as e:
                    logger.error(f"Error generating profile report: {e}", exc_info=True)
                    st.error(f"Error generating profile report: {e}")

    elif not selected_schema:
        st.info("Please select a schema from the sidebar.")
    # If schema selected but no table selected (or error fetching tables)
    elif not selected_table:
        st.info(
            f"Please select a table from schema '{selected_schema}' in the sidebar, or check if tables exist."
        )


if __name__ == "__main__":
    main()
