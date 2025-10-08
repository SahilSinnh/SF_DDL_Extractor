# The main Streamlit application file.
# Imports
import streamlit as st
from datetime import datetime
from collections import defaultdict
from typing import Dict, Any, Optional


# Import modularized functions
import snowflake_utils as sf
import sql_parser
import dependencies
import graph_utils
import login_ui
import streamlit.components.v1 as components

# By-Line
with st.container():
    st.markdown("<span style='position: absolute; top: 8%; right: 10%;'>:gray[Created by] :rainbow[**Sahil Singh**]</span>", unsafe_allow_html=True)

# Initialize session state and parameters for Snowflake session and login
st.query_params["sf"] = "None" if "sf" not in st.query_params else st.query_params["sf"]

try:
    from snowflake.snowpark.context import get_active_session
    st.session_state['snowflake_session'] = get_active_session()
    st.session_state['logged_in'] = True
    if st.query_params["sf"] == "None" or st.query_params["sf"] == "True":
        st.query_params["sf"] = "True"
except:
    st.session_state['logged_in'] = False
    st.query_params["sf"] = "False"
    
st.session_state['is_snowflake'] = st.query_params["sf"] == "True"


# -----------------------------
# STATE MANAGEMENT HELPERS
# -----------------------------
def init_session_state():
    # Initialize session state keys if they don't exist.
    if "db_selected" not in st.session_state:
        st.session_state.db_selected = None
    if "objects" not in st.session_state:
        st.session_state.objects = []
    if "raw_objects_list" not in st.session_state:
        st.session_state.raw_objects_list = []
    if "dependency_graph" not in st.session_state:
        st.session_state.dependency_graph = {}
    if "grouped_objects" not in st.session_state:
        st.session_state.grouped_objects = defaultdict(lambda: defaultdict(list))
    if "search_query" not in st.session_state:
        st.session_state.search_query = ""
    if "final_script_output" not in st.session_state:
        st.session_state.final_script_output = ""
    if "script_source_keys" not in st.session_state:
        st.session_state.script_source_keys = set()
    if "selected_schemas" not in st.session_state:
        st.session_state.selected_schemas = []

def reset_app_state():
    # Reset application-specific session state while preserving login-related state.
    login_keys = {'snowflake_session', 'logged_in', 'is_snowflake', 'is_loading',
                  'account', 'user', 'auth_method', 'warehouse', 'role',
                  'password', 'key_option', 'key_content', 'key_file'}
    keys_to_clear = [key for key in st.session_state.keys() if key not in login_keys]
    for key in keys_to_clear:
        del st.session_state[key]

def sync_checkbox_state(db_key):
    
    # Manages the bi-directional synchronization of hierarchical checkboxes.
    # This function uses a "compare with previous state" method to avoid callbacks.
    
    if not st.session_state.objects:
        return

    obj_keys = {o['obj_key'] for o in st.session_state.objects}
    sch_keys = {o['sch_key'] for o in st.session_state.objects}
    
    # Determine the source of the change by comparing current and previous states
    source_key = None
    source_value = None

    # Check global checkbox
    prev_global = st.session_state.get(f"__prev__{db_key}", False)
    cur_global = st.session_state.get(db_key, False)
    if cur_global != prev_global:
        source_key = db_key
        source_value = cur_global
    
    # Check schema checkboxes if global wasn't the source
    if not source_key:
        for sk in sch_keys:
            prev_group = st.session_state.get(f"__prev__{sk}", False)
            cur_group = st.session_state.get(sk, False)
            if cur_group != prev_group:
                source_key = sk
                source_value = cur_group
                break # Found the source, stop searching

    # --- Propagate Changes ---

    # If a parent was the source, propagate the change downwards to children
    if source_key == db_key:
        # Global checkbox was clicked, push its state down to all objects in selected schemas
        selected_schemas = [s for s, selected in st.session_state.get('schema_selection', {}).items() if selected]
        keys_to_update = {o['obj_key'] for o in st.session_state.objects if o.get('schema', 'N/A') in selected_schemas}
        for ok in keys_to_update:
            st.session_state[ok] = source_value
    
    elif source_key in sch_keys:
        # A schema checkbox was clicked, push its state down to its direct children
        child_keys = [o['obj_key'] for o in st.session_state.objects if o['sch_key'] == source_key]
        for ck in child_keys:
            st.session_state[ck] = source_value

    # --- Synchronize Upwards ---
    # Always recalculate parent states based on children states.
    # This ensures consistency after a child is clicked, or confirms the state after a parent is clicked.
    for sk in sch_keys:
        child_keys = [o['obj_key'] for o in st.session_state.objects if o['sch_key'] == sk]
        if child_keys:
            st.session_state[sk] = all(st.session_state.get(k, False) for k in child_keys)

    # Recalculate global checkbox state based on all objects in selected schemas
    selected_schemas = [s for s, selected in st.session_state.get('schema_selection', {}).items() if selected]
    keys_for_global_check = {o['obj_key'] for o in st.session_state.objects if o.get('schema', 'N/A') in selected_schemas}
    if keys_for_global_check:
        st.session_state[db_key] = all(st.session_state.get(k, False) for k in keys_for_global_check)
    else:
        st.session_state[db_key] = False

    # --- Update Previous State ---
    # Finally, update the '__prev__' state for the next rerun with the now-finalized values
    st.session_state[f"__prev__{db_key}"] = st.session_state.get(db_key, False)
    for sk in sch_keys:
        st.session_state[f"__prev__{sk}"] = st.session_state.get(sk, False)

# -----------------------------
# MAIN APP LOGIC
# -----------------------------
st.set_page_config(
    page_title="Snowflake DDL Extractor",
    page_icon=":material/ac_unit:",
    initial_sidebar_state="expanded"
)

if st.session_state['logged_in']:
    init_session_state()
    
    # Sync checkbox state before rendering the rest of the UI
    if st.session_state.db_selected and st.session_state.db_selected != "— Select a database —":
        db_key = f"DB|" + st.session_state.db_selected
        sync_checkbox_state(db_key)
        
    # --- Sidebar for Selections ---
    with st.sidebar:
        curr_acc = st.session_state['snowflake_session'].get_current_account().replace('"','').upper()
        curr_wh = st.session_state['snowflake_session'].get_current_warehouse().replace('"','')
        curr_usr = st.session_state['snowflake_session'].get_current_user().replace('"','')
        curr_role = st.session_state['snowflake_session'].get_current_role().replace('"','')
        a, b = st.columns([2,1])
        with a:
            if st.session_state['is_snowflake']:
                st.write(f"`(Snowsight)`")
            else:
                st.write(f"`(External)`")
            st.write(f"##### `{curr_acc}` :blue[|] `{curr_wh}`")
            st.write(f"##### `{curr_usr}` :blue[|] `{curr_role}`")
        with b:
            if not st.session_state['is_snowflake']:
                if st.button("**:material/logout: Logout**", key="logout_btn"):
                    with st.spinner(f"Logging out..."):
                        st.session_state['snowflake_session'].close()
                        st.session_state.clear()
                        st.rerun()
                        
        db_options = ["— Select a database —"] + sf.list_databases()
        selected_db = st.selectbox(
            f":orange[**{sql_parser.get_material_icon('DATABASE')} Database**]",
            db_options,
            index=0,
            key='db_selector'
        )

        # --- Main Processing Block ---
        if selected_db and selected_db != "— Select a database —":
            # If a new database is selected, reset state and fetch new data
            if st.session_state.db_selected != selected_db:
                reset_app_state() # Clear all state for new DB
                init_session_state()
                st.session_state.db_selected = selected_db
                with st.spinner(f"Extracting and parsing DDL for **{selected_db}**... This may take a moment."):
                    ddl_text, stage_ddls = sf.get_database_ddl(selected_db)
                    if ddl_text is not None:
                        raw_objects = []
                        full_ddl_text = (ddl_text or "") + (stage_ddls or "")
                        statements = sql_parser.split_sql_statements(full_ddl_text)
                        for idx, stmt in enumerate(statements):
                            # Remove database references from the statement
                            cleaned_stmt = sql_parser.remove_database_references(stmt, selected_db)
                            meta: Optional[Dict[str, Any]] = sql_parser.extract_object_metadata(cleaned_stmt)

                            if meta:
                                if not meta.get("database"): meta["database"] = selected_db
                                # Store the cleaned DDL
                                meta["ddl"] = cleaned_stmt.strip()
                                meta["index"] = idx
                                raw_objects.append(meta)

                        sorted_objects, deps = dependencies.order_objects_by_dependencies(raw_objects)
                        st.session_state.raw_objects_list = sorted_objects
                        st.session_state.dependency_graph = deps

                        # Filter out DATABASE and SCHEMA object types from being selectable things
                        filtered_sorted_objects = [
                            o for o in sorted_objects 
                            if o.get("object_type") not in ["DATABASE", "SCHEMA"]
                        ]

                        # Now, create a clean version of the objects for the UI, without the internal fields
                        clean_objects = []
                        for o in filtered_sorted_objects:
                            new_o = {k: v for k, v in o.items() if not k.startswith('_')}
                            clean_objects.append(new_o)

                        grouped = defaultdict(lambda: defaultdict(list))
                        for obj in clean_objects:
                            db = obj.get("database", "")
                            sch = obj.get("schema", "N/A")
                            obj_name = obj.get("object_name", "")
                            obj_type = obj.get("object_type", "UNKNOWN")

                            obj['db_key'] = f"DB|" + db
                            obj['sch_key'] = f"SCH|" + db + "|" + sch
                            obj['obj_key'] = f"OBJ|" + db + "|" + sch + "|" + obj_type + "|" + obj_name

                            grouped[sch][obj_type].append(obj)

                        st.session_state.objects = clean_objects
                        st.session_state.grouped_objects = grouped
                        st.success(f"Successfully parsed {len(clean_objects)} objects from '{selected_db}'.")

            # --- Schema Selection in Sidebar ---
            if st.session_state.objects:
                st.markdown(f":orange[**{sql_parser.get_material_icon('SCHEMA')} Select Schemas**]")
                all_schemas = sorted(st.session_state.grouped_objects.keys())

                # Initialize or update schema selection state
                if 'schema_selection' not in st.session_state or set(st.session_state.schema_selection.keys()) != set(all_schemas):
                    st.session_state.schema_selection = {s: True for s in all_schemas}

                # Create a list of all items for the grid
                grid_items = ["Toggle All"] + all_schemas

                num_cols = 3
                cols = st.columns(num_cols)

                for i, item in enumerate(grid_items):
                    with cols[i % num_cols]:
                        if item == "Toggle All":
                            all_selected = all(st.session_state.schema_selection.get(s) for s in all_schemas)
                            btn_type = "secondary" if all_selected else "primary"
                            if st.button(item, use_container_width=True, help="Select or deselect all schemas.", type=btn_type):
                                new_state = not all_selected
                                for s in all_schemas:
                                    st.session_state.schema_selection[s] = new_state
                                st.rerun() #GFIXX

                        else: # It's a schema name
                            schema = item
                            is_selected = st.session_state.schema_selection.get(schema, True)
                            btn_type = "primary" if is_selected else "secondary"
                            if st.button(schema, key=f"schema_btn_{schema}", use_container_width=True, type=btn_type):
                                st.session_state.schema_selection[schema] = not is_selected
                                st.rerun() #GFIXX


                # Update the global selected_schemas list
                st.session_state.selected_schemas = [s for s, selected in st.session_state.schema_selection.items() if selected]

                # --- Generate SQL Script ---
                st.markdown("---")
                st.header("Generated SQL Script")

                selected_objects = [o for o in st.session_state.objects if st.session_state.get(o['obj_key'])]

                if not selected_objects:
                    st.info("Select objects to generate the script.")
                else:
                    st.success(f"{len(selected_objects)} objects selected. Scroll down to download the SQL script.")

                    current_script_keys = {o['obj_key'] for o in selected_objects}
                    if current_script_keys != st.session_state.script_source_keys:
                        st.session_state.final_script_output = ";\n\n".join([o['ddl'] for o in selected_objects]) + ";"
                        st.session_state.script_source_keys = current_script_keys

                    # --- Pre-download check for database references ---
                    db_ref_warnings = []    # List to store warnings per object with multiple matches
                    db_name = st.session_state.db_selected
                    final_script_lines = st.session_state.final_script_output.split('\n')   # Split once for efficiency
                    
                    sel_obj = selected_objects
                    for obj in sel_obj:
                        # Create a temporary DDL with a semicolon for comparison, without modifying the object in session state
                        ddl_with_semicolon = obj['ddl'] + ";"
                        # Check if db_name exists in the object's DDL (case-insensitive).
                        if db_name.lower() in ddl_with_semicolon.lower():
                            obj_ddl_lines = ddl_with_semicolon.split('\n')  # Split individual DDL
                            matches = []  # List to store all matching positions and details for this object
                            
                            # Find all matching lines in obj DDL
                            for i, line in enumerate(obj_ddl_lines):
                                if db_name.lower() in line.lower():
                                    obj_line_number = i + 1  # Line number in obj['ddl'] (1-based)
                                    
                                    # Find the corresponding line number in final_script_output
                                    final_line_number = None
                                    for j, fs_line in enumerate(final_script_lines):
                                        if line.strip() == fs_line.strip():
                                            # Verify context with surrounding lines to avoid false matches
                                            is_match = True
                                            # Check previous line if exists
                                            if i > 0 and j > 0:  # Check previous line if available
                                                if obj_ddl_lines[i-1].strip() != final_script_lines[j-1].strip():
                                                    is_match = False
                                            # Check next line if exists
                                            if i < len(obj_ddl_lines)-1 and j < len(final_script_lines)-1:  # Check next line
                                                if obj_ddl_lines[i+1].strip() != final_script_lines[j+1].strip():
                                                    is_match = False
                                            if is_match:
                                                final_line_number = j + 1  # 1-based line number
                                                break
                                    matches.append({
                                        'ddl_line_number': obj_line_number,
                                        'script_line_number': final_line_number if final_line_number else 'Not found',
                                        'line_content': line.strip()
                                    })

                            # Initialize snippet as empty string to avoid KeyError
                            snippet = ""
                            if matches:
                                # Sort matches by ddl_line_number (though enumerate is ordered)
                                matches.sort(key=lambda m: m['ddl_line_number'])
                                
                                # Group consecutive matches into blocks
                                snippet_parts = []
                                current_block = []
                                last_end = -2  # Track end of last block for gap detection
                                for match in matches:
                                    ddl_line = match['ddl_line_number'] - 1  # 0-based
                                    # Start new block if gap > 1 from previous block's end
                                    if ddl_line - last_end > 1 and current_block:
                                        snippet_parts.append(list(current_block))
                                        current_block = []                                    # Add prev, match, next lines (avoid duplicates)
                                    start = max(0, ddl_line - 1)  # Safe prev line
                                    end = min(len(obj_ddl_lines), ddl_line + 2)  # Safe next line
                                    for k in range(start, end):
                                        if k not in [item[0] for item in current_block]:
                                            is_match_line = any(m['ddl_line_number'] - 1 == k for m in matches)
                                            # Find corresponding line in final_script_output
                                            final_k = None
                                            content = obj_ddl_lines[k].strip()
                                            for j, fs_line in enumerate(final_script_lines):
                                                if content == fs_line.strip():
                                                    is_context_match = True
                                                    # Check previous line if exists
                                                    if k > 0 and j > 0:
                                                        if obj_ddl_lines[k-1].strip() != final_script_lines[j-1].strip():
                                                            is_context_match = False
                                                    # Check next line if exists
                                                    if k < len(obj_ddl_lines) - 1 and j < len(final_script_lines) - 1:
                                                        if obj_ddl_lines[k+1].strip() != final_script_lines[j+1].strip():
                                                            is_context_match = False
                                                    if is_context_match:
                                                        final_k = j + 1  # 1-based
                                                        break
                                            final_k = final_k if final_k else 'N/A'
                                            current_block.append((k, is_match_line, final_k))
                                    last_end = end - 1
                                
                                # Append final block if exists
                                if current_block:
                                    snippet_parts.append(list(current_block))
                                
                                # Join blocks: "\n...\n" for gaps, "\n" for adjacent blocks
                                #snippet = "\n...\n".join(snippet_parts)
                                snippet = ""
                                if snippet_parts:
                                    snippet_lines = []
                                    for i, block in enumerate(snippet_parts):
                                        block_snippet = sql_parser.build_block_snippet(block, obj_ddl_lines, final_script_lines)
                                        snippet_lines.append(block_snippet)
                                        if i < len(snippet_parts) - 1:
                                            last_line = max(k for k, _, _ in block)  # Last k in current block
                                            next_start = min(k for k, _, _ in snippet_parts[i + 1])  # First k in next block
                                            separator = "\n" if next_start - last_line <= 1 else "\n...\n"
                                            snippet_lines.append(separator)
                                    snippet = "".join(snippet_lines)
                            
                            # Store warning info for this object
                            warning_info = {
                                "object_type": obj.get("object_type", "Object"),
                                "fully_qualified_name": obj.get("fully_qualified_name", "Unknown"),
                                "object_name": obj.get("object_name", "Unknown"),
                                "matches": matches,  # Array of all occurrences
                                "snippet": snippet  # Always defined, empty if no matches
                            }
                            db_ref_warnings.append(warning_info)
                
                    if db_ref_warnings:
                        st.warning(f"**Database Reference Warning:** The script contains hardcoded references to the '{db_name}' database. This may cause issues when deploying to other environments.", icon=":material/warning:")
                        with st.expander("Click to see details"):
                            for warning in db_ref_warnings:
                                num_matches = len(warning['matches'])
                                plural = "s" if num_matches > 1 else ""
                                st.markdown(f"- **{warning['object_type']}:** `{warning['fully_qualified_name']}`")
                                with st.expander(f"{num_matches} occurrence{plural}"):
                                    st.code(warning['snippet'], language='sql')

                    # --- Display and Download ---
                    code_container = st.container(height=400)
                    code_container.code(
                        st.session_state.final_script_output,
                        language='sql',
                        line_numbers=True
                    )

                    file_name = f"{st.session_state.db_selected}_DDL_Export_{datetime.now().strftime('%Y%m%d%H%M%S')}.sql"
                    if st.download_button(
                        label=":blue[:material/download_2: Download .sql File]",
                        data=st.session_state.final_script_output,
                        file_name=file_name,
                        mime="text/plain",
                        use_container_width=True,
                    ):
                        st.success(f":material/download: Downloaded - **{file_name}**!")
        else:
            reset_app_state() # Clear all state for new DB
            init_session_state()

    # --- Main Area for Object Display ---
    
    st.title(f":violet[:material/ac_unit: Snowflake DDL Extractor {st.__version__}]")
    st.markdown(":violet[**A tool to extract, parse, and download object DDLs from a Snowflake database.**]")

    if st.session_state.db_selected and st.session_state.db_selected != "— Select a database —":
        if st.session_state.objects:
            # --- Dialog Definition ---
            @st.dialog(":rainbow[:material/graph_4: Dependency Graph]", width="large")
            def dependency_graph_dialog():
                st.info(f"Showing dependencies among schemas - **{st.session_state.selected_schemas}** in database **{st.session_state.db_selected}**.")
                if not st.session_state.selected_schemas:
                    st.warning("No schemas selected. Please select at least one schema from the sidebar to see the graph.")
                else:
                    with st.spinner("Generating graph..."):
                        html_content = graph_utils.create_dependency_graph_figure(
                            st.session_state.raw_objects_list,
                            st.session_state.dependency_graph,
                            st.session_state.selected_schemas
                        )
                        if html_content:
                            components.html(html_content, height=800)

                if st.button("Close", key="close_graph_dialog"):
                    st.rerun()

            # --- Header with Visualize Button ---
            col1, col2 = st.columns([3, 2])
            with col1:
                st.markdown(f"### :orange[:material/data_table: Objects in **{st.session_state.db_selected}**]")
            with col2:
                if st.button(":rainbow[:material/graph_4: Dependency Graph]", use_container_width=True, help="Show the dependency graph for database objects."):
                    dependency_graph_dialog()

            # --- Global Expand/Collapse Toggle ---
            if 'expand_all_toggle' not in st.session_state:
                st.session_state.expand_all_toggle = None # None means use default states

            c1, c2, c3 = st.columns(3)
            if c3.button("Expand/Collapse All", type="tertiary", use_container_width=True, help="Expand/Collapse all schema and object type sections."):
                # If currently in a mixed state or all collapsed, expand all. Otherwise, collapse all.
                if st.session_state.expand_all_toggle is not True:
                    st.session_state.expand_all_toggle = True
                else:
                    st.session_state.expand_all_toggle = False


            with c1:
                st.text_input("Search objects by name", key="search_query", placeholder="e.g., my_table, my_view, ...")
            with c2:
                st.checkbox(f"**Select all objects in {st.session_state.db_selected}**", key=f"DB|" + st.session_state.db_selected, help="Toggles every object in the database.")
                
            search_term = st.session_state.search_query.lower()

            # Define the custom sort order for object types
            top_order = ['SEQUENCE', 'TABLE', 'DYNAMIC TABLE', 'VIEW']
            bottom_order = ['FILE FORMAT', 'STAGE', 'EXTERNAL TABLE', 'PIPE']

            def get_type_sort_key(obj_type: str) -> tuple[int, Any]:
                # Assigns a sort key to an object type for custom ordering.
                if obj_type in top_order: return (0, str(top_order.index(obj_type)).zfill(2))
                if obj_type in bottom_order: return (2, str(bottom_order.index(obj_type)).zfill(2))
                return (1, obj_type)
            
            # --- Display Objects ---
            if not st.session_state.selected_schemas:
                st.warning("Select one or more schemas from the sidebar to see the objects.")
                
            for schema in st.session_state.selected_schemas:
                types_dict = st.session_state.grouped_objects.get(schema, {})

                filtered_types = defaultdict(list)
                schema_object_count = 0
                for obj_type, obj_list in types_dict.items():
                    filtered_list = [o for o in obj_list if search_term in o['object_name'].lower()]
                    if filtered_list:
                        filtered_types[obj_type] = filtered_list
                        schema_object_count += len(filtered_list)

                if not schema_object_count: continue

                # Determine schema expander state
                schema_expanded = True if st.session_state.expand_all_toggle is None else st.session_state.expand_all_toggle

                with st.expander(f":yellow[Schema: **{schema}**] ({schema_object_count} objects)", expanded=schema_expanded):
                    sch_key = f"SCH|" + st.session_state.db_selected + "|" + schema
                    st.checkbox(f"Select all in **{schema}**", key=sch_key, help=f"Toggles all objects in the {schema} schema.")
                    st.markdown("---")

                    for obj_type, obj_list in sorted(filtered_types.items(), key=lambda item: get_type_sort_key(item[0])):
                        # Determine object type expander state
                        obj_type_expanded = False if st.session_state.expand_all_toggle is None else st.session_state.expand_all_toggle

                        with st.expander(f"{sql_parser.get_material_icon(obj_type)} {obj_type.upper()}S ({len(obj_list)})", expanded=obj_type_expanded):
                            for obj in obj_list:
                                st.checkbox(obj['object_name'], key=obj['obj_key'])

    else:
        st.info("Select a database from the dropdown menu in the sidebar to begin.")
else:
    # Show login form if not in Snowflake and not logged in
    if not st.session_state['is_snowflake']:
        with st.container():
            st.markdown("<span style='position: absolute; top: 8%; left: 10%;'>**`(External)`**</span>", unsafe_allow_html=True)
        login_ui.show_login_form()
        #st.rerun()  # Force page refresh to show main UI
    else:
        # This shouldn't happen, but just in case
        st.error("Unexpected state: Running in Snowflake but not logged in.")
        
# streamlit run Projects/Streamlit/SF_DDL_Extractor/app.py