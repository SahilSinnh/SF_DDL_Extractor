# The main Streamlit application file.
# Imports
import toml
import traceback
import streamlit as st
from datetime import datetime
from collections import defaultdict
from typing import Dict, Any, Optional
import streamlit.components.v1 as components
from snowflake.snowpark.context import get_active_session

# Import utils
import utils.snowflake_utils as sf
import utils.sql_parser as sql_parser
import utils.dependencies as dependencies
import utils.graph_utils as graph_utils
import utils.login_ui as login_ui
import utils.cortex_ai

snowflake_logo_path = "assets/icons/snowflake-logo.svg"     # Sidebar Header Icon
streamlit_logo_path = "assets/icons/streamlit-logo.svg"     # Main Page Icon
about_file_path = "src/utils/about.md"                      # About/Help content
config_file_path = "src/config.toml"                        # Cortex AI config environments

# ----------------------------->
# STATE MANAGEMENT
# ----------------------------->

# Initializes the session, checking for an active Snowflake connection.
def initialize_session():
    st.query_params["sf"] = "None" if "sf" not in st.query_params else st.query_params["sf"]
    try:
        # Attempt to get an active Snowflake session.
        st.session_state['snowflake_session'] = get_active_session()
        st.session_state['logged_in'] = True
        
        if st.query_params["sf"] == "None" or st.query_params["sf"] == "True":
            st.query_params["sf"] = "True"
        
        st.session_state['account'] = st.session_state['snowflake_session'].get_current_account().strip('"').upper()
        
        st.session_state['user'] = sf.get_user() or str(st.session_state['snowflake_session'].get_current_user().strip('"'))
        
        if "role_changed" not in st.session_state:
            st.session_state['role_changed'] = False
        
        if 'role_list' not in st.session_state:
            st.session_state['role_list'] = sf.list_roles(st.session_state['snowflake_session'].get_current_role().strip('"'))
        if st.session_state['snowflake_session'].get_current_role().strip('"') not in st.session_state['role_list']:
            for r in st.session_state['role_list']:
                try:
                    st.session_state['snowflake_session'].use_role(r)
                    st.toast(f'Selected role {st.session_state['snowflake_session'].get_current_role()} is not available. Switching to "{r}".', duration = "long")
                    st.session_state['role'] = r
                    break
                except Exception:
                    continue
        else:
            st.session_state['role'] = st.session_state['snowflake_session'].get_current_role().strip('"')
        
        if 'wh_list' not in st.session_state or st.session_state['role_changed']:
            st.session_state['wh_list'] = sf.list_warehouses(st.session_state['snowflake_session'].get_current_warehouse().strip('"'))
        if st.session_state['snowflake_session'].get_current_warehouse().strip('"') not in st.session_state['wh_list']:
            for w in st.session_state['wh_list']:
                try:
                    st.session_state['snowflake_session'].use_warehouse(w)
                    st.toast(f'Selected warehouse {st.session_state['snowflake_session'].get_current_warehouse()} is not available. Switching to "{w}".', duration = "long")
                    st.session_state['warehouse'] = w
                    break
                except Exception:
                    continue
        else:
            st.session_state['warehouse'] = st.session_state['snowflake_session'].get_current_warehouse().strip('"')
        
        if 'db_list' not in st.session_state or st.session_state['role_changed']:
            st.session_state['db_list'] = sf.list_databases()
        
        # Cortex AI Chatbot session state keys
        # Load configuration from a TOML file.
        try:
            if 'cortex_models' not in st.session_state or 'context_tables' not in st.session_state or 'cortex_services' not in st.session_state:
                config = toml.load(config_file_path)
                cortex_config = config.get("cortex", {})
        except FileNotFoundError:
            cortex_config = {}
        if 'chat_messages' not in st.session_state:
            st.session_state['chat_messages'] = [],
        if 'cortex_models' not in st.session_state:
            st.session_state['cortex_models'] = cortex_config.get("models", [])
        if 'context_tables' not in st.session_state:
            st.session_state['context_tables'] = cortex_config.get("tables", [])
        if 'cortex_services' not in st.session_state:
            st.session_state['cortex_services'] = cortex_config.get("services", [])
        if 'selected_cortex_model' not in st.session_state:
            st.session_state['selected_cortex_model'] = st.session_state['cortex_models'][0] if st.session_state['cortex_models'] else ""
        if 'selected_context_table' not in st.session_state:
            st.session_state['selected_context_table'] =   st.session_state['context_tables'][0] if st.session_state['context_tables'] else ""
        if 'selected_cortex_service' not in st.session_state:
            st.session_state['selected_cortex_service'] =  st.session_state['cortex_services'][0] if st.session_state['cortex_services'] else ""
        
    except Exception:
        # Handle cases where no active session is found.
        st.session_state['logged_in'] = False
        st.query_params["sf"] = "False"
    
    st.session_state['is_snowflake'] = st.query_params["sf"] == "True"
    if st.session_state['is_snowflake']:
        st.session_state['session_type'] = "Snowsight"
    else:
        st.session_state['session_type'] = "External"
        
    st.session_state['role_changed'] = False

# Initializes or re-initializes the Streamlit session state variables.
def init_session_state():
    if 'preserved_checkbox_states' in st.session_state:
        # Restore checkbox states after a rerun.
        for key, value in st.session_state.preserved_checkbox_states.items():
            st.session_state[key] = value
        del st.session_state.preserved_checkbox_states

    # Define and initialize default session state keys.
    keys_to_init = {
        "db_selected": None, "objects": [], "raw_objects_list": [],
        "dependency_graph": {}, "grouped_objects": defaultdict(lambda: defaultdict(list)),
        "search_query": "", "final_script_output": "", "script_source_keys": set(),
        "selected_schemas": [],
    }
    for key, value in keys_to_init.items():
        if key not in st.session_state:
            st.session_state[key] = value
            
# Resets the application state, preserving login information.
def reset_app_state():
    account_keys = {
        'snowflake_session', 'logged_in', 'is_snowflake', 'session_type', 
        'auth_method', 'password', 'key_option', 'key_content', 'key_file', 'is_loading', 
        'account', 'user', 'role', 'role_list', 'warehouse', 'wh_list', 'db_list', 'role_changed',
        'chat_messages', 'cortex_models', 'context_tables', 'cortex_services', 
        'selected_cortex_model', 'selected_context_table', 'selected_cortex_service',
    }
    keys_to_clear = [key for key in st.session_state.keys() if key not in account_keys]
    for key in keys_to_clear:
        del st.session_state[key]
        
# Synchronizes the state of checkboxes for database objects.
def sync_checkbox_state(db_key):
    if not st.session_state.objects:
        return

    sch_keys = {o['sch_key'] for o in st.session_state.objects}
    
    source_key, source_value = None, None

    # Determine which checkbox was changed by the user.
    prev_global = st.session_state.get(f"__prev__{db_key}", False)
    cur_global = st.session_state.get(db_key, False)
    if cur_global != prev_global:
        source_key, source_value = db_key, cur_global
    
    if not source_key:
        for sk in sch_keys:
            prev_group = st.session_state.get(f"__prev__{sk}", False)
            cur_group = st.session_state.get(sk, False)
            if cur_group != prev_group:
                source_key, source_value = sk, cur_group
                break

    # Update child checkboxes based on the source change.
    if source_key == db_key:
        selected_schemas = [s for s, selected in st.session_state.get('schema_selection', {}).items() if selected]
        keys_to_update = {o['obj_key'] for o in st.session_state.objects if o.get('schema', 'N/A') in selected_schemas}
        for ok in keys_to_update:
            st.session_state[ok] = source_value
    
    elif source_key in sch_keys:
        child_keys = [o['obj_key'] for o in st.session_state.objects if o['sch_key'] == source_key]
        for ck in child_keys:
            st.session_state[ck] = source_value

    # Update parent checkboxes based on the state of their children.
    for sk in sch_keys:
        child_keys = [o['obj_key'] for o in st.session_state.objects if o['sch_key'] == sk]
        if child_keys:
            st.session_state[sk] = all(st.session_state.get(k, False) for k in child_keys)

    selected_schemas = [s for s, selected in st.session_state.get('schema_selection', {}).items() if selected]
    keys_for_global_check = {o['obj_key'] for o in st.session_state.objects if o.get('schema', 'N/A') in selected_schemas}
    st.session_state[db_key] = all(st.session_state.get(k, False) for k in keys_for_global_check) if keys_for_global_check else False

    # Store the current state for the next comparison.
    st.session_state[f"__prev__{db_key}"] = st.session_state.get(db_key, False)
    for sk in sch_keys:
        st.session_state[f"__prev__{sk}"] = st.session_state.get(sk, False)

# ----------------------------->
# DATA PROCESSING
# ----------------------------->

# Parses raw DDL text into a list of structured object metadata.
def parse_ddl_statements(ddl_text, stage_ddls, selected_db):
    raw_objects = []
    full_ddl_text = (ddl_text or "") + (stage_ddls or "")
    statements = sql_parser.split_sql_statements(full_ddl_text)
    for idx, stmt in enumerate(statements):
        # Remove database references from the create statement
        cleaned_stmt = sql_parser.remove_database_references(stmt, selected_db)
        meta: Optional[Dict[str, Any]] = sql_parser.extract_object_metadata(cleaned_stmt)
        if meta:
            # Augment metadata with additional details.
            if not meta.get("database"): meta["database"] = selected_db
            meta["ddl"] = cleaned_stmt.strip()
            meta["index"] = idx
            raw_objects.append(meta)
    return raw_objects

# Processes the selection of a database, fetching and parsing DDLs.
def process_database_selection(selected_db):
    if st.session_state.db_selected != selected_db:
        reset_app_state()
        init_session_state()
        st.session_state.db_selected = selected_db
        with st.spinner(f"Extracting and parsing DDL for **{selected_db}**... This may take a moment."):
            # Fetch DDLs from Snowflake.
            ddl_text, stage_ddls = sf.get_database_ddl(selected_db)
            if ddl_text is not None:
                # Parse and process the fetched DDL statements.
                raw_objects = parse_ddl_statements(ddl_text, stage_ddls, selected_db)
                sorted_objects, deps = dependencies.order_objects_by_dependencies(raw_objects)
                
                st.session_state.raw_objects_list = sorted_objects
                st.session_state.dependency_graph = deps

                filtered_sorted_objects = [o for o in sorted_objects if o.get("object_type") not in ["DATABASE", "SCHEMA"]]
                
                clean_objects = [{k: v for k, v in o.items() if not k.startswith('_')} for o in filtered_sorted_objects]
                
                # Group objects by schema and type for display.
                grouped = defaultdict(lambda: defaultdict(list))
                for obj in clean_objects:
                    db, sch, obj_name, obj_type = obj.get("database", ""), obj.get("schema", ""), obj.get("object_name", ""), obj.get("object_type", "UNKNOWN")
                    obj['db_key'], obj['sch_key'], obj['obj_key'] = f"DB|{db}", f"SCH|{db}|{sch}", f"OBJ|{db}|{sch}|{obj_type}|{obj_name}"
                    grouped[sch][obj_type].append(obj)

                st.session_state.objects = clean_objects
                st.session_state.grouped_objects = grouped
                st.toast(f":green[Successfully parsed {len(clean_objects)} objects from '{selected_db}'.]", duration = "long")
                

# Checks for and warns about hardcoded database references in the DDL.
def check_and_warn_db_references(selected_objects):
    db_ref_warnings = []
    db_name = st.session_state.db_selected
    final_script_lines = st.session_state.final_script_output.split('\n')

    # Iterate through selected objects to find references.
    for obj in selected_objects:
        ddl_with_semicolon = obj['ddl'] + ";"
        if db_name.lower() in ddl_with_semicolon.lower():
            obj_ddl_lines = ddl_with_semicolon.split('\n')
            matches = []
            
            # Find the line numbers of the references.
            for i, line in enumerate(obj_ddl_lines):
                if db_name.lower() in line.lower():
                    obj_line_number = i + 1
                    final_line_number = None
                    for j, fs_line in enumerate(final_script_lines):
                        if line.strip() == fs_line.strip():
                            is_match = True
                            if i > 0 and j > 0 and obj_ddl_lines[i-1].strip() != final_script_lines[j-1].strip():
                                is_match = False
                            if i < len(obj_ddl_lines)-1 and j < len(final_script_lines)-1 and obj_ddl_lines[i+1].strip() != final_script_lines[j+1].strip():
                                is_match = False
                            if is_match:
                                final_line_number = j + 1
                                break
                    matches.append({
                        'ddl_line_number': obj_line_number,
                        'script_line_number': final_line_number if final_line_number else 'Not found',
                        'line_content': line.strip()
                    })

            # Build a code snippet to show the context of the reference.
            snippet = ""
            if matches:
                matches.sort(key=lambda m: m['ddl_line_number'])
                snippet_parts = []
                current_block = []
                last_end = -2
                for match in matches:
                    ddl_line = match['ddl_line_number'] - 1
                    if ddl_line - last_end > 1 and current_block:
                        snippet_parts.append(list(current_block))
                        current_block = []
                    start = max(0, ddl_line - 1)
                    end = min(len(obj_ddl_lines), ddl_line + 2)
                    for k in range(start, end):
                        if k not in [item[0] for item in current_block]:
                            for j, fs_line in enumerate(final_script_lines):
                                if obj_ddl_lines[k].strip() == fs_line.strip():
                                    is_context_match = True
                                    if k > 0 and j > 0 and obj_ddl_lines[k-1].strip() != final_script_lines[j-1].strip():
                                        is_context_match = False
                                    if k < len(obj_ddl_lines) - 1 and j < len(final_script_lines) - 1 and obj_ddl_lines[k+1].strip() != final_script_lines[j+1].strip():
                                        is_context_match = False
                                    if is_context_match:
                                        final_k = j + 1
                                        break
                            else:
                                final_k = 'N/A'
                            current_block.append((k, any(m['ddl_line_number'] - 1 == k for m in matches), final_k))
                    last_end = end - 1
                
                if current_block:
                    snippet_parts.append(list(current_block))
                
                if snippet_parts:
                    snippet_lines = []
                    for i, block in enumerate(snippet_parts):
                        block_snippet = sql_parser.build_block_snippet(block, obj_ddl_lines, final_script_lines)
                        snippet_lines.append(block_snippet)
                        if i < len(snippet_parts) - 1:
                            last_line = max(k for k, _, _ in block)
                            next_start = min(k for k, _, _ in snippet_parts[i + 1])
                            separator = "\n" if next_start - last_line <= 1 else "\n...\n"
                            snippet_lines.append(separator)
                    snippet = "".join(snippet_lines)
            
            # Collect warning information.
            warning_info = {
                "object_type": obj.get("object_type", "Object"),
                "fully_qualified_name": obj.get("fully_qualified_name", "Unknown"),
                "matches": matches,
                "snippet": snippet
            }
            db_ref_warnings.append(warning_info)

    # Display warnings if any references were found.
    if db_ref_warnings:
        st.warning(f"**Database Reference Warning:** The script contains hardcoded references to the '{db_name}' database. This may cause issues when deploying to other environments.", icon=":material/warning:")
        with st.expander("Click to see details"):
            for warning in db_ref_warnings:
                num_matches = len(warning['matches'])
                plural = "s" if num_matches > 1 else ""
                st.markdown(f"- **{warning['object_type']}:** `{warning['fully_qualified_name']}`")
                with st.expander(f"{num_matches} occurrence{plural}"):
                    st.code(warning['snippet'], language='sql')

# Generates the final SQL script and displays it in the UI.
def generate_and_display_script(selected_objects):
    current_script_keys = {o['obj_key'] for o in selected_objects}
    if 'include_schema_ddl' not in st.session_state:
        st.session_state.include_schema_ddl = False

    # Regenerate script only if selection or options change.
    if (
        current_script_keys != st.session_state.get('script_source_keys') or
        st.session_state.include_schema_ddl != st.session_state.get('prev_include_schema_ddl')):
        
        base_script = ";\n\n".join([o['ddl'] for o in selected_objects]) + ";"
        if st.session_state.include_schema_ddl:
            # Optionally include CREATE SCHEMA statements.
            distinct_schemas = sorted(list(set(o['schema'] for o in selected_objects if 'schema' in o)))
            schema_ddls = [f'CREATE SCHEMA IF NOT EXISTS "{schema}";' for schema in distinct_schemas]
            st.session_state.final_script_output = "\n".join(schema_ddls) + f"\n\n{base_script}"
        else:
            st.session_state.final_script_output = base_script

        st.session_state.script_source_keys = current_script_keys
        st.session_state.prev_include_schema_ddl = st.session_state.include_schema_ddl
        
    # Check for and warn about hardcoded database references.
    check_and_warn_db_references(selected_objects)
    
    st.checkbox("Include Schema DDL", key='include_schema_ddl', help="Adds `CREATE SCHEMA IF NOT EXISTS` statements for all the schemas in the selected objects.")
    
    # Display the generated script in a code block.
    code_container = st.container(height=400)
    code_container.code(st.session_state.final_script_output, language='sql', line_numbers=True)

    # Provide a download button for the script.
    file_name = f"{st.session_state.db_selected}_DDL_Export_{datetime.now().strftime('%Y%m%d%H%M%S')}.sql"
    if st.download_button(
        label="**Download DDL as a .sql File**",
        icon=":material/download_2:",
        data=st.session_state.final_script_output,
        file_name=file_name,
        mime="text/plain",
        width="stretch"
    ):
        st.toast(f":green[Downloaded - **{file_name}**]", icon=":material/download_done:", duration=6)
    
    # Display a helper for generating INSERT statements.
    st.write("")
    st.write("")
    st.info(":material/emoji_objects: To get Insert statement with existing data for a table, run this in the database replacing <table_name> and <schema_name> with your relevant name:")
    st.code('''WITH FormattedRows AS (SELECT f.seq, '(' || LISTAGG(IFF(f.value IS NULL, 'NULL', '''''' || REPLACE(f.value::varchar, '''''''', '''''''''''') || '''''''), ', ') WITHIN GROUP (ORDER BY f.index) || ')' AS RowValue FROM (SELECT ARRAY_CONSTRUCT(*) AS arr FROM <schema_name>.<table_name>), LATERAL FLATTEN(INPUT => arr) f GROUP BY f.seq) SELECT 'INSERT INTO '||SCHEMA_NAME||'.'||TABLE_NAME||' (' || (SELECT LISTAGG(COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '<schema_name>' AND TABLE_NAME = '<table_name>') || ') VALUES ' || LISTAGG(RowValue, ', ') || ';' FROM FormattedRows;''', language='sql')

# ----------------------------->
# DIALOGS
# ----------------------------->

# Defines a dialog to show the "About" information from a markdown file.
@st.dialog("About!", width="large")
@st.fragment
def about_dialog():
    with open(about_file_path, "r") as f:
        about_text = f.read()
    st.markdown(about_text, unsafe_allow_html=True)

# Defines a dialog to change the current Snowflake role.
@st.dialog("Select Role")
@st.fragment
def change_role():
    selected_role = st.selectbox(
        f":blue[**:material/assignment_ind: Roles**]",
        st.session_state['role_list'],
        index=0,
        key='role_selector'
    )
    if st.button("Submit"):
        st.session_state['snowflake_session'].use_role(selected_role)
        st.session_state['role'] = selected_role
        st.session_state['role_changed'] = True
        st.toast(f":material/info: Switching to Role - {selected_role}.", duration = 6)
        st.rerun()

# Defines a dialog to change the current Snowflake warehouse.
@st.dialog("Select Warehouse")
@st.fragment
def change_warehouse():
    selected_warehouse = st.selectbox(
        f":blue[**:material/select_all: Warehouses**]",
        st.session_state['wh_list'],
        index=0,
        key='wh_selector'
    )
    if st.button("Submit"):
        st.session_state['snowflake_session'].use_warehouse(selected_warehouse)
        st.session_state['snowflake_session'] = selected_warehouse
        st.toast(f":material/info: Switching to Warehouse - {selected_warehouse}.", duration = 6)
        st.rerun()
        
# Defines a dialog to display the dependency graph of database objects.
@st.dialog(":rainbow[:material/graph_4: Dependency Graph]", width="large")
@st.fragment
def dependency_graph_dialog():
    st.info(f"Showing dependencies among schemas - **{st.session_state.selected_schemas}** in database **{st.session_state.db_selected}**.")
    if not st.session_state.selected_schemas:
        st.warning("No schemas selected. Please select at least one schema from the sidebar to see the graph.")
    else:
        with st.spinner("Generating graph..."):
            # Create and display the dependency graph using utility functions.
            html_content = graph_utils.create_dependency_graph_figure(
                st.session_state.raw_objects_list,
                st.session_state.dependency_graph,
                st.session_state.selected_schemas
            )
            if html_content:
                components.html(html_content, height=800, width=1500)

    if st.button("Close", key="close_graph_dialog"):
        st.rerun()


# ----------------------------->
# UI RENDERING
# ----------------------------->

# Sets up the main page configuration and custom CSS.
def setup_page():
    st.set_page_config(
        page_title="Snowflake DDL Extractor",
        page_icon=":material/ac_unit:",
        initial_sidebar_state="expanded",
        layout = "wide"
    )
    # Custom CSS for the footer and other styles.
    footer_css = """
    <style>
        .main .block-container { padding-bottom: 5rem; }
        .footer {
            position: fixed; left: 0; bottom: 0; width: 100%;
            background-color: rgba(14, 17, 23, 0.65);
            -webkit-backdrop-filter: blur(10px); backdrop-filter: blur(10px);
            color: gray; text-align: right; font-size: 0.9rem; z-index: 9999;
        }
        .rainbow-text {
            background: linear-gradient(to right, #FF4B4B, #FFA500, #FFFF00, #4CAF50, #00BFFF, #4B0082, #EE82EE);
            -webkit-background-clip: text; background-clip: text;
            color: transparent; font-weight: bold;
        }
    </style>
    """
    # HTML for the footer.
    footer_html = f'''
    {footer_css}
    <div class="footer">
        <span style="color: gray;">Created by</span> <a href="https://www.linkedin.com/in/sahil-d-singh/" target="_blank" rel="noopener noreferrer" ><span class="rainbow-text">Sahil Singh</span></a>
    </div>
    '''
    st.html(footer_html)

    # "About" button in the top right corner.
    col1, col2 = st.columns([0.95, 0.05])
    with col2:
        col2_a, col2_b = st.columns(2)
        with col2_a:
            if st.button("", icon=":material/refresh:", help="Refresh Page", key="refresh_button", type="tertiary"):
                st.rerun()
        with col2_b:
            if st.button("", icon=":material/info:", help="About!", key="about_section", type="tertiary"):
                about_dialog()
            
# Renders the header section of the sidebar with session info.
def render_sidebar_header():
    col1, col2 = st.columns([5, 2])
    with col1:
        col1_1, col1_2 = st.columns([1, 4])
        with col1_1:
            st.image(snowflake_logo_path)
        with col1_2:
            st.write("")
            st.html(f"<div><span style='color: #29B5E8; font-weight: bold;'>{st.session_state['account']}<br>({st.session_state['session_type']} Session)</span></div>")
        
        # Display current warehouse and role with buttons to change them.
        col1_a, col1_b = st.columns(2)
        with col1_a:
            st.write("")
            if st.button(f":violet[:material/assignment_ind: **{st.session_state['role']}**]", help="Change active role."):
                change_role()
        with col1_b:
            st.write("")
            if st.button(f":violet[:material/select_all: **{st.session_state['warehouse']}**]", help="Change active warehouse."):
                change_warehouse()
    with col2:
        # Display username and logout button.
        if st.session_state['user']: st.markdown(f"### :rainbow[:material/account_circle: **{st.session_state['user']}**]")
        if not st.session_state['is_snowflake']:
            if st.button("**:material/logout: Log Out**", key="logout_btn"):
                with st.spinner("Logging out..."):
                    st.session_state['snowflake_session'].close()
                    st.toast("Logged out!", icon=":material/logout:", duration = "long")
                    st.cache_data.clear()
                    st.session_state.clear()
                    st.rerun()

# Renders the database selection dropdown in the sidebar.
def render_db_selector():
    db_options = ["— Select a database —"] + st.session_state['db_list']
    return st.selectbox(
        f":orange[**{sql_parser.get_material_icon('DATABASE')} Database**]",
        db_options, index=0, key='db_selector'
    )

# Renders the schema selection grid in the sidebar.
def render_schema_selector():
    st.markdown(f":orange[**{sql_parser.get_material_icon('SCHEMA')} Select Schemas**]")
    all_schemas = sorted(st.session_state.grouped_objects.keys())

    # Initialize schema selection state if not present.
    if 'schema_selection' not in st.session_state or set(st.session_state.schema_selection.keys()) != set(all_schemas):
        st.session_state.schema_selection = {s: True for s in all_schemas}

    # Create a grid of buttons for schema selection.
    grid_items = ["Toggle All"] + all_schemas
    num_cols = 3
    cols = st.columns(num_cols)

    for i, item in enumerate(grid_items):
        with cols[i % num_cols]:
            if item == "Toggle All":
                # "Toggle All" button logic.
                all_selected = all(st.session_state.schema_selection.get(s) for s in all_schemas)
                btn_type = "secondary" if all_selected else "primary"
                if st.button(item, width="stretch", help="Select or deselect all schemas.", type=btn_type):
                    new_state = not all_selected
                    for s in all_schemas:
                        st.session_state.schema_selection[s] = new_state
                    st.rerun()
            else:
                # Individual schema button logic.
                schema = item
                is_selected = st.session_state.schema_selection.get(schema, True)
                btn_type = "primary" if is_selected else "secondary"
                if st.button(schema, key=f"schema_btn_{schema}", width="stretch", type=btn_type):
                    st.session_state.schema_selection[schema] = not is_selected
                    st.rerun()
    
    # Update the list of selected schemas in the session state.
    st.session_state.selected_schemas = [s for s, selected in st.session_state.schema_selection.items() if selected]

# Renders the section for generating and downloading the SQL script.
def render_script_generation_section():
    st.markdown("---")
    st.header("Generated SQL Script")

    selected_objects = [o for o in st.session_state.objects if st.session_state.get(o['obj_key'])]

    if not selected_objects:
        st.info("Select objects in the main page to generate the script.")
        return

    st.success(f"{len(selected_objects)} objects selected. Scroll down to download the SQL script.")
    
    # Generate and display the final SQL script.
    generate_and_display_script(selected_objects)            
            
# Renders the sidebar components.
def render_sidebar():
    with st.sidebar:
        render_sidebar_header()
        st.markdown("---")
        selected_db = render_db_selector()
        if selected_db and selected_db != "— Select a database —":
            # Process selection and render subsequent UI elements.
            process_database_selection(selected_db)
            if st.session_state.objects:
                render_schema_selector()
                render_script_generation_section()
        else:
            # Reset state if no database is selected.
            reset_app_state()
            init_session_state()
            

# Renders an expander for a single schema, containing its objects.
def render_schema_expander(schema, search_term):
    types_dict = st.session_state.grouped_objects.get(schema, {})
    filtered_types = defaultdict(list)
    schema_object_count = 0
    # Filter objects based on the search term.
    for obj_type, obj_list in types_dict.items():
        filtered_list = [o for o in obj_list if search_term in o['object_name'].lower()]
        if filtered_list:
            filtered_types[obj_type] = filtered_list
            schema_object_count += len(filtered_list)

    if not schema_object_count: return

    # Schema expander with object type sub-expanders.
    schema_expanded = False if st.session_state.expand_all_toggle is None else st.session_state.expand_all_toggle
    with st.expander(f"Schema: :red[**{schema}**] ({schema_object_count} objects)", expanded=schema_expanded):
        sch_key = f"SCH|{st.session_state.db_selected}|{schema}"
        st.checkbox(f"Select all in **{schema}**", key=sch_key, help=f"Toggles all objects in the {schema} schema.")
        st.markdown("---")

        # Sort object types for consistent display order.
        def get_type_sort_key(obj_type: str) -> tuple[int, Any]:
            top_order = ['SEQUENCE', 'TABLE', 'DYNAMIC TABLE', 'VIEW']
            bottom_order = ['FILE FORMAT', 'STAGE', 'EXTERNAL TABLE', 'PIPE']
            if obj_type in top_order: return (0, str(top_order.index(obj_type)).zfill(2))
            if obj_type in bottom_order: return (2, str(bottom_order.index(obj_type)).zfill(2))
            return (1, obj_type)

        # Render sub-expanders for each object type.
        for obj_type, obj_list in sorted(filtered_types.items(), key=lambda item: get_type_sort_key(item[0])):
            obj_type_expanded = False if st.session_state.expand_all_toggle is None else st.session_state.expand_all_toggle
            with st.expander(f"{sql_parser.get_material_icon(obj_type)} {obj_type.upper()}S ({len(obj_list)})", expanded=obj_type_expanded):
                for obj in obj_list:
                    st.checkbox(obj['object_name'], key=obj['obj_key'])


# Renders the area for displaying and selecting database objects.
def render_object_display_area():
    col1, col2 = st.columns([3, 2])
    with col1:
        st.markdown(f"### :orange[:material/data_table: Objects in **{st.session_state.db_selected}**]")
    with col2:
        if st.button(":rainbow[:material/graph_4: Dependency Graph]", width="stretch", help="Show the dependency graph for database objects."):
            dependency_graph_dialog()

    # "Expand/Collapse All" button for object sections.
    if 'expand_all_toggle' not in st.session_state:
        st.session_state.expand_all_toggle = None
    
    exp_col_bttn_title = ":material/expand_all: Expand All" if st.session_state.expand_all_toggle is not True else ":material/collapse_all: Collapse All"
    c1, c2, c3 = st.columns(3)
    if c3.button(exp_col_bttn_title, type="tertiary", width="stretch", help=f"{exp_col_bttn_title} schema and object type sections."):
        st.session_state.preserved_checkbox_states = {o['obj_key']: st.session_state.get(o['obj_key'], False) for o in st.session_state.objects}
        st.session_state.expand_all_toggle = st.session_state.expand_all_toggle is not True
        st.rerun()

    # Search and "Select All" controls.
    with c1:
        st.text_input("Search objects by name", key="search_query", placeholder="e.g., my_table, my_view, ...")
    with c2:
        st.checkbox(f"**Select all objects in {st.session_state.db_selected}**", key=f"DB|{st.session_state.db_selected}", help="Toggles every object in the database.")
        
    search_term = st.session_state.search_query.lower()

    if not st.session_state.selected_schemas:
        st.warning("Select one or more schemas from the sidebar to see the objects.")
        return

    # Render an expander for each selected schema.
    for schema in st.session_state.selected_schemas:
        render_schema_expander(schema, search_term)

# Renders the main content area of the application.
def render_main_area():
    co1, co2 = st.columns([6, 1])
    with co1:
        st.title(":violet[:material/ac_unit: Snowflake DDL Extractor]")
        st.markdown(":violet[**A tool to extract, parse, and download object DDLs from a Snowflake database.**]")
    with co2:
        st.write("")
        st.write("")
        st.image(streamlit_logo_path, caption=st.__version__, width=40)
    st.markdown("---")

    # Display object details or a prompt to select a database.
    if st.session_state.db_selected and st.session_state.db_selected != "— Select a database —":
        if st.session_state.objects:
            render_object_display_area()
        else:
            st.warning("No objects found or parsed for the selected database.")
    else:
        st.info("Select a database from the dropdown menu in the sidebar to begin.")


# The main function that runs the Streamlit application.
def main():
    
    setup_page()
    
    # Initialize session state if not already done.
    initialize_session()
    
    # Main application logic for logged-in users.
    if st.session_state['logged_in']:
        init_session_state()
        
        # AI Chatbot feature.
        #show_chatbot()
    
        # Synchronize checkbox states if a database is selected.
        if st.session_state.db_selected and st.session_state.db_selected != "— Select a database —":
            db_key = f"DB|{st.session_state.db_selected}"
            sync_checkbox_state(db_key)
        
        # Render the main UI components.
        render_sidebar()
        render_main_area()
        
    else:
        # Show login form for external sessions.
        if not st.session_state['is_snowflake']:
            login_ui.show_login_form()
        else:
            st.error("Unexpected state: Running in Snowflake but not logged in.")

# Entry point of the script.
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(e)
        st.error(traceback.format_exc())