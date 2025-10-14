# Handles all direct interactions with Snowflake, such as fetching database lists and retrieving DDLs.

import json
import streamlit as st
from typing import List, Tuple, Optional

def list_databases() -> List[str]:
    # Fetches a list of all databases the current role has access to.

    session = st.session_state.get('snowflake_session')
    if not session:
        st.error("No active Snowflake session. Cannot list databases.")
        return []
    
    try:
        rows = session.sql("SHOW DATABASES").collect()
        # Explicitly cast to string to satisfy the type checker
        return sorted([str(r["name"]) for r in rows if r["kind"].lower() == "standard"])
    except Exception as e:
        st.error(f"Failed to list databases: {e}")
        return []

@st.cache_data(show_spinner=False, ttl=900)
def get_database_ddl(db_name: str) -> Tuple[Optional[str], Optional[str]]:
    # Fetches the DDL for an entire database and its stages.
    session = st.session_state.get('snowflake_session')
    if not session:
        st.error(f"No active Snowflake session. Cannot fetch DDL for {db_name}.")
        return None, None
        
    try:
        # GET_DDL is powerful but doesn't include stages, so we fetch them separately.
        # Explicitly cast the result to a string
        ddl_texts = str(session.sql(f"SELECT GET_DDL('DATABASE', '\"{db_name}\"', TRUE)").collect()[0][0])
        
        stage_ddls = ""
        stage_rows = session.sql(f"SHOW STAGES IN DATABASE \"{db_name}\"").collect()
        for r in stage_rows:
            # Construct a simple CREATE STAGE statement as GET_DDL doesn't cover them.
            stage_ddls += f"\nCREATE STAGE \"{r['database_name']}\".\"{r['schema_name']}\".\"{r['name']}\";"
        
        return ddl_texts, stage_ddls
    except Exception as e:
        st.error(f"Error fetching DDL for database '{db_name}': {e}")
        return None, None

@st.cache_data(show_spinner=False, ttl=900)
def get_user():
    # Fetches the current active User name.
    session = st.session_state.get('snowflake_session')
    if not session:
        st.error(f"No active Snowflake session. Cannot get user.")
        return ""
        
    try:
        current_user = str(session.sql("select CURRENT_USER()").collect()[0][0])
        user = {row['property']: row['value'] for row in 
                        session.sql(f"DESC USER {current_user}").collect()
                    }

        username = user["DISPLAY_NAME"] or user["NAME"]
        
        return username
        
    except Exception as e:
        st.error(f"Error getting user: {e}")
        return ""

@st.cache_data(show_spinner=False, ttl=900)
def list_roles(curr_role: str) -> List[str]:
    # Fetches a list of all roles the current user has access to.
    # Caches the result for 15 minutes.
    curr_role = curr_role.strip('"')
    session = st.session_state.get('snowflake_session')
    if not session:
        st.error("No active Snowflake session. Cannot list roles.")
        return []
    
    try:
        row = session.sql("SELECT PARSE_JSON(CURRENT_AVAILABLE_ROLES())").collect()[0][0]
        roles = sorted([str(r) for r in json.loads(row) if isinstance(r, str)])
        
        if curr_role in roles:
            # Remove the current WH (case-insensitive)
            roles = [r for r in roles if r.lower() != curr_role.lower()]
            # Add current WH at the beginning
            roles.insert(0, curr_role)
        
        return roles

    except Exception as e:
        st.error(f"Failed to list roles: {e}")
        return []
    

def list_warehouses(curr_wh: str) -> List[str]:
    # Fetches a list of all Warehouses the current user has access to.
    # Caches the result for 15 minutes.
    curr_wh = curr_wh.strip('"')
    session = st.session_state.get('snowflake_session')
    if not session:
        st.error("No active Snowflake session. Cannot list warehouses.")
        return []
    
    try:
        rows = session.sql("SHOW WAREHOUSES").collect()
        wh = sorted([str(r["name"]) for r in rows])
        
        if curr_wh in wh:
            # Remove the current WH (case-insensitive)
            wh = [w for w in wh if w.lower() != curr_wh.lower()]
            # Add current WH at the beginning
            wh.insert(0, curr_wh)
        
        return wh
        
    except Exception as e:
        st.error(f"Failed to list warehouses: {e}")
        return [] 