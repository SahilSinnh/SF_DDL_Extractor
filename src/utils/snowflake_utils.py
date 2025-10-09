# Handles all direct interactions with Snowflake, such as fetching database lists and retrieving DDLs.

import streamlit as st
from typing import List, Tuple, Optional

@st.cache_data(show_spinner="Listing databases...", ttl=900)
def list_databases() -> List[str]:
    # Fetches a list of all databases the current role has access to.
    # Caches the result for 15 minutes.

    session = st.session_state.get('snowflake_session')
    if not session:
        st.error("No active Snowflake session. Cannot list databases.")
        return []
    
    try:
        rows = session.sql("SHOW DATABASES").collect()
        # Explicitly cast to string to satisfy the type checker
        return sorted([str(r["name"]) for r in rows])
    except Exception as e:
        st.error(f"Failed to list databases: {e}")
        return []

@st.cache_data(show_spinner=False, ttl=300)
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