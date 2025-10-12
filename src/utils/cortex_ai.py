import time
import json
import streamlit as st
from datetime import datetime
from collections import defaultdict
from typing import Dict, Any, Optional
import streamlit.components.v1 as components

# Helper function to create a serializable dictionary of the app's state for the LLM
@st.cache_data(show_spinner=False)
def chunkstring(string, length):
    return (string[0+i:length+i] for i in range(0, len(string), length))

def get_app_context():
    serializable_state = {}
    # A set of keys to exclude from the context provided to the LLM.
    # This includes internal Streamlit keys, keys for the chat history itself,
    # and keys that might contain large, complex, or sensitive objects.
    excluded_keys = {'chat_messages', 'preserved_checkbox_states', 'prev_include_schema_ddl', 'script_source_keys'}
    for key, value in st.session_state.items():
        # Skip internal keys, chat history, and other excluded keys
        if key.startswith(('__', 'prev__')) or key in excluded_keys: # type:ignore
            continue

        # Special handling for the live Snowflake session object
        if key == 'snowflake_session' and value is not None:
            try:
                serializable_state['snowflake_session_details'] = {
                    'current_account': value.get_current_account(),
                    'current_user': value.get_current_user(),
                    'current_role': value.get_current_role(),
                    'current_warehouse': value.get_current_warehouse(),
                    'current_database': value.get_current_database(),
                    'current_schema': value.get_current_schema(),
                }
            except Exception as e:
                serializable_state['snowflake_session_details'] = f"Could not retrieve session details: {e}"
            continue

        # For all other keys, try to add them if they are serializable
        try:
            # Test if the value is JSON serializable
            json.dumps(value)
            serializable_state[key] = value
        except (TypeError, OverflowError):
            # If not serializable, add a string representation indicating its type
            serializable_state[key] = f"Non-serializable object of type: {type(value).__name__}"
    return serializable_state

@st.cache_data(show_spinner=False)
def cortex_search_context(prompt):
    prompt = prompt.replace("'", "''").replace('"', '""')
    escaped_context = str(json.dumps(get_app_context(), indent=2, default=str)).replace("'", "''")
    
    if len(escaped_context) > 134200000:
        raw_chunks = list(chunkstring(escaped_context, 134200000))
    else:
        raw_chunks = [escaped_context]
    
    for chunk in raw_chunks:
        st.session_state["snowflake_session"].sql(f"INSERT INTO {st.session_state.context_table} (SESSION_CONTEXT) VALUES ('{chunk}')").collect()
        
    st.session_state["snowflake_session"].sql(f"ALTER CORTEX SEARCH SERVICE {st.session_state.search_service} REFRESH;").collect()
    
    search_params = '{"query": "Key points for - ' + prompt + '", "limit": '+ str(len(raw_chunks)) +'}'
    response_df = st.session_state['snowflake_session'].sql(
                        f"""SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                                    '{st.session_state.search_service}', 
                                    '{search_params}'
                                )"""
                            ).collect()
    response = response_df[0][0].strip('"') if response_df else "Sorry, I couldn't generate a response."
    return response

def get_ai_response(prompt):
    # Build context from full chat history
    search_context = cortex_search_context(prompt)
    prompt = prompt.replace("'", "''")
    full_prompt = f"""
            [INST]
            You are an expert assistant for the Snowflake-DDL-Extractor Streamlit application.
            Your task is to answer questions based ONLY on the provided application state context and previous Chat History of the session messages.
            Do not make up information. If the answer is not in the context, say so.
            Be concise and helpful. Answer in AS LESS WORDS AS POSSIBLE.

            Here is the current application state info:
            {search_context}

            ---
            Question: {prompt}
            [/INST]
            """
    escaped_prompt = full_prompt
    response_df = st.session_state['snowflake_session'].sql(
                        f"SELECT SNOWFLAKE.CORTEX.AI_COMPLETE('{st.session_state["ai_modal"]}', '{escaped_prompt}')"
                ).collect()
    response = response_df[0][0].strip('"') if response_df else "Sorry, I couldn't generate a response."
    return response

def show_chatbot():
    try:
        
        st.session_state["snowflake_session"].sql(f"ALTER CORTEX SEARCH SERVICE IF EXISTS {st.session_state.search_service} RESUME SERVING;").collect()
        
        st.session_state["promp_allowed"] = True
        
        with st.popover("Talk to Doodloo!", icon="🤖"):
            # if "dummy_counter" not in st.session_state:
            #     st.session_state.dummy_counter = 0
            def stream_data(txt):
                for word in txt.split(" "):
                    yield word + " "
                    time.sleep(0.10)
            nonce = 0  
            chat_container = st.container()
            
            prompt = st.chat_input("Ask me anything!", key="prompt_key")
            a, b, c = st.columns(3)
            with a:
                st.text_input("Search Service", st.session_state.search_service, key="search_service", help="Choose an existing Cortex Search Service for your query. Refer you Snowflake Account for available Services.")
            with b:
                st.text_input("Context Table", st.session_state.context_table, key="context_table", help="Choose a Table associated with selected Search Service for looking up Contextual Data with column SESSION_CONTEXT.")
            with c:
                st.text_input("AI Modal", st.session_state.ai_modal, key="ai_modal", help="Choose an AI MOdal for your query. Available modals are - claude-4-opus, claude-4-sonnet, claude-3-7-sonnet, claude-3-5-sonnet, deepseek-r1, gemma-7b, jamba-1.5-mini, jamba-1.5-large, jamba-instruct, llama2-70b-chat, llama3-8b, llama3-70b, llama3.1-8b, llama3.1-70b, llama3.1-405b, llama3.2-1b, llama3.2-3b, llama3.3-70b, llama4-maverick, llama4-scout, mistral-large, mistral-large2, mistral-7b, mixtral-8x7b, openai-gpt-4.1, openai-o4-mini, reka-core, reka-flash, snowflake-arctic, snowflake-llama-3.1-405b, snowflake-llama-3.3-70b.  :blue[Check https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql#label-cortex-llm-cost-considerations for pricing and availability]")
            
            
            st.markdown("<div id='last_msg'></div>", unsafe_allow_html=True)
            if prompt:
                st.session_state.chat_messages.append({"role": "user", "content": prompt})
                # Generate response
                with st.spinner("Thinking..."):
                    response_content = get_ai_response(prompt)
                    st.session_state.chat_messages.append({"role": "assistant", "content": response_content})
                    
            with chat_container:
                for i, msg in enumerate(st.session_state.chat_messages):
                    with st.chat_message(msg["role"]):
                        nonce = len(st.session_state.chat_messages) #st.session_state.dummy_counter
                        components.html(f"""
                        <script>
                            // nonce to force re-exec each rerun: {nonce}
                            (function scrollIntoViewInPopover(tries) {{
                                try {{
                                const doc = window.parent?.document;
                                if (!doc) {{
                                    if (tries > 0) setTimeout(() => scrollIntoViewInPopover(tries - 1), 50);
                                    return;
                                }}
                                // Find the popover dialog and its scrollable content
                                const dialog = doc.querySelector('div[role="dialog"]');
                                const container = dialog?.querySelector('[data-testid="stPopoverContent"]') || dialog || doc.scrollingElement || doc.documentElement;
                                const anchor = doc.getElementById('last_msg');
                                if (anchor) {{
                                    // Smooth scroll to the anchor
                                    anchor.scrollIntoView({{ behavior: "smooth", block: "end" }});
                                    // Fallback: force container to bottom (helps when anchor is inside)
                                    if (container) {{
                                        container.scrollTop = container.scrollHeight;
                                        }}
                                    }} else if (tries > 0) {{
                                        setTimeout(() => scrollIntoViewInPopover(tries - 1), 50);
                                        }}
                                }} catch (e) {{
                                // Retry quietly if DOM not ready
                                if (tries > 0) setTimeout(() => scrollIntoViewInPopover(tries - 1), 50);
                                }}
                            }})(40); // ~2s total retry window
                        </script>
                """,   
                height=0   ,)

                        if msg["role"] == "assistant" and i == len(st.session_state.chat_messages) - 1:
                            st.write_stream(stream_data(msg["content"]))
                        else:
                            st.markdown(msg["content"])
    except Exception as e:
        st.error(f"Error occurred in getting response from Modal {st.session_state.ai_modal} : {e}")
        st.session_state["promp_allowed"] = False
        st.session_state["snowflake_session"].sql(f"ALTER CORTEX SEARCH SERVICE IF EXISTS {st.session_state.search_service} SUSPEND SERVING;").collect()
        print(e)
        import traceback
        traceback.print_exception(type(e), e, e.__traceback__)