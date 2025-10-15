import json
import streamlit as st
from typing import Any, Dict, List

# -----------------------------
# 1) SESSION & SMALL HELPERS
# -----------------------------

def stream_text(text: str):
    # Stream plain text token-by-token for nicer UX. Use st.write_stream(stream_text(text)).
    # Chunk by words to keep it responsive but smooth.
    for token in text.split(" "):
        yield token + " "
        
# Helper function to create a serializable dictionary of the app's state for the LLM
@st.cache_data(show_spinner=False)
def chunkstring(string, length):
    return (string[0+i:length+i] for i in range(0, len(string), length))


# -----------------------------
# 2) CORTEX WRAPPERS
# -----------------------------

def cortex_complete(model: str, prompt: str) -> str:
    # Run Snowflake Cortex Complete:
    #   SELECT SNOWFLAKE.CORTEX.COMPLETE(:model, :prompt)
    try:
        model, prompt = model.replace("'", ""), prompt.replace("'", "")
        # Named-arg signature.
        try_sql = (
            f"SELECT SNOWFLAKE.CORTEX.AI_COMPLETE('{model}', '{prompt}') AS R"
        )
        df = st.session_state['snowflake_session'].sql(try_sql)
        row = df.collect()[0]
    except Exception as e:
        if "exceed" in str(e):
            "Try querying with lower context length limit."
        st.exception(e)
        return ""
    
    text = row[0]
    if text is None:
        return ""
    return str(text)

# -----------------------------
# 3) CONTEXT & PROMPT ORCHESTRATION
# -----------------------------

def get_relevant_session_context():
    try:
        ctx = {}
        res = "# INFORMATION -"
        ctx['Is user logged into Snowflake?'] = st.session_state.get('logged_in', None)
        ctx['Is the app running from Snowflake Native UI / Snowsight'] = st.session_state.get('is_snowflake', None)
        ctx['What is the platform type of current session?'] = st.session_state.get('session_type', None)
        ctx['Current Snowflake Session Object'] = st.session_state.get('snowflake_session', None)
        ctx['Snowflake Account Identifier'] = st.session_state.get('account', None)
        ctx['Username'] = st.session_state.get('user', None)
        ctx['User''s Account Login Name'] = st.session_state['snowflake_session'].get_current_user().strip('"') if st.session_state['snowflake_session'] else None
        ctx['Selected Snowflake Role in App'] = st.session_state.get('role', None)
        ctx['Selected Snowflake Warehouse in App'] = st.session_state.get('warehouse', None)
        ctx['Selected Database (DB)'] = st.session_state.get('db_selected', None) or st.session_state.get('db_selector', None)
        
        dep = """"""
        for o in st.session_state.get('dependency_graph', None).keys():
            dep += f"""{o} is referecing to - """
            if st.session_state.get('dependency_graph', None)[o] != st.session_state.get('dependency_graph', None)[o] != []:
                for i in st.session_state.get('dependency_graph', None)[o]:
                    if i != [st.session_state.get('db_selected', None)]:
                        dep += f"""{i}, """
                dep += """.
                
            """
        ctx['Object Dependency / Refrence of Selected Database Objects'] = dep
        grp = """"""
        for s in st.session_state.get('grouped_objects', {}).keys():
            for t in st.session_state['grouped_objects'][s].keys():
                grp += f"""{t}(s) in {s} schema - """
                for o in st.session_state['grouped_objects'][s][t]:
                    grp += f"""{o['object_name']}, """
                grp += """.
                """
            grp += """
            """
        ctx['All database Objects Grouped by their Schema and Object Types (Schema>Tye>Object)'] = grp
        ctx['Filter query used in App to Search some objects'] = st.session_state.get('search_query', None)
        ctx['Selected Schemas in the selected Database'] = st.session_state.get('selected_schemas', None)
        ctx['Authentication Method used by User to Sign into Snowflake'] = st.session_state.get('auth_method', None)
        ctx['Private Key Option used in case of Key-Pair based authentication method'] = st.session_state.get('key_option', None)
        
        for i in ctx.keys():
            res += f"""- ** {i} ** : `{str(ctx[i]).replace("'", "").replace("\n", """
            """).replace("\t", "   ").replace('\\', '')}`
            
            """
        
    except Exception as e:
        st.exception(e)
    return res

def get_chat_history(n_last: int = 5) -> List[Dict[str, str]]:
    # Return the last N chat messages from st.session_state['chat_messages'].
    msgs = st.session_state.get("chat_messages")
    if not isinstance(msgs, list):
        return []
    # Normalize entries to {role, content}
    normalized: List[Dict[str, str]] = []
    for m in msgs[-max(0, n_last):]:
        role = str(m.get("role", "assistant"))
        content = str(m.get("content", ""))
        normalized.append({"role": role, "content": content})
    return normalized

def make_chat_history_summary(model: str, question: str, n_last: int = 8) -> str:
    # Summarize the last N messages as focused context for the current question.

    history = get_chat_history(n_last=n_last)
    
    if len(history) < 2:
        return ""
    
    prompt = f"""
        [INST]
        Based on the chat history below and the question, generate a query that extend the question
        with the chat history provided. The query should be in natural language.
        Answer with only the query. Do not add any explanation.

        <chat_history>
        {json.dumps(history, ensure_ascii=False)}
        </chat_history>
        <question>
        {question}
        </question>
        [/INST]
    """
    summary = cortex_complete(model=model, prompt=prompt)
    return summary.strip()

def create_prompt_from_session_state(question: str, history_summary: str, session_context: Any) -> str:
    # Build a compact, structured prompt reflecting current app state.
    
    sc_txt = session_context
    prompt = f"""
            [INST]
            You are a helpful AI chat assistant with RAG capabilities. When a user asks you a question,
            you will also be given context provided between <context> and </context> tags in Markdown text. Use that context
            with the user's chat history provided in the between <chat_history> and </chat_history> tags
            to provide a summary that addresses the user's question. Ensure the answer is coherent, concise,
            and directly relevant to the user's question.

            If the user asks a generic question which cannot be answered with the given context or chat_history,
            just say "I don't know the answer to that question.

            Do NOT say things like "according to the provided" or "Based on provided". Just tell starighforward answers.
            
            <question>
            {question}
            </question>
            <chat_history>
            {history_summary}
            </chat_history>
            <context>
            {sc_txt}
            </context>
            [/INST]
        """
    return prompt


@st.fragment
def render_bot():
    # Defensive reads with fallbacks
    cortex_models: List[str] = st.session_state.get("cortex_models", []) or []
    
    selected_model: str = st.session_state.get("selected_cortex_model", cortex_models[0] if cortex_models else "")
    
    st.markdown("""<style>
                    div[data-testid="stPopover"]{
                        height: 100%;
                        background: linear-gradient(to right, #0B0C64, #7B0000);
                        border-radius: 9px !important;
                        padding: 10px 10px !important;
                    }
                </style>""", unsafe_allow_html=True)
    
    with st.popover(":material/face_2:", help="Talk to DDLee"):
        try:
            st.markdown("<h2 style='font-family: Papyrus; background: linear-gradient(to right, #0B0C64, #7B0000, #6A0015, #55002A, #3A0045, #1D0030, #000000); -webkit-background-clip: text;-webkit-text-fill-color: transparent;background-clip: text;color: transparent;'><b>DDLee</b>&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;</b></h2>", unsafe_allow_html=True)
            st.markdown("<span style='color: #6A0015;'>Our friendly warehouse bot.</span>", unsafe_allow_html=True)
            # Selection row
            c1, c2, c3 = st.columns(3)
            with c1:
                if len(cortex_models) > 0:
                    new_model = st.selectbox(
                        ":material/network_intel_node:",
                        cortex_models,
                        index=max(0, cortex_models.index(selected_model)) if selected_model in cortex_models else 0,
                        key="selected_cortex_model",
                        help="Cortex AI model for to use in chat.",
                    )
                else:
                    st.warning("No models configured.")
            with c2:
                c2_1, c2_2 = st.columns([6,4])
                with c2_2: placeholder = st.empty()
                with c2_1:
                    if st.toggle(":material/chat_add_on:", value=True, key="use_history", help="Include chat history in context"):
                        placeholder.number_input(
                            ":material/speaker_notes:",
                            min_value = 2,
                            max_value = 12,
                            value = 6,
                            step = 2,
                            key = "n_history_last",
                            help = "Number of messages from chat history to include in context."
                        )
            with c3:
                st.slider(
                    ":material/image_aspect_ratio:",
                    min_value = 500,
                    max_value = 25000,
                    value = 7000,
                    step = 500,
                    key = "n_context_length",
                    help = "Number of characters to include in context. [4 characters ~ 1 token roughly]"
                )
            st.markdown("---")
            
            prompt = st.chat_input("Ask me anything!", key="prompt_key")
            
            if isinstance(prompt, str) and prompt.strip() != "":
                st.session_state.setdefault("chat_messages", [])
                st.session_state.chat_messages.append({"role": "user", "content": prompt})

                with st.spinner(":material/mindfulness: Thinking..."):

                    # Run pipeline
                    mdl = st.session_state.get("selected_cortex_model", "")

                    # Step A: optional history summary
                    use_hist = bool(st.session_state.get("use_history", True))
                    n_hist = int(st.session_state.get("n_history_last", 5))
                    hist_summary = make_chat_history_summary(mdl, prompt, n_last=n_hist) if use_hist else ""
                    
                    # Step B: get session states
                    state_context = get_relevant_session_context()
                    
                    # Step C: build prompt
                    final_prompt = create_prompt_from_session_state(
                        question=prompt,
                        history_summary=hist_summary,
                        session_context=state_context
                    )
                    
                    # Step D: completion
                    answer = cortex_complete(model=mdl, prompt=final_prompt)
                    
                    # Record assistant message
                    st.session_state["chat_messages"].append({"role": "assistant", "content": answer})
                    
            # Chat transcript
            chat_container = st.container()
            with chat_container:
                for i, msg in enumerate(reversed(st.session_state.get("chat_messages", []) or [])):
                        with st.chat_message(msg.get("role", "")):
                            if msg["role"] == "assistant" and i == 0:
                                with st.spinner(":material/stylus_note: "):
                                    st.write_stream(stream_text(msg["content"]))
                            else:
                                st.markdown(msg["content"])
        except Exception as e:
            st.exception(e)
            
def main():
    
    render_bot()
    
# Entry point of the script.
if __name__ == "__main__":
    main()