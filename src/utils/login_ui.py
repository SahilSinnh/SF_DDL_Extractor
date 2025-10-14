import streamlit as st
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.backends import default_backend as crypto_default_backend

def show_login_form():
    l, c, r = st.columns([2, 2.2, 2])
    with c:
        with st.container():
            # CSS for blurring the UI
            st.info(":material/info: Running **externally**. Please log into a **Snowflake** account to continue.")
            st.markdown("""
                <style>
                .blur {
                    filter: blur(5px);
                    pointer-events: none;
                    user-select: none;
                }
                </style>
            """, unsafe_allow_html=True)

            # Initialize session state variables if they don't exist
            if 'is_loading' not in st.session_state:
                st.session_state['is_loading'] = False
            if 'login_error' not in st.session_state:
                st.session_state['login_error'] = None

            # If the app is in the 'loading' state, attempt the login.
            # This logic runs *after* the button click and st.rerun().
            if st.session_state['is_loading']:
                with st.spinner("## :gray[Logging in...]"):
                    login_error = None
                    try:
                        account = st.session_state.get('account', '')
                        user = st.session_state.get('user', '')
                        warehouse = st.session_state.get('warehouse', '')
                        role = st.session_state.get('role', '')
                        auth_method = st.session_state.get('auth_method', 'Basic')

                        connection_params = {
                            "account": account,
                            "user": user,
                            "warehouse": warehouse,
                            "role": role
                        }

                        if auth_method == 'Basic':
                            connection_params["password"] = st.session_state.get('password', '')
                        elif auth_method == 'Key-Based':
                            key_option = st.session_state.get('key_option', 'Key Content')
                            private_key_data = None
                            if key_option == 'Key Content':
                                key_content = st.session_state.get('key_content', '')
                                if key_content:
                                    private_key_data = key_content.encode('utf-8')
                                else:
                                    login_error = "Please provide private key content."
                            else:  # Key File
                                if 'key_file' in st.session_state and st.session_state['key_file'] is not None:
                                    private_key_data = st.session_state['key_file'].getvalue()
                                else:
                                    login_error = "Please upload a key file."
                            
                            if private_key_data and not login_error:
                                pkb = crypto_serialization.load_pem_private_key(
                                    private_key_data, password=None, backend=crypto_default_backend()
                                ).private_bytes(
                                    encoding=crypto_serialization.Encoding.DER,
                                    format=crypto_serialization.PrivateFormat.PKCS8,
                                    encryption_algorithm=crypto_serialization.NoEncryption()
                                )
                                connection_params["private_key"] = pkb
                        
                        elif auth_method == 'Single Sign-On (SSO)':
                            connection_params["authenticator"] = "externalbrowser"
                        
                        if not login_error:
                            session = Session.builder.configs(connection_params).create()
                            st.session_state['snowflake_session'] = session
                            st.session_state['logged_in'] = True
                            st.session_state['login_error'] = None # Clear any previous errors

                    except Exception as e:
                        login_error = f"Login failed: {e}"

                    # Clean up sensitive data from session state
                    for key in ['password', 'key_content', 'key_file']:
                        if key in st.session_state:
                            del st.session_state[key]
                    
                    st.session_state['is_loading'] = False # Reset loading state
                    st.session_state['login_error'] = login_error # Store error message
                    st.rerun() # Rerun to either show the main app or the login form with an error

            # Display any login error that was stored in the session state
            if st.session_state['login_error']:
                st.error(st.session_state['login_error'])

            # Determine if the UI should be blurred
            container_class = "blur" if st.session_state['is_loading'] else ""

            # The main container for the form
            main_container = st.container()
            main_container.markdown(f'<div class="{container_class}">', unsafe_allow_html=True)

            # All form elements are now inside the main_container
            with main_container:
                st.title(":violet[Login to Snowflake]")
                st.text_input(":blue[Account Identifier]", key='account', help="...")
                auth_method = st.selectbox(":blue[Authentication Method]", ['Basic', 'Key-Based', 'Single Sign-On (SSO)'], key='auth_method', help="...")
                st.text_input(":blue[User Login Name]", key='user', help="...")

                if auth_method == 'Basic':
                    st.text_input(":blue[Password]", type='password', key='password', help="...")
                elif auth_method == 'Key-Based':
                    key_option = st.radio(":blue[Key Input Method]", ['Key Content', 'Key File'], key='key_option', horizontal=True)
                    if key_option == 'Key Content':
                        st.text_area(":blue[Private Key Content (PEM format)]", key='key_content', help="...")
                    else:
                        st.file_uploader(":blue[Upload Private Key File]", type=['pem'], key='key_file', help="...")
                
                st.text_input(":blue[Role Name]", key='role', help="...")
                st.text_input(":blue[Warehouse]", key='warehouse', help="...")

                button_text = "Login with SSO" if auth_method == 'Single Sign-On (SSO)' else "Login"
                
                # The button now ONLY sets the loading state and triggers a rerun.
                if st.button(f":green[**:material/login: {button_text}**]", use_container_width=True):
                    st.session_state['is_loading'] = True
                    st.session_state['login_error'] = None # Clear previous errors
                    st.rerun()

            main_container.markdown('</div>', unsafe_allow_html=True)