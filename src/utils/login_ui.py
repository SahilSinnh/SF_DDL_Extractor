import streamlit as st
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.backends import default_backend as crypto_default_backend

def show_login_form():
    l, c, r = st.columns([1, 5, 1])
    with c:
        # CSS for blurring the UI
        st.markdown("""
            <style>
            .blur {
                filter: blur(5px);
                pointer-events: none;
            }
            </style>
        """, unsafe_allow_html=True)

        # Initialize loading state
        if 'is_loading' not in st.session_state:
            st.session_state['is_loading'] = False

        # Apply blur class if loading
        container_class = "blur" if st.session_state['is_loading'] else ""
        with st.container():
            st.markdown(f'<div class="{container_class}">', unsafe_allow_html=True)
            st.title(":violet[Login to Snowflake]")
            st.text_input(":blue[Account Identifier]", key='account', help="Enter your Snowflake account identifier. This can be your organization-account name (e.g., myorg-myaccount) or a legacy account locator (e.g., xy12345). Do not include '.snowflakecomputing.com'.")
            auth_method = st.selectbox(":blue[Authentication Method]", ['Basic', 'Key-Based', 'Single Sign-On (SSO)'], key='auth_method', help="Choose your preferred authentication method. Below options will adjust based on your selection.")
            st.text_input(":blue[User Login Name]", key='user', help="Enter your Snowflake user login name.")
            
            if auth_method == 'Basic':
                st.text_input(":blue[Password]", type='password', key='password', help="Enter your Snowflake password.")
            elif auth_method == 'Key-Based':
                key_option = st.radio(":blue[Key Input Method]", ['Key Content', 'Key File'], key='key_option', horizontal=True)
                if key_option == 'Key Content':
                    st.text_area(":blue[Private Key Content (PEM format)]", key='key_content', help="Paste your private key in PEM format here.")
                else:
                    st.file_uploader(":blue[Upload Private Key File]", type=['pem'], key='key_file', help="Upload your private key file in PEM format here.")
            # No additional fields for SSO
            
            st.text_input(":blue[Role Name]", key='role', help="Enter your preferred Snowflake role name.")
            st.text_input(":blue[Warehouse]", key='warehouse', help="Enter your preferred Snowflake warehouse name.")

            button_text = "Login with SSO" if auth_method == 'Single Sign-On (SSO)' else "Login"
            
            if st.button(f":green[:material/login: {button_text}]", disabled=st.session_state['is_loading'], use_container_width=True):
                
                account = st.session_state.get('account', '')
                user = st.session_state.get('user', '')
                warehouse = st.session_state.get('warehouse', '')
                role = st.session_state.get('role', '')

                connection_params = {
                    "account": account,
                    "user": user,
                    "warehouse": warehouse,
                    "role": role
                }
                
                login_error = None

                if auth_method == 'Basic':
                    password = st.session_state.get('password', '')
                    connection_params["password"] = password
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
                    
                    if private_key_data:
                        try:
                            private_key = crypto_serialization.load_pem_private_key(
                                private_key_data,
                                password=None,
                                backend=crypto_default_backend()
                            )
                            pkb = private_key.private_bytes(
                                encoding=crypto_serialization.Encoding.DER,
                                format=crypto_serialization.PrivateFormat.PKCS8,
                                encryption_algorithm=crypto_serialization.NoEncryption()
                            )
                            connection_params["private_key"] = pkb
                        except Exception as e:
                            login_error = f"Error loading private key: {e}"
                
                elif auth_method == 'Single Sign-On (SSO)':
                    connection_params["authenticator"] = "externalbrowser"

                if login_error:
                    st.error(login_error)
                else:
                    with st.spinner("Logging in..."):
                        try:
                            session = Session.builder.configs(connection_params).create()
                            st.session_state['snowflake_session'] = session
                            st.session_state['logged_in'] = True
                            st.rerun()
                        except Exception as e:
                            st.error(f"Login failed: {e}")

            st.markdown('</div>', unsafe_allow_html=True)
