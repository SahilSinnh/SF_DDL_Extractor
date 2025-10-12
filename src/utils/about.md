
# Snowflake DDL Extractor

A Streamlit-based web application for extracting, parsing, and downloading Data Definition Language (DDL) scripts from Snowflake databases and visualizing object dependencies.

## Key Features

- **Secure Snowflake Connection**: Connect to your Snowflake account using basic authentication (username/password), private key pair, or Single Sign-On (SSO). When running within Snowsight, the app automatically authenticates using the existing session.

- **Interactive Object Browser**:
    - Browse and select database objects (schemas, tables, views, etc.) through a hierarchical interface.
    - Filter objects by name and select specific schemas to work with.
    - Global, per-schema, and individual object selection is supported.

- **DDL Generation and Export**:
    - Automatically generates DDL scripts for your selected objects.
    - Intelligently sorts the script based on object dependencies to ensure correct deployment order.
    - Optionally includes `CREATE SCHEMA` statements for the selected objects.
    - Provides warnings for any hardcoded database references in the DDL.
    - Download the final, consolidated DDL script as a single `.sql` file.

- **Dependency Visualization**:
    - Generate and view an interactive dependency graph to understand the relationships between your database objects.

- **Session Management**:
    - Change your active role and warehouse within the application.
    - Your session state is preserved for a seamless experience.

- **Data Export Helper**:
    - Provides a handy SQL snippet to generate `INSERT` statements for your tables, allowing you to export data along with the DDL.

## About the Author

This application was created by **Sahil Singh**.

