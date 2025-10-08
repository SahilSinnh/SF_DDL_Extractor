# Contains functions for visualizing the dependency graph.

import streamlit as st
from pyvis.network import Network
from typing import Dict, Set, List, Any

def create_dependency_graph_figure(objects: List[Dict[str, Any]], deps: Dict[str, Set[str]], selected_schemas: List[str]):
    # Generates an interactive dependency graph using pyvis.
    net = Network(height="750px", width="100%", bgcolor="#222222", font_color="white", notebook=True, cdn_resources='in_line', directed=True) #type: ignore

    # Create a set of canonical FQNs for all objects for quick lookups
    all_fqns = {obj["_CANON_FQN"] for obj in objects if obj.get("_CANON_FQN")}

    # Filter objects based on selected schemas
    selected_nodes = {
        obj["_CANON_FQN"]
        for obj in objects
        if obj.get("schema") in selected_schemas and obj.get("_CANON_FQN")
    }

    # Add nodes to the graph
    for obj in objects:
        fqn = obj.get("_CANON_FQN")
        if fqn in selected_nodes:
            obj_type = obj.get("object_type", "UNKNOWN")

            # Original color mapping
            color = {
                "DATABASE": "#2196F3",
                "SCHEMA": "#009688",
                "SEQUENCE": "#FFC107",
                "TABLE": "#3F51B5",
                "DYNAMIC TABLE": "#03A9F4",
                "VIEW": "#673AB7",
                "STAGE": "#00BCD4",
                "EXTERNAL TABLE": "#607D8B",
                "FILE FORMAT": "#FF9800",
                "PROCEDURE": "#C2185B",
                "FUNCTION": "#4CAF50",
                "PIPE": "#4682B4",
                "MATERIALIZED VIEW": "#9C27B0",
                "STREAM": "#8BC34A",
                "TASK": "#78909C",
                "MASKING POLICY": "#9E9E9E",
                "TAG": "#E91E63",
            }.get(obj_type, "#90A4AE")
            
            # Map object types to Material Design Icon URLs
            icon_url_base = "https://cdn.jsdelivr.net/npm/@mdi/svg/svg/"
            icon_map = {
                "DATABASE": f"{icon_url_base}database.svg",
                "SCHEMA": f"{icon_url_base}layers.svg",
                "SEQUENCE": f"{icon_url_base}pound.svg",
                "TABLE": f"{icon_url_base}table.svg",
                "DYNAMIC TABLE": f"{icon_url_base}table-refresh.svg",
                "VIEW": f"{icon_url_base}table-eye.svg",
                "STAGE": f"{icon_url_base}cloud-upload.svg",
                "EXTERNAL TABLE": f"{icon_url_base}table-network.svg",
                "FILE FORMAT": f"{icon_url_base}file-cog.svg",
                "PROCEDURE": f"{icon_url_base}script-text.svg",
                "FUNCTION": f"{icon_url_base}function-variant.svg",
                "PIPE": f"{icon_url_base}pipe.svg",
                "MATERIALIZED VIEW": f"{icon_url_base}table-star.svg",
                "STREAM": f"{icon_url_base}view-stream.svg",
                "TASK": f"{icon_url_base}clipboard-check.svg",
                "MASKING POLICY": f"{icon_url_base}shield-half-full.svg",
                "TAG": f"{icon_url_base}tag-multiple.svg",
            }
            icon_url = icon_map.get(obj_type, f"{icon_url_base}help-circle.svg")
            
            net.add_node(
                fqn, 
                label=obj.get("object_name"), 
                title=f"{obj_type}\n{fqn}", 
                shape="circularImage", 
                image=icon_url,
                color=color
            )

    # Add edges for the dependencies
    for fqn, dependencies in deps.items():
        if fqn in selected_nodes:
            for dep_fqn in dependencies:
                # Only add the edge if the dependency is also a node in our graph
                if dep_fqn in selected_nodes:
                    net.add_edge(source=fqn, to=dep_fqn)

    # Generate the HTML content directly
    try:
        return net.generate_html()
    except Exception as e:
        st.error(f"Failed to generate dependency graph: {e}")
        return None
